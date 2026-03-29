#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для извлечения контактов из архивов сайтов с проверкой через DaData.

Что делает:
1. Проверяет наличие полей email и phone в БД (добавляет если нет)
2. Ищет компании, у которых:
   - email = NULL (не проверяли) - проверяем
   - phone = NULL (не проверяли) - проверяем
   (NOT_FOUND и REJECTED не проверяем повторно)
3. Распаковывает архив сайта
4. Ищет email и телефоны во всех HTML файлах
5. Каждый найденный телефон проверяет через DaData
6. Сохраняет в БД:
   - email: найденный email или 'not_found'
   - phone: валидный телефон, 'rejected' или 'not_found'
   - NULL оставляем только при ошибке API

Запуск:
    python extract_contacts.py --db employers.db --archive-dir site_archive
    python extract_contacts.py --db employers.db --archive-dir site_archive --exclude "Другое"

Ключи DaData (получить на dadata.ru):
    DADATA_API_KEY = "ваш_ключ"
    DADATA_SECRET_KEY = "ваш_секрет"
"""

import sqlite3
import argparse
import re
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import logging
from collections import defaultdict

import requests
import time
from tqdm import tqdm
from bs4 import BeautifulSoup
import os
import sys
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# 🔑 НАСТРОЙКИ DADATA из .env
DADATA_API_KEY = os.getenv("DADATA_API_KEY", "")
DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY", "")

if not DADATA_API_KEY or not DADATA_SECRET_KEY:
    print("\n⚠️  DaData не настроен - телефоны будут сохраняться без проверки")
    print("   Получите ключи на dadata.ru и добавьте их в файл .env\n")
    sys.exit(1)

# Разделитель для множественных контактов
SEP = "; "

# Регулярные выражения
EMAIL_REGEX = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
PHONE_REGEX = re.compile(r'(?:(?:8|\+7)[\- ]?)?(?:\(?\d{3}\)?[\- ]?)?[\d\- ]{7,10}')

# Специальные значения для полей
NOT_FOUND = "not_found"
PHONE_REJECTED = "rejected"

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class DaDataValidator:
    """Проверка телефонов через DaData.ru"""
    
    def __init__(self):
        self.api_key = DADATA_API_KEY
        self.secret_key = DADATA_SECRET_KEY
        self.cache = {}           # Кэш результатов
        self.stats = {
            'checked': 0, 
            'valid': 0, 
            'invalid': 0, 
            'errors': 0,
            'api_errors': 0
        }
        self.last_request = 0
        self.min_delay = 0.1      # 10 запросов в секунду
        self.url = "https://cleaner.dadata.ru/api/v1/clean/phone"
        self.headers = {
            "Authorization": f"Token {self.api_key}",
            "X-Secret": self.secret_key,
            "Content-Type": "application/json"
        }
        
        # Проверка наличия ключей
        if self.api_key == "ваш_ключ" or self.secret_key == "ваш_секрет":
            print("\n⚠️  DaData не настроен - телефоны будут сохраняться без проверки")
            print("   Получите ключи на dadata.ru и вставьте их в переменные")
            print("   DADATA_API_KEY и DADATA_SECRET_KEY\n")
            self.disabled = True
        else:
            self.disabled = False
            print("✅ DaData валидатор подключён")
    
    def is_valid(self, phone: str) -> Tuple[bool, Optional[Dict], bool]:
        """
        Проверяет телефон через DaData
        Возвращает (is_valid, data, is_api_error)
        - is_valid: True если номер точно хороший
        - data: данные от DaData (если есть)
        - is_api_error: True если ошибка API (нет денег, ключ не работает и т.д.)
        """
        if self.disabled:
            return True, None, False
        
        # Нормализуем номер для кэша
        clean_phone = re.sub(r'\s+', ' ', phone.strip())
        
        # Проверка кэша
        if clean_phone in self.cache:
            return self.cache[clean_phone]
        
        # Задержка между запросами
        now = time.time()
        if now - self.last_request < self.min_delay:
            time.sleep(self.min_delay - (now - self.last_request))
        
        self.stats['checked'] += 1
        self.last_request = now
        
        try:
            response = requests.post(self.url, headers=self.headers, 
                                    json=[clean_phone], timeout=10)
            
            # ----- 1. Ошибки аутентификации или лимитов (нет денег) -----
            if response.status_code == 401 or response.status_code == 402:
                self.stats['api_errors'] += 1
                self.cache[clean_phone] = (False, None, True)
                return False, None, True
            
            # ----- 2. Успешный ответ -----
            if response.status_code == 200:
                data = response.json()[0]
                
                # Проверка валидности: телефон существует и qc = 0 (хорошее качество)
                if data.get('phone') and data.get('qc', 5) == 0:
                    self.stats['valid'] += 1
                    self.cache[clean_phone] = (True, data, False)
                    return True, data, False
                else:
                    # API отработал, но номер невалидный
                    self.stats['invalid'] += 1
                    self.cache[clean_phone] = (False, None, False)
                    return False, None, False
            else:
                # Другие HTTP ошибки (500, 404 и т.д.)
                self.stats['errors'] += 1
                self.cache[clean_phone] = (False, None, True)
                return False, None, True
                
        except Exception as e:
            # Ошибка соединения, таймаут
            self.stats['errors'] += 1
            logger.debug(f"Ошибка API: {e}")
            return False, None, True


class ContactExtractor:
    def __init__(self, db_path: Path, archive_dir: Path, exclude_categories: List[str] = None):
        self.db_path = db_path
        self.archive_dir = archive_dir
        self.exclude_categories = set(exclude_categories) if exclude_categories else set()
        self.validator = DaDataValidator()
        self.stats = defaultdict(int)
        self.skipped_stats = defaultdict(int)
        self._init_database()
        
        if self.exclude_categories:
            print(f"\n🚫 Исключаемые категории: {', '.join(self.exclude_categories)}")
    
    def _init_database(self):
        """Проверяет и добавляет нужные поля в БД"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        # Получаем существующие колонки
        cur.execute("PRAGMA table_info(employers)")
        columns = [col[1] for col in cur.fetchall()]
        
        # Добавляем email если нет
        if 'email' not in columns:
            try:
                cur.execute("ALTER TABLE employers ADD COLUMN email TEXT")
                print("✅ Добавлено поле email")
            except sqlite3.OperationalError:
                pass
        
        # Добавляем phone если нет
        if 'phone' not in columns:
            try:
                cur.execute("ALTER TABLE employers ADD COLUMN phone TEXT")
                print("✅ Добавлено поле phone")
            except sqlite3.OperationalError:
                pass
        
        # Добавляем дату обновления контактов
        if 'contacts_updated' not in columns:
            try:
                cur.execute("ALTER TABLE employers ADD COLUMN contacts_updated TIMESTAMP")
                print("✅ Добавлено поле contacts_updated")
            except sqlite3.OperationalError:
                pass
        
        # Проверяем наличие поля category (для фильтрации)
        if 'category' not in columns:
            print("⚠️  В базе нет поля category - фильтрация по категориям недоступна")
            print("   Запустите categorize_companies.py для категоризации компаний")
            self.exclude_categories = set()
        
        conn.commit()
        conn.close()
    
    def get_employers(self) -> List[Dict]:
        """
        Получает компании для обработки.
        
        Логика:
        - Email: проверяем если NULL (не проверяли)
        - Телефон: проверяем если NULL (не проверяли)
        - not_found и rejected НЕ проверяем повторно
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Базовый запрос - проверяем только NULL поля
        query = """
            SELECT 
                employer_id, 
                employer_name, 
                archive_path,
                email,
                phone,
                category
            FROM employers
            WHERE archive_path IS NOT NULL AND archive_path != ''
            AND (
                email IS NULL OR phone IS NULL
            )
        """
        
        # Добавляем фильтрацию по категориям
        params = []
        if self.exclude_categories:
            placeholders = ','.join(['?'] * len(self.exclude_categories))
            query += f" AND (category IS NULL OR category NOT IN ({placeholders}))"
            params.extend(self.exclude_categories)
        
        query += " ORDER BY employer_name"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        
        employers = []
        need_email_only = 0
        need_phone_only = 0
        need_both = 0
        
        for row in rows:
            emp = dict(row)
            
            # Определяем, что нужно искать (только NULL поля)
            email_value = emp.get('email')
            phone_value = emp.get('phone')
            
            need_email = (email_value is None)  # не not_found, не строка с email
            need_phone = (phone_value is None)  # не not_found, не rejected, не номер
            
            # Если оба поля не нужно проверять - пропускаем
            if not need_email and not need_phone:
                continue
            
            emp['need_email'] = need_email
            emp['need_phone'] = need_phone
            
            # Статистика
            if need_email and need_phone:
                need_both += 1
            elif need_email:
                need_email_only += 1
            elif need_phone:
                need_phone_only += 1
            
            archive_path = Path(emp['archive_path'])
            if not archive_path.is_absolute():
                archive_path = self.archive_dir / archive_path.name
            
            if archive_path.exists():
                emp['full_archive_path'] = archive_path
                employers.append(emp)
            else:
                logger.warning(f"Архив не найден: {archive_path}")
        
        # Выводим статистику
        print(f"\n📊 Статистика по компаниям для обработки:")
        print(f"   • Всего: {len(employers)}")
        print(f"   • Нужен только email: {need_email_only}")
        print(f"   • Нужен только телефон: {need_phone_only}")
        print(f"   • Нужны оба: {need_both}")
        
        # Статистика по уже обработанным (not_found/rejected)
        self._print_already_processed_stats()
        
        return employers
    
    def _print_already_processed_stats(self):
        """Выводит статистику по уже обработанным компаниям"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        # Email статистика
        cur.execute("""
            SELECT COUNT(*) FROM employers 
            WHERE email = ? AND archive_path IS NOT NULL
        """, (NOT_FOUND,))
        email_not_found = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM employers 
            WHERE email IS NOT NULL AND email != '' AND email != ? AND archive_path IS NOT NULL
        """, (NOT_FOUND,))
        email_found = cur.fetchone()[0]
        
        # Телефон статистика
        cur.execute("""
            SELECT COUNT(*) FROM employers 
            WHERE phone = ? AND archive_path IS NOT NULL
        """, (NOT_FOUND,))
        phone_not_found = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM employers 
            WHERE phone = ? AND archive_path IS NOT NULL
        """, (PHONE_REJECTED,))
        phone_rejected = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM employers 
            WHERE phone IS NOT NULL AND phone != '' AND phone != ? AND phone != ? AND archive_path IS NOT NULL
        """, (NOT_FOUND, PHONE_REJECTED))
        phone_valid = cur.fetchone()[0]
        
        conn.close()
        
        print(f"\n📊 Уже обработано (не требуют повторной проверки):")
        print(f"   • Email found: {email_found}")
        print(f"   • Email not_found: {email_not_found}")
        print(f"   • Phone valid: {phone_valid}")
        print(f"   • Phone rejected: {phone_rejected}")
        print(f"   • Phone not_found: {phone_not_found}")
    
    def extract_text_from_html(self, html_content: str) -> str:
        """Извлекает видимый текст из HTML"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            return ' '.join(chunk for chunk in chunks if chunk)
        except:
            return ""
    
    def format_phone(self, phone: str) -> str:
        """Форматирует телефон в +7 (123) 456-78-90"""
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 11 and digits[0] in ('7', '8'):
            return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
        elif len(digits) == 10:
            return f"+7 ({digits[:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:10]}"
        return phone
    
    def extract_contacts(self, text: str) -> Tuple[Set[str], Set[str]]:
        """Извлекает email и телефоны из текста"""
        emails = set()
        phones = set()
        
        # Email
        for email in EMAIL_REGEX.findall(text):
            if 5 < len(email) < 100 and '.' in email:
                if not any(x in email.lower() for x in ['example.com', 'test.com']):
                    emails.add(email.lower())
        
        # Телефоны
        for phone in PHONE_REGEX.findall(text):
            # Пропускаем явно не-телефоны
            context = text[max(0, text.find(phone)-20):min(len(text), text.find(phone)+len(phone)+20)]
            if 'ИНН' in context or 'инн' in context:
                continue
            if re.match(r'\d{2}\.\d{2}\.\d{4}', phone):
                continue
            
            formatted = self.format_phone(phone)
            if formatted.startswith('+7'):
                phones.add(formatted)
        
        return emails, phones
    
    def process_company(self, company: Dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Обрабатывает одну компанию
        Возвращает (email_value, phone_value)
        
        email_value:
        - найденный email (строка)
        - NOT_FOUND если не найден
        
        phone_value:
        - валидный телефон (строка)
        - PHONE_REJECTED если все телефоны невалидные
        - NOT_FOUND если телефоны не найдены
        - None только при ошибке API (будет повтор)
        """
        archive_path = company['full_archive_path']
        name = company['employer_name']
        category = company.get('category', 'не определена')
        
        print(f"\n📁 {name}")
        print(f"   Категория: {category}")
        print(f"   Архив: {archive_path.name}")
        need_email = company['need_email']
        need_phone = company['need_phone']
        print(f"   Нужно найти: {need_email and '📧' or ''} {need_phone and '📞' or ''}")
        
        # Проверяем, нужно ли исключить эту компанию
        if category in self.exclude_categories:
            print(f"   🚫 Исключена по категории: {category}")
            self.skipped_stats[category] += 1
            return None, None
        
        all_emails = set()
        all_phones = set()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            try:
                # Распаковка
                with tarfile.open(archive_path, 'r:gz') as tar:
                    tar.extractall(path=temp_path)
                
                # Поиск HTML файлов
                html_files = list(temp_path.rglob('*.html')) + list(temp_path.rglob('*.htm'))
                
                if not html_files:
                    print("   ⚠️  Нет HTML файлов")
                    # Если нет HTML файлов, считаем что контакты не найдены
                    final_email = NOT_FOUND if need_email else None
                    final_phone = NOT_FOUND if need_phone else None
                    return final_email, final_phone
                
                # Сортировка: сначала страницы с контактами
                html_files.sort(key=lambda x: 0 if 'contact' in x.name.lower() or 'контакт' in x.name.lower() else 1)
                
                # Обработка файлов
                for html_file in html_files:
                    try:
                        content = html_file.read_text(encoding='utf-8', errors='ignore')
                        text = self.extract_text_from_html(content)
                        
                        if text:
                            emails, phones = self.extract_contacts(text)
                            all_emails.update(emails)
                            all_phones.update(phones)
                    except:
                        continue
                
                # ========== ОБРАБОТКА EMAIL ==========
                final_email = None
                if need_email:
                    if all_emails:
                        print(f"   📧 Найдены email: {', '.join(all_emails)}")
                        final_email = SEP.join(sorted(all_emails))
                    else:
                        print("   📧 Email не найдены")
                        final_email = NOT_FOUND
                else:
                    if all_emails:
                        print(f"   📧 Email уже есть в базе (не NULL), найденные не сохраняем")
                
                # ========== ОБРАБОТКА ТЕЛЕФОНОВ ==========
                final_phone = None
                api_error_occurred = False
                
                if need_phone:
                    if all_phones:
                        print(f"   📞 Найдено кандидатов: {len(all_phones)}")
                        
                        valid_phones = []
                        rejected_count = 0
                        
                        for phone in sorted(all_phones):
                            is_valid, data, is_api_error = self.validator.is_valid(phone)
                            
                            if is_api_error:
                                print(f"      ⚠️ Ошибка API при проверке {phone} - проверка отложена")
                                api_error_occurred = True
                                break
                            elif is_valid:
                                valid_phones.append(phone)
                                print(f"      ✅ {phone} - ВАЛИДНЫЙ")
                                if data:
                                    print(f"         Оператор: {data.get('provider', '—')}")
                                    print(f"         Регион: {data.get('region', '—')}")
                            else:
                                rejected_count += 1
                                print(f"      ❌ {phone} - ОТВЕРГНУТ")
                        
                        if api_error_occurred:
                            # Ошибка API - ничего не сохраняем, оставляем NULL
                            final_phone = None
                            print(f"   ⚠️ Ошибка API DaData, проверка телефонов будет повторена при следующем запуске")
                        elif valid_phones:
                            final_phone = SEP.join(sorted(valid_phones))
                        elif rejected_count > 0:
                            final_phone = PHONE_REJECTED
                            print(f"   📞 Все найденные телефоны отвергнуты, сохранен статус 'rejected'")
                        else:
                            # Не должно случиться, но на всякий случай
                            final_phone = NOT_FOUND
                    else:
                        print("   📞 Телефоны не найдены")
                        final_phone = NOT_FOUND
                else:
                    if all_phones:
                        print(f"   📞 Телефон уже есть в базе (не NULL), найденные не сохраняем")
                
                return final_email, final_phone
                
            except Exception as e:
                print(f"   ❌ Ошибка: {e}")
                return None, None
    
    def save_to_db(self, employer_id: str, email: Optional[str], phone: Optional[str]):
        """Сохраняет контакты в БД (обновляет только указанные поля)"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        now = datetime.now().isoformat()
        
        # Формируем запрос динамически - обновляем только то, что нашли
        updates = []
        params = []
        
        if email is not None:
            updates.append("email = ?")
            params.append(email)
        
        if phone is not None:
            updates.append("phone = ?")
            params.append(phone)
        
        if updates:
            updates.append("contacts_updated = ?")
            params.append(now)
            params.append(employer_id)
            
            query = f"""
                UPDATE employers
                SET {', '.join(updates)}
                WHERE employer_id = ?
            """
            
            cur.execute(query, params)
            conn.commit()
        
        conn.close()
    
    def print_stats(self):
        """Выводит итоговую статистику"""
        print("\n" + "="*60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА (текущий запуск)")
        print("="*60)
        print(f"Всего обработано в этом запуске: {self.stats['total']}")
        
        if self.stats['total'] > 0:
            print(f"\n📧 Email:")
            print(f"   • Найдено: {self.stats['email_found']}")
            print(f"   • Не найдено (not_found): {self.stats['email_not_found']}")
            
            print(f"\n📞 Телефон:")
            print(f"   • Валидных: {self.stats['phone_valid']}")
            print(f"   • Отвергнуто (rejected): {self.stats['phone_rejected']}")
            print(f"   • Не найдено (not_found): {self.stats['phone_not_found']}")
            print(f"   • Отложено (ошибка API): {self.stats['phone_api_error']}")
        
        if self.stats['errors'] > 0:
            print(f"\n⚠️  Других ошибок: {self.stats['errors']}")
        
        if self.skipped_stats:
            print(f"\n🚫 Исключено по категориям: {sum(self.skipped_stats.values())}")
            for category, count in sorted(self.skipped_stats.items(), key=lambda x: -x[1]):
                print(f"   • {category}: {count}")
        
        if not self.validator.disabled and self.validator.stats['checked'] > 0:
            print("\n📊 DaData статистика:")
            print(f"   • Проверено телефонов: {self.validator.stats['checked']}")
            print(f"   • Валидных: {self.validator.stats['valid']}")
            print(f"   • Невалидных: {self.validator.stats['invalid']}")
            print(f"   • Ошибок API (лимиты, ключ): {self.validator.stats['api_errors']}")
            print(f"   • Других ошибок: {self.validator.stats['errors']}")
        
        print("="*60)
    
    def run(self):
        """Основной метод запуска"""
        
        # Получаем список компаний
        employers = self.get_employers()
        
        if not employers:
            logger.info("Нет компаний для обработки (все контакты уже проверены)")
            return
        
        print(f"\n📋 Компаний для обработки в этом запуске: {len(employers)}")
        if self.exclude_categories:
            print(f"🚫 Исключаемые категории: {', '.join(self.exclude_categories)}")
        print("="*60)
        
        # Обработка каждой компании
        for company in tqdm(employers, desc="Прогресс", unit="компания"):
            self.stats['total'] += 1
            
            try:
                email, phone = self.process_company(company)
                
                # Статистика по email
                if email is not None:
                    if email == NOT_FOUND:
                        self.stats['email_not_found'] += 1
                    else:
                        self.stats['email_found'] += 1
                
                # Статистика по телефону
                if phone is not None:
                    if phone == NOT_FOUND:
                        self.stats['phone_not_found'] += 1
                    elif phone == PHONE_REJECTED:
                        self.stats['phone_rejected'] += 1
                    else:
                        self.stats['phone_valid'] += 1
                elif phone is None and company['need_phone']:
                    # None при ошибке API
                    self.stats['phone_api_error'] += 1
                
                # Сохраняем в БД
                self.save_to_db(company['employer_id'], email, phone)
                
            except Exception as e:
                self.stats['errors'] += 1
                print(f"\n❌ Ошибка при обработке {company['employer_name']}: {e}")
                logger.error(f"Ошибка: {e}", exc_info=True)
        
        # Итог
        self.print_stats()
        logger.info("✅ Обработка завершена!")


def main():
    parser = argparse.ArgumentParser(description='Извлечение контактов с проверкой через DaData')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе (employers.db)')
    parser.add_argument('--archive-dir', default='site_archive', help='Папка с архивами сайтов')
    parser.add_argument('--exclude', nargs='+', metavar='КАТЕГОРИЯ',
                       help='Исключить компании с указанными категориями (можно несколько)')
    
    args = parser.parse_args()
    
    # Проверка путей
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База не найдена: {db_path}")
        return
    
    archive_dir = Path(args.archive_dir)
    if not archive_dir.exists():
        print(f"❌ Папка с архивами не найдена: {archive_dir}")
        return
    
    # Запуск
    extractor = ContactExtractor(db_path, archive_dir, args.exclude)
    extractor.run()


if __name__ == '__main__':
    main()
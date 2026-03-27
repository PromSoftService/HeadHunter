#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для извлечения контактов из архивов сайтов с проверкой через DaData.

Что делает:
1. Проверяет наличие полей email и phone в БД (добавляет если нет)
2. Ищет компании, у которых нет email ИЛИ нет phone
3. Для каждой компании определяет, что именно нужно искать
4. Распаковывает архив сайта
5. Ищет email и телефоны во всех HTML файлах
6. Каждый найденный телефон проверяет через DaData
7. Сохраняет в БД только то, что искали (email если был пустой, телефон если был пустой)
8. Пишет подробный лог в консоль

Запуск:
    python extract_contacts.py --db employers.db --archive-dir site_archive

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

# ============================================================
# 🔑 НАСТРОЙКИ DADATA - ВСТАВЬТЕ СВОИ КЛЮЧИ
# ============================================================
DADATA_API_KEY = "1f89f6298e5e8fc98500954a2a73498a58d39a24"
DADATA_SECRET_KEY = "4cf476bf5820e46c89424e9fe534cc5f440f2b72"

# Разделитель для множественных контактов
SEP = "; "

# Регулярные выражения
EMAIL_REGEX = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
PHONE_REGEX = re.compile(r'(?:(?:8|\+7)[\- ]?)?(?:\(?\d{3}\)?[\- ]?)?[\d\- ]{7,10}')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class DaDataValidator:
    """Проверка телефонов через DaData.ru"""
    
    def __init__(self):
        self.api_key = DADATA_API_KEY
        self.secret_key = DADATA_SECRET_KEY
        self.cache = {}           # Кэш результатов
        self.stats = {'checked': 0, 'valid': 0, 'invalid': 0, 'errors': 0}
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
    
    def is_valid(self, phone: str) -> Tuple[bool, Optional[Dict]]:
        """
        Проверяет телефон через DaData
        Возвращает (True, данные) если валидный, (False, None) если нет
        """
        if self.disabled:
            return True, None
        
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
            
            if response.status_code == 200:
                data = response.json()[0]
                
                # Проверка валидности
                if data.get('phone') and data.get('qc', 5) <= 2 and data.get('provider'):
                    self.stats['valid'] += 1
                    self.cache[clean_phone] = (True, data)
                    return True, data
                else:
                    self.stats['invalid'] += 1
                    self.cache[clean_phone] = (False, None)
                    return False, None
            else:
                self.stats['errors'] += 1
                return False, None
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.debug(f"Ошибка API: {e}")
            return False, None


class ContactExtractor:
    def __init__(self, db_path: Path, archive_dir: Path):
        self.db_path = db_path
        self.archive_dir = archive_dir
        self.validator = DaDataValidator()
        self.stats = defaultdict(int)
        self._init_database()
    
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
        
        conn.commit()
        conn.close()
    
    def get_employers(self) -> List[Dict]:
        """
        Получает компании для обработки и определяет, что именно нужно искать
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                employer_id, 
                employer_name, 
                archive_path,
                email,
                phone
            FROM employers
            WHERE archive_path IS NOT NULL AND archive_path != ''
            AND (email IS NULL OR email = '' OR phone IS NULL OR phone = '')
            ORDER BY employer_name
        """)
        
        rows = cur.fetchall()
        conn.close()
        
        employers = []
        for row in rows:
            emp = dict(row)
            
            # Определяем, что нужно искать
            emp['need_email'] = emp.get('email') is None or emp.get('email') == ''
            emp['need_phone'] = emp.get('phone') is None or emp.get('phone') == ''
            
            archive_path = Path(emp['archive_path'])
            if not archive_path.is_absolute():
                archive_path = self.archive_dir / archive_path.name
            
            if archive_path.exists():
                emp['full_archive_path'] = archive_path
                employers.append(emp)
            else:
                logger.warning(f"Архив не найден: {archive_path}")
        
        return employers
    
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
        Возвращает (email_str, phone_str)
        """
        archive_path = company['full_archive_path']
        name = company['employer_name']
        
        print(f"\n📁 {name}")
        print(f"   Архив: {archive_path.name}")
        print(f"   Нужно найти: {company['need_email'] and '📧' or ''} {company['need_phone'] and '📞' or ''}")
        
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
                    return None, None
                
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
                
                # Формируем результат ТОЛЬКО для того, что нужно
                final_emails = set()
                final_phones = set()
                
                # Email (если нужно)
                if company['need_email']:
                    if all_emails:
                        print(f"   📧 Найдены email: {', '.join(all_emails)}")
                        final_emails.update(all_emails)
                    else:
                        print("   📧 Email не найдены")
                else:
                    if all_emails:
                        print(f"   📧 Email уже есть в базе, найденные не сохраняем: {', '.join(all_emails)}")
                
                # Телефоны (если нужно)
                if company['need_phone']:
                    if all_phones:
                        print(f"   📞 Найдено кандидатов: {len(all_phones)}")
                        for phone in sorted(all_phones):
                            is_valid, data = self.validator.is_valid(phone)
                            if is_valid:
                                final_phones.add(phone)
                                print(f"      ✅ {phone} - ВАЛИДНЫЙ")
                                if data:
                                    print(f"         Оператор: {data.get('provider', '—')}")
                                    print(f"         Регион: {data.get('region', '—')}")
                            else:
                                print(f"      ❌ {phone} - ОТВЕРГНУТ")
                    else:
                        print("   📞 Телефоны не найдены")
                else:
                    if all_phones:
                        print(f"   📞 Телефон уже есть в базе, найденные не сохраняем")
                
                # Формирование результата
                email_str = SEP.join(sorted(final_emails)) if final_emails else None
                phone_str = SEP.join(sorted(final_phones)) if final_phones else None
                
                return email_str, phone_str
                
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
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("="*60)
        print(f"Всего обработано: {self.stats['total']}")
        print(f"✅ Найдены контакты: {self.stats['found']}")
        print(f"   • Email: {self.stats['email']}")
        print(f"   • Телефон: {self.stats['phone']}")
        print(f"   • И то и другое: {self.stats['both']}")
        print(f"❌ Контакты не найдены: {self.stats['not_found']}")
        print(f"⚠️  Ошибки: {self.stats['errors']}")
        
        if not self.validator.disabled:
            print("\n📊 DaData:")
            print(f"   • Проверено: {self.validator.stats['checked']}")
            print(f"   • Валидных: {self.validator.stats['valid']}")
            print(f"   • Невалидных: {self.validator.stats['invalid']}")
            print(f"   • Ошибок: {self.validator.stats['errors']}")
        print("="*60)
    
    def run(self):
        """Основной метод запуска"""
        
        # Получаем список компаний
        employers = self.get_employers()
        
        if not employers:
            logger.info("Нет компаний для обработки")
            return
        
        print(f"\n📋 Найдено компаний для обработки: {len(employers)}")
        print("="*60)
        
        # Обработка каждой компании
        for company in tqdm(employers, desc="Прогресс", unit="компания"):
            self.stats['total'] += 1
            
            try:
                email, phone = self.process_company(company)
                
                # Статистика
                if email or phone:
                    self.stats['found'] += 1
                if email:
                    self.stats['email'] += 1
                if phone:
                    self.stats['phone'] += 1
                if email and phone:
                    self.stats['both'] += 1
                
                if not email and not phone:
                    self.stats['not_found'] += 1
                
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
    extractor = ContactExtractor(db_path, archive_dir)
    extractor.run()


if __name__ == '__main__':
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для категоризации компаний из базы данных через DeepSeek API.
Версия 4.0 - двухэтапная: сначала по company_info, затем по архиву сайта.
"""

import sqlite3
import argparse
import json
import time
import re
import tarfile
import tempfile
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import requests
from tqdm import tqdm
import os
import sys
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Загружаем переменные из .env
load_dotenv()

# 🔑 API КЛЮЧ DEEPSEEK из .env
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
if not DEEPSEEK_API_KEY:
    print("❌ ОШИБКА: DEEPSEEK_API_KEY не найден в .env")
    print("   Получите ключ на platform.deepseek.com и добавьте в файл .env")
    print("   Пример: DEEPSEEK_API_KEY=sk-ваш_ключ\n")
    sys.exit(1)

# Конфигурация API
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
MODEL = "deepseek-chat"

# ============================================================
# 🎯 КАТЕГОРИИ (в порядке убывания приоритета)
# ============================================================

CATEGORIES = [
    # 🔥 Наивысший приоритет - узкие специализации
    "Производители котельного оборудования",
    "Поставщики котельного оборудования",
    "Монтаж/строительство котельных",
    "Интеграторы АСУТП",
    "Разработчики промышленного ПО",
    "Проектные институты (автоматизация)",
    "Крупные промышленные корпорации",
    
    # 🟡 Средний приоритет
    "Поставщики промышленного оборудования",
    "Машиностроительные заводы",
    "Энергетические компании",
    "Химические производства",
    "Пищевые производства",
    "Металлургические комбинаты",
    "Нефтегазовые компании (не киты)",
    "Строительно-монтажные организации",
    
    # 🔴 Всё остальное
    "Другое",
]

# ============================================================
# 📝 ПРОМПТЫ ДЛЯ DEEPSEEK
# ============================================================

SYSTEM_MESSAGE = "Ты эксперт по классификации промышленных компаний. Отвечай только в формате JSON. Строго выбирай категорию из предложенного списка."

PROMPT_COMPANY_INFO = """Ты эксперт по классификации промышленных компаний для поиска заказчиков программистов АСУТП.

Информация о компании:
Название: {company_name}
Описание: {company_info}

Выбери ОДНУ категорию из списка. Категории расположены в порядке приоритета — если компания подходит под несколько, выбери ту, которая выше в списке.

Список категорий:
{categories}

Правила:
- Интеграторы АСУТП = сами разрабатывают и внедряют системы автоматизации, SCADA, ПЛК
- Производители котельного оборудования = производят котлы, котельные установки
- Поставщики котельного оборудования = продают котлы (не производят)
- Монтаж/строительство котельных = строят, монтируют, обслуживают котельные
- Поставщики промышленного оборудования = продают КИПиА, контроллеры, насосы (не внедряют)
- Проектные институты = только проектируют, ищут подрядчиков
- Крупные промышленные корпорации = Газпром, Росатом, РУСАЛ, Норникель, Северсталь и т.п.
- Машиностроительные заводы = производят оборудование, есть автоматизация
- Энергетические компании = ТЭЦ, ГРЭС, электросети
- Химические производства = нефтехимия, удобрения, кислоты
- Пищевые производства = пищевая промышленность
- Металлургические комбинаты = чёрная и цветная металлургия
- Нефтегазовые компании (не киты) = добыча и переработка (кроме крупнейших корпораций)
- Строительно-монтажные организации = монтируют, налаживают оборудование

Если компания НЕ ПОДХОДИТ ни под одну категорию из списка, верни "Другое".

Верни ТОЛЬКО JSON в формате:
{{"category": "название категории"}}
"""

PROMPT_ARCHIVE = """Ты эксперт по классификации промышленных компаний для поиска заказчиков программистов АСУТП.

Название компании: {company_name}

Текст с сайта (первые {text_length} символов):
{text}

Выбери ОДНУ категорию из списка. Категории расположены в порядке приоритета — если компания подходит под несколько, выбери ту, которая выше в списке.

Список категорий:
{categories}

Правила:
- Интеграторы АСУТП = сами разрабатывают и внедряют системы автоматизации, SCADA, ПЛК
- Производители котельного оборудования = производят котлы, котельные установки
- Поставщики котельного оборудования = продают котлы (не производят)
- Монтаж/строительство котельных = строят, монтируют, обслуживают котельные
- Поставщики промышленного оборудования = продают КИПиА, контроллеры, насосы (не внедряют)
- Проектные институты = только проектируют, ищут подрядчиков
- Крупные промышленные корпорации = Газпром, Росатом, РУСАЛ, Норникель, Северсталь и т.п.
- Машиностроительные заводы = производят оборудование, есть автоматизация
- Энергетические компании = ТЭЦ, ГРЭС, электросети
- Химические производства = нефтехимия, удобрения, кислоты
- Пищевые производства = пищевая промышленность
- Металлургические комбинаты = чёрная и цветная металлургия
- Нефтегазовые компании (не киты) = добыча и переработка (кроме крупнейших корпораций)
- Строительно-монтажные организации = монтируют, налаживают оборудование

Если компания НЕ ПОДХОДИТ ни под одну категорию из списка, верни "Другое".

Верни ТОЛЬКО JSON в формате:
{{"category": "название категории"}}
"""


def init_database(db_path: Path):
    """Добавляет поля для категорий, если их нет"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    
    if 'category' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category TEXT")
        print("✅ Добавлено поле category")
    
    if 'category_priority' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_priority TEXT")
        print("✅ Добавлено поле category_priority")
    
    if 'category_updated' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_updated TIMESTAMP")
        print("✅ Добавлено поле category_updated")
    
    if 'category_notes' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_notes TEXT")
        print("✅ Добавлено поле category_notes")
    
    conn.commit()
    conn.close()


def get_priority(category: str) -> str:
    """Определяет приоритет категории"""
    primary = [
        "Производители котельного оборудования",
        "Поставщики котельного оборудования",
        "Монтаж/строительство котельных",
        "Интеграторы АСУТП",
        "Разработчики промышленного ПО",
        "Проектные институты (автоматизация)",
        "Крупные промышленные корпорации"
    ]
    medium = [
        "Поставщики промышленного оборудования",
        "Машиностроительные заводы",
        "Энергетические компании",
        "Химические производства",
        "Пищевые производства",
        "Металлургические комбинаты",
        "Нефтегазовые компании (не киты)",
        "Строительно-монтажные организации"
    ]
    
    if category in primary:
        return "primary"
    elif category in medium:
        return "medium"
    else:
        return "excluded"


def extract_text_from_html(html_content: str) -> str:
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


def extract_text_from_archive(archive_path: Path, max_chars: int = 3000) -> str:
    """
    Распаковывает архив и извлекает текст из HTML файлов.
    Равномерно распределяет max_chars между страницами.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        try:
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(path=temp_path)
            
            # Находим все HTML файлы
            html_files = list(temp_path.rglob('*.html')) + list(temp_path.rglob('*.htm'))
            
            if not html_files:
                return ""
            
            # Сортируем по важности: index, contact, about, остальные
            def priority(filepath):
                name = filepath.name.lower()
                if name in ['index.html', 'index.htm', 'default.html', 'default.htm']:
                    return 0
                elif 'contact' in name or 'kontakt' in name or 'контакт' in name:
                    return 1
                elif 'about' in name or 'company' in name or 'о компании' in name:
                    return 2
                else:
                    return 3
            
            html_files.sort(key=priority)
            
            # Сначала извлекаем текст со всех страниц, чтобы знать их длину
            page_texts = []
            for html_file in html_files:
                try:
                    content = html_file.read_text(encoding='utf-8', errors='ignore')
                    text = extract_text_from_html(content)
                    text = ' '.join(text.split())  # Нормализуем пробелы
                    if text:
                        page_texts.append(text)
                except:
                    continue
            
            if not page_texts:
                return ""
            
            # Равномерно распределяем max_chars между страницами
            remaining = max_chars
            n_pages = len(page_texts)
            result_parts = []
            
            for i, text in enumerate(page_texts):
                if remaining <= 0:
                    break
                
                # Бюджет для текущей страницы
                budget = remaining // (n_pages - i)
                # Берём текст, но не больше бюджета
                chunk = text[:budget]
                result_parts.append(chunk)
                remaining -= len(chunk)
            
            return "\n\n".join(result_parts)
                
        except Exception as e:
            return ""


def call_deepseek(prompt: str) -> Tuple[Optional[str], str]:
    """
    Отправляет промпт в DeepSeek и возвращает (категория, notes)
    """
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 100
    }
    
    try:
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        content = result['choices'][0]['message']['content'].strip()
        
        # Парсим JSON
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            category = data.get('category', '').strip()
            
            if category in CATEGORIES:
                return category, ""
            else:
                return "Другое", f"API вернул неизвестную категорию: {category}"
        else:
            return "Другое", f"Не удалось распарсить JSON: {content[:100]}"
            
    except Exception as e:
        return None, f"Ошибка API: {e}"


def categorize_by_info(company_name: str, company_info: str) -> Tuple[Optional[str], str]:
    """
    Категоризация по company_info
    """
    # Ограничиваем описание
    if len(company_info) > 2000:
        company_info = company_info[:2000]
    
    categories_str = "\n".join([f"  • {cat}" for cat in CATEGORIES])
    
    prompt = PROMPT_COMPANY_INFO.format(
        company_name=company_name,
        company_info=company_info,
        categories=categories_str
    )
    return call_deepseek(prompt)


def categorize_by_archive(archive_path: Path, company_name: str) -> Tuple[Optional[str], str]:
    """
    Категоризация по содержимому архива сайта
    """
    print(f"   📦 Анализ архива: {archive_path.name}")
    
    text = extract_text_from_archive(archive_path, max_chars=3000)
    if not text:
        return "Другое", "Архив пуст или не содержит текста"
    
    categories_str = "\n".join([f"  • {cat}" for cat in CATEGORIES])
    
    prompt = PROMPT_ARCHIVE.format(
        company_name=company_name,
        text=text,
        text_length=len(text),
        categories=categories_str
    )
    return call_deepseek(prompt)


def get_companies_to_categorize(db_path: Path, limit: Optional[int] = None, 
                               all_companies: bool = False,
                               company_id: Optional[str] = None,
                               priority_only: bool = False) -> List[Dict]:
    """
    Получает компании для категоризации
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    if company_id:
        cur.execute("""
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers
            WHERE employer_id = ?
        """, (company_id,))
    elif all_companies:
        cur.execute("""
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
            ORDER BY employer_name
        """)
    else:
        limit = limit or 10
        base_query = """
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
            AND (category IS NULL OR category = '')
        """
        
        if priority_only:
            # Исключаем заведомо ненужных по ключевым словам в названии
            exclude_patterns = [
                'газпром', 'роснефть', 'лукойл', 'северсталь', 'нлмк', 'ммк',
                'росатом', 'транснефть', 'сургутнефтегаз', 'татнефть',
                'hh.ru', 'headhunter', 'superjob', 'кадровое', 'рекрутинг',
                'школа', 'колледж', 'институт', 'университет'
            ]
            for pattern in exclude_patterns:
                base_query += f" AND employer_name NOT LIKE '%{pattern}%'"
        
        base_query += " ORDER BY employer_name LIMIT ?"
        cur.execute(base_query, (limit,))
    
    rows = cur.fetchall()
    conn.close()
    
    companies = []
    for row in rows:
        emp = dict(row)
        # Проверяем существование архива
        if emp.get('archive_path'):
            archive_path = Path(emp['archive_path'])
            if not archive_path.is_absolute():
                archive_path = Path('site_archive') / archive_path.name
            if archive_path.exists():
                emp['full_archive_path'] = archive_path
        companies.append(emp)
    
    return companies


def save_category(db_path: Path, employer_id: str, category: str, notes: str = ""):
    """Сохраняет категорию в базу данных"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    now = datetime.now().isoformat()
    priority = get_priority(category)
    
    cur.execute("""
        UPDATE employers
        SET category = ?, category_priority = ?, category_updated = ?, category_notes = ?
        WHERE employer_id = ?
    """, (category, priority, now, notes, employer_id))
    
    conn.commit()
    conn.close()


def categorize_company(company: Dict) -> Tuple[Optional[str], str, bool]:
    """
    Двухэтапная категоризация:
    1. Сначала по company_info
    2. Если вернулось "Другое" или None, то по архиву сайта
    Возвращает (category, notes, used_archive)
    """
    name = company['employer_name']
    info = company['company_info']
    
    # Этап 1: по company_info
    category, notes = categorize_by_info(name, info)
    
    if category and category != "Другое":
        return category, notes, False
    
    # Этап 2: по архиву сайта
    archive_path = company.get('full_archive_path')
    if archive_path and archive_path.exists():
        category, notes = categorize_by_archive(archive_path, name)
        if category and category != "Другое":
            return category, notes, True
    
    # Всё, ничего не подошло
    return "Другое", "Не удалось определить категорию", False


def main():
    parser = argparse.ArgumentParser(description='Категоризация компаний через DeepSeek API (версия 4.0)')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе')
    parser.add_argument('--archive-dir', default='site_archive', help='Папка с архивами сайтов')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--limit', type=int, help='Количество компаний для категоризации')
    group.add_argument('--all', action='store_true', help='Категоризировать ВСЕ компании')
    group.add_argument('--id', dest='company_id', help='Категоризировать конкретную компанию')
    group.add_argument('--priority', action='store_true', 
                      help='Только приоритетные категории (исключить заведомо ненужных)')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База не найдена: {db_path}")
        return
    
    init_database(db_path)
    
    if args.limit or args.all or args.company_id or args.priority:
        if args.priority:
            mode = "priority"
            limit = args.limit or 10
        elif args.limit:
            mode = "limit"
            limit = args.limit
        elif args.all:
            mode = "all"
            limit = None
        elif args.company_id:
            mode = "id"
            limit = None
        else:
            mode = "default"
            limit = 10
        
        companies = get_companies_to_categorize(
            db_path, 
            limit=limit if mode != "all" else None,
            all_companies=(mode == "all"),
            company_id=args.company_id,
            priority_only=(mode == "priority")
        )
        
        if not companies:
            print("✅ Нет компаний для категоризации")
            return
        
        print(f"\n{'='*60}")
        if mode == "id":
            print(f"🔍 Обработка компании с ID: {args.company_id}")
        elif mode == "all":
            print(f"📊 Обработка ВСЕХ компаний: {len(companies)} шт")
        elif mode == "priority":
            print(f"🔥 Обработка {limit} приоритетных компаний")
        else:
            print(f"📊 Обработка {limit} компаний")
        print('='*60)
        
        stats = {
            'total': len(companies),
            'primary': 0,
            'medium': 0,
            'excluded': 0,
            'by_archive': 0,
            'failed': 0,
            'by_category': {}
        }
        
        for company in tqdm(companies, desc="Категоризация", unit="комп"):
            emp_id = company['employer_id']
            name = company['employer_name']
            
            print(f"\n📁 {name}")
            print(f"   ID: {emp_id}")
            
            category, notes, used_archive = categorize_company(company)
            
            if category:
                priority = get_priority(category)
                stats[priority] += 1
                stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
                if used_archive:
                    stats['by_archive'] += 1
                
                save_category(db_path, emp_id, category, notes)
                
                priority_symbol = {
                    "primary": "🔥",
                    "medium": "🟡",
                    "excluded": "🔴"
                }.get(priority, "❓")
                
                archive_mark = " 📦 (по архиву)" if used_archive else ""
                print(f"   {priority_symbol} {category}{archive_mark}")
            else:
                stats['failed'] += 1
                print(f"   ❌ Ошибка")
        
        print("\n" + "="*60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("="*60)
        print(f"Всего обработано: {stats['total']}")
        print(f"🔥 Приоритетных: {stats['primary']}")
        print(f"🟡 Средний потенциал: {stats['medium']}")
        print(f"🔴 Исключено: {stats['excluded']}")
        print(f"📦 Определено по архиву: {stats['by_archive']}")
        print(f"❌ Ошибок: {stats['failed']}")
        
        print("\n📊 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
        for category, count in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
            priority = get_priority(category)
            symbol = "🔥" if priority == "primary" else "🟡" if priority == "medium" else "🔴"
            print(f"   {symbol} {category}: {count}")
        print("="*60)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
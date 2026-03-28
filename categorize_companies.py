#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для категоризации компаний из базы данных через DeepSeek API.
Версия 5.0 - двухэтапная категоризация с приоритетом групп.

Логика работы:
1. Сначала категоризация по company_info из hh.ru
2. Если категория из ГРУППЫ_1 (высокий приоритет) - сразу сохраняем
3. Если категория из ГРУППЫ_2 или "Другое" - анализируем архив сайта
4. Если архив дает категорию из ГРУППЫ_1 - берем её
5. Если архив дает категорию из ГРУППЫ_2 - берем её (уточнили)
6. Если и архив не дал категории - оставляем "Другое"
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
from collections import defaultdict

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
# 🎯 КАТЕГОРИИ (разделены на группы приоритета)
# ============================================================

# ГРУППА 1: Высокий приоритет - целевые компании (сразу принимаем)
GROUP_1_CATEGORIES = [
    "Производители котельного оборудования",
    "Поставщики котельного оборудования",
    "Монтаж/строительство котельных",
    "Интеграторы АСУТП",
    "Разработчики промышленного ПО",
    "Проектные институты (автоматизация)",
    "Крупные промышленные корпорации"
]

# ГРУППА 2: Средний приоритет - потенциальные заказчики (требуют проверки архивом)
GROUP_2_CATEGORIES = [
    "Поставщики промышленного оборудования",
    "Машиностроительные заводы",
    "Энергетические компании",
    "Химические производства",
    "Пищевые производства",
    "Металлургические комбинаты",
    "Нефтегазовые компании (не киты)",
    "Строительно-монтажные организации"
]

# Все категории (для API и совместимости)
CATEGORIES = GROUP_1_CATEGORIES + GROUP_2_CATEGORIES + ["Другое"]

# Множества для быстрой проверки
GROUP_1_SET = set(GROUP_1_CATEGORIES)
GROUP_2_SET = set(GROUP_2_CATEGORIES)

# ============================================================
# 📝 ПРОМПТЫ ДЛЯ DEEPSEEK
# ============================================================

SYSTEM_MESSAGE = "Ты эксперт по классификации промышленных компаний. Отвечай только в формате JSON. Строго выбирай категорию из предложенного списка."

PROMPT_COMPANY_INFO = """Ты эксперт по классификации промышленных компаний для поиска заказчиков программистов АСУТП.

Информация о компании:
Название: {company_name}
Описание: {company_info}

Выбери ОДНУ категорию из списка.

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

Выбери ОДНУ категорию из списка.

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


def get_category_group(category: str) -> int:
    """
    Возвращает номер группы приоритета категории:
    1 - высший приоритет (целевые компании)
    2 - средний приоритет (потенциальные заказчики)
    0 - исключено (Другое)
    """
    if category in GROUP_1_SET:
        return 1
    elif category in GROUP_2_SET:
        return 2
    else:
        return 0


def get_priority_level(category: str) -> str:
    """Возвращает строковое обозначение приоритета для БД"""
    group = get_category_group(category)
    if group == 1:
        return "primary"
    elif group == 2:
        return "medium"
    else:
        return "excluded"


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
    
    # Новые поля для отслеживания источника категоризации
    if 'category_source' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_source TEXT")
        print("✅ Добавлено поле category_source (company_info/archive)")
    
    if 'original_category' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN original_category TEXT")
        print("✅ Добавлено поле original_category (категория из company_info)")
    
    conn.commit()
    conn.close()


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


def categorize_company(company: Dict) -> Tuple[Optional[str], str, str, Optional[str]]:
    """
    Двухэтапная категоризация с приоритетом групп:
    
    Логика:
    1. Сначала категоризация по company_info
    2. Если категория из ГРУППЫ_1 → принимаем сразу
    3. Если категория из ГРУППЫ_2 или "Другое" → идем к архиву
    4. Если архив дает категорию из ГРУППЫ_1 → берем её
    5. Если архив дает категорию из ГРУППЫ_2 → берем её
    6. Если и архив не дал категории → оставляем "Другое"
    
    Возвращает (category, notes, source, original_category)
    """
    name = company['employer_name']
    info = company['company_info']
    archive_path = company.get('full_archive_path')
    has_archive = archive_path and archive_path.exists()
    
    # ЭТАП 1: Категоризация по company_info
    print(f"\n📁 {name}")
    print(f"   🔍 Этап 1: анализ company_info...")
    
    info_category, info_notes = categorize_by_info(name, info)
    original_category = info_category if info_category else "Другое"
    
    if not info_category:
        info_category = "Другое"
    
    info_group = get_category_group(info_category)
    
    # Если категория из ГРУППЫ_1 - сразу принимаем
    if info_group == 1:
        print(f"   ✅ Категория из ГРУППЫ_1: {info_category}")
        print(f"   📌 Принята без анализа архива")
        return info_category, info_notes, "company_info", original_category
    
    # Если категория из ГРУППЫ_2 или "Другое" - нужен архив
    print(f"   ⏸️  Категория из ГРУППЫ_{info_group if info_group > 0 else '0 (Другое)'}: {info_category}")
    print(f"   🔍 Этап 2: анализ архива сайта...")
    
    # Проверяем наличие архива
    if not has_archive:
        print(f"   ⚠️  Архив не найден, оставляем категорию из company_info")
        if info_group == 2:
            print(f"   📌 Принята категория из ГРУППЫ_2: {info_category}")
            return info_category, info_notes, "company_info", original_category
        else:
            print(f"   ❌ Категория не определена (Другое)")
            return "Другое", "Не удалось определить категорию", "none", original_category
    
    # ЭТАП 2: Категоризация по архиву
    archive_category, archive_notes = categorize_by_archive(archive_path, name)
    
    if not archive_category:
        archive_category = "Другое"
    
    archive_group = get_category_group(archive_category)
    
    # Если архив дал категорию из ГРУППЫ_1 - берем её
    if archive_group == 1:
        print(f"   ✅ Архив дал категорию из ГРУППЫ_1: {archive_category}")
        print(f"   📌 Принята категория из архива (уточнение)")
        return archive_category, f"Уточнено по архиву (было: {info_category})", "archive", original_category
    
    # Если архив дал категорию из ГРУППЫ_2 - берем её
    elif archive_group == 2:
        print(f"   ✅ Архив дал категорию из ГРУППЫ_2: {archive_category}")
        print(f"   📌 Принята категория из архива")
        return archive_category, f"Определено по архиву (company_info: {info_category})", "archive", original_category
    
    # Если архив не дал категории
    else:
        # Если исходная категория была из ГРУППЫ_2 - берем её
        if info_group == 2:
            print(f"   ⚠️  Архив не дал категории, возвращаемся к company_info: {info_category}")
            return info_category, info_notes, "company_info", original_category
        else:
            print(f"   ❌ Категория не определена (Другое)")
            return "Другое", "Не удалось определить категорию", "none", original_category


def get_companies_for_training(db_path: Path, sample_size: int = 50) -> List[Dict]:
    """
    Получает случайную выборку компаний для обучения
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT employer_id, employer_name, company_info, archive_path
        FROM employers
        WHERE company_info IS NOT NULL AND company_info != ''
        ORDER BY RANDOM()
        LIMIT ?
    """, (sample_size,))
    
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


def save_category(db_path: Path, employer_id: str, category: str, 
                  notes: str = "", source: str = "", 
                  original_category: Optional[str] = None):
    """Сохраняет категорию в базу данных"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    now = datetime.now().isoformat()
    priority = get_priority_level(category)
    
    cur.execute("""
        UPDATE employers
        SET category = ?, 
            category_priority = ?, 
            category_updated = ?, 
            category_notes = ?,
            category_source = ?,
            original_category = ?
        WHERE employer_id = ?
    """, (category, priority, now, notes, source, original_category, employer_id))
    
    conn.commit()
    conn.close()


def train_on_sample(db_path: Path, sample_size: int, archive_dir: Path):
    """
    Режим обучения на случайной выборке
    """
    print(f"\n🎓 РЕЖИМ ОБУЧЕНИЯ (выборка: {sample_size} компаний)")
    print("=" * 60)
    
    companies = get_companies_for_training(db_path, sample_size)
    
    if not companies:
        print("❌ Нет компаний с информацией для обучения")
        return
    
    print(f"\n📊 Найдено компаний: {len(companies)}")
    
    results = []
    stats = {
        'total': len(companies),
        'primary': 0,
        'medium': 0,
        'excluded': 0,
        'by_archive': 0,
        'by_info': 0,
        'failed': 0,
        'by_category': {}
    }
    
    for i, company in enumerate(tqdm(companies, desc="Обучение", unit="комп"), 1):
        emp_id = company['employer_id']
        name = company['employer_name']
        
        print(f"\n[{i}/{len(companies)}] 🔍 Обработка: {name}")
        print(f"   ID: {emp_id}")
        
        category, notes, source, original_category = categorize_company(company)
        
        if category:
            priority = get_priority_level(category)
            stats[priority] += 1
            stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            
            if source == 'archive':
                stats['by_archive'] += 1
            elif source == 'company_info':
                stats['by_info'] += 1
            
            save_category(db_path, emp_id, category, notes, source, original_category)
            
            results.append({
                'id': emp_id,
                'name': name,
                'category': category,
                'priority': priority,
                'source': source,
                'original_category': original_category,
                'notes': notes
            })
            
            priority_symbol = {
                "primary": "🔥",
                "medium": "🟡",
                "excluded": "🔴"
            }.get(priority, "❓")
            
            source_mark = " 📦 (по архиву)" if source == 'archive' else " 📝 (по company_info)" if source == 'company_info' else ""
            print(f"   {priority_symbol} {category}{source_mark}")
            if original_category and original_category != category:
                print(f"   📌 Исходная категория: {original_category}")
        else:
            stats['failed'] += 1
            print(f"   ❌ Ошибка")
    
    # Сохраняем результаты
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"training_results_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'sample_size': sample_size,
            'stats': stats,
            'results': results
        }, f, ensure_ascii=False, indent=2)
    
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ОБУЧЕНИЯ")
    print("=" * 60)
    print(f"Всего обработано: {stats['total']}")
    print(f"🔥 Приоритетных (Группа 1): {stats['primary']}")
    print(f"🟡 Средний потенциал (Группа 2): {stats['medium']}")
    print(f"🔴 Исключено: {stats['excluded']}")
    print(f"📦 Определено по архиву: {stats['by_archive']}")
    print(f"📝 Определено по company_info: {stats['by_info']}")
    print(f"❌ Ошибок: {stats['failed']}")
    
    print("\n📊 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
    for category, count in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
        group = get_category_group(category)
        if group == 1:
            symbol = "🔥"
        elif group == 2:
            symbol = "🟡"
        else:
            symbol = "🔴"
        print(f"   {symbol} {category}: {count}")
    
    print(f"\n✅ Результаты сохранены в: {output_file}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Категоризация компаний через DeepSeek API (версия 5.0)')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе')
    parser.add_argument('--archive-dir', default='site_archive', help='Папка с архивами сайтов')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--train', action='store_true', help='Режим обучения (случайная выборка)')
    group.add_argument('--limit', type=int, help='Количество компаний для категоризации')
    group.add_argument('--all', action='store_true', help='Категоризировать ВСЕ компании')
    group.add_argument('--id', dest='company_id', help='Категоризировать конкретную компанию')
    group.add_argument('--priority', action='store_true', 
                      help='Только приоритетные категории (исключить заведомо ненужных)')
    
    parser.add_argument('--sample', type=int, default=50, 
                       help='Размер выборки для обучения (по умолчанию 50)')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База не найдена: {db_path}")
        return
    
    archive_dir = Path(args.archive_dir)
    
    init_database(db_path)
    
    # Режим обучения
    if args.train:
        train_on_sample(db_path, args.sample, archive_dir)
        return
    
    # Режим категоризации
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
            'by_info': 0,
            'failed': 0,
            'by_category': {}
        }
        
        for company in tqdm(companies, desc="Категоризация", unit="комп"):
            emp_id = company['employer_id']
            name = company['employer_name']
            
            category, notes, source, original_category = categorize_company(company)
            
            if category:
                priority = get_priority_level(category)
                stats[priority] += 1
                stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
                
                if source == 'archive':
                    stats['by_archive'] += 1
                elif source == 'company_info':
                    stats['by_info'] += 1
                
                save_category(db_path, emp_id, category, notes, source, original_category)
                
                priority_symbol = {
                    "primary": "🔥",
                    "medium": "🟡",
                    "excluded": "🔴"
                }.get(priority, "❓")
                
                source_mark = " 📦 (по архиву)" if source == 'archive' else " 📝 (по company_info)" if source == 'company_info' else ""
                print(f"\n📁 {name}")
                print(f"   {priority_symbol} {category}{source_mark}")
                if original_category and original_category != category:
                    print(f"   📌 Исходная категория: {original_category}")
            else:
                stats['failed'] += 1
                print(f"\n📁 {name}")
                print(f"   ❌ Ошибка")
        
        print("\n" + "="*60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("="*60)
        print(f"Всего обработано: {stats['total']}")
        print(f"🔥 Группа 1 (высокий приоритет): {stats['primary']}")
        print(f"🟡 Группа 2 (средний приоритет): {stats['medium']}")
        print(f"🔴 Исключено: {stats['excluded']}")
        print(f"📦 Определено по архиву: {stats['by_archive']}")
        print(f"📝 Определено по company_info: {stats['by_info']}")
        print(f"❌ Ошибок: {stats['failed']}")
        
        print("\n📊 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
        for category, count in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
            group = get_category_group(category)
            if group == 1:
                symbol = "🔥"
            elif group == 2:
                symbol = "🟡"
            else:
                symbol = "🔴"
            print(f"   {symbol} {category}: {count}")
        print("="*60)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
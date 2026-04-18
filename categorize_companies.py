#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для категоризации компаний из базы данных через DeepSeek API.
Версия 6.0 - единый запрос с company_info и архивом сайта, детальный промпт.
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

load_dotenv()

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
if not DEEPSEEK_API_KEY:
    print("❌ ОШИБКА: DEEPSEEK_API_KEY не найден в .env")
    sys.exit(1)

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
MODEL = "deepseek-chat"

# ============================================================
# 🎯 ЕДИНЫЙ СПИСОК КАТЕГОРИЙ (16 штук)
# ============================================================
CATEGORIES = [
    "Производители котельного оборудования",
    "Поставщики котельного оборудования",
    "Монтаж/строительство котельных",
    "Интеграторы АСУТП",
    "Разработчики промышленного ПО",
    "Проектные институты (автоматизация)",
    "Крупные промышленные корпорации",
    "Поставщики промышленного оборудования",
    "Машиностроительные заводы",
    "Энергетические компании",
    "Химические производства",
    "Пищевые производства",
    "Металлургические комбинаты",
    "Нефтегазовые компании (не киты)",
    "Строительно-монтажные организации",
    "Другое"
]

# ============================================================
# 📝 ДЕТАЛЬНЫЙ ПРОМПТ (один для всех случаев)
# ============================================================

SYSTEM_MESSAGE = "Ты эксперт по классификации промышленных компаний. Отвечай только в формате JSON."

DETAILED_PROMPT = """Ты эксперт по классификации промышленных компаний для поиска заказчиков программистов АСУТП.

Ниже приведена информация о компании:
- Название: {company_name}
- Описание из базы вакансий (hh.ru):
{company_info}

{archive_section}

Твоя задача — выбрать ОДНУ категорию из строго фиксированного списка.

Разрешённые категории (только из этого списка):
{categories}

---

## ПРИНЦИПЫ КАТЕГОРИЗАЦИИ

1. **Сначала определи, ложится ли компания в эту промышленную таксономию.**
   - Если нет — ставь "Другое".
   - Не нужно насильно притягивать компанию в промышленную категорию.

2. **Если ложится, определи роль компании:**
   - производитель оборудования
   - поставщик / дистрибьютор
   - монтаж / строительство / подряд
   - интегратор / инжиниринг
   - разработчик ПО
   - отраслевой производственный заказчик / промышленное предприятие
   - крупная промышленная группа / холдинг

3. **После этого выбери конкретную категорию из списка.**

---

## ОСОБЫЙ ПРИОРИТЕТ (проверяй внимательно)
Эти 4 категории важны, но записывай компанию туда только при наличии явных признаков:
- Производители котельного оборудования
- Поставщики котельного оборудования
- Монтаж/строительство котельных
- Интеграторы АСУТП

---

## СИЛЬНЫЕ ПРИЗНАКИ ДЛЯ КАТЕГОРИЙ

### Производители котельного оборудования
- собственное производство котлов, горелок, водогрейных/паровых котлов
- блочно-модульные котельные, котельные установки
- теплоэнергетическое оборудование, горелочные устройства

### Поставщики котельного оборудования
- поставка котлов, горелок, котельного оборудования
- продажа промышленных котлов, теплообменников
- дилер котельного оборудования

### Монтаж/строительство котельных
- строительство, монтаж, реконструкция котельных
- пусконаладка котельных, тепловые пункты (ИТП/ЦТП)

### Интеграторы АСУТП
- АСУ ТП, SCADA, ПЛК, КИПиА, шкафы автоматики
- диспетчеризация технологических процессов
- промышленная автоматизация как основной профиль
- автоматизация котельных, насосных, производств

### Разработчики промышленного ПО
- собственная промышленная платформа, SCADA/MES
- лицензируемое промышленное ПО (не только внедрение)

### Проектные институты (автоматизация)
- проектирование систем автоматизации, АСУ ТП, КИПиА

### Поставщики промышленного оборудования
- поставка насосов, КИП, резервуаров, компрессоров и т.п.
- не попадает в более узкую котельную категорию

### Машиностроительные заводы
- производство машин, станков, тяжёлого оборудования
- заводской профиль

### Отраслевые категории (энергетика, химия, пищевая, металлургия, нефтегаз)
- компания сама является производственным предприятием отрасли
- не путать с поставщиком оборудования для отрасли

### Крупные промышленные корпорации
- крупный холдинг (Газпром, Росатом, РУСАЛ, Норникель, Северсталь...)
- более узкая категория не описывает сущность лучше

### Строительно-монтажные организации
- общестроительные, подрядные, EPC-работы
- если нет явного котельного профиля

---

## ЗАПРЕТЫ (типовые ошибки)

1. Производитель/поставщик оборудования для отрасли ≠ сама отрасль.
   Пример: поставщик для нефтегаза ≠ нефтегазовая компания.
2. Производитель упаковки ≠ пищевое производство.
3. Производитель холодильников/насосов/телематики/медтехники ≠ производитель котельного оборудования без прямых признаков.
4. Сервисный центр по котлам ≠ строительство котельных.
5. Наличие слов "энерго", "тепло", "пром", "автоматизация" в названии само по себе ничего не доказывает.
6. ИТ-интегратор (ERP, CRM, документооборот) ≠ интегратор АСУТП без промышленной автоматизации.
7. Любая автоматизация ≠ АСУТП.
8. Если компания не ложится в список — ставь "Другое", не насилуй.

---

## ПРАВИЛА ВЫБОРА МЕЖДУ ПОХОЖИМИ КАТЕГОРИЯМИ

- Внедряет, проектирует, собирает АСУТП → Интеграторы АСУТП.
- Продаёт или производит оборудование → Поставщики промышленного оборудования (или узкая котельная).
- Собственный промышленный ПО → Разработчики промышленного ПО.
- Строит, монтирует, подряд → Строительно-монтажные организации (если не котельная).
- Если одновременно делает многое — выбери наиболее узкий профиль по доминанте.
- Если уверенность невысокая — всё равно выбери наиболее вероятную категорию, не занижай до "Другое" без причины.

---

Верни ТОЛЬКО JSON в формате:
{{"category": "название категории"}}
"""

def build_prompt(company_name: str, company_info: str, archive_text: Optional[str] = None) -> str:
    """Формирует промпт с учётом наличия архива."""
    categories_str = "\n".join(f"  • {cat}" for cat in CATEGORIES)
    
    if archive_text:
        archive_section = f"Текст с сайта компании (из архива, первые 20000 символов):\n{archive_text}"
    else:
        archive_section = "Архив сайта отсутствует."
    
    return DETAILED_PROMPT.format(
        company_name=company_name,
        company_info=company_info,
        archive_section=archive_section,
        categories=categories_str
    )

# ============================================================
# 🔧 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def init_database(db_path: Path):
    """Добавляет поля для категорий, если их нет (сохраняем старые поля, но не используем активно)."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    
    if 'category' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category TEXT")
    if 'category_updated' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_updated TIMESTAMP")
    if 'category_notes' not in columns:
        cur.execute("ALTER TABLE employers ADD COLUMN category_notes TEXT")
    # Оставляем старые поля для совместимости, но не используем
    for col in ['category_priority', 'category_source', 'original_category']:
        if col not in columns:
            cur.execute(f"ALTER TABLE employers ADD COLUMN {col} TEXT")
    
    conn.commit()
    conn.close()

def extract_text_from_html(html_content: str) -> str:
    """Извлекает видимый текст из HTML."""
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

def extract_text_from_archive(archive_path: Path, max_chars: int = 20000) -> str:
    """
    Распаковывает архив и извлекает текст из HTML файлов.
    Равномерно распределяет max_chars между страницами, приоритет: index, contact, about, остальные.
    """
    if not archive_path.exists():
        return ""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        try:
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(path=temp_path)
        except Exception:
            return ""
        
        html_files = list(temp_path.rglob('*.html')) + list(temp_path.rglob('*.htm'))
        if not html_files:
            return ""
        
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
        
        page_texts = []
        for html_file in html_files:
            try:
                content = html_file.read_text(encoding='utf-8', errors='ignore')
                text = extract_text_from_html(content)
                text = ' '.join(text.split())
                if text:
                    page_texts.append(text)
            except:
                continue
        
        if not page_texts:
            return ""
        
        remaining = max_chars
        n_pages = len(page_texts)
        result_parts = []
        for i, text in enumerate(page_texts):
            if remaining <= 0:
                break
            budget = remaining // (n_pages - i) if n_pages - i > 0 else remaining
            chunk = text[:budget]
            result_parts.append(chunk)
            remaining -= len(chunk)
        
        return "\n\n".join(result_parts)

def call_deepseek(prompt: str) -> Tuple[Optional[str], str]:
    """Отправляет промпт в DeepSeek, возвращает (категория, notes)."""
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
        "max_tokens": 200  # достаточно для названия категории
    }
    try:
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()
        content = result['choices'][0]['message']['content'].strip()
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            category = data.get('category', '').strip()
            if category in CATEGORIES:
                return category, ""
            else:
                return "Другое", f"Неизвестная категория: {category}"
        else:
            return "Другое", f"Не удалось распарсить JSON: {content[:100]}"
    except Exception as e:
        return None, f"Ошибка API: {e}"

def categorize_company(company: Dict) -> Tuple[Optional[str], str]:
    """
    Единая категоризация: объединяет company_info (до 8000 символов)
    и текст из архива (до 20000 символов) в один промпт.
    """
    name = company['employer_name']
    info = company.get('company_info', '')
    if info and len(info) > 8000:
        info = info[:8000]
    
    archive_text = ""
    archive_path = company.get('full_archive_path')
    if archive_path and archive_path.exists():
        archive_text = extract_text_from_archive(archive_path, max_chars=20000)
    
    prompt = build_prompt(name, info, archive_text if archive_text else None)
    return call_deepseek(prompt)

def get_companies_to_categorize(db_path: Path, limit: Optional[int] = None,
                                all_companies: bool = False,
                                company_id: Optional[str] = None) -> List[Dict]:
    """Получает компании для категоризации (только без категории)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    if company_id:
        cur.execute("""
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers WHERE employer_id = ?
        """, (company_id,))
    elif all_companies:
        cur.execute("""
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
            ORDER BY employer_name
        """)
    else:
        cur.execute("""
            SELECT employer_id, employer_name, company_info, archive_path
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
              AND (category IS NULL OR category = '')
            ORDER BY employer_name
            LIMIT ?
        """, (limit or 10,))
    
    rows = cur.fetchall()
    conn.close()
    
    companies = []
    for row in rows:
        emp = dict(row)
        if emp.get('archive_path'):
            archive_path = Path(emp['archive_path'])
            if not archive_path.is_absolute():
                archive_path = Path('site_archive') / archive_path.name
            if archive_path.exists():
                emp['full_archive_path'] = archive_path
        companies.append(emp)
    return companies

def save_category(db_path: Path, employer_id: str, category: str, notes: str = ""):
    """Сохраняет категорию в базу."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = datetime.now().isoformat()
    cur.execute("""
        UPDATE employers
        SET category = ?, category_updated = ?, category_notes = ?
        WHERE employer_id = ?
    """, (category, now, notes, employer_id))
    conn.commit()
    conn.close()

# ============================================================
# 📊 ОСНОВНОЙ ЦИКЛ И СТАТИСТИКА
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Категоризация компаний через DeepSeek (единый промпт, версия 6.0)')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе')
    parser.add_argument('--archive-dir', default='site_archive', help='Папка с архивами сайтов')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--limit', type=int, help='Количество компаний для категоризации (без категории)')
    group.add_argument('--all', action='store_true', help='Категоризировать ВСЕ компании (без категории)')
    group.add_argument('--id', dest='company_id', help='Категоризировать конкретную компанию')
    
    parser.add_argument('--sample', type=int, default=50, help='Размер выборки для обучения (не используется в этой версии, оставлен для совместимости)')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База не найдена: {db_path}")
        return
    
    init_database(db_path)
    
    # Определяем режим
    if args.limit:
        mode = "limit"
        limit = args.limit
    elif args.all:
        mode = "all"
        limit = None
    elif args.company_id:
        mode = "id"
        limit = None
    else:
        # по умолчанию 10
        mode = "limit"
        limit = 10
    
    companies = get_companies_to_categorize(
        db_path,
        limit=limit if mode != "all" else None,
        all_companies=(mode == "all"),
        company_id=args.company_id
    )
    
    if not companies:
        print("✅ Нет компаний для категоризации")
        return
    
    print(f"\n{'='*60}")
    if mode == "id":
        print(f"🔍 Обработка компании с ID: {args.company_id}")
    elif mode == "all":
        print(f"📊 Обработка ВСЕХ компаний: {len(companies)} шт")
    else:
        print(f"📊 Обработка {limit} компаний")
    print('='*60)
    
    stats = {
        'total': len(companies),
        'categorized': 0,
        'failed': 0,
        'by_category': {}
    }
    
    for company in tqdm(companies, desc="Категоризация", unit="комп"):
        emp_id = company['employer_id']
        name = company['employer_name']
        
        category, notes = categorize_company(company)
        
        if category:
            save_category(db_path, emp_id, category, notes)
            stats['categorized'] += 1
            stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            print(f"\n📁 {name}")
            print(f"   ✅ {category}")
            if notes:
                print(f"   📝 {notes[:100]}")
        else:
            stats['failed'] += 1
            print(f"\n📁 {name}")
            print(f"   ❌ Ошибка: {notes}")
    
    print("\n" + "="*60)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("="*60)
    print(f"Всего обработано: {stats['total']}")
    print(f"✅ Успешно: {stats['categorized']}")
    print(f"❌ Ошибок: {stats['failed']}")
    print("\n📊 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
    for cat, cnt in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
        print(f"   • {cat}: {cnt}")
    print("="*60)

if __name__ == "__main__":
    main()
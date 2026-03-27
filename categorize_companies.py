#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для категоризации компаний из базы данных через DeepSeek API.
Версия 2.0 - оптимизирован на основе обучения на 800 компаниях.

Категории оптимизированы для поиска компаний, которым нужен программист АСУТП на аутсорс.

Запуск:
    # Обучение на выборке компаний
    python categorize_companies.py --db employers.db --train --sample 50
    
    # Категоризация N компаний
    python categorize_companies.py --db employers.db --limit 10
    
    # Категоризация всех компаний
    python categorize_companies.py --db employers.db --all
    
    # Только приоритетные категории
    python categorize_companies.py --db employers.db --priority --limit 20
"""

import sqlite3
import argparse
import json
import time
import re
import random
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import requests
from tqdm import tqdm
import os
import sys
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# 🔑 API КЛЮЧ DEEPSEEK из .env
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
if not DEEPSEEK_API_KEY:
    print("⚠️  Предупреждение: DEEPSEEK_API_KEY не найден в .env")
    sys.exit(1)

# Конфигурация API
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
MODEL = "deepseek-chat"

# ============================================================
# 🎯 ОБНОВЛЁННЫЕ КАТЕГОРИИ (на основе обучения 800 компаний)
# ============================================================

# 🔥 ОСНОВНЫЕ ЦЕЛИ - ваш приоритет
PRIMARY_CATEGORIES = [
    "Интеграторы АСУТП",                     # Разрабатывают и внедряют системы автоматизации
    "Производители котельного оборудования", #
    "Поставщики котельного оборудования",    #
    "Монтаж/строительство котельных",        #
    "Разработчики промышленного ПО",         # Пишут SCADA, MES, визуализацию
    "Проектные институты (автоматизация)",   # Проектируют АСУТП, нужна реализация
]

# 🟡 СРЕДНИЙ ПОТЕНЦИАЛ - могут дать подряд
MEDIUM_CATEGORIES = [
    "Поставщики промышленного оборудования",  # Продают КИПиА, контроллеры, насосы
    "Машиностроительные заводы",             # Производят оборудование, есть автоматизация
    "Энергетические компании",                # ТЭЦ, ГРЭС, электросети
    "Химические производства",                # Нефтехимия, удобрения, кислоты
    "Пищевые производства",                    # Пищевая промышленность
    "Металлургические комбинаты",             # Чёрная и цветная металлургия
    "Нефтегазовые компании (не киты)",        # Добыча и переработка
    "Строительно-монтажные организации",       # Монтируют, налаживают оборудование
]

# 🔴 НИЗКИЙ ПОТЕНЦИАЛ - редко, но можно проверить
LOW_CATEGORIES = [
    "Логистические компании",                 # Склады, транспорт
    "Агропромышленные комплексы",              # Теплицы, фермы, элеваторы
    "Сервисные компании",                      # Обслуживают оборудование
    "Поставщики расходных материалов",         # Не интересно
    "Торговые компании",                       # Только перепродажа
]

# 🚫 ИСКЛЮЧИТЬ - не тратить время
EXCLUDED_CATEGORIES = [
    "Крупные корпорации (Газпром, Росатом)",   # Свои программисты
    "Кадровые агентства",                       # Продают людей, а не проекты
    "Образовательные учреждения",               # Институты, школы, курсы
    "IT-аутсорсинг (веб/мобилки)",              # Не наша сфера
    "Консалтинг/аудит",                         # Только советы
    "Госучреждения",                             # Бюрократия
    "Медицинские учреждения",                    # Клиники, больницы
    "Другое",                                     # Неопределено
]

# Полный список категорий
CATEGORIES = PRIMARY_CATEGORIES + MEDIUM_CATEGORIES + LOW_CATEGORIES + EXCLUDED_CATEGORIES

# ============================================================
# 📚 ОБНОВЛЁННАЯ БАЗА ЗНАНИЙ (на основе обучения)
# ============================================================

KNOWN_COMPANIES = {
    # Интеграторы АСУТП (из обучения)
    "инсистемс": "Интеграторы АСУТП",
    "прософт": "Интеграторы АСУТП",
    "овен": "Поставщики промышленного оборудования",
    "круг": "Интеграторы АСУТП",
    "трейд": "Поставщики промышленного оборудования",
    "элемер": "Поставщики промышленного оборудования",
    "промышленная автоматизация": "Интеграторы АСУТП",
    "системы автоматизации": "Интеграторы АСУТП",
    "проминжиниринг": "Интеграторы АСУТП",
    "электротех": "Поставщики промышленного оборудования",
    
    # Киты
    "газпром": "Крупные корпорации (Газпром, Росатом)",
    "роснефть": "Крупные корпорации (Газпром, Росатом)",
    "лукойл": "Крупные корпорации (Газпром, Росатом)",
    "северсталь": "Крупные корпорации (Газпром, Росатом)",
    "нлмк": "Крупные корпорации (Газпром, Росатом)",
    "ммк": "Крупные корпорации (Газпром, Росатом)",
    "росатом": "Крупные корпорации (Газпром, Росатом)",
    "транснефть": "Крупные корпорации (Газпром, Росатом)",
    "рос атом": "Крупные корпорации (Газпром, Росатом)",
    "сургутнефтегаз": "Крупные корпорации (Газпром, Росатом)",
    "татнефть": "Крупные корпорации (Газпром, Росатом)",
    
    # Проектные институты
    "гипрокаучук": "Проектные институты (автоматизация)",
    "нефтехимпроект": "Проектные институты (автоматизация)",
    "технопроект": "Проектные институты (автоматизация)",
    "промпроект": "Проектные институты (автоматизация)",
    
    # Машиностроение
    "уралмаш": "Машиностроительные заводы",
    "ижорские заводы": "Машиностроительные заводы",
    "тяжмаш": "Машиностроительные заводы",
    "энергомаш": "Машиностроительные заводы",
    
    # Кадровые
    "headhunter": "Кадровые агентства",
    "hh.ru": "Кадровые агентства",
    "superjob": "Кадровые агентства",
    "работа.ру": "Кадровые агентства",
    "кадровое агентство": "Кадровые агентства",
    "рекрутинг": "Кадровые агентства",
    "персонал": "Кадровые агентства",
}

# Ключевые слова для быстрой классификации (без API)
KEYWORDS = {
    "Интеграторы АСУТП": [
        "асутп", "асу тп", "суутп", "сгдо", "scada", "плк", "plc",
        "программирование плк", "контроллер", "автоматизация технологических",
        "промышленная автоматизация", "система управления", "диспетчеризация",
        "автоматизация производства", "асу", "микроконтроллер", "промышленные контроллеры"
    ],
    
    "Разработчики промышленного ПО": [
        "разработка по", "программное обеспечение", "промышленное по",
        "scada система", "разработка scada", "m es", "mes система",
        "визуализация технологических", "опа", "hmi", "human machine interface"
    ],
    
    "Проектные институты (автоматизация)": [
        "проектирование", "проектный институт", "проектная документация",
        "рабочая документация", "техническое перевооружение", "ппр",
        "проектно-сметная", "инженерные изыскания", "промышленное проектирование"
    ],
    
    "Поставщики промышленного оборудования": [
        "поставка оборудования", "кипиа", "датчики", "контрольно-измерительные",
        "промышленное оборудование", "дистрибьютор", "официальный дилер",
        "запорная арматура", "насосы", "компрессоры", "электродвигатели",
        "частотные преобразователи", "шкафы управления"
    ],
    
    "Машиностроительные заводы": [
        "машиностроительный завод", "производство оборудования", "станкостроение",
        "тяжелое машиностроение", "завод изготовитель", "серийное производство",
        "производственная площадка", "цех", "металлообработка"
    ],
    
    "Химические производства": [
        "химическая промышленность", "нефтехимия", "производство удобрений",
        "органическая химия", "неорганическая химия", "химический комбинат",
        "синтез", "полимеры", "лакокрасочный", "фармацевтический"
    ],
    
    "Пищевые производства": [
        "пищевая промышленность", "молочный комбинат", "мясокомбинат",
        "хлебозавод", "кондитерская фабрика", "пивоваренный завод",
        "переработка сельхозпродукции", "продукты питания"
    ],
    
    "Энергетические компании": [
        "энергетика", "тэц", "грэс", "электростанция", "электросетевая",
        "генерация электроэнергии", "теплоснабжение", "энергосбыт"
    ],
    
    "Нефтегазовые компании (не киты)": [
        "нефтегаз", "добыча нефти", "газодобыча", "нефтепереработка",
        "нефтесервис", "буровая", "нефтепромысел"
    ],
    
    "Крупные корпорации (Газпром, Росатом)": [
        "газпром", "роснефть", "лукойл", "северсталь", "нлмк", "ммк",
        "росатом", "транснефть", "сургутнефтегаз", "татнефть"
    ],
    
    "Кадровые агентства": [
        "кадровое агентство", "рекрутинг", "подбор персонала", "headhunter",
        "hh.ru", "superjob", "работа.ру", "поиск сотрудников", "hr-агентство",
        "кадровый центр"
    ],

    "Производители котельного оборудования": [
        "котел", "котлы", "котельное оборудование", "котлостроение",
        "котлостроительный завод", "производство котлов", "водогрейный котел",
        "паровой котел", "котлоагрегат", "теплогенератор"
    ],

    "Поставщики котельного оборудования": [
        "поставка котлов", "дилер котлов", "реализация котельного оборудования",
        "торговля котлами", "котельное оборудование оптом"
    ],

    "Монтаж/строительство котельных": [
        "монтаж котельных", "строительство котельных", "пусконаладка котельных",
        "ремонт котельных", "обслуживание котельных", "котельные под ключ"
    ],
}


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
        print("✅ Добавлено поле category_priority (primary/medium/low/excluded)")
    
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
    if category in PRIMARY_CATEGORIES:
        return "primary"
    elif category in MEDIUM_CATEGORIES:
        return "medium"
    elif category in LOW_CATEGORIES:
        return "low"
    else:
        return "excluded"


def quick_classify(company_name: str, company_info: str) -> Optional[str]:
    """
    Быстрая классификация по ключевым словам (без API)
    """
    text = (company_name + " " + company_info).lower()
    
    # Проверяем по базе знаний
    name_lower = company_name.lower()
    for key, category in KNOWN_COMPANIES.items():
        if key in name_lower:
            return category
    
    # Проверяем по ключевым словам
    for category, keywords in KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return category
    
    return None


def get_companies_for_training(db_path: Path, sample_size: int = 50) -> List[Dict]:
    """
    Получает случайную выборку компаний для обучения
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT employer_id, employer_name, company_info
        FROM employers
        WHERE company_info IS NOT NULL AND company_info != ''
        ORDER BY RANDOM()
        LIMIT ?
    """, (sample_size,))
    
    rows = cur.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]


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
            SELECT employer_id, employer_name, company_info
            FROM employers
            WHERE employer_id = ? AND company_info IS NOT NULL AND company_info != ''
        """, (company_id,))
    elif all_companies:
        cur.execute("""
            SELECT employer_id, employer_name, company_info
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
            ORDER BY employer_name
        """)
    else:
        limit = limit or 10
        base_query = """
            SELECT employer_id, employer_name, company_info
            FROM employers
            WHERE company_info IS NOT NULL AND company_info != ''
            AND (category IS NULL OR category = '')
        """
        
        if priority_only:
            # Исключаем заведомо ненужных
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
    
    return [dict(row) for row in rows]


def save_category(db_path: Path, employer_id: str, category: str, notes: str = ""):
    """
    Сохраняет категорию в базу данных
    """
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


def categorize_company(company_info: str, company_name: str, 
                      is_training: bool = False) -> Tuple[Optional[str], str]:
    """
    Отправляет информацию о компании в DeepSeek API и получает категорию
    Версия 2.0 с улучшенным промптом
    """
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Формируем промпт с обновлёнными категориями
    prompt = f"""Ты эксперт по классификации промышленных компаний для поиска заказчиков программистов АСУТП.

Информация о компании:
Название: {company_name}
Описание: {company_info[:1500]}

Выбери ОДНУ категорию из списка:

🔥 ОСНОВНЫЕ ЗАКАЗЧИКИ (наивысший приоритет):
{', '.join(PRIMARY_CATEGORIES)}

🟡 СРЕДНИЙ ПОТЕНЦИАЛ (могут дать подряд):
{', '.join(MEDIUM_CATEGORIES)}

🟢 НИЗКИЙ ПОТЕНЦИАЛ (редко, но проверять):
{', '.join(LOW_CATEGORIES)}

🔴 ИСКЛЮЧИТЬ (не тратить время):
{', '.join(EXCLUDED_CATEGORIES)}

ВАЖНЫЕ ПРАВИЛА:
1. Интеграторы АСУТП = сами разрабатывают и внедряют системы автоматизации, SCADA, ПЛК
2. Поставщики промышленного оборудования = продают КИПиА, контроллеры, датчики (но не внедряют)
3. Проектные институты = только проектируют, ищут подрядчиков на реализацию
4. Машиностроительные заводы = производят оборудование, нужна автоматизация производства
5. Химия/нефтегаз/энергетика = конечные заказчики, но у крупных (Газпром) свои программисты

Верни ТОЛЬКО JSON в формате:
{{"category": "название категории", "reasoning": "почему так решил (1 предложение)"}}
"""
    
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "Ты эксперт по классификации промышленных компаний. Отвечай только в формате JSON."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 150
    }
    
    try:
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        content = result['choices'][0]['message']['content'].strip()
        
        # Парсим JSON
        try:
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                category = data.get('category', '').strip()
                reasoning = data.get('reasoning', '')
                
                # Проверяем, что категория из списка
                if category in CATEGORIES:
                    return category, reasoning
                else:
                    if is_training:
                        print(f"⚠️ Неожиданная категория: '{category}'")
                    return "Другое", f"API вернул '{category}': {reasoning}"
            else:
                return "Другое", f"Не удалось распарсить JSON: {content[:100]}"
        except json.JSONDecodeError:
            return "Другое", f"Ошибка парсинга JSON: {content[:100]}"
        
    except Exception as e:
        if is_training:
            print(f"❌ Ошибка API: {e}")
        return None, ""


def print_company_result(company: Dict, category: Optional[str], notes: str, 
                        from_cache: bool = False, is_training: bool = False):
    """Красиво выводит результат обработки компании"""
    priority_symbol = {
        "primary": "🔥",
        "medium": "🟡",
        "low": "🟢",
        "excluded": "🔴"
    }
    
    if category:
        priority = get_priority(category)
        symbol = priority_symbol.get(priority, "❓")
        
        if from_cache:
            print(f"   {symbol} [БАЗА] {category}")
        else:
            print(f"   {symbol} {category}")
        
        if notes and not from_cache and is_training:
            print(f"      📝 {notes}")
    else:
        print(f"   ❌ Не удалось определить")


def train_on_sample(db_path: Path, sample_size: int = 50):
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
        'low': 0,
        'excluded': 0,
        'cached': 0,
        'failed': 0,
        'by_category': {}
    }
    
    for i, company in enumerate(tqdm(companies, desc="Обучение", unit="комп"), 1):
        emp_id = company['employer_id']
        name = company['employer_name']
        info = company['company_info']
        
        print(f"\n[{i}/{len(companies)}] 📁 {name}")
        print(f"   ID: {emp_id}")
        
        # Пробуем быструю классификацию
        category = quick_classify(name, info)
        from_cache = False
        notes = ""
        
        if category:
            from_cache = True
            stats['cached'] += 1
            print(f"   📦 Найдено в базе знаний")
        else:
            category, notes = categorize_company(info, name, is_training=True)
            time.sleep(0.5)
        
        if category:
            priority = get_priority(category)
            stats[priority] += 1
            stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            
            save_category(db_path, emp_id, category, notes)
            
            results.append({
                'id': emp_id,
                'name': name,
                'category': category,
                'priority': priority,
                'notes': notes,
                'from_cache': from_cache
            })
            
            print_company_result(company, category, notes, from_cache, is_training=True)
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
    print(f"🔥 Приоритетных: {stats['primary']}")
    print(f"🟡 Средний потенциал: {stats['medium']}")
    print(f"🟢 Низкий потенциал: {stats['low']}")
    print(f"🔴 Исключено: {stats['excluded']}")
    print(f"📦 Из базы знаний: {stats['cached']}")
    print(f"❌ Ошибок: {stats['failed']}")
    
    print("\n📊 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
    for category, count in sorted(stats['by_category'].items(), key=lambda x: -x[1]):
        priority = get_priority(category)
        symbol = "🔥" if priority == "primary" else "🟡" if priority == "medium" else "🟢" if priority == "low" else "🔴"
        print(f"   {symbol} {category}: {count}")
    
    print(f"\n✅ Результаты сохранены в: {output_file}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Категоризация компаний через DeepSeek API (версия 2.0)')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--train', action='store_true', help='Режим обучения')
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
    
    init_database(db_path)
    
    if args.train:
        train_on_sample(db_path, args.sample)
        return
    
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
            'low': 0,
            'excluded': 0,
            'cached': 0,
            'failed': 0
        }
        
        for company in tqdm(companies, desc="Категоризация", unit="комп"):
            emp_id = company['employer_id']
            name = company['employer_name']
            info = company['company_info']
            
            print(f"\n📁 {name}")
            print(f"   ID: {emp_id}")
            
            category = quick_classify(name, info)
            notes = ""
            from_cache = False
            
            if category:
                from_cache = True
                stats['cached'] += 1
            else:
                category, notes = categorize_company(info, name)
                time.sleep(0.5)
            
            if category:
                priority = get_priority(category)
                stats[priority] += 1
                save_category(db_path, emp_id, category, notes)
                
                priority_symbol = {
                    "primary": "🔥",
                    "medium": "🟡",
                    "low": "🟢",
                    "excluded": "🔴"
                }.get(priority, "❓")
                
                if from_cache:
                    print(f"   {priority_symbol} [БАЗА] {category}")
                else:
                    print(f"   {priority_symbol} {category}")
            else:
                stats['failed'] += 1
                print(f"   ❌ Ошибка")
        
        print("\n" + "="*60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("="*60)
        print(f"Всего обработано: {stats['total']}")
        print(f"🔥 Приоритетных: {stats['primary']}")
        print(f"🟡 Средний потенциал: {stats['medium']}")
        print(f"🟢 Низкий потенциал: {stats['low']}")
        print(f"🔴 Исключено: {stats['excluded']}")
        print(f"📦 Из базы знаний: {stats['cached']}")
        print(f"❌ Ошибок: {stats['failed']}")
        print("="*60)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
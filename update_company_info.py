#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для обновления информации о компаниях (company_info) из hh.ru.
Обновляет только те компании, у которых нет описания.

Запуск:
    python update_company_info.py --db employers.db --contact "tg:@promsoftservice"
    python update_company_info.py --db employers.db --contact "tg:@promsoftservice" --force
    python update_company_info.py --db employers.db --contact "tg:@promsoftservice" --id 12345
"""

import sqlite3
import argparse
import time
import requests
import re
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
from tqdm import tqdm

# Настройки API hh.ru
HH_API = "https://api.hh.ru"
HEADERS = {
    "User-Agent": "PromSoftService-Company-Info-Updater/1.0 (tg:@promsoftservice)",
    "Accept": "application/json",
}


def clean_str(s) -> str:
    """Очищает строку от лишних пробелов."""
    if s is None:
        return ""
    return str(s).replace("\r", " ").replace("\n", " ").strip()


def fetch_company_info(employer_id: str) -> str:
    """
    Запрашивает информацию о компании с hh.ru
    """
    url = f"{HH_API}/employers/{employer_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return ""
        
        data = resp.json()
        info_parts = []
        
        # Название компании
        if data.get("name"):
            info_parts.append(f"Название: {data['name']}")
        
        # Описание компании
        if data.get("description"):
            # Убираем HTML теги
            description = re.sub(r'<[^>]+>', '', data['description'])
            description = clean_str(description)
            if description:
                info_parts.append(f"Описание: {description}")
        
        # Отрасли
        industries = data.get("industries", [])
        if industries:
            industry_names = [ind.get("name", "") for ind in industries if ind.get("name")]
            if industry_names:
                info_parts.append(f"Отрасли: {', '.join(industry_names)}")
        
        # Тип компании
        company_type = data.get("type")
        if company_type:
            if isinstance(company_type, dict):
                type_name = company_type.get("name", "")
            else:
                type_name = str(company_type)
            if type_name:
                info_parts.append(f"Тип: {type_name}")
        
        # Сайт
        if data.get("site_url"):
            info_parts.append(f"Сайт: {data['site_url']}")
        
        # Адрес
        address = data.get("address")
        if address:
            if isinstance(address, dict):
                if address.get("raw"):
                    info_parts.append(f"Адрес: {address['raw']}")
                elif address.get("city") or address.get("street"):
                    addr_parts = []
                    if address.get("city"):
                        addr_parts.append(address["city"])
                    if address.get("street"):
                        addr_parts.append(address["street"])
                    if address.get("building"):
                        addr_parts.append(address["building"])
                    if addr_parts:
                        info_parts.append(f"Адрес: {', '.join(addr_parts)}")
            elif isinstance(address, str):
                info_parts.append(f"Адрес: {address}")
        
        # Регион
        if data.get("area"):
            area = data.get("area", {})
            if isinstance(area, dict):
                area_name = area.get("name", "")
                if area_name:
                    info_parts.append(f"Регион: {area_name}")
        
        # Контакты
        if data.get("relations"):
            for rel in data["relations"]:
                if rel.get("type") == "email" and rel.get("url"):
                    email = rel["url"].replace("mailto:", "").strip()
                    if email:
                        info_parts.append(f"Email: {email}")
                elif rel.get("type") == "phone" and rel.get("url"):
                    phone = rel["url"].replace("tel:", "").strip()
                    if phone:
                        info_parts.append(f"Телефон: {phone}")
        
        return "\n\n".join(info_parts) if info_parts else ""
        
    except Exception as e:
        print(f"   ❌ Ошибка при запросе {employer_id}: {e}")
        return ""


def get_companies_without_info(db_path: Path, force: bool = False, specific_id: str = None) -> List[Dict]:
    """
    Получает список компаний, у которых нет информации
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    if specific_id:
        # Конкретная компания
        cur.execute("""
            SELECT employer_id, employer_name, hh_url
            FROM employers
            WHERE employer_id = ?
        """, (specific_id,))
    elif force:
        # Принудительно обновляем ВСЕ компании
        cur.execute("""
            SELECT employer_id, employer_name, hh_url
            FROM employers
            WHERE hh_url IS NOT NULL AND hh_url != ''
            ORDER BY employer_name
        """)
    else:
        # Только те, у которых нет информации
        cur.execute("""
            SELECT employer_id, employer_name, hh_url
            FROM employers
            WHERE hh_url IS NOT NULL AND hh_url != ''
            AND (company_info IS NULL OR company_info = '')
            ORDER BY employer_name
        """)
    
    rows = cur.fetchall()
    conn.close()
    
    companies = []
    for row in rows:
        emp = dict(row)
        # Извлекаем ID из hh_url
        if emp['hh_url']:
            # URL может быть в формате https://hh.ru/employer/123456
            match = re.search(r'/(\d+)$', emp['hh_url'])
            if match:
                emp['hh_id'] = match.group(1)
                companies.append(emp)
    
    return companies


def update_company_info(db_path: Path, employer_id: str, info: str):
    """Обновляет информацию о компании в БД"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = datetime.now().isoformat()
    
    cur.execute("""
        UPDATE employers
        SET company_info = ?, company_info_updated = ?
        WHERE employer_id = ?
    """, (info, now, employer_id))
    
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Обновление информации о компаниях из hh.ru')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе')
    parser.add_argument('--contact', required=True, help='Контакт (tg:@name или email) для User-Agent')
    parser.add_argument('--force', action='store_true', help='Принудительно обновить ВСЕ компании')
    parser.add_argument('--id', dest='specific_id', help='Обновить только конкретную компанию по ID')
    
    args = parser.parse_args()
    
    # Обновляем User-Agent
    HEADERS["User-Agent"] = f"Company-Info-Updater/1.0 ({args.contact})"
    HEADERS["HH-User-Agent"] = f"Company-Info-Updater/1.0 ({args.contact})"
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База не найдена: {db_path}")
        return
    
    print(f"\n🔍 Поиск компаний для обновления...")
    
    companies = get_companies_without_info(db_path, args.force, args.specific_id)
    
    if not companies:
        print("✅ Нет компаний для обновления")
        return
    
    mode = "ВСЕ компании" if args.force else "только без информации"
    print(f"\n📊 Найдено компаний: {len(companies)} (режим: {mode})")
    
    if args.specific_id:
        print(f"🔍 Обновляем компанию с ID: {args.specific_id}")
    
    print("\n" + "=" * 60)
    
    updated = 0
    failed = 0
    
    for company in tqdm(companies, desc="Обновление", unit="комп"):
        emp_id = company['employer_id']
        name = company['employer_name']
        hh_id = company.get('hh_id')
        
        if not hh_id:
            print(f"\n⚠️  Не удалось извлечь ID для {name}")
            failed += 1
            continue
        
        print(f"\n📁 {name}")
        print(f"   ID: {emp_id}")
        print(f"   HH ID: {hh_id}")
        
        # Получаем информацию
        info = fetch_company_info(hh_id)
        
        if info:
            # Показываем кратко
            preview = info[:150] + "..." if len(info) > 150 else info
            print(f"   ✅ Получена информация: {preview}")
            update_company_info(db_path, emp_id, info)
            updated += 1
        else:
            print(f"   ❌ Информация не найдена")
            failed += 1
        
        time.sleep(0.3)  # Пауза между запросами
    
    print("\n" + "=" * 60)
    print("📊 ИТОГ:")
    print(f"   • Обработано: {len(companies)}")
    print(f"   • ✅ Обновлено: {updated}")
    print(f"   • ❌ Не найдено: {failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()
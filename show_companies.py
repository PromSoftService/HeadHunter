#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой скрипт для просмотра информации о компаниях из базы данных.

Примеры запуска:
    py -3.14 show_companies.py --db employers.db                  # показать первые 10
    py -3.14 show_companies.py --db employers.db -n 5             # показать 5 компаний
    py -3.14 show_companies.py --db employers.db -a               # показать ВСЕ компании
    py -3.14 show_companies.py --db employers.db --id 12345       # показать конкретную компанию
    py -3.14 show_companies.py --db employers.db --search "газ"   # поиск по названию
"""

import sqlite3
import argparse
from pathlib import Path
from datetime import datetime

def show_companies(db_path: Path, limit: int = None, all_companies: bool = False, 
                   company_id: str = None, search: str = None):
    """
    Выводит информацию о компаниях из базы
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем список колонок
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    print(f"\n📋 Колонки в таблице: {', '.join(columns)}")
    
    # Получаем общее количество компаний
    cur.execute("SELECT COUNT(*) FROM employers")
    total = cur.fetchone()[0]
    print(f"📊 Всего компаний в базе: {total}")
    
    # Формируем запрос
    if company_id:
        # Поиск по конкретному ID
        cur.execute("SELECT * FROM employers WHERE employer_id = ?", (company_id,))
        rows = cur.fetchall()
        print(f"\n🔍 Поиск компании с ID: {company_id}")
    elif search:
        # Поиск по названию
        cur.execute("""
            SELECT * FROM employers 
            WHERE employer_name LIKE ? 
            ORDER BY employer_name
        """, (f'%{search}%',))
        rows = cur.fetchall()
        print(f"\n🔍 Поиск компаний по названию: '{search}'")
    elif all_companies:
        # Все компании
        cur.execute("SELECT * FROM employers ORDER BY employer_name")
        rows = cur.fetchall()
        print(f"\n📊 Все компании ({len(rows)} шт):")
    else:
        # Компании с лимитом
        limit = limit or 10
        cur.execute("SELECT * FROM employers ORDER BY employer_name LIMIT ?", (limit,))
        rows = cur.fetchall()
        print(f"\n📊 Первые {limit} компаний (из {total}):")
    
    print("=" * 80)
    
    if not rows:
        print("❌ Компании не найдены")
        conn.close()
        return
    
    # Счетчики для статистики
    email_count = 0
    phone_count = 0
    both_count = 0
    
    for row in rows:
        emp = dict(row)
        
        print(f"\n🏢 Компания: {emp.get('employer_name', '—')}")
        print(f"   ID: {emp.get('employer_id', '—')}")
        print(f"   HH URL: {emp.get('hh_url', '—')}")
        print(f"   Сайт: {emp.get('site_url', '—')}")
        
        # Email
        email = emp.get('email')
        if email:
            print(f"   📧 Email: {email}")
            email_count += 1
        
        # Телефон
        phone = emp.get('phone')
        if phone:
            print(f"   📞 Телефон: {phone}")
            phone_count += 1
        
        if email and phone:
            both_count += 1
        
        # Информация о компании (если есть)
        if emp.get('company_info'):
            # Показываем только первые 200 символов
            info = emp['company_info'][:200] + "..." if len(emp['company_info']) > 200 else emp['company_info']
            print(f"   ℹ️  Информация: {info}")
        
        # Статус зеркалирования
        if emp.get('mirror_status'):
            status = emp['mirror_status']
            pages = emp.get('pages_count', 0)
            print(f"   📦 Зеркало: {status} (страниц: {pages}")
        
        # Дата добавления
        if emp.get('added_at'):
            print(f"   🕒 Добавлена: {emp['added_at'][:10]} {emp['added_at'][11:19]}")
        
        # Дата обновления контактов
        if emp.get('contacts_updated'):
            print(f"   🕒 Контакты обновлены: {emp['contacts_updated'][:10]} {emp['contacts_updated'][11:19]}")
        
        print("-" * 60)
    
    # Итоговая статистика
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА ПО ВЫВОДУ")
    print(f"   Всего показано: {len(rows)} компаний")
    print(f"   📧 С email: {email_count}")
    print(f"   📞 С телефоном: {phone_count}")
    print(f"   ✅ С email и телефоном: {both_count}")
    print(f"   ❌ Без контактов: {len(rows) - email_count - phone_count + both_count}")
    print("=" * 60)
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Просмотр компаний из базы данных')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе (employers.db)')
    
    # Группа аргументов для выбора компаний
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-n', '--number', type=int, help='Вывести N компаний')
    group.add_argument('-a', '--all', action='store_true', help='Вывести ВСЕ компании')
    group.add_argument('--id', dest='company_id', help='Показать компанию с конкретным ID')
    group.add_argument('--search', help='Поиск по названию компании')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    show_companies(
        db_path, 
        limit=args.number,
        all_companies=args.all,
        company_id=args.company_id,
        search=args.search
    )


if __name__ == '__main__':
    main()
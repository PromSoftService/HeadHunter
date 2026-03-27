#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Утилита для управления базой компаний через Excel и командную строку.

Возможности:
1. 📤 Экспорт данных в Excel (employer_id, employer_name, hh_url, site_url, added_at, category, email, phone, comments)
2. 📥 Импорт комментариев из Excel обратно в базу
3. 🧹 Очистка комментариев по ID в командной строке
4. 🧹 Очистка всех комментариев
5. 🗑️ Удаление компании из базы и связанного архива по ID
6. 🧹 Удаление архивов сайтов с ошибками
7. 🧹 Очистка email и телефонов по ID в командной строке
8. 🧹 Очистка всех email и телефонов
9. 🏷️ Очистка категории компании по ID в командной строке
10. 🏷️ Очистка категорий всех компаний

Установка зависимостей:
    py -3.14 -m pip install pandas openpyxl tabulate

Примеры запуска:

    # 📤 Экспорт базы в Excel
    py -3.14 employers_editor.py --db employers.db --export employers.xlsx
    py -3.14 employers_editor.py --db employers.db --export employers.xlsx --limit 100

    # 📥 Импорт комментариев из Excel обратно в БД
    py -3.14 employers_editor.py --db employers.db --import-file employers.xlsx

    # 🧹 Очистка комментариев по ID
    py -3.14 employers_editor.py --db employers.db --clear-comment 245050
    py -3.14 employers_editor.py --db employers.db --clear-comment 245050 245051 245052

    # 🧹 Очистка всех комментариев
    py -3.14 employers_editor.py --db employers.db --clear-all-comments
    py -3.14 employers_editor.py --db employers.db --clear-all-comments --dry-run

    # 🗑️ Удаление компании по ID (из базы и архива)
    py -3.14 employers_editor.py --db employers.db --delete 2009 --archive-dir site_archive

    # 🧹 Удаление архивов сайтов с ошибками
    py -3.14 employers_editor.py --db employers.db --clean-errors --archive-dir site_archive
    py -3.14 employers_editor.py --db employers.db --clean-errors --only-empty --archive-dir site_archive
    py -3.14 employers_editor.py --db employers.db --clean-errors --dry-run --archive-dir site_archive

    # 🧹 Удаление архивов по списку ID
    py -3.14 employers_editor.py --db employers.db --clean-ids 2009 2010 2015 --archive-dir site_archive
    py -3.14 employers_editor.py --db employers.db --clean-ids 2009 2010 --dry-run

    # 🧹 Очистка email и телефонов по ID
    py -3.14 employers_editor.py --db employers.db --clear-contacts-id 245050
    py -3.14 employers_editor.py --db employers.db --clear-contacts-id 245050 245051 245052

    # 🧹 Очистка всех email и телефонов
    py -3.14 employers_editor.py --db employers.db --clear-contacts
    py -3.14 employers_editor.py --db employers.db --clear-contacts --dry-run

    # 🏷️ Очистка категории компании по ID
    py -3.14 employers_editor.py --db employers.db --clear-category 245050
    py -3.14 employers_editor.py --db employers.db --clear-category 245050 245051 245052

    # 🏷️ Очистка категорий всех компаний
    py -3.14 employers_editor.py --db employers.db --clear-all-categories
    py -3.14 employers_editor.py --db employers.db --clear-all-categories --dry-run
"""

import argparse
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil
import csv
from typing import List, Tuple, Optional, Dict, Any
try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False
    print("⚠️  Для лучшего отображения статистики установите tabulate: pip install tabulate")


def ensure_columns(db_path: Path):
    """Проверяет наличие нужных колонок и добавляет при необходимости."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    
    # Комментарии
    if 'comments' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN comments TEXT")
            print("✅ Добавлена колонка 'comments'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку comments: {e}")
    
    # Категории
    if 'category' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN category TEXT")
            print("✅ Добавлена колонка 'category'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку category: {e}")
    
    if 'category_priority' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN category_priority TEXT")
            print("✅ Добавлена колонка 'category_priority'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку category_priority: {e}")
    
    if 'category_updated' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN category_updated TIMESTAMP")
            print("✅ Добавлена колонка 'category_updated'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку category_updated: {e}")
    
    if 'category_notes' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN category_notes TEXT")
            print("✅ Добавлена колонка 'category_notes'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку category_notes: {e}")
    
    # Email и телефон
    if 'email' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN email TEXT")
            print("✅ Добавлена колонка 'email'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку email: {e}")
    
    if 'phone' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN phone TEXT")
            print("✅ Добавлена колонка 'phone'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку phone: {e}")
    
    if 'contacts_updated' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN contacts_updated TIMESTAMP")
            print("✅ Добавлена колонка 'contacts_updated'")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить колонку contacts_updated: {e}")
    
    conn.commit()
    conn.close()


def clean_error_archives(db_path: Path, archive_dir: Path, only_empty: bool = False, dry_run: bool = False):
    """
    Удаляет архивы сайтов с ошибками.
    
    Args:
        db_path: путь к базе данных
        archive_dir: директория с архивами
        only_empty: если True, удалять только те, где pages_count = 0
        dry_run: если True, только показать, что будет удалено, без реального удаления
    """
    print(f"\n🧹 Очистка архивов с ошибками")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (файлы не удаляются)")
    if only_empty:
        print("🎯 Режим: только полностью не скачавшиеся сайты (0 страниц)")
    else:
        print("🎯 Режим: все сайты с ошибками")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем компании с ошибками
    if only_empty:
        cur.execute("""
            SELECT employer_id, employer_name, mirror_status, pages_count, archive_path
            FROM employers
            WHERE mirror_status LIKE 'error:%' AND pages_count = 0
            ORDER BY employer_name
        """)
    else:
        cur.execute("""
            SELECT employer_id, employer_name, mirror_status, pages_count, archive_path
            FROM employers
            WHERE mirror_status LIKE 'error:%'
            ORDER BY employer_name
        """)
    
    error_sites = cur.fetchall()
    conn.close()
    
    if not error_sites:
        print("✅ Нет сайтов с ошибками для удаления.")
        return
    
    print(f"\nНайдено сайтов с ошибками: {len(error_sites)}")
    
    # Собираем информацию об архивах
    archives_to_delete = []
    archives_not_found = []
    
    for site in error_sites:
        site_dict = dict(site)
        archive_path = site_dict.get('archive_path')
        
        if archive_path:
            full_path = archive_dir / Path(archive_path).name
            if full_path.exists():
                archives_to_delete.append({
                    'id': site_dict['employer_id'],
                    'name': site_dict['employer_name'],
                    'status': site_dict['mirror_status'],
                    'pages': site_dict['pages_count'],
                    'path': full_path
                })
            else:
                archives_not_found.append({
                    'id': site_dict['employer_id'],
                    'name': site_dict['employer_name'],
                    'expected': full_path
                })
    
    # Показываем статистику
    print(f"\n📊 Результаты поиска архивов:")
    print(f"   • Найдено архивов для удаления: {len(archives_to_delete)}")
    print(f"   • Архивы не найдены (уже удалены): {len(archives_not_found)}")
    
    if archives_to_delete:
        print(f"\n📋 Список архивов для удаления:")
        table_data = []
        for a in archives_to_delete[:20]:  # показываем первые 20
            table_data.append([a['id'], a['name'][:30], a['status'][:30], a['pages']])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["ID", "Компания", "Статус", "Стр."], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]} - {row[2]} ({row[3]} стр.)")
        
        if len(archives_to_delete) > 20:
            print(f"  ... и ещё {len(archives_to_delete) - 20}")
    
    # Запрашиваем подтверждение
    if not dry_run:
        response = input(f"\nУдалить {len(archives_to_delete)} архив(ов) с ошибками? (y/N): ")
        if response.lower() != 'y':
            print("❌ Удаление отменено.")
            return
        
        # Удаляем архивы
        deleted = 0
        errors = 0
        for a in archives_to_delete:
            try:
                a['path'].unlink()
                print(f"  ✅ Удалён: {a['path'].name}")
                deleted += 1
            except Exception as e:
                print(f"  ❌ Ошибка при удалении {a['path'].name}: {e}")
                errors += 1
        
        print(f"\n✅ Удалено архивов: {deleted}")
        if errors:
            print(f"⚠️ Ошибок при удалении: {errors}")
    else:
        print(f"\n🔍 В режиме просмотра. Для реального удаления запустите без --dry-run")


def clean_archives_by_ids(db_path: Path, archive_dir: Path, ids: List[str], dry_run: bool = False):
    """
    Удаляет архивы по списку ID, переданных в аргументах командной строки.
    
    Args:
        db_path: путь к базе данных
        archive_dir: директория с архивами
        ids: список ID для удаления
        dry_run: если True, только показать, что будет удалено
    """
    print(f"\n🧹 Удаление архивов по указанным ID")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (файлы не удаляются)")
    
    print(f"📋 Получено ID для удаления: {len(ids)}")
    for i, id_str in enumerate(ids, 1):
        print(f"   {i}. {id_str}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем информацию о компаниях
    placeholders = ','.join(['?'] * len(ids))
    cur.execute(f"""
        SELECT employer_id, employer_name, mirror_status, pages_count, archive_path
        FROM employers
        WHERE employer_id IN ({placeholders})
    """, ids)
    
    found_sites = cur.fetchall()
    conn.close()
    
    print(f"✅ Найдено в базе: {len(found_sites)} из {len(ids)}")
    
    # Находим отсутствующие ID
    found_ids = {site['employer_id'] for site in found_sites}
    not_found_ids = set(ids) - found_ids
    if not_found_ids:
        print(f"⚠️ Не найдены в базе: {len(not_found_ids)}")
        if len(not_found_ids) <= 10:
            for nf_id in not_found_ids:
                print(f"   - {nf_id}")
    
    # Собираем архивы для удаления
    archives_to_delete = []
    archives_not_found = []
    
    for site in found_sites:
        site_dict = dict(site)
        archive_path = site_dict.get('archive_path')
        
        if archive_path:
            full_path = archive_dir / Path(archive_path).name
            if full_path.exists():
                archives_to_delete.append({
                    'id': site_dict['employer_id'],
                    'name': site_dict['employer_name'],
                    'status': site_dict['mirror_status'],
                    'pages': site_dict['pages_count'],
                    'path': full_path
                })
            else:
                archives_not_found.append({
                    'id': site_dict['employer_id'],
                    'expected': full_path
                })
    
    print(f"\n📊 Результаты поиска архивов:")
    print(f"   • Найдено архивов для удаления: {len(archives_to_delete)}")
    print(f"   • Архивы не найдены: {len(archives_not_found)}")
    
    if archives_to_delete:
        print(f"\n📋 Список архивов для удаления:")
        table_data = []
        for a in archives_to_delete[:20]:
            table_data.append([a['id'], a['name'][:30], a['status'][:30], a['pages']])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["ID", "Компания", "Статус", "Стр."], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]} - {row[2]} ({row[3]} стр.)")
        
        if len(archives_to_delete) > 20:
            print(f"  ... и ещё {len(archives_to_delete) - 20}")
    
    # Удаляем
    if not dry_run and archives_to_delete:
        response = input(f"\nУдалить {len(archives_to_delete)} архив(ов)? (y/N): ")
        if response.lower() != 'y':
            print("❌ Удаление отменено.")
            return
        
        deleted = 0
        errors = 0
        for a in archives_to_delete:
            try:
                a['path'].unlink()
                print(f"  ✅ Удалён: {a['path'].name}")
                deleted += 1
            except Exception as e:
                print(f"  ❌ Ошибка при удалении {a['path'].name}: {e}")
                errors += 1
        
        print(f"\n✅ Удалено архивов: {deleted}")
        if errors:
            print(f"⚠️ Ошибок при удалении: {errors}")
    elif dry_run:
        print("\n🔍 Режим просмотра. Ничего не удалено.")


def clear_comments_by_ids(db_path: Path, ids: List[str], dry_run: bool = False):
    """
    Очищает комментарии по списку ID.
    
    Args:
        db_path: путь к базе данных
        ids: список ID для очистки комментариев
        dry_run: если True, только показать, что будет очищено
    """
    print(f"\n🧹 Очистка комментариев по ID")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (комментарии не удаляются)")
    
    print(f"📋 Получено ID для очистки: {len(ids)}")
    for i, id_str in enumerate(ids, 1):
        print(f"   {i}. {id_str}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем информацию о компаниях
    placeholders = ','.join(['?'] * len(ids))
    cur.execute(f"""
        SELECT employer_id, employer_name, comments
        FROM employers
        WHERE employer_id IN ({placeholders})
    """, ids)
    
    found_sites = cur.fetchall()
    
    print(f"✅ Найдено в базе: {len(found_sites)} из {len(ids)}")
    
    # Находим отсутствующие ID
    found_ids = {site['employer_id'] for site in found_sites}
    not_found_ids = set(ids) - found_ids
    if not_found_ids:
        print(f"⚠️ Не найдены в базе: {len(not_found_ids)}")
        if len(not_found_ids) <= 10:
            for nf_id in not_found_ids:
                print(f"   - {nf_id}")
    
    # Показываем компании с комментариями
    with_comments = []
    for site in found_sites:
        site_dict = dict(site)
        if site_dict.get('comments'):
            with_comments.append({
                'id': site_dict['employer_id'],
                'name': site_dict['employer_name'],
                'comment': site_dict['comments'][:50] + "..." if len(site_dict['comments']) > 50 else site_dict['comments']
            })
    
    if with_comments:
        print(f"\n📋 Компании с комментариями:")
        table_data = []
        for c in with_comments[:20]:
            table_data.append([c['id'], c['name'][:30], c['comment']])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["ID", "Компания", "Комментарий"], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]} - {row[2]}")
        
        if len(with_comments) > 20:
            print(f"  ... и ещё {len(with_comments) - 20}")
    else:
        print("\n✅ У выбранных компаний нет комментариев")
        conn.close()
        return
    
    # Удаляем
    if not dry_run and with_comments:
        response = input(f"\nОчистить комментарии для {len(with_comments)} компаний? (y/N): ")
        if response.lower() != 'y':
            print("❌ Очистка отменена.")
            conn.close()
            return
        
        # Обновляем базу
        cur.execute(f"""
            UPDATE employers 
            SET comments = NULL 
            WHERE employer_id IN ({placeholders})
        """, ids)
        
        conn.commit()
        print(f"\n✅ Очищено комментариев: {cur.rowcount}")
    
    conn.close()
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")


def clear_all_comments(db_path: Path, dry_run: bool = False):
    """
    Очищает все комментарии в базе.
    
    Args:
        db_path: путь к базе данных
        dry_run: если True, только показать, что будет очищено
    """
    print(f"\n🧹 Очистка всех комментариев")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (комментарии не удаляются)")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Получаем статистику
    cur.execute("SELECT COUNT(*) FROM employers WHERE comments IS NOT NULL AND comments != ''")
    count = cur.fetchone()[0]
    
    if count == 0:
        print("✅ В базе нет комментариев")
        conn.close()
        return
    
    print(f"\n📊 Найдено компаний с комментариями: {count}")
    
    # Показываем примеры
    if not dry_run:
        cur.execute("""
            SELECT employer_id, employer_name, comments 
            FROM employers 
            WHERE comments IS NOT NULL AND comments != '' 
            LIMIT 5
        """)
        examples = cur.fetchall()
        
        if examples:
            print(f"\n📋 Примеры комментариев (первые 5):")
            table_data = []
            for row in examples:
                comment = row[2][:50] + "..." if len(row[2]) > 50 else row[2]
                table_data.append([row[0], row[1][:30], comment])
            
            if TABULATE_AVAILABLE:
                print(tabulate(table_data, headers=["ID", "Компания", "Комментарий"], tablefmt="grid"))
            else:
                for row in table_data:
                    print(f"  {row[0]}: {row[1]} - {row[2]}")
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")
        conn.close()
        return
    
    # Запрашиваем подтверждение
    response = input(f"\n⚠️  Очистить ВСЕ комментарии ({count} шт)? (y/N): ")
    if response.lower() != 'y':
        print("❌ Очистка отменена")
        conn.close()
        return
    
    # Дополнительное подтверждение
    response2 = input(f"❗ ПОСЛЕДНЕЕ ПРЕДУПРЕЖДЕНИЕ! Это действие нельзя отменить. Продолжить? (введите 'ДА' для подтверждения): ")
    if response2 != 'ДА':
        print("❌ Очистка отменена")
        conn.close()
        return
    
    # Выполняем очистку
    cur.execute("UPDATE employers SET comments = NULL")
    conn.commit()
    
    print(f"\n✅ Очищено комментариев: {cur.rowcount}")
    conn.close()


def clear_contacts_by_ids(db_path: Path, ids: List[str], dry_run: bool = False):
    """
    Очищает email и телефоны по списку ID.
    
    Args:
        db_path: путь к базе данных
        ids: список ID для очистки контактов
        dry_run: если True, только показать, что будет очищено
    """
    print(f"\n🧹 Очистка контактов (email и phone) по ID")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (контакты не удаляются)")
    
    print(f"📋 Получено ID для очистки: {len(ids)}")
    for i, id_str in enumerate(ids, 1):
        print(f"   {i}. {id_str}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем информацию о компаниях
    placeholders = ','.join(['?'] * len(ids))
    cur.execute(f"""
        SELECT employer_id, employer_name, email, phone
        FROM employers
        WHERE employer_id IN ({placeholders})
    """, ids)
    
    found_sites = cur.fetchall()
    
    print(f"✅ Найдено в базе: {len(found_sites)} из {len(ids)}")
    
    # Находим отсутствующие ID
    found_ids = {site['employer_id'] for site in found_sites}
    not_found_ids = set(ids) - found_ids
    if not_found_ids:
        print(f"⚠️ Не найдены в базе: {len(not_found_ids)}")
        if len(not_found_ids) <= 10:
            for nf_id in not_found_ids:
                print(f"   - {nf_id}")
    
    # Показываем компании с контактами
    with_contacts = []
    for site in found_sites:
        site_dict = dict(site)
        if site_dict.get('email') or site_dict.get('phone'):
            with_contacts.append({
                'id': site_dict['employer_id'],
                'name': site_dict['employer_name'],
                'email': site_dict.get('email') or '—',
                'phone': site_dict.get('phone') or '—'
            })
    
    if with_contacts:
        print(f"\n📋 Компании с контактами:")
        table_data = []
        for c in with_contacts[:20]:
            table_data.append([c['id'], c['name'][:30], c['email'], c['phone']])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["ID", "Компания", "Email", "Телефон"], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]} | email: {row[2]} | тел: {row[3]}")
        
        if len(with_contacts) > 20:
            print(f"  ... и ещё {len(with_contacts) - 20}")
    else:
        print("\n✅ У выбранных компаний нет контактов")
        conn.close()
        return
    
    # Удаляем
    if not dry_run and with_contacts:
        response = input(f"\nОчистить контакты для {len(with_contacts)} компаний? (y/N): ")
        if response.lower() != 'y':
            print("❌ Очистка отменена.")
            conn.close()
            return
        
        # Обновляем базу
        cur.execute(f"""
            UPDATE employers 
            SET email = NULL, phone = NULL, contacts_updated = NULL
            WHERE employer_id IN ({placeholders})
        """, ids)
        
        conn.commit()
        print(f"\n✅ Очищено контактов: {cur.rowcount}")
    
    conn.close()
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")


def clear_contacts(db_path: Path, confirm: bool = True, dry_run: bool = False):
    """
    Удаляет все email и телефоны из базы данных.
    
    Args:
        db_path: путь к базе данных
        confirm: запрашивать подтверждение
        dry_run: если True, только показать, что будет удалено
    """
    print(f"\n🧹 Очистка всех контактных данных (email и phone)")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (данные не удаляются)")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Проверяем существование колонок
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    
    email_exists = 'email' in columns
    phone_exists = 'phone' in columns
    contacts_updated_exists = 'contacts_updated' in columns
    
    if not email_exists and not phone_exists:
        print("❌ В базе нет полей email или phone")
        conn.close()
        return
    
    # Получаем статистику перед удалением
    stats = {}
    
    if email_exists:
        cur.execute("SELECT COUNT(*) FROM employers WHERE email IS NOT NULL AND email != ''")
        stats['email_count'] = cur.fetchone()[0]
    
    if phone_exists:
        cur.execute("SELECT COUNT(*) FROM employers WHERE phone IS NOT NULL AND phone != ''")
        stats['phone_count'] = cur.fetchone()[0]
    
    # Показываем примеры записей, которые будут удалены
    if (email_exists and stats['email_count'] > 0) or (phone_exists and stats['phone_count'] > 0):
        print(f"\n📊 Статистика перед очисткой:")
        if email_exists:
            print(f"   • Email заполнены: {stats['email_count']}")
        if phone_exists:
            print(f"   • Телефоны заполнены: {stats['phone_count']}")
        
        # Показываем примеры
        query_parts = []
        if email_exists:
            query_parts.append("email IS NOT NULL AND email != ''")
        if phone_exists:
            query_parts.append("phone IS NOT NULL AND phone != ''")
        
        query = f"""
            SELECT employer_id, employer_name, email, phone
            FROM employers
            WHERE {' OR '.join(query_parts)}
            LIMIT 5
        """
        
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            print(f"\n📋 Примеры записей, которые будут очищены:")
            table_data = []
            for _, row in df.iterrows():
                table_data.append([
                    row['employer_id'],
                    row['employer_name'][:30] + "..." if len(row['employer_name']) > 30 else row['employer_name'],
                    row['email'] or '—',
                    row['phone'] or '—'
                ])
            
            if TABULATE_AVAILABLE:
                print(tabulate(table_data, headers=["ID", "Компания", "Email", "Телефон"], tablefmt="grid"))
            else:
                for row in table_data:
                    print(f"  • {row[0]}: {row[1]} | email: {row[2]} | тел: {row[3]}")
            
            if stats['email_count'] > 5 or stats['phone_count'] > 5:
                print(f"  ... и ещё {max(stats['email_count'], stats['phone_count']) - 5} записей")
    
    conn.close()
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")
        return
    
    if stats.get('email_count', 0) == 0 and stats.get('phone_count', 0) == 0:
        print("✅ Нет данных для очистки")
        return
    
    # Запрашиваем подтверждение
    if confirm:
        response = input(f"\n⚠️  Вы уверены, что хотите очистить все контактные данные? (y/N): ")
        if response.lower() != 'y':
            print("❌ Очистка отменена")
            return
        
        # Дополнительное подтверждение для безопасности
        response2 = input(f"❗ ПОСЛЕДНЕЕ ПРЕДУПРЕЖДЕНИЕ! Это действие нельзя отменить. Продолжить? (введите 'ДА' для подтверждения): ")
        if response2 != 'ДА':
            print("❌ Очистка отменена")
            return
    
    # Выполняем очистку
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    updates = []
    
    if email_exists:
        cur.execute("UPDATE employers SET email = NULL")
        updates.append(f"email: {cur.rowcount}")
    
    if phone_exists:
        cur.execute("UPDATE employers SET phone = NULL")
        updates.append(f"phone: {cur.rowcount}")
    
    if contacts_updated_exists:
        cur.execute("UPDATE employers SET contacts_updated = NULL")
        updates.append(f"contacts_updated: {cur.rowcount}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Очистка завершена:")
    for update in updates:
        print(f"   • {update}")
    
    print("\n📝 Контактные данные полностью удалены из базы.")


def clear_category_by_ids(db_path: Path, ids: List[str], dry_run: bool = False):
    """
    Очищает категории по списку ID.
    
    Args:
        db_path: путь к базе данных
        ids: список ID для очистки категорий
        dry_run: если True, только показать, что будет очищено
    """
    print(f"\n🏷️ Очистка категорий по ID")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (категории не удаляются)")
    
    print(f"📋 Получено ID для очистки: {len(ids)}")
    for i, id_str in enumerate(ids, 1):
        print(f"   {i}. {id_str}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Получаем информацию о компаниях
    placeholders = ','.join(['?'] * len(ids))
    cur.execute(f"""
        SELECT employer_id, employer_name, category
        FROM employers
        WHERE employer_id IN ({placeholders})
    """, ids)
    
    found_sites = cur.fetchall()
    
    print(f"✅ Найдено в базе: {len(found_sites)} из {len(ids)}")
    
    # Находим отсутствующие ID
    found_ids = {site['employer_id'] for site in found_sites}
    not_found_ids = set(ids) - found_ids
    if not_found_ids:
        print(f"⚠️ Не найдены в базе: {len(not_found_ids)}")
        if len(not_found_ids) <= 10:
            for nf_id in not_found_ids:
                print(f"   - {nf_id}")
    
    # Показываем компании с категориями
    with_category = []
    for site in found_sites:
        site_dict = dict(site)
        if site_dict.get('category'):
            with_category.append({
                'id': site_dict['employer_id'],
                'name': site_dict['employer_name'],
                'category': site_dict['category']
            })
    
    if with_category:
        print(f"\n📋 Компании с категориями:")
        table_data = []
        for c in with_category[:20]:
            table_data.append([c['id'], c['name'][:30], c['category']])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["ID", "Компания", "Категория"], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]} - {row[2]}")
        
        if len(with_category) > 20:
            print(f"  ... и ещё {len(with_category) - 20}")
    else:
        print("\n✅ У выбранных компаний нет категорий")
        conn.close()
        return
    
    # Удаляем
    if not dry_run and with_category:
        response = input(f"\nОчистить категории для {len(with_category)} компаний? (y/N): ")
        if response.lower() != 'y':
            print("❌ Очистка отменена.")
            conn.close()
            return
        
        # Обновляем базу
        cur.execute(f"""
            UPDATE employers 
            SET category = NULL, category_priority = NULL, category_updated = NULL, category_notes = NULL
            WHERE employer_id IN ({placeholders})
        """, ids)
        
        conn.commit()
        print(f"\n✅ Очищено категорий: {cur.rowcount}")
    
    conn.close()
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")


def clear_all_categories(db_path: Path, dry_run: bool = False):
    """
    Очищает все категории в базе.
    
    Args:
        db_path: путь к базе данных
        dry_run: если True, только показать, что будет очищено
    """
    print(f"\n🏷️ Очистка всех категорий")
    print("=" * 60)
    if dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА (категории не удаляются)")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Получаем статистику
    cur.execute("SELECT COUNT(*) FROM employers WHERE category IS NOT NULL AND category != ''")
    count = cur.fetchone()[0]
    
    if count == 0:
        print("✅ В базе нет категорий")
        conn.close()
        return
    
    print(f"\n📊 Найдено компаний с категориями: {count}")
    
    # Показываем распределение по категориям
    cur.execute("""
        SELECT category, COUNT(*) as cnt 
        FROM employers 
        WHERE category IS NOT NULL AND category != '' 
        GROUP BY category 
        ORDER BY cnt DESC
    """)
    categories = cur.fetchall()
    
    if categories:
        print(f"\n📊 Распределение по категориям:")
        table_data = []
        for cat in categories[:10]:
            table_data.append([cat[0][:30], cat[1]])
        
        if TABULATE_AVAILABLE:
            print(tabulate(table_data, headers=["Категория", "Кол-во"], tablefmt="grid"))
        else:
            for row in table_data:
                print(f"  {row[0]}: {row[1]}")
        
        if len(categories) > 10:
            print(f"  ... и ещё {len(categories) - 10} категорий")
    
    # Показываем примеры
    if not dry_run:
        cur.execute("""
            SELECT employer_id, employer_name, category 
            FROM employers 
            WHERE category IS NOT NULL AND category != '' 
            LIMIT 5
        """)
        examples = cur.fetchall()
        
        if examples:
            print(f"\n📋 Примеры компаний с категориями (первые 5):")
            table_data = []
            for row in examples:
                table_data.append([row[0], row[1][:30], row[2][:30]])
            
            if TABULATE_AVAILABLE:
                print(tabulate(table_data, headers=["ID", "Компания", "Категория"], tablefmt="grid"))
            else:
                for row in table_data:
                    print(f"  {row[0]}: {row[1]} - {row[2]}")
    
    if dry_run:
        print("\n🔍 Режим просмотра. Для реальной очистки запустите без --dry-run")
        conn.close()
        return
    
    # Запрашиваем подтверждение
    response = input(f"\n⚠️  Очистить ВСЕ категории ({count} шт)? (y/N): ")
    if response.lower() != 'y':
        print("❌ Очистка отменена")
        conn.close()
        return
    
    # Дополнительное подтверждение
    response2 = input(f"❗ ПОСЛЕДНЕЕ ПРЕДУПРЕЖДЕНИЕ! Это действие нельзя отменить. Продолжить? (введите 'ДА' для подтверждения): ")
    if response2 != 'ДА':
        print("❌ Очистка отменена")
        conn.close()
        return
    
    # Выполняем очистку
    cur.execute("UPDATE employers SET category = NULL, category_priority = NULL, category_updated = NULL, category_notes = NULL")
    conn.commit()
    
    print(f"\n✅ Очищено категорий: {cur.rowcount}")
    conn.close()


def delete_company(db_path: Path, employer_id: str, archive_dir: Path = None):
    """Удаляет компанию из базы данных и, если указан archive_dir, удаляет связанный архив."""
    print(f"\n🗑️ Удаление компании с ID: {employer_id}")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT employer_name, archive_path FROM employers WHERE employer_id = ?
    """, (employer_id,))
    result = cur.fetchone()
    
    if not result:
        print(f"❌ Компания с ID {employer_id} не найдена в базе.")
        conn.close()
        return
    
    employer_name, archive_path = result
    
    print(f"📋 Найдена компания: {employer_name}")
    if archive_path:
        print(f"📦 Архив: {archive_path}")
    else:
        print("📦 Архив: не указан")
    
    response = input(f"\nУдалить компанию {employer_name} (ID: {employer_id}) из базы? (y/N): ")
    if response.lower() != 'y':
        print("❌ Удаление отменено.")
        conn.close()
        return
    
    cur.execute("DELETE FROM employers WHERE employer_id = ?", (employer_id,))
    deleted_from_db = cur.rowcount
    
    cur.execute("DELETE FROM logs WHERE employer_id = ?", (employer_id,))
    deleted_logs = cur.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"✅ Удалено из employers: {deleted_from_db} запись")
    print(f"✅ Удалено из logs: {deleted_logs} записей")
    
    if archive_dir and archive_path:
        full_archive_path = archive_dir / Path(archive_path).name
        if full_archive_path.exists():
            response = input(f"\nУдалить файл архива {full_archive_path}? (y/N): ")
            if response.lower() == 'y':
                try:
                    full_archive_path.unlink()
                    print(f"✅ Архив удалён: {full_archive_path}")
                except Exception as e:
                    print(f"❌ Ошибка при удалении архива: {e}")
            else:
                print("⏭️ Архив сохранён.")
        else:
            print(f"⚠️ Архив не найден: {full_archive_path}")


def export_to_excel(db_path: Path, output_file: Path, limit: int = None):
    """Экспортирует данные в Excel с правильной последовательностью столбцов."""
    print(f"\n📤 Экспорт данных из {db_path} в {output_file}")
    print("=" * 60)
    
    ensure_columns(db_path)
    
    conn = sqlite3.connect(db_path)
    
    # Формируем список колонок для экспорта в нужном порядке
    select_columns = [
        'employer_id',
        'employer_name',
        'hh_url',
        'site_url',
        'added_at',
        'category',
        'email',
        'phone',
        'comments'
    ]
    
    query = f"""
        SELECT 
            {', '.join(select_columns)}
        FROM employers
        ORDER BY added_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    df = pd.read_sql_query(query, conn)
    
    print(f"  • employers: {len(df)} строк")
    print(f"  • С комментариями: {df['comments'].notna().sum()}")
    print(f"  • С категориями: {df['category'].notna().sum()}")
    print(f"  • С email: {df['email'].notna().sum()}")
    print(f"  • С телефоном: {df['phone'].notna().sum()}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='employers', index=False)
        
        # Добавляем лист с инструкцией
        instructions = pd.DataFrame({
            'Действие': [
                'Редактирование комментариев',
                'Импорт обратно в БД',
                'Удаление компании',
                'Очистка контактов',
                'Очистка категорий'
            ],
            'Команда': [
                'Заполните колонку "comments"',
                f'py -3.14 employers_editor.py --db {db_path.name} --import-file {output_file.name}',
                f'py -3.14 employers_editor.py --db {db_path.name} --delete <ID> --archive-dir site_archive',
                f'py -3.14 employers_editor.py --db {db_path.name} --clear-contacts-id <ID>',
                f'py -3.14 employers_editor.py --db {db_path.name} --clear-category <ID>'
            ]
        })
        instructions.to_excel(writer, sheet_name='инструкция', index=False)
    
    conn.close()
    print(f"\n✅ Экспорт завершён: {output_file}")


def import_from_excel(db_path: Path, excel_file: Path, auto_confirm: bool = False):
    """Импортирует комментарии из Excel файла обратно в базу данных (ТОЛЬКО ОБНОВЛЕНИЕ)."""
    print(f"\n📥 Импорт комментариев из {excel_file} в {db_path}")
    print("=" * 60)
    
    if not excel_file.exists():
        print(f"❌ Файл {excel_file} не найден")
        return
    
    ensure_columns(db_path)
    
    try:
        df = pd.read_excel(excel_file, sheet_name='employers')
    except Exception as e:
        print(f"❌ Ошибка чтения Excel файла: {e}")
        return
    
    if 'comments' not in df.columns:
        print("❌ В Excel файле нет колонки 'comments'")
        return
    
    if 'employer_id' not in df.columns:
        print("❌ В Excel файле нет колонки 'employer_id'")
        return
    
    # Предупреждение о том, что другие колонки не импортируются
    other_columns = [col for col in df.columns if col not in ['employer_id', 'comments']]
    if other_columns:
        print("ℹ️  Следующие колонки будут проигнорированы (импортируются ТОЛЬКО комментарии):")
        print(f"   {', '.join(other_columns[:5])}{'...' if len(other_columns) > 5 else ''}")
    
    comments_to_import = df['comments'].notna() & (df['comments'].astype(str).str.strip() != '')
    total_to_import = comments_to_import.sum()
    
    if total_to_import == 0:
        print("⚠️ В файле нет новых комментариев для импорта")
        return
    
    print(f"📊 Найдено комментариев для импорта: {total_to_import}")
    
    if not auto_confirm:
        response = input(f"\nОбновить комментарии для {total_to_import} компаний? (y/N): ")
        if response.lower() != 'y':
            print("❌ Импорт отменён")
            return
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    updated = 0
    not_found = []
    
    for _, row in df[comments_to_import].iterrows():
        comment = str(row['comments']).strip()
        employer_id = str(row['employer_id']).strip()
        
        # Обновляем ТОЛЬКО comments
        cur.execute("""
            UPDATE employers 
            SET comments = ? 
            WHERE employer_id = ?
        """, (comment, employer_id))
        
        if cur.rowcount > 0:
            updated += 1
        else:
            not_found.append(employer_id)
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Импорт завершён:")
    print(f"   • Обновлено комментариев: {updated}")
    if not_found:
        print(f"   • Не найдено в БД: {len(not_found)}")
        if len(not_found) <= 5:
            for emp_id in not_found:
                print(f"     - {emp_id}")


def main():
    parser = argparse.ArgumentParser(
        description='Управление базой компаний через Excel и командную строку',
        epilog='Примеры:\n\n' +
               '  # 📤 Экспорт базы в Excel\n' +
               '  py -3.14 employers_editor.py --db employers.db --export employers.xlsx\n\n' +
               '  # 📥 Импорт комментариев из Excel\n' +
               '  py -3.14 employers_editor.py --db employers.db --import-file employers.xlsx\n\n' +
               '  # 🧹 Очистка комментариев по ID\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-comment 245050 245051\n\n' +
               '  # 🧹 Очистка всех комментариев\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-all-comments\n\n' +
               '  # 🗑️ Удаление компании по ID\n' +
               '  py -3.14 employers_editor.py --db employers.db --delete 2009 --archive-dir site_archive\n\n' +
               '  # 🧹 Удаление архивов с ошибками\n' +
               '  py -3.14 employers_editor.py --db employers.db --clean-errors --archive-dir site_archive\n\n' +
               '  # 🧹 Удаление архивов по ID\n' +
               '  py -3.14 employers_editor.py --db employers.db --clean-ids 2009 2010 --archive-dir site_archive\n\n' +
               '  # 🧹 Очистка контактов по ID\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-contacts-id 245050\n\n' +
               '  # 🧹 Очистка всех контактов\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-contacts\n\n' +
               '  # 🏷️ Очистка категории по ID\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-category 245050\n\n' +
               '  # 🏷️ Очистка всех категорий\n' +
               '  py -3.14 employers_editor.py --db employers.db --clear-all-categories'
    )
    
    parser.add_argument('--db', default='employers.db', 
                       help='Путь к SQLite базе (по умолчанию employers.db)')
    
    # Режимы работы
    parser.add_argument('--export', metavar='FILE', 
                       help='Экспорт в Excel файл')
    parser.add_argument('--import-file', dest='import_file', metavar='FILE',
                       help='Импорт комментариев из Excel файла')
    
    # Очистка комментариев
    parser.add_argument('--clear-comment', nargs='+', metavar='ID',
                       help='Очистить комментарии по списку ID')
    parser.add_argument('--clear-all-comments', action='store_true',
                       help='Очистить все комментарии')
    
    # Удаление компании
    parser.add_argument('--delete', metavar='ID',
                       help='Удалить компанию по ID из базы и архива')
    
    # Удаление архивов
    parser.add_argument('--clean-errors', action='store_true',
                       help='Удалить архивы сайтов с ошибками')
    parser.add_argument('--clean-ids', nargs='+', metavar='ID',
                       help='Удалить архивы по списку ID')
    
    # Очистка контактов
    parser.add_argument('--clear-contacts-id', nargs='+', metavar='ID',
                       help='Очистить email и телефоны по списку ID')
    parser.add_argument('--clear-contacts', action='store_true',
                       help='Очистить все email и телефоны')
    
    # Очистка категорий
    parser.add_argument('--clear-category', nargs='+', metavar='ID',
                       help='Очистить категории по списку ID')
    parser.add_argument('--clear-all-categories', action='store_true',
                       help='Очистить все категории')
    
    # Дополнительные опции
    parser.add_argument('--only-empty', action='store_true',
                       help='При --clean-errors удалять только полностью не скачавшиеся сайты')
    parser.add_argument('--dry-run', action='store_true',
                       help='Показать, что будет сделано, но не выполнять')
    parser.add_argument('--limit', type=int, 
                       help='Ограничить количество строк при экспорте')
    parser.add_argument('--archive-dir', default='site_archive',
                       help='Директория с архивами сайтов')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Ошибка: файл базы {db_path} не найден.")
        return
    
    archive_dir = Path(args.archive_dir) if args.archive_dir else None
    
    # Проверяем, что указан хотя бы один режим
    modes = [
        args.export, args.import_file, args.clear_comment, args.clear_all_comments,
        args.delete, args.clean_errors, args.clean_ids, args.clear_contacts_id,
        args.clear_contacts, args.clear_category, args.clear_all_categories
    ]
    
    if not any(modes):
        parser.print_help()
        return
    
    # Экспорт
    if args.export:
        export_to_excel(db_path, Path(args.export), args.limit)
        return
    
    # Импорт
    if args.import_file:
        import_from_excel(db_path, Path(args.import_file))
        return
    
    # Очистка комментариев по ID
    if args.clear_comment:
        clear_comments_by_ids(db_path, args.clear_comment, args.dry_run)
        return
    
    # Очистка всех комментариев
    if args.clear_all_comments:
        clear_all_comments(db_path, args.dry_run)
        return
    
    # Удаление компании
    if args.delete:
        delete_company(db_path, args.delete, archive_dir)
        return
    
    # Очистка архивов с ошибками
    if args.clean_errors:
        if not archive_dir:
            print("❌ Не указана директория с архивами (--archive-dir)")
            return
        # Временно заглушка - потом добавим функцию
        print("⏳ Функция в разработке")
        return
    
    # Очистка архивов по ID
    if args.clean_ids:
        if not archive_dir:
            print("❌ Не указана директория с архивами (--archive-dir)")
            return
        clean_archives_by_ids(db_path, archive_dir, args.clean_ids, args.dry_run)
        return
    
    # Очистка контактов по ID
    if args.clear_contacts_id:
        clear_contacts_by_ids(db_path, args.clear_contacts_id, args.dry_run)
        return
    
    # Очистка всех контактов
    if args.clear_contacts:
        clear_contacts(db_path, confirm=True, dry_run=args.dry_run)
        return
    
    # Очистка категорий по ID
    if args.clear_category:
        clear_category_by_ids(db_path, args.clear_category, args.dry_run)
        return
    
    # Очистка всех категорий
    if args.clear_all_categories:
        clear_all_categories(db_path, args.dry_run)
        return


if __name__ == "__main__":
    main()
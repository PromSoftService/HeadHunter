#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сбор компаний с hh.ru по заданным запросам и сохранение в SQLite.
Добавляет информацию о компании (описание) из профиля работодателя.

Установка зависимостей:
    py -3.14 -m pip install requests tqdm

Пример запуска:
    py -3.14 hh_to_sqlite.py -q "руководитель проектов АСУ ТП, инженер АСУ ТП, программист АСУ ТП, программист ПЛК, SCADA, проектировщик АСУ ТП, инженер ПНР" --contact "tg:@promsoftservice" --db employers.db
    
    # Обновить информацию для существующих компаний
    py -3.14 hh_to_sqlite.py -q "руководитель проектов АСУ ТП" --contact "tg:@promsoftservice" --db employers.db --update-info
"""

import argparse
import datetime as dt
import json
import platform
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

import requests
from tqdm import tqdm

HH_API = "https://api.hh.ru"

# ---------------------------- Вспомогательные функции ----------------------------

def _ascii_clean(s: str) -> str:
    """Оставляет только ASCII символы для заголовка User-Agent."""
    return re.sub(r"[^\x20-\x7E]+", "", (s or "")).strip()


def build_headers(app_name: str, contact: str) -> Dict[str, str]:
    if not contact:
        raise ValueError("Укажите --contact (tg:@name или email)")
    app_name = _ascii_clean(app_name) or "HH-Collector/1.0"
    contact = _ascii_clean(contact)
    ua = f"{app_name} (Python/{platform.python_version()}; contact: {contact})"
    return {
        "User-Agent": ua,
        "HH-User-Agent": ua,
        "Accept": "application/json",
    }


def hh_get(path: str, params: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    """GET-запрос к API hh.ru с обработкой пагинации."""
    resp = requests.get(f"{HH_API}{path}", params=params, headers=headers, timeout=30)
    if resp.status_code == 400:
        raise Exception(f"400 Error:\n{resp.text}")
    resp.raise_for_status()
    return resp.json()


def http_get_json(url: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Загружает JSON по произвольному URL (для получения деталей компании)."""
    if not url:
        return None
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def clean_str(s: Any) -> str:
    """Очищает строку от лишних пробелов и символов перевода строки."""
    if s is None:
        return ""
    return str(s).replace("\r", " ").replace("\n", " ").strip()


# ---------------------------- Работа с SQLite ----------------------------

def init_db(db_path: Path):
    """Создаёт таблицы, если их нет, и добавляет новые колонки."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Создаём таблицу employers если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employers (
            employer_id TEXT PRIMARY KEY,
            employer_name TEXT NOT NULL,
            vacancy_url TEXT,
            hh_url TEXT,
            site_url TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_checked TIMESTAMP,
            mirror_status TEXT,
            pages_count INTEGER DEFAULT 0,
            archive_path TEXT
        )
    """)
    
    # Добавляем колонку company_info если её нет
    cur.execute("PRAGMA table_info(employers)")
    columns = [col[1] for col in cur.fetchall()]
    
    if 'company_info' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN company_info TEXT")
            print("✅ Добавлено поле company_info в таблицу employers")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить поле company_info: {e}")
    
    if 'company_info_updated' not in columns:
        try:
            cur.execute("ALTER TABLE employers ADD COLUMN company_info_updated TIMESTAMP")
            print("✅ Добавлено поле company_info_updated в таблицу employers")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Не удалось добавить поле company_info_updated: {e}")
    
    # Создаём таблицу logs если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employer_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level TEXT,
            message TEXT,
            error_type TEXT,
            url TEXT,
            FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
        )
    """)
    
    conn.commit()
    conn.close()


def load_existing_ids(db_path: Path) -> Set[str]:
    """Возвращает множество существующих employer_id."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT employer_id FROM employers")
    ids = {row[0] for row in cur.fetchall()}
    conn.close()
    return ids


def load_existing_company_info(db_path: Path) -> Set[str]:
    """Возвращает множество employer_id, у которых уже есть информация о компании."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT employer_id FROM employers WHERE company_info IS NOT NULL AND company_info != ''")
    ids = {row[0] for row in cur.fetchall()}
    conn.close()
    return ids


def insert_employers(db_path: Path, employers: List[Dict[str, Any]]):
    """Вставляет новых работодателей (пакетно)."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = dt.datetime.now().isoformat()
    for emp in employers:
        cur.execute("""
            INSERT OR IGNORE INTO employers
            (employer_id, employer_name, vacancy_url, hh_url, site_url, added_at, last_checked, company_info, company_info_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            emp['employer_id'],
            emp['employer_name'],
            emp.get('vacancy_url', ''),
            emp.get('hh_url', ''),
            emp.get('site_url', ''),
            now,
            None,  # last_checked ещё не было
            emp.get('company_info'),
            emp.get('company_info_updated')
        ))
    conn.commit()
    conn.close()


def update_company_info(db_path: Path, employer_id: str, company_info: str):
    """Обновляет информацию о компании для существующего работодателя."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = dt.datetime.now().isoformat()
    
    cur.execute("""
        UPDATE employers
        SET company_info = ?, company_info_updated = ?
        WHERE employer_id = ?
    """, (company_info, now, employer_id))
    
    conn.commit()
    conn.close()


def log_event(db_path: Path, employer_id: str, level: str, message: str,
              error_type: str = None, url: str = None):
    """Добавляет запись в лог-таблицу."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs (employer_id, level, message, error_type, url)
        VALUES (?, ?, ?, ?, ?)
    """, (employer_id, level, message, error_type, url))
    conn.commit()
    conn.close()


# ---------------------------- Сбор компаний ----------------------------

def collect_employers_for_query(query: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """Собирает компании по одному поисковому запросу вакансий."""
    employers_dict = {}  # ключ: employer_id

    params = {"text": query, "per_page": 50, "page": 0}
    first = hh_get("/vacancies", params, headers)
    pages = min(int(first.get("pages", 0) or 0), 50)  # ограничим 50 страницами

    # Используем tqdm для прогресса по страницам
    for page in tqdm(range(pages), desc=f"Страницы '{query[:30]}'", unit="стр", leave=False):
        if page > 0:
            params["page"] = page
            data = hh_get("/vacancies", params, headers)
        else:
            data = first

        for vacancy in data.get("items", []):
            emp = vacancy.get("employer") or {}
            emp_id = clean_str(emp.get("id"))
            if not emp_id:
                continue

            if emp_id not in employers_dict:
                employers_dict[emp_id] = {
                    "employer_id": emp_id,
                    "employer_name": clean_str(emp.get("name", "")),
                    "vacancy_url": clean_str(vacancy.get("alternate_url", "")),
                    "hh_url": clean_str(emp.get("alternate_url", "")),
                    "_employer_api_url": clean_str(emp.get("url", "")),
                }
        time.sleep(0.2)  # вежливая пауза

    return list(employers_dict.values())


def fetch_site_url(employer_api_url: str, headers: Dict[str, str]) -> str:
    """Запрашивает детальную информацию о компании и извлекает site_url."""
    data = http_get_json(employer_api_url, headers=headers)
    if not data:
        return ""
    return clean_str(data.get("site_url") or "")


def fetch_company_info(employer_api_url: str, headers: Dict[str, str]) -> str:
    """
    Запрашивает детальную информацию о компании и извлекает описание.
    Объединяет название, описание, отрасли и другие поля.
    """
    data = http_get_json(employer_api_url, headers=headers)
    if not data:
        return ""
    
    info_parts = []
    
    # Название компании
    if data.get("name"):
        info_parts.append(f"Название: {data['name']}")
    
    # Описание компании
    if data.get("description"):
        # Убираем HTML теги из описания
        description = re.sub(r'<[^>]+>', '', data['description'])
        description = clean_str(description)
        if description:
            info_parts.append(f"Описание: {description}")
    
    # Отрасли компании
    industries = data.get("industries", [])
    if industries:
        industry_names = [ind.get("name", "") for ind in industries if ind.get("name")]
        if industry_names:
            info_parts.append(f"Отрасли: {', '.join(industry_names)}")
    
    # Тип компании (может быть строкой или словарем)
    company_type = data.get("type")
    if company_type:
        if isinstance(company_type, dict):
            type_name = company_type.get("name", "")
        else:
            type_name = str(company_type)
        if type_name:
            info_parts.append(f"Тип: {type_name}")
    
    # Сайт (дублируем для полноты)
    if data.get("site_url"):
        info_parts.append(f"Сайт: {data['site_url']}")
    
    # Адрес (может быть строкой или словарем)
    address = data.get("address")
    if address:
        if isinstance(address, dict):
            # Пробуем получить сырой адрес
            if address.get("raw"):
                info_parts.append(f"Адрес: {address['raw']}")
            # Или собираем из компонентов
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
        elif isinstance(area, str):
            info_parts.append(f"Регион: {area}")
    
    # Контактная информация (если есть)
    if data.get("relations"):
        relations = []
        for rel in data["relations"]:
            if rel.get("type") == "email" and rel.get("url"):
                # Очищаем email от mailto:
                email = rel["url"].replace("mailto:", "").strip()
                if email:
                    relations.append(f"Email: {email}")
            elif rel.get("type") == "phone" and rel.get("url"):
                # Очищаем телефон от tel:
                phone = rel["url"].replace("tel:", "").strip()
                if phone:
                    relations.append(f"Телефон: {phone}")
        if relations:
            info_parts.extend(relations)
    
    # Объединяем всё с разделителем
    return "\n\n".join(info_parts) if info_parts else ""


# ---------------------------- Основная функция ----------------------------

def main():
    parser = argparse.ArgumentParser(description="Сбор компаний с hh.ru в SQLite")
    parser.add_argument("-q", "--query", action="append", required=True,
                        help="Поисковый запрос (можно несколько, можно через запятую)")
    parser.add_argument("--db", default="employers.db", help="Путь к SQLite базе данных")
    parser.add_argument("--app", default="PromSoftService-HH-Collector/1.0", help="Имя приложения для User-Agent")
    parser.add_argument("--contact", required=True, help="Контакт (tg:@name или email)")
    parser.add_argument("--update-info", action="store_true", 
                        help="Обновить информацию о компании для существующих записей")
    args = parser.parse_args()

    # Подготовка заголовков
    headers = build_headers(args.app, args.contact)

    # Инициализация базы
    db_path = Path(args.db)
    init_db(db_path)

    # Разбираем запросы (поддерживаем запятые внутри одного -q)
    queries = []
    for q in args.query:
        parts = [p.strip() for p in q.split(",") if p.strip()]
        queries.extend(parts)

    print(f"Всего запросов: {len(queries)}")
    print(f"Первые 5: {queries[:5]}")

    # Загружаем уже существующие ID
    existing_ids = load_existing_ids(db_path)
    existing_info_ids = load_existing_company_info(db_path)
    
    print(f"Уже в базе: {len(existing_ids)} компаний")
    print(f"Из них с информацией: {len(existing_info_ids)}")

    # Собираем компании по всем запросам
    all_employers = {}
    for q in queries:
        print(f"\n--- Запрос: {q} ---")
        try:
            employers = collect_employers_for_query(q, headers)
        except Exception as e:
            msg = f"Ошибка при выполнении запроса '{q}': {e}"
            print(msg)
            log_event(db_path, "GLOBAL", "ERROR", msg, error_type="query_failed")
            continue

        for emp in employers:
            all_employers[emp["employer_id"]] = emp

    print(f"\nУникальных компаний по всем запросам: {len(all_employers)}")

    # Определяем новые компании и компании для обновления информации
    new_employers = []
    update_employers = []
    
    for emp_id, emp in all_employers.items():
        if emp_id not in existing_ids:
            new_employers.append(emp)
        elif args.update_info and emp_id not in existing_info_ids:
            update_employers.append(emp)

    print(f"Новых компаний для добавления: {len(new_employers)}")
    if args.update_info:
        print(f"Компаний для обновления информации: {len(update_employers)}")

    # Получаем полную информацию для компаний
    all_to_process = new_employers + update_employers
    
    if not all_to_process:
        print("Нет компаний для обработки. Выход.")
        return

    print(f"\nПолучаем информацию о компаниях ({len(all_to_process)} шт)...")
    
    successful = 0
    failed = 0
    info_successful = 0
    
    for emp in tqdm(all_to_process, desc="Обработка компаний", unit="комп"):
        api_url = emp.get("_employer_api_url")
        if not api_url:
            emp["site_url"] = ""
            emp["company_info"] = ""
            log_event(db_path, emp["employer_id"], "WARNING",
                      "Нет API URL компании", url=emp.get("hh_url"))
            failed += 1
            continue

        # Получаем сайт
        site_url = fetch_site_url(api_url, headers)
        emp["site_url"] = site_url
        
        # Получаем информацию о компании
        company_info = fetch_company_info(api_url, headers)
        emp["company_info"] = company_info
        emp["company_info_updated"] = dt.datetime.now().isoformat()
        
        if site_url:
            successful += 1
        if company_info:
            info_successful += 1
        
        time.sleep(0.2)  # пауза между запросами к API

    # Вставляем новые записи в базу
    if new_employers:
        insert_employers(db_path, new_employers)
        print(f"\n✅ Добавлено новых компаний: {len(new_employers)}")
    
    # Обновляем информацию для существующих
    if update_employers:
        for emp in update_employers:
            update_company_info(db_path, emp['employer_id'], emp['company_info'])
        print(f"✅ Обновлена информация для: {len(update_employers)} компаний")

    print("\n📊 Итог:")
    print(f"  • Всего обработано: {len(all_to_process)}")
    print(f"  • С сайтом: {successful}")
    print(f"  • С информацией о компании: {info_successful}")
    print(f"  • Без сайта: {failed}")
    print(f"\nБаза данных: {db_path.resolve()}")


if __name__ == "__main__":
    main()
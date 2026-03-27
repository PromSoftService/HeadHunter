#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Зеркалирование сайтов работодателей из SQLite базы.
Проверяет наличие архива на диске; если архива нет — пытается скачать.
Даже при ошибках загрузки создаётся архив (пустой или с частично скачанными страницами).

Логи пишутся ТОЛЬКО в базу данных (таблица logs).
Для выгрузки логов в JSON используйте employers_editor.py

Запуск:
    py -3.14 site_mirror.py --db employers.db --base-dir site_archive --max-pages 12 --concurrency 6
    py -3.14 site_mirror.py --db employers.db --base-dir site_archive --retry-errors  (повторить только с ошибками)
"""

import asyncio
import argparse
import sqlite3
import logging
import re
import shutil
import tarfile
import time
import traceback
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from tqdm import tqdm
from tabulate import tabulate

# ------------------------------ Настройки ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ------------------------------ Работа с БД ------------------------------
class EmployerDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()
        self._migrate_logs_table()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
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
                archive_path TEXT,
                error_type TEXT,
                error_message TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT,
                error_type TEXT,
                url TEXT,
                details TEXT,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
        """)
        conn.commit()
        conn.close()
    
    def _migrate_logs_table(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(logs)")
        columns = [column[1] for column in cur.fetchall()]
        
        if 'details' not in columns:
            try:
                cur.execute("ALTER TABLE logs ADD COLUMN details TEXT")
                logger.info("Добавлена колонка details в таблицу logs")
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось добавить колонку details: {e}")
        
        cur.execute("PRAGMA table_info(employers)")
        emp_columns = [col[1] for col in cur.fetchall()]
        
        if 'error_type' not in emp_columns:
            try:
                cur.execute("ALTER TABLE employers ADD COLUMN error_type TEXT")
                logger.info("Добавлена колонка error_type в таблицу employers")
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось добавить колонку error_type: {e}")
        
        if 'error_message' not in emp_columns:
            try:
                cur.execute("ALTER TABLE employers ADD COLUMN error_message TEXT")
                logger.info("Добавлена колонка error_message в таблицу employers")
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось добавить колонку error_message: {e}")
        
        conn.commit()
        conn.close()

    def get_all_with_site(self, retry_errors: bool = False) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        if retry_errors:
            cur.execute("""
                SELECT employer_id, employer_name, site_url
                FROM employers
                WHERE site_url IS NOT NULL AND site_url != ''
                AND (mirror_status LIKE 'error:%' OR mirror_status IS NULL OR error_type IS NOT NULL)
                ORDER BY added_at
            """)
        else:
            cur.execute("""
                SELECT employer_id, employer_name, site_url
                FROM employers
                WHERE site_url IS NOT NULL AND site_url != ''
                ORDER BY added_at
            """)
        
        rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def update_employer(self, employer_id: str, status: str, pages: int = 0, 
                       archive_path: Optional[str] = None, 
                       error_type: Optional[str] = None,
                       error_message: Optional[str] = None):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        now = datetime.now().isoformat()
        
        cur.execute("""
            UPDATE employers
            SET mirror_status = ?, pages_count = ?, archive_path = ?, 
                last_checked = ?, error_type = ?, error_message = ?
            WHERE employer_id = ?
        """, (status, pages, archive_path, now, error_type, error_message, employer_id))
        
        conn.commit()
        conn.close()

    def log_event(self, employer_id: str, level: str, message: str, 
                 error_type: Optional[str] = None, url: Optional[str] = None, 
                 details: Optional[str] = None):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO logs (employer_id, level, message, error_type, url, details)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (employer_id, level, message, error_type, url, details))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Не удалось записать лог в БД для {employer_id}: {e}")
            logger.error(f"Лог: {level} - {message}")


# ------------------------------ Вспомогательные функции ------------------------------
def normalize_url_for_filename(url: str) -> str:
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    if netloc.startswith('www.'):
        netloc = netloc[4:]
    return netloc.replace('.', '_')

def is_same_domain(url: str, base_domain: str) -> bool:
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return True
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain == base_domain
    except Exception:
        return False

def normalize_href(base_url: str, href: str) -> Optional[str]:
    if not href or href.startswith(('#', 'mailto:', 'tel:', 'javascript:', 'data:', 'whatsapp:', 'viber:')):
        return None
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    if parsed.fragment:
        absolute = urlunparse(parsed._replace(fragment=''))
    query = parse_qs(parsed.query, keep_blank_values=True)
    clean_query = {}
    for key, values in query.items():
        if not key.startswith(('utm_', 'fbclid', 'gclid', 'yclid', 'openstat')):
            clean_query[key] = values
    if clean_query != query:
        new_query = '&'.join(f'{k}={v[0]}' for k, v in clean_query.items())
        absolute = urlunparse(parsed._replace(query=new_query))
    return absolute

def is_likely_about_or_contact(url: str, text: str) -> bool:
    keywords = {
        'about', 'company', 'contact', 'о компании', 'контакты', 'aboutus',
        'about-us', 'about_us', 'contactus', 'contact-us', 'contact_us',
        'о-компании', 'о_компании', 'контакт', 'kontakt', 'aboutcompany',
        'company-profile', 'profile', 'компания', 'сведения'
    }
    url_low = url.lower()
    text_low = text.lower()
    for kw in keywords:
        if kw in url_low or kw in text_low:
            return True
    return False

def safe_filename_from_url(url: str, base_dir: Path) -> Path:
    parsed = urlparse(url)
    path = parsed.path
    if not path or path == '/':
        filename = 'index.html'
    else:
        parts = path.strip('/').split('/')
        last = parts[-1]
        if '.' in last and not last.endswith('/'):
            filename = '_'.join(parts[:-1] + [last]) if len(parts) > 1 else last
        else:
            filename = '_'.join(parts) + '.html'
    filename = re.sub(r'[^\w\.]', '_', filename)
    return base_dir / filename


# ------------------------------ Основной класс ------------------------------
class SiteMirror:
    def __init__(self, args, db: EmployerDB):
        self.args = args
        self.db = db
        self.base_dir = Path(args.base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(args.concurrency)
        
        self.stats = defaultdict(int)
        self.results = []
        self.stats_lock = asyncio.Lock()

    def archive_path_for_url(self, site_url: str) -> Path:
        domain = normalize_url_for_filename(site_url)
        return self.base_dir / f'{domain}.tar.gz'

    async def update_stats(self, emp_id: str, name: str, status: str, pages: int = 0, error: str = ''):
        async with self.stats_lock:
            self.results.append((emp_id, name, status, pages, error))
            if status.startswith('ok'):
                self.stats['ok'] += 1
            elif status.startswith('skipped'):
                self.stats['skipped'] += 1
            elif status.startswith('error'):
                self.stats['error'] += 1
                if 'timeout' in error.lower():
                    self.stats['error_timeout'] += 1
                elif '404' in error:
                    self.stats['error_404'] += 1
                elif 'connection' in error.lower() or 'refused' in error.lower():
                    self.stats['error_connection'] += 1
                else:
                    self.stats['error_other'] += 1

    async def print_stats_table(self):
        async with self.stats_lock:
            if not self.results:
                return
            
            total = len(self.results)
            ok_count = self.stats['ok']
            skipped_count = self.stats['skipped']
            error_count = self.stats['error']
            
            recent = self.results[-10:]
            headers = ["ID", "Компания", "Статус", "Стр.", "Ошибка"]
            table_data = []
            for r in recent:
                table_data.append([
                    r[0], 
                    r[1][:30] + "..." if len(r[1]) > 30 else r[1],
                    r[2],
                    r[3],
                    r[4][:30] if r[4] else ""
                ])
            
            tqdm.write("\n" + "="*80)
            tqdm.write(f"Прогресс: {total}/{self.total_employers} | ✅ {ok_count} | ⏭️ {skipped_count} | ❌ {error_count}")
            tqdm.write(f"Ошибки: timeout {self.stats['error_timeout']}, connection {self.stats['error_connection']}, 404 {self.stats['error_404']}, other {self.stats['error_other']}")
            tqdm.write(tabulate(table_data, headers=headers, tablefmt="grid"))
            tqdm.write("="*80 + "\n")

    async def process_employer(self, employer: Dict[str, Any], browser, pbar: tqdm) -> None:
        emp_id = employer['employer_id']
        name = employer['employer_name']
        site_url = employer['site_url'].strip()

        # Проверяем наличие архива
        archive_path = self.archive_path_for_url(site_url)
        if archive_path.exists() and not self.args.retry_errors:
            status = 'skipped: archive exists'
            self.db.update_employer(emp_id, status)
            self.db.log_event(emp_id, 'INFO', 'Архив уже существует', 'archive_exists', site_url)
            await self.update_stats(emp_id, name, 'skipped', 0, '')
            tqdm.write(f"[{emp_id}] {name} – архив уже существует, пропуск")
            pbar.update(1)
            await self.print_stats_table()
            return

        pages_count = 0
        error_msg = ''
        error_type = None
        final_status = ''
        temp_dir = None

        try:
            # Создаём временную папку
            temp_dir = self.base_dir / f'temp_{emp_id}_{int(time.time())}'
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            for attempt in range(1, self.args.retries + 1):
                context = None
                page = None
                try:
                    tqdm.write(f"[{emp_id}] {name} – попытка {attempt}/{self.args.retries}")
                    context = await browser.new_context(
                        viewport={'width': 1280, 'height': 800},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    )
                    page = await context.new_page()

                    # Переходим на главную
                    wait_until = self.args.wait
                    timeout = self.args.timeout_ms
                    response = await page.goto(site_url, wait_until=wait_until, timeout=timeout)
                    if not response:
                        raise Exception('Нет ответа от сервера')
                    await asyncio.sleep(self.args.settle_ms / 1000.0)

                    # Сохраняем главную
                    main_html = await page.content()
                    main_file = safe_filename_from_url(site_url, temp_dir)
                    main_file.write_text(main_html, encoding='utf-8')
                    pages_count = 1

                    # Извлекаем ссылки
                    links = await self._extract_links(page, site_url)

                    # Определяем базовый домен
                    base_parsed = urlparse(site_url)
                    base_domain = base_parsed.netloc.lower()
                    if base_domain.startswith('www.'):
                        base_domain = base_domain[4:]

                    # Собираем уникальные внутренние ссылки
                    candidates = {}
                    for href, text in links:
                        abs_href = normalize_href(site_url, href)
                        if not abs_href:
                            continue
                        if not is_same_domain(abs_href, base_domain):
                            continue
                        candidates[abs_href] = candidates.get(abs_href, text) or text

                    # Сортируем: сначала важные
                    important = []
                    other = []
                    for href, text in candidates.items():
                        if is_likely_about_or_contact(href, text):
                            important.append(href)
                        else:
                            other.append(href)

                    to_download = important + other
                    to_download = to_download[:self.args.max_pages - 1]

                    # Скачиваем дополнительные страницы
                    for link in to_download:
                        try:
                            resp = await page.goto(link, wait_until=wait_until, timeout=timeout)
                            if not resp:
                                continue
                            await asyncio.sleep(self.args.settle_ms / 1000.0)
                            html = await page.content()
                            file_path = safe_filename_from_url(link, temp_dir)
                            file_path.write_text(html, encoding='utf-8')
                            pages_count += 1
                        except Exception as e:
                            tqdm.write(f"[{emp_id}] Не удалось загрузить {link}: {e}")
                            self.db.log_event(emp_id, 'WARNING', f'Не удалось загрузить страницу: {e}', 'page_load_failed', link, traceback.format_exc())
                            continue

                    final_status = f'ok:{pages_count}'
                    error_type = None
                    error_msg = ''
                    break  # успех

                except PlaywrightTimeoutError as e:
                    error_msg = f'timeout: {e}'
                    error_type = 'timeout'
                    tqdm.write(f"[{emp_id}] {name} – таймаут (попытка {attempt})")
                    self.db.log_event(emp_id, 'ERROR', f'Таймаут при загрузке', 'timeout', site_url, traceback.format_exc())
                    if attempt == self.args.retries:
                        final_status = f'error: {error_msg}'
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    error_msg = f'exception: {e}'
                    error_type = 'exception'
                    tqdm.write(f"[{emp_id}] {name} – ошибка (попытка {attempt}): {e}")
                    self.db.log_event(emp_id, 'ERROR', f'Ошибка при загрузке: {e}', 'exception', site_url, traceback.format_exc())
                    if attempt == self.args.retries:
                        final_status = f'error: {error_msg}'
                    await asyncio.sleep(2)
                    
                finally:
                    if page:
                        await page.close()
                    if context:
                        await context.close()

            # СОЗДАЁМ АРХИВ (всегда, даже если страниц 0)
            try:
                with tarfile.open(archive_path, 'w:gz') as tar:
                    tar.add(temp_dir, arcname=archive_path.stem)
                tqdm.write(f"[{emp_id}] Архив создан: {archive_path.name}")
                
                # Обновляем статус в БД
                if pages_count == 0:
                    if not final_status:
                        final_status = f'error: {error_msg}' if error_msg else 'error: unknown'
                    self.db.update_employer(
                        emp_id, final_status, pages_count, 
                        str(archive_path.relative_to(self.base_dir.parent)),
                        error_type, error_msg
                    )
                    self.db.log_event(emp_id, 'ERROR', error_msg or 'Неизвестная ошибка', 'final_failure', site_url, traceback.format_exc())
                    tqdm.write(f"[{emp_id}] {name} – не удалось загрузить страницы, но архив создан")
                else:
                    self.db.update_employer(
                        emp_id, final_status, pages_count,
                        str(archive_path.relative_to(self.base_dir.parent)),
                        None, None
                    )
                    self.db.log_event(emp_id, 'INFO', f'Успешно скачано {pages_count} страниц', url=site_url)
                    
            except Exception as e:
                tqdm.write(f"[{emp_id}] Ошибка при создании архива: {e}")
                self.db.log_event(emp_id, 'ERROR', f'Ошибка при создании архива: {e}', 'archive_creation_failed', site_url, traceback.format_exc())
                # Даже если архив не создался, всё равно обновляем статус
                self.db.update_employer(
                    emp_id, f'error: archive creation failed: {e}', pages_count, None,
                    'archive_failed', str(e)
                )

        finally:
            # ГАРАНТИРОВАННОЕ УДАЛЕНИЕ ВРЕМЕННОЙ ПАПКИ
            if temp_dir and temp_dir.exists():
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    tqdm.write(f"[{emp_id}] Временная папка удалена")
                except Exception as e:
                    tqdm.write(f"[{emp_id}] Ошибка при удалении временной папки: {e}")

        await self.update_stats(emp_id, name, final_status or 'error: unknown', pages_count, error_msg)
        pbar.update(1)
        await self.print_stats_table()

    async def _extract_links(self, page, base_url: str) -> List[Tuple[str, str]]:
        try:
            links = await page.eval_on_selector_all(
                'a',
                """
                (elements) => elements.map(el => ({
                    href: el.href,
                    text: el.innerText.trim()
                })).filter(item => item.href && !item.href.startsWith('javascript:'))
                """
            )
            return [(link['href'], link['text']) for link in links]
        except Exception:
            return []

    async def run(self):
        employers = self.db.get_all_with_site(self.args.retry_errors)
        if not employers:
            logger.info("Нет компаний с сайтами для обработки.")
            return

        self.total_employers = len(employers)
        mode = "повторная обработка ошибок" if self.args.retry_errors else "полная обработка"
        logger.info(f"Начинаем {mode} {len(employers)} компаний")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                ]
            )

            with tqdm(total=len(employers), desc="Сайты обработано", unit="сайт", position=0) as pbar:
                tasks = []
                for emp in employers:
                    task = asyncio.create_task(self._process_with_semaphore(emp, browser, pbar))
                    tasks.append(task)

                await asyncio.gather(*tasks)

            await browser.close()

        tqdm.write("\n" + "="*80)
        tqdm.write("ИТОГОВАЯ СТАТИСТИКА")
        tqdm.write(f"Всего компаний: {self.total_employers}")
        tqdm.write(f"✅ Успешно: {self.stats['ok']}")
        tqdm.write(f"⏭️ Пропущено (архив есть): {self.stats['skipped']}")
        tqdm.write(f"❌ Ошибок: {self.stats['error']}")
        tqdm.write(f"   ├─ Таймаут: {self.stats['error_timeout']}")
        tqdm.write(f"   ├─ 404: {self.stats['error_404']}")
        tqdm.write(f"   ├─ Connection: {self.stats['error_connection']}")
        tqdm.write(f"   └─ Другие: {self.stats['error_other']}")
        tqdm.write("="*80)

        logger.info("Все задачи завершены.")

    async def _process_with_semaphore(self, emp, browser, pbar):
        async with self.semaphore:
            await self.process_employer(emp, browser, pbar)


# ------------------------------ Точка входа ------------------------------
def main():
    parser = argparse.ArgumentParser(description='Зеркалирование сайтов работодателей из SQLite')
    parser.add_argument('--db', required=True, help='Путь к SQLite базе (например, employers.db)')
    parser.add_argument('--base-dir', default='site_archive', help='Папка для хранения архивов')
    parser.add_argument('--max-pages', type=int, default=12, help='Максимальное количество страниц на сайт (включая главную)')
    parser.add_argument('--concurrency', type=int, default=6, help='Количество одновременно обрабатываемых сайтов')
    parser.add_argument('--timeout-ms', type=int, default=30000, help='Таймаут загрузки страницы (мс)')
    parser.add_argument('--wait', default='domcontentloaded', choices=['load', 'domcontentloaded', 'networkidle'],
                        help='Событие, после которого считать страницу загруженной')
    parser.add_argument('--settle-ms', type=int, default=600, help='Дополнительная пауза после загрузки (мс)')
    parser.add_argument('--retries', type=int, default=2, help='Количество повторных попыток при ошибке')
    parser.add_argument('--retry-errors', action='store_true', help='Повторить только сайты с ошибками')
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ База данных {db_path} не найдена.")
        return

    db = EmployerDB(db_path)
    mirror = SiteMirror(args, db)
    asyncio.run(mirror.run())


if __name__ == '__main__':
    main()
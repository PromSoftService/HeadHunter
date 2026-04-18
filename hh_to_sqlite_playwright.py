#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import datetime as dt
import json
import os
import random
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urlparse

from playwright.async_api import async_playwright

# ==================== Глобальные настройки ====================

STATE_FILE = "hh_auth.json"

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
BROWSER_START_ARGS = ["--start-maximized"]
BROWSER_NO_VIEWPORT = True
BROWSER_HEADLESS = False

BASE_HH_URL = "https://hh.ru"
SEARCH_URL = "https://hh.ru/search/vacancy?area=113"

LOGIN_CHECK_SELECTOR = 'a[data-qa="mainmenu_profile"]'
COOKIE_ACCEPT_SELECTOR = 'button[data-qa="cookies-policy-informer-accept"]'
SEARCH_INPUT_SELECTOR = 'input[data-qa="search-input"]'
SEARCH_BUTTON_SELECTOR = 'button[data-qa="search-button"]'
SEARCH_RESULT_TITLE_SELECTOR = 'a[data-qa="serp-item__title"]'
PAGINATION_NEXT_SELECTOR_CANDIDATES = [
    'a[data-qa="pager-next"]',
    'a[data-qa="bloko-button"][rel="next"]',
    'a[rel="next"]',
    'a[href*="page="] span[data-qa="pager-next"]',
]
SEARCH_RESULTS_PAGE_MARKER_SELECTORS = [
    SEARCH_INPUT_SELECTOR,
    SEARCH_BUTTON_SELECTOR,
]
VACANCY_COMPANY_SELECTOR_CANDIDATES = [
    'a[data-qa="vacancy-company-name"]',
    'a[data-qa="vacancy-company-name-link"]',
    'a[href*="/employer/"]',
]
COMPANY_H1_SELECTOR = "h1"
COMPANY_CANONICAL_SELECTOR = 'link[rel="canonical"]'
HTTP_LINKS_SELECTOR = 'a[href^="http"]'
PARAGRAPH_SELECTOR = "p"
BODY_SELECTOR = "body"

EXCLUDED_SITE_URL_SUBSTRINGS = [
    "hh.ru",
    "hhcdn.ru",
]

EMPLOYER_ID_REGEX = r"/employer/(\d+)"
VACANCY_ID_REGEX = r"/vacancy/(\d+)"

MAX_PAGE_DETECTION_LIMIT = 40

RETRY_DELAYS_SECONDS = [5, 10, 15, 20, 25, 30]

MICRO_DELAY_MIN = 0.08
MICRO_DELAY_MAX = 0.4

TYPE_DELAY_MIN_MS = 90
TYPE_DELAY_MAX_MS = 280
TYPE_EXTRA_PAUSE_PROBABILITY = 0.12
TYPE_EXTRA_PAUSE_MIN = 0.3
TYPE_EXTRA_PAUSE_MAX = 1.2
TYPE_PRECLICK_PAUSE_MIN = 0.2
TYPE_PRECLICK_PAUSE_MAX = 0.8
TYPE_POST_PAUSE_MIN = 0.4
TYPE_POST_PAUSE_MAX = 1.0

RESULTS_PRE_CLICK_PAUSE_MIN = 1.0
RESULTS_PRE_CLICK_PAUSE_MAX = 2.0

SEARCH_RESULTS_ADVANCE_SCROLL_MIN = 140
SEARCH_RESULTS_ADVANCE_SCROLL_MAX = 320
SEARCH_RESULTS_ADVANCE_PAUSE_MIN = 0.15
SEARCH_RESULTS_ADVANCE_PAUSE_MAX = 0.5

VACANCY_SCROLL_STEPS_MIN = 1
VACANCY_SCROLL_STEPS_MAX = 2
VACANCY_SCROLL_DELTA_MIN = 120
VACANCY_SCROLL_DELTA_MAX = 260
VACANCY_SCROLL_PAUSE_MIN = 0.15
VACANCY_SCROLL_PAUSE_MAX = 0.45

COMPANY_SCROLL_STEPS_MIN = 1
COMPANY_SCROLL_STEPS_MAX = 2
COMPANY_SCROLL_DELTA_MIN = 80
COMPANY_SCROLL_DELTA_MAX = 180
COMPANY_SCROLL_PAUSE_MIN = 0.12
COMPANY_SCROLL_PAUSE_MAX = 0.35

VACANCY_PAGE_INITIAL_SLEEP_MIN = 1.6
VACANCY_PAGE_INITIAL_SLEEP_MAX = 3.6
VACANCY_READ_PAUSE_MIN = 1.4
VACANCY_READ_PAUSE_MAX = 4.0
VACANCY_TIME_MULTIPLIER_MIN = 1.0
VACANCY_TIME_MULTIPLIER_MAX = 2.0

COMPANY_PAGE_INITIAL_SLEEP_MIN = 1.0
COMPANY_PAGE_INITIAL_SLEEP_MAX = 2.2
COMPANY_READ_PAUSE_MIN = 1.0
COMPANY_READ_PAUSE_MAX = 2.8
COMPANY_TIME_MULTIPLIER_MIN = 1.0
COMPANY_TIME_MULTIPLIER_MAX = 2.0

POST_LOGIN_RELOAD_SLEEP = 2.0
POST_SEARCH_RESULTS_SLEEP_MIN = 1.0
POST_SEARCH_RESULTS_SLEEP_MAX = 2.0
POST_PAGE_SWITCH_SLEEP_MIN = 1.0
POST_PAGE_SWITCH_SLEEP_MAX = 2.0
RETURN_TO_RESULTS_SLEEP = 0.5
POST_RESET_SEARCH_SLEEP = 1.2
BROWSER_CLOSE_DELAY = 5.0

WAIT_TIMEOUT_SHORT_MS = 5000
WAIT_TIMEOUT_MEDIUM_MS = 15000
WAIT_TIMEOUT_LONG_MS = 30000
WAIT_TIMEOUT_PAGE_MS = 60000

DESCRIPTION_MIN_LENGTH = 80
DESCRIPTION_MAX_PARAGRAPHS = 5

RIGHT_PANEL_CITY_LABEL = "Город"
RIGHT_PANEL_INDUSTRIES_LABEL = "Сферы деятельности"
RIGHT_PANEL_REGISTRATION_TYPE_LABEL = "Тип регистрации"

INFO_PREFIX_DESCRIPTION = "Описание: "
INFO_PREFIX_INDUSTRIES = "Отрасли: "
INFO_PREFIX_REGION = "Регион: "
INFO_PREFIX_ADDRESS = "Адрес: "
INFO_PREFIX_SITE = "Сайт: "

TITLE_DB_DATA = "ДАННЫЕ ДЛЯ БАЗЫ"
TITLE_FINAL_RESULTS = "ИТОГ: ЧТО СОБРАЛИ"

LINE_WIDTH = 100

MOUSE_MOVES_MIN = 1
MOUSE_MOVES_MAX = 3
MOUSE_MARGIN = 50
MOUSE_MIN_SAFE_COORD = 60
MOUSE_MOVE_STEPS_MIN = 6
MOUSE_MOVE_STEPS_MAX = 18
MOUSE_MOVE_PAUSE_MIN = 0.05
MOUSE_MOVE_PAUSE_MAX = 0.25

# ==================== Логирование ====================


def log_info(stage: str, message: str):
    print(f"[INFO] [{stage}] {message}")


def log_ok(stage: str, message: str):
    print(f"[ OK ] [{stage}] {message}")


def log_warn(stage: str, message: str):
    print(f"[WARN] [{stage}] {message}")


def log_err(stage: str, message: str):
    print(f"[ERR ] [{stage}] {message}")


def log_step(stage: str, message: str):
    print(f"[....] [{stage}] {message}")


def log_block(title: str):
    line = "=" * LINE_WIDTH
    print(f"\n{line}\n{title}\n{line}")


# ==================== Вспомогательные функции ====================


def clean_str(s: Any) -> str:
    if s is None:
        return ""
    return str(s).replace("\r", " ").replace("\n", " ").strip()


def is_connection_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = [
        "timeout",
        "timed out",
        "err_timed_out",
        "err_internet_disconnected",
        "err_network_changed",
        "err_connection_reset",
        "err_connection_closed",
        "err_name_not_resolved",
        "err_proxy_connection_failed",
        "net::err_",
    ]
    return any(marker in text for marker in markers)


class RetryablePageError(Exception):
    pass


def wait_for_user_retry(stage: str, action_name: str):
    log_warn(stage, f"{action_name}: автоматические попытки исчерпаны")
    input(f"[{stage}] {action_name}: исправьте ситуацию и нажмите Enter для повторной попытки...")


async def run_with_retries(
    stage: str,
    action_name: str,
    coro_factory: Callable[[], Awaitable[Any]],
    is_retryable_error: Optional[Callable[[Exception], bool]] = None,
) -> Any:
    retry_checker = is_retryable_error or is_connection_error

    while True:
        for attempt_index, delay in enumerate(RETRY_DELAYS_SECONDS, start=1):
            try:
                return await coro_factory()
            except Exception as exc:
                if not retry_checker(exc):
                    raise

                log_warn(stage, f"{action_name}: retryable ошибка на попытке {attempt_index}/{len(RETRY_DELAYS_SECONDS)}: {exc}")

                if attempt_index == len(RETRY_DELAYS_SECONDS):
                    wait_for_user_retry(stage, action_name)
                    break

                log_warn(stage, f"{action_name}: жду {delay} сек перед следующей попыткой")
                await asyncio.sleep(delay)


def is_retryable_search_page_error(exc: Exception) -> bool:
    return is_connection_error(exc) or isinstance(exc, RetryablePageError)


def is_retryable_company_page_error(exc: Exception) -> bool:
    return is_connection_error(exc) or isinstance(exc, RetryablePageError)


# ==================== SQLite ====================


def init_db(db_path: Path):
    conn = sqlite3.connect(db_path)
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
            company_info TEXT,
            company_info_updated TIMESTAMP
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
            url TEXT
        )
    """)

    conn.commit()
    conn.close()


def ensure_indices(db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_employers_vacancy_url ON employers(vacancy_url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_employers_hh_url ON employers(hh_url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_employer_id ON logs(employer_id)")
    conn.commit()
    conn.close()


def log_event(
    db_path: Path,
    employer_id: Optional[str],
    level: str,
    message: str,
    error_type: Optional[str] = None,
    url: Optional[str] = None,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs (employer_id, level, message, error_type, url)
        VALUES (?, ?, ?, ?, ?)
    """, (employer_id, level, message, error_type, url))
    conn.commit()
    conn.close()


def load_existing_employer_ids(db_path: Path) -> Set[str]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT employer_id FROM employers WHERE employer_id IS NOT NULL AND employer_id != ''")
    items = {row[0] for row in cur.fetchall() if row[0]}
    conn.close()
    return items


def load_existing_vacancy_urls(db_path: Path) -> Set[str]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT vacancy_url FROM employers WHERE vacancy_url IS NOT NULL AND vacancy_url != ''")
    items = {normalize_vacancy_url(row[0]) for row in cur.fetchall() if row[0]}
    conn.close()
    return items


def upsert_employer(db_path: Path, employer: Dict[str, Any]):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    now = dt.datetime.now().isoformat()

    cur.execute("""
        INSERT INTO employers (
            employer_id, employer_name, vacancy_url, hh_url, site_url,
            added_at, last_checked, company_info, company_info_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(employer_id) DO UPDATE SET
            employer_name = COALESCE(excluded.employer_name, employers.employer_name),
            vacancy_url = COALESCE(excluded.vacancy_url, employers.vacancy_url),
            hh_url = COALESCE(excluded.hh_url, employers.hh_url),
            site_url = COALESCE(excluded.site_url, employers.site_url),
            company_info = COALESCE(excluded.company_info, employers.company_info),
            company_info_updated = COALESCE(excluded.company_info_updated, employers.company_info_updated)
    """, (
        employer.get("employer_id", ""),
        employer.get("employer_name", ""),
        employer.get("vacancy_url", ""),
        employer.get("hh_url", ""),
        employer.get("site_url", ""),
        now,
        None,
        employer.get("company_info", ""),
        now if employer.get("company_info") else None,
    ))

    conn.commit()
    conn.close()


# ==================== Общие утилиты ====================


async def rand_sleep(a: float, b: float, stage: str = "WAIT", label: str = ""):
    delay = random.uniform(a, b)
    if label:
        log_step(stage, f"{label}; пауза {delay:.1f} c")
    await asyncio.sleep(delay)


async def micro_sleep(a: float = MICRO_DELAY_MIN, b: float = MICRO_DELAY_MAX):
    await asyncio.sleep(random.uniform(a, b))


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def build_company_info(info: Dict[str, Any]) -> str:
    parts = []

    if info.get("description"):
        parts.append(f"{INFO_PREFIX_DESCRIPTION}{info['description']}")
    if info.get("industries"):
        parts.append(f"{INFO_PREFIX_INDUSTRIES}{info['industries']}")
    if info.get("region"):
        parts.append(f"{INFO_PREFIX_REGION}{info['region']}")
    if info.get("address"):
        parts.append(f"{INFO_PREFIX_ADDRESS}{info['address']}")
    if info.get("site_url"):
        parts.append(f"{INFO_PREFIX_SITE}{info['site_url']}")

    return "\n\n".join(parts)


def normalize_vacancy_url(url: str) -> str:
    if not url:
        return ""
    cleaned = clean_text(url)
    match = re.search(VACANCY_ID_REGEX, cleaned)
    if not match:
        return cleaned

    vacancy_id = match.group(1)
    parsed = urlparse(cleaned)
    base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else BASE_HH_URL
    return f"{base}/vacancy/{vacancy_id}"


def normalize_employer_url(url: str) -> str:
    if not url:
        return ""
    cleaned = clean_text(url)
    match = re.search(EMPLOYER_ID_REGEX, cleaned)
    if not match:
        return cleaned

    employer_id = match.group(1)
    parsed = urlparse(cleaned)
    base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else BASE_HH_URL
    return f"{base}/employer/{employer_id}"


def extract_employer_id(url: str) -> str:
    if not url:
        return ""
    match = re.search(EMPLOYER_ID_REGEX, url)
    return match.group(1) if match else ""


def scaled_range(min_value: float, max_value: float, mult_min: float, mult_max: float) -> Tuple[float, float]:
    multiplier = random.uniform(mult_min, mult_max)
    return min_value * multiplier, max_value * multiplier


def build_search_page_url(query: str, page_index: int) -> str:
    encoded_query = quote_plus(query)
    return f"{SEARCH_URL}&text={encoded_query}&page={page_index}"


# ==================== Имитация человека ====================


async def move_mouse_naturally(page, moves_min=MOUSE_MOVES_MIN, moves_max=MOUSE_MOVES_MAX):
    try:
        viewport = page.viewport_size or {"width": 1600, "height": 900}
        width = viewport["width"]
        height = viewport["height"]

        for _ in range(random.randint(moves_min, moves_max)):
            x = random.randint(MOUSE_MARGIN, max(MOUSE_MIN_SAFE_COORD, width - MOUSE_MARGIN))
            y = random.randint(MOUSE_MARGIN, max(MOUSE_MIN_SAFE_COORD, height - MOUSE_MARGIN))
            steps = random.randint(MOUSE_MOVE_STEPS_MIN, MOUSE_MOVE_STEPS_MAX)
            await page.mouse.move(x, y, steps=steps)
            await micro_sleep(MOUSE_MOVE_PAUSE_MIN, MOUSE_MOVE_PAUSE_MAX)
    except Exception:
        pass


async def human_type(locator, text: str, stage="TYPE"):
    await locator.click()
    await micro_sleep(TYPE_PRECLICK_PAUSE_MIN, TYPE_PRECLICK_PAUSE_MAX)

    try:
        await locator.fill("")
    except Exception:
        pass

    log_step(stage, f"печатаю запрос посимвольно: {text}")

    for ch in text:
        delay = random.randint(TYPE_DELAY_MIN_MS, TYPE_DELAY_MAX_MS)
        await locator.type(ch, delay=delay)

        if random.random() < TYPE_EXTRA_PAUSE_PROBABILITY:
            await asyncio.sleep(random.uniform(TYPE_EXTRA_PAUSE_MIN, TYPE_EXTRA_PAUSE_MAX))

    await micro_sleep(TYPE_POST_PAUSE_MIN, TYPE_POST_PAUSE_MAX)


async def quick_search_results_advance(page, stage="RESULTS"):
    delta = random.randint(SEARCH_RESULTS_ADVANCE_SCROLL_MIN, SEARCH_RESULTS_ADVANCE_SCROLL_MAX)
    await page.mouse.wheel(0, delta)
    await asyncio.sleep(random.uniform(SEARCH_RESULTS_ADVANCE_PAUSE_MIN, SEARCH_RESULTS_ADVANCE_PAUSE_MAX))
    if random.random() < 0.45:
        await move_mouse_naturally(page, moves_min=1, moves_max=2)
    log_step(stage, f"быстрый переход к следующей вакансии, скролл {delta}")


async def quick_skip_results_page(page, stage="RESULTS"):
    steps = random.randint(2, 4)
    log_info(stage, "на странице нет новых компаний; быстро пролистываю и перехожу дальше")

    for i in range(steps):
        delta = random.randint(700, 1800)
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(0.08, 0.35))
        log_step(stage, f"быстрый скролл страницы {i + 1}/{steps}, delta={delta}")

    if random.random() < 0.35:
        await move_mouse_naturally(page, moves_min=1, moves_max=2)


async def quick_vacancy_view(page, stage="VACANCY_READ"):
    await move_mouse_naturally(page, moves_min=1, moves_max=2)

    init_min, init_max = scaled_range(
        VACANCY_PAGE_INITIAL_SLEEP_MIN,
        VACANCY_PAGE_INITIAL_SLEEP_MAX,
        VACANCY_TIME_MULTIPLIER_MIN,
        VACANCY_TIME_MULTIPLIER_MAX,
    )
    await rand_sleep(init_min, init_max, stage=stage, label="смотрим верх вакансии")

    steps = random.randint(VACANCY_SCROLL_STEPS_MIN, VACANCY_SCROLL_STEPS_MAX)
    for i in range(steps):
        delta = random.randint(VACANCY_SCROLL_DELTA_MIN, VACANCY_SCROLL_DELTA_MAX)
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(VACANCY_SCROLL_PAUSE_MIN, VACANCY_SCROLL_PAUSE_MAX))
        log_step(stage, f"быстрый скролл вакансии {i + 1}/{steps}, delta={delta}")

    read_min, read_max = scaled_range(
        VACANCY_READ_PAUSE_MIN,
        VACANCY_READ_PAUSE_MAX,
        VACANCY_TIME_MULTIPLIER_MIN,
        VACANCY_TIME_MULTIPLIER_MAX,
    )
    await rand_sleep(read_min, read_max, stage=stage, label="короткий просмотр вакансии")


async def quick_company_view(page, stage="COMPANY_READ"):
    await move_mouse_naturally(page, moves_min=1, moves_max=2)

    init_min, init_max = scaled_range(
        COMPANY_PAGE_INITIAL_SLEEP_MIN,
        COMPANY_PAGE_INITIAL_SLEEP_MAX,
        COMPANY_TIME_MULTIPLIER_MIN,
        COMPANY_TIME_MULTIPLIER_MAX,
    )
    await rand_sleep(init_min, init_max, stage=stage, label="смотрим верх страницы компании")

    steps = random.randint(COMPANY_SCROLL_STEPS_MIN, COMPANY_SCROLL_STEPS_MAX)
    for i in range(steps):
        delta = random.randint(COMPANY_SCROLL_DELTA_MIN, COMPANY_SCROLL_DELTA_MAX)
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(COMPANY_SCROLL_PAUSE_MIN, COMPANY_SCROLL_PAUSE_MAX))
        log_step(stage, f"быстрый скролл компании {i + 1}/{steps}, delta={delta}")

    read_min, read_max = scaled_range(
        COMPANY_READ_PAUSE_MIN,
        COMPANY_READ_PAUSE_MAX,
        COMPANY_TIME_MULTIPLIER_MIN,
        COMPANY_TIME_MULTIPLIER_MAX,
    )
    await rand_sleep(read_min, read_max, stage=stage, label="короткий просмотр компании")


# ==================== Сессия ====================


async def is_logged_in(page) -> bool:
    try:
        await page.wait_for_selector(LOGIN_CHECK_SELECTOR, timeout=WAIT_TIMEOUT_SHORT_MS)
        return True
    except Exception:
        return False


async def save_state(context):
    state = await context.storage_state()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    log_ok("AUTH", "состояние сессии сохранено")


async def try_click_cookie_accept(page):
    try:
        btn = page.locator(COOKIE_ACCEPT_SELECTOR)
        if await btn.count() > 0 and await btn.first.is_visible():
            await btn.first.click()
            await micro_sleep()
            log_ok("COOKIE", "баннер cookies принят")
    except Exception:
        pass


# ==================== Парсинг компании ====================


async def get_company_info(company_page) -> Dict[str, Any]:
    info = {
        "employer_id": "",
        "employer_name": "",
        "hh_url": "",
        "site_url": "",
        "description": "",
        "industries": "",
        "address": "",
        "region": "",
        "company_info": "",
    }

    try:
        await company_page.wait_for_selector(COMPANY_H1_SELECTOR, timeout=WAIT_TIMEOUT_MEDIUM_MS)
    except Exception:
        log_warn("COMPANY", "не дождались h1 на странице компании")
        return info

    await try_click_cookie_accept(company_page)

    try:
        info["employer_name"] = clean_text(await company_page.locator(COMPANY_H1_SELECTOR).first.inner_text())
        if info["employer_name"]:
            log_ok("COMPANY", f"название: {info['employer_name']}")
    except Exception:
        pass

    try:
        canonical = await company_page.locator(COMPANY_CANONICAL_SELECTOR).get_attribute("href")
        canonical = clean_text(canonical or "")
        info["hh_url"] = canonical

        m = re.search(EMPLOYER_ID_REGEX, canonical)
        if m:
            info["employer_id"] = m.group(1)

        if info["hh_url"]:
            log_ok("COMPANY", f"hh_url: {info['hh_url']}")
        if info["employer_id"]:
            log_ok("COMPANY", f"employer_id: {info['employer_id']}")
    except Exception:
        pass

    if not info["employer_id"]:
        try:
            employer_id = await company_page.evaluate(
                """
                () => {
                    try {
                        const ap = window.globalVars?.analyticsParams;
                        if (!ap) return "";
                        if (typeof ap === "string") {
                            const parsed = JSON.parse(ap);
                            return parsed?.employerId || "";
                        }
                        return ap?.employerId || "";
                    } catch (e) {
                        return "";
                    }
                }
                """
            )
            info["employer_id"] = clean_text(employer_id)
            if info["employer_id"]:
                log_ok("COMPANY", f"fallback employer_id: {info['employer_id']}")
        except Exception:
            pass

    try:
        links = await company_page.locator(HTTP_LINKS_SELECTOR).evaluate_all(
            """
            els => els.map(a => ({
                href: a.href || "",
                text: (a.innerText || "").trim()
            }))
            """
        )

        external_links = []
        for link in links:
            href = clean_text(link.get("href", ""))
            if not href:
                continue

            is_excluded = False
            for bad_part in EXCLUDED_SITE_URL_SUBSTRINGS:
                if bad_part in href:
                    is_excluded = True
                    break

            if is_excluded:
                continue

            external_links.append(href)

        if external_links:
            unique_links = list(dict.fromkeys(external_links))
            info["site_url"] = unique_links[0]
            log_ok("COMPANY", f"site_url: {info['site_url']}")
    except Exception:
        pass

    try:
        paragraphs = await company_page.locator(PARAGRAPH_SELECTOR).all_inner_texts()
        cleaned = [clean_text(p) for p in paragraphs if clean_text(p)]
        long_paragraphs = [p for p in cleaned if len(p) >= DESCRIPTION_MIN_LENGTH]

        if long_paragraphs:
            info["description"] = "\n\n".join(long_paragraphs[:DESCRIPTION_MAX_PARAGRAPHS])
            log_ok("COMPANY", "описание найдено")
    except Exception:
        pass

    try:
        all_text = await company_page.locator(BODY_SELECTOR).inner_text()
        lines = [clean_text(x) for x in all_text.splitlines() if clean_text(x)]

        for i, line in enumerate(lines):
            if line == RIGHT_PANEL_CITY_LABEL and i > 0:
                info["region"] = lines[i - 1]
            elif line == RIGHT_PANEL_INDUSTRIES_LABEL and i > 0:
                info["industries"] = lines[i - 1]
            elif line == RIGHT_PANEL_REGISTRATION_TYPE_LABEL and i > 0:
                info["address"] = lines[i - 1]

        if info["region"]:
            log_ok("COMPANY", f"region: {info['region']}")
        if info["industries"]:
            log_ok("COMPANY", f"industries: {info['industries']}")
        if info["address"]:
            log_ok("COMPANY", f"address/type: {info['address']}")
    except Exception:
        pass

    info["company_info"] = build_company_info(info)

    if info["company_info"]:
        log_ok("COMPANY", "company_info собрано")

    return info


# ==================== Консоль ====================


def print_company_for_db(company: Dict[str, Any]):
    log_block(TITLE_DB_DATA)
    print(f"employer_id   : {company.get('employer_id', '')}")
    print(f"employer_name : {company.get('employer_name', '')}")
    print(f"vacancy_url   : {company.get('vacancy_url', '')}")
    print(f"hh_url        : {company.get('hh_url', '')}")
    print(f"site_url      : {company.get('site_url', '')}")
    print("company_info  :")
    print(company.get("company_info", ""))
    print("=" * LINE_WIDTH)


async def ensure_search_results_page_is_valid(results_page):
    search_items = await collect_search_items_from_current_page(results_page)
    if not search_items:
        raise RetryablePageError("на странице выдачи нет ни одной карточки вакансии")
    return search_items


async def has_next_results_page(results_page, query: str, current_page_index: int) -> bool:
    for selector in PAGINATION_NEXT_SELECTOR_CANDIDATES:
        try:
            locator = results_page.locator(selector)
            if await locator.count() > 0 and await locator.first.is_visible():
                return True
        except Exception:
            pass

    expected_next_page_param = f"page={current_page_index + 1}"
    expected_next_page_url = build_search_page_url(query, current_page_index + 1)

    try:
        hrefs = await results_page.locator('a[href*="page="]').evaluate_all(
            "els => els.map(a => a.getAttribute('href') || a.href || '')"
        )
    except Exception:
        hrefs = []

    for href in hrefs:
        normalized_href = clean_text(href)
        if expected_next_page_param in normalized_href or normalized_href == expected_next_page_url:
            return True

    return False


async def ensure_company_page_is_valid(company_page, employer_hh_url: str):
    info = await get_company_info(company_page)

    if not info.get("employer_name"):
        raise RetryablePageError(f"страница компании открылась без названия компании: {employer_hh_url}")

    if not info.get("employer_id"):
        raise RetryablePageError(f"страница компании открылась без employer_id: {employer_hh_url}")

    return info


# ==================== Пагинация ====================


async def open_results_page_for_query(results_page, query: str, page_index: int) -> List[Dict[str, str]]:
    target_url = build_search_page_url(query, page_index)

    async def _open():
        await results_page.goto(
            target_url,
            timeout=WAIT_TIMEOUT_PAGE_MS,
            wait_until="domcontentloaded",
        )

        for selector in SEARCH_RESULTS_PAGE_MARKER_SELECTORS:
            await results_page.wait_for_selector(selector, timeout=WAIT_TIMEOUT_MEDIUM_MS)

        await rand_sleep(
            POST_PAGE_SWITCH_SLEEP_MIN,
            POST_PAGE_SWITCH_SLEEP_MAX,
            stage="SEARCH_PAGE",
            label=f"смотрим выдачу страницы {page_index + 1}",
        )

        return await ensure_search_results_page_is_valid(results_page)

    log_info("SEARCH_PAGE", f"открываю страницу {page_index + 1}: {target_url}")
    return await run_with_retries(
        "SEARCH_PAGE",
        f"открытие страницы {page_index + 1}",
        _open,
        is_retryable_error=is_retryable_search_page_error,
    )


async def collect_search_items_from_current_page(results_page) -> List[Dict[str, str]]:
    items = await results_page.evaluate(
        """
        (titleSelector) => {
            const toAbsoluteUrl = (href) => {
                if (!href) return "";
                try {
                    return new URL(href, window.location.origin).toString();
                } catch (e) {
                    return href;
                }
            };

            const normalizeEmployerUrl = (href) => {
                if (!href) return "";
                const match = href.match(/\\/employer\\/(\\d+)/);
                if (!match) return href;
                try {
                    const u = new URL(href, window.location.origin);
                    return `${u.origin}/employer/${match[1]}`;
                } catch (e) {
                    return href;
                }
            };

            const titleAnchors = Array.from(document.querySelectorAll(titleSelector));
            const results = [];
            const seenVacancyUrls = new Set();

            for (const titleAnchor of titleAnchors) {
                const vacancyHref = toAbsoluteUrl(titleAnchor.getAttribute("href") || titleAnchor.href || "");
                if (!vacancyHref || vacancyHref.includes("adsrv.hh.ru")) {
                    continue;
                }

                let container = titleAnchor;
                let employerAnchor = null;

                for (let depth = 0; depth < 8 && container; depth += 1) {
                    const candidate = container.querySelector('a[href*="/employer/"]');
                    if (candidate && candidate !== titleAnchor) {
                        employerAnchor = candidate;
                        break;
                    }
                    container = container.parentElement;
                }

                if (!employerAnchor) {
                    const serpItem = titleAnchor.closest('[data-qa="serp-item"], .serp-item, article, main li, main div');
                    if (serpItem) {
                        const candidate = serpItem.querySelector('a[href*="/employer/"]');
                        if (candidate && candidate !== titleAnchor) {
                            employerAnchor = candidate;
                        }
                    }
                }

                const employerHrefRaw = employerAnchor
                    ? (employerAnchor.getAttribute("href") || employerAnchor.href || "")
                    : "";
                const employerHref = normalizeEmployerUrl(toAbsoluteUrl(employerHrefRaw));
                const employerIdMatch = employerHref.match(/\\/employer\\/(\\d+)/);
                const employerId = employerIdMatch ? employerIdMatch[1] : "";

                if (seenVacancyUrls.has(vacancyHref)) {
                    continue;
                }
                seenVacancyUrls.add(vacancyHref);

                results.push({
                    vacancy_url: vacancyHref,
                    employer_hh_url: employerHref,
                    employer_id: employerId,
                });
            }

            return results;
        }
        """,
        SEARCH_RESULT_TITLE_SELECTOR,
    )

    normalized_items: List[Dict[str, str]] = []
    seen_vacancy_urls: Set[str] = set()

    for item in items:
        vacancy_url = normalize_vacancy_url(item.get("vacancy_url", ""))
        employer_hh_url = normalize_employer_url(item.get("employer_hh_url", ""))
        employer_id = clean_text(item.get("employer_id", "")) or extract_employer_id(employer_hh_url)

        if not vacancy_url or vacancy_url in seen_vacancy_urls:
            continue

        seen_vacancy_urls.add(vacancy_url)
        normalized_items.append(
            {
                "vacancy_url": vacancy_url,
                "employer_hh_url": employer_hh_url,
                "employer_id": employer_id,
            }
        )

    return normalized_items



def classify_search_item_state(
    item: Dict[str, str],
    existing_employer_ids: Set[str],
    existing_vacancy_urls: Set[str],
    seen_vacancy_urls: Set[str],
    seen_employer_ids: Set[str],
) -> str:
    vacancy_url = item.get("vacancy_url", "")
    employer_id = item.get("employer_id", "")

    if vacancy_url in existing_vacancy_urls:
        return "SKIP"
    if vacancy_url in seen_vacancy_urls:
        return "SEEN"
    if not employer_id:
        return "SKIP"
    if employer_id in existing_employer_ids:
        return "SKIP"
    if employer_id in seen_employer_ids:
        return "SEEN"
    return "NEW "

# ==================== Обработка работодателя из выдачи ====================


async def process_search_result_item(
    context,
    item: Dict[str, str],
    results_page,
    query: str,
    collected_data: List[Dict[str, Any]],
    db_path: Path,
    existing_employer_ids: Set[str],
    existing_vacancy_urls: Set[str],
    seen_vacancy_urls: Set[str],
    seen_employer_ids: Set[str],
):
    vacancy_url = normalize_vacancy_url(item.get("vacancy_url", ""))
    employer_hh_url = normalize_employer_url(item.get("employer_hh_url", ""))
    employer_id = clean_text(item.get("employer_id", "")) or extract_employer_id(employer_hh_url)

    if vacancy_url in existing_vacancy_urls:
        log_ok("SKIP", f"вакансия уже есть в employers: {vacancy_url}")
        seen_vacancy_urls.add(vacancy_url)
        if employer_id:
            seen_employer_ids.add(employer_id)
        return

    if vacancy_url in seen_vacancy_urls:
        log_ok("SKIP", f"вакансия уже встречалась в текущем запуске: {vacancy_url}")
        return

    if employer_id and employer_id in existing_employer_ids:
        log_ok("SKIP", f"компания уже есть в employers: {employer_id}")
        seen_vacancy_urls.add(vacancy_url)
        seen_employer_ids.add(employer_id)
        return

    if employer_id and employer_id in seen_employer_ids:
        log_ok("SKIP", f"компания уже встречалась в текущем запуске: {employer_id}")
        seen_vacancy_urls.add(vacancy_url)
        return

    if not employer_hh_url:
        log_warn("SEARCH", f"не нашли employer_hh_url на выдаче: {vacancy_url}")
        log_event(
            db_path,
            employer_id or None,
            "WARNING",
            "Не нашли employer_hh_url на странице выдачи",
            "no_employer_url_on_results",
            vacancy_url,
        )
        seen_vacancy_urls.add(vacancy_url)
        return

    log_info("COMPANY", f"обработка работодателя из выдачи: vacancy={vacancy_url}, employer={employer_hh_url}")

    company_page = await context.new_page()

    try:
        async def _open_company():
            await company_page.goto(employer_hh_url, timeout=WAIT_TIMEOUT_PAGE_MS, wait_until="domcontentloaded")
            await quick_company_view(company_page, stage="COMPANY_READ")
            return await ensure_company_page_is_valid(company_page, employer_hh_url)

        company_info = await run_with_retries(
            "COMPANY",
            f"открытие компании {employer_hh_url}",
            _open_company,
            is_retryable_error=is_retryable_company_page_error,
        )
        log_ok("COMPANY", "страница компании открыта и валидна")

        company_info["query"] = query
        company_info["vacancy_url"] = vacancy_url

        if not company_info.get("employer_id"):
            company_info["employer_id"] = employer_id
        if not company_info.get("hh_url"):
            company_info["hh_url"] = employer_hh_url

        if not company_info.get("employer_id"):
            log_warn("COMPANY", "не получили employer_id, в базу не пишу")
            log_event(
                db_path,
                None,
                "WARNING",
                "Не получили employer_id после открытия страницы компании",
                "no_employer_id_after_company_open",
                employer_hh_url,
            )
            seen_vacancy_urls.add(vacancy_url)
            return

        if not company_info.get("employer_name"):
            log_warn("COMPANY", "не получили employer_name, в базу не пишу")
            log_event(
                db_path,
                company_info.get("employer_id"),
                "WARNING",
                "Не получили employer_name после открытия страницы компании",
                "no_employer_name_after_company_open",
                employer_hh_url,
            )
            seen_vacancy_urls.add(vacancy_url)
            seen_employer_ids.add(company_info["employer_id"])
            return

        collected_data.append(company_info)
        print_company_for_db(company_info)

        upsert_employer(db_path, company_info)

        existing_employer_ids.add(company_info["employer_id"])
        existing_vacancy_urls.add(vacancy_url)
        seen_employer_ids.add(company_info["employer_id"])
        seen_vacancy_urls.add(vacancy_url)

        log_ok("DB", f"компания сохранена в базу: {company_info['employer_id']}")

    except Exception as e:
        log_err("COMPANY", f"ошибка при обработке работодателя: {e}")
        log_event(
            db_path,
            employer_id or None,
            "ERROR",
            str(e),
            "company_processing_failed",
            employer_hh_url,
        )
        raise
    finally:
        try:
            await company_page.close()
            log_ok("COMPANY", "вкладка компании закрыта")
        except Exception:
            pass

        try:
            await results_page.bring_to_front()
            await asyncio.sleep(RETURN_TO_RESULTS_SLEEP)
        except Exception:
            pass


# ==================== Проход по странице выдачи ====================


async def process_results_page(
    context,
    results_page,
    query: str,
    page_index: int,
    collected_data: List[Dict[str, Any]],
    db_path: Path,
    existing_employer_ids: Set[str],
    existing_vacancy_urls: Set[str],
    seen_vacancy_urls: Set[str],
    seen_employer_ids: Set[str],
) -> Tuple[List[Dict[str, str]], bool]:
    log_block(f"СТРАНИЦА ВЫДАЧИ {page_index + 1}")

    search_items = await open_results_page_for_query(results_page, query, page_index)

    log_info("SEARCH", f"на странице {page_index + 1} найдено вакансий: {len(search_items)}")
    page_states: List[str] = []

    for idx, item in enumerate(search_items, 1):
        vacancy_url = item.get("vacancy_url", "")
        employer_id = item.get("employer_id", "")
        state = classify_search_item_state(
            item,
            existing_employer_ids,
            existing_vacancy_urls,
            seen_vacancy_urls,
            seen_employer_ids,
        )

        page_states.append(state)
        suffix = f" employer_id={employer_id}" if employer_id else ""
        log_step("SEARCH", f"{idx}. [{state}] {vacancy_url}{suffix}")

    has_new_items = any(state == "NEW " for state in page_states)
    if not has_new_items and search_items:
        await quick_skip_results_page(results_page, stage="RESULTS")
        has_next_page = await has_next_results_page(results_page, query, page_index)
        return search_items, has_next_page

    for idx, item in enumerate(search_items, 1):
        await rand_sleep(
            RESULTS_PRE_CLICK_PAUSE_MIN,
            RESULTS_PRE_CLICK_PAUSE_MAX,
            stage="RESULTS",
            label="смотрим выдачу перед обработкой работодателя",
        )

        if idx > 1:
            await quick_search_results_advance(results_page, stage="RESULTS")

        log_info(
            "QUEUE",
            f"обработка позиции {idx} из {len(search_items)} на странице {page_index + 1}",
        )
        await process_search_result_item(
            context,
            item,
            results_page,
            query,
            collected_data,
            db_path,
            existing_employer_ids,
            existing_vacancy_urls,
            seen_vacancy_urls,
            seen_employer_ids,
        )

        remaining_items = search_items[idx:]
        has_remaining_new_items = any(
            classify_search_item_state(
                remaining_item,
                existing_employer_ids,
                existing_vacancy_urls,
                seen_vacancy_urls,
                seen_employer_ids,
            )
            == "NEW "
            for remaining_item in remaining_items
        )
        if not has_remaining_new_items and remaining_items:
            log_info(
                "RESULTS",
                "на остатке страницы новых компаний больше нет; быстро пролистываю и перехожу дальше",
            )
            await quick_skip_results_page(results_page, stage="RESULTS")
            break

    has_next_page = await has_next_results_page(results_page, query, page_index)
    return search_items, has_next_page


async def process_all_results_pages(
    context,
    results_page,
    query: str,
    collected_data: List[Dict[str, Any]],
    db_path: Path,
    existing_employer_ids: Set[str],
    existing_vacancy_urls: Set[str],
    seen_vacancy_urls: Set[str],
    seen_employer_ids: Set[str],
    max_pages: int,
):
    seen_page_signatures: Set[Tuple[str, ...]] = set()

    for page_index in range(max_pages):
        search_items, has_next_page = await process_results_page(
            context,
            results_page,
            query,
            page_index,
            collected_data,
            db_path,
            existing_employer_ids,
            existing_vacancy_urls,
            seen_vacancy_urls,
            seen_employer_ids,
        )

        vacancy_urls = [item.get("vacancy_url", "") for item in search_items if item.get("vacancy_url", "")]
        signature = tuple(vacancy_urls)
        if signature in seen_page_signatures:
            log_warn(
                "SEARCH_PAGE",
                f"страница {page_index + 1} повторяет уже просмотренную выдачу, останавливаюсь",
            )
            break

        seen_page_signatures.add(signature)

        if not has_next_page:
            log_ok("SEARCH_PAGE", f"страница {page_index + 1} последняя; следующей страницы нет")
            break

        log_info(
            "SEARCH_PAGE",
            f"страница {page_index + 1} обработана; продолжаю проверять следующую страницу",
        )
    else:
        log_warn(
            "SEARCH_PAGE",
            f"достигнут лимит страниц {max_pages}; дальше не иду",
        )


# ==================== Главный browser flow ====================


async def run_browser_flow(args):
    db_path = Path(args.db)
    init_db(db_path)
    ensure_indices(db_path)

    existing_employer_ids = load_existing_employer_ids(db_path)
    existing_vacancy_urls = load_existing_vacancy_urls(db_path)
    seen_vacancy_urls: Set[str] = set()
    seen_employer_ids: Set[str] = set()

    log_info("DB", f"уже в базе работодателей: {len(existing_employer_ids)}")
    log_info("DB", f"уже в базе вакансий по employers.vacancy_url: {len(existing_vacancy_urls)}")

    all_companies: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        log_info("BROWSER", "запуск браузера")
        browser = await p.chromium.launch(
            headless=BROWSER_HEADLESS,
            args=BROWSER_START_ARGS,
        )

        context = await browser.new_context(
            user_agent=BROWSER_USER_AGENT,
            no_viewport=BROWSER_NO_VIEWPORT,
        )

        if os.path.exists(STATE_FILE):
            log_info("AUTH", "загружаю сохранённую сессию")
            await browser.close()

            browser = await p.chromium.launch(
                headless=BROWSER_HEADLESS,
                args=BROWSER_START_ARGS,
            )
            context = await browser.new_context(
                user_agent=BROWSER_USER_AGENT,
                no_viewport=BROWSER_NO_VIEWPORT,
                storage_state=STATE_FILE,
            )

        results_page = await context.new_page()
        log_info("SEARCH", "открываю страницу поиска")

        async def _open_search():
            await results_page.goto(
                SEARCH_URL,
                timeout=WAIT_TIMEOUT_PAGE_MS,
                wait_until="domcontentloaded",
            )

        await run_with_retries("SEARCH", "открытие страницы поиска", _open_search)

        if await is_logged_in(results_page):
            log_ok("AUTH", "сессия активна")
        else:
            log_warn("AUTH", "сессия не активна; нужен ручной вход")
            input("После входа нажмите Enter...")
            await save_state(context)

            try:
                await results_page.close()
            except Exception:
                pass

            results_page = await context.new_page()

            async def _open_search_after_login():
                await results_page.goto(
                    SEARCH_URL,
                    timeout=WAIT_TIMEOUT_PAGE_MS,
                    wait_until="domcontentloaded",
                )

            await run_with_retries("AUTH", "открытие поиска после логина", _open_search_after_login)
            log_ok("AUTH", "после ручного входа открыта страница поиска")
            await asyncio.sleep(POST_LOGIN_RELOAD_SLEEP)

        for query in args.queries:
            log_block(f"ПОИСК: {query}")

            search_input = await results_page.wait_for_selector(
                SEARCH_INPUT_SELECTOR,
                timeout=WAIT_TIMEOUT_MEDIUM_MS,
            )
            await human_type(search_input, query, stage="SEARCH")

            await rand_sleep(0.8, 1.8, stage="SEARCH", label="пауза перед нажатием поиска")

            search_button = await results_page.wait_for_selector(
                SEARCH_BUTTON_SELECTOR,
                timeout=WAIT_TIMEOUT_SHORT_MS,
            )
            await search_button.click()
            log_ok("SEARCH", "поиск выполнен")

            async def _validate_initial_results():
                await rand_sleep(
                    POST_SEARCH_RESULTS_SLEEP_MIN,
                    POST_SEARCH_RESULTS_SLEEP_MAX,
                    stage="SEARCH",
                    label="смотрим выдачу",
                )
                items = await ensure_search_results_page_is_valid(results_page)
                log_ok("SEARCH", f"найдены вакансии по запросу '{query}'")
                return items

            await run_with_retries(
                "SEARCH",
                f"первичная проверка выдачи по запросу '{query}'",
                _validate_initial_results,
                is_retryable_error=is_retryable_search_page_error,
            )

            max_pages = args.max_pages if args.max_pages is not None else MAX_PAGE_DETECTION_LIMIT

            await process_all_results_pages(
                context,
                results_page,
                query,
                all_companies,
                db_path,
                existing_employer_ids,
                existing_vacancy_urls,
                seen_vacancy_urls,
                seen_employer_ids,
                max_pages,
            )

            log_step("SEARCH", "сброс страницы поиска")

            async def _reset_search():
                await results_page.goto(
                    SEARCH_URL,
                    timeout=WAIT_TIMEOUT_PAGE_MS,
                    wait_until="domcontentloaded",
                )

            await run_with_retries("SEARCH", "сброс страницы поиска", _reset_search)
            await asyncio.sleep(POST_RESET_SEARCH_SLEEP)

        log_ok("DONE", "все запросы обработаны")

        log_block(TITLE_FINAL_RESULTS)
        for i, comp in enumerate(all_companies, 1):
            print(f"\n--- Компания #{i} ---")
            print(f"employer_id   : {comp.get('employer_id', '')}")
            print(f"employer_name : {comp.get('employer_name', '')}")
            print(f"vacancy_url   : {comp.get('vacancy_url', '')}")
            print(f"hh_url        : {comp.get('hh_url', '')}")
            print(f"site_url      : {comp.get('site_url', '')}")
            print("company_info  :")
            print(comp.get("company_info", ""))

        log_info("DB", f"итого работодателей в базе: {len(load_existing_employer_ids(db_path))}")
        log_info("DB", f"итого вакансий по employers.vacancy_url: {len(load_existing_vacancy_urls(db_path))}")

        log_info("BROWSER", f"браузер закроется через {int(BROWSER_CLOSE_DELAY)} секунд")
        await asyncio.sleep(BROWSER_CLOSE_DELAY)
        await browser.close()


# ==================== CLI ====================


def parse_queries(query_args: List[str]) -> List[str]:
    queries: List[str] = []
    for q in query_args:
        parts = [p.strip() for p in q.split(",") if p.strip()]
        queries.extend(parts)
    return queries


def main():
    parser = argparse.ArgumentParser(
        description="Сбор компаний с hh.ru через Playwright и сохранение в SQLite"
    )
    parser.add_argument(
        "-q",
        "--query",
        action="append",
        required=True,
        help="Поисковый запрос (можно несколько, можно через запятую)",
    )
    parser.add_argument(
        "--db",
        default="employers.db",
        help="Путь к SQLite базе данных",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Максимальное количество страниц выдачи для обхода",
    )

    args = parser.parse_args()
    args.queries = parse_queries(args.query)

    if args.max_pages is not None and args.max_pages <= 0:
        parser.error("--max-pages должен быть положительным числом")

    print(f"Всего запросов: {len(args.queries)}")
    print(f"Первые 5: {args.queries[:5]}")
    print(f"База данных: {Path(args.db).resolve()}")
    print(f"Лимит страниц: {args.max_pages if args.max_pages is not None else MAX_PAGE_DETECTION_LIMIT}")

    try:
        asyncio.run(run_browser_flow(args))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем")
        sys.exit(130)
    except SystemExit:
        raise


if __name__ == "__main__":
    main()

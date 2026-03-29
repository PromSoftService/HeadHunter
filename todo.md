
# Схема базы данных employers.db

## Таблица employers

| Поле | Тип | Описание | Как заполняется |
|------|-----|----------|-----------------|
| `employer_id` | TEXT PRIMARY KEY | Уникальный идентификатор компании в hh.ru | `hh_to_sqlite.py` (из API hh.ru) |
| `employer_name` | TEXT NOT NULL | Название компании | `hh_to_sqlite.py` (из API hh.ru) |
| `vacancy_url` | TEXT | URL вакансии, по которой найдена компания | `hh_to_sqlite.py` (из API hh.ru) |
| `hh_url` | TEXT | Ссылка на профиль компании на hh.ru | `hh_to_sqlite.py` (из API hh.ru) |
| `site_url` | TEXT | Сайт компании (из профиля hh.ru) | `hh_to_sqlite.py` (из API hh.ru) |
| `added_at` | TIMESTAMP | Дата и время добавления компании в базу | `hh_to_sqlite.py` (автоматически) |
| `last_checked` | TIMESTAMP | Дата последней проверки/зеркалирования | `site_mirror.py` |
| `mirror_status` | TEXT | Статус зеркалирования: `ok:N` (успешно, N страниц), `error:...` (ошибка), `skipped:...` (пропущено) | `site_mirror.py` |
| `pages_count` | INTEGER | Количество успешно скачанных страниц | `site_mirror.py` |
| `archive_path` | TEXT | Путь к архиву сайта (относительный) | `site_mirror.py` |
| `error_type` | TEXT | Тип ошибки (timeout, connection, 404, exception, archive_failed и др.) | `site_mirror.py` |
| `error_message` | TEXT | Детальное сообщение об ошибке | `site_mirror.py` |
| `company_info` | TEXT | Структурированная информация о компании: название, описание, отрасли, тип, сайт, адрес, регион, контакты | `hh_to_sqlite.py` (при добавлении), `update_company_info.py` (при обновлении) |
| `company_info_updated` | TIMESTAMP | Дата последнего обновления company_info | `hh_to_sqlite.py`, `update_company_info.py` |
| `category` | TEXT | Категория компании (например, "Интеграторы АСУТП", "Кадровые агентства") | `categorize_companies.py` |
| `category_priority` | TEXT | Приоритет категории: primary / medium / low / excluded | `categorize_companies.py` (вычисляется автоматически) |
| `category_updated` | TIMESTAMP | Дата последнего обновления категории | `categorize_companies.py` |
| `category_notes` | TEXT | Пояснение от DeepSeek API, почему выбрана такая категория | `categorize_companies.py` |
| `email` | TEXT | Email-адреса компании (множественные разделяются `; `) | `extract_contacts.py` |
| `phone` | TEXT | Телефоны компании (множественные разделяются `; `), проходят валидацию через DaData | `extract_contacts.py` |
| `contacts_updated` | TIMESTAMP | Дата последнего обновления email и phone | `extract_contacts.py` |
| `comments` | TEXT | Ручные комментарии пользователя (импортируются из Excel) | `employers_editor.py` (импорт из Excel) |

---

## Таблица logs

| Поле | Тип | Описание | Как заполняется |
|------|-----|----------|-----------------|
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | Уникальный идентификатор записи лога | Автоматически |
| `employer_id` | TEXT | ID компании, к которой относится лог (может быть NULL для глобальных событий) | Все скрипты |
| `timestamp` | TIMESTAMP | Дата и время события | Автоматически |
| `level` | TEXT | Уровень логирования: INFO, WARNING, ERROR | Все скрипты |
| `message` | TEXT | Текст сообщения | Все скрипты |
| `error_type` | TEXT | Тип ошибки (timeout, connection, page_load_failed, archive_creation_failed, query_failed и др.) | `site_mirror.py`, `hh_to_sqlite.py` |
| `url` | TEXT | URL, с которым связана ошибка (если применимо) | `site_mirror.py`, `hh_to_sqlite.py` |
| `details` | TEXT | Подробная информация, стек ошибки (traceback) | `site_mirror.py` |
---

# Установка зависимостей

```bash
py -3.14 -m pip install -r requirements.txt
playwright install chromium
```
---

# 1. Сбор компаний с hh.ru

Сбор компаний по поисковым запросам вакансий, сохранение в базу данных. При первом запуске создаётся база employers.db с таблицами employers и logs. Заполняются поля: `employer_id`, `employer_name`, `vacancy_url`, `hh_url`, `site_url`, `added_at`, `company_info`, `company_info_updated`.

```bash
py -3.14 hh_to_sqlite.py -q "руководитель проектов АСУ ТП" -q "инженер АСУ ТП" -q "программист ПЛК" -q "SCADA" -q "проектировщик АСУ ТП" -q "инженер ПНР" --contact "tg:@promsoftservice" --db employers.db
```
---

# 2. Зеркалирование сайтов

Скачивание сайтов компаний (максимум 12 страниц), сохранение в tar.gz архивы в папку site_archive. Скачивает только те компании, для которых ещё нет архива.

```bash
py -3.14 site_mirror.py --db employers.db --base-dir site_archive --max-pages 12 --concurrency 6
```

Повторно скачать только сайты с ошибками (нескачавшиеся):

```bash
py -3.14 site_mirror.py --db employers.db --base-dir site_archive --retry-errors
```
---

# 3. Извлечение контактов из архивов сайтов

Распаковка архивов, поиск email и телефонов, проверка телефонов через DaData, сохранение в базу.

```bash
py -3.14 extract_contacts.py --db employers.db --archive-dir site_archive --exclude "Другое"
```
---

# 4. Категоризация компаний через DeepSeek API

Автоматическое определение категории компании (приоритетные, средние, исключённые).

Тест на выборке (50 компаний):

```bash
py -3.14 categorize_companies.py --db employers.db --train --sample 50
```

Категоризация 10 компаний (без категории):

```bash
py -3.14 categorize_companies.py --db employers.db --limit 10
```

Категоризация всех компаний:

```bash
py -3.14 categorize_companies.py --db employers.db --all
```

Только приоритетные категории (исключить кадровые агентства и крупные корпорации):

```bash
py -3.14 categorize_companies.py --db employers.db --priority --limit 20
```

Категоризация конкретной компании:

```bash
py -3.14 categorize_companies.py --db employers.db --id 12345
```
---

# 5. Просмотр компаний в базе

Просмотр информации о компаниях из командной строки.

Первые 10 компаний:

```bash
py -3.14 show_companies.py --db employers.db
```

Вывести 5 компаний:

```bash
py -3.14 show_companies.py --db employers.db -n 5
```

Вывести все компании:

```bash
py -3.14 show_companies.py --db employers.db -a
```

Показать компанию по ID:

```bash
py -3.14 show_companies.py --db employers.db --id 12345
```

Поиск по названию:

```bash
py -3.14 show_companies.py --db employers.db --search "газ"
```
---

# 6. Экспорт/импорт данных (employers_editor.py)

## 6.1. Экспорт в Excel

Экспорт данных в Excel для ручного редактирования комментариев.

```bash
py -3.14 employers_editor.py --db employers.db --export employers.xlsx
```

Экспорт с ограничением количества строк:

```bash
py -3.14 employers_editor.py --db employers.db --export employers.xlsx --limit 100
```

## 6.2. Импорт комментариев из Excel

Импортирует только колонку comments из Excel файла обратно в базу.

```bash
py -3.14 employers_editor.py --db employers.db --import-file employers.xlsx
```

## 6.3. Очистка комментариев

Очистка комментариев по списку ID:

```bash
py -3.14 employers_editor.py --db employers.db --clear-comment 245050
py -3.14 employers_editor.py --db employers.db --clear-comment 245050 245051 245052
```

Очистка всех комментариев (с подтверждением):

```bash
py -3.14 employers_editor.py --db employers.db --clear-all-comments
```

Просмотр что будет очищено (без реальной очистки):

```bash
py -3.14 employers_editor.py --db employers.db --clear-all-comments --dry-run
```

## 6.4. Очистка контактов

Очистка email и телефонов по списку ID:

```bash
py -3.14 employers_editor.py --db employers.db --clear-contacts-id 245050
py -3.14 employers_editor.py --db employers.db --clear-contacts-id 245050 245051 245052
```

Очистка всех email и телефонов:

```bash
py -3.14 employers_editor.py --db employers.db --clear-contacts
```

## 6.5. Очистка категорий

Очистка категорий по списку ID:

```bash
py -3.14 employers_editor.py --db employers.db --clear-category 245050
py -3.14 employers_editor.py --db employers.db --clear-category 245050 245051 245052
```

Очистка всех категорий:

```bash
py -3.14 employers_editor.py --db employers.db --clear-all-categories
```

## 6.6. Удаление компании

Удаляет компанию из базы и (опционально) архив сайта.

```bash
py -3.14 employers_editor.py --db employers.db --delete 2009 --archive-dir site_archive
```

## 6.7. Удаление архивов с ошибками

Удаление всех архивов с ошибками:

```bash
py -3.14 employers_editor.py --db employers.db --clean-errors --archive-dir site_archive
```

Только полностью не скачавшиеся (0 страниц):

```bash
py -3.14 employers_editor.py --db employers.db --clean-errors --only-empty --archive-dir site_archive
```

Просмотр без удаления:

```bash
py -3.14 employers_editor.py --db employers.db --clean-errors --dry-run --archive-dir site_archive
```

## 6.8. Удаление архивов по списку ID

```bash
py -3.14 employers_editor.py --db employers.db --clean-ids 2009 2010 2015 --archive-dir site_archive
```
---

# Полная последовательность работы
```bash
# 1. Сбор компаний с hh.ru
py -3.14 hh_to_sqlite.py -q "руководитель проектов АСУ ТП" -q "инженер АСУ ТП" -q "программист ПЛК" -q "SCADA" --contact "tg:@promsoftservice" --db employers.db

# 2. Зеркалирование сайтов
py -3.14 site_mirror.py --db employers.db --base-dir site_archive --max-pages 12 --concurrency 6

# 3. Извлечение контактов из архивов
py -3.14 extract_contacts.py --db employers.db --archive-dir site_archive --exclude "Другое"

# 4. Категоризация всех компаний
py -3.14 categorize_companies.py --db employers.db --all

# 5. Экспорт в Excel для ручного редактирования
py -3.14 employers_editor.py --db employers.db --export employers.xlsx

# 6. Импорт комментариев после редактирования
py -3.14 employers_editor.py --db employers.db --import-file employers.xlsx
```

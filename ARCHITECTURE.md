# Архитектура проекта `Tg_mtproto`

## 1. Назначение системы

Проект автоматизирует цепочку поиска контактных данных по ИНН через внешний Telegram-бот и web-отчёт.

Базовый бизнес-процесс:

`ИНН -> поиск карточки контрагента -> извлечение телефона -> получение краткой сводки по телефону -> сохранение результата`

Проект поддерживает два основных режима работы:

1. Локальный запуск из консоли по одному ИНН или по входному файлу.
2. Автоматическая обработка `csv/xlsx` файлов через собственного Telegram file-bot.


## 2. Системный контекст

В проекте участвуют две разные Telegram-интеграции:

1. `Telethon`-клиент с пользовательской сессией.
   Используется для общения с внешним Telegram-ботом, который возвращает карточки компаний, физлиц и сводки.

2. Telegram Bot API по токену собственного бота.
   Используется только для file-bot сценария: принять файл в чате, отправить статусы, вернуть готовый `xlsx`.

Дополнительно для сценария ИП используется:

3. `Playwright`.
   Нужен для открытия web-отчёта по ссылке из Telegram-бота и парсинга содержимого страницы.


## 3. Высокоуровневая схема

```text
Пользователь / оператор
    |
    +--> Локальный CLI
    |      |
    |      +--> run_pipeline.py
    |              |
    |              +--> get_director_phone.py
    |              +--> get_ip_phone.py
    |              +--> get_phone_summary.py
    |
    +--> Telegram file-bot
           |
           +--> tg_file_pipeline_bot.py
                   |
                   +--> run_pipeline.py
                           |
                           +--> сценарии поиска и обогащения


Внешние зависимости:

Telethon user session --> внешний Telegram-бот
Playwright --> web-страница отчёта
Telegram Bot API --> чат, куда загружают входные файлы
```


## 4. Основные компоненты

### 4.1. Оркестратор пайплайна

Файл: `run_pipeline.py`

Ответственность:

- загрузка входного `csv/xlsx`;
- определение типа контрагента;
- запуск правильного сценария поиска телефона;
- запуск обогащения по найденному телефону;
- сбор итоговой строки результата;
- запись результатов в `pipeline_results.csv/xlsx`.

Ключевые функции:

- `load_runtime_config()` - собирает runtime-конфиг из `.env`;
- `load_input_rows()` - читает входной файл;
- `detect_entity_type()` - определяет `company` или `ip`;
- `resolve_row()` - главный orchestration-step для одной строки;
- `append_pipeline_result()` - сохраняет итог в `csv/xlsx`.


### 4.2. Сценарий для компаний

Файл: `get_director_phone.py`

Ответственность:

- отправить `/inn <ИНН>` внешнему Telegram-боту;
- дождаться карточки компании;
- распарсить компанию и директора;
- рекурсивно обойти inline-кнопки;
- дойти до карточки физлица;
- извлечь телефон, email и ИНН физлица;
- при standalone-режиме сохранить результат в `results.csv/xlsx`.

Особенности реализации:

- используется очередь входящих событий `Telethon`;
- есть защита от циклов через `seen_cards`;
- глубина обхода ограничена `MAX_DEPTH`;
- результаты представлены как `QueryState`.


### 4.3. Сценарий для ИП

Файл: `get_ip_phone.py`

Ответственность:

- отправить `/inn <ИНН>` внешнему Telegram-боту;
- дождаться сообщения со ссылкой на полный web-отчёт;
- выбрать кнопку со ссылкой;
- открыть web-страницу через `Playwright`;
- распарсить первую найденную личность и её контакты;
- при необходимости сохранить debug-артефакты.

Особенности реализации:

- работа идёт не по Telegram-карточкам, а по web-странице;
- есть попытка чинить битую кириллицу через `maybe_fix_mojibake()`;
- при неудачном парсинге страница сохраняется в `report_debug/`.


### 4.4. Сводка по телефону

Файл: `get_phone_summary.py`

Ответственность:

- отправить номер телефона внешнему Telegram-боту;
- дождаться краткой сводки;
- распарсить поля профиля;
- извлечь Telegram, email и ссылки на соцсети;
- при standalone-режиме сохранить результат в `phone_summary_results.csv/xlsx`.

Особенности реализации:

- логика парсинга построена вокруг labeled fields;
- ссылки извлекаются из Telegram entities;
- результат хранится в `PhoneSummary` и `QueryState`.


### 4.5. Telegram file-bot

Файл: `tg_file_pipeline_bot.py`

Ответственность:

- слушать обновления собственного Telegram-бота;
- принимать только `csv/xlsx/xlsm` файлы из разрешённого чата;
- скачивать входной файл;
- запускать пайплайн по каждой строке;
- обновлять статус обработки в чате;
- отправлять обратно итоговый `xlsx`.

Особенности реализации:

- использует Telegram Bot API напрямую через `urllib`;
- хранит каждый запуск в отдельной job-папке;
- рабочий каталог по умолчанию: `tg_bot_jobs/`;
- не использует webhook, работает long polling'ом.


### 4.6. Авторизация и служебные утилиты

Файлы:

- `qr_login.py` - создаёт пользовательскую `Telethon`-сессию через QR;
- `util_print_tg_chat_id.py` - помогает узнать `ID_TG_CHAT` для file-bot режима.


## 5. Потоки выполнения

### 5.1. Поток для одной строки входного файла

```text
run_pipeline.resolve_row()
    |
    +--> detect_entity_type()
            |
            +--> company -> get_director_phone.run_single_query()
            |
            +--> ip -> get_ip_phone.run_single_query()
    |
    +--> если телефон найден:
            |
            +--> get_phone_summary.run_single_query()
    |
    +--> build_pipeline_row()
    |
    +--> append_pipeline_result()
```


### 5.2. Поток file-bot обработки

```text
Пользователь отправляет файл в Telegram-чат
    |
    +--> tg_file_pipeline_bot.py принимает document
    |
    +--> файл сохраняется в tg_bot_jobs/<job_id>/
    |
    +--> process_input_file()
    |
    +--> run_pipeline.resolve_row() по каждой строке
    |
    +--> результат пишется в <input_name>_result.csv/xlsx
    |
    +--> готовый xlsx отправляется обратно в чат
```


## 6. Контракты данных

### 6.1. Входной файл

Поддерживаемые форматы:

- `.csv`
- `.xlsx`
- `.xlsm`

Ожидаемая структура:

- 1 столбец: название контрагента;
- 2 столбец: ИНН.

Поддержка заголовка есть. Оркестратор пытается автоматически определить, есть ли header в первых строках.


### 6.2. Внутренняя модель строки

`run_pipeline.py` использует `InputRow`:

- `source_row`
- `source_name`
- `source_inn`


### 6.3. Итог пайплайна

Основной consolidated output:

- `source_row`
- `source_name`
- `source_inn`
- `entity_type`
- `phone_source`
- `phone_lookup_status`
- `phone_lookup_message`
- `found_person`
- `found_phone`
- `found_email`
- `found_person_inn`
- `summary_status`
- `summary_message`
- `summary_fio`
- `summary_birth_date`
- `summary_age`
- `summary_telegram`
- `summary_email`
- `summary_inn`
- `vk_text`
- `vk_urls`
- `instagram_text`
- `instagram_urls`
- `ok_text`
- `ok_urls`
- `pipeline_status`
- `pipeline_message`


## 7. Конфигурация

### 7.1. Обязательные переменные

- `API_ID`
- `API_HASH`
- `SESSION_NAME`
- `BOT`

Для file-bot режима дополнительно:

- `TG_BOT_TOKEN`
- `ID_TG_CHAT`


### 7.2. Полезные optional переменные

- `LOG_LEVEL`
- `PLAYWRIGHT_HEADLESS`
- `REPORT_DEBUG_DIR`
- `RESULTS_CSV`
- `RESULTS_XLSX`
- `PHONE_SUMMARY_RESULTS_CSV`
- `PHONE_SUMMARY_RESULTS_XLSX`
- `PIPELINE_RESULTS_CSV`
- `PIPELINE_RESULTS_XLSX`
- `PIPELINE_STEP_DELAY_SECONDS`
- `PIPELINE_ROW_DELAY_SECONDS`
- `TG_BOT_JOBS_DIR`


## 8. Структура файлов проекта

```text
.
├── qr_login.py
├── util_print_tg_chat_id.py
├── get_director_phone.py
├── get_ip_phone.py
├── get_phone_summary.py
├── run_pipeline.py
├── tg_file_pipeline_bot.py
├── requirements.txt
├── readme.md
├── report_debug/        # debug web-страниц
├── tg_bot_jobs/         # jobs file-bot режима
└── *.session            # локальная Telethon-сессия
```


## 9. Технические решения

### 9.1. Почему orchestration вынесен в `run_pipeline.py`

Это правильное разделение ответственности:

- сценарии поиска остаются независимыми;
- общая бизнес-цепочка находится в одном месте;
- file-bot не дублирует основную логику;
- проще расширять пайплайн новыми шагами.


### 9.2. Почему file-bot не использует Telethon

Собственный file-bot отделён от пользовательской сессии:

- user session нужна для общения с внешним ботом;
- bot token нужен для приёма файлов и ответа в чат;
- это два разных канала доступа и разные зоны ответственности.

Такое разделение соответствует хорошей интеграционной практике: не смешивать automation account и bot account в одну абстракцию без необходимости.


### 9.3. Почему результаты пишутся в `csv` и `xlsx`

Это удобно для операторского сценария:

- `csv` подходит для автоматической обработки;
- `xlsx` удобно отдавать пользователю как итоговый файл;
- формат прозрачен и не требует отдельной БД для базового сценария.


## 10. Наблюдаемые ограничения текущей реализации

Это не критика, а фиксирование текущего состояния системы.

### 10.1. Архитектура пока скриптовая

Код хорошо решает прикладную задачу, но пока не оформлен как пакет с явными слоями:

- `config`
- `domain`
- `adapters`
- `application services`
- `storage`

Сейчас эти роли частично смешаны внутри отдельных скриптов.


### 10.2. Повторяется служебный код

В нескольких файлах повторяются:

- `load_dotenv()`
- `get_required_env()`
- настройка логирования
- append в `csv/xlsx`

С точки зрения лучших практик это кандидаты на вынос в общие модули:

- `config.py`
- `logging_setup.py`
- `result_writer.py`
- `telegram_bot_api.py`


### 10.3. Парсинг жёстко привязан к текстовым шаблонам

Система сильно зависит от формата ответов внешнего Telegram-бота и структуры web-страниц.

Это значит:

- любое изменение формата ответа может ломать парсер;
- особенно уязвимы regex и текстовые маркеры;
- полезно иметь sample fixtures и smoke-тесты на реальные ответы.


### 10.4. Нет автоматических тестов

Для production-quality поддержки здесь логично добавить хотя бы:

- unit-тесты на парсеры карточек;
- unit-тесты на определение типа контрагента;
- fixture-based тесты на phone summary;
- smoke-тесты на обработку небольшого входного файла.


## 11. Как бы это выглядело в более зрелой архитектуре

Если развивать проект по best practices, естественная целевая структура могла бы быть такой:

```text
src/
  app/
    pipeline_service.py
    file_bot_service.py
  domain/
    models.py
    statuses.py
  integrations/
    telethon_client.py
    telegram_bot_api.py
    report_scraper.py
  parsers/
    company_parser.py
    person_parser.py
    phone_summary_parser.py
    report_parser.py
  infrastructure/
    config.py
    logging.py
    csv_writer.py
    xlsx_writer.py
  cli/
    run_pipeline.py
    run_file_bot.py
```

Но для текущего объёма задачи существующая структура со скриптами остаётся понятной и рабочей.


## 12. Практический способ быстро объяснить проект в новом чате

Можно использовать такое краткое описание:

> Это Python-проект, который через Telethon общается с внешним Telegram-ботом и по ИНН находит телефон контрагента. Для компаний телефон ищется через обход Telegram-карточек, для ИП через web-отчёт, открываемый Playwright. Если телефон найден, проект делает второй запрос и получает краткую сводку по номеру. Главный оркестратор `run_pipeline.py`, а `tg_file_pipeline_bot.py` принимает `csv/xlsx` в Telegram-чате, прогоняет пайплайн по строкам и отправляет обратно итоговый `xlsx`.


## 13. Точка входа для нового разработчика

Рекомендуемый порядок изучения:

1. `run_pipeline.py`
2. `get_director_phone.py`
3. `get_ip_phone.py`
4. `get_phone_summary.py`
5. `tg_file_pipeline_bot.py`
6. `qr_login.py`

Именно в таком порядке проще всего понять:

- общую orchestration-логику;
- различия между company и ip flow;
- где заканчивается поиск телефона и начинается enrichment;
- как локальный пайплайн превращён в Telegram file-bot.

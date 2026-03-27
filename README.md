# ABCP -> Bitrix24 Garage Sync

Сервис загружает данные гаража из ABCP в локальную SQLite и синхронизирует их в пользовательские поля уже существующих сделок Bitrix24.

## Что делает проект

- Загружает "гараж" ABCP за заданный период.
- Хранит сырые и нормализованные данные в SQLite.
- Для каждого пользователя берёт только последнюю запись гаража.
- Находит сделку Bitrix24 по `UF_B24_DEAL_ABCP_USER_ID` в заданной воронке.
- Обновляет только реально изменившиеся UF-поля.
- Пишет сервисные логи и отдельный JSONL-аудит исходящих HTTP-запросов.

## Что проект не делает

- Не создаёт сделки в Bitrix24.
- Не создаёт новые UF-поля в Bitrix24.
- Не синхронизирует произвольные `UF_B24_DEAL_*`, если для них нет явного маппинга в коде.
- Не использует часть legacy-переменных из старых конфигураций, даже если они заданы в `.env`.

## Поток данных

1. CLI определяет период загрузки.
2. ABCP-клиент запрашивает данные гаража и режет большой диапазон по годам.
3. Ответы сохраняются в таблицу `garage` через upsert по `id`.
4. Из SQLite выбирается последняя запись гаража на пользователя.
5. Сервис собирает набор UF-полей по env-маппингу.
6. Если по кэшу видно, что данные уже синхронизированы, удалённый вызов в Bitrix пропускается.
7. Если сделка найдена, обновляются только изменившиеся поля.
8. Результат сохраняется в `sync_status` и `sync_audit`.

## Требования

- Python 3.11+
- доступ к ABCP API
- входящий webhook Bitrix24 с правами на чтение и обновление сделок

Зависимости из [requirements.txt](requirements.txt):

- `python-dotenv`
- `requests`
- `urllib3`

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Минимально заполните:

- `ABCP_BASE_URL`
- `ABCP_USERLOGIN`
- `ABCP_USERPSW`
- `B24_WEBHOOK_URL`
- `B24_DEAL_CATEGORY_ID_USERS`
- `UF_B24_DEAL_ABCP_USER_ID`
- нужные `UF_B24_DEAL_GARAGE_*`, если хотите переносить поля гаража в Bitrix24

Запуск:

```bash
python -m abcp_b24_garage_sync
```

Точка входа:

- [__main__.py](abcp_b24_garage_sync/__main__.py)
- [main.py](abcp_b24_garage_sync/main.py)

## CLI

```bash
python -m abcp_b24_garage_sync [--from YYYY-MM-DD|ISO] [--to YYYY-MM-DD|ISO] [--only-store] [--only-sync] [--user ID] [--loop-every MINUTES]
```

Поддерживаемые режимы:

- `python -m abcp_b24_garage_sync`
  Автоматический инкрементальный режим по курсору `fetch_state`.

- `python -m abcp_b24_garage_sync --from 2026-01-01 --to 2026-03-27`
  Явный исторический прогон.

- `python -m abcp_b24_garage_sync --only-store`
  Только загрузка из ABCP в SQLite, без записи в Bitrix24.

- `python -m abcp_b24_garage_sync --only-sync`
  Только синхронизация из уже сохранённых локально данных в Bitrix24.

- `python -m abcp_b24_garage_sync --only-sync --user 123456`
  Синхронизация одного пользователя из локальной SQLite.

- `python -m abcp_b24_garage_sync --from 2026-03-27T00:00:00 --to 2026-03-27T12:00:00 --user 123456`
  Сначала загрузка диапазона из ABCP, затем синхронизация только одного пользователя.

- `python -m abcp_b24_garage_sync --loop-every 30`
  Запуск цикла каждые 30 минут внутри одного процесса.

Важно:

- `--from` и `--to` должны передаваться только вместе.
- `--loop-every` принимает минуты, не секунды.
- `--user` влияет только на этап синхронизации в Bitrix24, а не на upstream-запрос в ABCP.

## Как определяется период загрузки

Если `--from` и `--to` не заданы:

- сервис читает `fetch_state`
- если там есть `lastSuccessTo`, следующий интервал начинается с `lastSuccessTo - ABCP_INCREMENTAL_OVERLAP_MINUTES`
- если курсора ещё нет, старт берётся с `1 января` года `now.year - ABCP_INITIAL_LOOKBACK_YEARS`

Эта логика реализована в [main.py](abcp_b24_garage_sync/main.py).

## Конфигурация

### Где ищется `.env`

Сервис ищет файл окружения в таком порядке:

1. `ABCP_B24_ENV_FILE`
2. `<project_root>/.env`
3. `<project_root>/../.env`

Поддерживается legacy-алиас `ABC_B24_ENV_FILE`.

### Поддерживаемые переменные окружения

#### ABCP

- `ABCP_BASE_URL`
  Базовый URL ABCP. Нормальные варианты: `/cp/garage`, `/cp/garage/`, `/cp/garage/list`.
  Если по ошибке указан `/cp/users`, клиент автоматически попробует исправить его на `/cp/garage`.

- `ABCP_USERLOGIN`
  Логин ABCP.

- `ABCP_USERPSW`
  Пароль или токен ABCP.

- `ABCP_INITIAL_LOOKBACK_YEARS`
  Сколько лет захватывает первый авто-запуск без `fetch_state`. Значение по умолчанию: `2`.

- `ABCP_INCREMENTAL_OVERLAP_MINUTES`
  Overlap между последовательными авто-загрузками. Значение по умолчанию: `5`.

- `ABCP_LIMIT`
  Сейчас читается из env, но текущей версией кода не используется.

#### Bitrix24

- `B24_WEBHOOK_URL`
  Базовый URL входящего webhook Bitrix24. Код сам добавляет завершающий `/`, если его нет.

- `B24_DEAL_CATEGORY_ID_USERS`
  ID воронки, в которой искать сделки пользователей.

- `B24_DEAL_TITLE_PREFIX`
  Префикс заголовка сделки. Сейчас используется как конфигурация, но в текущей версии сделки не создаются.

- `UF_B24_DEAL_ABCP_USER_ID`
  Обязательный UF-код, по которому сервис ищет сделку Bitrix24.

- `B24_TZ_OFFSET`
  Смещение часового пояса для datetime-полей Bitrix24. Значение по умолчанию: `+03:00`.

- `B24_VERIFY_UPDATES`
  Если `true`, после `crm.deal.update` сервис перечитывает сделку и проверяет, что поля реально применились. По умолчанию: `false`.

- `B24_USE_BATCH`
  Разрешает batched-вызовы Bitrix24 для поиска, чтения и обновлений. По умолчанию: `true`.

- `B24_BATCH_SIZE`
  Размер батча Bitrix24. По умолчанию: `25`.

#### Маппинг ABCP -> UF Bitrix24

Поддерживаются только эти env-переменные:

- `UF_B24_DEAL_GARAGE_ID`
- `UF_B24_DEAL_GARAGE_USER_ID`
- `UF_B24_DEAL_GARAGE_NAME`
- `UF_B24_DEAL_GARAGE_COMMENT`
- `UF_B24_DEAL_GARAGE_YEAR`
- `UF_B24_DEAL_GARAGE_VIN`
- `UF_B24_DEAL_GARAGE_FRAME`
- `UF_B24_DEAL_GARAGE_MILEAGE`
- `UF_B24_DEAL_GARAGE_MANUFACTURER_ID`
- `UF_B24_DEAL_GARAGE_MANUFACTURER`
- `UF_B24_DEAL_GARAGE_MODEL_ID`
- `UF_B24_DEAL_GARAGE_MODEL`
- `UF_B24_DEAL_GARAGE_MODIFICATION_ID`
- `UF_B24_DEAL_GARAGE_MODIFICATION`
- `UF_B24_DEAL_GARAGE_DATE_UPDATED`
- `UF_B24_DEAL_GARAGE_VEHICLE_REG_PLATE`

Если какая-то переменная не задана, соответствующее поле просто не будет синхронизироваться.

#### Хранилище и пути

- `ABCP_B24_DATA_DIR`
  Корневая директория runtime-артефактов. Если не задана, используется корень проекта.

- `SQLITE_PATH`
  Путь к SQLite. Если путь относительный, он разрешается относительно `ABCP_B24_DATA_DIR`. Значение по умолчанию: `abcp_b24.s3db`.

- `LOG_DIR`
  Каталог логов. Если путь относительный, он тоже разрешается относительно `ABCP_B24_DATA_DIR`. По умолчанию: `logs`.

- `LOG_FILE`
  Имя основного сервисного лога. По умолчанию: `service.log`.

- `LOG_LEVEL`
  Уровень стандартного логирования Python. По умолчанию: `INFO`.

#### HTTP и аудит запросов

- `REQUESTS_TIMEOUT`
  HTTP timeout в секундах. По умолчанию: `20`.

- `REQUESTS_RETRIES`
  Количество retry для временных ошибок. По умолчанию: `3`.

- `REQUESTS_RETRY_BACKOFF`
  Backoff между retry. По умолчанию: `1.5`.

- `RATE_LIMIT_SLEEP`
  Пауза после успешных HTTP-вызовов в секундах. По умолчанию: `0.2`.

- `REQUEST_AUDIT_ENABLED`
  Включает отдельный JSONL-аудит исходящих HTTP-запросов. По умолчанию: `true`.

#### Поведение синхронизации

- `SYNC_OVERWRITE_DEFAULT`
  Разрешение на перезапись полей по умолчанию. По умолчанию: `true`.

- `SYNC_OVERWRITE_FIELDS`
  Точечные переопределения на уровне поля ABCP. Пример:
  `{"vin": false, "vehicleRegPlate": false}`

- `SYNC_PAUSE_BETWEEN_USERS`
  Дополнительная пауза между пользователями в секундах. По умолчанию: `0`.

- `SYNC_PAUSE_BETWEEN_DEALS`
  Дополнительная пауза между сделками в секундах. По умолчанию: `0`.

#### Технические и deploy-переменные

- `ABCP_B24_PROJECT_ROOT`
  Внутренний override корня проекта.

- `ABCP_B24_LOOP_LIMIT`
  Ограничение числа циклов для `--loop-every`. Полезно для тестов и отладки.

Поддерживаются legacy-алиасы:

- `ABC_B24_DATA_DIR`
- `ABC_B24_ENV_FILE`

### Legacy-переменные, которые сейчас игнорируются

Если в старых конфигурациях присутствуют такие переменные, текущая версия проекта их не использует:

- `ABCP_TIMEZONE`
- `ABCP_INITIAL_IMPORT_MODE`
- `ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES`
- `ABCP_INCREMENTAL_LOOKBACK_MINUTES`
- `ABCP_INCREMENTAL_MAX_WINDOW_MINUTES`
- `B24_OUT_TZ_ISO`
- `B24_DEAL_STAGE_NEW_USERS`
- `SYNC_INTERVAL_SECONDS`

Также не сработают произвольные `UF_B24_DEAL_*`, если они не входят в список поддерживаемого маппинга выше.

## Локальное хранилище

SQLite-логика находится в [db.py](abcp_b24_garage_sync/db.py).

Таблицы:

- `garage`
  Последний известный срез гаража ABCP. Upsert по `id`.

- `sync_status`
  Последнее состояние синхронизации по пользователю: `dealId`, `sourceGarageId`, `sourceDateUpdated`, `sourcePayloadHash`, `lastResult`, `lastError`.

- `sync_audit`
  История всех попыток синхронизации по пользователям.

- `fetch_state`
  Курсор инкрементальной загрузки из ABCP.

## Как работает синхронизация в Bitrix24

Логика реализована в [sync_service.py](abcp_b24_garage_sync/sync_service.py).

Ключевые правила:

- В работу берётся только самая свежая строка `garage` на пользователя.
- Если по `sync_status` видно, что та же полезная нагрузка уже была синхронизирована, удалённый вызов пропускается.
- Если сделка по `UF_B24_DEAL_ABCP_USER_ID` не найдена, результат сохраняется как `skipped`.
- Перед обновлением сервис читает текущие значения полей и отправляет в Bitrix только diff.
- При включённом `B24_VERIFY_UPDATES` сервис дополнительно проверяет, что поля реально применились.
- При ошибках batch-режима есть fallback на одиночные вызовы.

Текущая версия не делает `crm.deal.add`; она работает только с существующими сделками.

## HTTP-клиенты

### ABCP

Клиент: [abcp_client.py](abcp_b24_garage_sync/abcp_client.py)

Особенности:

- поддерживает retry через `requests` + `urllib3`
- трактует часть `404` и `errorCode=301/404` как "пустой интервал", а не как ошибку
- разбивает длинный период на годовые slice'ы через [util.py](abcp_b24_garage_sync/util.py)

### Bitrix24

Клиент: [b24_client.py](abcp_b24_garage_sync/b24_client.py)

Особенности:

- пакетные вызовы `batch` для find/get/update
- нормализация типов UF перед обновлением
- маскировка секретов в логах

## Логи

Логирование настраивается в [log_setup.py](abcp_b24_garage_sync/log_setup.py).

Создаются два вида логов:

- `service.log`
  Обычный текстовый лог сервиса. Ротируется по дням, хранит `7` backup-файлов.

- `http-requests-YYYY-MM-DD.jsonl`
  Аудит исходящих HTTP-запросов. Каждая строка — отдельный JSON-объект.

Поля audit-записи:

- `timestamp`
- `service`
- `request.method`
- `request.url`
- `request.headers`
- `request.payload`
- `response.ok`
- `response.outcome`
- `response.status_code`
- `response.duration_ms`
- `response.content_length`
- `response.body_preview`
- `response.error`
- `meta`

Важно:

- секреты маскируются, но полезная нагрузка и фрагменты ответа всё равно могут содержать персональные или бизнес-данные
- для audit-файлов в текущей версии нет автоматической очистки старых дней

## Примеры проверки

Проверить только загрузку ABCP:

```bash
python -m abcp_b24_garage_sync --only-store
```

Проверить синхронизацию из уже заполненной SQLite по одному пользователю:

```bash
python -m abcp_b24_garage_sync --only-sync --user 123456
```

Запустить один цикл с явным диапазоном:

```bash
python -m abcp_b24_garage_sync --from 2026-03-01 --to 2026-03-27
```

Ограничить loop-режим двумя итерациями для отладки:

```bash
ABCP_B24_LOOP_LIMIT=2 python -m abcp_b24_garage_sync --loop-every 1
```

## Развёртывание на сервере

Системный unit: [deploy/systemd/abcp-b24-garage-sync.service](deploy/systemd/abcp-b24-garage-sync.service)

Bootstrap-скрипт: [deploy/remote_bootstrap.sh](deploy/remote_bootstrap.sh)

Шаги:

1. Скопируйте проект в `/opt/abcp-b24-garage-sync/current`.
2. Создайте виртуальное окружение в `/opt/abcp-b24-garage-sync/venv`.
3. Создайте `/opt/abcp-b24-garage-sync/current/.env`.
4. Установите systemd unit:

```bash
sudo cp deploy/systemd/abcp-b24-garage-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.service
```

Либо используйте:

```bash
sudo bash deploy/remote_bootstrap.sh
```

Полезные команды:

```bash
sudo systemctl restart abcp-b24-garage-sync.service
sudo systemctl status abcp-b24-garage-sync.service --no-pager
journalctl -u abcp-b24-garage-sync.service -f
```

## Скрипты из `scripts/`

Служебные примеры:

- [run.sh](scripts/run.sh)
- [run.bat](scripts/run.bat)
- [abcp-b24-garage-sync.service.example](scripts/abcp-b24-garage-sync.service.example)

## Структура проекта

- [abcp_client.py](abcp_b24_garage_sync/abcp_client.py) — загрузка из ABCP
- [b24_client.py](abcp_b24_garage_sync/b24_client.py) — вызовы Bitrix24
- [sync_service.py](abcp_b24_garage_sync/sync_service.py) — логика синхронизации
- [db.py](abcp_b24_garage_sync/db.py) — SQLite и курсоры
- [request_audit.py](abcp_b24_garage_sync/request_audit.py) — JSONL-аудит HTTP
- [log_setup.py](abcp_b24_garage_sync/log_setup.py) — настройка логов
- [config.py](abcp_b24_garage_sync/config.py) — чтение env

## Разработка и проверки

Основной набор тестов:

```bash
python -m unittest discover -s tests -v
```

Полезная дополнительная проверка:

```bash
python -m compileall .\abcp_b24_garage_sync .\tests
```

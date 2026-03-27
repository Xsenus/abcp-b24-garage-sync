# ABCP -> Bitrix24 Garage Sync

Сервис синхронизирует данные гаража из ABCP в пользовательские поля сделок Bitrix24.

Основной режим работы теперь инкрементальный:
- если `--from/--to` не указаны, сервис берёт период из локального курсора `fetch_state`
- при первом автоматическом запуске курсор инициализируется от `1 января` года, рассчитанного по `ABCP_INITIAL_LOOKBACK_YEARS`
- каждый следующий автоматический запуск дочитывает только новый хвост с overlap `ABCP_INCREMENTAL_OVERLAP_MINUTES`

Для полного исторического backfill можно по-прежнему явно передавать `--from` и `--to`.

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Однократный инкрементальный запуск:

```bash
python -m abcp_b24_garage_sync
```

Явный исторический прогон:

```bash
python -m abcp_b24_garage_sync --from 2024-01-01 --to 2026-03-26
```

Только загрузка из ABCP в локальную SQLite:

```bash
python -m abcp_b24_garage_sync --only-store
```

Непрерывный режим:

```bash
python -m abcp_b24_garage_sync --loop-every 30
```

Поддерживаются режимы:
- `--only-store`
- `--only-sync`
- `--only-sync --user <ID>`

## Что изменилось в логике

Сервис уменьшает количество запросов за счёт нескольких уровней оптимизации:
- хранит курсор последней успешной загрузки из ABCP в `fetch_state`
- кэширует `dealId` в `sync_status` и не ищет сделку в Bitrix заново без необходимости
- сохраняет `sourcePayloadHash` и пропускает Bitrix, если набор полей уже синхронизирован
- использует batched вызовы Bitrix для поиска сделок, чтения текущих значений и массовых обновлений

Это особенно важно для loop-режима через systemd: повторный запуск больше не перечитывает один и тот же исторический диапазон на каждом цикле.

## Конфигурация

### Где лежит `.env`

Сервис ищет файл окружения в таком порядке:
1. путь из `ABCP_B24_ENV_FILE`
2. `<project_root>/.env`
3. `<project_root>/../.env`

Для production рекомендуется хранить файл в:

```text
/opt/abcp-b24-garage-sync/current/.env
```

Скрипт `deploy/remote_bootstrap.sh` при необходимости переносит старый общий файл `/opt/abcp-b24-garage-sync/.env` в новый путь.

### Важные переменные

`ABCP_INITIAL_LOOKBACK_YEARS`
- сколько лет захватывает первый автоматический запуск, если курсора ещё нет

`ABCP_INCREMENTAL_OVERLAP_MINUTES`
- overlap между последовательными автоматическими загрузками из ABCP

`ABCP_B24_DATA_DIR`
- базовая директория для SQLite, fetch state и логов

`B24_USE_BATCH`
- включает пакетные вызовы Bitrix24

`B24_BATCH_SIZE`
- размер батча для операций `find/get/update` в Bitrix24

`B24_VERIFY_UPDATES`
- если `true`, после обновления сервис повторно читает сделку и проверяет, что поля реально применились
- по умолчанию `false`, чтобы не добавлять лишние запросы

`REQUEST_AUDIT_ENABLED`
- если `true`, сервис пишет отдельный JSONL-аудит всех исходящих HTTP-запросов
- по умолчанию `true`; файл создаётся по дням в `LOG_DIR` с именем `http-requests-YYYY-MM-DD.jsonl`

`SYNC_OVERWRITE_FIELDS`
- точечное управление перезаписью отдельных полей
- пример: `{"vin": false, "vehicleRegPlate": false}`

Если какой-то `UF_B24_DEAL_*` оставлен пустым, соответствующее поле просто не синхронизируется. Это нормальный способ отключить часть маппинга, например `comment`.

## Хранилище и логи

По умолчанию сервис создаёт:
- SQLite по пути `SQLITE_PATH` внутри `ABCP_B24_DATA_DIR`
- лог-файл `LOG_FILE` внутри `LOG_DIR`
- дневной аудит исходящих HTTP-запросов `http-requests-YYYY-MM-DD.jsonl` внутри `LOG_DIR`

В базе используются таблицы:
- `garage` — последние данные из ABCP
- `sync_status` — текущее состояние синка по пользователю
- `sync_audit` — аудит всех попыток синка
- `fetch_state` — курсор инкрементальной загрузки из ABCP

## Развёртывание на сервере

1. Скопируйте проект в `/opt/abcp-b24-garage-sync/current`.
2. Создайте виртуальное окружение в `/opt/abcp-b24-garage-sync/venv`.
3. Заполните `/opt/abcp-b24-garage-sync/current/.env`.
4. Установите сервис:

```bash
sudo cp deploy/systemd/abcp-b24-garage-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.service
```

Или используйте bootstrap-скрипт:

```bash
sudo bash deploy/remote_bootstrap.sh
```

Сервис запускает:

```bash
python -m abcp_b24_garage_sync --loop-every 30
```

То есть каждые 30 минут выполняется новый инкрементальный цикл внутри одного процесса.

Полезные команды:

```bash
sudo systemctl restart abcp-b24-garage-sync.service
sudo systemctl status abcp-b24-garage-sync.service --no-pager
journalctl -u abcp-b24-garage-sync.service -f
```

## Примеры безопасной проверки

Проверить только загрузку из ABCP, не трогая Bitrix:

```bash
python -m abcp_b24_garage_sync --only-store
```

Проверить работу на одном пользователе:

```bash
python -m abcp_b24_garage_sync --only-sync --user 123456
```

Важно: `--only-sync` уже может писать в Bitrix24.

## Скрипты из `scripts/`

Служебные примеры в каталоге `scripts/` тоже переведены на актуальную схему:
- `run.sh`
- `run.bat`
- `abcp-b24-garage-sync.service.example`

Они используют автоматический инкрементальный режим, а не фиксированный исторический диапазон.

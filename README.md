
# ABCP → Bitrix24 Garage Sync (Python)

Сервис синхронизации данных «гаража» из ABCP в сделки Bitrix24. Приложение можно запускать разово или в непрерывном режиме через systemd.

## Быстрый старт для разработки

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                  # заполните креды ABCP/B24

# однократный прогон (дату можно не указывать — main.py подставит 2024-01-01..2025-12-31)
python -m abcp_b24_garage_sync --from 2020-01-01 --to 2025-12-31

# непрерывный режим (повтор каждые 30 минут; период можно не указывать — подставится 2024-01-01..2025-12-31)
python -m abcp_b24_garage_sync --loop-every 30 --from 2024-01-01 --to 2025-12-31
```

Поддерживаются режимы `--only-store`, `--only-sync`, `--only-sync --user <ID>`.

## Конфигурация окружения

* `.env` располагается в каталоге проекта (`current/.env`). Скрипт `deploy/remote_bootstrap.sh` автоматически перенесёт файл из прежнего общего расположения (`/opt/abcp-b24-garage-sync/.env`), если вы использовали старую схему. Путь можно явно указать через переменную `ABCP_B24_ENV_FILE`.
* `ABCP_B24_DATA_DIR` определяет базовую директорию для базы и логов. По умолчанию — корень проекта. Любые относительные пути (например, `SQLITE_PATH`, `LOG_DIR`) интерпретируются относительно этого каталога.
* Логи пишутся в консоль и файл (по умолчанию `logs/service.log`, ротация раз в сутки, хранится 7 файлов). Переопределяется переменными `LOG_DIR` и `LOG_FILE`.

См. `.env.example` для подсказок по переменным.

## Развёртывание на сервер (systemd)

1. Скопируйте проект (rsync/git) в `/opt/abcp-b24-garage-sync/current` и создайте Python‑виртуальное окружение в `/opt/abcp-b24-garage-sync/venv`.
2. Заполните `/opt/abcp-b24-garage-sync/current/.env` (можно создать из `.env.example`).
3. Установите systemd-юнит из `deploy/systemd/`:

```bash
sudo cp deploy/systemd/abcp-b24-garage-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.service
```

Сервис запускает `python -m abcp_b24_garage_sync --loop-every 30` из виртуального окружения и ожидает, что зависимости уже установлены (`pip install -r requirements.txt`). Повторный запуск выполняется каждые 30 минут внутри одного процесса, поэтому `systemctl stop abcp-b24-garage-sync.service` действительно останавливает синхронизацию и больше не будет перезапускать её таймер.

Для разового запуска:

```bash
sudo systemctl start abcp-b24-garage-sync.service
journalctl -u abcp-b24-garage-sync.service -f
```

Логи также доступны в `${LOG_DIR:-<data_dir>/logs}/service.log`.

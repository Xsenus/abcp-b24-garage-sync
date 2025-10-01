
# ABCP → Bitrix24 Garage Sync (Python)

Сервис синхронизации данных «гаража» из ABCP в сделки Bitrix24. Приложение можно запускать разово или по расписанию через systemd.

## Быстрый старт для разработки

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                  # заполните креды ABCP/B24

# однократный прогон (дату можно не указывать — main.py подставит 2024-01-01..2025-12-31)
python -m abcp_b24_garage_sync --from 2020-01-01 --to 2025-12-31
```

Поддерживаются режимы `--only-store`, `--only-sync`, `--only-sync --user <ID>`.

## Конфигурация окружения

* `.env` можно размещать как в каталоге проекта (`current/.env`), так и на уровень выше (`/opt/abcp-b24-garage-sync/.env`). Первый найденный файл будет загружен автоматически. Путь можно явно указать через переменную `ABCP_B24_ENV_FILE`.
* `ABCP_B24_DATA_DIR` определяет базовую директорию для базы и логов. По умолчанию — корень проекта. Любые относительные пути (например, `SQLITE_PATH`, `LOG_DIR`) интерпретируются относительно этого каталога.
* Логи пишутся в консоль и файл (по умолчанию `logs/service.log`, ротация раз в сутки, хранится 7 файлов). Переопределяется переменными `LOG_DIR` и `LOG_FILE`.

См. `.env.example` для подсказок по переменным.

## Развёртывание на сервер (systemd + таймер)

1. Скопируйте проект (rsync/git) в `/opt/abcp-b24-garage-sync/current` и создайте Python‑виртуальное окружение в `/opt/abcp-b24-garage-sync/venv`.
2. Заполните `/opt/abcp-b24-garage-sync/.env` (можно создать из `.env.example`) и при необходимости выполните `ln -sf ../.env current/.env`.
3. Установите systemd-юниты из `deploy/systemd/`:

```bash
sudo cp deploy/systemd/abcp-b24-garage-sync.service /etc/systemd/system/
sudo cp deploy/systemd/abcp-b24-garage-sync.timer    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.timer
```

Сервис запускает `python -m abcp_b24_garage_sync` из виртуального окружения и ожидает, что зависимости уже установлены (`pip install -r requirements.txt`). Таймер по умолчанию — каждые 30 минут (`OnCalendar=*:0/30`).

Для разового запуска:

```bash
sudo systemctl start abcp-b24-garage-sync.service
journalctl -u abcp-b24-garage-sync.service -f
```

Логи также доступны в `${LOG_DIR:-<data_dir>/logs}/service.log`.

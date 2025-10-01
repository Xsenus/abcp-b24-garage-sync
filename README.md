
# ABCP → Bitrix24 Garage Sync (Python)

Сервис синхронизации данных «гаража» из ABCP в сделки Bitrix24.

## Установка
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```
## Запуск
```bash
# как пакет
python -m abcp_b24_garage_sync --from 2020-01-01 --to 2025-12-31
# или напрямую
python abcp_b24_garage_sync/main.py --from 2020-01-01 --to 2025-12-31
```
Режимы: `--only-store`, `--only-sync`, `--only-sync --user <id>`.
Логи: в консоль и в `logs/service.log` (ротация, 7 файлов).

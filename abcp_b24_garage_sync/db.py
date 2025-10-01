from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .config import SQLITE_PATH

# Модульный логгер для БД
log = logging.getLogger("db")


def _project_root() -> Path:
    env_value = os.getenv("ABCP_B24_PROJECT_ROOT")
    if env_value:
        try:
            return Path(env_value).expanduser()
        except Exception:
            pass
    return Path(__file__).resolve().parents[1]


def _data_root() -> Path:
    env_value = os.getenv("ABCP_B24_DATA_DIR") or os.getenv("ABC_B24_DATA_DIR")
    if env_value:
        try:
            return Path(env_value).expanduser()
        except Exception:
            pass
    return _project_root()


def _resolve_db_path(raw_path: str) -> Path:
    """Return an absolute path for the SQLite file and ensure parent dir exists."""

    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = _data_root() / candidate

    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate

DDL = '''
CREATE TABLE IF NOT EXISTS garage (
    id              INTEGER PRIMARY KEY,
    userId          INTEGER NOT NULL,
    name            TEXT,
    comment         TEXT,
    year            INTEGER,
    vin             TEXT,
    frame           TEXT,
    mileage         INTEGER,
    manufacturerId  INTEGER,
    manufacturer    TEXT,
    modelId         INTEGER,
    model           TEXT,
    modificationId  INTEGER,
    modification    TEXT,
    dateUpdated     TEXT NOT NULL,
    vehicleRegPlate TEXT,
    raw_json        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_garage_user ON garage(userId);
CREATE INDEX IF NOT EXISTS ix_garage_dateUpdated ON garage(dateUpdated);

-- Сводная таблица статуса по пользователю (последний исходник и результат)
CREATE TABLE IF NOT EXISTS sync_status (
    userId              INTEGER PRIMARY KEY,
    dealId              INTEGER,
    sourceGarageId      INTEGER,
    sourceDateUpdated   TEXT,
    lastSyncedAt        TEXT NOT NULL,
    lastResult          TEXT NOT NULL,                 -- 'updated' | 'skipped' | 'error'
    fieldsUpdatedCount  INTEGER DEFAULT 0,
    fieldsUpdatedJson   TEXT,                          -- JSON со списком UF-кодов (коротко)
    lastError           TEXT
);
CREATE INDEX IF NOT EXISTS ix_sync_status_deal ON sync_status(dealId);

-- Полный аудит всех попыток синхронизации
CREATE TABLE IF NOT EXISTS sync_audit (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    userId              INTEGER NOT NULL,
    dealId              INTEGER,
    sourceGarageId      INTEGER,
    sourceDateUpdated   TEXT,
    result              TEXT NOT NULL,                 -- 'updated' | 'skipped' | 'error'
    fieldsUpdatedCount  INTEGER DEFAULT 0,
    fieldsUpdatedJson   TEXT,                          -- JSON со списком UF-кодов
    error               TEXT,
    createdAt           TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_sync_audit_user ON sync_audit(userId);
CREATE INDEX IF NOT EXISTS ix_sync_audit_createdAt ON sync_audit(createdAt);
'''

def _preview(value: Any, maxlen: int = 160) -> str:
    """Компактное превью значения для логов (без «простыней»)."""
    try:
        if isinstance(value, (dict, list)):
            s = json.dumps(value, ensure_ascii=False)
        else:
            s = "" if value is None else str(value)
        s = s.replace("\n", " ")
        return (s[:maxlen] + "...") if len(s) > maxlen else s
    except Exception:
        return f"<{type(value).__name__}>"

def connect() -> sqlite3.Connection:
    """Открывает соединение к SQLite и включает Row-фабрику."""
    db_path = _resolve_db_path(SQLITE_PATH)
    log.debug("DB connect: path=%s", db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Включим foreign keys на всякий (для будущих расширений)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    return conn

def init_db() -> None:
    """Создаёт таблицы и индексы, если их ещё нет."""
    log.info("DB init: start (path=%s)", SQLITE_PATH)
    try:
        with connect() as c:
            c.executescript(DDL)
            c.commit()
        log.info("DB init: done")
    except Exception:
        log.exception("DB init: FAILED")
        raise

UPSERT_SQL = '''
INSERT INTO garage (id,userId,name,comment,year,vin,frame,mileage,manufacturerId,manufacturer,modelId,model,modificationId,modification,dateUpdated,vehicleRegPlate,raw_json)
VALUES (:id,:userId,:name,:comment,:year,:vin,:frame,:mileage,:manufacturerId,:manufacturer,:modelId,:model,:modificationId,:modification,:dateUpdated,:vehicleRegPlate,:raw_json)
ON CONFLICT(id) DO UPDATE SET
    userId=excluded.userId,
    name=excluded.name,
    comment=excluded.comment,
    year=excluded.year,
    vin=excluded.vin,
    frame=excluded.frame,
    mileage=excluded.mileage,
    manufacturerId=excluded.manufacturerId,
    manufacturer=excluded.manufacturer,
    modelId=excluded.modelId,
    model=excluded.model,
    modificationId=excluded.modificationId,
    modification=excluded.modification,
    dateUpdated=excluded.dateUpdated,
    vehicleRegPlate=excluded.vehicleRegPlate,
    raw_json=excluded.raw_json
'''

def store_payload(payload: dict) -> int:
    """
    Сохраняет данные «гаража» из ABCP в таблицу garage (upsert по id).
    Логирует итоги и, в DEBUG, — каждую операцию.
    """
    if not payload:
        log.info("STORE: empty payload -> nothing to write")
        return 0

    total = 0
    users_count = len(payload)
    log.info("STORE: start (users=%s)", users_count)

    try:
        with connect() as c:
            for user_id_str, cars in (payload or {}).items():
                try:
                    uid = int(user_id_str)
                except Exception:
                    uid = None
                cars = cars or []
                log.debug("STORE: userId=%s cars=%s", uid, len(cars))

                for car in cars:
                    car_dict: Dict[str, Any] = dict(car)
                    car_id = car_dict.get("id")
                    # Подготовим значения по умолчанию
                    car_dict.setdefault("userId", uid if uid is not None else 0)
                    car_dict.setdefault("name", "")
                    car_dict.setdefault("comment", "")
                    car_dict.setdefault("frame", "")
                    car_dict.setdefault("vehicleRegPlate", "")
                    car_dict.setdefault("mileage", 0)
                    car_dict.setdefault("year", 0)

                    # Безопасная сериализация «сырых» данных
                    try:
                        raw_json = json.dumps(car, ensure_ascii=False)
                    except Exception as e:
                        log.warning("STORE: car_id=%s userId=%s raw_json dump failed: %s", car_id, uid, e)
                        raw_json = "{}"

                    params = {**car_dict, "raw_json": raw_json}

                    # В DEBUG показываем ключевые поля, без «сырого» JSON
                    log.debug(
                        "STORE UPSERT: id=%s userId=%s dateUpdated=%s manufacturer=%s model=%s modification=%s vin=%s",
                        params.get("id"),
                        params.get("userId"),
                        params.get("dateUpdated"),
                        _preview(params.get("manufacturer")),
                        _preview(params.get("model")),
                        _preview(params.get("modification")),
                        _preview(params.get("vin")),
                    )

                    c.execute(UPSERT_SQL, params)
                    total += 1

            c.commit()

        log.info("STORE: done (rows upserted=%s, users=%s)", total, users_count)
        return total

    except Exception:
        log.exception("STORE: FAILED after %s rows (users=%s)", total, users_count)
        raise

# ---------- фиксация результата синхронизации ----------

def _now_iso() -> str:
    """Текущее локальное время (ISO без TZ)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_sync_result(
    *,
    user_id: int,
    deal_id: int | None,
    source_garage_id: int | None,
    source_date_updated: str | None,
    result: str,                              # 'updated' | 'skipped' | 'error'
    updated_field_codes: list[str] | None = None,
    error: str | None = None,
) -> None:
    """
    Записывает и аудит (sync_audit), и сводный статус (sync_status) за один вызов.
    В логах фиксируем краткое превью: кто/куда/что/чем закончилось.
    """
    fields_json = json.dumps(updated_field_codes or [], ensure_ascii=False)
    fields_count = len(updated_field_codes or [])

    log.info(
        "SYNC-DB: userId=%s dealId=%s garageId=%s result=%s fields=%s",
        user_id, deal_id, source_garage_id, result, fields_count
    )
    log.debug(
        "SYNC-DB details: sourceDateUpdated=%s field_codes=%s error=%s",
        source_date_updated, updated_field_codes or [], _preview(error)
    )

    try:
        with connect() as c:
            # Полный аудит — каждая попытка
            c.execute(
                """
                INSERT INTO sync_audit
                (userId, dealId, sourceGarageId, sourceDateUpdated, result, fieldsUpdatedCount, fieldsUpdatedJson, error, createdAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    deal_id,
                    source_garage_id,
                    source_date_updated,
                    result,
                    fields_count,
                    fields_json,
                    (error or None),
                    _now_iso(),
                ),
            )

            # Сводный статус — один ряд на пользователя (upsert)
            c.execute(
                """
                INSERT INTO sync_status
                (userId, dealId, sourceGarageId, sourceDateUpdated, lastSyncedAt, lastResult, fieldsUpdatedCount, fieldsUpdatedJson, lastError)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(userId) DO UPDATE SET
                  dealId=excluded.dealId,
                  sourceGarageId=excluded.sourceGarageId,
                  sourceDateUpdated=excluded.sourceDateUpdated,
                  lastSyncedAt=excluded.lastSyncedAt,
                  lastResult=excluded.lastResult,
                  fieldsUpdatedCount=excluded.fieldsUpdatedCount,
                  fieldsUpdatedJson=excluded.fieldsUpdatedJson,
                  lastError=excluded.lastError
                """,
                (
                    user_id,
                    deal_id,
                    source_garage_id,
                    source_date_updated,
                    _now_iso(),
                    result,
                    fields_count,
                    fields_json,
                    (error or None),
                ),
            )
            c.commit()

        log.debug("SYNC-DB: persisted (userId=%s, result=%s)", user_id, result)

    except Exception:
        log.exception(
            "SYNC-DB: FAILED (userId=%s dealId=%s garageId=%s result=%s)",
            user_id, deal_id, source_garage_id, result
        )
        raise

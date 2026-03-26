from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .config import SQLITE_PATH


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


DDL = """
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
CREATE INDEX IF NOT EXISTS ix_garage_user_date_id ON garage(userId, dateUpdated, id);

CREATE TABLE IF NOT EXISTS sync_status (
    userId              INTEGER PRIMARY KEY,
    dealId              INTEGER,
    sourceGarageId      INTEGER,
    sourceDateUpdated   TEXT,
    sourcePayloadHash   TEXT,
    lastSyncedAt        TEXT NOT NULL,
    lastResult          TEXT NOT NULL,                 -- 'updated' | 'skipped' | 'error'
    fieldsUpdatedCount  INTEGER DEFAULT 0,
    fieldsUpdatedJson   TEXT,
    lastError           TEXT
);
CREATE INDEX IF NOT EXISTS ix_sync_status_deal ON sync_status(dealId);

CREATE TABLE IF NOT EXISTS sync_audit (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    userId              INTEGER NOT NULL,
    dealId              INTEGER,
    sourceGarageId      INTEGER,
    sourceDateUpdated   TEXT,
    sourcePayloadHash   TEXT,
    result              TEXT NOT NULL,                 -- 'updated' | 'skipped' | 'error'
    fieldsUpdatedCount  INTEGER DEFAULT 0,
    fieldsUpdatedJson   TEXT,
    error               TEXT,
    createdAt           TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_sync_audit_user ON sync_audit(userId);
CREATE INDEX IF NOT EXISTS ix_sync_audit_createdAt ON sync_audit(createdAt);

CREATE TABLE IF NOT EXISTS fetch_state (
    source              TEXT PRIMARY KEY,
    lastRequestedFrom   TEXT,
    lastRequestedTo     TEXT,
    lastSuccessFrom     TEXT,
    lastSuccessTo       TEXT,
    lastRunAt           TEXT NOT NULL,
    lastStatus          TEXT NOT NULL,                 -- 'success' | 'error'
    lastError           TEXT
);
"""


def _preview(value: Any, maxlen: int = 160) -> str:
    """Compact preview for logs."""
    try:
        if isinstance(value, (dict, list)):
            s = json.dumps(value, ensure_ascii=False)
        else:
            s = "" if value is None else str(value)
        s = s.replace("\n", " ")
        return (s[:maxlen] + "...") if len(s) > maxlen else s
    except Exception:
        return f"<{type(value).__name__}>"


@contextmanager
def connect():
    """Open the SQLite database and always close the connection."""
    db_path = _resolve_db_path(SQLITE_PATH)
    log.debug("DB connect: path=%s", db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    try:
        yield conn
    finally:
        conn.close()


def _table_columns(c: sqlite3.Connection, table: str) -> set[str]:
    rows = c.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_column(c: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column in _table_columns(c, table):
        return
    log.info("DB migration: adding %s.%s", table, column)
    c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db() -> None:
    """Create tables and indexes when missing."""
    log.info("DB init: start (path=%s)", SQLITE_PATH)
    try:
        with connect() as c:
            c.executescript(DDL)
            _ensure_column(c, "sync_status", "sourcePayloadHash", "TEXT")
            _ensure_column(c, "sync_audit", "sourcePayloadHash", "TEXT")
            c.commit()
        log.info("DB init: done")
    except Exception:
        log.exception("DB init: FAILED")
        raise


UPSERT_SQL = """
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
"""


def store_payload(payload: dict) -> int:
    """Store ABCP garage payload into SQLite with upsert by garage id."""
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

                    car_dict.setdefault("userId", uid if uid is not None else 0)
                    car_dict.setdefault("name", "")
                    car_dict.setdefault("comment", "")
                    car_dict.setdefault("frame", "")
                    car_dict.setdefault("vehicleRegPlate", "")
                    car_dict.setdefault("mileage", 0)
                    car_dict.setdefault("year", 0)

                    try:
                        raw_json = json.dumps(car, ensure_ascii=False)
                    except Exception as e:
                        log.warning("STORE: car_id=%s userId=%s raw_json dump failed: %s", car_id, uid, e)
                        raw_json = "{}"

                    params = {**car_dict, "raw_json": raw_json}

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


def _now_iso() -> str:
    """Current local time as an ISO string without timezone."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_fetch_state(source: str) -> Optional[Dict[str, Any]]:
    """Return the last fetch state for a source, if it exists."""
    with connect() as c:
        row = c.execute(
            """
            SELECT source, lastRequestedFrom, lastRequestedTo,
                   lastSuccessFrom, lastSuccessTo, lastRunAt,
                   lastStatus, lastError
            FROM fetch_state
            WHERE source = ?
            """,
            (source,),
        ).fetchone()
    return dict(row) if row else None


def save_fetch_state(
    *,
    source: str,
    requested_from: str | None,
    requested_to: str | None,
    success_from: str | None,
    success_to: str | None,
    status: str,
    error: str | None = None,
) -> None:
    """Persist the current cursor/progress for incremental upstream fetches."""
    log.info(
        "FETCH-STATE: source=%s status=%s requested=%s..%s success=%s..%s",
        source, status, requested_from, requested_to, success_from, success_to
    )
    if error:
        log.debug("FETCH-STATE details: source=%s error=%s", source, _preview(error))

    with connect() as c:
        c.execute(
            """
            INSERT INTO fetch_state
            (source, lastRequestedFrom, lastRequestedTo, lastSuccessFrom, lastSuccessTo, lastRunAt, lastStatus, lastError)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET
              lastRequestedFrom=excluded.lastRequestedFrom,
              lastRequestedTo=excluded.lastRequestedTo,
              lastSuccessFrom=COALESCE(excluded.lastSuccessFrom, fetch_state.lastSuccessFrom),
              lastSuccessTo=COALESCE(excluded.lastSuccessTo, fetch_state.lastSuccessTo),
              lastRunAt=excluded.lastRunAt,
              lastStatus=excluded.lastStatus,
              lastError=excluded.lastError
            """,
            (
                source,
                requested_from,
                requested_to,
                success_from,
                success_to,
                _now_iso(),
                status,
                error,
            ),
        )
        c.commit()


def save_sync_result(
    *,
    user_id: int,
    deal_id: int | None,
    source_garage_id: int | None,
    source_date_updated: str | None,
    source_payload_hash: str | None = None,
    result: str,
    updated_field_codes: list[str] | None = None,
    error: str | None = None,
) -> None:
    """Persist both sync audit and current sync status."""
    fields_json = json.dumps(updated_field_codes or [], ensure_ascii=False)
    fields_count = len(updated_field_codes or [])

    log.info(
        "SYNC-DB: userId=%s dealId=%s garageId=%s result=%s fields=%s",
        user_id, deal_id, source_garage_id, result, fields_count
    )
    log.debug(
        "SYNC-DB details: sourceDateUpdated=%s payloadHash=%s field_codes=%s error=%s",
        source_date_updated, source_payload_hash, updated_field_codes or [], _preview(error)
    )

    try:
        with connect() as c:
            c.execute(
                """
                INSERT INTO sync_audit
                (userId, dealId, sourceGarageId, sourceDateUpdated, sourcePayloadHash, result, fieldsUpdatedCount, fieldsUpdatedJson, error, createdAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    deal_id,
                    source_garage_id,
                    source_date_updated,
                    source_payload_hash,
                    result,
                    fields_count,
                    fields_json,
                    error or None,
                    _now_iso(),
                ),
            )

            c.execute(
                """
                INSERT INTO sync_status
                (userId, dealId, sourceGarageId, sourceDateUpdated, sourcePayloadHash, lastSyncedAt, lastResult, fieldsUpdatedCount, fieldsUpdatedJson, lastError)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(userId) DO UPDATE SET
                  dealId=excluded.dealId,
                  sourceGarageId=excluded.sourceGarageId,
                  sourceDateUpdated=excluded.sourceDateUpdated,
                  sourcePayloadHash=excluded.sourcePayloadHash,
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
                    source_payload_hash,
                    _now_iso(),
                    result,
                    fields_count,
                    fields_json,
                    error or None,
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

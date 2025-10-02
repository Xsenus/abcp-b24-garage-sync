from __future__ import annotations

import json
import time
import logging
from typing import Any, Dict, Optional, Tuple
from os import getenv

from .config import (
    BITRIX_FIELD_ENV_MAP,
    SYNC_OVERWRITE_DEFAULT,
    SYNC_OVERWRITE_FIELDS,
    SYNC_PAUSE_BETWEEN_USERS,
    SYNC_PAUSE_BETWEEN_DEALS,
)
from .db import connect, save_sync_result
from .b24_client import (
    find_deal_by_user,
    update_deal_fields,
    get_deal_fields,
    get_deal_userfield_map,  # для валидации .env → UF
)

log = logging.getLogger("sync_service")

# ---------- вспомогательные утилиты ----------

def _preview(val: Any, maxlen: int = 120) -> str:
    try:
        if isinstance(val, (dict, list)):
            s = json.dumps(val, ensure_ascii=False)
        else:
            s = "" if val is None else str(val)
        s = s.replace("\n", " ")
        return (s[:maxlen] + "...") if len(s) > maxlen else s
    except Exception:
        return f"<{type(val).__name__}>"

def _overwrite_for_field(name: str) -> bool:
    return SYNC_OVERWRITE_FIELDS.get(name, SYNC_OVERWRITE_DEFAULT)

# ---------- валидация маппинга ABCP → UF из .env ----------

_VALIDATED_ENV_MAP_ONCE = False

def _validate_env_mapping_once() -> None:
    global _VALIDATED_ENV_MAP_ONCE
    if _VALIDATED_ENV_MAP_ONCE:
        return
    _VALIDATED_ENV_MAP_ONCE = True

    try:
        uf_meta_map = get_deal_userfield_map()
    except Exception as e:
        log.warning("ENV-MAP: cannot load UF meta map: %s", e)
        uf_meta_map = {}

    for abcp_key, env_names in (BITRIX_FIELD_ENV_MAP or {}).items():
        if isinstance(env_names, str):
            env_names = [env_names]
        for env_name in env_names:
            code = (getenv(env_name, "") or "").strip()
            if not code:
                log.debug("ENV-MAP: %s -> %s not set (skip)", abcp_key, env_name)
                continue
            meta = uf_meta_map.get(code)
            if meta:
                log.info(
                    "ENV-MAP: %s -> %s=%s (type=%s, multiple=%s, id=%s)",
                    abcp_key, env_name, code, meta.get("USER_TYPE_ID"),
                    meta.get("MULTIPLE"), meta.get("ID"),
                )
            else:
                log.warning("ENV-MAP: %s -> %s=%s (UF not found in Bitrix)", abcp_key, env_name, code)

# ---------- построение набора UF-полей ----------

def _build_update_fields(row) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for abcp_key, env_names in BITRIX_FIELD_ENV_MAP.items():
        env_candidates = (env_names,) if isinstance(env_names, str) else tuple(env_names)

        resolved_codes = []
        for env_name in env_candidates:
            candidate = (getenv(env_name, "") or "").strip()
            if candidate:
                resolved_codes.append((env_name, candidate))

        if not resolved_codes:
            log.debug("BUILD: skip abcp_key=%s (no UF envs set: %s)", abcp_key, env_candidates)
            continue

        value = row[abcp_key]
        allow_overwrite = _overwrite_for_field(abcp_key)
        if not allow_overwrite and value in (None, "", 0):
            log.debug(
                "BUILD: skip abcp_key=%s -> UF=%s (overwrite=False and new value is empty)",
                abcp_key, [code for _, code in resolved_codes],
            )
            continue

        if isinstance(value, (dict, list)):
            log.debug("BUILD: abcp_key=%s is %s -> JSON stringify", abcp_key, type(value).__name__)
            value = json.dumps(value, ensure_ascii=False)

        prepared = "" if value is None else value  # тип оставляем исходный; b24_client нормализует

        for env_name, uf_code in resolved_codes:
            fields[uf_code] = prepared
            log.debug(
                "BUILD: set UF=%s (from abcp_key=%s, env=%s, overwrite=%s) value_preview=%s",
                uf_code, abcp_key, env_name, allow_overwrite, _preview(prepared),
            )

    log.debug("BUILD: total UF fields prepared=%s", len(fields))
    return fields

# ---------- сравнение «было/стало» ----------

def _normalize(v: Any) -> str:
    if v is None:
        return ""
    return str(v)

def _diff_fields(current: Dict[str, Any], new_fields: Dict[str, Any]) -> Dict[str, Any]:
    diff: Dict[str, Any] = {}
    for code, new_val in (new_fields or {}).items():
        cur_val = current.get(code)
        if _normalize(cur_val) != _normalize(new_val):
            diff[code] = new_val
            log.debug("DIFF: UF=%s changed: %s -> %s", code, _preview(cur_val), _preview(new_val))
        else:
            log.debug("DIFF: UF=%s unchanged", code)
    log.debug("DIFF: changed fields count=%s", len(diff))
    return diff

# ---------- выбор «последних» записей ----------

def _select_latest_rows_per_user(c, only_user: Optional[int] = None):
    if only_user:
        sql = """
        SELECT *
        FROM garage
        WHERE userId = ?
        ORDER BY datetime(dateUpdated) DESC
        LIMIT 1
        """
        params = (only_user,)
    else:
        sql = """
        SELECT g.*
        FROM garage g
        JOIN (
            SELECT userId, MAX(datetime(dateUpdated)) AS maxd
            FROM garage
            GROUP BY userId
        ) t ON t.userId = g.userId AND t.maxd = datetime(g.dateUpdated)
        ORDER BY g.userId ASC
        """
        params = ()
    rows = c.execute(sql, params).fetchall()
    log.info("SELECT: rows fetched=%s", len(rows))
    return rows

# ---------- основной алгоритм ----------

def sync_all(user_id: Optional[int] = None) -> Tuple[int, int, int]:
    _validate_env_mapping_once()

    ok = skipped = errors = 0

    with connect() as c:
        rows = _select_latest_rows_per_user(c, user_id)

    log.info("SYNC: users to process=%s (strategy=latest_per_user)", len(rows))

    for i, row in enumerate(rows, 1):
        uid = int(row["userId"]) if row["userId"] is not None else None
        garage_id = row["id"]
        dt_updated = row["dateUpdated"]

        if not uid:
            skipped += 1
            log.warning("SYNC %d/%d: row id=%s has no userId -> skipped", i, len(rows), garage_id)
            save_sync_result(
                user_id=0, deal_id=None, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="skipped",
                updated_field_codes=None, error="row without userId",
            )
            continue

        log.info("SYNC %d/%d: userId=%s row_id=%s dateUpdated=%s -> find deal",
                 i, len(rows), uid, garage_id, dt_updated)

        deal = find_deal_by_user(uid)
        if not deal:
            skipped += 1
            log.info("SYNC %d/%d: userId=%s -> no deal found, skip", i, len(rows), uid)
            save_sync_result(
                user_id=uid, deal_id=None, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="skipped",
                updated_field_codes=None, error="deal not found",
            )
            if SYNC_PAUSE_BETWEEN_USERS:
                time.sleep(SYNC_PAUSE_BETWEEN_USERS)
            continue

        deal_id = int(deal["ID"])
        log.info("SYNC %d/%d: userId=%s -> deal_id=%s", i, len(rows), uid, deal_id)

        new_fields = _build_update_fields(row)
        uf_codes = list(new_fields.keys())
        log.debug("SYNC %d/%d: deal_id=%s -> UF codes prepared=%s", i, len(rows), deal_id, uf_codes)

        try:
            current = get_deal_fields(deal_id, uf_codes)
        except Exception as e:
            errors += 1
            log.exception("SYNC %d/%d: get_deal_fields failed (deal_id=%s)", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid, deal_id=deal_id, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="error",
                updated_field_codes=uf_codes, error=str(e),
            )
            continue

        diff = _diff_fields(current, new_fields)

        if not diff:
            skipped += 1
            log.info("SYNC %d/%d: deal_id=%s -> no changes (skip)", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid, deal_id=deal_id, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="skipped",
                updated_field_codes=[], error=None,
            )
            if SYNC_PAUSE_BETWEEN_DEALS:
                time.sleep(SYNC_PAUSE_BETWEEN_DEALS)
            continue

        try:
            fields_to_update = list(diff.keys())
            log.info("SYNC %d/%d: deal_id=%s -> fields_to_update=%s (count=%d)",
                     i, len(rows), deal_id, fields_to_update, len(fields_to_update))
            update_deal_fields(deal_id, diff)
            ok += 1
            log.info("SYNC %d/%d: deal_id=%s -> update OK", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid, deal_id=deal_id, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="updated",
                updated_field_codes=fields_to_update, error=None,
            )
        except Exception as e:
            errors += 1
            log.exception("SYNC %d/%d: update failed (deal_id=%s)", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid, deal_id=deal_id, source_garage_id=garage_id,
                source_date_updated=dt_updated, result="error",
                updated_field_codes=list(diff.keys()), error=str(e),
            )

        if SYNC_PAUSE_BETWEEN_DEALS:
            time.sleep(SYNC_PAUSE_BETWEEN_DEALS)

    log.info("SYNC: finished -> updated=%s skipped=%s errors=%s", ok, skipped, errors)
    return ok, skipped, errors

from __future__ import annotations

import hashlib
import json
import logging
import time
from os import getenv
from typing import Any, Dict, Optional, Tuple

from .b24_client import (
    find_deals_by_users,
    get_deal_fields,
    get_deal_fields_batch,
    get_deal_userfield_map,
    update_deal_fields,
    update_deals_fields_batch,
)
from .config import (
    BITRIX_FIELD_ENV_MAP,
    SYNC_OVERWRITE_DEFAULT,
    SYNC_OVERWRITE_FIELDS,
    SYNC_PAUSE_BETWEEN_DEALS,
    SYNC_PAUSE_BETWEEN_USERS,
)
from .db import connect, save_sync_result


log = logging.getLogger("sync_service")

_VALIDATED_ENV_MAP_ONCE = False


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


def _normalize(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def _stable_payload_hash(fields: Dict[str, Any]) -> str:
    payload = json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
                    abcp_key,
                    env_name,
                    code,
                    meta.get("USER_TYPE_ID"),
                    meta.get("MULTIPLE"),
                    meta.get("ID"),
                )
            else:
                log.warning("ENV-MAP: %s -> %s=%s (UF not found in Bitrix)", abcp_key, env_name, code)


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
                abcp_key,
                [code for _, code in resolved_codes],
            )
            continue

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        elif value is not None:
            value = str(value)

        prepared = "" if value is None else value

        for env_name, uf_code in resolved_codes:
            fields[uf_code] = prepared
            log.debug(
                "BUILD: set UF=%s (from abcp_key=%s, env=%s, overwrite=%s) value_preview=%s",
                uf_code,
                abcp_key,
                env_name,
                allow_overwrite,
                _preview(prepared),
            )

    log.debug("BUILD: total UF fields prepared=%s", len(fields))
    return fields


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


def _select_latest_rows_per_user(c, only_user: Optional[int] = None):
    sql = """
    SELECT
        g.*,
        ss.dealId AS cachedDealId,
        ss.sourceGarageId AS cachedSourceGarageId,
        ss.sourceDateUpdated AS cachedSourceDateUpdated,
        ss.sourcePayloadHash AS cachedSourcePayloadHash,
        ss.lastResult AS cachedLastResult,
        ss.lastError AS cachedLastError
    FROM garage g
    LEFT JOIN sync_status ss ON ss.userId = g.userId
    WHERE g.id = (
        SELECT g2.id
        FROM garage g2
        WHERE g2.userId = g.userId
        ORDER BY datetime(g2.dateUpdated) DESC, g2.id DESC
        LIMIT 1
    )
    """
    params: tuple[Any, ...] = ()
    if only_user is not None:
        sql += " AND g.userId = ?"
        params = (only_user,)

    sql += " ORDER BY g.userId ASC"
    rows = c.execute(sql, params).fetchall()
    log.info("SELECT: latest rows fetched=%s", len(rows))
    return rows


def _can_skip_remote_sync(row, payload_hash: Optional[str] = None) -> bool:
    cached_deal_id = row["cachedDealId"]
    if not cached_deal_id:
        return False
    if row["cachedLastResult"] == "error":
        return False

    cached_hash = _normalize(row["cachedSourcePayloadHash"])
    if payload_hash and cached_hash and cached_hash == payload_hash:
        return True

    return (
        _normalize(row["cachedSourceGarageId"]) == _normalize(row["id"])
        and _normalize(row["cachedSourceDateUpdated"]) == _normalize(row["dateUpdated"])
    )


def _should_persist_local_skip(row, payload_hash: str) -> bool:
    return (
        _normalize(row["cachedSourceGarageId"]) != _normalize(row["id"])
        or _normalize(row["cachedSourceDateUpdated"]) != _normalize(row["dateUpdated"])
        or _normalize(row["cachedSourcePayloadHash"]) != _normalize(payload_hash)
    )


def _resolve_current_fields_batch(items: list[dict[str, Any]]) -> dict[int, Dict[str, Any]]:
    deal_field_map: Dict[int, list[str]] = {}
    for item in items:
        deal_id = item.get("deal_id")
        if not deal_id:
            continue
        existing = deal_field_map.setdefault(deal_id, [])
        for code in item["uf_codes"]:
            if code not in existing:
                existing.append(code)

    if not deal_field_map:
        return {}

    try:
        return get_deal_fields_batch(deal_field_map)
    except Exception as e:
        log.warning("SYNC: batch get current fields failed, fallback to single calls: %s", e)
        return {deal_id: get_deal_fields(deal_id, uf_codes) for deal_id, uf_codes in deal_field_map.items()}


def _apply_updates_batch(
    updates: Dict[int, Dict[str, Any]],
    before_fields_by_deal: Dict[int, Dict[str, Any]],
) -> tuple[Dict[int, bool], Dict[int, Exception]]:
    if not updates:
        return {}, {}
    try:
        return update_deals_fields_batch(updates, before_fields_by_deal=before_fields_by_deal), {}
    except Exception as e:
        log.warning("SYNC: bulk update path failed, retrying individually: %s", e)

    applied_map: Dict[int, bool] = {}
    errors: Dict[int, Exception] = {}
    for deal_id, diff in updates.items():
        try:
            applied_map[deal_id] = update_deal_fields(
                deal_id,
                diff,
                before_fields=before_fields_by_deal.get(deal_id),
            )
        except Exception as err:
            errors[deal_id] = err
    return applied_map, errors


def sync_all(user_id: Optional[int] = None) -> Tuple[int, int, int]:
    _validate_env_mapping_once()

    ok = skipped = errors = 0

    with connect() as c:
        rows = _select_latest_rows_per_user(c, user_id)

    log.info("SYNC: users to process=%s (strategy=latest_per_user)", len(rows))

    work_items: list[dict[str, Any]] = []

    for i, row in enumerate(rows, 1):
        uid = int(row["userId"]) if row["userId"] is not None else None
        garage_id = row["id"]
        dt_updated = row["dateUpdated"]

        if not uid:
            skipped += 1
            log.warning("SYNC %d/%d: row id=%s has no userId -> skipped", i, len(rows), garage_id)
            save_sync_result(
                user_id=0,
                deal_id=None,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=None,
                result="skipped",
                updated_field_codes=None,
                error="row without userId",
            )
            continue

        new_fields = _build_update_fields(row)
        uf_codes = list(new_fields.keys())
        payload_hash = _stable_payload_hash(new_fields)

        if not uf_codes:
            skipped += 1
            log.warning("SYNC %d/%d: userId=%s -> no UF mappings resolved, skip", i, len(rows), uid)
            save_sync_result(
                user_id=uid,
                deal_id=row["cachedDealId"],
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="skipped",
                updated_field_codes=[],
                error="no UF mappings resolved",
            )
            continue

        if _can_skip_remote_sync(row, payload_hash):
            skipped += 1
            log.info(
                "SYNC %d/%d: userId=%s row_id=%s -> local skip by cached state/payload hash",
                i,
                len(rows),
                uid,
                garage_id,
            )
            if _should_persist_local_skip(row, payload_hash):
                save_sync_result(
                    user_id=uid,
                    deal_id=row["cachedDealId"],
                    source_garage_id=garage_id,
                    source_date_updated=dt_updated,
                    source_payload_hash=payload_hash,
                    result="skipped",
                    updated_field_codes=[],
                    error=None,
                )
            if SYNC_PAUSE_BETWEEN_DEALS:
                time.sleep(SYNC_PAUSE_BETWEEN_DEALS)
            continue

        work_items.append(
            {
                "index": i,
                "total": len(rows),
                "row": row,
                "uid": uid,
                "garage_id": garage_id,
                "dt_updated": dt_updated,
                "new_fields": new_fields,
                "uf_codes": uf_codes,
                "payload_hash": payload_hash,
                "deal_id": int(row["cachedDealId"]) if row["cachedDealId"] else None,
            }
        )

    unresolved_items = [item for item in work_items if not item["deal_id"]]
    if unresolved_items:
        try:
            deals_by_user = find_deals_by_users([item["uid"] for item in unresolved_items])
        except Exception as e:
            deals_by_user = {}
            log.warning("SYNC: bulk deal lookup failed, fallback to empty map: %s", e)

        for item in unresolved_items:
            deal = deals_by_user.get(item["uid"])
            if deal:
                item["deal_id"] = int(deal["ID"])

    current_fields_by_deal = _resolve_current_fields_batch([item for item in work_items if item["deal_id"]])

    updates: Dict[int, Dict[str, Any]] = {}
    before_fields_by_deal: Dict[int, Dict[str, Any]] = {}
    update_items: Dict[int, dict[str, Any]] = {}

    for item in work_items:
        uid = item["uid"]
        garage_id = item["garage_id"]
        dt_updated = item["dt_updated"]
        deal_id = item["deal_id"]
        payload_hash = item["payload_hash"]

        if not deal_id:
            skipped += 1
            log.info("SYNC %d/%d: userId=%s -> no deal found, skip", item["index"], item["total"], uid)
            save_sync_result(
                user_id=uid,
                deal_id=None,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="skipped",
                updated_field_codes=None,
                error="deal not found",
            )
            if SYNC_PAUSE_BETWEEN_USERS:
                time.sleep(SYNC_PAUSE_BETWEEN_USERS)
            continue

        current = current_fields_by_deal.get(deal_id)
        if current is None:
            errors += 1
            log.error("SYNC %d/%d: failed to load current fields for deal_id=%s", item["index"], item["total"], deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="error",
                updated_field_codes=item["uf_codes"],
                error="failed to load current deal fields",
            )
            continue

        current_for_item = {code: current.get(code) for code in item["uf_codes"]}
        diff = _diff_fields(current_for_item, item["new_fields"])

        if not diff:
            skipped += 1
            log.info("SYNC %d/%d: deal_id=%s -> no changes (skip)", item["index"], item["total"], deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="skipped",
                updated_field_codes=[],
                error=None,
            )
            if SYNC_PAUSE_BETWEEN_DEALS:
                time.sleep(SYNC_PAUSE_BETWEEN_DEALS)
            continue

        updates[deal_id] = diff
        before_fields_by_deal[deal_id] = current_for_item
        update_items[deal_id] = item

    applied_map, update_errors = _apply_updates_batch(updates, before_fields_by_deal)

    for deal_id, item in update_items.items():
        uid = item["uid"]
        garage_id = item["garage_id"]
        dt_updated = item["dt_updated"]
        payload_hash = item["payload_hash"]
        fields_to_update = list(updates[deal_id].keys())

        if deal_id in update_errors:
            errors += 1
            log.exception(
                "SYNC %d/%d: update failed (deal_id=%s)",
                item["index"],
                item["total"],
                deal_id,
                exc_info=update_errors[deal_id],
            )
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="error",
                updated_field_codes=fields_to_update,
                error=str(update_errors[deal_id]),
            )
            continue

        applied = applied_map.get(deal_id)
        if not applied:
            errors += 1
            error_text = "Bitrix accepted update call, but verification reported unapplied fields"
            log.error("SYNC %d/%d: update not applied (deal_id=%s)", item["index"], item["total"], deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                source_payload_hash=payload_hash,
                result="error",
                updated_field_codes=fields_to_update,
                error=error_text,
            )
            continue

        ok += 1
        log.info("SYNC %d/%d: deal_id=%s -> update OK", item["index"], item["total"], deal_id)
        save_sync_result(
            user_id=uid,
            deal_id=deal_id,
            source_garage_id=garage_id,
            source_date_updated=dt_updated,
            source_payload_hash=payload_hash,
            result="updated",
            updated_field_codes=fields_to_update,
            error=None,
        )
        if SYNC_PAUSE_BETWEEN_DEALS:
            time.sleep(SYNC_PAUSE_BETWEEN_DEALS)

    log.info("SYNC: finished -> updated=%s skipped=%s errors=%s", ok, skipped, errors)
    return ok, skipped, errors

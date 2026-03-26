from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode

import requests

from .config import (
    B24_BATCH_SIZE,
    B24_DEAL_CATEGORY_ID_USERS,
    B24_TZ_OFFSET,
    B24_USE_BATCH,
    B24_VERIFY_UPDATES,
    B24_WEBHOOK_URL,
    RATE_LIMIT_SLEEP,
    REQUESTS_TIMEOUT,
    UF_B24_DEAL_ABCP_USER_ID,
)


DEFAULT_TZ_OFFSET = B24_TZ_OFFSET
_FALLBACK_TZ = timezone(timedelta(hours=3))

logger = logging.getLogger("b24_client")
SESSION = requests.Session()


def _mask_url(url: str) -> str:
    try:
        head, rest = url.split("/rest/", 1)
        parts = rest.split("/")
        if len(parts) >= 2:
            parts[1] = "********"
        return head + "/rest/" + "/".join(parts)
    except Exception:
        return url


def _preview_json(data: Any, limit: int = 900) -> str:
    try:
        s = json.dumps(data, ensure_ascii=False)
        return s if len(s) <= limit else s[:limit] + "...(truncated)"
    except Exception:
        return f"<{type(data).__name__}>"


def _safe_params_for_log(params: dict) -> dict:
    try:
        view: Dict[str, Any] = {}
        for k, v in (params or {}).items():
            if k == "fields" and isinstance(v, dict):
                view[k] = {
                    "__keys__": list(v.keys()),
                    "__types__": {kk: type(vv).__name__ for kk, vv in v.items()},
                }
            elif isinstance(v, (dict, list, tuple)):
                view[k] = {"__type__": type(v).__name__, "__len__": len(v)}
            else:
                view[k] = v
        return view
    except Exception:
        return {"info": "params present, failed to render safely"}


def _tz_from_offset(offset: Optional[str]) -> timezone:
    s = (offset or DEFAULT_TZ_OFFSET or "+03:00").strip()
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", s)
    if m is None:
        logger.warning("B24 TZ: invalid offset '%s'; using +03:00 fallback", s)
        return _FALLBACK_TZ
    sign, hh, mm = m.groups()
    delta = timedelta(hours=int(hh), minutes=int(mm))
    if sign == "-":
        delta = -delta
    return timezone(delta)


def _ensure_datetime_str(v: Any, fallback_offset: str = DEFAULT_TZ_OFFSET) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, datetime):
        dt = v if v.tzinfo else v.replace(tzinfo=_tz_from_offset(fallback_offset))
        return dt.isoformat(timespec="seconds")
    if isinstance(v, date) and not isinstance(v, datetime):
        dt = datetime(v.year, v.month, v.day, 0, 0, 0, tzinfo=_tz_from_offset(fallback_offset))
        return dt.isoformat(timespec="seconds")
    if isinstance(v, str):
        s = v.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})", s):
            return s
        m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", s)
        if m:
            return f"{m.group(1)}T{m.group(2)}{fallback_offset}"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return f"{s}T00:00:00{fallback_offset}"
        return s
    return str(v)


def _ensure_date_str(v: Any) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, str):
        s = v.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return s
        m = re.match(r"(\d{4}-\d{2}-\d{2})[ T]\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})?$", s)
        if m:
            return m.group(1)
        return s
    return str(v)


def _to_bool_y_n(v: Any) -> str:
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"y", "yes", "true", "1"}:
            return "Y"
        if s in {"n", "no", "false", "0"}:
            return "N"
    if isinstance(v, bool):
        return "Y" if v else "N"
    if isinstance(v, (int, float)):
        return "Y" if v != 0 else "N"
    return "Y"


def _call(method: str, params: dict) -> Any:
    url = f"{B24_WEBHOOK_URL}{method}"
    masked_url = _mask_url(url)

    logger.debug(
        "B24 CALL start: method=%s url=%s params=%s",
        method, masked_url, _preview_json(_safe_params_for_log(params)),
    )

    r = SESSION.post(url, json=params, timeout=REQUESTS_TIMEOUT)
    logger.debug("B24 CALL response: status=%s, content_length=%s", r.status_code, len(r.content or b""))

    try:
        data: Any = r.json()
    except Exception:
        snippet = (r.text or "")[:800]
        logger.error("B24 CALL non-JSON response (snippet): %s", snippet)
        r.raise_for_status()
        raise

    if isinstance(data, dict) and "error" in data:
        logger.error(
            "B24 CALL API error: method=%s url=%s error=%s description=%s",
            method, masked_url, data.get("error"), data.get("error_description"),
        )
        raise RuntimeError(_preview_json(data))

    logger.debug("B24 CALL ok: method=%s url=%s preview=%s", method, masked_url, _preview_json(data))

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        time.sleep(RATE_LIMIT_SLEEP)

    if isinstance(data, dict) and "result" in data:
        return data["result"]
    return data


def _call_full(method: str, params: dict) -> dict:
    url = f"{B24_WEBHOOK_URL}{method}"
    masked_url = _mask_url(url)

    logger.debug(
        "B24 CALL FULL start: method=%s url=%s params=%s",
        method, masked_url, _preview_json(_safe_params_for_log(params)),
    )

    r = SESSION.post(url, json=params, timeout=REQUESTS_TIMEOUT)
    logger.debug("B24 CALL FULL response: status=%s, content_length=%s", r.status_code, len(r.content or b""))

    try:
        data: Any = r.json()
    except Exception:
        snippet = (r.text or "")[:800]
        logger.error("B24 CALL FULL non-JSON response (snippet): %s", snippet)
        r.raise_for_status()
        raise

    if isinstance(data, dict) and "error" in data:
        logger.error(
            "B24 CALL FULL API error: method=%s url=%s error=%s description=%s",
            method, masked_url, data.get("error"), data.get("error_description"),
        )
        raise RuntimeError(_preview_json(data))

    logger.debug("B24 CALL FULL ok: method=%s url=%s preview=%s", method, masked_url, _preview_json(data))

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        time.sleep(RATE_LIMIT_SLEEP)

    if not isinstance(data, dict):
        return {"result": data}
    return data


@lru_cache(maxsize=1)
def _deal_userfields_map() -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    start: Optional[int] = 0

    while True:
        payload: Dict[str, Any] = {"order": {"ID": "ASC"}, "filter": {}}
        if start is not None:
            payload["start"] = start

        data = _call_full("crm.deal.userfield.list", payload)
        items = data.get("result") or []
        if isinstance(items, list):
            for it in items:
                code = it.get("FIELD_NAME")
                if code:
                    result[code] = it

        if "next" in data:
            start = data["next"]
        else:
            break

    logger.debug("UF map loaded (paginated): %s fields", len(result))
    return result


def _load_uf_meta_by_code(code: str) -> dict:
    mp = _deal_userfields_map()
    meta = mp.get(code)
    if not meta:
        _deal_userfields_map.cache_clear()
        mp = _deal_userfields_map()
        meta = mp.get(code)

    if not meta:
        return {}

    if meta.get("USER_TYPE_ID") and (meta.get("USER_TYPE_ID") != "enumeration" or meta.get("LIST")):
        return meta

    uf_id = meta.get("ID")
    if not uf_id:
        return meta

    try:
        full = _call("crm.deal.userfield.get", {"id": int(uf_id)}) or {}
        if isinstance(full, dict):
            return full
    except Exception as e:
        logger.debug("UF get failed for %s: %s", code, e)
    return meta


@lru_cache(maxsize=256)
def _get_uf_meta_by_code(code: str) -> dict:
    return _load_uf_meta_by_code(code)


def get_deal_userfield_map() -> Dict[str, dict]:
    return _deal_userfields_map()


def _ensure_numeric(utype: str, v: Any) -> Any:
    if v is None or v == "":
        return ""
    try:
        if utype == "integer":
            if isinstance(v, bool):
                return int(bool(v))
            return int(str(v).strip())
        if utype == "double":
            if isinstance(v, bool):
                return 1.0 if v else 0.0
            return float(str(v).strip().replace(",", "."))
    except Exception:
        return v
    return v


def _enum_map_from_meta(meta: dict) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    lst = meta.get("LIST") or []
    if isinstance(lst, list):
        for it in lst:
            try:
                enum_id = int(it.get("ID"))
            except Exception:
                continue
            val = (it.get("VALUE") or "").strip().lower()
            xml = (it.get("XML_ID") or "").strip().lower()
            if val:
                mapping[val] = enum_id
            if xml:
                mapping[xml] = enum_id
    return mapping


def _ensure_enum_ids(v: Any, meta: dict) -> Union[int, List[int], str, Any]:
    if v == "":
        return ""
    mapping = _enum_map_from_meta(meta)
    is_multiple = bool(meta.get("MULTIPLE") in ("Y", True, "true", "True", 1))

    def _one(x: Any) -> Any:
        if x is None or x == "":
            return ""
        s = str(x).strip()
        if re.fullmatch(r"\d+", s):
            try:
                return int(s)
            except Exception:
                pass
        return mapping.get(s.lower(), x)

    if is_multiple:
        if isinstance(v, (list, tuple, set)):
            return [_one(x) for x in v]
        return [_one(v)]
    return _one(v)


def _normalize_fields_for_update(fields: Dict[str, Any]) -> Dict[str, Any]:
    norm: Dict[str, Any] = {}
    for code, value in fields.items():
        if value is None:
            norm[code] = ""
            continue

        meta = _get_uf_meta_by_code(code)
        utype = (meta.get("USER_TYPE_ID") or "").lower()

        if utype == "datetime":
            norm[code] = _ensure_datetime_str(value, DEFAULT_TZ_OFFSET)
        elif utype == "date":
            norm[code] = _ensure_date_str(value)
        elif utype == "boolean":
            norm[code] = _to_bool_y_n(value)
        elif utype in {"integer", "double"}:
            norm[code] = _ensure_numeric(utype, value)
        elif utype == "enumeration":
            norm[code] = _ensure_enum_ids(value, meta)
        else:
            norm[code] = value

        logger.debug(
            "NORM: field=%s type=%s multiple=%s -> %s",
            code, utype or "<unknown>", meta.get("MULTIPLE"), _preview_json(norm[code], 200),
        )
    return norm


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    chunk_size = max(1, size)
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _flatten_rest_pairs(value: Any, prefix: str) -> List[tuple[str, str]]:
    pairs: List[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            nested_prefix = f"{prefix}[{key}]"
            pairs.extend(_flatten_rest_pairs(nested, nested_prefix))
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            nested_prefix = f"{prefix}[{index}]"
            pairs.extend(_flatten_rest_pairs(nested, nested_prefix))
    else:
        if value is None:
            rendered = ""
        elif isinstance(value, bool):
            rendered = "Y" if value else "N"
        else:
            rendered = str(value)
        pairs.append((prefix, rendered))
    return pairs


def _build_rest_query(params: Dict[str, Any]) -> str:
    pairs: List[tuple[str, str]] = []
    for key, value in (params or {}).items():
        pairs.extend(_flatten_rest_pairs(value, key))
    return urlencode(pairs)


def _build_batch_command(method: str, params: Dict[str, Any]) -> str:
    query = _build_rest_query(params)
    return f"{method}?{query}" if query else method


def _extract_batch_payload(data: Any) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not isinstance(data, dict):
        return {}, {}

    payload = data.get("result")
    if isinstance(payload, dict):
        results = payload.get("result")
        errors = payload.get("result_error")
        if isinstance(results, dict):
            return results, errors if isinstance(errors, dict) else {}

    results = data.get("result")
    errors = data.get("result_error")
    return results if isinstance(results, dict) else {}, errors if isinstance(errors, dict) else {}


def _call_batch(commands: Dict[str, str]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not commands:
        return {}, {}

    data = _call_full("batch", {"halt": 0, "cmd": commands})
    return _extract_batch_payload(data)


def find_deals_by_users(user_ids: List[int]) -> Dict[int, Optional[Dict[str, Any]]]:
    if not user_ids:
        return {}

    unique_user_ids = list(dict.fromkeys(int(uid) for uid in user_ids))
    if not B24_USE_BATCH or len(unique_user_ids) == 1:
        return {uid: find_deal_by_user(uid) for uid in unique_user_ids}

    resolved: Dict[int, Optional[Dict[str, Any]]] = {}
    select = ["ID", "TITLE", UF_B24_DEAL_ABCP_USER_ID]

    for batch in _chunked(unique_user_ids, B24_BATCH_SIZE):
        commands = {
            f"find_{uid}": _build_batch_command(
                "crm.deal.list",
                {
                    "filter": {
                        UF_B24_DEAL_ABCP_USER_ID: str(uid),
                        "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,
                    },
                    "select": select,
                    "start": 0,
                },
            )
            for uid in batch
        }

        try:
            results, errors = _call_batch(commands)
        except Exception as e:
            logger.warning("B24 batch find failed, fallback to single calls: %s", e)
            for uid in batch:
                resolved[uid] = find_deal_by_user(uid)
            continue

        for uid in batch:
            command_key = f"find_{uid}"
            if command_key in errors:
                logger.warning("B24 batch find per-command error for user_id=%s, fallback to single call", uid)
                resolved[uid] = find_deal_by_user(uid)
                continue

            items = results.get(command_key)
            if isinstance(items, list) and items:
                resolved[uid] = items[0]
            elif isinstance(items, list):
                resolved[uid] = None
            else:
                logger.warning("B24 batch find returned unexpected payload for user_id=%s, fallback to single call", uid)
                resolved[uid] = find_deal_by_user(uid)

    return resolved


def get_deal_fields_batch(deal_field_map: Dict[int, List[str]]) -> Dict[int, Dict[str, Any]]:
    if not deal_field_map:
        return {}

    normalized_map = {int(deal_id): list(dict.fromkeys(fields)) for deal_id, fields in deal_field_map.items()}
    deal_ids = list(normalized_map.keys())

    if not B24_USE_BATCH or len(deal_ids) == 1:
        return {deal_id: get_deal_fields(deal_id, normalized_map[deal_id]) for deal_id in deal_ids}

    resolved: Dict[int, Dict[str, Any]] = {}
    for batch in _chunked(deal_ids, B24_BATCH_SIZE):
        commands = {
            f"get_{deal_id}": _build_batch_command("crm.deal.get", {"id": int(deal_id)})
            for deal_id in batch
        }

        try:
            results, errors = _call_batch(commands)
        except Exception as e:
            logger.warning("B24 batch get failed, fallback to single calls: %s", e)
            for deal_id in batch:
                resolved[deal_id] = get_deal_fields(deal_id, normalized_map[deal_id])
            continue

        for deal_id in batch:
            command_key = f"get_{deal_id}"
            uf_codes = normalized_map[deal_id]
            if command_key in errors:
                logger.warning("B24 batch get per-command error for deal_id=%s, fallback to single call", deal_id)
                resolved[deal_id] = get_deal_fields(deal_id, uf_codes)
                continue

            deal = results.get(command_key)
            if isinstance(deal, dict):
                resolved[deal_id] = {code: deal.get(code) for code in uf_codes}
            else:
                logger.warning("B24 batch get returned unexpected payload for deal_id=%s, fallback to single call", deal_id)
                resolved[deal_id] = get_deal_fields(deal_id, uf_codes)

    return resolved


def update_deals_fields_batch(
    updates: Dict[int, Dict[str, Any]],
    *,
    before_fields_by_deal: Optional[Dict[int, Dict[str, Any]]] = None,
    verify: Optional[bool] = None,
) -> Dict[int, bool]:
    if not updates:
        return {}

    if verify is None:
        verify = B24_VERIFY_UPDATES

    normalized_updates = {int(deal_id): _normalize_fields_for_update(fields) for deal_id, fields in updates.items()}
    if verify or not B24_USE_BATCH or len(normalized_updates) == 1:
        return {
            deal_id: update_deal_fields(
                deal_id,
                fields,
                before_fields=(before_fields_by_deal or {}).get(deal_id),
                verify=verify,
            )
            for deal_id, fields in updates.items()
        }

    results_map: Dict[int, bool] = {}
    for batch in _chunked(list(normalized_updates.keys()), B24_BATCH_SIZE):
        commands = {
            f"upd_{deal_id}": _build_batch_command(
                "crm.deal.update",
                {
                    "id": int(deal_id),
                    "fields": normalized_updates[deal_id],
                    "params": {"REGISTER_SONET_EVENT": "N"},
                },
            )
            for deal_id in batch
        }

        try:
            results, errors = _call_batch(commands)
        except Exception as e:
            logger.warning("B24 batch update failed, fallback to single calls: %s", e)
            for deal_id in batch:
                results_map[deal_id] = update_deal_fields(
                    deal_id,
                    updates[deal_id],
                    before_fields=(before_fields_by_deal or {}).get(deal_id),
                    verify=verify,
                )
            continue

        for deal_id in batch:
            command_key = f"upd_{deal_id}"
            if command_key in errors:
                logger.warning("B24 batch update per-command error for deal_id=%s, fallback to single call", deal_id)
                results_map[deal_id] = update_deal_fields(
                    deal_id,
                    updates[deal_id],
                    before_fields=(before_fields_by_deal or {}).get(deal_id),
                    verify=verify,
                )
                continue

            result_value = results.get(command_key)
            if result_value in (True, "true", "1", 1):
                results_map[deal_id] = True
            elif result_value in (False, "false", "0", 0):
                logger.warning("B24 batch update reported false for deal_id=%s, fallback to single call", deal_id)
                results_map[deal_id] = update_deal_fields(
                    deal_id,
                    updates[deal_id],
                    before_fields=(before_fields_by_deal or {}).get(deal_id),
                    verify=verify,
                )
            else:
                logger.warning("B24 batch update returned unexpected payload for deal_id=%s, fallback to single call", deal_id)
                results_map[deal_id] = update_deal_fields(
                    deal_id,
                    updates[deal_id],
                    before_fields=(before_fields_by_deal or {}).get(deal_id),
                    verify=verify,
                )

    return results_map


def find_deal_by_user(user_id: int) -> Optional[Dict[str, Any]]:
    logger.info("FIND deal by user: user_id=%s, category_id=%s", user_id, B24_DEAL_CATEGORY_ID_USERS)

    filter_ = {
        UF_B24_DEAL_ABCP_USER_ID: str(user_id),
        "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,
    }
    select = ["ID", "TITLE", UF_B24_DEAL_ABCP_USER_ID]

    items: Any = _call("crm.deal.list", {"filter": filter_, "select": select, "start": 0}) or []
    if not isinstance(items, list):
        logger.warning("FIND unexpected result type: %s", type(items).__name__)
        return None

    logger.info("FIND result count=%s", len(items))
    if len(items) > 1:
        logger.warning(
            "FIND multiple deals matched user_id=%s; taking the first (deal_id=%s)",
            user_id, items[0].get("ID"),
        )

    return items[0] if items else None


def get_deal(deal_id: int) -> Dict[str, Any]:
    logger.info("GET deal: deal_id=%s", deal_id)
    res: Any = _call("crm.deal.get", {"id": int(deal_id)})
    if not isinstance(res, dict):
        logger.warning("GET deal unexpected type: %s (returning empty dict)", type(res).__name__)
        return {}
    return res


def get_deal_fields(deal_id: int, uf_codes: List[str]) -> Dict[str, Any]:
    deal = get_deal(deal_id)
    return {code: deal.get(code) for code in uf_codes}


def update_deal_fields(
    deal_id: int,
    fields: Dict[str, Any],
    *,
    before_fields: Optional[Dict[str, Any]] = None,
    verify: Optional[bool] = None,
) -> bool:
    if not fields:
        logger.info("UPDATE skip: deal_id=%s (no fields provided)", deal_id)
        return True

    if verify is None:
        verify = B24_VERIFY_UPDATES

    norm_fields = _normalize_fields_for_update(fields)
    logger.info("UPDATE start: deal_id=%s, field_keys=%s verify=%s", deal_id, list(norm_fields.keys()), verify)

    before = dict(before_fields or {})
    if not before:
        try:
            before = get_deal_fields(deal_id, list(norm_fields.keys()))
        except Exception as e:
            logger.warning("UPDATE warn: failed to fetch 'before' fields for deal_id=%s: %s", deal_id, e)
            before = {}

    try:
        changes_preview = [{"field": k, "from": before.get(k), "to": norm_fields.get(k)} for k in norm_fields.keys()]
        logger.debug("UPDATE diff(before): deal_id=%s changes=%s", deal_id, _preview_json(changes_preview))
    except Exception:
        pass

    _call(
        "crm.deal.update",
        {
            "id": int(deal_id),
            "fields": norm_fields,
            "params": {"REGISTER_SONET_EVENT": "N"},
        },
    )

    logger.info("UPDATE api-ok: deal_id=%s", deal_id)

    if not verify:
        return True

    try:
        after = get_deal_fields(deal_id, list(norm_fields.keys()))
    except Exception as e:
        logger.warning("UPDATE warn: failed to fetch 'after' fields for deal_id=%s: %s", deal_id, e)
        return True

    not_applied: List[str] = []
    for code, new_val in norm_fields.items():
        old = before.get(code)
        cur = after.get(code)
        same = (cur == new_val) or (cur is not None and str(cur) == str(new_val))
        if not same and str(old) == str(cur):
            not_applied.append(code)

    if not_applied:
        diag = []
        for code in not_applied:
            meta = _get_uf_meta_by_code(code)
            diag.append(
                {
                    "field": code,
                    "type": meta.get("USER_TYPE_ID"),
                    "multiple": meta.get("MULTIPLE"),
                    "hint": "Enumeration needs item IDs; integer/double needs numeric value; boolean needs Y/N; datetime needs ISO8601 with offset.",
                }
            )
        logger.warning("UPDATE verify: fields not applied for deal_id=%s -> %s", deal_id, _preview_json(diag))
        return False

    logger.debug("UPDATE verify: all fields applied for deal_id=%s", deal_id)
    return True

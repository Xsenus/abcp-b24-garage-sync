from __future__ import annotations  # поддержка аннотаций типов в рантайме (для Python <3.11)

import re
import time
import json
import logging
from functools import lru_cache
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, List, Optional, Union

import requests

# Импорты конфигурации
from .config import (
    B24_WEBHOOK_URL,
    B24_DEAL_CATEGORY_ID_USERS,
    UF_B24_DEAL_ABCP_USER_ID,
    REQUESTS_TIMEOUT,
    RATE_LIMIT_SLEEP,
    B24_TZ_OFFSET,  # таймзона по умолчанию для datetime UF
)

# ====== Время/таймзона ======
DEFAULT_TZ_OFFSET = B24_TZ_OFFSET
_FALLBACK_TZ = timezone(timedelta(hours=3))  # безопасный дефолт +03:00

logger = logging.getLogger("b24_client")
SESSION = requests.Session()

# -------------------- утилиты логирования/нормализации -----------------------

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
    if isinstance(v, (int, float)):
        return "Y" if v != 0 else "N"
    if isinstance(v, bool):
        return "Y" if v else "N"
    return "Y"

# --------------------------- Метаданные UF-полей ------------------------------

@lru_cache(maxsize=1)
def _deal_userfields_map() -> Dict[str, dict]:
    """Карта UF-полей сделки с пагинацией: FIELD_NAME -> meta."""
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

def _get_uf_meta_by_code(code: str) -> dict:
    mp = _deal_userfields_map()
    meta = mp.get(code)
    if not meta:
        _deal_userfields_map.cache_clear()
        mp = _deal_userfields_map()
        meta = mp.get(code)

    if not meta:
        return {}

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

def get_deal_userfield_map() -> Dict[str, dict]:
    """Публичный доступ к карте UF — для валидации маппинга в sync_service."""
    return _deal_userfields_map()

# ------------------------- Нормализация значений UF ---------------------------

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
                _id = int(it.get("ID"))
            except Exception:
                continue
            val = (it.get("VALUE") or "").strip().lower()
            xml = (it.get("XML_ID") or "").strip().lower()
            if val:
                mapping[val] = _id
            if xml:
                mapping[xml] = _id
    return mapping

def _ensure_enum_ids(code: str, v: Any, meta: dict) -> Union[int, List[int], str, Any]:
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
        key = s.lower()
        if key in mapping:
            return mapping[key]
        return x

    if is_multiple:
        if isinstance(v, (list, tuple, set)):
            return [_one(x) for x in v]
        else:
            return [_one(v)]
    else:
        return _one(v)

def _normalize_fields_for_update(fields: Dict[str, Any]) -> Dict[str, Any]:
    norm: Dict[str, Any] = {}
    for k, v in fields.items():
        if v is None:
            norm[k] = ""
            continue

        meta = _get_uf_meta_by_code(k)
        utype = (meta.get("USER_TYPE_ID") or "").lower()

        if utype == "datetime":
            norm[k] = _ensure_datetime_str(v, DEFAULT_TZ_OFFSET)
        elif utype == "date":
            norm[k] = _ensure_date_str(v)
        elif utype == "boolean":
            norm[k] = _to_bool_y_n(v)
        elif utype in {"integer", "double"}:
            norm[k] = _ensure_numeric(utype, v)
        elif utype == "enumeration":
            norm[k] = _ensure_enum_ids(k, v, meta)
        else:
            norm[k] = v

        logger.debug(
            "NORM: field=%s type=%s multiple=%s -> %s",
            k, utype or "<unknown>", meta.get("MULTIPLE"), _preview_json(norm[k], 200)
        )
    return norm

# --------------------------- низкоуровневые REST-вызовы -----------------------

def _call(method: str, params: dict) -> Any:
    url = f"{B24_WEBHOOK_URL}{method}"
    masked_url = _mask_url(url)

    logger.debug("B24 CALL start: method=%s url=%s params=%s",
                 method, masked_url, _preview_json(_safe_params_for_log(params)))

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
        logger.error("B24 CALL API error: method=%s url=%s error=%s description=%s",
                     method, masked_url, data.get("error"), data.get("error_description"))
        raise RuntimeError(_preview_json(data))

    logger.debug("B24 CALL ok: method=%s url=%s preview=%s", method, masked_url, _preview_json(data))

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        logger.debug("B24 CALL sleep: %.3f sec", RATE_LIMIT_SLEEP)
        time.sleep(RATE_LIMIT_SLEEP)

    if isinstance(data, dict) and "result" in data:
        return data["result"]
    return data

def _call_full(method: str, params: dict) -> dict:
    url = f"{B24_WEBHOOK_URL}{method}"
    masked_url = _mask_url(url)

    logger.debug("B24 CALL FULL start: method=%s url=%s params=%s",
                 method, masked_url, _preview_json(_safe_params_for_log(params)))

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
        logger.error("B24 CALL FULL API error: method=%s url=%s error=%s description=%s",
                     method, masked_url, data.get("error"), data.get("error_description"))
        raise RuntimeError(_preview_json(data))

    logger.debug("B24 CALL FULL ok: method=%s url=%s preview=%s", method, masked_url, _preview_json(data))

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        logger.debug("B24 CALL FULL sleep: %.3f sec", RATE_LIMIT_SLEEP)
        time.sleep(RATE_LIMIT_SLEEP)

    if not isinstance(data, dict):
        return {"result": data}
    return data

# ------------------------------- публичные API --------------------------------

def find_deal_by_user(user_id: int) -> Optional[Dict[str, Any]]:
    logger.info("FIND deal by user: user_id=%s, category_id=%s", user_id, B24_DEAL_CATEGORY_ID_USERS)

    filter_ = {
        UF_B24_DEAL_ABCP_USER_ID: str(user_id),
        "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,
    }
    select = ["ID", "TITLE", UF_B24_DEAL_ABCP_USER_ID]

    logger.debug("FIND filter=%s select=%s", _preview_json(filter_), select)

    items: Any = _call("crm.deal.list", {"filter": filter_, "select": select, "start": 0}) or []
    if not isinstance(items, list):
        logger.warning("FIND unexpected result type: %s", type(items).__name__)
        return None

    logger.info("FIND result count=%s", len(items))
    if len(items) > 1:
        logger.warning("FIND multiple deals matched user_id=%s; taking the first (deal_id=%s)",
                       user_id, items[0].get("ID"))

    deal: Optional[Dict[str, Any]] = items[0] if items else None
    logger.debug("FIND chosen deal=%s", _preview_json(deal) if deal else "None")
    return deal

def update_deal_fields(deal_id: int, fields: Dict[str, Any]) -> bool:
    if not fields:
        logger.info("UPDATE skip: deal_id=%s (no fields provided)", deal_id)
        return True

    norm_fields = _normalize_fields_for_update(fields)

    logger.info("UPDATE start: deal_id=%s, field_keys=%s", deal_id, list(norm_fields.keys()))

    try:
        before = get_deal_fields(deal_id, list(norm_fields.keys()))
    except Exception as e:
        logger.warning("UPDATE warn: failed to fetch 'before' fields for deal_id=%s: %s", deal_id, e)
        before = {}

    try:
        changes_preview = [{"field": k, "from": before.get(k, None), "to": norm_fields.get(k)} for k in norm_fields.keys()]
        logger.debug("UPDATE diff(before): deal_id=%s changes=%s", deal_id, _preview_json(changes_preview))
    except Exception:
        pass

    _call("crm.deal.update", {
        "id": int(deal_id),
        "fields": norm_fields,
        "params": {"REGISTER_SONET_EVENT": "N"}
    })

    logger.info("UPDATE api-ok: deal_id=%s", deal_id)

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        logger.debug("UPDATE sleep: %.3f sec", RATE_LIMIT_SLEEP)
        time.sleep(RATE_LIMIT_SLEEP)

    try:
        after = get_deal_fields(deal_id, list(norm_fields.keys()))
    except Exception as e:
        logger.warning("UPDATE warn: failed to fetch 'after' fields for deal_id=%s: %s", deal_id, e)
        return True

    not_applied: List[str] = []
    for k, new_val in norm_fields.items():
        old = before.get(k)
        cur = after.get(k)
        same = (cur == new_val) or (cur is not None and str(cur) == str(new_val))
        if not same and str(old) == str(cur):
            not_applied.append(k)

    if not_applied:
        diag = []
        for code in not_applied:
            meta = _get_uf_meta_by_code(code)
            diag.append({
                "field": code,
                "type": meta.get("USER_TYPE_ID"),
                "multiple": meta.get("MULTIPLE"),
                "hint": "Для enumeration нужны ID пунктов; для integer/double — число; boolean — Y/N; datetime — ISO8601 с офсетом.",
            })
        logger.warning("UPDATE verify: fields not applied for deal_id=%s -> %s", deal_id, _preview_json(diag))
        return False

    logger.debug("UPDATE verify: all fields applied for deal_id=%s", deal_id)
    return True

def get_deal(deal_id: int) -> Dict[str, Any]:
    logger.info("GET deal: deal_id=%s", deal_id)
    res: Any = _call("crm.deal.get", {"id": int(deal_id)})
    if not isinstance(res, dict):
        logger.warning("GET deal unexpected type: %s (returning empty dict)", type(res).__name__)
        return {}
    logger.debug("GET deal ok: keys=%s", list(res.keys()))
    return res

def get_deal_fields(deal_id: int, uf_codes: List[str]) -> Dict[str, Any]:
    deal = get_deal(deal_id)
    current: Dict[str, Any] = {}
    for code in uf_codes:
        current[code] = deal.get(code)
    logger.debug("GET deal fields: deal_id=%s fields=%s", deal_id, uf_codes)
    return current

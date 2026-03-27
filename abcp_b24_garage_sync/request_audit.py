from __future__ import annotations

import json
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from .config import REQUEST_AUDIT_ENABLED
from .log_setup import _resolve_log_dir


_AUDIT_FILE_PREFIX = "http-requests"
_SECRET_MASK = "********"
_WRITE_LOCK = threading.Lock()
_SECRET_KEY_PARTS = (
    "authorization",
    "password",
    "passwd",
    "pass",
    "psw",
    "secret",
    "token",
    "apikey",
    "api_key",
    "access_token",
    "refresh_token",
    "userlogin",
)


def _is_secret_key(key: Any) -> bool:
    normalized = str(key or "").strip().lower()
    return any(part in normalized for part in _SECRET_KEY_PARTS)


def sanitize_for_audit(value: Any, *, key: str | None = None) -> Any:
    if key is not None and _is_secret_key(key):
        return _SECRET_MASK

    if isinstance(value, Mapping):
        return {
            str(item_key): sanitize_for_audit(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_audit(item) for item in value]

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    return str(value)


def _response_preview(value: Any, *, limit: int = 4000) -> str | None:
    if value is None:
        return None

    safe_value = sanitize_for_audit(value)
    if isinstance(safe_value, str):
        rendered = safe_value
    else:
        try:
            rendered = json.dumps(safe_value, ensure_ascii=False, sort_keys=True)
        except Exception:
            rendered = str(safe_value)

    if len(rendered) <= limit:
        return rendered
    return rendered[:limit] + "...(truncated)"


def _coerce_timestamp(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now().astimezone()
    if value.tzinfo is None:
        return value.astimezone()
    return value


def _audit_path(timestamp: datetime) -> Path:
    return _resolve_log_dir() / f"{_AUDIT_FILE_PREFIX}-{timestamp.date().isoformat()}.jsonl"


def audit_http_transaction(
    *,
    service: str,
    method: str,
    url: str,
    request_payload: Any = None,
    request_headers: Mapping[str, Any] | None = None,
    started_at: datetime | None = None,
    elapsed_ms: float | None = None,
    status_code: int | None = None,
    ok: bool,
    outcome: str,
    response_body: Any = None,
    response_content_length: int | None = None,
    error: str | None = None,
    meta: Mapping[str, Any] | None = None,
) -> None:
    if not REQUEST_AUDIT_ENABLED:
        return

    timestamp = _coerce_timestamp(started_at)
    entry = {
        "timestamp": timestamp.isoformat(timespec="milliseconds"),
        "service": service,
        "request": {
            "method": method.upper(),
            "url": url,
            "headers": sanitize_for_audit(dict(request_headers or {})),
            "payload": sanitize_for_audit(request_payload),
        },
        "response": {
            "ok": bool(ok),
            "outcome": outcome,
            "status_code": status_code,
            "duration_ms": round(float(elapsed_ms), 3) if elapsed_ms is not None else None,
            "content_length": response_content_length,
            "body_preview": _response_preview(response_body),
            "error": error,
        },
    }

    if meta:
        entry["meta"] = sanitize_for_audit(dict(meta))

    payload = json.dumps(entry, ensure_ascii=False) + "\n"
    path = _audit_path(timestamp)

    with _WRITE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(payload)

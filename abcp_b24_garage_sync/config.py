from __future__ import annotations
import os, json
from typing import Dict, Tuple, Optional

# ---------- helpers ----------

_TRUTHY = {"1","true","yes","y","on","да","истина","ok"}
_FALSY  = {"0","false","no","n","off","нет","ложь"}

def getenv_str(name: str, default: Optional[str]=None, *, strip: bool=True, empty_to_none: bool=True) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip() if strip else v
    if empty_to_none and v == "":
        return None if default is None else default
    return v

def getenv_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in _TRUTHY: return True
    if s in _FALSY:  return False
    try:
        return bool(int(s))
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None: return default
    s = v.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None: return default
    try:
        return int(v.strip())
    except Exception:
        return default

def getenv_json(name: str, default):
    v = os.getenv(name)
    if v is None: return default
    try:
        return json.loads(v)
    except Exception:
        return default

# ---------- ABCP ----------

ABCP_BASE_URL = getenv_str("ABCP_BASE_URL", "https://abcp61741.public.api.abcp.ru/cp/garage/") or "https://abcp61741.public.api.abcp.ru/cp/garage/"
ABCP_USERLOGIN = getenv_str("ABCP_USERLOGIN")
ABCP_USERPSW   = getenv_str("ABCP_USERPSW")
ABCP_LIMIT     = getenv_int("ABCP_LIMIT", 500)

# ---------- Bitrix24 ----------

_raw_b24 = getenv_str("B24_WEBHOOK_URL", "")
B24_WEBHOOK_URL = (_raw_b24.rstrip("/") + "/") if _raw_b24 else ""

B24_DEAL_CATEGORY_ID_USERS = getenv_int("B24_DEAL_CATEGORY_ID_USERS", 0)
B24_DEAL_TITLE_PREFIX      = getenv_str("B24_DEAL_TITLE_PREFIX", "ABCP Регистрация:") or "ABCP Регистрация:"
UF_B24_DEAL_ABCP_USER_ID   = getenv_str("UF_B24_DEAL_ABCP_USER_ID")

# Для нормализации datetime в UF
B24_TZ_OFFSET = getenv_str("B24_TZ_OFFSET", "+03:00") or "+03:00"

# Привязка ABCP-полей к env-именам с UF-кодами
BITRIX_FIELD_ENV_MAP: Dict[str, Tuple[str, ...]] = {
    "id":               ("UF_B24_DEAL_GARAGE_ID",),
    "userId":           ("UF_B24_DEAL_GARAGE_USER_ID", "UF_B24_DEAL_ABCP_USER_ID"),
    "name":             ("UF_B24_DEAL_GARAGE_NAME",),
    "comment":          ("UF_B24_DEAL_GARAGE_COMMENT",),
    "year":             ("UF_B24_DEAL_GARAGE_YEAR",),
    "vin":              ("UF_B24_DEAL_GARAGE_VIN",),
    "frame":            ("UF_B24_DEAL_GARAGE_FRAME",),
    "mileage":          ("UF_B24_DEAL_GARAGE_MILEAGE",),
    "manufacturerId":   ("UF_B24_DEAL_GARAGE_MANUFACTURER_ID",),
    "manufacturer":     ("UF_B24_DEAL_GARAGE_MANUFACTURER",),
    "modelId":          ("UF_B24_DEAL_GARAGE_MODEL_ID",),
    "model":            ("UF_B24_DEAL_GARAGE_MODEL",),
    "modificationId":   ("UF_B24_DEAL_GARAGE_MODIFICATION_ID",),
    "modification":     ("UF_B24_DEAL_GARAGE_MODIFICATION",),
    "dateUpdated":      ("UF_B24_DEAL_GARAGE_DATE_UPDATED",),
    "vehicleRegPlate":  ("UF_B24_DEAL_GARAGE_VEHICLE_REG_PLATE",),
}

# ---------- Storage ----------
SQLITE_PATH = getenv_str("SQLITE_PATH", "abcp_b24.s3db") or "abcp_b24.s3db"

# ---------- HTTP / Limits ----------
REQUESTS_TIMEOUT        = getenv_int("REQUESTS_TIMEOUT", 20)
REQUESTS_RETRIES        = getenv_int("REQUESTS_RETRIES", 3)
REQUESTS_RETRY_BACKOFF  = getenv_float("REQUESTS_RETRY_BACKOFF", 1.5)
RATE_LIMIT_SLEEP        = getenv_float("RATE_LIMIT_SLEEP", 0.2)

# ---------- Sync behavior ----------
SYNC_OVERWRITE_DEFAULT = getenv_bool("SYNC_OVERWRITE_DEFAULT", True)
SYNC_OVERWRITE_FIELDS: Dict[str, bool] = {}
_tmp = getenv_json("SYNC_OVERWRITE_FIELDS", {})
if isinstance(_tmp, dict):
    for k, v in _tmp.items():
        try:
            SYNC_OVERWRITE_FIELDS[str(k)] = bool(v)
        except Exception:
            pass

SYNC_PAUSE_BETWEEN_USERS = getenv_float("SYNC_PAUSE_BETWEEN_USERS", 0.0)
SYNC_PAUSE_BETWEEN_DEALS = getenv_float("SYNC_PAUSE_BETWEEN_DEALS", 0.0)

LOG_LEVEL = getenv_str("LOG_LEVEL", "INFO") or "INFO"

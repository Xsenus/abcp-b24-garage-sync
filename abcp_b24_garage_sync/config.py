
from __future__ import annotations
import os, json

BOOL_TRUE = {"1","true","yes","y","on","да","истина"}

def getenv_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in BOOL_TRUE

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v is not None else default

ABCP_BASE_URL = os.getenv("ABCP_BASE_URL", "https://abcp61741.public.api.abcp.ru/cp/garage/")
ABCP_USERLOGIN = os.getenv("ABCP_USERLOGIN")
ABCP_USERPSW   = os.getenv("ABCP_USERPSW")
ABCP_LIMIT     = int(os.getenv("ABCP_LIMIT", "500"))

B24_WEBHOOK_URL = os.getenv("B24_WEBHOOK_URL")
B24_DEAL_CATEGORY_ID_USERS = int(os.getenv("B24_DEAL_CATEGORY_ID_USERS", "0"))
B24_DEAL_TITLE_PREFIX = os.getenv("B24_DEAL_TITLE_PREFIX", "ABCP Регистрация:")
UF_B24_DEAL_ABCP_USER_ID = os.getenv("UF_B24_DEAL_ABCP_USER_ID")

# BITRIX_FIELD_ENV_MAP может содержать один или несколько env-переменных для каждого поля.
# Каждая непустая переменная будет использована как отдельный UF-код, поэтому можно
# синхронизировать одно и то же значение в несколько полей. При отсутствии первых кодов
# значение автоматически отправится в доступные «fallback»-поля.
BITRIX_FIELD_ENV_MAP: dict[str, tuple[str, ...]] = {
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

SQLITE_PATH = os.getenv("SQLITE_PATH", "abcp_b24.s3db")

REQUESTS_TIMEOUT = int(os.getenv("REQUESTS_TIMEOUT", "20"))
REQUESTS_RETRIES = int(os.getenv("REQUESTS_RETRIES", "3"))
REQUESTS_RETRY_BACKOFF = float(os.getenv("REQUESTS_RETRY_BACKOFF", "1.5"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "0.2"))

SYNC_OVERWRITE_DEFAULT = getenv_bool("SYNC_OVERWRITE_DEFAULT", True)
try:
    SYNC_OVERWRITE_FIELDS: dict[str,bool] = {k: bool(v) for k,v in json.loads(os.getenv("SYNC_OVERWRITE_FIELDS","{}")).items()}
except Exception:
    SYNC_OVERWRITE_FIELDS = {}

SYNC_PAUSE_BETWEEN_USERS = getenv_float("SYNC_PAUSE_BETWEEN_USERS", 0.0)
SYNC_PAUSE_BETWEEN_DEALS = getenv_float("SYNC_PAUSE_BETWEEN_DEALS", 0.0)

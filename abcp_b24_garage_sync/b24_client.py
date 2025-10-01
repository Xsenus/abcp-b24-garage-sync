from __future__ import annotations  # поддержка аннотаций типов в рантайме (для Python <3.11)

import time  # для паузы между запросами (RATE_LIMIT_SLEEP)
import json  # для безопасного логирования структур
import logging  # стандартный логгер Python
from typing import Any, Dict, List, Optional  # явные типы для Pylance
import requests  # HTTP-клиент

# Импорты конфигурации из нашего пакета (значения приходят из .env, загружается в main.py)
from .config import (
    B24_WEBHOOK_URL,               # базовый вебхук Bitrix24 (содержит домен, ID пользователя и токен)
    B24_DEAL_CATEGORY_ID_USERS,    # ID воронки сделок, где ищем
    UF_B24_DEAL_ABCP_USER_ID,      # UF-поле сделки, в котором лежит userId из ABCP
    REQUESTS_TIMEOUT,              # таймаут HTTP-запроса
    RATE_LIMIT_SLEEP,              # пауза между запросами, чтобы не спамить API
)

# Создаём модульный логгер; формат и уровень задаются в setup_logging()
logger = logging.getLogger("b24_client")

# Создаём сессию requests один раз на модуль (reuse TCP-соединений)
SESSION = requests.Session()  # HTTP-сессия (connection pooling)

def _mask_url(url: str) -> str:
    """
    Маскируем секретную часть вебхука Bitrix24 в URL, чтобы не утек токен в логи.
    Пример:
      https://example.bitrix24.ru/rest/39768/qj1s459snz44ovce/ -> https://example.bitrix24.ru/rest/39768/********/
    """
    try:
        head, rest = url.split("/rest/", 1)          # часть до /rest/ и после
        parts = rest.split("/")                      # rest: userId/token/optional_path...
        if len(parts) >= 2:
            parts[1] = "********"                    # маскируем токен
        return head + "/rest/" + "/".join(parts)
    except Exception:
        return url

def _safe_params_for_log(params: dict) -> dict:
    """
    Возвращаем «обеззараженную» версию params для логов:
    - не логируем значения полей (fields), только список ключей;
    - для крупных структур пишем только тип и длину.
    """
    try:
        view: Dict[str, Any] = {}
        for k, v in (params or {}).items():
            if k == "fields" and isinstance(v, dict):
                view[k] = {"__keys__": list(v.keys())}
            elif isinstance(v, (dict, list, tuple)):
                view[k] = {"__type__": type(v).__name__, "__len__": len(v)}
            else:
                view[k] = v
        return view
    except Exception:
        return {"info": "params present, failed to render safely"}

def _call(method: str, params: dict) -> Any:
    """
    Универсальный вызов Bitrix24 REST через вебхук.
    Логируем каждый шаг: метод, URL (замаскирован), параметры (без чувствительных значений),
    HTTP-статус, превью ответа, паузу.
    """
    url = f"{B24_WEBHOOK_URL}{method}"               # полный URL метода
    masked_url = _mask_url(url)                      # замаскированный для логов

    logger.debug("B24 CALL start: method=%s url=%s params=%s",
                 method, masked_url, json.dumps(_safe_params_for_log(params), ensure_ascii=False))

    r = SESSION.post(url, json=params, timeout=REQUESTS_TIMEOUT)  # сам запрос
    logger.debug("B24 CALL response: status=%s, content_length=%s", r.status_code, len(r.content or b""))

    try:
        data: Any = r.json()                         # пытаемся разбрать JSON
    except Exception:
        snippet = (r.text or "")[:500]
        logger.error("B24 CALL non-JSON response (snippet): %s", snippet)
        r.raise_for_status()
        raise

    if isinstance(data, dict) and "error" in data:   # ошибка на уровне Bitrix API
        logger.error("B24 CALL API error: method=%s url=%s error=%s description=%s",
                     method, masked_url, data.get("error"), data.get("error_description"))
        raise RuntimeError(json.dumps(data, ensure_ascii=False))

    # успешный ответ: логируем превью
    try:
        preview = json.dumps(data, ensure_ascii=False)
        if len(preview) > 800:
            preview = preview[:800] + "...(truncated)"
    except Exception:
        preview = f"<{type(data).__name__}>"
    logger.debug("B24 CALL ok: method=%s url=%s preview=%s", method, masked_url, preview)

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:    # пауза между запросами
        logger.debug("B24 CALL sleep: %.3f sec", RATE_LIMIT_SLEEP)
        time.sleep(RATE_LIMIT_SLEEP)

    if isinstance(data, dict) and "result" in data:
        return data["result"]
    return data                                   # тип намеренно Any — выше строго приведём

def find_deal_by_user(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Находим первую сделку по UF-полю с ABCP userId в указанной воронке.
    Возвращаем словарь сделки или None, если ничего не нашли.
    """
    logger.info("FIND deal by user: user_id=%s, category_id=%s", user_id, B24_DEAL_CATEGORY_ID_USERS)

    filter_ = {
        UF_B24_DEAL_ABCP_USER_ID: str(user_id),     # UF сравниваем как строку
        "CATEGORY_ID": B24_DEAL_CATEGORY_ID_USERS,  # ограничиваемся нужной воронкой
    }
    select = ["ID", "TITLE", UF_B24_DEAL_ABCP_USER_ID]

    logger.debug("FIND filter=%s select=%s", json.dumps(filter_, ensure_ascii=False), select)

    items: Any = _call("crm.deal.list", {"filter": filter_, "select": select, "start": 0}) or []
    if not isinstance(items, list):
        logger.warning("FIND unexpected result type: %s", type(items).__name__)
        return None

    logger.info("FIND result count=%s", len(items))
    if len(items) > 1:
        logger.warning("FIND multiple deals matched user_id=%s; taking the first (deal_id=%s)", user_id, items[0].get("ID"))

    deal: Optional[Dict[str, Any]] = items[0] if items else None
    logger.debug("FIND chosen deal=%s", json.dumps(deal, ensure_ascii=False) if deal else "None")
    return deal

def update_deal_fields(deal_id: int, fields: Dict[str, Any]) -> bool:
    """
    Обновляем UF-поля сделки. Возвращаем True при успешном вызове.
    В лог выводим только ключи полей — без значений.
    """
    if not fields:
        logger.info("UPDATE skip: deal_id=%s (no fields provided)", deal_id)
        return True

    logger.info("UPDATE start: deal_id=%s, field_keys=%s", deal_id, list(fields.keys()))

    _call("crm.deal.update", {"id": int(deal_id), "fields": fields})

    logger.info("UPDATE ok: deal_id=%s", deal_id)

    if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
        logger.debug("UPDATE sleep: %.3f sec", RATE_LIMIT_SLEEP)
        time.sleep(RATE_LIMIT_SLEEP)

    return True

def get_deal(deal_id: int) -> Dict[str, Any]:
    """
    Возвращает полную карточку сделки (crm.deal.get) как dict.
    Если Bitrix по какой-то причине вернул не dict — логируем и возвращаем {}.
    """
    logger.info("GET deal: deal_id=%s", deal_id)
    res: Any = _call("crm.deal.get", {"id": int(deal_id)})
    if not isinstance(res, dict):
        logger.warning("GET deal unexpected type: %s (returning empty dict)", type(res).__name__)
        return {}
    logger.debug("GET deal ok: keys=%s", list(res.keys()))
    return res

def get_deal_fields(deal_id: int, uf_codes: List[str]) -> Dict[str, Any]:
    """
    Возвращает только интересующие UF-поля из сделки как dict {UF_CODE: value}.
    """
    deal = get_deal(deal_id)
    current: Dict[str, Any] = {}
    for code in uf_codes:
        current[code] = deal.get(code)
    logger.debug("GET deal fields: deal_id=%s fields=%s", deal_id, uf_codes)
    return current

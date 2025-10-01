from __future__ import annotations  # корректная работа аннотаций типов в рантайме

import time  # пауза между запросами (RATE_LIMIT_SLEEP)
import json  # компактное и безопасное логирование структур
import logging  # стандартная система логов
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse  # чтобы маскировать креды в URL
import requests  # HTTP-клиент
from requests.adapters import HTTPAdapter, Retry  # адаптер и стратегия ретраев

# Конфигурация подтягивается из .env (который грузится в main.py до наших импортов)
from .config import (
    ABCP_BASE_URL,            # базовый URL эндпоинта гаража
    ABCP_USERLOGIN,           # логин ABCP
    ABCP_USERPSW,             # пароль/токен ABCP
    REQUESTS_TIMEOUT,         # таймаут HTTP-запросов
    REQUESTS_RETRIES,         # количество ретраев
    REQUESTS_RETRY_BACKOFF,   # backoff-фактор между повторами
    RATE_LIMIT_SLEEP,         # пауза между запросами для бережности
)

# Модульный логгер для этого клиента
logger = logging.getLogger("abcp_client")

def _mask_url_qs(url: str) -> str:
    """
    Маскируем чувствительные query-параметры в URL (userpsw, userlogin), чтобы их можно было безопасно логировать.
    """
    try:
        # Разбираем URL на компоненты
        p = urlparse(url)
        # Парсим query в список пар, чтобы безопасно модифицировать значения
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        # Подменяем секреты, если они есть
        if "userpsw" in q:
            q["userpsw"] = "********"
        if "userlogin" in q:
            # логин не секретный на 100%, но скроем тоже, чтобы не светить лишний раз
            q["userlogin"] = "********"
        # Собираем строку запроса обратно
        new_q = urlencode(q, doseq=True)
        # Возвращаем новый URL с замаскированными параметрами
        return urlunparse(p._replace(query=new_q))
    except Exception:
        # На всякий случай — если что-то пошло не так, лучше вернуть исходный URL, чем падать
        return url

def _session() -> requests.Session:
    """
    Создаём HTTP-сессию с ретраями, чтобы повторять временные ошибки (429/5xx).
    """
    # Инициализируем сессию
    s = requests.Session()
    # Стратегия ретраев: сколько попыток, backoff, на какие коды
    r = Retry(
        total=REQUESTS_RETRIES,                      # общее число повторов
        backoff_factor=REQUESTS_RETRY_BACKOFF,      # пауза между повторами: 0.0, 1.5, 3.0, ...
        status_forcelist=(429, 500, 502, 503, 504), # повторяем только эти коды
        allowed_methods=("GET", "POST"),            # для каких методов разрешены ретраи
        raise_on_status=False,                      # не бросать сразу — разберём сами
    )
    # Прикручиваем адаптер с ретраями на http/https
    ad = HTTPAdapter(max_retries=r)
    s.mount("http://", ad)
    s.mount("https://", ad)
    logger.debug("ABCP session configured: retries=%s backoff=%.2f timeout=%s",
                 REQUESTS_RETRIES, REQUESTS_RETRY_BACKOFF, REQUESTS_TIMEOUT)
    # Возвращаем готовую сессию
    return s

# Держим одну сессию на модуль (пул соединений)
SESSION = _session()

class AbcpConfigError(RuntimeError):
    """Исключение конфигурации ABCP (нет логина/пароля и т.п.)."""
    pass

def _require_creds():
    """
    Валидируем, что логин и пароль заданы. Если нет — сразу бьём тревогу, чтобы не делать пустой запрос.
    """
    if not ABCP_USERLOGIN or not ABCP_USERPSW:
        logger.error("ABCP credentials missing: login=%s password=%s",
                     "set" if ABCP_USERLOGIN else "unset",
                     "set" if ABCP_USERPSW else "unset")
        raise AbcpConfigError(
            "ABCP_USERLOGIN/ABCP_USERPSW не заданы. Проверьте .env (он должен грузиться в main.py)."
        )
    else:
        logger.debug("ABCP credentials present (values masked in logs)")

def _candidate_urls(base: str) -> list[str]:
    """
    Формируем список кандидатных URL (со слэшем/без, вариант /list),
    плюс автопочинка если по ошибке указан /cp/users.
    """
    # Берём базу из конфига или дефолтную
    b = (base or "").strip() or "https://abcp61741.public.api.abcp.ru/cp/garage"
    # Убираем завершающий слэш, будем явно его добавлять дальше
    b = b.rstrip("/")
    # Базовый набор вариантов
    cands = [b, b + "/", b + "/list"]
    # Если случайно оставили /cp/users — попытка автоисправления на /cp/garage
    if cands[0].endswith("/cp/users"):
        fixed = cands[0].rsplit("/", 1)[0] + "/garage"
        cands = [fixed, fixed + "/", fixed + "/list"]
    # Уникализируем, сохраняя порядок
    seen, out = set(), []
    for u in cands:
        if u not in seen:
            seen.add(u)
            out.append(u)
    logger.debug("ABCP candidate URLs: %s", out)
    return out

def _is_empty_not_found(status_code: int, data: dict | None, text: str | None) -> bool:
    """
    Ряд стендов ABCP при отсутствии данных возвращают 404 с JSON:
      {"errorCode":301,"errorMessage":"Автомобили не найдены"}
    Это не ошибка логики — это «пустой результат», и его нужно трактовать как {}.
    """
    # Подготовка полей
    msg = ""
    code = None
    if isinstance(data, dict):
        code = data.get("errorCode")
        msg = (data.get("errorMessage") or "")
    msg_l = (msg or text or "").lower()
    # Правила «пустоты»: 404 + код 301/404 или характерная фраза
    return (
        status_code == 404 and (
            str(code) in {"301", "404"} or
            "не найден" in msg_l or
            "not found" in msg_l
        )
    )

def fetch_garage(start, end) -> dict:
    """
    Запрашиваем «гараж» ABCP за указанный интервал дат.
    Возвращаем dict { "<userId>": [ {...car...}, ... ], ... } или {} если записей нет.
    Логируем: интервал, кандидатные URL, каждый запрос/ответ (статус, сниппет), итоговое количество записей.
    """
    # Гарантируем, что креды есть
    _require_creds()

    # Готовим параметры запроса (дату форматируем под API)
    params = {
        "userlogin": ABCP_USERLOGIN,                                 # логин
        "userpsw": ABCP_USERPSW,                                     # пароль/токен
        "dateUpdatedStart": start.strftime("%Y-%m-%d %H:%M:%S"),     # начало интервала
        "dateUpdatedEnd": end.strftime("%Y-%m-%d %H:%M:%S"),         # конец интервала
    }
    headers = {"Accept": "application/json"}  # просим JSON

    # Логируем старт операции
    logger.info("ABCP FETCH start: %s → %s", params["dateUpdatedStart"], params["dateUpdatedEnd"])
    logger.debug("ABCP params (masked in URL): userlogin=user**** userpsw=****")

    # Сюда будем собирать диагностику по неуспешным попыткам
    errors: list[tuple[int, str, str]] = []

    # Перебираем кандидаты URL до первого успешного ответа
    for url in _candidate_urls(ABCP_BASE_URL):
        # Старт запроса — логируем замаскированный URL (креды скрыты)
        masked_url = _mask_url_qs(url + "?" + urlencode({**params}))
        logger.debug("ABCP GET try: %s", masked_url)

        # Делаем запрос
        r = SESSION.get(url, params=params, headers=headers, timeout=REQUESTS_TIMEOUT)

        # Логируем базовую информацию об ответе
        logger.debug("ABCP RESP status=%s len=%s url=%s",
                     r.status_code, len(r.content or b""), _mask_url_qs(r.url))

        # Успех: HTTP 200
        if r.status_code == 200:
            # Пытаемся распарсить JSON
            try:
                data = r.json()
            except Exception:
                # Если пришёл не JSON — логируем фрагмент и бросаем ошибку статуса
                snippet = (r.text or "")[:500]
                logger.error("ABCP non-JSON 200 response (snippet): %s", snippet)
                r.raise_for_status()
                raise  # теоретически не дойдём, т.к. выше бросит

            # Некоторые стенды возвращают 200 с errorCode=301 — тоже трактуем как пусто
            if isinstance(data, dict) and str(data.get("errorCode")) in {"301", "404"}:
                logger.info("ABCP FETCH empty (200 with errorCode=%s)", data.get("errorCode"))
                return {}

            # Посчитаем, сколько записей получили (сумма длин списков по пользователям)
            total = 0
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        total += len(v)

            # Логируем успех, но без гигантского дампа
            logger.info("ABCP FETCH ok: items=%s", total)
            preview = json.dumps({k: (len(v) if isinstance(v, list) else "obj")
                                  for k, v in (data or {}).items()}) if isinstance(data, dict) else "non-dict"
            if isinstance(preview, str) and len(preview) > 800:
                preview = preview[:800] + "...(truncated)"
            logger.debug("ABCP FETCH preview: %s", preview)

            # Делаем паузу, если настроено, и возвращаем данные
            if RATE_LIMIT_SLEEP and RATE_LIMIT_SLEEP > 0:
                logger.debug("ABCP sleep: %.3f sec", RATE_LIMIT_SLEEP)
                time.sleep(RATE_LIMIT_SLEEP)
            return data or {}

        # Если не 200 — пробуем распознать «пустой» случай
        try:
            data = r.json()
        except Exception:
            data = None

        if _is_empty_not_found(r.status_code, data, r.text):
            # Пустой интервал (типично 404 + errorCode=301) — это НЕ ошибка
            logger.info("ABCP FETCH empty (status=%s, interval without cars)", r.status_code)
            return {}

        # Не пустой и не 200 — собираем сниппет для диагностики и решаем, идти ли дальше
        snippet = (r.text or "")[:500]
        logger.warning("ABCP FETCH non-200: status=%s url=%s snippet=%s",
                       r.status_code, _mask_url_qs(r.url), snippet)
        errors.append((r.status_code, r.url, snippet))

        # Если код не «кандидатский» (404/редиректы), дальше не мучаем — бросаем ошибку
        if r.status_code not in (404, 301, 302):
            r.raise_for_status()

    # Если ни один кандидат не сработал — формируем понятное исключение и логируем
    lines = ["ABCP responded non-200 for all candidate URLs:"]
    for st, u, sn in errors:
        lines.append(f"  [{st}] {_mask_url_qs(u)}\n    {sn}")
    msg = "\n".join(lines)
    logger.error(msg)
    raise requests.HTTPError(msg)

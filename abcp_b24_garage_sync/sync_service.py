from __future__ import annotations

import json
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from os import getenv

from .config import (
    BITRIX_FIELD_ENV_MAP,
    SYNC_OVERWRITE_DEFAULT,
    SYNC_OVERWRITE_FIELDS,
    SYNC_PAUSE_BETWEEN_USERS,
    SYNC_PAUSE_BETWEEN_DEALS,
)
from .db import connect, save_sync_result
from .b24_client import find_deal_by_user, update_deal_fields, get_deal_fields

log = logging.getLogger("sync_service")

# ---------- вспомогательные утилиты логирования ----------

def _preview(val: Any, maxlen: int = 120) -> str:
    """Безопасный превью для значений полей (обрезаем, сериализуем)."""
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
    """Правило перезаписи для поля: SYNC_OVERWRITE_FIELDS[name] или дефолт."""
    return SYNC_OVERWRITE_FIELDS.get(name, SYNC_OVERWRITE_DEFAULT)

# ---------- построение набора UF-полей ----------

def _build_update_fields(row) -> Dict[str, Any]:
    """
    Строим словарь {UF_CODE: value} для Bitrix24 по одной записи из таблицы garage.
    Логируем принятие каждого решения: какой UF-код, что в значении, и почему мог быть пропуск.
    """
    fields: Dict[str, Any] = {}
    for abcp_key, env_name in BITRIX_FIELD_ENV_MAP.items():
        uf_code = getenv(env_name, "")
        if not uf_code:
            log.debug("BUILD: skip abcp_key=%s (env %s not set -> no UF code)", abcp_key, env_name)
            continue

        value = row[abcp_key]
        allow_overwrite = _overwrite_for_field(abcp_key)

        # если перезапись выключена — не затираем пустыми
        if not allow_overwrite and value in (None, "", 0):
            log.debug(
                "BUILD: skip abcp_key=%s -> UF=%s (overwrite=False and new value is empty)",
                abcp_key, uf_code
            )
            continue

        if isinstance(value, (dict, list)):
            log.debug("BUILD: abcp_key=%s is %s -> JSON stringify", abcp_key, type(value).__name__)
            value = json.dumps(value, ensure_ascii=False)

        # Bitrix любит строки — приводим к строке, пустое -> ""
        prepared = "" if value is None else str(value)
        fields[uf_code] = prepared

        log.debug(
            "BUILD: set UF=%s (from abcp_key=%s, overwrite=%s) value_preview=%s",
            uf_code, abcp_key, allow_overwrite, _preview(prepared)
        )
    log.debug("BUILD: total UF fields prepared=%s", len(fields))
    return fields

# ---------- сравнение «было/стало» ----------

def _normalize(v: Any) -> str:
    """
    Нормализуем значение для сравнения:
      - None и пустая строка считаются одинаковыми;
      - приводим к строке (как и шлём в Bitrix).
    """
    if v is None:
        return ""
    return str(v)

def _diff_fields(current: Dict[str, Any], new_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает только те поля, где значение реально изменится. Подробно логирует сравнение.
    """
    diff: Dict[str, Any] = {}
    for code, new_val in (new_fields or {}).items():
        cur_val = current.get(code)
        cur_n = _normalize(cur_val)
        new_n = _normalize(new_val)
        if cur_n != new_n:
            diff[code] = new_val
            log.debug("DIFF: UF=%s changed: %s -> %s", code, _preview(cur_n), _preview(new_n))
        else:
            log.debug("DIFF: UF=%s unchanged", code)
    log.debug("DIFF: changed fields count=%s", len(diff))
    return diff

# ---------- выбор «последних» записей на пользователя ----------

def _select_latest_rows_per_user(c, only_user: Optional[int] = None):
    """
    Достаём по 1 записи на userId — самую свежую по dateUpdated.
    """
    if only_user:
        log.debug("SELECT: latest row for single userId=%s", only_user)
        sql = """
        SELECT *
        FROM garage
        WHERE userId = ?
        ORDER BY datetime(dateUpdated) DESC
        LIMIT 1
        """
        params = (only_user,)
    else:
        log.debug("SELECT: latest rows per each user")
        # для всех пользователей: join на подзапрос с MAX(dateUpdated)
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

# ---------- основной алгоритм синхронизации ----------

def sync_all(user_id: Optional[int] = None) -> Tuple[int, int, int]:
    """
    Синхронизация:
      1) выбираем «по одной последней записи на пользователя»;
      2) ищем сделку в Bitrix24 по userId;
      3) строим набор UF-полей и считаем diff с текущими значениями сделки;
      4) если есть изменения — обновляем только изменившиеся UF.
    Фиксируем исход и метаданные в БД (sync_status + sync_audit).
    Возвращаем (updated, skipped, errors).
    """
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
            # аудит (без сделки)
            save_sync_result(
                user_id=0,
                deal_id=None,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="skipped",
                updated_field_codes=None,
                error="row without userId",
            )
            continue

        log.info(
            "SYNC %d/%d: userId=%s row_id=%s dateUpdated=%s -> find deal",
            i, len(rows), uid, garage_id, dt_updated
        )

        # Поиск сделки по пользователю (подробно логирует сам b24_client)
        deal = find_deal_by_user(uid)
        if not deal:
            skipped += 1
            log.info("SYNC %d/%d: userId=%s -> no deal found, skip", i, len(rows), uid)
            save_sync_result(
                user_id=uid,
                deal_id=None,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="skipped",
                updated_field_codes=None,
                error="deal not found",
            )
            if SYNC_PAUSE_BETWEEN_USERS:
                log.debug("SYNC pause between users: %.3f sec", SYNC_PAUSE_BETWEEN_USERS)
                time.sleep(SYNC_PAUSE_BETWEEN_USERS)
            continue

        deal_id = int(deal["ID"])
        log.info("SYNC %d/%d: userId=%s -> deal_id=%s", i, len(rows), uid, deal_id)

        # Строим новые UF и логируем список кодов
        new_fields = _build_update_fields(row)
        uf_codes = list(new_fields.keys())
        log.debug("SYNC %d/%d: deal_id=%s -> UF codes prepared=%s", i, len(rows), deal_id, uf_codes)

        # Читаем текущие значения этих полей из сделки (для диффа)
        try:
            current = get_deal_fields(deal_id, uf_codes)
            log.debug(
                "SYNC %d/%d: deal_id=%s -> fetched current UF values (keys=%s)",
                i, len(rows), deal_id, list(current.keys())
            )
        except Exception as e:
            errors += 1
            msg = f"get_deal_fields failed: {e}"
            log.exception("SYNC %d/%d: %s (deal_id=%s)", i, len(rows), msg, deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="error",
                updated_field_codes=uf_codes,
                error=msg,
            )
            continue

        # Считаем изменения
        diff = _diff_fields(current, new_fields)

        if not diff:
            skipped += 1
            log.info("SYNC %d/%d: deal_id=%s -> no changes (skip)", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="skipped",
                updated_field_codes=[],
                error=None,
            )
            if SYNC_PAUSE_BETWEEN_DEALS:
                log.debug("SYNC pause between deals: %.3f sec", SYNC_PAUSE_BETWEEN_DEALS)
                time.sleep(SYNC_PAUSE_BETWEEN_DEALS)
            continue

        # Отправляем только изменившиеся поля
        try:
            fields_to_update = list(diff.keys())
            log.info(
                "SYNC %d/%d: deal_id=%s -> fields_to_update=%s (count=%d)",
                i, len(rows), deal_id, fields_to_update, len(fields_to_update)
            )
            update_deal_fields(deal_id, diff)
            ok += 1
            log.info("SYNC %d/%d: deal_id=%s -> update OK", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="updated",
                updated_field_codes=fields_to_update,
                error=None,
            )
        except Exception as e:
            errors += 1
            log.exception("SYNC %d/%d: update failed (deal_id=%s)", i, len(rows), deal_id)
            save_sync_result(
                user_id=uid,
                deal_id=deal_id,
                source_garage_id=garage_id,
                source_date_updated=dt_updated,
                result="error",
                updated_field_codes=list(diff.keys()),
                error=str(e),
            )

        if SYNC_PAUSE_BETWEEN_DEALS:
            log.debug("SYNC pause between deals: %.3f sec", SYNC_PAUSE_BETWEEN_DEALS)
            time.sleep(SYNC_PAUSE_BETWEEN_DEALS)

    log.info("SYNC: finished -> updated=%s skipped=%s errors=%s", ok, skipped, errors)
    return ok, skipped, errors

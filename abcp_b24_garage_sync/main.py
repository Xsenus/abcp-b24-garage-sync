from __future__ import annotations
import signal  # корректная работа аннотаций типов в рантайме

# --- bootstrap for direct run (python path\to\main.py) ---
if __name__ == "__main__" and (__package__ is None or __package__ == ""):  # если запускаем файл напрямую (а не пакетом)
    import os, sys                                                   # импортируем os/sys для манипуляции путями
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))   # добавляем в sys.path родительскую папку проекта
    __package__ = "abcp_b24_garage_sync"                              # указываем имя пакета для корректных относительных импортов
# -----------------------------------------------------------

import argparse, logging, os, sys, time       # argparse — парсинг аргументов CLI; logging — логирование; sys/time — доступ к argv и паузы
from pathlib import Path                      # Path — удобная работа с путями
from datetime import datetime                 # datetime — парсинг и форматирование дат
from dotenv import load_dotenv                # загрузка переменных окружения из .env
from .log_setup import setup_logging          # наша настройка логирования (консоль + файл)


def _discover_env_file(project_root: Path) -> Path | None:
    """Return the first existing .env candidate for the current deployment."""

    candidates: list[Path] = []

    override = os.getenv("ABCP_B24_ENV_FILE") or os.getenv("ABC_B24_ENV_FILE")
    if override:
        candidates.append(Path(override).expanduser())

    candidates.append(project_root / ".env")
    candidates.append(project_root.parent / ".env")

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_file():
            return candidate

    return None

def parse_dt(s: str) -> datetime:
    """Разбираем дату из строки: поддерживаем ISO с временем и просто YYYY-MM-DD."""
    if "T" in s or ":" in s:                  # если есть разделители времени — используем fromisoformat
        return datetime.fromisoformat(s)       # парсим в datetime
    return datetime.strptime(s, "%Y-%m-%d")    # иначе парсим формат YYYY-MM-DD


def _execute_sync(a, log: logging.Logger, env_path: Path | None, effective_argv: list[str] | tuple[str, ...]) -> None:
    """Выполняет одну итерацию синхронизации, не заботясь о циклах systemd."""

    log.info("=== ABCP→B24 garage sync: start ===")
    if env_path is not None:
        log.info("Using .env at: %s (exists=%s)", env_path, env_path.exists())
    else:
        log.warning(".env file not found (searched in project and parent directories)")
    raw_cli = list(effective_argv)
    log.info("CLI argv (raw): %s", raw_cli)
    if getattr(a, "auto_period", False):
        log.info("CLI period auto-filled: --from=%s --to=%s", a.date_from, a.date_to)

    # imports после загрузки .env — чтобы модули увидели переменные окружения
    from .db import init_db, store_payload                       # функции работы с БД (инициализация и запись)
    from .abcp_client import fetch_garage                        # клиент ABCP (забор данных по годам)
    from .sync_service import sync_all                           # сервис синхронизации с Bitrix24
    from .util import slice_by_years                             # разбиение заданного интервала на годовые срезы

    # логируем разобранные аргументы
    log.info("Args parsed: from=%s to=%s only_store=%s only_sync=%s user=%s",
             a.date_from, a.date_to, a.only_store, a.only_sync, a.only_user)

    # инициализируем БД (создаём таблицы, индексы)
    log.info("DB init: start")
    init_db()                                                        # создаём структуру БД при необходимости
    log.info("DB init: done")

    # приводим входные даты к datetime
    dt_from = parse_dt(a.date_from)                                  # парсим дату начала
    dt_to   = parse_dt(a.date_to)                                    # парсим дату конца
    log.info("Effective period: %s → %s", dt_from, dt_to)            # фиксируем период в логах

    # общий счётчик сохранённых записей (по всем годам)
    total_saved = 0                                                  # подготовим переменную для сводки

    # Если НЕ включён режим «только синхронизировать» — сначала забираем данные из ABCP и пишем в БД
    if not a.only_sync:
        # заранее нарежем период по годам, чтобы видеть план работ
        slices = slice_by_years(dt_from, dt_to)                      # получаем список (start, end) по каждому году
        log.info("Year slices: %d", len(slices))                     # сколько годовых интервалов получилось
        for i, (start, end) in enumerate(slices, 1):                 # проходим по каждому отрезку
            log.info("Slice %d/%d: fetch ABCP %s → %s", i, len(slices), start, end)  # логируем границы среза
            try:
                payload = fetch_garage(start, end)                   # забираем данные ABCP по интервалу (сам клиент тоже логирует)
                # оценим объём полученных данных для лога (не дампим целиком)
                if isinstance(payload, dict):                        # ожидаем словарь {userId: [cars]}
                    batch_count = sum(len(v) for v in payload.values() if isinstance(v, list))  # считаем суммарно элементы
                else:
                    batch_count = 0                                  # на всякий — не словарь
                log.info("Slice %d: fetched items=%s", i, batch_count)  # логируем объём
                cnt = store_payload(payload)                         # пишем в БД (upsert по ключу id)
                total_saved += cnt                                   # накапливаем общий счётчик
                log.info("Slice %d: stored rows=%s (total_saved=%s)", i, cnt, total_saved)  # фиксируем запись в логах
            except KeyboardInterrupt:                                # позволяем корректно прервать процесс
                log.warning("Interrupted by user on slice %d/%d", i, len(slices))
                raise                                               # пробрасываем дальше
            except Exception as e:                                   # любая иная ошибка на срезе
                log.exception("Slice %d FAILED: %s", i, str(e))      # логируем стек и продолжаем к следующему срезу
        log.info("ABCP fetch/store finished: total_saved=%s", total_saved)  # итог по блоку забора данных

    # Если НЕ включён режим «только записать» — запускаем синхронизацию с Bitrix24
    if not a.only_store:
        log.info("Sync to Bitrix24: start (user=%s)", a.only_user if a.only_user else "ALL")  # логируем старт синка
        ok, skipped, errors = sync_all(a.only_user)                   # синхронизация (подробные логи — внутри сервиса)
        log.info("Sync to Bitrix24: finished (updated=%s, skipped=%s, errors=%s)", ok, skipped, errors)  # итог синка

    # финал: красивая подпись
    log.info("=== ABCP→B24 garage sync: done ===")                   # конец работы

def main(argv=None):
    """Точка входа CLI: загрузка .env, парсинг аргументов, импорт модулей, цикл по годам и синхронизация."""
    # поддерживаем автоподстановку периода, если даты явно не переданы
    if argv is None:                           # если argv не передали извне
        argv = list(sys.argv[1:])              # используем реальные аргументы командной строки
    else:
        argv = list(argv)                      # создаём копию, чтобы не мутировать входящие данные
    # грузим .env из корня проекта (на уровень выше пакета)
    project_root = Path(__file__).resolve().parents[1]          # вычисляем корень проекта
    os.environ.setdefault("ABCP_B24_PROJECT_ROOT", str(project_root))

    env_path = _discover_env_file(project_root)
    if env_path:
        load_dotenv(dotenv_path=env_path)                        # загружаем .env из найденного места
    else:
        # Пробуем стандартный путь, даже если файла нет — load_dotenv тихо вернёт False
        fallback_env = project_root / ".env"
        load_dotenv(dotenv_path=fallback_env)
        env_path = fallback_env if fallback_env.exists() else None

    setup_logging()                                              # настраиваем логирование (уровень берётся из LOG_LEVEL)
    log = logging.getLogger("main")                              # получаем модульный логгер

# --- мягкие сигналы завершения ---
    def _graceful_exit(signum=None, frame=None):
        signame = {getattr(signal, "SIGINT", 2): "SIGINT",
                   getattr(signal, "SIGTERM", 15): "SIGTERM"}.get(signum, str(signum))
        log.warning("Received %s — graceful shutdown", signame)
        sys.exit(0)

    # SIGINT (Ctrl+C) и SIGTERM (останов от ОС/сервиса)
    try:
        signal.signal(signal.SIGINT, _graceful_exit)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _graceful_exit)
    except Exception:
        # на некоторых платформах/рантаймах сигналов может не быть
        log.debug("Signal handlers not installed", exc_info=True)
    # --- конец блока сигналов ---

    # описываем CLI и парсим аргументы
    p = argparse.ArgumentParser(description="ABCP→B24 garage sync")  # создаём парсер с описанием
    p.add_argument("--from", dest="date_from")                       # начало периода (по умолчанию заполним сами)
    p.add_argument("--to", dest="date_to")                           # конец периода (по умолчанию заполним сами)
    p.add_argument("--only-store", action="store_true")              # режим: только записать в БД (без синхронизации)
    p.add_argument("--only-sync", action="store_true")               # режим: только синхронизация (без запроса ABCP)
    p.add_argument("--user", dest="only_user", type=int)             # ограничение синхронизации конкретным userId
    p.add_argument("--loop-every", dest="loop_every", type=int, metavar="MINUTES",
                   help="Повторять запуск каждые N минут (используется в systemd-сервисе)")

    effective_argv = list(argv)
    a = p.parse_args(argv)                                           # парсим аргументы (argv уже содержит автодефолт, если надо)

    auto_period = False
    if a.date_from is None and a.date_to is None:
        a.date_from = "2024-01-01"
        a.date_to = "2025-12-31"
        auto_period = True
    elif (a.date_from is None) != (a.date_to is None):
        p.error("--from and --to must be specified together")

    setattr(a, "auto_period", auto_period)

    if a.loop_every is not None and a.loop_every <= 0:
        p.error("--loop-every must be a positive integer (minutes)")

    loop_every = a.loop_every
    loop_limit = None
    loop_limit_env = os.getenv("ABCP_B24_LOOP_LIMIT")
    if loop_limit_env:
        try:
            loop_limit_candidate = int(loop_limit_env)
            if loop_limit_candidate > 0:
                loop_limit = loop_limit_candidate
            else:
                log.warning("ABCP_B24_LOOP_LIMIT must be > 0, got %r — ignoring", loop_limit_env)
        except ValueError:
            log.warning("Invalid ABCP_B24_LOOP_LIMIT=%r — ignoring", loop_limit_env)

    iteration = 0
    while True:
        iteration += 1
        _execute_sync(a, log, env_path, effective_argv)

        if loop_every is None:
            break

        if loop_limit is not None and iteration >= loop_limit:
            log.info("Loop limit reached (%s iterations) — exiting", loop_limit)
            break

        log.info("Sleeping %s minutes before next run", loop_every)
        time.sleep(loop_every * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger("main").warning("Interrupted by user (Ctrl+C) — graceful shutdown")
        sys.exit(0)          # код 0 — корректное завершение
    except SystemExit:
        raise                 # уважим явные sys.exit(...)
    except Exception:
        logging.getLogger("main").exception("Fatal error")
        sys.exit(1)           # код 1 — фатальная ошибка

from __future__ import annotations

import signal

if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    __package__ = "abcp_b24_garage_sync"

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from .log_setup import setup_logging


FETCH_STATE_SOURCE = "abcp_garage"


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
    """Parse either YYYY-MM-DD or ISO datetime."""
    if "T" in s or ":" in s:
        return datetime.fromisoformat(s)
    return datetime.strptime(s, "%Y-%m-%d")


def _format_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _resolve_auto_period(log: logging.Logger) -> tuple[datetime, datetime]:
    from .db import get_fetch_state
    from .config import ABCP_INCREMENTAL_OVERLAP_MINUTES, ABCP_INITIAL_LOOKBACK_YEARS

    now = datetime.now().replace(microsecond=0)
    overlap_minutes = max(0, ABCP_INCREMENTAL_OVERLAP_MINUTES)
    overlap = timedelta(minutes=overlap_minutes)
    state = get_fetch_state(FETCH_STATE_SOURCE)

    if state and state.get("lastSuccessTo"):
        dt_from = parse_dt(state["lastSuccessTo"]) - overlap
        if dt_from > now:
            dt_from = now
        log.info(
            "Auto period resolved from fetch_state: lastSuccessTo=%s overlap=%s min => %s -> %s",
            state["lastSuccessTo"], overlap_minutes, dt_from, now
        )
        return dt_from, now

    lookback_years = max(0, ABCP_INITIAL_LOOKBACK_YEARS)
    bootstrap_year = max(1, now.year - lookback_years)
    dt_from = datetime(bootstrap_year, 1, 1, 0, 0, 0)
    log.info(
        "Auto period bootstrap: no fetch_state, using %s -> %s (initial_lookback_years=%s)",
        dt_from, now, lookback_years
    )
    return dt_from, now


def _resolve_period(a, log: logging.Logger) -> tuple[datetime, datetime]:
    if getattr(a, "auto_period", False):
        return _resolve_auto_period(log)
    return parse_dt(a.date_from), parse_dt(a.date_to)


def _persist_fetch_success(start: datetime, end: datetime) -> None:
    from .db import save_fetch_state

    save_fetch_state(
        source=FETCH_STATE_SOURCE,
        requested_from=_format_dt(start),
        requested_to=_format_dt(end),
        success_from=_format_dt(start),
        success_to=_format_dt(end),
        status="success",
        error=None,
    )


def _persist_fetch_error(start: datetime, end: datetime, error: Exception) -> None:
    from .db import save_fetch_state

    save_fetch_state(
        source=FETCH_STATE_SOURCE,
        requested_from=_format_dt(start),
        requested_to=_format_dt(end),
        success_from=None,
        success_to=None,
        status="error",
        error=str(error),
    )


def _execute_sync(a, log: logging.Logger, env_path: Path | None, effective_argv: list[str] | tuple[str, ...]) -> None:
    """Execute one sync iteration."""
    log.info("=== ABCP->B24 garage sync: start ===")
    if env_path is not None:
        log.info("Using .env at: %s (exists=%s)", env_path, env_path.exists())
    else:
        log.warning(".env file not found (searched in project and parent directories)")

    log.info("CLI argv (raw): %s", list(effective_argv))

    from .abcp_client import fetch_garage
    from .db import init_db, store_payload
    from .sync_service import sync_all
    from .util import slice_by_years

    log.info(
        "Args parsed: from=%s to=%s only_store=%s only_sync=%s user=%s auto_period=%s",
        a.date_from, a.date_to, a.only_store, a.only_sync, a.only_user, getattr(a, "auto_period", False)
    )

    log.info("DB init: start")
    init_db()
    log.info("DB init: done")

    dt_from, dt_to = _resolve_period(a, log)
    log.info("Effective period: %s -> %s", dt_from, dt_to)

    total_saved = 0

    if not a.only_sync:
        slices = slice_by_years(dt_from, dt_to)
        log.info("Year slices: %d", len(slices))
        for i, (start, end) in enumerate(slices, 1):
            log.info("Slice %d/%d: fetch ABCP %s -> %s", i, len(slices), start, end)
            try:
                payload = fetch_garage(start, end)
                if isinstance(payload, dict):
                    batch_count = sum(len(v) for v in payload.values() if isinstance(v, list))
                else:
                    batch_count = 0
                log.info("Slice %d: fetched items=%s", i, batch_count)
                cnt = store_payload(payload)
                total_saved += cnt
                _persist_fetch_success(start, end)
                log.info("Slice %d: stored rows=%s (total_saved=%s)", i, cnt, total_saved)
            except KeyboardInterrupt:
                log.warning("Interrupted by user on slice %d/%d", i, len(slices))
                raise
            except Exception as e:
                _persist_fetch_error(start, end, e)
                log.exception("Slice %d FAILED: %s", i, str(e))
        log.info("ABCP fetch/store finished: total_saved=%s", total_saved)

    if not a.only_store:
        log.info("Sync to Bitrix24: start (user=%s)", a.only_user if a.only_user else "ALL")
        ok, skipped, errors = sync_all(a.only_user)
        log.info("Sync to Bitrix24: finished (updated=%s, skipped=%s, errors=%s)", ok, skipped, errors)

    log.info("=== ABCP->B24 garage sync: done ===")


def main(argv=None):
    """CLI entrypoint."""
    if argv is None:
        argv = list(sys.argv[1:])
    else:
        argv = list(argv)

    project_root = Path(__file__).resolve().parents[1]
    os.environ.setdefault("ABCP_B24_PROJECT_ROOT", str(project_root))

    env_path = _discover_env_file(project_root)
    if env_path:
        load_dotenv(dotenv_path=env_path)
    else:
        fallback_env = project_root / ".env"
        load_dotenv(dotenv_path=fallback_env)
        env_path = fallback_env if fallback_env.exists() else None

    setup_logging()
    log = logging.getLogger("main")

    def _graceful_exit(signum=None, frame=None):
        signame = {
            getattr(signal, "SIGINT", 2): "SIGINT",
            getattr(signal, "SIGTERM", 15): "SIGTERM",
        }.get(signum, str(signum))
        log.warning("Received %s - graceful shutdown", signame)
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, _graceful_exit)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _graceful_exit)
    except Exception:
        log.debug("Signal handlers not installed", exc_info=True)

    p = argparse.ArgumentParser(description="ABCP->B24 garage sync")
    p.add_argument("--from", dest="date_from")
    p.add_argument("--to", dest="date_to")
    p.add_argument("--only-store", action="store_true")
    p.add_argument("--only-sync", action="store_true")
    p.add_argument("--user", dest="only_user", type=int)
    p.add_argument(
        "--loop-every",
        dest="loop_every",
        type=int,
        metavar="MINUTES",
        help="Repeat execution every N minutes",
    )

    effective_argv = list(argv)
    a = p.parse_args(argv)

    auto_period = False
    if a.date_from is None and a.date_to is None:
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
                log.warning("ABCP_B24_LOOP_LIMIT must be > 0, got %r - ignoring", loop_limit_env)
        except ValueError:
            log.warning("Invalid ABCP_B24_LOOP_LIMIT=%r - ignoring", loop_limit_env)

    iteration = 0
    while True:
        iteration += 1
        _execute_sync(a, log, env_path, effective_argv)

        if loop_every is None:
            break

        if loop_limit is not None and iteration >= loop_limit:
            log.info("Loop limit reached (%s iterations) - exiting", loop_limit)
            break

        log.info("Sleeping %s minutes before next run", loop_every)
        time.sleep(loop_every * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger("main").warning("Interrupted by user (Ctrl+C) - graceful shutdown")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception:
        logging.getLogger("main").exception("Fatal error")
        sys.exit(1)

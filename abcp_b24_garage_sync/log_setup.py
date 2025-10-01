
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


_CONFIGURED = False


def _project_root() -> Path:
    """Return the project root directory even if launched from a release symlink."""

    env_value = os.getenv("ABCP_B24_PROJECT_ROOT")
    if env_value:
        try:
            return Path(env_value).expanduser()
        except Exception:
            pass
    return Path(__file__).resolve().parents[1]


def _data_root() -> Path:
    """Base directory for runtime artefacts (DB, logs)."""

    env_value = os.getenv("ABCP_B24_DATA_DIR") or os.getenv("ABC_B24_DATA_DIR")
    if env_value:
        try:
            return Path(env_value).expanduser()
        except Exception:
            pass
    return _project_root()


def _resolve_log_dir() -> Path:
    """Resolve LOG_DIR env to an absolute directory and create it if needed."""

    raw = os.getenv("LOG_DIR")
    base = _data_root()

    if raw:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = base / candidate
    else:
        candidate = base / "logs"

    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def setup_logging() -> None:
    """Configure console + rotating file logging only once per process."""

    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    log_dir = _resolve_log_dir()
    log_file = os.getenv("LOG_FILE", "service.log")
    log_path = log_dir / log_file

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = TimedRotatingFileHandler(
        filename=str(log_path), when="D", backupCount=7, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    root.debug("Logging configured: level=%s log_path=%s", level_name, log_path)

    _CONFIGURED = True

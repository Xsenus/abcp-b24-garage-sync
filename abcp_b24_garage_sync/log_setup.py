
import os, sys, logging
from logging.handlers import TimedRotatingFileHandler
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    level = getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); root.addHandler(sh)
    fh = TimedRotatingFileHandler("logs/service.log", when="D", backupCount=7, encoding="utf-8")
    fh.setFormatter(fmt); root.addHandler(fh)

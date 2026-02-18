import os
from zoneinfo import ZoneInfo

from controltower.config import load_config
from controltower.db.connection import get_engine

CHILE_TZ = ZoneInfo("America/Santiago")

_ENGINE = None
_CFG = None


def get_engine_cached():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = get_engine()
    return _ENGINE


def get_cfg():
    global _CFG
    if _CFG is None:
        cfg_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "config/config.example.yaml"
        _CFG = load_config(cfg_path)
    return _CFG

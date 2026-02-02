from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv
import yaml

load_dotenv()

def _env_expand(value: str) -> str:
    # supports ${VAR} interpolation for YAML strings
    import re
    pattern = re.compile(r"\$\{([A-Z0-9_]+)\}")
    def repl(m):
        return os.getenv(m.group(1), "")
    return pattern.sub(repl, value)

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # recursively expand env vars
    def walk(obj):
        if isinstance(obj, dict):
            return {k: walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [walk(x) for x in obj]
        if isinstance(obj, str):
            return _env_expand(obj)
        return obj
    return walk(cfg)

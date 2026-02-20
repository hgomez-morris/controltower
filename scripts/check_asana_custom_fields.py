#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import json
from pathlib import Path

from controltower.asana.client import AsanaReadOnlyClient


def _load_env(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip("'").strip('"')


def main() -> None:
    _load_env()
    token = os.getenv("ASANA_ACCESS_TOKEN")
    if not token:
        print("Missing ASANA_ACCESS_TOKEN in env/.env")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: python scripts/check_asana_custom_fields.py <project_gid>")
        sys.exit(1)

    project_gid = sys.argv[1]
    client = AsanaReadOnlyClient(token)
    project = client.get_project(project_gid)
    fields = project.get("custom_fields") or []

    names = [
        "Horas planificadas",
        "Horas efectivas",
        "Fecha Inicio del Proyecto",
    ]

    filtered = []
    found_names = set()
    for f in fields:
        if f.get("name") in names:
            found_names.add(f.get("name"))
            filtered.append({
                "gid": f.get("gid"),
                "name": f.get("name"),
                "type": f.get("type"),
                "display_value": f.get("display_value"),
                "number_value": f.get("number_value"),
                "text_value": f.get("text_value"),
                "date_value": (f.get("date_value") or {}).get("date") if isinstance(f.get("date_value"), dict) else None,
                "is_formula_field": f.get("is_formula_field"),
                "is_value_read_only": f.get("is_value_read_only"),
            })

    result = {
        "project_gid": project_gid,
        "found": filtered,
        "missing": [n for n in names if n not in found_names],
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

from __future__ import annotations
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict
from sqlalchemy import text
from asana.rest import ApiException

# Ensure src is on path when running as script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from controltower.asana.client import AsanaReadOnlyClient
from controltower.config import load_config
from controltower.db.connection import get_engine
from controltower.utils.logging import configure_logging


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _load_env() -> None:
    env_path = ".env"
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def _custom_field_map(project_raw: Dict[str, Any]) -> Dict[str, Any]:
    fields = (project_raw or {}).get("custom_fields") or []
    out: Dict[str, Any] = {}
    for f in fields:
        name = f.get("name") or ""
        if not name:
            continue
        if f.get("display_value") is not None:
            val = f.get("display_value")
        else:
            val = f.get("text_value") or f.get("number_value") or f.get("enum_value") or f.get("multi_enum_values") or f.get("date_value")
            if isinstance(val, dict) and "name" in val:
                val = val.get("name")
        out[name] = val
    return out


def _search_text(fields: Dict[str, Any]) -> str:
    parts = []
    for k, v in fields.items():
        if v is None:
            continue
        parts.append(str(v))
    return " | ".join(parts).lower()


def main() -> None:
    configure_logging()
    log = logging.getLogger("history_load")
    _load_env()
    cfg = load_config("config/config.yaml") if os.path.exists("config/config.yaml") else load_config("config/config.example.yaml")

    token = os.getenv("ASANA_ACCESS_TOKEN", "")
    workspace_gid = cfg.get("asana", {}).get("workspace_gid")
    if not token or not workspace_gid:
        raise RuntimeError("Missing ASANA_ACCESS_TOKEN or ASANA_WORKSPACE_GID")

    engine = get_engine()
    client = AsanaReadOnlyClient(token)
    projects = client.list_projects(workspace_gid)
    total = len(projects)
    log.info("Starting historical load. total_projects=%s", total)

    t0 = time.time()
    inserted = 0
    skipped_existing = 0
    skipped_history = 0
    skipped_forbidden = 0
    forbidden_gids = []

    project_opts = {"opt_fields": ",".join([
        "name",
        "owner",
        "owner.name",
        "owner.gid",
        "due_date",
        "due_on",
        "start_on",
        "created_at",
        "modified_at",
        "completed",
        "completed_at",
        "current_status",
        "custom_fields",
        "custom_field_settings",
    ])}

    with engine.begin() as conn:
        for i, p in enumerate(projects, start=1):
            if i == 1 or i % 25 == 0 or i == total:
                elapsed = time.time() - t0
                rate = elapsed / i if i else 0
                eta = int(rate * (total - i)) if total else 0
                log.info("Progress: %s/%s | elapsed=%ss eta=%ss inserted=%s", i, total, int(elapsed), eta, inserted)

            pgid = p.get("gid")
            if not pgid:
                continue

            exists = conn.execute(text("SELECT 1 FROM projects WHERE gid=:gid"), {"gid": pgid}).scalar()
            if exists:
                skipped_existing += 1
                continue

            exists_hist = conn.execute(text("SELECT 1 FROM projects_history WHERE gid=:gid"), {"gid": pgid}).scalar()
            if exists_hist:
                skipped_history += 1
                continue

            try:
                pfull = client.projects_api.get_project(pgid, opts=project_opts)
            except ApiException as e:
                if getattr(e, "status", None) == 403:
                    skipped_forbidden += 1
                    forbidden_gids.append(pgid)
                    log.warning("No access to project. gid=%s", pgid)
                    continue
                raise
            raw = pfull or {}
            cf = _custom_field_map(raw)

            row = {
                "gid": pgid,
                "name": raw.get("name"),
                "owner_gid": (raw.get("owner") or {}).get("gid"),
                "owner_name": (raw.get("owner") or {}).get("name"),
                "status": (raw.get("current_status") or {}).get("color"),
                "last_status_update_at": (raw.get("current_status") or {}).get("created_at"),
                "last_status_update_by": ((raw.get("current_status") or {}).get("author") or {}).get("name"),
                "pmo_id": cf.get("PMO ID"),
                "cliente_nuevo": cf.get("cliente_nuevo") or cf.get("Cliente_nuevo"),
                "responsable_proyecto": cf.get("Responsable Proyecto"),
                "sponsor": cf.get("Sponsor"),
                "aws_opp_id": cf.get("AWS OPP ID"),
                "id_comercial": cf.get("ID_Comercial"),
                "search_text": _search_text({
                    "name": raw.get("name"),
                    "pmo_id": cf.get("PMO ID"),
                    "cliente_nuevo": cf.get("cliente_nuevo") or cf.get("Cliente_nuevo"),
                    "responsable_proyecto": cf.get("Responsable Proyecto"),
                    "aws_opp_id": cf.get("AWS OPP ID"),
                    "id_comercial": cf.get("ID_Comercial"),
                }),
                "raw_data": json.dumps({"project": raw}),
                "snapshot_at": _utcnow().isoformat(),
            }

            conn.execute(text("""
                INSERT INTO projects_history (
                    gid, name, owner_gid, owner_name, status, last_status_update_at, last_status_update_by,
                    pmo_id, cliente_nuevo, responsable_proyecto, sponsor, aws_opp_id, id_comercial,
                    search_text, raw_data, snapshot_at
                ) VALUES (
                    :gid, :name, :owner_gid, :owner_name, :status, :last_status_update_at, :last_status_update_by,
                    :pmo_id, :cliente_nuevo, :responsable_proyecto, :sponsor, :aws_opp_id, :id_comercial,
                    :search_text, CAST(:raw_data AS jsonb), :snapshot_at
                )
                ON CONFLICT (gid) DO NOTHING
            """), row)
            inserted += 1

    log.info(
        "Historical load completed. total=%s inserted=%s skipped_existing=%s skipped_history=%s skipped_forbidden=%s",
        total, inserted, skipped_existing, skipped_history, skipped_forbidden
    )
    if forbidden_gids:
        try:
            with open("logs/forbidden_projects.log", "a", encoding="utf-8") as f:
                for gid in forbidden_gids:
                    f.write(f"{gid}\n")
            log.info("Forbidden project gids appended to logs/forbidden_projects.log")
        except Exception as e:
            log.warning("Failed to write forbidden project log: %s", e)


if __name__ == "__main__":
    main()

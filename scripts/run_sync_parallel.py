from __future__ import annotations
import logging
import os
import sys
import time
import threading
import json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure src is on path when running as script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from sqlalchemy import text
from asana.rest import ApiException
from controltower.asana.client import AsanaReadOnlyClient
from controltower.config import load_config
from controltower.db.connection import get_engine
from controltower.rules.engine import evaluate_rules
from controltower.sync.sync_runner import (
    compute_last_status_from_project,
    compute_task_metrics,
    upsert_project,
    upsert_status_update,
    insert_status_update_comment,
    _detect_changes,
    _phase_is_terminated_or_cancelled,
    _recently_closed_or_cancelled,
)
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


_thread_local = threading.local()

def _get_client(token: str) -> AsanaReadOnlyClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = AsanaReadOnlyClient(token)
        _thread_local.client = client
    return client

def _fetch_project_bundle(pgid: str, token: str, lookback_days: int, cutoff: datetime, existing_mod: str | None) -> dict:
    client = _get_client(token)
    try:
        pfull = client.get_project(pgid)
    except ApiException as e:
        if getattr(e, "status", None) == 403:
            return {"gid": pgid, "skip": "forbidden"}
        raise

    modified_at = pfull.get("modified_at")
    if existing_mod and modified_at and str(modified_at) == str(existing_mod):
        return {"gid": pgid, "skip": "unchanged"}

    is_completed = pfull.get("completed") is True
    is_terminated_or_cancelled = _phase_is_terminated_or_cancelled(pfull)
    if is_completed or is_terminated_or_cancelled:
        if not _recently_closed_or_cancelled(pfull, cutoff):
            return {"gid": pgid, "skip": "closed_old"}

    tasks = client.get_project_tasks(pgid)
    metrics = compute_task_metrics(tasks, lookback_days=lookback_days)

    updates = []
    comments_by_update = {}
    try:
        updates = client.list_status_updates(pgid)
        for su in updates:
            if not su.get("gid"):
                continue
            try:
                comments = client.list_status_update_comments(su["gid"])
                comments_by_update[su["gid"]] = comments
            except Exception:
                comments_by_update[su["gid"]] = []
    except Exception:
        updates = []
        comments_by_update = {}

    return {
        "gid": pgid,
        "project": pfull,
        "metrics": metrics,
        "updates": updates,
        "comments_by_update": comments_by_update,
    }


def main() -> None:
    configure_logging()
    log = logging.getLogger("sync_parallel")
    _load_env()
    cfg = load_config("config/config.yaml") if os.path.exists("config/config.yaml") else load_config("config/config.example.yaml")

    token = os.getenv("ASANA_ACCESS_TOKEN", "")
    workspace_gid = cfg.get("asana", {}).get("workspace_gid")
    if not token or not workspace_gid:
        raise RuntimeError("Missing ASANA_ACCESS_TOKEN or ASANA_WORKSPACE_GID")

    engine = get_engine()
    sync_id = str(os.urandom(16).hex())
    started = _utcnow()

    client = AsanaReadOnlyClient(token)
    lookback_cfg = (cfg.get("rules") or {}).get("no_tasks_activity_last_7_days") or {}
    lookback_days = int(lookback_cfg.get("days_threshold", 7))
    cutoff = _utcnow() - timedelta(days=30)

    projects = client.list_projects(workspace_gid)
    total = len(projects)
    gids = [p.get("gid") for p in projects if p.get("gid")]

    max_workers = int(os.getenv("SYNC_WORKERS", "4"))
    log.info("Parallel sync start. total=%s workers=%s", total, max_workers)

    with engine.begin() as conn:
        existing_mod_map = {}
        try:
            rows = conn.execute(text("""
                SELECT gid, raw_data->'project'->>'modified_at' AS modified_at
                FROM projects
            """)).mappings().all()
            existing_mod_map = {r["gid"]: r["modified_at"] for r in rows if r.get("gid")}
        except Exception:
            existing_mod_map = {}

        conn.execute(text("INSERT INTO sync_log(sync_id, started_at, status) VALUES(:sync_id,:started,'running')"),
                     {"sync_id": sync_id, "started": started.isoformat()})

        changes_detected = 0
        processed = 0
        t0 = time.time()
        forbidden = 0
        skipped = 0
        skipped_unchanged = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(_fetch_project_bundle, gid, token, lookback_days, cutoff, existing_mod_map.get(gid))
                for gid in gids
            ]
            for i, fut in enumerate(as_completed(futures), start=1):
                try:
                    bundle = fut.result()
                except Exception as e:
                    log.warning("Worker failed: %s", e)
                    continue

                if bundle.get("skip"):
                    if bundle["skip"] == "forbidden":
                        forbidden += 1
                    elif bundle["skip"] == "unchanged":
                        skipped_unchanged += 1
                    else:
                        skipped += 1
                    continue

                pfull = bundle["project"]
                metrics = bundle["metrics"]
                updates = bundle.get("updates") or []
                comments_by_update = bundle.get("comments_by_update") or {}

                last_status = compute_last_status_from_project(pfull)
                owner = pfull.get("owner") or {}
                row = {
                    "gid": pfull.get("gid"),
                    "name": pfull.get("name"),
                    "owner_gid": owner.get("gid"),
                    "owner_name": owner.get("name"),
                    "due_date": pfull.get("due_date") or pfull.get("due_on"),
                    "status": last_status.get("status"),
                    "calculated_progress": metrics["calculated_progress"],
                    "last_status_update_at": last_status.get("last_status_update_at"),
                    "last_status_update_by": last_status.get("last_status_update_by"),
                    "last_activity_at": metrics["last_activity_at"] or pfull.get("modified_at") or pfull.get("created_at"),
                    "total_tasks": metrics["total_tasks"],
                    "completed_tasks": metrics["completed_tasks"],
                    "tasks_created_last_7d": metrics["tasks_created_last_7d"],
                    "tasks_completed_last_7d": metrics["tasks_completed_last_7d"],
                    "tasks_modified_last_7d": metrics["tasks_modified_last_7d"],
                    "raw_data": json.dumps({"project": pfull}),
                    "synced_at": _utcnow().isoformat(),
                }

                existing = conn.execute(text("""
                    SELECT gid, name, owner_gid, owner_name, due_date, status, calculated_progress,
                           last_status_update_at, last_status_update_by, last_activity_at,
                           total_tasks, completed_tasks, tasks_created_last_7d, tasks_completed_last_7d, tasks_modified_last_7d
                    FROM projects WHERE gid=:gid
                """), {"gid": pfull.get("gid")}).mappings().first()

                changes = _detect_changes(existing, row)
                if changes:
                    detected_at = _utcnow().isoformat()
                    for field_name, old_v, new_v in changes:
                        conn.execute(text("""
                            INSERT INTO project_changelog(
                                project_gid, field_name, old_value, new_value, changed_at, detected_at, sync_id
                            ) VALUES (
                                :gid, :field, :old, :new, :changed_at, :detected_at, :sync_id
                            )
                        """), {
                            "gid": pfull.get("gid"),
                            "field": field_name,
                            "old": old_v,
                            "new": new_v,
                            "changed_at": row["synced_at"],
                            "detected_at": detected_at,
                            "sync_id": sync_id,
                        })
                    changes_detected += len(changes)

                upsert_project(conn, row)

                for su in updates:
                    if not su.get("gid"):
                        continue
                    upsert_status_update(conn, pfull.get("gid"), su)
                    for st in comments_by_update.get(su.get("gid"), []) or []:
                        if not st.get("gid"):
                            continue
                        insert_status_update_comment(conn, su.get("gid"), st)

                processed += 1
                if processed == 1 or processed % 25 == 0 or processed == total:
                    elapsed = time.time() - t0
                    rate = elapsed / processed if processed else 0
                    eta = int(rate * (total - processed)) if total else 0
                    log.info("Sync progress: %s/%s processed | elapsed=%ss eta=%ss", processed, total, int(elapsed), eta)

        conn.execute(text("""
            UPDATE sync_log
            SET completed_at=:completed, status='completed', projects_synced=:n, changes_detected=:c
            WHERE sync_id=:sync_id
        """), {
            "completed": _utcnow().isoformat(),
            "n": processed,
            "c": changes_detected,
            "sync_id": sync_id,
        })

    created = evaluate_rules(cfg, sync_id)
    log.info("Parallel sync completed. sync_id=%s processed=%s forbidden=%s skipped=%s findings_created=%s",
             sync_id, processed, forbidden, skipped + skipped_unchanged, created)


if __name__ == "__main__":
    main()

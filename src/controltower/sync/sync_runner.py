from __future__ import annotations
import os, uuid, json, logging, time
from datetime import datetime, timezone, timedelta
from datetime import date as dt_date
from decimal import Decimal
from sqlalchemy import text
from controltower.asana.client import AsanaReadOnlyClient
from controltower.db.connection import get_engine

CRITICAL_FIELDS = [
    "due_date",
    "owner_gid",
    "owner_name",
    "status",
    "last_status_update_at",
    "last_status_update_by",
    "total_tasks",
    "completed_tasks",
    "tasks_created_last_7d",
    "tasks_completed_last_7d",
    "calculated_progress",
    "last_activity_at",
]

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def compute_task_metrics(tasks: list[dict], lookback_days: int) -> dict:
    total = len(tasks)
    completed = sum(1 for t in tasks if t.get("completed") is True)
    progress = (completed / total * 100.0) if total > 0 else 0.0
    lookback = _utcnow() - timedelta(days=lookback_days)

    created_last = sum(
        1
        for t in tasks
        if t.get("created_at")
        and datetime.fromisoformat(t["created_at"].replace("Z", "+00:00")) >= lookback
    )
    completed_last = sum(
        1
        for t in tasks
        if t.get("completed_at")
        and datetime.fromisoformat(t["completed_at"].replace("Z", "+00:00")) >= lookback
    )

    times = []
    for t in tasks:
        for k in ("modified_at", "completed_at", "created_at"):
            v = t.get(k)
            if v:
                times.append(datetime.fromisoformat(v.replace("Z", "+00:00")))
    last_activity_at = max(times).astimezone(timezone.utc) if times else None

    return {
        "total_tasks": total,
        "completed_tasks": completed,
        "calculated_progress": round(progress, 2),
        "tasks_created_last_7d": created_last,
        "tasks_completed_last_7d": completed_last,
        "last_activity_at": last_activity_at.isoformat() if last_activity_at else None,
    }

def compute_last_status_from_project(project: dict) -> dict:
    status = project.get("current_status") or {}
    return {
        "last_status_update_at": status.get("created_at"),
        "last_status_update_by": (status.get("author") or {}).get("name"),
        "status": (status.get("color") or None),
    }

def _norm_value(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, (datetime, dt_date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return str(v)

def _detect_changes(existing: dict | None, new_row: dict) -> list[tuple[str, str | None, str | None]]:
    if not existing:
        return []
    changes: list[tuple[str, str | None, str | None]] = []
    for f in CRITICAL_FIELDS:
        old_v = _norm_value(existing.get(f))
        new_v = _norm_value(new_row.get(f))
        if old_v != new_v:
            changes.append((f, old_v, new_v))
    return changes

def upsert_project(conn, project: dict) -> None:
    q = text("""
    INSERT INTO projects (
        gid, name, owner_gid, owner_name, due_date, status, calculated_progress,
        last_status_update_at, last_status_update_by, last_activity_at,
        total_tasks, completed_tasks, tasks_created_last_7d, tasks_completed_last_7d,
        raw_data, synced_at
    ) VALUES (
        :gid, :name, :owner_gid, :owner_name, :due_date, :status, :calculated_progress,
        :last_status_update_at, :last_status_update_by, :last_activity_at,
        :total_tasks, :completed_tasks, :tasks_created_last_7d, :tasks_completed_last_7d,
        CAST(:raw_data AS jsonb), :synced_at
    )
    ON CONFLICT (gid) DO UPDATE SET
        name = EXCLUDED.name,
        owner_gid = EXCLUDED.owner_gid,
        owner_name = EXCLUDED.owner_name,
        due_date = EXCLUDED.due_date,
        status = EXCLUDED.status,
        calculated_progress = EXCLUDED.calculated_progress,
        last_status_update_at = EXCLUDED.last_status_update_at,
        last_status_update_by = EXCLUDED.last_status_update_by,
        last_activity_at = EXCLUDED.last_activity_at,
        total_tasks = EXCLUDED.total_tasks,
        completed_tasks = EXCLUDED.completed_tasks,
        tasks_created_last_7d = EXCLUDED.tasks_created_last_7d,
        tasks_completed_last_7d = EXCLUDED.tasks_completed_last_7d,
        raw_data = EXCLUDED.raw_data,
        synced_at = EXCLUDED.synced_at
    """)
    conn.execute(q, project)

def main_sync(config: dict) -> str:
    log = logging.getLogger("sync")
    sync_id = str(uuid.uuid4())
    started = _utcnow()

    engine = get_engine()
    token = os.getenv("ASANA_ACCESS_TOKEN","")
    workspace_gid = config["asana"]["workspace_gid"]
    client = AsanaReadOnlyClient(token)
    lookback_days = int(config["rules"]["no_activity"].get("days_threshold", 7))

    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO sync_log(sync_id, started_at, status) VALUES(:sync_id,:started,'running')"""),
                     {"sync_id": sync_id, "started": started.isoformat()})

        projects = client.list_projects(workspace_gid)
        changes_detected = 0
        total = len(projects)
        t0 = time.time()
        for i, p in enumerate(projects, start=1):
            if i == 1 or i % 25 == 0 or i == total:
                elapsed = time.time() - t0
                rate = elapsed / i if i else 0
                eta = int(rate * (total - i)) if total else 0
                log.info("Sync progress: %s/%s projects | elapsed=%ss eta=%ss", i, total, int(elapsed), eta)
            pgid = p["gid"]
            pfull = client.get_project(pgid)
            if pfull.get("completed") is True:
                continue

            tasks = client.get_project_tasks(pgid)
            metrics = compute_task_metrics(tasks, lookback_days=lookback_days)
            last_status = compute_last_status_from_project(pfull)

            owner = (pfull.get("owner") or {})
            row = {
                "gid": pgid,
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
                "raw_data": json.dumps({"project": pfull}),
                "synced_at": _utcnow().isoformat(),
            }
            existing = conn.execute(text("""
                SELECT gid, name, owner_gid, owner_name, due_date, status, calculated_progress,
                       last_status_update_at, last_status_update_by, last_activity_at,
                       total_tasks, completed_tasks
                FROM projects WHERE gid=:gid
            """), {"gid": pgid}).mappings().first()

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
                        "gid": pgid,
                        "field": field_name,
                        "old": old_v,
                        "new": new_v,
                        "changed_at": row["synced_at"],
                        "detected_at": detected_at,
                        "sync_id": sync_id,
                    })
                changes_detected += len(changes)

            upsert_project(conn, row)

        conn.execute(text("""
            UPDATE sync_log
            SET completed_at=:completed, status='completed', projects_synced=:n, changes_detected=:c
            WHERE sync_id=:sync_id
        """), {
            "completed": _utcnow().isoformat(),
            "n": len(projects),
            "c": changes_detected,
            "sync_id": sync_id
        })

    log.info("Sync completed. sync_id=%s projects=%s", sync_id, len(projects))
    return sync_id

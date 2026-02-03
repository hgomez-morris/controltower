from __future__ import annotations
import logging, json
from datetime import datetime, timezone, timedelta, date
from sqlalchemy import text
from controltower.db.connection import get_engine

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _parse_iso(ts: str | datetime | None) -> datetime | None:
    if not ts:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)
    return datetime.fromisoformat(str(ts).replace("Z","+00:00")).astimezone(timezone.utc)

def evaluate_rules(config: dict, sync_id: str) -> int:
    log = logging.getLogger("rules")
    engine = get_engine()
    created = 0

    with engine.begin() as conn:
        projects = conn.execute(text("""
            SELECT gid, name, owner_name, due_date, calculated_progress, last_status_update_at,
                   raw_data, last_activity_at, tasks_created_last_7d, tasks_completed_last_7d,
                   total_tasks
            FROM projects
        """)).mappings().all()

        for p in projects:
            created += _rule_no_status_update(conn, config, p)
            created += _rule_no_activity(conn, config, p)
            created += _rule_schedule_risk(conn, config, p)
            created += _rule_amount_of_tasks(conn, config, p)

        conn.execute(text("""UPDATE sync_log SET findings_created=:n WHERE sync_id=:sync_id"""),
                     {"n": created, "sync_id": sync_id})

    log.info("Rules evaluated. findings_created=%s", created)
    return created

def _get_open_finding(conn, project_gid: str, rule_id: str) -> dict | None:
    r = conn.execute(text("""
        SELECT id, severity, details
        FROM findings
        WHERE project_gid=:g AND rule_id=:r AND status='open'
        LIMIT 1
    """), {"g": project_gid, "r": rule_id}).mappings().first()
    return r

def _create_or_update_finding(conn, project_gid: str, rule_id: str, severity: str, details: dict) -> int:
    existing = _get_open_finding(conn, project_gid, rule_id)
    if not existing:
        conn.execute(text("""
        INSERT INTO findings(project_gid, rule_id, severity, status, details)
        VALUES(:g,:r,:s,'open',CAST(:d AS jsonb))
        """), {"g": project_gid, "r": rule_id, "s": severity, "d": json.dumps(details)})
        return 1

    if existing["severity"] != severity:
        new_details = dict(details)
        new_details["prev_severity"] = existing["severity"]
        new_details["slack_sent"] = False
        conn.execute(text("""
            UPDATE findings
            SET severity=:s, details=:d::jsonb
            WHERE id=:id
        """), {"id": existing["id"], "s": severity, "d": json.dumps(new_details)})
        return 1

    return 0

def _rule_no_status_update(conn, config: dict, p) -> int:
    rule = config["rules"]["no_status_update"]
    if not rule.get("enabled", True):
        return 0
    last = _parse_iso(p["last_status_update_at"])
    if not last:
        days = 999
    else:
        days = (_utcnow() - last).days
    if days > int(rule["days_threshold"]):
        return _create_or_update_finding(conn, p["gid"], "no_status_update", rule["base_severity"], {
            "project_name": p["name"],
            "owner_name": p["owner_name"],
            "days_since_last_status_update": days,
        })
    return 0

def _rule_no_activity(conn, config: dict, p) -> int:
    rule = config["rules"]["no_activity"]
    if not rule.get("enabled", True):
        return 0

    if p["tasks_created_last_7d"] is None or p["tasks_completed_last_7d"] is None:
        return 0
    created_7d = int(p["tasks_created_last_7d"] or 0)
    completed_7d = int(p["tasks_completed_last_7d"] or 0)
    if created_7d == 0 and completed_7d == 0:
        return _create_or_update_finding(conn, p["gid"], "no_activity", rule["base_severity"], {
            "project_name": p["name"],
            "owner_name": p["owner_name"],
            "tasks_created_last_7d": created_7d,
            "tasks_completed_last_7d": completed_7d,
        })
    return 0

def _rule_schedule_risk(conn, config: dict, p) -> int:
    rule = config["rules"]["schedule_risk"]
    if not rule.get("enabled", True):
        return 0
    due = p["due_date"]
    if not due:
        return 0  # cannot evaluate without due date
    if isinstance(due, str):
        due_date = date.fromisoformat(due)
    else:
        due_date = due
    days_remaining = (due_date - date.today()).days
    if p["calculated_progress"] is None:
        return 0
    progress = float(p["calculated_progress"] or 0.0)

    thresholds = sorted(rule["thresholds"], key=lambda x: x["days_remaining"])
    for t in thresholds:
        if days_remaining <= int(t["days_remaining"]) and progress < float(t["min_progress"]):
            return _create_or_update_finding(conn, p["gid"], "schedule_risk", t["severity"], {
                "project_name": p["name"],
                "owner_name": p["owner_name"],
                "days_remaining": days_remaining,
                "progress": progress,
                "min_progress_required": float(t["min_progress"]),
            })
    return 0

def _rule_amount_of_tasks(conn, config: dict, p) -> int:
    rule = config["rules"]["amount_of_tasks"]
    if not rule.get("enabled", True):
        return 0
    total = p["total_tasks"]
    if total is None:
        return 0
    max_tasks = int(rule.get("max_tasks", 3))
    if int(total) <= max_tasks:
        return _create_or_update_finding(conn, p["gid"], "amount_of_tasks", rule["base_severity"], {
            "project_name": p["name"],
            "owner_name": p["owner_name"],
            "total_tasks": int(total),
            "max_tasks": max_tasks,
        })
    return 0

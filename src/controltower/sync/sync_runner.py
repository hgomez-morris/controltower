from __future__ import annotations
import os, uuid, json, logging, time, re
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
    "tasks_modified_last_7d",
    "start_date",
    "planned_hours_total",
    "effective_hours_total",
    "calculated_progress",
    "last_activity_at",
]

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _phase_is_terminated_or_cancelled(project: dict) -> bool:
    fields = project.get("custom_fields") or []
    for f in fields:
        if (f.get("gid") == "1207505889399747") or (f.get("name") == "Fase del proyecto"):
            val = f.get("display_value") or (f.get("enum_value") or {}).get("name") or ""
            val = str(val).strip().lower()
            if ("terminad" in val) or ("cancelad" in val):
                return True
    return False


def _cf_map(project: dict) -> dict:
    fields = project.get("custom_fields") or []
    out = {}
    for f in fields:
        name = f.get("name")
        if not name:
            continue
        if f.get("display_value") is not None:
            val = f.get("display_value")
        else:
            val = f.get("text_value") or f.get("number_value") or (f.get("enum_value") or {}).get("name")
        out[name] = val
    return out


def _cf_value(project: dict, name: str) -> str:
    cf = _cf_map(project)
    val = cf.get(name, "")
    return "" if val is None else str(val)


def _cf_value_by_gid_or_name(project: dict, gid: str, name: str) -> str:
    fields = project.get("custom_fields") or []
    for f in fields:
        if f.get("gid") == gid or f.get("name") == name:
            val = f.get("display_value")
            if val is None:
                val = f.get("text_value") or f.get("number_value") or (f.get("enum_value") or {}).get("name")
            return "" if val is None else str(val)
    return ""

def _cf_value_any(project: dict, names: list[str]) -> str:
    targets = {n.strip().lower() for n in names}
    fields = project.get("custom_fields") or []
    for f in fields:
        name = str(f.get("name") or "").strip().lower()
        if name in targets:
            val = f.get("display_value")
            if val is None:
                val = f.get("text_value") or f.get("number_value") or (f.get("enum_value") or {}).get("name")
            return "" if val is None else str(val)
    return ""


def _cf_bool_like(project: dict, prefix: str) -> bool:
    fields = project.get("custom_fields") or []
    for f in fields:
        name = str(f.get("name") or "")
        if name.lower().startswith(prefix.lower()):
            val = f.get("display_value")
            if val is None:
                val = f.get("text_value") or f.get("number_value") or (f.get("enum_value") or {}).get("name")
            sval = str(val or "").strip().lower()
            return sval in {"si", "sí", "yes", "true", "1"}
    return False


def _cf_number_like(project: dict, names: list[str], gids: list[str] | None = None) -> float | None:
    fields = project.get("custom_fields") or []
    name_set = {n.lower() for n in names}
    gid_set = set(gids or [])
    for f in fields:
        name = str(f.get("name") or "")
        if (gid_set and f.get("gid") in gid_set) or name.lower() in name_set:
            val = f.get("display_value")
            if val is None:
                val = f.get("text_value") or f.get("number_value") or (f.get("enum_value") or {}).get("name")
            if val is None:
                return None
            text = str(val).strip().replace(",", ".")
            if not text:
                return None
            m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
            if not m:
                return None
            try:
                return float(m.group(0))
            except Exception:
                return None
    return None


def _cf_date_by_name(project: dict, name: str, gid: str | None = None) -> str | None:
    fields = project.get("custom_fields") or []
    for f in fields:
        if (gid and f.get("gid") == gid) or (f.get("name") == name):
            date_val = None
            if isinstance(f.get("date_value"), dict):
                date_val = f.get("date_value", {}).get("date")
            if not date_val:
                date_val = f.get("display_value")
            return date_val
    return None

def _recently_closed_or_cancelled(project: dict, cutoff: datetime) -> bool:
    # Use completed_at or modified_at as proxy for closure timing
    ts = project.get("completed_at") or project.get("modified_at") or project.get("created_at")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return False
    return dt >= cutoff

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
    modified_last = sum(
        1
        for t in tasks
        if t.get("modified_at")
        and datetime.fromisoformat(t["modified_at"].replace("Z", "+00:00")) >= lookback
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
        "tasks_modified_last_7d": modified_last,
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
        total_tasks, completed_tasks, tasks_created_last_7d, tasks_completed_last_7d, tasks_modified_last_7d,
        start_date, planned_hours_total, effective_hours_total,
        pmo_id, sponsor, cliente_nuevo, tipo_proyecto, clasificacion, segmento_empresa, pais,
        responsable_proyecto, business_vertical, fase_proyecto, en_plan_facturacion, completed_flag,
        raw_data, synced_at
    ) VALUES (
        :gid, :name, :owner_gid, :owner_name, :due_date, :status, :calculated_progress,
        :last_status_update_at, :last_status_update_by, :last_activity_at,
        :total_tasks, :completed_tasks, :tasks_created_last_7d, :tasks_completed_last_7d, :tasks_modified_last_7d,
        :start_date, :planned_hours_total, :effective_hours_total,
        :pmo_id, :sponsor, :cliente_nuevo, :tipo_proyecto, :clasificacion, :segmento_empresa, :pais,
        :responsable_proyecto, :business_vertical, :fase_proyecto, :en_plan_facturacion, :completed_flag,
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
        tasks_modified_last_7d = EXCLUDED.tasks_modified_last_7d,
        start_date = EXCLUDED.start_date,
        planned_hours_total = EXCLUDED.planned_hours_total,
        effective_hours_total = EXCLUDED.effective_hours_total,
        pmo_id = EXCLUDED.pmo_id,
        sponsor = EXCLUDED.sponsor,
        cliente_nuevo = EXCLUDED.cliente_nuevo,
        tipo_proyecto = EXCLUDED.tipo_proyecto,
        clasificacion = EXCLUDED.clasificacion,
        segmento_empresa = EXCLUDED.segmento_empresa,
        pais = EXCLUDED.pais,
        responsable_proyecto = EXCLUDED.responsable_proyecto,
        business_vertical = EXCLUDED.business_vertical,
        fase_proyecto = EXCLUDED.fase_proyecto,
        en_plan_facturacion = EXCLUDED.en_plan_facturacion,
        completed_flag = EXCLUDED.completed_flag,
        raw_data = EXCLUDED.raw_data,
        synced_at = EXCLUDED.synced_at
    """)
    conn.execute(q, project)

def upsert_status_update(conn, project_gid: str, su: dict) -> None:
    author = su.get("author") or {}
    row = {
        "gid": su.get("gid"),
        "project_gid": project_gid,
        "created_at": su.get("created_at"),
        "author_gid": author.get("gid"),
        "author_name": author.get("name"),
        "status_type": su.get("status_type"),
        "title": su.get("title"),
        "text": su.get("text"),
        "html_text": su.get("html_text"),
        "raw_data": json.dumps(su),
        "synced_at": _utcnow().isoformat(),
    }
    conn.execute(text("""
        INSERT INTO status_updates (
            gid, project_gid, created_at, author_gid, author_name, status_type,
            title, text, html_text, raw_data, synced_at
        ) VALUES (
            :gid, :project_gid, :created_at, :author_gid, :author_name, :status_type,
            :title, :text, :html_text, CAST(:raw_data AS jsonb), :synced_at
        )
        ON CONFLICT (gid) DO UPDATE SET
            project_gid = EXCLUDED.project_gid,
            created_at = EXCLUDED.created_at,
            author_gid = EXCLUDED.author_gid,
            author_name = EXCLUDED.author_name,
            status_type = EXCLUDED.status_type,
            title = EXCLUDED.title,
            text = EXCLUDED.text,
            html_text = EXCLUDED.html_text,
            raw_data = EXCLUDED.raw_data,
            synced_at = EXCLUDED.synced_at
    """), row)

def insert_status_update_comment(conn, status_update_gid: str, story: dict) -> None:
    author = story.get("created_by") or story.get("author") or {}
    row = {
        "status_update_gid": status_update_gid,
        "story_gid": story.get("gid"),
        "created_at": story.get("created_at"),
        "author_gid": author.get("gid"),
        "author_name": author.get("name"),
        "text": story.get("text"),
        "html_text": story.get("html_text"),
        "raw_data": json.dumps(story),
        "synced_at": _utcnow().isoformat(),
    }
    conn.execute(text("""
        INSERT INTO status_update_comments (
            status_update_gid, story_gid, created_at, author_gid, author_name,
            text, html_text, raw_data, synced_at
        ) VALUES (
            :status_update_gid, :story_gid, :created_at, :author_gid, :author_name,
            :text, :html_text, CAST(:raw_data AS jsonb), :synced_at
        )
        ON CONFLICT (story_gid) DO UPDATE SET
            status_update_gid = EXCLUDED.status_update_gid,
            created_at = EXCLUDED.created_at,
            author_gid = EXCLUDED.author_gid,
            author_name = EXCLUDED.author_name,
            text = EXCLUDED.text,
            html_text = EXCLUDED.html_text,
            raw_data = EXCLUDED.raw_data,
            synced_at = EXCLUDED.synced_at
    """), row)

def main_sync(config: dict) -> str:
    log = logging.getLogger("sync")
    sync_id = str(uuid.uuid4())
    started = _utcnow()

    engine = get_engine()
    token = os.getenv("ASANA_ACCESS_TOKEN","")
    workspace_gid = config["asana"]["workspace_gid"]
    client = AsanaReadOnlyClient(token)
    lookback_cfg = (config.get("rules") or {}).get("no_tasks_activity_last_7_days")
    if not lookback_cfg:
        lookback_cfg = (config.get("rules") or {}).get("no_activity", {})
    lookback_days = int((lookback_cfg or {}).get("days_threshold", 7))

    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO sync_log(sync_id, started_at, status) VALUES(:sync_id,:started,'running')"""),
                     {"sync_id": sync_id, "started": started.isoformat()})

        cutoff = _utcnow() - timedelta(days=30)
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
            is_completed = pfull.get("completed") is True
            is_terminated_or_cancelled = _phase_is_terminated_or_cancelled(pfull)
            if is_completed or is_terminated_or_cancelled:
                if not _recently_closed_or_cancelled(pfull, cutoff):
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
                "tasks_modified_last_7d": metrics["tasks_modified_last_7d"],
                "start_date": _cf_date_by_name(
                    pfull,
                    "Fecha Inicio del Proyecto",
                    gid="1207505889399729",
                ),
                "planned_hours_total": _cf_number_like(
                    pfull,
                    ["Horas planificadas", "Horas Planificadas"],
                    gids=["1207505889399760"],
                ),
                "effective_hours_total": _cf_number_like(
                    pfull,
                    ["Horas efectivas", "Horas efectivas "],
                    gids=["1207505889399792"],
                ),
                "pmo_id": _cf_value(pfull, "PMO ID"),
                "sponsor": _cf_value(pfull, "Sponsor"),
                "cliente_nuevo": _cf_value_any(pfull, ["cliente_nuevo", "cliente nuevo"]),
                "tipo_proyecto": _cf_value_any(pfull, ["tipo de proyecto", "tipo proyecto"]),
                "clasificacion": _cf_value_any(pfull, ["clasificación", "clasificacion"]),
                "segmento_empresa": _cf_value_any(pfull, ["segmento empresa", "segmento de empresa", "segmento"]),
                "pais": _cf_value_any(pfull, ["país", "pais"]),
                "responsable_proyecto": _cf_value(pfull, "Responsable Proyecto"),
                "business_vertical": _cf_value_by_gid_or_name(pfull, "1209701308000267", "Business Vertical"),
                "fase_proyecto": _cf_value_by_gid_or_name(pfull, "1207505889399747", "Fase del proyecto"),
                "en_plan_facturacion": _cf_bool_like(pfull, "En plan de fact"),
                "completed_flag": pfull.get("completed") is True,
                "raw_data": json.dumps({"project": pfull}),
                "synced_at": _utcnow().isoformat(),
            }
            existing = conn.execute(text("""
                SELECT gid, name, owner_gid, owner_name, due_date, status, calculated_progress,
                       last_status_update_at, last_status_update_by, last_activity_at,
                       total_tasks, completed_tasks, tasks_created_last_7d, tasks_completed_last_7d, tasks_modified_last_7d,
                       start_date, planned_hours_total, effective_hours_total
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

            # Status updates and comments
            try:
                updates = client.list_status_updates(pgid)
                for su in updates:
                    if not su.get("gid"):
                        continue
                    upsert_status_update(conn, pgid, su)
                    try:
                        stories = client.list_status_update_comments(su["gid"])
                        for st in stories:
                            if not st.get("gid"):
                                continue
                            insert_status_update_comment(conn, su["gid"], st)
                    except Exception as e:
                        log.warning("Failed to fetch status update comments. project=%s status_update=%s err=%s", pgid, su.get("gid"), e)
            except Exception as e:
                log.warning("Failed to fetch status updates. project=%s err=%s", pgid, e)

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

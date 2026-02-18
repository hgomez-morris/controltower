#!/usr/bin/env python3
"""Clockify -> PostgreSQL sync (single-file, self-contained).

Default mode is incremental with a rolling window (`--incremental-days`, default 90).
You can also run by explicit range with `--start/--end`.

Examples:
  python scripts/run_clockify_sync.py
  python scripts/run_clockify_sync.py --incremental-days 30
  python scripts/run_clockify_sync.py --start 2025-01-01T00:00:00Z --end 2025-10-31T23:59:59Z
"""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg

ISO_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)


def load_env(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ[key] = value


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def format_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_duration_to_seconds(duration: Optional[str]) -> float:
    if not duration:
        return 0.0
    match = ISO_DURATION_RE.match(duration)
    if not match:
        return 0.0
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return float(days * 86400 + hours * 3600 + minutes * 60 + seconds)


def request_json(url: str, api_key: str) -> Any:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        detail = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Clockify API error {err.code} for {url}: {detail}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"Network error while contacting Clockify: {err}") from err


def normalize_items_response(payload: Any) -> List[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "results", "content", "entities"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        if payload.get("id"):
            return [payload]
    return []


def paginated_get(base_url: str, api_key: str, path: str, query: Dict[str, str]) -> List[dict]:
    items: List[dict] = []
    page = 1
    page_size = 5000
    while True:
        params = dict(query)
        params["page"] = str(page)
        params["page-size"] = str(page_size)
        url = f"{base_url}{path}?{urllib.parse.urlencode(params)}"
        data = normalize_items_response(request_json(url, api_key))
        if not data:
            break
        items.extend(data)
        if len(data) < page_size:
            break
        page += 1
    return items


def paginated_entity_changes(
    base_url: str,
    api_key: str,
    workspace_id: str,
    change_type: str,
    entity_type: str,
    start: str,
    end: str,
) -> List[dict]:
    items: List[dict] = []
    page = 1
    limit = 5000
    while True:
        query = {
            "type": entity_type,
            "start": start,
            "end": end,
            "page": str(page),
            "limit": str(limit),
        }
        url = f"{base_url}/workspaces/{workspace_id}/entities/{change_type}?{urllib.parse.urlencode(query)}"
        data = normalize_items_response(request_json(url, api_key))
        if not data:
            break
        items.extend(data)
        if len(data) < limit:
            break
        page += 1
    return items


def fetch_users(base_url: str, api_key: str, workspace_id: str) -> List[dict]:
    return paginated_get(base_url, api_key, f"/workspaces/{workspace_id}/users", {})


def fetch_projects(base_url: str, api_key: str, workspace_id: str) -> List[dict]:
    return paginated_get(base_url, api_key, f"/workspaces/{workspace_id}/projects", {})


def fetch_time_entries_for_user(
    base_url: str,
    api_key: str,
    workspace_id: str,
    user_id: str,
    start: Optional[str],
    end: Optional[str],
) -> List[dict]:
    query: Dict[str, str] = {}
    if start:
        query["start"] = start
    if end:
        query["end"] = end
    return paginated_get(base_url, api_key, f"/workspaces/{workspace_id}/user/{user_id}/time-entries", query)


def fetch_time_entry_by_id(base_url: str, api_key: str, workspace_id: str, time_entry_id: str) -> Optional[dict]:
    url = f"{base_url}/workspaces/{workspace_id}/time-entries/{time_entry_id}"
    payload = request_json(url, api_key)
    return payload if isinstance(payload, dict) else None


def extract_time_entry_id(item: dict) -> Optional[str]:
    for key in ("timeEntryId", "entityId", "id"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    nested = item.get("entity")
    if isinstance(nested, dict):
        for key in ("id", "timeEntryId"):
            value = nested.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def extract_embedded_time_entry(item: dict) -> Optional[dict]:
    if isinstance(item.get("timeInterval"), dict) and item.get("id"):
        return item
    nested = item.get("entity")
    if isinstance(nested, dict) and isinstance(nested.get("timeInterval"), dict) and nested.get("id"):
        return nested
    return None


def fetch_incremental_time_entries(
    base_url: str,
    api_key: str,
    workspace_id: str,
    start: str,
    end: str,
) -> Tuple[List[dict], List[str], Dict[str, int]]:
    created_items = paginated_entity_changes(base_url, api_key, workspace_id, "created", "TIME_ENTRY", start, end)
    updated_items = paginated_entity_changes(base_url, api_key, workspace_id, "updated", "TIME_ENTRY", start, end)
    deleted_items = paginated_entity_changes(base_url, api_key, workspace_id, "deleted", "TIME_ENTRY", start, end)

    deleted_ids: Set[str] = set()
    for item in deleted_items:
        entry_id = extract_time_entry_id(item)
        if entry_id:
            deleted_ids.add(entry_id)

    seen_ids: Set[str] = set()
    entries_by_id: Dict[str, dict] = {}

    for item in created_items + updated_items:
        entry_id = extract_time_entry_id(item)
        if not entry_id or entry_id in deleted_ids:
            continue
        seen_ids.add(entry_id)
        embedded = extract_embedded_time_entry(item)
        if embedded:
            entries_by_id[entry_id] = embedded

    for entry_id in sorted(seen_ids):
        if entry_id in entries_by_id:
            continue
        fetched = fetch_time_entry_by_id(base_url, api_key, workspace_id, entry_id)
        if fetched:
            entries_by_id[entry_id] = fetched

    return (
        list(entries_by_id.values()),
        sorted(deleted_ids),
        {
            "created": len(created_items),
            "updated": len(updated_items),
            "deleted": len(deleted_items),
            "resolvedEntries": len(entries_by_id),
        },
    )


def normalize_user_name(user: dict) -> str:
    return (user.get("name") or "").strip() or (user.get("email") or "").strip() or user.get("id", "Unknown")


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "controltower"),
        user=os.getenv("DB_USER", "controltower"),
        password=os.getenv("DB_PASSWORD", "controltower"),
    )


def to_bool_int(value) -> Optional[int]:
    if value is None:
        return None
    return 1 if bool(value) else 0


def parse_entry_date(value: Optional[str]) -> Optional[date]:
    if not value or value == "UNKNOWN_DATE":
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def extract_tag_pairs(entry: dict) -> List[Tuple[str, str]]:
    pairs: Dict[str, str] = {}
    for tag_id in entry.get("tagIds") or []:
        if isinstance(tag_id, str) and tag_id:
            pairs[tag_id] = tag_id
    for tag in entry.get("tags") or []:
        if isinstance(tag, dict):
            tag_id = tag.get("id")
            tag_name = tag.get("name") or tag_id
            if tag_id:
                pairs[str(tag_id)] = str(tag_name or tag_id)
    return list(pairs.items())


def week_start_monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


def next_id(cur, table: str, column: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX({column}),0)+1 FROM clockify.{table}")
    return int(cur.fetchone()[0])


def ensure_clockify_schema(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS clockify")


def upsert_workspace(cur, workspace_id: str) -> None:
    cur.execute(
        """
        INSERT INTO clockify.workspaces (workspace_id, source, updated_at)
        VALUES (%s, 'clockify', NOW())
        ON CONFLICT(workspace_id) DO UPDATE SET source=EXCLUDED.source, updated_at=NOW()
        """,
        (workspace_id,),
    )


def upsert_client(cur, workspace_id: str, client_id: str, client_name: str) -> bool:
    if not client_id:
        return False
    cur.execute(
        """
        INSERT INTO clockify.clients (client_id, workspace_id, name, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT(client_id) DO UPDATE SET workspace_id=EXCLUDED.workspace_id, name=EXCLUDED.name, updated_at=NOW()
        """,
        (client_id, workspace_id, client_name or client_id),
    )
    return True


def upsert_project(cur, workspace_id: str, project: dict) -> bool:
    project_id = project.get("id")
    if not project_id:
        return False
    client_id = project.get("clientId") or None
    cur.execute(
        """
        INSERT INTO clockify.projects (
          project_id, workspace_id, client_id, name, archived, billable, client_name, color, note, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT(project_id) DO UPDATE SET
          workspace_id=EXCLUDED.workspace_id,
          client_id=EXCLUDED.client_id,
          name=EXCLUDED.name,
          archived=EXCLUDED.archived,
          billable=EXCLUDED.billable,
          client_name=EXCLUDED.client_name,
          color=EXCLUDED.color,
          note=EXCLUDED.note,
          updated_at=NOW()
        """,
        (
            project_id,
            workspace_id,
            client_id,
            project.get("name") or "Unnamed Project",
            to_bool_int(project.get("archived")),
            to_bool_int(project.get("billable")),
            project.get("clientName"),
            project.get("color"),
            project.get("note"),
        ),
    )
    return True


def upsert_person(cur, workspace_id: str, person_id: str, person_name: str) -> bool:
    if not person_id:
        return False
    cur.execute(
        """
        INSERT INTO clockify.people (person_id, workspace_id, name, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT(person_id) DO UPDATE SET workspace_id=EXCLUDED.workspace_id, name=EXCLUDED.name, updated_at=NOW()
        """,
        (person_id, workspace_id, person_name or person_id),
    )
    return True


def upsert_task(cur, workspace_id: str, task_id: str, task_name: str, project_id: Optional[str]) -> bool:
    if not task_id:
        return False
    cur.execute(
        """
        INSERT INTO clockify.tasks (task_id, workspace_id, project_id, name, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT(task_id) DO UPDATE SET
          workspace_id=EXCLUDED.workspace_id,
          project_id=EXCLUDED.project_id,
          name=EXCLUDED.name,
          updated_at=NOW()
        """,
        (task_id, workspace_id, project_id, task_name or task_id),
    )
    return True


def upsert_tag(cur, workspace_id: str, tag_id: str, tag_name: str) -> bool:
    if not tag_id:
        return False
    cur.execute(
        """
        INSERT INTO clockify.tags (tag_id, workspace_id, name, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT(tag_id) DO UPDATE SET workspace_id=EXCLUDED.workspace_id, name=EXCLUDED.name, updated_at=NOW()
        """,
        (tag_id, workspace_id, tag_name or tag_id),
    )
    return True


def upsert_time_entry(cur, workspace_id: str, entry: dict) -> bool:
    time_entry_id = entry.get("timeEntryId")
    person_id = entry.get("userId")
    if not time_entry_id or not person_id:
        return False

    project_id = entry.get("projectId")
    task_id = entry.get("taskId")
    if project_id in ("NO_PROJECT", "", None):
        project_id = None
    if task_id in ("NO_TASK", "", None):
        task_id = None

    duration_iso = entry.get("duration")
    hours = float(entry.get("hours") or 0.0)
    duration_seconds = parse_iso_duration_to_seconds(duration_iso)
    if duration_seconds <= 0 and hours > 0:
        duration_seconds = round(hours * 3600.0, 3)

    cur.execute(
        """
        INSERT INTO clockify.time_entries (
          time_entry_id, workspace_id, project_id, task_id, person_id,
          start_time_utc, end_time_utc, entry_date, duration_iso, duration_seconds, hours,
          description, billable, source_payload, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT(time_entry_id) DO UPDATE SET
          workspace_id=EXCLUDED.workspace_id,
          project_id=EXCLUDED.project_id,
          task_id=EXCLUDED.task_id,
          person_id=EXCLUDED.person_id,
          start_time_utc=EXCLUDED.start_time_utc,
          end_time_utc=EXCLUDED.end_time_utc,
          entry_date=EXCLUDED.entry_date,
          duration_iso=EXCLUDED.duration_iso,
          duration_seconds=EXCLUDED.duration_seconds,
          hours=EXCLUDED.hours,
          description=EXCLUDED.description,
          billable=EXCLUDED.billable,
          source_payload=EXCLUDED.source_payload,
          updated_at=NOW()
        """,
        (
            time_entry_id,
            workspace_id,
            project_id,
            task_id,
            person_id,
            entry.get("start"),
            entry.get("end"),
            entry.get("date"),
            duration_iso,
            duration_seconds,
            hours,
            entry.get("description"),
            to_bool_int(entry.get("billable")),
            json.dumps(entry, ensure_ascii=False),
        ),
    )
    return True


def upsert_time_entry_tag(cur, time_entry_id: str, tag_id: str) -> bool:
    if not time_entry_id or not tag_id:
        return False
    cur.execute(
        """
        INSERT INTO clockify.time_entry_tags (time_entry_id, tag_id)
        VALUES (%s, %s)
        ON CONFLICT(time_entry_id, tag_id) DO NOTHING
        """,
        (time_entry_id, tag_id),
    )
    return True


def populate_calendar_weeks(cur, min_date: Optional[date], max_date: Optional[date]) -> int:
    if not min_date or not max_date:
        return 0
    current = week_start_monday(min_date)
    end_week = week_start_monday(max_date)
    count = 0
    while current <= end_week:
        week_end = current + timedelta(days=6)
        iso_year, iso_week, _ = current.isocalendar()
        quarter = ((current.month - 1) // 3) + 1
        cur.execute(
            """
            INSERT INTO clockify.calendar_weeks (
              week_start, week_end, iso_year, iso_week, fiscal_year, fiscal_week, month_key, quarter_key, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT(week_start) DO UPDATE SET
              week_end=EXCLUDED.week_end,
              iso_year=EXCLUDED.iso_year,
              iso_week=EXCLUDED.iso_week,
              fiscal_year=EXCLUDED.fiscal_year,
              fiscal_week=EXCLUDED.fiscal_week,
              month_key=EXCLUDED.month_key,
              quarter_key=EXCLUDED.quarter_key,
              updated_at=NOW()
            """,
            (
                current.isoformat(),
                week_end.isoformat(),
                iso_year,
                iso_week,
                iso_year,
                iso_week,
                current.strftime("%Y-%m"),
                f"{current.year}-Q{quarter}",
            ),
        )
        current += timedelta(days=7)
        count += 1
    return count


def build_entries_range_by_user(
    base_url: str,
    api_key: str,
    workspace_id: str,
    users: List[dict],
    user_name_by_id: Dict[str, str],
    start: Optional[str],
    end: Optional[str],
) -> List[dict]:
    entries: List[dict] = []
    for idx, user in enumerate(users, start=1):
        user_id = user.get("id")
        if not user_id:
            continue
        user_name = user_name_by_id.get(user_id, user_id)
        print(f"[{idx}/{len(users)}] Fetching user time entries: {user_name}")
        user_entries = fetch_time_entries_for_user(base_url, api_key, workspace_id, user_id, start, end)
        entries.extend(user_entries)
    return entries


def normalize_entry(entry: dict, project_name_by_id: Dict[str, str], user_name_by_id: Dict[str, str]) -> Optional[dict]:
    project_id = entry.get("projectId") or "NO_PROJECT"
    task_id = entry.get("taskId") or "NO_TASK"
    user_id = entry.get("userId") or "UNKNOWN_USER"
    interval = entry.get("timeInterval") or {}

    start_dt = parse_iso_datetime(interval.get("start"))
    end_dt = parse_iso_datetime(interval.get("end"))
    duration_seconds = parse_iso_duration_to_seconds(interval.get("duration"))
    if duration_seconds <= 0 and start_dt and end_dt:
        duration_seconds = max((end_dt - start_dt).total_seconds(), 0.0)

    hours = round(duration_seconds / 3600.0, 6)
    if hours <= 0:
        return None

    date_key = start_dt.astimezone(timezone.utc).date().isoformat() if start_dt else "UNKNOWN_DATE"

    return {
        "timeEntryId": entry.get("id"),
        "projectId": project_id,
        "projectName": project_name_by_id.get(project_id, "No Project"),
        "taskId": task_id if task_id != "NO_TASK" else None,
        "taskName": entry.get("taskName"),
        "userId": user_id,
        "userName": user_name_by_id.get(user_id, user_id),
        "start": interval.get("start"),
        "end": interval.get("end"),
        "date": date_key,
        "duration": interval.get("duration"),
        "hours": round(hours, 4),
        "description": entry.get("description"),
        "billable": entry.get("billable"),
        "tagIds": entry.get("tagIds") or [],
        "tags": entry.get("tags") or [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Clockify directly into PostgreSQL clockify schema")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--start", default=None, help="ISO datetime start")
    parser.add_argument("--end", default=None, help="ISO datetime end")
    parser.add_argument("--incremental-days", type=int, default=90, help="Rolling window days (default: 90)")
    parser.add_argument(
        "--mode",
        choices=("auto", "incremental", "range_by_user"),
        default="auto",
        help="auto: incremental unless --start/--end is provided",
    )
    args = parser.parse_args()

    load_env(Path(args.env_file))

    api_key = os.getenv("CLOCKIFY_API_KEY")
    workspace_id = os.getenv("CLOCKIFY_WORKSPACE_ID")
    base_url = os.getenv("CLOCKIFY_BASE_URL", "https://api.clockify.me/api/v1").rstrip("/")

    if not api_key:
        raise RuntimeError("Missing CLOCKIFY_API_KEY")
    if not workspace_id:
        raise RuntimeError("Missing CLOCKIFY_WORKSPACE_ID")

    mode = args.mode
    if mode == "auto":
        mode = "range_by_user" if (args.start or args.end) else "incremental"

    start = args.start
    end = args.end
    if mode == "incremental" and not (start or end):
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=args.incremental_days)
        start = format_utc_z(start_dt)
        end = format_utc_z(end_dt)

    print(f"Mode: {mode}")
    print(f"Workspace: {workspace_id}")
    print(f"Range: start={start} end={end}")

    users = fetch_users(base_url, api_key, workspace_id)
    projects = fetch_projects(base_url, api_key, workspace_id)
    user_name_by_id = {u.get("id", ""): normalize_user_name(u) for u in users if u.get("id")}
    project_name_by_id = {p.get("id", ""): p.get("name", "") for p in projects if p.get("id")}

    raw_entries: List[dict]
    deleted_time_entry_ids: List[str] = []
    entity_changes: Dict[str, int] = {}

    if mode == "incremental":
        raw_entries, deleted_time_entry_ids, entity_changes = fetch_incremental_time_entries(
            base_url=base_url,
            api_key=api_key,
            workspace_id=workspace_id,
            start=start or "",
            end=end or "",
        )
    else:
        raw_entries = build_entries_range_by_user(base_url, api_key, workspace_id, users, user_name_by_id, start, end)

    normalized_entries: List[dict] = []
    for entry in raw_entries:
        norm = normalize_entry(entry, project_name_by_id, user_name_by_id)
        if norm:
            normalized_entries.append(norm)

    started_at = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ensure_clockify_schema(cur)

            run_id = next_id(cur, "sync_runs", "run_id")
            source_ref = f"clockify_api:{mode}"
            cur.execute(
                """
                INSERT INTO clockify.sync_runs (run_id, started_at, source_type, source_reference, workspace_id, status)
                VALUES (%s, %s, 'clockify_api', %s, %s, 'running')
                """,
                (run_id, started_at, source_ref, workspace_id),
            )

            upsert_workspace(cur, workspace_id)

            clients_upserted = 0
            projects_upserted = 0
            for project in projects:
                client_id = project.get("clientId")
                if client_id:
                    clients_upserted += 1 if upsert_client(cur, workspace_id, client_id, project.get("clientName") or client_id) else 0
                projects_upserted += 1 if upsert_project(cur, workspace_id, project) else 0

            people_upserted = 0
            for user in users:
                user_id = user.get("id")
                if user_id:
                    people_upserted += 1 if upsert_person(cur, workspace_id, user_id, user_name_by_id.get(user_id, user_id)) else 0

            tasks_seen: Dict[str, Tuple[str, Optional[str]]] = {}
            tags_seen: Dict[str, str] = {}
            entry_tag_links: Set[Tuple[str, str]] = set()
            entry_ids_in_payload: Set[str] = set()
            min_entry_date: Optional[date] = None
            max_entry_date: Optional[date] = None

            for entry in normalized_entries:
                entry_id = entry.get("timeEntryId")
                if entry_id:
                    entry_ids_in_payload.add(entry_id)

                task_id = entry.get("taskId")
                project_id = entry.get("projectId")
                if task_id and task_id not in ("NO_TASK", ""):
                    tasks_seen[str(task_id)] = (entry.get("taskName") or str(task_id), project_id)

                for tag_id, tag_name in extract_tag_pairs(entry):
                    tags_seen[tag_id] = tag_name
                    if entry_id:
                        entry_tag_links.add((entry_id, tag_id))

                entry_date = parse_entry_date(entry.get("date"))
                if entry_date:
                    if min_entry_date is None or entry_date < min_entry_date:
                        min_entry_date = entry_date
                    if max_entry_date is None or entry_date > max_entry_date:
                        max_entry_date = entry_date

            tasks_upserted = 0
            for task_id, (task_name, project_id) in tasks_seen.items():
                if project_id in ("NO_PROJECT", "", None):
                    project_id = None
                tasks_upserted += 1 if upsert_task(cur, workspace_id, task_id, task_name, project_id) else 0

            tags_upserted = 0
            for tag_id, tag_name in tags_seen.items():
                tags_upserted += 1 if upsert_tag(cur, workspace_id, tag_id, tag_name) else 0

            entries_upserted = 0
            for entry in normalized_entries:
                entries_upserted += 1 if upsert_time_entry(cur, workspace_id, entry) else 0

            if entry_ids_in_payload:
                cur.execute("DELETE FROM clockify.time_entry_tags WHERE time_entry_id = ANY(%s)", (list(entry_ids_in_payload),))

            deleted_entries_applied = 0
            if deleted_time_entry_ids:
                cur.execute("DELETE FROM clockify.time_entry_tags WHERE time_entry_id = ANY(%s)", (deleted_time_entry_ids,))
                cur.execute("DELETE FROM clockify.time_entries WHERE time_entry_id = ANY(%s)", (deleted_time_entry_ids,))
                deleted_entries_applied = cur.rowcount if cur.rowcount is not None else 0

            time_entry_tags_upserted = 0
            for time_entry_id, tag_id in entry_tag_links:
                time_entry_tags_upserted += 1 if upsert_time_entry_tag(cur, time_entry_id, tag_id) else 0

            cur.execute(
                """
                SELECT MIN(entry_date), MAX(entry_date)
                FROM clockify.time_entries
                WHERE workspace_id=%s AND entry_date IS NOT NULL
                """,
                (workspace_id,),
            )
            db_min_max = cur.fetchone()
            if db_min_max and db_min_max[0] and db_min_max[1]:
                min_entry_date = db_min_max[0]
                max_entry_date = db_min_max[1]

            calendar_weeks_generated = populate_calendar_weeks(cur, min_entry_date, max_entry_date)

            finished_at = datetime.now(timezone.utc).isoformat()
            cur.execute(
                """
                UPDATE clockify.sync_runs
                SET finished_at=%s,
                    entries_upserted=%s,
                    people_upserted=%s,
                    projects_upserted=%s,
                    tasks_upserted=%s,
                    clients_upserted=%s,
                    tags_upserted=%s,
                    time_entry_tags_upserted=%s,
                    calendar_weeks_generated=%s,
                    status='success'
                WHERE run_id=%s
                """,
                (
                    finished_at,
                    entries_upserted,
                    people_upserted,
                    projects_upserted,
                    tasks_upserted,
                    clients_upserted,
                    tags_upserted,
                    time_entry_tags_upserted,
                    calendar_weeks_generated,
                    run_id,
                ),
            )

            sync_id = next_id(cur, "sync_history", "sync_id")
            cur.execute(
                """
                INSERT INTO clockify.sync_history (
                  sync_id, run_id, workspace_id, source_type, source_reference, status, synced_at, created_at
                ) VALUES (%s, %s, %s, 'clockify_api', %s, 'success', %s, NOW())
                """,
                (sync_id, run_id, workspace_id, source_ref, finished_at),
            )

        conn.commit()
        print("Clockify sync completed successfully")
        print(f"Users: {len(users)} | Projects: {len(projects)} | Entries upserted: {len(normalized_entries)}")
        if mode == "incremental":
            print(f"Entity changes: {entity_changes}")
            print(f"Deleted entry ids applied: {len(deleted_time_entry_ids)}")
        return 0

    except Exception as exc:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                run_id = next_id(cur, "sync_runs", "run_id")
                finished_at = datetime.now(timezone.utc).isoformat()
                source_ref = f"clockify_api:{mode}"
                cur.execute(
                    """
                    INSERT INTO clockify.sync_runs (
                      run_id, started_at, finished_at, source_type, source_reference, workspace_id, status, error_message
                    ) VALUES (%s, %s, %s, 'clockify_api', %s, %s, 'failed', %s)
                    """,
                    (run_id, started_at, finished_at, source_ref, workspace_id, str(exc)),
                )
                sync_id = next_id(cur, "sync_history", "sync_id")
                cur.execute(
                    """
                    INSERT INTO clockify.sync_history (
                      sync_id, run_id, workspace_id, source_type, source_reference, status, synced_at, created_at
                    ) VALUES (%s, %s, %s, 'clockify_api', %s, 'failed', %s, NOW())
                    """,
                    (sync_id, run_id, workspace_id, source_ref, finished_at),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

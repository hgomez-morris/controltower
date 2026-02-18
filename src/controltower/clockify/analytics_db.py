import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd

DEFAULT_DB_PATH = "postgres"
DEFAULT_WEEKLY_HOURS = 45

ASANA_PLANNED_FIELD_NAMES = {
    "horas planificadas",
    "horas planificada",
    "horas planificadas (hh)",
    "horas planificadas (h)",
}

ASANA_EFFECTIVE_FIELD_NAMES = {
    "horas efectivas",
    "horas efectivas (hh)",
    "horas efectivas (h)",
    "horas reales",
    "horas reales (h)",
}

ASANA_STATUS_FIELD_NAMES = {
    "status",
    "estado",
    "project status",
}

ASANA_PHASE_FIELD_NAMES = {
    "fase de proyecto",
    "fase del proyecto",
    "fase proyecto",
}

ASANA_RESPONSIBLE_FIELD_NAMES = {
    "responsable de proyecto",
    "responsable del proyecto",
    "responsable proyecto",
    "project manager",
}


def _load_env_file_if_needed(env_path: Path = Path(".env")) -> None:
    if os.getenv("DB_HOST"):
        return
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


def get_conn(_db_path: Path | None = None):
    _load_env_file_if_needed()
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "controltower"),
        user=os.getenv("DB_USER", "controltower"),
        password=os.getenv("DB_PASSWORD", "controltower"),
        row_factory=dict_row,
    )


def extract_pmo_id(project_name: str) -> str | None:
    if not project_name:
        return None
    match = re.search(r"(PMO-\d+)", str(project_name).upper())
    return match.group(1) if match else None


def _parse_hours_value(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _parse_bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si", "sí"}


def _query_df(conn, query: str, params=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        if isinstance(rows[0], dict):
            return pd.DataFrame(rows)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)


def fetch_asana_hours_by_pmo_ids(pmo_ids: List[str]) -> Dict[str, Dict]:
    normalized_ids = sorted({str(p).strip().upper() for p in pmo_ids if p})
    if not normalized_ids:
        return {}

    _load_env_file_if_needed()
    required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    if any(not os.getenv(k) for k in required):
        return {}

    query = """
        WITH source_rows AS (
            SELECT raw_data, 0 AS source_priority
            FROM projects
            UNION ALL
            SELECT raw_data, 1 AS source_priority
            FROM projects_history
        ),
        base AS (
            SELECT
                UPPER(
                    COALESCE(
                        (
                            SELECT COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value')
                            FROM jsonb_array_elements(COALESCE(raw_data->'project'->'custom_fields', '[]'::jsonb)) cf
                            WHERE lower(cf->>'name') = 'pmo id'
                            LIMIT 1
                        ),
                        ''
                    )
                ) AS pmo_id,
                raw_data->'project'->>'gid' AS gid,
                raw_data->'project'->>'name' AS name,
                raw_data->'project'->>'completed' AS completed_raw,
                COALESCE(
                    raw_data->'project'->'current_status'->>'title',
                    raw_data->'project'->'current_status'->>'text',
                    raw_data->'project'->>'status'
                ) AS status_top_raw,
                source_priority,
                jsonb_array_elements(COALESCE(raw_data->'project'->'custom_fields', '[]'::jsonb)) AS cf
            FROM source_rows
        ),
        agg AS (
            SELECT
                pmo_id,
                gid,
                name,
                completed_raw,
                status_top_raw,
                MIN(source_priority) AS source_priority,
                MAX(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value'))
                    FILTER (WHERE lower(cf->>'name') = ANY(%s)) AS horas_planificadas_raw,
                MAX(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value'))
                    FILTER (WHERE lower(cf->>'name') = ANY(%s)) AS horas_efectivas_raw,
                MAX(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value'))
                    FILTER (WHERE lower(cf->>'name') = ANY(%s)) AS status_cf_raw,
                MAX(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value'))
                    FILTER (WHERE lower(cf->>'name') = ANY(%s)) AS fase_proyecto_raw,
                MAX(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value'))
                    FILTER (WHERE lower(cf->>'name') = ANY(%s)) AS responsable_proyecto_raw
            FROM base
            WHERE pmo_id = ANY(%s)
            GROUP BY pmo_id, gid, name, completed_raw, status_top_raw
        ),
        ranked AS (
            SELECT
                pmo_id,
                gid,
                name,
                completed_raw,
                status_top_raw,
                horas_planificadas_raw,
                horas_efectivas_raw,
                status_cf_raw,
                fase_proyecto_raw,
                responsable_proyecto_raw,
                ROW_NUMBER() OVER (
                    PARTITION BY pmo_id
                    ORDER BY
                        source_priority ASC,
                        (CASE WHEN horas_planificadas_raw IS NOT NULL THEN 1 ELSE 0 END
                         + CASE WHEN horas_efectivas_raw IS NOT NULL THEN 1 ELSE 0 END) DESC,
                        gid
                ) AS rn
            FROM agg
        )
        SELECT
            pmo_id,
            gid,
            name,
            completed_raw,
            status_top_raw,
            horas_planificadas_raw,
            horas_efectivas_raw,
            status_cf_raw,
            fase_proyecto_raw,
            responsable_proyecto_raw
        FROM ranked
        WHERE rn = 1;
    """

    params = (
        [v.lower() for v in sorted(ASANA_PLANNED_FIELD_NAMES)],
        [v.lower() for v in sorted(ASANA_EFFECTIVE_FIELD_NAMES)],
        [v.lower() for v in sorted(ASANA_STATUS_FIELD_NAMES)],
        [v.lower() for v in sorted(ASANA_PHASE_FIELD_NAMES)],
        [v.lower() for v in sorted(ASANA_RESPONSIBLE_FIELD_NAMES)],
        normalized_ids,
    )

    try:
        with get_conn() as conn:
            rows = _query_df(conn, query, params=params)
    except Exception:
        return {}

    result: Dict[str, Dict] = {}
    for _, row in rows.iterrows():
        pmo_id = str(row.get("pmo_id", "")).strip().upper()
        if not pmo_id:
            continue
        result[pmo_id] = {
            "gid": row.get("gid"),
            "name": row.get("name"),
            "completed": _parse_bool_value(row.get("completed_raw")),
            "status": row.get("status_cf_raw") or row.get("status_top_raw"),
            "fase_proyecto": row.get("fase_proyecto_raw"),
            "responsable_proyecto": row.get("responsable_proyecto_raw"),
            "horas_planificadas": _parse_hours_value(row.get("horas_planificadas_raw")),
            "horas_efectivas": _parse_hours_value(row.get("horas_efectivas_raw")),
        }
    return result


def fetch_last_sync(conn) -> Dict:
    row = conn.execute(
        """
        SELECT synced_at, status, source_reference
        FROM clockify.sync_history
        ORDER BY synced_at DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else {}


def fetch_week_starts(conn, weeks_count: int, include_current_week: bool = True) -> List[str]:
    today = date.today()
    current_week_start = (today - timedelta(days=today.weekday())).isoformat()
    if include_current_week:
        rows = conn.execute(
            """
            SELECT week_start
            FROM clockify.calendar_weeks
            WHERE week_start <= %s
            ORDER BY week_start DESC
            LIMIT %s
            """,
            (current_week_start, weeks_count),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT week_start
            FROM clockify.calendar_weeks
            WHERE week_start < %s
            ORDER BY week_start DESC
            LIMIT %s
            """,
            (current_week_start, weeks_count),
        ).fetchall()
    return [str(r["week_start"]) for r in rows]


def fetch_time_entries_count_for_weeks(conn, week_starts: List[str]) -> int:
    if not week_starts:
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM clockify.time_entries te
        JOIN clockify.calendar_weeks cw
          ON te.entry_date BETWEEN cw.week_start AND cw.week_end
        WHERE cw.week_start = ANY(%s)
        """,
        (week_starts,),
    ).fetchone()
    return int(row["n"]) if row else 0


def fetch_kpis(conn) -> Dict[str, int]:
    return {
        "time_entries": conn.execute("SELECT COUNT(*) AS n FROM clockify.time_entries").fetchone()["n"],
        "people": conn.execute("SELECT COUNT(*) AS n FROM clockify.people").fetchone()["n"],
        "projects": conn.execute("SELECT COUNT(*) AS n FROM clockify.projects").fetchone()["n"],
    }


def fetch_weekly_hours_by_person(conn, week_starts: List[str]) -> pd.DataFrame:
    if not week_starts:
        return pd.DataFrame(columns=["person_name", "week_start", "total_hours"])

    query = """
        SELECT person_name, week_start, SUM(total_hours) AS total_hours
        FROM clockify.v_person_weekly_hours
        WHERE week_start = ANY(%s)
        GROUP BY person_name, week_start
    """
    return _query_df(conn, query, params=(week_starts,))


def fetch_weekly_hours_by_person_project(
    conn,
    week_starts: List[str],
    person_name: str,
) -> pd.DataFrame:
    if not week_starts:
        return pd.DataFrame(columns=["project_name", "week_start", "total_hours"])
    query = """
        SELECT
          COALESCE(p.name, 'No Project') AS project_name,
          cw.week_start,
          SUM(te.hours) AS total_hours
        FROM clockify.time_entries te
        JOIN clockify.people pe
          ON pe.person_id = te.person_id
        LEFT JOIN clockify.projects p
          ON p.project_id = te.project_id
        JOIN clockify.calendar_weeks cw
          ON te.entry_date BETWEEN cw.week_start AND cw.week_end
        WHERE pe.name = %s
          AND cw.week_start = ANY(%s)
        GROUP BY COALESCE(p.name, 'No Project'), cw.week_start
    """
    return _query_df(conn, query, params=(person_name, week_starts))


def fetch_weekly_hours_by_project_person(
    conn,
    week_starts: List[str],
    project_name: str,
) -> pd.DataFrame:
    if not week_starts:
        return pd.DataFrame(columns=["person_name", "week_start", "total_hours"])
    query = """
        SELECT
          pe.name AS person_name,
          cw.week_start,
          SUM(te.hours) AS total_hours
        FROM clockify.time_entries te
        JOIN clockify.people pe
          ON pe.person_id = te.person_id
        LEFT JOIN clockify.projects p
          ON p.project_id = te.project_id
        JOIN clockify.calendar_weeks cw
          ON te.entry_date BETWEEN cw.week_start AND cw.week_end
        WHERE COALESCE(p.name, 'No Project') = %s
          AND cw.week_start = ANY(%s)
        GROUP BY pe.name, cw.week_start
    """
    return _query_df(conn, query, params=(project_name, week_starts))


def fetch_total_hours_by_project_person(conn, project_name: str) -> Dict[str, float]:
    query = """
        SELECT
          pe.name AS person_name,
          SUM(te.hours) AS total_hours
        FROM clockify.time_entries te
        JOIN clockify.people pe
          ON pe.person_id = te.person_id
        LEFT JOIN clockify.projects p
          ON p.project_id = te.project_id
        WHERE COALESCE(p.name, 'No Project') = %s
        GROUP BY pe.name
    """
    df = _query_df(conn, query, params=(project_name,))
    if df.empty:
        return {}
    return {
        str(row["person_name"]): round(float(row["total_hours"]), 2)
        for _, row in df.iterrows()
    }


def fetch_weekly_hours_by_project(conn, week_starts: List[str]) -> pd.DataFrame:
    if not week_starts:
        return pd.DataFrame(columns=["project_name", "week_start", "total_hours"])

    query = """
        SELECT
          COALESCE(p.name, 'No Project') AS project_name,
          cw.week_start,
          SUM(te.hours) AS total_hours
        FROM clockify.time_entries te
        LEFT JOIN clockify.projects p ON p.project_id = te.project_id
        JOIN clockify.calendar_weeks cw
          ON te.entry_date BETWEEN cw.week_start AND cw.week_end
        WHERE cw.week_start = ANY(%s)
        GROUP BY COALESCE(p.name, 'No Project'), cw.week_start
    """
    return _query_df(conn, query, params=(week_starts,))


def fetch_total_hours_by_project(conn) -> Dict[str, float]:
    query = """
        SELECT
          COALESCE(p.name, 'No Project') AS project_name,
          SUM(te.hours) AS total_hours
        FROM clockify.time_entries te
        LEFT JOIN clockify.projects p ON p.project_id = te.project_id
        GROUP BY COALESCE(p.name, 'No Project')
    """
    rows = _query_df(conn, query)
    if rows.empty:
        return {}
    return {
        str(row["project_name"]): round(float(row["total_hours"]), 2)
        for _, row in rows.iterrows()
    }


def to_csv(rows: List[Dict]) -> str:
    if not rows:
        return ""
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        values = []
        for h in headers:
            val = str(row.get(h, ""))
            if "," in val or '"' in val:
                val = '"' + val.replace('"', '""') + '"'
            values.append(val)
        lines.append(",".join(values))
    return "\n".join(lines)


def sparkline(values: List[float]) -> str:
    if not values:
        return ""
    ticks = "▁▂▃▄▅▆▇█"
    v_min = min(values)
    v_max = max(values)
    if v_max == v_min:
        return ticks[3] * len(values)
    scale = (len(ticks) - 1) / (v_max - v_min)
    return "".join(ticks[int((v - v_min) * scale)] for v in values)


def person_percent_rows(df_hours: pd.DataFrame, week_starts: List[str], weekly_hours: int) -> List[Dict]:
    if df_hours.empty:
        return []
    df_hours = df_hours.copy()
    df_hours["week_start"] = df_hours["week_start"].astype(str)

    pivot = (
        df_hours.pivot(index="person_name", columns="week_start", values="total_hours")
        .fillna(0.0)
        .astype(float)
    )

    for week in week_starts:
        if week not in pivot.columns:
            pivot[week] = 0.0

    pivot = pivot[week_starts]

    rows = []
    for person in pivot.index:
        hours_values = [float(pivot.at[person, week]) for week in week_starts]
        pct_values = [round((h / weekly_hours) * 100.0, 1) for h in hours_values]
        row = {"Persona": person, "Tendencia": sparkline(pct_values)}
        for week, pct in zip(week_starts, pct_values):
            row[week] = pct
        row["Promedio"] = round(sum(pct_values) / len(pct_values), 1)
        rows.append(row)

    rows.sort(key=lambda r: (-r["Promedio"], r["Persona"].lower()))
    return rows


def project_hours_rows(
    df_hours: pd.DataFrame,
    week_starts: List[str],
    total_hours_by_project: Dict[str, float] | None = None,
) -> List[Dict]:
    if df_hours.empty:
        return []
    df_hours = df_hours.copy()
    df_hours["week_start"] = df_hours["week_start"].astype(str)

    pivot = (
        df_hours.pivot(index="project_name", columns="week_start", values="total_hours")
        .fillna(0.0)
        .astype(float)
    )

    for week in week_starts:
        if week not in pivot.columns:
            pivot[week] = 0.0

    pivot = pivot[week_starts]

    rows = []
    for project in pivot.index:
        week_vals = [round(float(pivot.at[project, week]), 2) for week in week_starts]
        chrono_week_vals = list(reversed(week_vals))
        cumulative_vals = []
        acc = 0.0
        for hours in chrono_week_vals:
            acc += hours
            cumulative_vals.append(round(acc, 2))

        if any(hours < 0 for hours in chrono_week_vals):
            trend_label = "BAJA"
        elif chrono_week_vals and chrono_week_vals[-1] <= 0:
            trend_label = "SIN AVANCE"
        else:
            trend_label = "OK"

        if trend_label == "BAJA":
            trend_cell = f"▼ {sparkline(cumulative_vals)}"
        elif trend_label == "SIN AVANCE":
            trend_cell = f"■ {sparkline(cumulative_vals)}"
        else:
            trend_cell = f"● {sparkline(cumulative_vals)}"

        row = {
            "Proyecto": project,
            "Tendencia acumulada": trend_cell,
            "Total (semanas visibles)": round(sum(week_vals), 2),
        }
        if total_hours_by_project is not None:
            row["Total proyecto"] = round(float(total_hours_by_project.get(project, 0.0)), 2)
        for week, val in zip(week_starts, week_vals):
            row[week] = val
        rows.append(row)

    rows.sort(key=lambda r: (-r["Total (semanas visibles)"], r["Proyecto"].lower()))
    return rows

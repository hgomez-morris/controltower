import streamlit as st
import json
import os
import pandas as pd
import io
import zipfile
import plotly.express as px
from openpyxl.utils import get_column_letter
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text
from controltower.db.connection import get_engine
from controltower.config import load_config
from controltower.actions.slack import post_new_findings_to_slack, post_findings_to_slack_by_ids, post_slack_message, post_dm_by_email
from controltower.asana.client import AsanaReadOnlyClient
import unicodedata
import re

st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
engine = get_engine()
cfg = load_config("config/config.yaml") if os.path.exists("config/config.yaml") else load_config("config/config.example.yaml")
CHILE_TZ = ZoneInfo("America/Santiago")

st.markdown("""
<style>
section[data-testid="stSidebar"] > div:first-child {
    display: flex;
    flex-direction: column;
    height: 100%;
}
section[data-testid="stSidebar"] > div:first-child > div {
    flex: 1 1 auto;
}
.sidebar-footer {
    margin-top: auto;
    padding-top: 0.5rem;
    font-size: 0.85rem;
    color: #6c757d;
}
</style>
""", unsafe_allow_html=True)

st.title("PMO Control Tower - MVP")

def _jsonable(obj):
    if hasattr(obj, "items"):
        obj = dict(obj)
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return {"value": obj}
    return json.loads(json.dumps(obj, default=str))

def _extract_custom_fields(project_raw):
    try:
        fields = (project_raw or {}).get("custom_fields") or []
    except Exception:
        return []
    out = []
    for f in fields:
        name = f.get("name") or "(sin nombre)"
        if f.get("display_value") is not None:
            val = f.get("display_value")
        else:
            val = f.get("text_value") or f.get("number_value") or f.get("enum_value") or f.get("multi_enum_values") or f.get("date_value")
        out.append({"name": name, "value": val})
    return out

def _custom_field_map(project_raw):
    fields = _extract_custom_fields(project_raw)
    return {f["name"]: f["value"] for f in fields}

def _humanize_last_update(ts):
    if not ts:
        return ""
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return str(ts)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - ts.astimezone(timezone.utc)
        days = delta.days
        if days <= 0:
            hours = int(delta.total_seconds() // 3600)
            if hours <= 0:
                return "hoy"
            return f"hace {hours}h"
        if days == 1:
            return "ayer"
        return f"hace {days}d"
    return str(ts)

def _days_since_last_update(ts, today_date=None):
    if not ts:
        return ""
    if today_date is None:
        today_date = date.today()
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return ""
    else:
        return ""
    try:
        return (today_date - dt.date()).days
    except Exception:
        return ""

def _fmt_date(val):
    if not val:
        return ""
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            return val.split("T")[0] if "T" in val else val
    return str(val)

def _cf_value_from_project_row(p, field_name):
    project_raw = (p.get("raw_data") or {}).get("project") if hasattr(p, "get") else None
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    return cf.get(field_name, "")

def _fmt_status(val):
    if not val:
        return "-"
    mapping = {
        "on_track": "On track",
        "on_hold": "On hold",
        "off_track": "Off track",
        "at_risk": "At risk",
        "green": "On track",
        "yellow": "At risk",
        "red": "Off track",
        "blue": "On hold",
    }
    return mapping.get(str(val), str(val).replace("_", " ").title())

def _fmt_task_progress(completed, total):
    try:
        total_i = int(total) if total is not None else 0
        completed_i = int(completed) if completed is not None else 0
    except Exception:
        return ""
    if total_i <= 0:
        return "0 de 0 tareas (0%)"
    pct = int(round((completed_i / total_i) * 100))
    return f"{completed_i} de {total_i} tareas ({pct}%)"

def _parse_budget(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val)
    # normalize: remove thousands separators, keep decimal
    s = s.replace(" ", "")
    # detect decimal comma
    if "," in s and "." not in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _get_query_params():
    try:
        return dict(st.query_params)
    except Exception:
        return st.experimental_get_query_params()

def _set_query_params(**kwargs):
    try:
        st.query_params.clear()
        st.query_params.update(kwargs)
    except Exception:
        st.experimental_set_query_params(**kwargs)

def _extract_next_steps(raw: dict) -> str:
    if not raw:
        return ""
    for key in ("next_steps", "next_steps_text", "next_steps_html", "next_steps_summary"):
        if raw.get(key):
            return str(raw.get(key))
    return ""

def _normalize_email_from_name(name: str) -> str:
    if not name:
        return ""
    # remove accents
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    parts = [p for p in n.replace("-", " ").split() if p.strip()]
    if len(parts) == 0:
        return ""
    first = parts[0].lower()
    last = parts[-1].lower()
    return f"{first[:1]}{last}@morrisopazo.com"

def _truncate_text(s: str, n: int = 10) -> str:
    if not s:
        return ""
    s = str(s)
    return s[:n] + "..." if len(s) > n else s

def _ensure_kpi_tables() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kpi_snapshots (
                id SERIAL PRIMARY KEY,
                kpi_id VARCHAR(50) NOT NULL,
                scope_type VARCHAR(20) NOT NULL,
                scope_value VARCHAR(200) NOT NULL,
                as_of TIMESTAMP NOT NULL,
                total_projects INTEGER NOT NULL,
                compliant_projects INTEGER NOT NULL,
                kpi_value DECIMAL(5,2) NOT NULL
            )
        """))
        conn.execute(text("""
            ALTER TABLE projects
            ADD COLUMN IF NOT EXISTS tasks_modified_last_7d INTEGER
        """))

def _cf_value_from_project_raw(project_raw: dict, field_name: str) -> str:
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    return cf.get(field_name, "")

def _cf_first_value(project_raw: dict, field_names: list[str]) -> str:
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    for n in field_names:
        v = cf.get(n, "")
        if v:
            return v
    return ""

def _get_last_sync_label() -> str:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT completed_at, started_at
            FROM sync_log
            WHERE status = 'completed'
            ORDER BY completed_at DESC NULLS LAST
            LIMIT 1
        """)).mappings().first()
        if not row:
            row = conn.execute(text("""
                SELECT completed_at, started_at
                FROM sync_log
                ORDER BY started_at DESC NULLS LAST
                LIMIT 1
            """)).mappings().first()
    ts = row.get("completed_at") if row else None
    if ts is None and row:
        ts = row.get("started_at")
    if not ts:
        return "Última actualización: -"
    try:
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_local = ts.astimezone(CHILE_TZ) if isinstance(ts, datetime) else ts
        label = ts_local.strftime("%Y/%m/%d %H:%M")
        return f"Última actualización (Chile): {label}"
    except Exception:
        return f"Última actualización: {ts}"

# Sidebar menu only
with st.sidebar:
    st.header("Menu")
    page = st.radio("Ir a", ["Dashboard", "Findings", "Mensajes", "Seguimiento", "KPI", "Búsqueda", "Plan de facturación"], label_visibility="collapsed")
    st.markdown(f"<div class='sidebar-footer'>{_get_last_sync_label()}</div>", unsafe_allow_html=True)

if page == "Dashboard":
    st.subheader("Dashboard")
    with engine.begin() as conn:
        sponsor_options = conn.execute(text("""
            SELECT DISTINCT TRIM(COALESCE(cf_s->>'display_value','')) AS sponsor
            FROM projects p
            JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              ON cf_s->>'name' = 'Sponsor'
            WHERE TRIM(COALESCE(cf_s->>'display_value','')) <> ''
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            ORDER BY sponsor
        """)).mappings().all()

        bv_options = conn.execute(text("""
            SELECT DISTINCT TRIM(COALESCE(cf_bv->>'display_value', cf_bv->'enum_value'->>'name','')) AS bv
            FROM projects p
            JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              ON (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
            WHERE TRIM(COALESCE(cf_bv->>'display_value', cf_bv->'enum_value'->>'name','')) <> ''
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
                WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv2->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv2->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv2->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            ORDER BY bv
        """)).mappings().all()

    sponsor_values = ["(todos)"] + [r["sponsor"] for r in sponsor_options if r.get("sponsor")]
    bv_values = ["(todos)"] + [r["bv"] for r in bv_options if r.get("bv")]

    fcols = st.columns(2)
    sponsor_query = fcols[0].selectbox("Sponsor", sponsor_values, index=0)
    bv_query = fcols[1].selectbox("Business Vertical", bv_values, index=0)
    with engine.begin() as conn:
        counts = conn.execute(text("""
            SELECT
              SUM(CASE WHEN f.status IN ('open','acknowledged') THEN 1 ELSE 0 END) AS open_findings,
              SUM(CASE WHEN f.severity='high' AND f.status IN ('open','acknowledged') THEN 1 ELSE 0 END) AS high_open
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
            )
            AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            AND (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
        """), {
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
            "bv": "" if bv_query == "(todos)" else bv_query.strip(),
            "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
        }).mappings().one()
        by_rule = conn.execute(text("""
            SELECT f.rule_id, COUNT(*) AS n
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE f.status IN ('open','acknowledged') AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
            )
            AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            AND (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
            GROUP BY f.rule_id
            ORDER BY n DESC
        """), {
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
            "bv": "" if bv_query == "(todos)" else bv_query.strip(),
            "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
        }).mappings().all()

        by_project_status = conn.execute(text("""
            SELECT COALESCE(p.status,'(sin status)') AS project_status, COUNT(*) AS n
            FROM projects p
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
            )
            AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            AND (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
            GROUP BY project_status
            ORDER BY n DESC
        """), {
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
            "bv": "" if bv_query == "(todos)" else bv_query.strip(),
            "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
        }).mappings().all()
    with engine.begin() as conn:
        total_projects = conn.execute(text("""
            SELECT COUNT(*) AS n
            FROM projects p
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
            )
            AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            AND (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
        """), {
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
            "bv": "" if bv_query == "(todos)" else bv_query.strip(),
            "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
        }).mappings().one()

    c1, c2 = st.columns(2)
    c1.metric("Hallazgos abiertos", counts["open_findings"] or 0)
    c2.metric("Proyectos", total_projects["n"] or 0)
    st.markdown("**Desglose por regla (open)**")
    table_col, venn_col = st.columns([1, 1])
    with table_col:
        if by_rule:
            df_rules = pd.DataFrame(by_rule).rename(columns={"rule_id": "Regla", "n": "Cantidad de problemas"})
            if "Regla" in df_rules.columns:
                df_rules = df_rules[df_rules["Regla"].fillna("").astype(str).str.strip() != ""]
            if "Regla" in df_rules.columns and "Cantidad de problemas" in df_rules.columns:
                df_rules = df_rules[["Regla", "Cantidad de problemas"]]
            row_count = len(df_rules)
            if row_count == 0:
                st.info("No hay datos para mostrar.")
            else:
                height = min(400, max(80, 35 * row_count + 35))
                st.dataframe(df_rules, use_container_width=True, height=height, hide_index=True)
        else:
            st.info("No hay datos para mostrar.")

    with venn_col:
        st.markdown("**Venn: proyectos con reglas críticas (open)**")
        rule_a = "no_status_update"
        rule_b = "no_tasks_activity_last_7_days"
        rule_c = "amount_of_tasks"
        rule_labels = {
            rule_a: "Sin status update",
            rule_b: "Sin actividad tareas 7d",
            rule_c: "Pocas tareas (<=3)",
        }
        with engine.begin() as conn:
            venn_rows = conn.execute(text("""
                SELECT f.project_gid, f.rule_id
                FROM findings f
                JOIN projects p ON p.gid = f.project_gid
                WHERE f.status IN ('open','acknowledged')
                  AND f.rule_id IN (:r1, :r2, :r3)
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                    WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
                  )
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                    WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                      AND (
                        (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                        OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                        OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                      )
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                    WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                      AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                  )
                  AND (:sponsor = '' OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                    WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                  ))
            """), {
                "r1": rule_a,
                "r2": rule_b,
                "r3": rule_c,
                "sponsor": sponsor_query.strip(),
                "sponsor_like": f"%{sponsor_query.strip()}%",
            }).mappings().all()

        sets = {rule_a: set(), rule_b: set(), rule_c: set()}
        for r in venn_rows:
            rid = r.get("rule_id")
            if rid in sets:
                sets[rid].add(r.get("project_gid"))

        A, B, C = sets[rule_a], sets[rule_b], sets[rule_c]
        only_a = A - B - C
        only_b = B - A - C
        only_c = C - A - B
        ab = (A & B) - C
        ac = (A & C) - B
        bc = (B & C) - A
        abc = A & B & C

        total = len(A | B | C)
        if total == 0:
            st.info("No hay proyectos que transgredan estas reglas.")
        else:
            import plotly.graph_objects as go
            fig = go.Figure()
            # Circle shapes
            fig.add_shape(type="circle", xref="x", yref="y",
                          x0=0.1, y0=0.2, x1=0.7, y1=0.8,
                          line_color="rgba(0,123,255,0.7)", fillcolor="rgba(0,123,255,0.15)")
            fig.add_shape(type="circle", xref="x", yref="y",
                          x0=0.4, y0=0.2, x1=1.0, y1=0.8,
                          line_color="rgba(40,167,69,0.7)", fillcolor="rgba(40,167,69,0.15)")
            fig.add_shape(type="circle", xref="x", yref="y",
                          x0=0.25, y0=0.0, x1=0.85, y1=0.6,
                          line_color="rgba(255,193,7,0.8)", fillcolor="rgba(255,193,7,0.15)")

            # Labels
            fig.add_annotation(x=0.2, y=0.85, text=rule_labels[rule_a], showarrow=False)
            fig.add_annotation(x=0.85, y=0.85, text=rule_labels[rule_b], showarrow=False)
            fig.add_annotation(x=0.55, y=-0.05, text=rule_labels[rule_c], showarrow=False)

            # Counts
            fig.add_annotation(x=0.27, y=0.63, text=str(len(only_a)), showarrow=False)
            fig.add_annotation(x=0.83, y=0.63, text=str(len(only_b)), showarrow=False)
            fig.add_annotation(x=0.55, y=0.12, text=str(len(only_c)), showarrow=False)
            fig.add_annotation(x=0.55, y=0.63, text=str(len(ab)), showarrow=False)
            fig.add_annotation(x=0.40, y=0.36, text=str(len(ac)), showarrow=False)
            fig.add_annotation(x=0.70, y=0.36, text=str(len(bc)), showarrow=False)
            fig.add_annotation(x=0.55, y=0.42, text=str(len(abc)), showarrow=False)

            fig.update_xaxes(visible=False)
            fig.update_yaxes(visible=False)
            fig.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)

    with engine.begin() as conn:
        by_responsable = conn.execute(text("""
            SELECT COALESCE(cf->>'display_value','(sin responsable)') AS responsable, COUNT(*) AS n
            FROM projects p
            LEFT JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              ON cf->>'name' = 'Responsable Proyecto'
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf2
              WHERE cf2->>'name' = 'PMO ID' AND COALESCE(cf2->>'display_value','') <> ''
            )
            AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
            )
            AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            GROUP BY responsable
            ORDER BY n DESC
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
        }).mappings().all()

    pie_cols = st.columns(2)
    with pie_cols[0]:
        st.markdown("**Proyectos por estado**")
        if by_project_status:
            df_status = pd.DataFrame(by_project_status).rename(columns={"n": "Cantidad"})
            df_status["project_status_label"] = df_status["project_status"].apply(_fmt_status)
            status_colors = {
                "On track": "#2e7d32",
                "At risk": "#f9a825",
                "Off track": "#c62828",
                "On hold": "#1565c0",
                "-": "#9e9e9e",
                "(Sin Status)": "#9e9e9e",
            }
            fig_status = px.pie(
                df_status,
                values="Cantidad",
                names="project_status_label",
                color="project_status_label",
                color_discrete_map=status_colors,
            )
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.info("No hay datos para mostrar.")
    with pie_cols[1]:
        st.markdown("**Proyectos por responsable**")
        if by_responsable:
            df_resp = pd.DataFrame(by_responsable).rename(columns={"n": "Cantidad"})
            fig_resp = px.pie(df_resp, values="Cantidad", names="responsable")
            st.plotly_chart(fig_resp, use_container_width=True)
        else:
            st.info("No hay datos para mostrar.")

    st.markdown("**Proyectos por semana de cierre (futuro)**")
    with engine.begin() as conn:
        closing_dates = conn.execute(text("""
            SELECT
              COALESCE(
                (cf_end->'date_value'->>'date')::date,
                (cf_end->>'display_value')::date
              ) AS planned_end_date,
              p.name,
              p.raw_data
            FROM projects p
            LEFT JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_end
              ON cf_end->>'name' = 'Fecha Planificada Termino del proyecto'
            WHERE COALESCE(
                     (cf_end->'date_value'->>'date')::date,
                     (cf_end->>'display_value')::date
                  ) IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
              ))
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
        }).mappings().all()

    if closing_dates:
        today = date.today()
        buckets = {}
        for r in closing_dates:
            d = r.get("planned_end_date")
            if not d:
                continue
            if d < today:
                key = "Semana pasada"
            else:
                # ISO week label
                iso_year, iso_week, _ = d.isocalendar()
                key = f"{iso_year}-W{iso_week:02d}"
            buckets[key] = buckets.get(key, 0) + 1
        # Order: Semana pasada first, then chronological weeks
        keys = sorted([k for k in buckets.keys() if k != "Semana pasada"])
        ordered = ["Semana pasada"] + keys if "Semana pasada" in buckets else keys
        df_close_week = pd.DataFrame([{"Semana": k, "Cantidad": buckets[k]} for k in ordered])
        fig_close = px.bar(df_close_week, x="Semana", y="Cantidad")
        st.plotly_chart(fig_close, use_container_width=True)
    else:
        st.info("No hay fechas de cierre para mostrar.")

    st.markdown("**Flujo de caja (Total presupuestado)**")
    if closing_dates:
        period = st.radio("Ver por", ["Semana", "Mes"], horizontal=True, label_visibility="collapsed")
        rows_budget = []
        today = date.today()
        month_names = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
        }
        for r in closing_dates:
            d = r.get("planned_end_date")
            if not d:
                continue
            if period == "Semana":
                if d < today:
                    bucket_key = "Semana pasada"
                else:
                    iso_year, iso_week, _ = d.isocalendar()
                    bucket_key = f"{iso_year}-W{iso_week:02d}"
                bucket_label = "Semana"
            else:
                bucket_key = f"{d.year}-{d.month:02d}"
                bucket_label = "Mes"
                bucket_display = f"{month_names.get(d.month, d.month)} {d.year}"
            # budget from custom field
            raw = (r.get("raw_data") or {}).get("project") or {}
            budget_val = _cf_value_from_project_row({"raw_data": {"project": raw}}, "Total presupuestado")
            amount = _parse_budget(budget_val)
            if amount is None:
                continue
            if period == "Mes":
                rows_budget.append({
                    "Mes": bucket_display,
                    "MesOrden": bucket_key,
                    "Proyecto": r.get("name") or "",
                    "Monto": amount,
                })
            else:
                rows_budget.append({
                    bucket_label: bucket_key,
                    "Proyecto": r.get("name") or "",
                    "Monto": amount,
                })
        if rows_budget:
            df_budget = pd.DataFrame(rows_budget)
            if period == "Mes":
                order = sorted(df_budget["MesOrden"].unique().tolist())
                order_labels = []
                for key in order:
                    y, m = key.split("-")
                    order_labels.append(f"{month_names.get(int(m), m)} {y}")
                fig_budget = px.bar(
                    df_budget,
                    x="Mes",
                    y="Monto",
                    color="Proyecto",
                    category_orders={"Mes": order_labels},
                )
            else:
                fig_budget = px.bar(df_budget, x=bucket_label, y="Monto", color="Proyecto")
            st.plotly_chart(fig_budget, use_container_width=True)
        else:
            st.info("No hay montos en 'Total presupuestado' para mostrar.")

elif page == "Proyectos":
    st.subheader("Proyectos")
    fcols = st.columns(7)
    project_query = fcols[0].text_input("Proyecto contiene")
    pmo_id_query = fcols[1].text_input("PMO-ID contiene")
    opp_query = fcols[2].text_input("AWS OPP ID contiene")
    resp_query = fcols[3].text_input("Responsable contiene")
    client_query = fcols[4].text_input("Cliente contiene")
    sponsor_query = fcols[5].text_input("Sponsor contiene", value="Abrigo")
    status_filter = fcols[6].selectbox("Estado", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])

    fcols2 = st.columns(2)
    sort_stale = fcols2[0].checkbox("Ordenar por ultimo update (mas antiguo primero)", value=False)
    show_raw = fcols2[1].checkbox("Mostrar raw del proyecto", value=False)

    where = ["1=1"]
    params = {}
    where.append("""
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
          WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
        )
    """)
    where.append("""
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
          WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
            AND (
              (cf_bv->'enum_value'->>'gid') = '1209701308000273'
              OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
              OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
            )
        )
    """)
    where.append("""
        NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
          WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
            AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
        )
    """)
    if project_query.strip():
        where.append("name ILIKE :pname")
        params["pname"] = f"%{project_query.strip()}%"
    if pmo_id_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') ILIKE :pmo
            )
        """)
        params["pmo"] = f"%{pmo_id_query.strip()}%"
    if resp_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'Responsable Proyecto' AND COALESCE(cf->>'display_value','') ILIKE :resp
            )
        """)
        params["resp"] = f"%{resp_query.strip()}%"
    if opp_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'AWS OPP ID' AND COALESCE(cf->>'display_value','') ILIKE :opp
            )
        """)
        params["opp"] = f"%{opp_query.strip()}%"
    if client_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'cliente_nuevo' AND COALESCE(cf->>'display_value','') ILIKE :client
            )
        """)
        params["client"] = f"%{client_query.strip()}%"
    if sponsor_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'Sponsor' AND COALESCE(cf->>'display_value','') ILIKE :sponsor
            )
        """)
        params["sponsor"] = f"%{sponsor_query.strip()}%"
    if status_filter != "(todos)":
        if status_filter == "none":
            where.append("status IS NULL")
        else:
            status_color_map = {
                "on_track": "green",
                "at_risk": "yellow",
                "off_track": "red",
                "on_hold": "blue",
            }
            where.append("(status = :status OR status = :status_color)")
            params["status"] = status_filter
            params["status_color"] = status_color_map.get(status_filter, status_filter)

    order_by = "last_status_update_at ASC NULLS LAST" if sort_stale else "name ASC"
    with engine.begin() as conn:
        projects = conn.execute(text(f"""
            SELECT gid, name, owner_name, due_date, calculated_progress,
                   total_tasks, completed_tasks, last_status_update_at, last_activity_at, status, raw_data
            FROM projects
            WHERE {' AND '.join(where)}
            ORDER BY {order_by}
        """), params).mappings().all()
    st.caption(f"Total proyectos: {len(projects)}")

    if "show_project_gid" not in st.session_state:
        st.session_state["show_project_gid"] = None

    @st.dialog("Updates de proyecto")
    def _show_updates_dialog(project_gid: str):
        if not project_gid:
            st.info("Proyecto no encontrado.")
            return
        proj = next((x for x in projects if x.get("gid") == project_gid), None)
        if proj:
            st.markdown(f"**Proyecto:** {proj.get('name')}")

        # Pagination state
        if st.session_state.get("updates_project_gid") != project_gid:
            st.session_state["updates_project_gid"] = project_gid
            st.session_state["updates_offset"] = 0
        offset = int(st.session_state.get("updates_offset", 0))

        with engine.begin() as conn:
            su = conn.execute(text("""
                SELECT gid, created_at, author_name, status_type, title, text, html_text, raw_data
                FROM status_updates
                WHERE project_gid = :gid
                ORDER BY created_at DESC
                LIMIT 1 OFFSET :offset
            """), {"gid": project_gid, "offset": offset}).mappings().first()

        if not su:
            st.info("No hay updates para este proyecto.")
        else:
            st.markdown(f"**Fecha:** {su.get('created_at')}")
            st.markdown(f"**Estado:** {su.get('status_type')}")
            st.markdown(f"**Autor:** {su.get('author_name')}")
            st.markdown("**Texto del update:**")
            st.write(su.get("text") or "")

            raw = su.get("raw_data") or {}
            next_steps = _extract_next_steps(raw)
            if next_steps:
                st.markdown("**Next steps:**")
                st.write(next_steps)

            with engine.begin() as conn:
                comments = conn.execute(text("""
                    SELECT created_at, author_name, text
                    FROM status_update_comments
                    WHERE status_update_gid = :gid
                    ORDER BY created_at ASC
                """), {"gid": su.get("gid")}).mappings().all()
            st.markdown("**Comentarios:**")
            if comments:
                for c in comments:
                    st.markdown(f"- [{c.get('created_at')}] {c.get('author_name')}: {c.get('text')}")
            else:
                st.write("Sin comentarios.")

        cols = st.columns(2)
        if cols[0].button("Ver más"):
            st.session_state["updates_offset"] = offset + 1
            st.rerun()
        if cols[1].button("Cerrar"):
            st.session_state["updates_offset"] = 0
            st.rerun()

    @st.dialog("Detalle de proyecto")
    def _show_project_dialog(p):
        project_raw = (p.get("raw_data") or {}).get("project") if hasattr(p, "get") else None
        st.json(_jsonable(p))
        if project_raw:
            if st.button("Ver campos personalizados"):
                custom_fields = _extract_custom_fields(project_raw)
                if custom_fields:
                    st.markdown("**Campos personalizados**")
                    st.json(_jsonable(custom_fields))
            if show_raw:
                st.markdown("**Proyecto (raw)**")
                st.json(_jsonable(project_raw))
        if st.button("Cerrar"):
            st.session_state["show_project_gid"] = None
            st.rerun()

    # Preload update counts for visible projects
    project_gids = [p.get("gid") for p in projects if p.get("gid")]
    updates_count = {}
    if project_gids:
        with engine.begin() as conn:
            rows_updates = conn.execute(text("""
                SELECT project_gid, COUNT(*) AS n
                FROM status_updates
                WHERE project_gid = ANY(:gids)
                GROUP BY project_gid
            """), {"gids": project_gids}).mappings().all()
        updates_count = {r["project_gid"]: int(r["n"]) for r in rows_updates}

    pdf = pd.DataFrame([{
        "select": False,
        "pmo_id": _cf_value_from_project_row(p, "PMO ID"),
        "proyecto": p.get("name") or "",
        "responsable": _cf_value_from_project_row(p, "Responsable Proyecto") or p.get("owner_name") or "",
        "sponsor": _cf_value_from_project_row(p, "Sponsor"),
        "cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
        "inicio": _fmt_date(_cf_value_from_project_row(p, "Fecha Inicio del proyecto") or _cf_value_from_project_row(p, "Fecha Inicio")),
        "termino_plan": _fmt_date(_cf_value_from_project_row(p, "Fecha Planificada Termino del proyecto") or _cf_value_from_project_row(p, "Fecha Planificada Termino del proyecto")),
        "status_ultimo": f"{p.get('status') or ''} {_humanize_last_update(p.get('last_status_update_at'))}".strip(),
        "updates": "Ver updates" if updates_count.get(p.get("gid"), 0) > 0 else "",
        "updates_n": updates_count.get(p.get("gid"), 0),
        "gid": p.get("gid"),
    } for p in projects])
    if not pdf.empty:
        pdf["pmo_num"] = pd.to_numeric(pdf["pmo_id"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
        pdf = pdf.sort_values(by=["pmo_num", "pmo_id", "proyecto"], na_position="last").reset_index(drop=True)

    edited_p = st.data_editor(
        pdf.drop(columns=["gid", "pmo_num"], errors="ignore"),
        use_container_width=True,
        height=420,
        column_config={
            "select": st.column_config.CheckboxColumn(""),
            "updates": st.column_config.TextColumn("Updates"),
        },
        disabled=["pmo_id", "proyecto", "responsable", "sponsor", "cliente", "inicio", "termino_plan", "status_ultimo", "updates", "updates_n"],
        hide_index=True,
    )

    selected_gids = []
    if not edited_p.empty:
        selected_idx = edited_p[edited_p["select"] == True].index.tolist()
        selected_gids = [pdf.loc[i, "gid"] for i in selected_idx]

    p_actions = st.columns(3)
    if p_actions[0].button("Descargar detalle"):
        if not selected_gids:
            st.warning("Selecciona al menos un proyecto.")
        else:
            selected_projects = [p for p in projects if p.get("gid") in selected_gids]
            if len(selected_projects) == 1:
                p = selected_projects[0]
                pmo = _cf_value_from_project_row(p, "PMO ID") or p.get("gid")
                filename = f"{pmo}.json"
                data = json.dumps(_jsonable((p.get("raw_data") or {}).get("project") or p), ensure_ascii=False, indent=2)
                st.download_button(
                    "Descargar JSON",
                    data=data,
                    file_name=filename,
                    mime="application/json",
                )
            else:
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    for p in selected_projects:
                        pmo = _cf_value_from_project_row(p, "PMO ID") or p.get("gid")
                        filename = f"{pmo}.json"
                        content = json.dumps(_jsonable((p.get("raw_data") or {}).get("project") or p), ensure_ascii=False, indent=2)
                        zf.writestr(filename, content)
                st.download_button(
                    "Descargar ZIP",
                    data=buf.getvalue(),
                    file_name="proyectos_detalle.zip",
                    mime="application/zip",
                )
    if p_actions[1].button("Ver updates"):
        if not selected_gids:
            st.warning("Selecciona al menos un proyecto.")
        else:
            sel_gid = selected_gids[0]
            if updates_count.get(sel_gid, 0) <= 0:
                st.info("Este proyecto no tiene updates.")
            else:
                _show_updates_dialog(sel_gid)
    p_actions[2].button("Limpiar selección", on_click=lambda: None)

elif page == "Findings":
    st.subheader("Findings")
    if st.button("Enviar findings a Slack"):
        try:
            sent = post_new_findings_to_slack(cfg)
            st.success(f"Mensajes enviados: {sent}")
        except Exception as e:
            st.error(f"Error enviando a Slack: {e}")

    # Build rule list dynamically from config
    rule_ids = ["(todas)"]
    try:
        rule_ids += list((cfg.get("rules") or {}).keys())
    except Exception:
        rule_ids += ["no_status_update", "no_tasks_activity_last_7_days", "schedule_risk"]

    fcols = st.columns(6)
    rule_filter = fcols[0].selectbox("Regla", rule_ids)
    severity_filter = fcols[1].selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    status_filter = fcols[2].selectbox("Estado finding", ["open", "acknowledged", "open+ack", "resolved", "(todos)"], index=2)
    project_status_filter = fcols[3].selectbox("Estado proyecto", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])
    sponsor_query = fcols[4].text_input("Sponsor contiene", value="Abrigo")
    resp_query = fcols[5].text_input("Responsable contiene")

    fcols2 = st.columns(2)
    project_query = fcols2[0].text_input("Proyecto contiene")

    where = ["1=1"]
    params = {}

    if rule_filter != "(todas)":
        where.append("rule_id = :rule_id")
        params["rule_id"] = rule_filter
    if severity_filter != "(todas)":
        where.append("severity = :severity")
        params["severity"] = severity_filter
    if status_filter != "(todos)":
        if status_filter == "open+ack":
            where.append("f.status IN ('open','acknowledged')")
        else:
            where.append("f.status = :fstatus")
            params["fstatus"] = status_filter
    if project_query.strip():
        where.append("(details->>'project_name') ILIKE :pname")
        params["pname"] = f"%{project_query.strip()}%"
    join_projects = "JOIN projects p ON p.gid = f.project_gid"
    where.append("""
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
          WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
        )
    """)
    where.append("""
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
          WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
            AND (
              (cf_bv->'enum_value'->>'gid') = '1209701308000273'
              OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
              OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
            )
        )
    """)
    where.append("""
        NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
          WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
            AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
        )
    """)
    if sponsor_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'Sponsor' AND COALESCE(cf->>'display_value','') ILIKE :sponsor
            )
        """)
        params["sponsor"] = f"%{sponsor_query.strip()}%"
    if project_status_filter != "(todos)":
        if project_status_filter == "none":
            where.append("p.status IS NULL")
        else:
            status_color_map = {
                "on_track": "green",
                "at_risk": "yellow",
                "off_track": "red",
                "on_hold": "blue",
            }
            where.append("(p.status = :pstatus OR p.status = :pstatus_color)")
            params["pstatus"] = project_status_filter
            params["pstatus_color"] = status_color_map.get(project_status_filter, project_status_filter)
    if resp_query.strip():
        where.append("""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'Responsable Proyecto' AND COALESCE(cf->>'display_value','') ILIKE :resp
            )
        """)
        params["resp"] = f"%{resp_query.strip()}%"

    q = f"""
        SELECT f.id, f.project_gid, f.rule_id, f.severity, f.status, f.created_at, f.details
        FROM findings f
        {join_projects}
        WHERE {' AND '.join(where)}
        ORDER BY f.created_at DESC
    """

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).mappings().all()

    # Build project lookup for richer grid fields
    project_gids = list({r.get("project_gid") for r in rows if r.get("project_gid")})
    projects_map = {}
    if project_gids:
        with engine.begin() as conn:
            proj_rows = conn.execute(text("""
                SELECT gid, name, status, last_status_update_at, raw_data
                FROM projects
                WHERE gid = ANY(:gids)
            """), {"gids": project_gids}).mappings().all()
        projects_map = {p["gid"]: p for p in proj_rows}

    rule_cols = [r for r in (cfg.get("rules") or {}).keys() if r != "schedule_risk"]
    if not rule_cols:
        rule_cols = ["no_status_update", "no_tasks_activity_last_7_days", "amount_of_tasks"]

    def _rule_message_short(rule: str, details: dict) -> str:
        if rule == "no_status_update":
            days = details.get("days_since_last_status_update", "N/A")
            return f"{days} días sin update de proyecto."
        if rule in ("no_tasks_activity_last_7_days", "no_activity"):
            days = (cfg.get("rules") or {}).get("no_tasks_activity_last_7_days", {}).get("days_threshold", 7)
            return f"{days} días sin actualizar tasks."
        if rule == "amount_of_tasks":
            return "Sin tareas creadas."
        return ""

    # Aggregate by project
    by_project = {}
    for r in rows:
        gid = r.get("project_gid")
        if not gid:
            continue
        p = projects_map.get(gid) or {}
        if gid not in by_project:
            row = {
                "select": False,
                "project_gid": gid,
                "finding_ids": [],
                "pmo_id": _cf_value_from_project_row(p, "PMO ID"),
                "proyecto": f"{(p.get('name') or '')} ({_fmt_status(p.get('status'))})".strip(),
                "cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
                "responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
                "sponsor": _cf_value_from_project_row(p, "Sponsor"),
                "motivo": [],
            }
            for rc in rule_cols:
                row[rc] = ""
            by_project[gid] = row

        rule_id = r.get("rule_id")
        if rule_id in by_project[gid]:
            by_project[gid][rule_id] = "X"
            msg = _rule_message_short(rule_id, r.get("details") or {})
            if msg:
                by_project[gid]["motivo"].append(msg)
        by_project[gid]["finding_ids"].append(r.get("id"))

    fdf = pd.DataFrame([
        {
            **{k: v for k, v in row.items() if k not in ("motivo", "finding_ids")},
            "motivo": " | ".join(row.get("motivo") or []),
        }
        for row in by_project.values()
    ])

    display_df = fdf.drop(columns=["project_gid"], errors="ignore")
    edited = st.data_editor(
        display_df,
        use_container_width=True,
        height=400,
        column_config={"select": st.column_config.CheckboxColumn("")},
        disabled=["pmo_id", "proyecto", "cliente", "responsable", "sponsor", "motivo"] + rule_cols,
        hide_index=True,
    )
    st.caption(f"Total registros: {len(fdf)}")

    selected_project_gids = []
    if not edited.empty and "select" in edited.columns:
        selected_idx = edited[edited["select"] == True].index.tolist()
        if selected_idx:
            selected_project_gids = fdf.loc[selected_idx, "project_gid"].tolist()
    selected_ids = []
    if selected_project_gids:
        for gid in selected_project_gids:
            selected_ids.extend(by_project.get(gid, {}).get("finding_ids", []))

    action_cols = st.columns(3)
    if action_cols[0].button("Ver detalle"):
        if not selected_ids:
            st.warning("Selecciona al menos un finding.")
        else:
            rmap = {r.get("id"): r for r in rows}
            details = [rmap[i].get("details") for i in selected_ids if i in rmap]
            st.json(_jsonable(details))

    if action_cols[1].button("Enviar seleccionados a Slack"):
        if not selected_ids:
            st.warning("Selecciona al menos un finding.")
        else:
            try:
                sent = post_findings_to_slack_by_ids(cfg, selected_ids)
                st.success(f"Mensajes enviados: {sent}")
            except Exception as e:
                st.error(f"Error enviando a Slack: {e}")

    if action_cols[2].button("Exportar seleccionados (CSV)"):
        if not selected_ids:
            st.warning("Selecciona al menos un finding.")
        else:
            export_rows = []
            for r in rows:
                if r.get("id") not in selected_ids:
                    continue
                p = projects_map.get(r.get("project_gid")) or {}
                export_rows.append({
                    "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
                    "Proyecto": p.get("name") or "",
                    "Cliente": _cf_value_from_project_row(p, "cliente_nuevo") or "",
                    "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto") or "",
                    "Regla": r.get("rule_id"),
                    "Motivo": _rule_message_short(r.get("rule_id"), r.get("details") or {}),
                })

            csv = pd.DataFrame(export_rows).to_csv(index=False)
            st.download_button(
                "Descargar CSV",
                data=csv,
                file_name="findings.csv",
                mime="text/csv",
            )

    # Exportar a Excel (por proyecto, marcando reglas)
    project_gids = list({r.get("project_gid") for r in rows if r.get("project_gid")})
    projects_map = {}
    if project_gids:
        with engine.begin() as conn:
            proj_rows = conn.execute(text("""
                SELECT gid, name, raw_data, last_status_update_at, total_tasks, status, calculated_progress
                FROM projects
                WHERE gid = ANY(:gids)
            """), {"gids": project_gids}).mappings().all()
        projects_map = {p["gid"]: p for p in proj_rows}

    # Build rule columns from config
    rule_cols = [r for r in (cfg.get("rules") or {}).keys() if r != "schedule_risk"]
    if not rule_cols:
        rule_cols = ["no_status_update", "no_tasks_activity_last_7_days", "amount_of_tasks"]

    # Aggregate per project
    by_project = {}
    for r in rows:
        gid = r.get("project_gid")
        if not gid:
            continue
        p = projects_map.get(gid) or {}
        if gid not in by_project:
            last_update = p.get("last_status_update_at")
            days_since = ""
            if last_update:
                if isinstance(last_update, str):
                    try:
                        last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                    except Exception:
                        last_update = None
                if isinstance(last_update, datetime):
                    days_since = (datetime.now(timezone.utc) - last_update.astimezone(timezone.utc)).days
            total_tasks = p.get("total_tasks")
            progress = p.get("calculated_progress")
            progress_fmt = ""
            if progress is not None:
                try:
                    progress_fmt = f"{int(round(float(progress)))} %"
                except Exception:
                    progress_fmt = ""
            by_project[gid] = {
                "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
                "Nombre de proyecto": p.get("name") or "",
                "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
                "Responsable del proyecto": _cf_value_from_project_row(p, "Responsable Proyecto"),
                "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
                "Estado": _fmt_status(p.get("status")),
                "Dias desde ultimo update": days_since,
                "Cantidad de tareas": total_tasks if total_tasks is not None else "",
                "Avance": progress_fmt,
            }
            for rc in rule_cols:
                by_project[gid][rc] = ""
        rule_id = r.get("rule_id")
        if rule_id in by_project[gid]:
            by_project[gid][rule_id] = "X"

    export_rows = list(by_project.values())
    df_export = pd.DataFrame(export_rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Reorder columns to place Avance between tareas and rule columns
        base_cols = [
            "PMO-ID",
            "Nombre de proyecto",
            "Cliente",
            "Responsable del proyecto",
            "Sponsor",
            "Estado",
            "Dias desde ultimo update",
            "Cantidad de tareas",
            "Avance",
        ]
        rule_cols_present = [c for c in df_export.columns if c not in base_cols]
        df_export = df_export[base_cols + rule_cols_present]
        df_export.to_excel(writer, index=False, sheet_name="findings")
        ws = writer.sheets["findings"]
        widths = {
            "PMO-ID": 15,
            "Nombre de proyecto": 60,
            "Cliente": 25,
            "Responsable del proyecto": 25,
            "Sponsor": 25,
            "Avance": 10,
        }
        for idx, col in enumerate(df_export.columns, start=1):
            col_name = str(col)
            ws.column_dimensions[get_column_letter(idx)].width = widths.get(col_name, 10)
    st.download_button(
        "Exportar a Excel",
        data=buf.getvalue(),
        file_name="findings.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    ack_cols = st.columns(3)
    ack_ids = ack_cols[0].text_input("IDs para Acknowledge (coma)")
    ack = ack_cols[1].text_input("Comentario (obligatorio)")
    ack_by = ack_cols[2].text_input("Acknowledged by", value="PMO")
    if st.button("Acknowledge seleccionados"):
        ids = [int(x.strip()) for x in ack_ids.split(",") if x.strip().isdigit()]
        if not ids or not ack.strip():
            st.error("IDs y comentario son obligatorios.")
        else:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE findings
                    SET status='acknowledged',
                        acknowledged_at=NOW(),
                        acknowledged_by=:by,
                        ack_comment=:c
                    WHERE id = ANY(:ids)
                """), {"ids": ids, "c": ack, "by": ack_by or "PMO"})
            st.success("Hallazgos acknowledged.")
            st.rerun()

elif page == "Mensajes":
    st.subheader("Mensajes a Responsables")
    st.caption("Compila un solo mensaje por responsable, con proyectos y reglas transgredidas (sin schedule_risk).")

    sponsor_filter = st.text_input("Sponsor contiene", value="")

    # Load responsables list
    with engine.begin() as conn:
        responsables = conn.execute(text("""
            SELECT DISTINCT TRIM(COALESCE(cf->>'display_value','')) AS responsable
            FROM projects p
            JOIN findings f ON f.project_gid = p.gid
            JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              ON cf->>'name' = 'Responsable Proyecto'
            WHERE TRIM(COALESCE(cf->>'display_value','')) <> ''
              AND f.status IN ('open','acknowledged')
              AND f.rule_id <> 'schedule_risk'
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_pmo
                WHERE cf_pmo->>'name' = 'PMO ID' AND COALESCE(cf_pmo->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
              ))
            ORDER BY responsable
        """), {
            "sponsor": sponsor_filter.strip(),
            "sponsor_like": f"%{sponsor_filter.strip()}%",
        }).mappings().all()
    responsables_list = [r["responsable"] for r in responsables] if responsables else []
    resp = st.selectbox("Responsable de proyecto", ["(selecciona)"] + responsables_list)
    email_default = _normalize_email_from_name(resp if resp != "(selecciona)" else "")
    email_to = st.text_input("Email (DM)", value=email_default, help="Si está vacío, se envía al canal", key="dm_email")

    msg = ""
    msg_rows = []
    if resp and resp != "(selecciona)":
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT f.rule_id, f.details, p.gid, p.name, p.raw_data
                FROM findings f
                JOIN projects p ON p.gid = f.project_gid
                WHERE f.status IN ('open','acknowledged')
                  AND f.rule_id <> 'schedule_risk'
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_pmo
                    WHERE cf_pmo->>'name' = 'PMO ID' AND COALESCE(cf_pmo->>'display_value','') <> ''
                  )
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                    WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                      AND (
                        (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                        OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                        OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                      )
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                    WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                      AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                  )
                  AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_resp
                    WHERE cf_resp->>'name' = 'Responsable Proyecto'
                      AND COALESCE(cf_resp->>'display_value','') ILIKE :resp
                  )
                  AND (:sponsor = '' OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                    WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                  ))
                ORDER BY p.name ASC
            """), {
                "resp": f"%{resp}%",
                "sponsor": sponsor_filter.strip(),
                "sponsor_like": f"%{sponsor_filter.strip()}%",
            }).mappings().all()

        grouped = {}
        days_tasks = int((cfg.get("rules") or {}).get("no_tasks_activity_last_7_days", {}).get("days_threshold", 7))
        for r in rows:
            gid = r.get("gid")
            if gid not in grouped:
                p = {"gid": gid, "name": r.get("name"), "raw_data": r.get("raw_data")}
                grouped[gid] = {"project": p, "reasons": [], "has_no_tasks": False}
            rule = r.get("rule_id")
            details = r.get("details") or {}
            if rule == "no_status_update":
                days = details.get("days_since_last_status_update", "N/A")
                if isinstance(days, int) and days >= 999:
                    grouped[gid]["reasons"].append("Proyecto no tiene updates")
                else:
                    grouped[gid]["reasons"].append(f"{days} días sin update de proyecto")
            elif rule == "no_tasks_activity_last_7_days":
                grouped[gid]["reasons"].append(f"{days_tasks} días sin actualizar tasks")
            elif rule == "amount_of_tasks":
                grouped[gid]["has_no_tasks"] = True
                grouped[gid]["reasons"].append("Deben crearse las tareas del proyecto")

        lines = []
        seen = set()
        for g in grouped.values():
            p = g["project"]
            pmo = _cf_value_from_project_row(p, "PMO ID")
            name = p.get("name") or ""
            reasons_list = list(dict.fromkeys(g["reasons"]))
            if g.get("has_no_tasks"):
                reasons_list = [r for r in reasons_list if "actualizar tasks" not in r]
            reasons = "; ".join(reasons_list)
            key = (str(pmo).strip(), str(name).strip())
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"{pmo} {name}: {reasons}")

        if lines:
            # Render as a pipe-separated table for Slack (monospace in code block)
            def _trim(s, width):
                s = (s or "").strip()
                return (s[: width - 3] + "...") if len(s) > width else s

            rows = []
            for line in lines:
                try:
                    left, reasons = line.split(":", 1)
                    pmo, name = left.split(" ", 1)
                except Exception:
                    pmo, name, reasons = "", line, ""
                rows.append((pmo.strip(), name.strip(), reasons.strip()))
            msg_rows = rows

            col_pmo = 12
            col_name = 50
            col_motivo = 80
            header = f"{_trim('PMO-ID', col_pmo)} | {_trim('Nombre', col_name)} | {_trim('Motivo', col_motivo)}"
            body = "\n".join([
                f"{_trim(p, col_pmo)} | {_trim(n, col_name)} | {_trim(m, col_motivo)}"
                for p, n, m in rows
            ])
            msg = "Hola, los siguientes proyectos tienen situaciones que se deben corregir:\n" \
                  "```\n" + header + "\n" + body + "\n```"
        else:
            msg = "Hola, no hay proyectos con hallazgos abiertos para este responsable."

    msg = st.text_area("Mensaje", value=msg, height=250)
    use_blocks = st.checkbox("Enviar como bloques (tabla)", value=True)
    if st.button("Enviar a Slack"):
        if not msg.strip():
            st.warning("No hay mensaje para enviar.")
        else:
            try:
                blocks = None
                if use_blocks and msg_rows:
                    blocks = [
                        {"type": "section", "text": {"type": "mrkdwn", "text": "Hola, los siguientes proyectos tienen situaciones que se deben corregir:"}}
                    ]
                    for pmo, name, motivo in msg_rows:
                        blocks.append({
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*PMO-ID:*\n{pmo or '-'}"},
                                {"type": "mrkdwn", "text": f"*Nombre:*\n{name or '-'}"},
                                {"type": "mrkdwn", "text": f"*Motivo:*\n{motivo or '-'}"},
                            ],
                        })
                    # Slack limit: 50 blocks
                    if len(blocks) > 50:
                        blocks = blocks[:49] + [{
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": "_(Mensaje truncado: demasiados proyectos)_"}
                        }]
                dm_email_val = (email_to or "").strip()
                if not dm_email_val and resp and resp != "(selecciona)":
                    dm_email_val = _normalize_email_from_name(resp)
                if dm_email_val:
                    post_dm_by_email(cfg, dm_email_val, msg, blocks=blocks)
                else:
                    post_slack_message(cfg, msg, blocks=blocks)
                st.success("Mensaje enviado.")
            except Exception as e:
                st.error(f"Error enviando a Slack: {e}")

elif page == "Seguimiento":
    st.subheader("Seguimiento")
    monitoring_cfg = cfg.get("monitoring", {}) if isinstance(cfg, dict) else {}
    new_days_default = int(monitoring_cfg.get("new_projects_days", 7))
    closing_days_default = int(monitoring_cfg.get("closing_soon_days", 15))
    stale_status_days_default = int(monitoring_cfg.get("no_recent_status_update_days", 90))

    fcols = st.columns(4)
    sponsor_query = fcols[0].text_input("Sponsor contiene", value="Abrigo")
    resp_query = fcols[1].text_input("Responsable contiene")
    new_days = fcols[2].number_input("Nuevos (días)", min_value=1, max_value=120, value=new_days_default, step=1)
    closing_days = fcols[3].number_input("Cierre (días)", min_value=1, max_value=120, value=closing_days_default, step=1)

    st.markdown(f"**Proyectos nuevos (últimos {new_days} días)**")
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=new_days)
    with engine.begin() as conn:
        new_projects = conn.execute(text("""
            SELECT gid, name, owner_name, due_date, status, raw_data, last_status_update_at
            FROM projects
            WHERE (raw_data->'project'->>'created_at')::timestamptz >= :cutoff
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
            ORDER BY (raw_data->'project'->>'created_at')::timestamptz DESC
        """), {
            "cutoff": cutoff_dt,
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
            "resp": resp_query.strip(),
            "resp_like": f"%{resp_query.strip()}%",
        }).mappings().all()

    if new_projects:
        df_new = pd.DataFrame([{
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
            "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
            "Estado": _fmt_status(p.get("status")),
            "Fecha creación": _fmt_date((p.get("raw_data") or {}).get("project", {}).get("created_at")),
        } for p in new_projects])
        st.dataframe(df_new, use_container_width=True, height=260, hide_index=True)
        st.caption(f"Total: {len(df_new)}")
    else:
        st.info("No hay proyectos nuevos en el periodo.")

    st.markdown("**Proyectos On Hold**")
    with engine.begin() as conn:
        on_hold = conn.execute(text("""
            SELECT p.gid, p.name, p.status, p.raw_data,
                   su.gid AS update_gid,
                   su.created_at AS update_at,
                   su.author_name AS update_author,
                   su.text AS update_text
            FROM projects p
            LEFT JOIN LATERAL (
                SELECT gid, created_at, author_name, text
                FROM status_updates
                WHERE project_gid = p.gid
                ORDER BY created_at DESC
                LIMIT 1
            ) su ON TRUE
            WHERE EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
              AND (p.status = 'on_hold' OR p.status = 'blue')
            ORDER BY p.name ASC
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
            "resp": resp_query.strip(),
            "resp_like": f"%{resp_query.strip()}%",
        }).mappings().all()

    if on_hold:
        df_hold = pd.DataFrame([{
            "select": False,
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
            "Update": _truncate_text(p.get("update_text") or "", 10),
            "update_gid": p.get("update_gid"),
        } for p in on_hold])

        edited_hold = st.data_editor(
            df_hold.drop(columns=["update_gid"], errors="ignore"),
            use_container_width=True,
            height=260,
            column_config={"select": st.column_config.CheckboxColumn("")},
            disabled=["PMO-ID", "Proyecto", "Cliente", "Responsable", "Update"],
            hide_index=True,
        )
        st.caption(f"Total: {len(df_hold)}")

        selected_idx = edited_hold[edited_hold["select"] == True].index.tolist() if not edited_hold.empty else []
        if selected_idx:
            sel_idx = selected_idx[0]
            sel = df_hold.loc[sel_idx]
            # show update details in a dialog
            @st.dialog("Update del proyecto")
            def _show_on_hold_update():
                with engine.begin() as conn:
                    su = conn.execute(text("""
                        SELECT gid, created_at, author_name, status_type, text
                        FROM status_updates
                        WHERE gid = :gid
                    """), {"gid": sel["update_gid"]}).mappings().first()
                    comments = conn.execute(text("""
                        SELECT created_at, author_name, text
                        FROM status_update_comments
                        WHERE status_update_gid = :gid
                        ORDER BY created_at ASC
                    """), {"gid": sel["update_gid"]}).mappings().all()
                if su:
                    st.markdown(f"**Fecha:** {su.get('created_at')}")
                    st.markdown(f"**Autor:** {su.get('author_name')}")
                    st.markdown("**Update:**")
                    st.write(su.get("text") or "")
                st.markdown("**Comentarios:**")
                if comments:
                    for c in comments:
                        st.markdown(f"- [{c.get('created_at')}] {c.get('author_name')}: {c.get('text')}")
                else:
                    st.write("Sin comentarios.")
                if st.button("Cerrar"):
                    st.rerun()
            _show_on_hold_update()
    else:
        st.info("No hay proyectos On Hold.")

    st.markdown(f"**Proyectos por cerrar (atrasados o en los próximos {closing_days} días)**")
    today = date.today()
    end_date = today + timedelta(days=closing_days)
    with engine.begin() as conn:
        closing_projects = conn.execute(text("""
            SELECT gid, name, owner_name, status, raw_data, last_status_update_at,
                   total_tasks, completed_tasks,
                   COALESCE(
                     (cf_end->'date_value'->>'date')::date,
                     (cf_end->>'display_value')::date
                   ) AS planned_end_date
            FROM projects
            LEFT JOIN LATERAL jsonb_array_elements(raw_data->'project'->'custom_fields') cf_end
              ON cf_end->>'name' = 'Fecha Planificada Termino del proyecto'
            WHERE COALESCE(
                     (cf_end->'date_value'->>'date')::date,
                     (cf_end->>'display_value')::date
                  ) IS NOT NULL
              AND COALESCE(
                     (cf_end->'date_value'->>'date')::date,
                     (cf_end->>'display_value')::date
                  ) <= :end_date
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
            ORDER BY planned_end_date ASC
        """), {
            "end_date": end_date,
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
            "resp": resp_query.strip(),
            "resp_like": f"%{resp_query.strip()}%",
        }).mappings().all()

    if closing_projects:
        df_close = pd.DataFrame([{
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
            "Estado": _fmt_status(p.get("status")),
            "Fecha término": _fmt_date(p.get("planned_end_date")),
            "Días a cierre": (p.get("planned_end_date") - today).days if p.get("planned_end_date") else "",
            "Días sin update": _days_since_last_update(p.get("last_status_update_at"), today),
            "Tareas": _fmt_task_progress(p.get("completed_tasks"), p.get("total_tasks")),
        } for p in closing_projects])
        df_close = df_close.reset_index(drop=True)
        def _row_style(row):
            try:
                days = int(row["Días a cierre"])
            except Exception:
                return [""] * len(row)
            if days < 0:
                return ["background-color: #f8d7da"] * len(row)
            if 0 <= days <= 3:
                return ["background-color: #fff3cd"] * len(row)
            return [""] * len(row)

        styled = df_close.style.apply(_row_style, axis=1)
        st.dataframe(styled, use_container_width=True, height=260, hide_index=True)
        total_count = len(df_close)
        try:
            days_series = pd.to_numeric(df_close["Días a cierre"], errors="coerce")
            total_atrasados = int((days_series < 0).sum())
            total_cierran_semana = int(((days_series >= 0) & (days_series <= 6)).sum())
        except Exception:
            total_atrasados = 0
            total_cierran_semana = 0
        c_total, c_stats = st.columns([1, 3])
        c_total.caption(f"Total: {total_count}")
        c_stats.caption(f"Atrasados: {total_atrasados} | Cierran esta semana: {total_cierran_semana}")
    else:
        st.info("No hay proyectos con cierre próximo.")

    st.markdown(f"**Proyectos sin status update reciente (≥ {stale_status_days_default} días)**")
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=stale_status_days_default)
    with engine.begin() as conn:
        no_updates = conn.execute(text("""
            SELECT gid, name, owner_name, status, raw_data, last_status_update_at
            FROM projects
            WHERE (last_status_update_at IS NULL OR last_status_update_at <= :stale_cutoff)
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
            ORDER BY name ASC
        """), {
            "stale_cutoff": stale_cutoff,
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
            "resp": resp_query.strip(),
            "resp_like": f"%{resp_query.strip()}%",
        }).mappings().all()

    if no_updates:
        df_no_updates = pd.DataFrame([{
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
            "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
            "Estado": _fmt_status(p.get("status")),
            "Inicio planificado": _fmt_date(_cf_value_from_project_row(p, "Fecha Inicio del proyecto") or _cf_value_from_project_row(p, "Fecha Inicio")),
        } for p in no_updates])
        def _row_style_no_updates(row):
            try:
                start = row.get("Inicio planificado")
                if start:
                    dt = datetime.fromisoformat(str(start))
                    if dt.date() > date.today():
                        return ["background-color: #d4edda"] * len(row)
            except Exception:
                pass
            return [""] * len(row)
        styled = df_no_updates.style.apply(_row_style_no_updates, axis=1)
        st.dataframe(styled, use_container_width=True, height=260, hide_index=True)
        st.caption(f"Total: {len(df_no_updates)}")
    else:
        st.info("No hay proyectos sin status update.")

    st.markdown("**Proyectos con inicio en la semana (±7 días)**")
    today = date.today()
    start_window = today - timedelta(days=7)
    end_window = today + timedelta(days=7)
    with engine.begin() as conn:
        week_start_projects = conn.execute(text("""
            SELECT gid, name, owner_name, status, raw_data,
                   start_cf.planned_start_date AS planned_start_date,
                   phase_cf.phase_name AS phase_name
            FROM projects
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    (cf->'date_value'->>'date')::date,
                    (cf->>'display_value')::date
                ) AS planned_start_date
                FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' IN ('Fecha Inicio del proyecto', 'Fecha Inicio')
                LIMIT 1
            ) start_cf ON TRUE
            LEFT JOIN LATERAL (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','') AS phase_name
                FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE (cf->>'gid' = '1207505889399747' OR cf->>'name' = 'Fase del proyecto')
                LIMIT 1
            ) phase_cf ON TRUE
            WHERE start_cf.planned_start_date IS NOT NULL
              AND start_cf.planned_start_date BETWEEN :start_date AND :end_date
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                  AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
              )
              AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
            ORDER BY start_cf.planned_start_date ASC, name ASC
        """), {
            "start_date": start_window,
            "end_date": end_window,
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
            "resp": resp_query.strip(),
            "resp_like": f"%{resp_query.strip()}%",
        }).mappings().all()

    if week_start_projects:
        df_week = pd.DataFrame([{
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
            "Estado": _fmt_status(p.get("status")),
            "Fase": p.get("phase_name") or "",
            "Inicio planificado": _fmt_date(p.get("planned_start_date")),
        } for p in week_start_projects])

        def _row_style_week_start(row):
            phase = str(row.get("Fase") or "").strip().lower()
            if "ejecucion" in phase:
                return ["background-color: #d4edda"] * len(row)
            if "definicion" in phase or "planificacion" in phase:
                return ["background-color: #fff3cd"] * len(row)
            return [""] * len(row)

        styled = df_week.style.apply(_row_style_week_start, axis=1)
        st.dataframe(styled, use_container_width=True, height=260, hide_index=True)
        st.caption(f"Total: {len(df_week)}")
    else:
        st.info("No hay proyectos con inicio en la semana.")

    st.markdown("**Últimos 20 proyectos cerrados (Terminados / Cancelados)**")
    with engine.begin() as conn:
        closed_projects = conn.execute(text("""
            SELECT gid, name, raw_data
            FROM projects
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                  AND (
                    (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                    OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                    OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                  )
              )
              AND (
                COALESCE(raw_data->'project'->>'completed','false') = 'true'
                OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                  WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                    AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                )
              )
            ORDER BY COALESCE((raw_data->'project'->>'completed_at')::timestamptz, (raw_data->'project'->>'modified_at')::timestamptz, (raw_data->'project'->>'created_at')::timestamptz) DESC
            LIMIT 20
        """)).mappings().all()

    if closed_projects:
        rows = []
        for p in closed_projects:
            raw = (p.get("raw_data") or {}).get("project") or {}
            phase_val = ""
            for f in (raw.get("custom_fields") or []):
                if f.get("gid") == "1207505889399747" or f.get("name") == "Fase del proyecto":
                    phase_val = (f.get("display_value") or (f.get("enum_value") or {}).get("name") or "")
                    break
            phase_val_l = str(phase_val).strip().lower()
            estado = "Cancelado" if "cancelad" in phase_val_l else "Terminado"
            closed_at = raw.get("completed_at") or raw.get("modified_at") or raw.get("created_at")
            rows.append({
                "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
                "Proyecto": p.get("name") or "",
                "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
                "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
                "Fecha cierre": _fmt_date(closed_at),
                "Estado": estado,
            })
        df_closed = pd.DataFrame(rows)
        st.dataframe(df_closed, use_container_width=True, height=260, hide_index=True)
        st.caption(f"Total: {len(df_closed)}")
    else:
        st.info("No hay proyectos cerrados para mostrar.")

elif page == "KPI":
    st.subheader("KPI")
    _ensure_kpi_tables()

    kpi_cfg = (cfg.get("kpi") or {}) if isinstance(cfg, dict) else {}
    lookback_days = int(kpi_cfg.get("weekly_visibility_days", 7))
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)

    def _parse_last(ts):
        if not ts:
            return None
        if isinstance(ts, datetime):
            return ts.astimezone(timezone.utc)
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    def _compute_kpi(rows, group_fn, ok_fn, ok_label):
        grouped = {}
        for p in rows:
            raw = (p.get("raw_data") or {}).get("project") or {}
            key = group_fn(raw)
            if not key:
                key = "(Sin asignar)"
            grouped.setdefault(key, {"total": 0, "ok": 0})
            grouped[key]["total"] += 1
            if ok_fn(p):
                grouped[key]["ok"] += 1
        out = []
        for key, v in grouped.items():
            total = v["total"]
            ok = v["ok"]
            kpi_val = round((ok / total * 100.0), 2) if total else 0.0
            out.append({"Grupo": key, "Total": total, ok_label: ok, "KPI (%)": kpi_val})
        out.sort(key=lambda x: x["KPI (%)"], reverse=True)
        return out

    def _render_kpi_tables(empresa_rows, jp_rows, sponsor_rows):
        c_emp, c_jp, c_sp = st.columns(3)
        with c_emp:
            st.markdown("**Empresa**")
            st.dataframe(pd.DataFrame(empresa_rows), use_container_width=True, height=180, hide_index=True)
        with c_jp:
            st.markdown("**JP**")
            st.dataframe(pd.DataFrame(jp_rows), use_container_width=True, height=320, hide_index=True)
        with c_sp:
            st.markdown("**Sponsor**")
            st.dataframe(pd.DataFrame(sponsor_rows), use_container_width=True, height=320, hide_index=True)

    def _save_kpi_snapshot(kpi_id, empresa_rows, jp_rows, sponsor_rows, ok_label):
        as_of = datetime.now(timezone.utc).isoformat()
        with engine.begin() as conn:
            for row in empresa_rows:
                conn.execute(text("""
                    INSERT INTO kpi_snapshots(kpi_id, scope_type, scope_value, as_of, total_projects, compliant_projects, kpi_value)
                    VALUES(:kpi, :stype, :sval, :as_of, :total, :ok, :val)
                """), {
                    "kpi": kpi_id,
                    "stype": "empresa",
                    "sval": row["Grupo"],
                    "as_of": as_of,
                    "total": row["Total"],
                    "ok": row[ok_label],
                    "val": row["KPI (%)"],
                })
            for row in jp_rows:
                conn.execute(text("""
                    INSERT INTO kpi_snapshots(kpi_id, scope_type, scope_value, as_of, total_projects, compliant_projects, kpi_value)
                    VALUES(:kpi, :stype, :sval, :as_of, :total, :ok, :val)
                """), {
                    "kpi": kpi_id,
                    "stype": "jp",
                    "sval": row["Grupo"],
                    "as_of": as_of,
                    "total": row["Total"],
                    "ok": row[ok_label],
                    "val": row["KPI (%)"],
                })
            for row in sponsor_rows:
                conn.execute(text("""
                    INSERT INTO kpi_snapshots(kpi_id, scope_type, scope_value, as_of, total_projects, compliant_projects, kpi_value)
                    VALUES(:kpi, :stype, :sval, :as_of, :total, :ok, :val)
                """), {
                    "kpi": kpi_id,
                    "stype": "sponsor",
                    "sval": row["Grupo"],
                    "as_of": as_of,
                    "total": row["Total"],
                    "ok": row[ok_label],
                    "val": row["KPI (%)"],
                })
        st.success("Snapshot guardado.")

    def _render_kpi_history(kpi_id):
        st.markdown("**Histórico**")
        with engine.begin() as conn:
            snapshots = conn.execute(text("""
                SELECT kpi_id, scope_type, scope_value, as_of, total_projects, compliant_projects, kpi_value
                FROM kpi_snapshots
                WHERE kpi_id = :kpi
                ORDER BY as_of ASC
            """), {"kpi": kpi_id}).mappings().all()
        if snapshots:
            df_snap = pd.DataFrame(snapshots)
            scope_type = st.selectbox("Ámbito", ["empresa", "sponsor", "jp"], key=f"{kpi_id}_scope")
            scope_values = sorted(df_snap[df_snap["scope_type"] == scope_type]["scope_value"].unique().tolist())
            scope_value = st.selectbox("Valor", scope_values, key=f"{kpi_id}_value")
            df_f = df_snap[(df_snap["scope_type"] == scope_type) & (df_snap["scope_value"] == scope_value)]
            if not df_f.empty:
                fig = px.line(df_f, x="as_of", y="kpi_value", markers=True)
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_f, use_container_width=True, height=240, hide_index=True)
            else:
                st.info("No hay datos históricos para el filtro seleccionado.")
        else:
            st.info("No hay snapshots guardados aún.")

    tab1, tab2, tab3 = st.tabs(["KPI 1", "KPI 2", "KPI 3"])

    with tab1:
        st.markdown("**KPI 1 — Cumplimiento de visibilidad semanal**")
        st.caption(f"Ventana: últimos {lookback_days} días (update con created_at >= now - {lookback_days}d)")
        sponsor_filter = st.text_input("Sponsor contiene", value="Abrigo", key="kpi1_sponsor_filter")
        with engine.begin() as conn:
            projects = conn.execute(text("""
                SELECT gid, name, last_status_update_at, raw_data
                FROM projects
                WHERE EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
                )
                AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                  WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                    AND (
                      (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                      OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                      OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                  WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                    AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_life
                  WHERE cf_life->>'name' IN ('Estado del proyecto', 'Estado proyecto', 'Ciclo de vida', 'Estado del proyecto (ciclo de vida)')
                    AND (
                      lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%cerrad%'
                      OR lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%finaliz%'
                    )
                )
                AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
                AND (name IS NULL OR name NOT ILIKE '%template%')
                AND (:sponsor = '' OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_s
                  WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                ))
            """), {
                "sponsor": sponsor_filter.strip(),
                "sponsor_like": f"%{sponsor_filter.strip()}%",
            }).mappings().all()

        def _kpi1_ok(p):
            last_ts = _parse_last(p.get("last_status_update_at"))
            return bool(last_ts and last_ts >= cutoff)

        empresa_rows = _compute_kpi(projects, lambda raw: "Empresa", _kpi1_ok, "Con update 7d")
        jp_rows = _compute_kpi(projects, lambda raw: _cf_first_value(raw, ["JP responsable", "Responsable Proyecto"]), _kpi1_ok, "Con update 7d")
        sponsor_rows = _compute_kpi(projects, lambda raw: _cf_value_from_project_raw(raw, "Sponsor"), _kpi1_ok, "Con update 7d")
        _render_kpi_tables(empresa_rows, jp_rows, sponsor_rows)
        if st.button("Guardar snapshot KPI 1"):
            _save_kpi_snapshot("weekly_visibility", empresa_rows, jp_rows, sponsor_rows, "Con update 7d")
        _render_kpi_history("weekly_visibility")

    with tab2:
        st.markdown("**KPI 2 — % de proyectos con tareas actualizadas semanalmente**")
        st.caption(f"Ventana: últimos {lookback_days} días (tareas con modified_at >= now - {lookback_days}d)")
        sponsor_filter = st.text_input("Sponsor contiene", value="Abrigo", key="kpi2_sponsor_filter")
        with engine.begin() as conn:
            projects = conn.execute(text("""
                SELECT gid, name, tasks_modified_last_7d, raw_data
                FROM projects
                WHERE EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
                )
                AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                  WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                    AND (
                      (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                      OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                      OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                  WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                    AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_life
                  WHERE cf_life->>'name' IN ('Estado del proyecto', 'Estado proyecto', 'Ciclo de vida', 'Estado del proyecto (ciclo de vida)')
                    AND (
                      lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%cerrad%'
                      OR lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%finaliz%'
                    )
                )
                AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
                AND (name IS NULL OR name NOT ILIKE '%template%')
                AND (:sponsor = '' OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_s
                  WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                ))
            """), {
                "sponsor": sponsor_filter.strip(),
                "sponsor_like": f"%{sponsor_filter.strip()}%",
            }).mappings().all()

        def _kpi2_ok(p):
            try:
                return int(p.get("tasks_modified_last_7d") or 0) > 0
            except Exception:
                return False

        empresa_rows = _compute_kpi(projects, lambda raw: "Empresa", _kpi2_ok, "Con tareas 7d")
        jp_rows = _compute_kpi(projects, lambda raw: _cf_first_value(raw, ["JP responsable", "Responsable Proyecto"]), _kpi2_ok, "Con tareas 7d")
        sponsor_rows = _compute_kpi(projects, lambda raw: _cf_value_from_project_raw(raw, "Sponsor"), _kpi2_ok, "Con tareas 7d")
        _render_kpi_tables(empresa_rows, jp_rows, sponsor_rows)
        if st.button("Guardar snapshot KPI 2"):
            _save_kpi_snapshot("weekly_task_activity", empresa_rows, jp_rows, sponsor_rows, "Con tareas 7d")
        _render_kpi_history("weekly_task_activity")

    with tab3:
        st.markdown("**KPI 3 — % de proyectos con avance consistente**")
        st.caption("Regla: progress_pct >= time_pct - 0.10 (solo proyectos con fechas y tareas válidas)")
        sponsor_filter = st.text_input("Sponsor contiene", value="Abrigo", key="kpi3_sponsor_filter")

        with engine.begin() as conn:
            projects = conn.execute(text("""
                SELECT gid, name, total_tasks, completed_tasks, raw_data
                FROM projects
                WHERE EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
                )
                AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_bv
                  WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                    AND (
                      (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                      OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                      OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                  WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                    AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%' OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                )
                AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_life
                  WHERE cf_life->>'name' IN ('Estado del proyecto', 'Estado proyecto', 'Ciclo de vida', 'Estado del proyecto (ciclo de vida)')
                    AND (
                      lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%cerrad%'
                      OR lower(COALESCE(cf_life->>'display_value', cf_life->'enum_value'->>'name','')) LIKE '%finaliz%'
                    )
                )
                AND COALESCE(raw_data->'project'->>'completed','false') <> 'true'
                AND (name IS NULL OR name NOT ILIKE '%template%')
                AND (:sponsor = '' OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_s
                  WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                ))
            """), {
                "sponsor": sponsor_filter.strip(),
                "sponsor_like": f"%{sponsor_filter.strip()}%",
            }).mappings().all()

        def _parse_date_value(val):
            if not val:
                return None
            if isinstance(val, date) and not isinstance(val, datetime):
                return val
            if isinstance(val, datetime):
                return val.date()
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00")).date()
            except Exception:
                return None

        def _get_project_start_end(raw):
            # start: prefer custom fields, then project.start_on
            start_val = _cf_first_value(raw, ["Fecha Inicio del proyecto", "Fecha Inicio"])
            start_date = _parse_date_value(start_val) if start_val else None
            if not start_date:
                start_date = _parse_date_value(raw.get("start_on"))

            # due: prefer project due_date/due_on, then custom planned end
            due_val = raw.get("due_date") or raw.get("due_on")
            due_date = _parse_date_value(due_val) if due_val else None
            if not due_date:
                due_cf = _cf_first_value(raw, ["Fecha Planificada Termino del proyecto"])
                due_date = _parse_date_value(due_cf) if due_cf else None
            return start_date, due_date

        def _kpi3_group(rows, group_fn):
            grouped = {}
            for p in rows:
                raw = (p.get("raw_data") or {}).get("project") or {}
                key = group_fn(raw) or "(Sin asignar)"
                grouped.setdefault(key, {"eligible": 0, "ok": 0, "ineligible": 0})

                start_date, due_date = _get_project_start_end(raw)
                total = int(p.get("total_tasks") or 0)
                completed = int(p.get("completed_tasks") or 0)
                if not start_date or not due_date or due_date <= start_date or total <= 0:
                    grouped[key]["ineligible"] += 1
                    continue

                days_total = (due_date - start_date).days
                if days_total <= 0:
                    grouped[key]["ineligible"] += 1
                    continue

                time_pct = (date.today() - start_date).days / days_total
                if time_pct < 0:
                    time_pct = 0.0
                if time_pct > 1:
                    time_pct = 1.0

                progress_pct = completed / total if total > 0 else 0.0
                is_consistent = progress_pct >= (time_pct - 0.10)

                grouped[key]["eligible"] += 1
                if is_consistent:
                    grouped[key]["ok"] += 1

            out = []
            for key, v in grouped.items():
                eligible = v["eligible"]
                ok = v["ok"]
                ineligible = v["ineligible"]
                kpi_val = round((ok / eligible * 100.0), 2) if eligible else 0.0
                out.append({
                    "Grupo": key,
                    "Elegibles": eligible,
                    "Consistentes": ok,
                    "Sin datos": ineligible,
                    "KPI (%)": kpi_val,
                })
            out.sort(key=lambda x: x["KPI (%)"], reverse=True)
            return out

        empresa_rows = _kpi3_group(projects, lambda raw: "Empresa")
        jp_rows = _kpi3_group(projects, lambda raw: _cf_first_value(raw, ["JP responsable", "Responsable Proyecto"]))
        sponsor_rows = _kpi3_group(projects, lambda raw: _cf_value_from_project_raw(raw, "Sponsor"))
        _render_kpi_tables(empresa_rows, jp_rows, sponsor_rows)

        if st.button("Guardar snapshot KPI 3"):
            _save_kpi_snapshot("consistent_progress", empresa_rows, jp_rows, sponsor_rows, "Consistentes")
        _render_kpi_history("consistent_progress")

elif page == "Búsqueda":
    st.subheader("Búsqueda")
    st.caption("Consulta en tablas locales: proyectos sincronizados + históricos. Prioriza sincronizados.")

    if "search_mode" not in st.session_state:
        st.session_state["search_mode"] = None
    if "search_query" not in st.session_state:
        st.session_state["search_query"] = ""
    if "search_filters" not in st.session_state:
        st.session_state["search_filters"] = {"status": "", "sponsor": "", "phase": ""}
    if "use_history" not in st.session_state:
        st.session_state["use_history"] = False
    if "use_history_widget" not in st.session_state:
        st.session_state["use_history_widget"] = st.session_state["use_history"]
    if "filter_status_widget" not in st.session_state:
        st.session_state["filter_status_widget"] = "(todos)"
    if "filter_sponsor_widget" not in st.session_state:
        st.session_state["filter_sponsor_widget"] = ""
    if "filter_phase_widget" not in st.session_state:
        st.session_state["filter_phase_widget"] = ""
    if "pending_apply" not in st.session_state:
        st.session_state["pending_apply"] = False

    search_text = st.text_input(
        "Buscar",
        placeholder="PMO-ID, Cliente, Nombre, Responsable, AWS OPP ID, ID_Comercial",
        key="search_text",
    )
    use_history = st.checkbox(
        "Usar proyectos históricos",
        value=st.session_state["use_history_widget"],
        key="use_history_widget",
    )

    fcols = st.columns(3)
    status_options = ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "green", "yellow", "red", "blue"]
    current_status = st.session_state["filter_status_widget"] or "(todos)"
    status_index = status_options.index(current_status) if current_status in status_options else 0
    status_filter = fcols[0].selectbox("Estado", status_options, index=status_index, key="filter_status_widget")
    sponsor_filter = fcols[1].text_input(
        "Sponsor contiene",
        value=st.session_state["filter_sponsor_widget"],
        key="filter_sponsor_widget",
    )
    phase_filter = fcols[2].text_input(
        "Fase del proyecto contiene",
        value=st.session_state["filter_phase_widget"],
        key="filter_phase_widget",
    )

    bcols = st.columns(4)
    run_search = bcols[0].button("Buscar")
    run_list_all = bcols[1].button("Listar todos")
    apply_filters = bcols[2].button("Aplicar filtros")
    clear_filters = bcols[3].button("Limpiar filtros")

    if run_search:
        st.session_state["search_mode"] = "search"
        st.session_state["search_query"] = (search_text or "").strip()
        st.session_state["pending_apply"] = True
    if run_list_all:
        st.session_state["search_mode"] = "list_all"
        st.session_state["search_query"] = ""
        st.session_state["pending_apply"] = True
    if clear_filters:
        st.session_state["search_filters"] = {"status": "", "sponsor": "", "phase": ""}
        st.session_state["filter_status_widget"] = "(todos)"
        st.session_state["filter_sponsor_widget"] = ""
        st.session_state["filter_phase_widget"] = ""
        st.session_state["pending_apply"] = True
        st.info("Filtros limpiados.")

    if apply_filters:
        status_val = "" if status_filter == "(todos)" else status_filter
        st.session_state["search_filters"] = {
            "status": status_val,
            "sponsor": sponsor_filter.strip(),
            "phase": phase_filter.strip(),
        }
        st.session_state["use_history"] = bool(use_history)
        st.session_state["pending_apply"] = False

        mode = st.session_state.get("search_mode")
        query = (st.session_state.get("search_query") or "").strip()
        filters = st.session_state["search_filters"]

        if mode is None:
            st.warning("Usa 'Buscar' o 'Listar todos' antes de aplicar filtros.")
        elif mode == "search" and not query:
            st.warning("Ingresa un texto de búsqueda o usa 'Listar todos'.")
        else:
            with engine.begin() as conn:
                where_sync = []
                params_sync = {}
                if mode == "search":
                    where_sync.append("""
                        (
                          p.name ILIKE :q
                          OR EXISTS (
                            SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                            WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') ILIKE :q
                          )
                          OR EXISTS (
                            SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                            WHERE cf->>'name' IN ('cliente_nuevo','Cliente_nuevo') AND COALESCE(cf->>'display_value','') ILIKE :q
                          )
                          OR EXISTS (
                            SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                            WHERE cf->>'name' = 'Responsable Proyecto' AND COALESCE(cf->>'display_value','') ILIKE :q
                          )
                          OR EXISTS (
                            SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                            WHERE cf->>'name' = 'AWS OPP ID' AND COALESCE(cf->>'display_value','') ILIKE :q
                          )
                          OR EXISTS (
                            SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                            WHERE cf->>'name' = 'ID_Comercial' AND COALESCE(cf->>'display_value','') ILIKE :q
                          )
                        )
                    """)
                    params_sync["q"] = f"%{query}%"
                if filters.get("status"):
                    where_sync.append("p.status = :status")
                    params_sync["status"] = filters["status"]
                if filters.get("sponsor"):
                    where_sync.append("""
                        EXISTS (
                          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                          WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
                        )
                    """)
                    params_sync["sponsor_like"] = f"%{filters['sponsor']}%"
                if filters.get("phase"):
                    where_sync.append("""
                        EXISTS (
                          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_phase
                          WHERE cf_phase->>'name' = 'Fase del proyecto' AND COALESCE(cf_phase->>'display_value','') ILIKE :phase_like
                        )
                    """)
                    params_sync["phase_like"] = f"%{filters['phase']}%"

                sql_sync = """
                    SELECT p.gid, p.name, p.owner_name, p.due_date, p.status, p.raw_data
                    FROM projects p
                """
                if where_sync:
                    sql_sync += " WHERE " + " AND ".join(where_sync)
                sql_sync += " ORDER BY p.name ASC"

                rows_sync = conn.execute(text(sql_sync), params_sync).mappings().all()
                sync_gids = {r.get("gid") for r in rows_sync if r.get("gid")}

                rows_hist = []
                if st.session_state.get("use_history"):
                    where_hist = []
                    params_hist = {}
                    if mode == "search":
                        where_hist.append("""
                            (
                              search_text ILIKE :q
                              OR COALESCE(name,'') ILIKE :q
                              OR COALESCE(pmo_id,'') ILIKE :q
                              OR COALESCE(cliente_nuevo,'') ILIKE :q
                              OR COALESCE(responsable_proyecto,'') ILIKE :q
                              OR COALESCE(aws_opp_id,'') ILIKE :q
                              OR COALESCE(id_comercial,'') ILIKE :q
                            )
                        """)
                        params_hist["q"] = f"%{query}%"
                    if filters.get("status"):
                        where_hist.append("status = :status")
                        params_hist["status"] = filters["status"]
                    if filters.get("sponsor"):
                        where_hist.append("COALESCE(sponsor,'') ILIKE :sponsor_like")
                        params_hist["sponsor_like"] = f"%{filters['sponsor']}%"
                    if filters.get("phase"):
                        where_hist.append("""
                            EXISTS (
                              SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf_phase
                              WHERE cf_phase->>'name' = 'Fase del proyecto' AND COALESCE(cf_phase->>'display_value','') ILIKE :phase_like
                            )
                        """)
                        params_hist["phase_like"] = f"%{filters['phase']}%"

                    sql_hist = """
                        SELECT gid, name, owner_name, status, raw_data
                        FROM projects_history
                    """
                    if where_hist:
                        sql_hist += " WHERE " + " AND ".join(where_hist)
                    sql_hist += " ORDER BY name ASC"

                    rows_hist = conn.execute(text(sql_hist), params_hist).mappings().all()

            rows = []
            for r in rows_sync:
                raw = (r.get("raw_data") or {}).get("project") or {}
                cf_map = _custom_field_map(raw)
                row = {
                    "gid": r.get("gid"),
                    "name": r.get("name"),
                    "owner_name": r.get("owner_name"),
                    "due_date": r.get("due_date"),
                    "status": r.get("status"),
                    "source": "sync",
                }
                for k, v in cf_map.items():
                    row[f"cf:{k}"] = v
                rows.append(row)

            for r in rows_hist:
                if r.get("gid") in sync_gids:
                    continue
                raw = (r.get("raw_data") or {}).get("project") or {}
                cf_map = _custom_field_map(raw)
                row = {
                    "gid": r.get("gid"),
                    "name": r.get("name"),
                    "owner_name": r.get("owner_name"),
                    "due_date": raw.get("due_date") or raw.get("due_on"),
                    "status": r.get("status"),
                    "source": "history",
                }
                for k, v in cf_map.items():
                    row[f"cf:{k}"] = v
                rows.append(row)

            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, height=520, hide_index=True)
                st.caption(f"Total: {len(df)}")
            else:
                st.info("No se encontraron proyectos para los filtros aplicados.")
    elif st.session_state.get("pending_apply"):
        st.info("Presiona 'Aplicar filtros' para ejecutar la consulta.")

elif page == "Plan de facturación":
    st.subheader("Plan de facturación")
    st.caption("Proyectos con custom field 'En plan de facturación' = SI.")

    sponsor_filter = st.text_input("Sponsor contiene", value="", key="billing_sponsor_filter")

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT p.gid, p.name, p.owner_name, p.due_date, p.status, p.raw_data
            FROM projects p
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_plan
              WHERE cf_plan->>'name' = 'En plan de facturación'
                AND lower(COALESCE(cf_plan->>'display_value','')) = 'si'
            )
            AND (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor'
                AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            ORDER BY p.name ASC
        """), {
            "sponsor": sponsor_filter.strip(),
            "sponsor_like": f"%{sponsor_filter.strip()}%",
        }).mappings().all()

    def _is_terminated(p):
        raw = (p.get("raw_data") or {}).get("project") or {}
        phase = (_cf_value_from_project_row(p, "Fase del proyecto") or "").strip().lower()
        completed = str(raw.get("completed", "")).lower() == "true"
        return completed or ("terminad" in phase) or ("cancelad" in phase)

    def _is_risk(status):
        return (status or "").lower() in {"at_risk", "off_track", "on_hold", "yellow", "red", "blue"}

    if rows:
        df = pd.DataFrame([{
            "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto") or p.get("owner_name") or "",
            "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
            "Fase del proyecto": _cf_value_from_project_row(p, "Fase del proyecto"),
            "Término estimado": _fmt_date(
                _cf_first_value((p.get("raw_data") or {}).get("project") or {}, ["Fecha Planificada Termino del proyecto"])
                or (p.get("raw_data") or {}).get("project", {}).get("due_date")
                or (p.get("raw_data") or {}).get("project", {}).get("due_on")
                or p.get("due_date")
            ),
            "Total presupuestado": _cf_first_value(
                (p.get("raw_data") or {}).get("project") or {},
                ["Total presupuestado", "Presupuesto total", "Budget", "Total Budget"],
            ),
            "Estado": _fmt_status(p.get("status")),
            "status_raw": (p.get("status") or ""),
            "Terminado": "Sí" if _is_terminated(p) else "No",
        } for p in rows])

        def _style_row(row):
            status_label = row.get("status_raw") or row.get("Estado") or ""
            status_norm = str(status_label).strip().lower().replace(" ", "_")
            if status_norm in {"complete", "completed"}:
                return ["background-color: #1e7e34; color: white"] * len(row)
            if status_norm in {"on_track", "green"}:
                return ["background-color: #d4edda"] * len(row)
            return ["background-color: #f8d7da"] * len(row)

        def _sort_rank(row):
            status_label = row.get("status_raw") or row.get("Estado") or ""
            status_norm = str(status_label).strip().lower().replace(" ", "_")
            if status_norm in {"complete", "completed"}:
                return 0
            if status_norm in {"on_track", "green"}:
                return 1
            return 2

        df["_sort_rank"] = df.apply(_sort_rank, axis=1)
        df = df.sort_values(by=["_sort_rank", "Proyecto"], kind="mergesort").reset_index(drop=True)
        styled = df.drop(columns=["status_raw", "_sort_rank"], errors="ignore").style.apply(_style_row, axis=1)
        st.dataframe(styled, use_container_width=True, height=520, hide_index=True)
        st.caption(f"Total: {len(df)}")

        budget_values = df["Total presupuestado"].apply(_parse_budget)
        df_budget = df.assign(_budget=budget_values)
        total_budget = float(df_budget["_budget"].fillna(0).sum())
        by_status = (
            df_budget.groupby("Estado", dropna=False)["_budget"]
            .sum()
            .reset_index()
            .rename(columns={"_budget": "Presupuesto"})
        )
        if total_budget > 0:
            by_status["Porcentaje"] = by_status["Presupuesto"] / total_budget * 100.0
            def _status_color_key(label):
                norm = str(label or "").strip().lower().replace(" ", "_")
                if norm in {"complete", "completed"}:
                    return "complete"
                if norm in {"on_track", "green"}:
                    return "on_track"
                return "other"

            color_map = {}
            for label in by_status["Estado"].tolist():
                key = _status_color_key(label)
                if key == "complete":
                    color_map[label] = "#1e7e34"
                elif key == "on_track":
                    color_map[label] = "#d4edda"
                else:
                    color_map[label] = "#f8d7da"

            fig = px.pie(by_status, values="Presupuesto", names="Estado", color="Estado", color_discrete_map=color_map)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Total presupuestado: {total_budget:,.2f}")
        else:
            st.info("No hay monto presupuestado para calcular porcentajes por estado.")
    else:
        st.info("No hay proyectos en plan de facturación para el filtro seleccionado.")

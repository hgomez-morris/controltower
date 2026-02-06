import streamlit as st
import json
import os
import pandas as pd
import io
import zipfile
import plotly.express as px
from openpyxl.utils import get_column_letter
from datetime import datetime, timezone, date, timedelta
from sqlalchemy import text
from controltower.db.connection import get_engine
from controltower.config import load_config
from controltower.actions.slack import post_new_findings_to_slack, post_findings_to_slack_by_ids, post_slack_message, post_dm_by_email
import unicodedata
import re

st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
engine = get_engine()
cfg = load_config("config/config.yaml") if os.path.exists("config/config.yaml") else load_config("config/config.example.yaml")

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

# Sidebar menu only
with st.sidebar:
    st.header("Menu")
    page = st.radio("Ir a", ["Dashboard", "Proyectos", "Findings", "Mensajes", "Seguimiento"], label_visibility="collapsed")

if page == "Dashboard":
    st.subheader("Dashboard")
    sponsor_query = st.text_input("Sponsor contiene", value="Abrigo")
    with engine.begin() as conn:
        counts = conn.execute(text("""
            SELECT
              SUM(CASE WHEN f.status='open' THEN 1 ELSE 0 END) AS open_findings,
              SUM(CASE WHEN f.severity='high' AND f.status='open' THEN 1 ELSE 0 END) AS high_open
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
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
        }).mappings().one()
        by_rule = conn.execute(text("""
            SELECT f.rule_id, COUNT(*) AS n
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE f.status='open' AND EXISTS (
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
            GROUP BY f.rule_id
            ORDER BY n DESC
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
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
            GROUP BY project_status
            ORDER BY n DESC
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
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
        """), {
            "sponsor": sponsor_query.strip(),
            "sponsor_like": f"%{sponsor_query.strip()}%",
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
                WHERE f.status='open'
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

    st.markdown("**Flujo de caja por semana (Total presupuestado)**")
    if closing_dates:
        rows_budget = []
        today = date.today()
        for r in closing_dates:
            d = r.get("planned_end_date")
            if not d:
                continue
            # week bucket
            if d < today:
                week_key = "Semana pasada"
            else:
                iso_year, iso_week, _ = d.isocalendar()
                week_key = f"{iso_year}-W{iso_week:02d}"
            # budget from custom field
            raw = (r.get("raw_data") or {}).get("project") or {}
            budget_val = _cf_value_from_project_row({"raw_data": {"project": raw}}, "Total presupuestado")
            amount = _parse_budget(budget_val)
            if amount is None:
                continue
            rows_budget.append({
                "Semana": week_key,
                "Proyecto": r.get("name") or "",
                "Monto": amount,
            })
        if rows_budget:
            df_budget = pd.DataFrame(rows_budget)
            fig_budget = px.bar(df_budget, x="Semana", y="Monto", color="Proyecto")
            st.plotly_chart(fig_budget, use_container_width=True)
        else:
            st.info("No hay montos en 'Total presupuestado' para mostrar.")

elif page == "Proyectos":
    st.subheader("Proyectos")
    fcols = st.columns(6)
    project_query = fcols[0].text_input("Proyecto contiene")
    pmo_id_query = fcols[1].text_input("PMO-ID contiene")
    resp_query = fcols[2].text_input("Responsable contiene")
    client_query = fcols[3].text_input("Cliente contiene")
    sponsor_query = fcols[4].text_input("Sponsor contiene", value="Abrigo")
    status_filter = fcols[5].selectbox("Estado", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])

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

    fcols = st.columns(5)
    rule_filter = fcols[0].selectbox("Regla", rule_ids)
    severity_filter = fcols[1].selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    project_status_filter = fcols[2].selectbox("Estado proyecto", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])
    sponsor_query = fcols[3].text_input("Sponsor contiene", value="Abrigo")
    resp_query = fcols[4].text_input("Responsable contiene")

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

    # Load responsables list
    with engine.begin() as conn:
        responsables = conn.execute(text("""
            SELECT DISTINCT TRIM(COALESCE(cf->>'display_value','')) AS responsable
            FROM projects p
            JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              ON cf->>'name' = 'Responsable Proyecto'
            WHERE TRIM(COALESCE(cf->>'display_value','')) <> ''
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
            ORDER BY responsable
        """)).mappings().all()
    responsables_list = [r["responsable"] for r in responsables] if responsables else []
    sponsor_filter = st.text_input("Sponsor contiene", value="Abrigo")
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
                WHERE f.status='open'
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
        st.caption(f"Total: {len(df_close)}")
    else:
        st.info("No hay proyectos con cierre próximo.")

    st.markdown("**Proyectos sin status update (nunca actualizado)**")
    with engine.begin() as conn:
        no_updates = conn.execute(text("""
            SELECT gid, name, owner_name, status, raw_data, last_status_update_at
            FROM projects
            WHERE last_status_update_at IS NULL
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

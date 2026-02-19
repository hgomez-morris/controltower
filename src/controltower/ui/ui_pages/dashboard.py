
import io
import json
import os
import re
import zipfile
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from openpyxl.utils import get_column_letter
from sqlalchemy import text
from streamlit_plotly_events import plotly_events

from controltower.actions.slack import (
    post_dm_by_email,
    post_findings_to_slack_by_ids,
    post_new_findings_to_slack,
    post_slack_message,
)
from controltower.asana.client import AsanaReadOnlyClient
from controltower.ui.lib.common import (
    _cf_first_value,
    _cf_value_from_project_raw,
    _cf_value_from_project_row,
    _custom_field_map,
    _days_since_last_update,
    _extract_next_steps,
    _fmt_date,
    _fmt_status,
    _fmt_task_progress,
    _humanize_last_update,
    _jsonable,
    _normalize_email_from_name,
    _parse_budget,
    _set_query_params,
    _get_query_params,
    _truncate_text,
)
from controltower.ui.lib.context import CHILE_TZ, get_cfg, get_engine_cached
from controltower.ui.lib.db_admin import _ensure_kpi_tables, _ensure_payments_tables


def render():
    st.title("PMO Control Tower - MVP")
    engine = get_engine_cached()
    cfg = get_cfg()

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

    if "dashboard_pie_selection" not in st.session_state:
        st.session_state["dashboard_pie_selection"] = None
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
                "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
                "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
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
            AND (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
            GROUP BY responsable
            ORDER BY n DESC
        """), {
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
            "bv": "" if bv_query == "(todos)" else bv_query.strip(),
            "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
        }).mappings().all()

    pie_cols = st.columns(2)
    with pie_cols[0]:
        st.markdown("**Proyectos por estado**")
        if by_project_status:
            df_status = pd.DataFrame(by_project_status).rename(columns={"n": "Cantidad"})
            df_status["project_status_label"] = df_status["project_status"].apply(_fmt_status)
            status_map = {}
            for r in by_project_status:
                label = _fmt_status(r.get("project_status"))
                status_map.setdefault(label, set()).add(r.get("project_status"))
            df_status["Cantidad"] = pd.to_numeric(df_status["Cantidad"], errors="coerce").fillna(0)
            df_status = (
                df_status.groupby("project_status_label", dropna=False)["Cantidad"]
                .sum()
                .reset_index()
            )
            df_status["Cantidad"] = pd.to_numeric(df_status["Cantidad"], errors="coerce").fillna(0).astype(float)
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
            selected = plotly_events(
                fig_status,
                click_event=True,
                select_event=False,
                hover_event=False,
                key="dashboard_pie_status",
            )
            if selected:
                point = selected[0] or {}
                label = point.get("label")
                if label is None:
                    idx = point.get("pointNumber")
                    if idx is not None and 0 <= int(idx) < len(df_status):
                        label = df_status.iloc[int(idx)]["project_status_label"]
                label = label or ""
                st.session_state["dashboard_pie_selection"] = {
                    "type": "status",
                    "label": label,
                    "raw_statuses": sorted({s for s in status_map.get(label, set()) if s is not None}),
                }
        else:
            st.info("No hay datos para mostrar.")
    with pie_cols[1]:
        st.markdown("**Proyectos por responsable**")
        if by_responsable:
            df_resp = pd.DataFrame(by_responsable).rename(columns={"n": "Cantidad"})
            df_resp["Cantidad"] = pd.to_numeric(df_resp["Cantidad"], errors="coerce").fillna(0).astype(float)
            fig_resp = px.pie(df_resp, values="Cantidad", names="responsable")
            selected = plotly_events(
                fig_resp,
                click_event=True,
                select_event=False,
                hover_event=False,
                key="dashboard_pie_responsable",
            )
            if selected:
                point = selected[0] or {}
                responsable = point.get("label")
                if responsable is None:
                    idx = point.get("pointNumber")
                    if idx is not None and 0 <= int(idx) < len(df_resp):
                        responsable = df_resp.iloc[int(idx)]["responsable"]
                responsable = responsable or ""
                st.session_state["dashboard_pie_selection"] = {
                    "type": "responsable",
                    "label": responsable,
                }
        else:
            st.info("No hay datos para mostrar.")

    sel = st.session_state.get("dashboard_pie_selection") or {}
    if sel:
        st.markdown("**Proyectos del segmento seleccionado**")
        with engine.begin() as conn:
            where = []
            params = {
                "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
                "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
                "bv": "" if bv_query == "(todos)" else bv_query.strip(),
                "bv_like": f"%{'' if bv_query == '(todos)' else bv_query.strip()}%",
            }
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
                    AND (lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%'
                      OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%')
                )
            """)
            where.append("COALESCE(p.raw_data->'project'->>'completed','false') <> 'true'")
            where.append("""(:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_s
                WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))""")
            where.append("""(:bv = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_bv2
                WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                  AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))""")

            if sel.get("type") == "status":
                raw_statuses = sel.get("raw_statuses") or []
                raw_statuses = [s for s in raw_statuses if s]
                clauses = []
                if raw_statuses:
                    clauses.append("p.status = ANY(:status_in)")
                    params["status_in"] = raw_statuses
                if sel.get("label") == "(Sin Status)":
                    clauses.append("(p.status IS NULL OR p.status = '')")
                if clauses:
                    where.append("(" + " OR ".join(clauses) + ")")
            elif sel.get("type") == "responsable":
                resp = (sel.get("label") or "").strip()
                if resp.lower() == "(sin responsable)":
                    where.append("""
                        NOT EXISTS (
                          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_r
                          WHERE cf_r->>'name' = 'Responsable Proyecto' AND COALESCE(cf_r->>'display_value','') <> ''
                        )
                    """)
                else:
                    where.append("""
                        EXISTS (
                          SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf_r
                          WHERE cf_r->>'name' = 'Responsable Proyecto' AND COALESCE(cf_r->>'display_value','') ILIKE :resp_like
                        )
                    """)
                    params["resp_like"] = f"%{resp}%"

            sql = """
                SELECT p.gid, p.name, p.owner_name, p.status, p.raw_data
                FROM projects p
            """
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY p.name ASC"
            rows = conn.execute(text(sql), params).mappings().all()

        if rows:
            df_sel = pd.DataFrame([{
                "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
                "Proyecto": p.get("name") or "",
                "Cliente": _cf_value_from_project_row(p, "cliente_nuevo"),
                "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto"),
                "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
                "Estado": _fmt_status(p.get("status")),
            } for p in rows])
            st.dataframe(df_sel, use_container_width=True, height=360, hide_index=True)
            st.caption(f"Total: {len(df_sel)}")
        else:
            st.info("No hay proyectos para el segmento seleccionado.")

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
            "sponsor": "" if sponsor_query == "(todos)" else sponsor_query.strip(),
            "sponsor_like": f"%{'' if sponsor_query == '(todos)' else sponsor_query.strip()}%",
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


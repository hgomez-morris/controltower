
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
from controltower.ui.lib.sidebar import apply_sidebar_style, render_sidebar_footer


def render():
    apply_sidebar_style()
    render_sidebar_footer()
    st.title("PMO Control Tower - MVP")
    engine = get_engine_cached()
    cfg = get_cfg()

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


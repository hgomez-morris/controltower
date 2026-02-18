
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


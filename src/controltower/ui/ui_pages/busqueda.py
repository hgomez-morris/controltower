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


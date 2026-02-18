
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



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


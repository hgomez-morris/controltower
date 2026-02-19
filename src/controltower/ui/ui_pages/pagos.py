
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

    st.subheader("Pagos")
    _ensure_payments_tables()

    pmo_id_input = st.text_input("PMO-ID")
    if pmo_id_input.strip():
        pmo_id = pmo_id_input.strip()
        with engine.begin() as conn:
            projects = conn.execute(text("""
                SELECT p.gid, p.name
                FROM projects p
                WHERE EXISTS (
                  SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') = :pmo
                )
                LIMIT 5
            """), {"pmo": pmo_id}).mappings().all()

        project_gid = ""
        project_name = ""
        if projects:
            if len(projects) == 1:
                project_gid = projects[0].get("gid") or ""
                project_name = projects[0].get("name") or ""
            else:
                options = [f"{p.get('gid')} | {p.get('name')}" for p in projects]
                sel = st.selectbox("Proyecto", options)
                if sel:
                    project_gid = sel.split("|")[0].strip()
                    for p in projects:
                        if p.get("gid") == project_gid:
                            project_name = p.get("name") or ""
                            break
        else:
            st.info("No se encontró proyecto con ese PMO-ID en la base local.")

        if project_name:
            st.caption(f"Proyecto: {project_name}")

        with engine.begin() as conn:
            payments = conn.execute(text("""
                SELECT id, project_gid, pmo_id, status, payment_date, glosa, created_at, updated_at
                FROM payments
                WHERE pmo_id = :pmo
                ORDER BY payment_date ASC NULLS LAST, created_at ASC
            """), {"pmo": pmo_id}).mappings().all()

        st.markdown("**Pagos registrados**")
        if payments:
            df_pay = pd.DataFrame([{
                "ID": r.get("id"),
                "Estado": r.get("status"),
                "Fecha": _fmt_date(r.get("payment_date")),
                "Glosa": r.get("glosa") or "",
                "Creado": _fmt_date(r.get("created_at")),
            } for r in payments])
            st.dataframe(df_pay, use_container_width=True, height=260, hide_index=True)
        else:
            st.info("No hay pagos registrados para este PMO-ID.")

        st.markdown("**Registrar nuevo pago**")
        with st.form("new_payment"):
            status_new = st.selectbox("Estado", ["Estimado", "Efectuado"])
            date_new = st.date_input("Fecha")
            glosa_new = st.text_area("Glosa", height=80)
            submitted = st.form_submit_button("Guardar pago")
            if submitted:
                status_db = "estimado" if status_new == "Estimado" else "efectuado"
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO payments (project_gid, pmo_id, status, payment_date, glosa)
                        VALUES (:gid, :pmo, :status, :fecha, :glosa)
                    """), {
                        "gid": project_gid or None,
                        "pmo": pmo_id,
                        "status": status_db,
                        "fecha": date_new,
                        "glosa": glosa_new.strip() or None,
                    })
                st.success("Pago registrado.")
                st.rerun()

        if payments:
            st.markdown("**Actualizar pagos estimados**")
            for r in payments:
                if (r.get("status") or "").lower() != "estimado":
                    continue
                pid = r.get("id")
                with st.form(f"edit_payment_{pid}"):
                    st.write(f"Pago estimado #{pid}")
                    current_date = r.get("payment_date")
                    new_date = st.date_input("Nueva fecha", value=current_date, key=f"date_{pid}")
                    glosa_edit = st.text_area("Glosa", value=r.get("glosa") or "", height=60, key=f"glosa_{pid}")
                    save = st.form_submit_button("Actualizar estimado")
                    if save:
                        with engine.begin() as conn:
                            if current_date != new_date:
                                conn.execute(text("""
                                    INSERT INTO payment_estimate_history (payment_id, old_date, new_date)
                                    VALUES (:pid, :old, :new)
                                """), {"pid": pid, "old": current_date, "new": new_date})
                            conn.execute(text("""
                                UPDATE payments
                                SET payment_date = :fecha, glosa = :glosa, updated_at = NOW()
                                WHERE id = :pid
                            """), {"pid": pid, "fecha": new_date, "glosa": glosa_edit.strip() or None})
                        st.success("Pago estimado actualizado.")
                        st.rerun()
    else:
        st.info("No hay proyectos en plan de facturación para el filtro seleccionado.")

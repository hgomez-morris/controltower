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
            "PMO-ID": p.get("pmo_id") or _cf_value_from_project_row(p, "PMO ID"),
            "Proyecto": p.get("name") or "",
            "Responsable": p.get("responsable_proyecto") or _cf_value_from_project_row(p, "Responsable Proyecto") or p.get("owner_name") or "",
            "Sponsor": p.get("sponsor") or _cf_value_from_project_row(p, "Sponsor"),
            "Fase del proyecto": p.get("fase_proyecto") or _cf_value_from_project_row(p, "Fase del proyecto"),
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

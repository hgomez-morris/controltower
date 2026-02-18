
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


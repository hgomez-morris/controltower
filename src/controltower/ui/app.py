import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
from controltower.db.connection import get_engine
from controltower.config import load_config
from controltower.actions.slack import post_new_findings_to_slack, post_findings_to_slack_by_ids

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

# Sidebar menu only
with st.sidebar:
    st.header("Menu")
    page = st.radio("Ir a", ["Dashboard", "Proyectos", "Findings"], label_visibility="collapsed")

if page == "Dashboard":
    st.subheader("Dashboard")
    with engine.begin() as conn:
        counts = conn.execute(text("""
            SELECT
              SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_findings,
              SUM(CASE WHEN severity='high' AND status='open' THEN 1 ELSE 0 END) AS high_open
            FROM findings
        """)).mappings().one()
    c1, c2 = st.columns(2)
    c1.metric("Hallazgos abiertos", counts["open_findings"] or 0)
    c2.metric("Hallazgos alta severidad", counts["high_open"] or 0)

elif page == "Proyectos":
    st.subheader("Proyectos")
    fcols = st.columns(6)
    project_query = fcols[0].text_input("Proyecto contiene")
    pmo_id_query = fcols[1].text_input("PMO-ID contiene")
    resp_query = fcols[2].text_input("Responsable contiene")
    client_query = fcols[3].text_input("Cliente contiene")
    sponsor_query = fcols[4].text_input("Sponsor contiene")
    status_filter = fcols[5].selectbox("Estado", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])

    fcols2 = st.columns(3)
    limit = fcols2[0].number_input("Limite", min_value=20, max_value=200, value=20, step=20)
    sort_stale = fcols2[1].checkbox("Ordenar por ultimo update (mas antiguo primero)", value=False)
    show_raw = fcols2[2].checkbox("Mostrar raw del proyecto", value=False)

    if "page_projects" not in st.session_state:
        st.session_state["page_projects"] = 1
    page_num = int(st.session_state["page_projects"])
    offset = (page_num - 1) * int(limit)

    where = ["1=1"]
    params = {"limit": int(limit), "offset": int(offset)}
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
              WHERE cf->>'name' = 'Cliente' AND COALESCE(cf->>'display_value','') ILIKE :client
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
            where.append("status = :status")
            params["status"] = status_filter

    order_by = "last_status_update_at ASC NULLS LAST" if sort_stale else "name ASC"
    with engine.begin() as conn:
        projects = conn.execute(text(f"""
            SELECT gid, name, owner_name, due_date, calculated_progress,
                   total_tasks, completed_tasks, last_status_update_at, last_activity_at, status, raw_data
            FROM projects
            WHERE {' AND '.join(where)}
            ORDER BY {order_by}
            LIMIT :limit OFFSET :offset
        """), params).mappings().all()

    nav_cols = st.columns([1, 1, 2, 2])
    if nav_cols[0].button("Pagina anterior") and page_num > 1:
        st.session_state["page_projects"] = page_num - 1
        st.rerun()
    nav_cols[1].button("Pagina siguiente", on_click=lambda: st.session_state.__setitem__("page_projects", page_num + 1))
    nav_cols[2].markdown(f"**Pagina:** {page_num}")
    nav_cols[3].markdown(f"**Pagina size:** {int(limit)}")

    header_cols = st.columns([3, 2, 2, 2, 2, 2, 2, 2, 2, 1])
    header_cols[0].markdown("**Proyecto**")
    header_cols[1].markdown("**PMO-ID**")
    header_cols[2].markdown("**Responsable**")
    header_cols[3].markdown("**Sponsor**")
    header_cols[4].markdown("**Prioridad**")
    header_cols[5].markdown("**Cliente**")
    header_cols[6].markdown("**Inicio**")
    header_cols[7].markdown("**Termino Plan**")
    header_cols[8].markdown("**Status / ultimo update**")
    header_cols[9].markdown("**Ver**")

    if "show_project_gid" not in st.session_state:
        st.session_state["show_project_gid"] = None

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

    for p in projects:
        project_raw = (p.get("raw_data") or {}).get("project") if hasattr(p, "get") else None
        cf = _custom_field_map(project_raw) if project_raw else {}
        cols = st.columns([3, 2, 2, 2, 2, 2, 2, 2, 2, 1])
        cols[0].write(p.get("name") or "(sin nombre)")
        cols[1].write(cf.get("PMO ID") or "")
        cols[2].write(cf.get("Responsable Proyecto") or p.get("owner_name") or "")
        cols[3].write(cf.get("Sponsor") or "")
        cols[4].write(cf.get("Priority") or cf.get("Prioridad") or "")
        cols[5].write(cf.get("Cliente") or "")
        cols[6].write(_fmt_date(cf.get("Fecha Inicio del proyecto") or cf.get("Fecha Inicio") or ""))
        cols[7].write(_fmt_date(cf.get("Fecha Planificada Termino del proyecto") or cf.get("Fecha Planificada Termino del proyecto") or ""))
        last_update = _humanize_last_update(p.get("last_status_update_at"))
        status = p.get("status") or ""
        cols[8].write(f"{status} {last_update}".strip())
        if cols[9].button("Ver", key=f"view_{p.get('gid')}"):
            st.session_state["show_project_gid"] = p.get("gid")
            st.session_state["show_project_row"] = p
            _show_project_dialog(p)

    if st.session_state.get("show_project_gid") and st.session_state.get("show_project_row"):
        _show_project_dialog(st.session_state["show_project_row"])

elif page == "Findings":
    st.subheader("Findings")
    if st.button("Enviar findings a Slack"):
        try:
            sent = post_new_findings_to_slack(cfg)
            st.success(f"Mensajes enviados: {sent}")
        except Exception as e:
            st.error(f"Error enviando a Slack: {e}")

    fcols = st.columns(4)
    rule_filter = fcols[0].selectbox("Regla", ["(todas)", "no_status_update", "no_activity", "schedule_risk"])
    severity_filter = fcols[1].selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    status_filter = fcols[2].selectbox("Estado", ["open", "acknowledged", "resolved", "(todas)"])
    sponsor_query = fcols[3].text_input("Sponsor contiene")

    fcols2 = st.columns(3)
    project_query = fcols2[0].text_input("Proyecto contiene")
    limit = fcols2[1].number_input("Limite", min_value=20, max_value=200, value=20, step=20)

    where = ["1=1"]
    params = {"limit": int(limit)}

    if rule_filter != "(todas)":
        where.append("rule_id = :rule_id")
        params["rule_id"] = rule_filter
    if severity_filter != "(todas)":
        where.append("severity = :severity")
        params["severity"] = severity_filter
    if status_filter != "(todas)":
        where.append("status = :status")
        params["status"] = status_filter
    if project_query.strip():
        where.append("(details->>'project_name') ILIKE :pname")
        params["pname"] = f"%{project_query.strip()}%"
    if sponsor_query.strip():
        where.append("(details->>'sponsor') ILIKE :sponsor")
        params["sponsor"] = f"%{sponsor_query.strip()}%"

    q = f"""
        SELECT id, project_gid, rule_id, severity, status, created_at, details
        FROM findings
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).mappings().all()

    fdf = pd.DataFrame([{
        "select": False,
        "id": r.get("id"),
        "project_gid": r.get("project_gid"),
        "rule_id": r.get("rule_id"),
        "severity": r.get("severity"),
        "status": r.get("status"),
        "created_at": r.get("created_at"),
    } for r in rows])

    edited = st.data_editor(
        fdf,
        use_container_width=True,
        height=400,
        column_config={"select": st.column_config.CheckboxColumn("Seleccionar")},
        disabled=["id", "project_gid", "rule_id", "severity", "status", "created_at"],
    )

    selected_ids = edited[edited["select"] == True]["id"].tolist() if not edited.empty else []

    action_cols = st.columns(3)
    if action_cols[0].button("Ver detalle"):
        if not selected_ids:
            st.warning("Selecciona al menos un finding.")
        else:
            rmap = {r.get("id"): r for r in rows}
            sel = rmap.get(selected_ids[0])
            if sel:
                st.json(_jsonable(sel["details"] or {}))

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
            project_gids = list({r.get("project_gid") for r in rows if r.get("id") in selected_ids})
            projects_map = {}
            if project_gids:
                with engine.begin() as conn:
                    proj_rows = conn.execute(text("""
                        SELECT gid, name, status, last_status_update_at, raw_data
                        FROM projects
                        WHERE gid = ANY(:gids)
                    """), {"gids": project_gids}).mappings().all()
                projects_map = {p["gid"]: p for p in proj_rows}

            export_rows = []
            for r in rows:
                if r.get("id") not in selected_ids:
                    continue
                p = projects_map.get(r.get("project_gid")) or {}
                export_rows.append({
                    "PMO-ID": _cf_value_from_project_row(p, "PMO ID"),
                    "Proyecto": p.get("name") or "",
                    "Fecha inicio": _fmt_date(_cf_value_from_project_row(p, "Fecha Inicio del proyecto") or _cf_value_from_project_row(p, "Fecha Inicio")),
                    "Fecha termino": _fmt_date(_cf_value_from_project_row(p, "Fecha Planificada Termino del proyecto") or _cf_value_from_project_row(p, "Fecha Planificada Término del proyecto")),
                    "Status": p.get("status") or "",
                    "Ultima actualizacion": _humanize_last_update(p.get("last_status_update_at")),
                    "Responsable": _cf_value_from_project_row(p, "Responsable Proyecto") or "",
                    "Sponsor": _cf_value_from_project_row(p, "Sponsor") or "",
                })

            csv = pd.DataFrame(export_rows).to_csv(index=False)
            st.download_button(
                "Descargar CSV",
                data=csv,
                file_name="findings.csv",
                mime="text/csv",
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

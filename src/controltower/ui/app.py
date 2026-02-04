import streamlit as st
import json
import os
import pandas as pd
import io
import plotly.express as px
from openpyxl.utils import get_column_letter
from datetime import datetime, timezone, date, timedelta
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

def _fmt_status(val):
    if not val:
        return "-"
    mapping = {
        "on_track": "On track",
        "on_hold": "On hold",
        "off_track": "Off track",
        "at_risk": "At risk",
        "green": "On track",
        "yellow": "At risk",
        "red": "Off track",
        "blue": "On track",
    }
    return mapping.get(str(val), str(val).replace("_", " ").title())

# Sidebar menu only
with st.sidebar:
    st.header("Menu")
    page = st.radio("Ir a", ["Dashboard", "Proyectos", "Findings", "Seguimiento"], label_visibility="collapsed")

if page == "Dashboard":
    st.subheader("Dashboard")
    with engine.begin() as conn:
        counts = conn.execute(text("""
            SELECT
              SUM(CASE WHEN f.status='open' THEN 1 ELSE 0 END) AS open_findings,
              SUM(CASE WHEN f.severity='high' AND f.status='open' THEN 1 ELSE 0 END) AS high_open
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
        """)).mappings().one()
        by_rule = conn.execute(text("""
            SELECT f.rule_id, COUNT(*) AS n
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE f.status='open' AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            GROUP BY f.rule_id
            ORDER BY n DESC
        """)).mappings().all()

        by_sponsor = conn.execute(text("""
            SELECT COALESCE(cf->>'display_value','(sin sponsor)') AS sponsor, COUNT(*) AS n
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            LEFT JOIN LATERAL jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              ON cf->>'name' = 'Sponsor'
            WHERE f.status='open' AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf2
              WHERE cf2->>'name' = 'PMO ID' AND COALESCE(cf2->>'display_value','') <> ''
            )
            GROUP BY sponsor
            ORDER BY n DESC
        """)).mappings().all()

        by_project_status = conn.execute(text("""
            SELECT COALESCE(p.status,'(sin status)') AS project_status, COUNT(*) AS n
            FROM findings f
            JOIN projects p ON p.gid = f.project_gid
            WHERE f.status='open' AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            GROUP BY project_status
            ORDER BY n DESC
        """)).mappings().all()
    c1, c2 = st.columns(2)
    c1.metric("Hallazgos abiertos", counts["open_findings"] or 0)
    c2.metric("Hallazgos alta severidad", counts["high_open"] or 0)
    st.markdown("**Desglose por regla (open)**")
    if by_rule:
        df_rules = pd.DataFrame(by_rule).rename(columns={"rule_id": "Regla", "n": "Cantidad de problemas"})
        st.dataframe(df_rules, use_container_width=False, width=520, height=200, hide_index=True)

    st.markdown("**Distribución por sponsor (open)**")
    if by_sponsor:
        df_sponsor = pd.DataFrame(by_sponsor).rename(columns={"n": "Cantidad"})
        fig_sponsor = px.pie(df_sponsor, values="Cantidad", names="sponsor")
        st.plotly_chart(fig_sponsor, use_container_width=True)

    st.markdown("**Distribución por estado del proyecto (open)**")
    if by_project_status:
        df_status = pd.DataFrame(by_project_status).rename(columns={"n": "Cantidad"})
        fig_status = px.pie(df_status, values="Cantidad", names="project_status")
        st.plotly_chart(fig_status, use_container_width=True)

elif page == "Proyectos":
    st.subheader("Proyectos")
    fcols = st.columns(6)
    project_query = fcols[0].text_input("Proyecto contiene")
    pmo_id_query = fcols[1].text_input("PMO-ID contiene")
    resp_query = fcols[2].text_input("Responsable contiene")
    client_query = fcols[3].text_input("Cliente contiene")
    sponsor_query = fcols[4].text_input("Sponsor contiene", value="Abrigo")
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
    where.append("""
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
          WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
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
        "gid": p.get("gid"),
    } for p in projects])

    edited_p = st.data_editor(
        pdf.drop(columns=["gid"]),
        use_container_width=True,
        height=420,
        column_config={"select": st.column_config.CheckboxColumn("")},
        disabled=["pmo_id", "proyecto", "responsable", "sponsor", "cliente", "inicio", "termino_plan", "status_ultimo"],
        hide_index=True,
    )

    selected_gids = []
    if not edited_p.empty:
        selected_idx = edited_p[edited_p["select"] == True].index.tolist()
        selected_gids = [pdf.loc[i, "gid"] for i in selected_idx]

    p_actions = st.columns(2)
    if p_actions[0].button("Ver detalle"):
        if not selected_gids:
            st.warning("Selecciona al menos un proyecto.")
        else:
            sel_gid = selected_gids[0]
            sel = next((x for x in projects if x.get("gid") == sel_gid), None)
            if sel:
                _show_project_dialog(sel)
    p_actions[1].button("Limpiar selección", on_click=lambda: None)

elif page == "Findings":
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
        rule_ids += ["no_status_update", "no_activity", "schedule_risk"]

    fcols = st.columns(5)
    rule_filter = fcols[0].selectbox("Regla", rule_ids)
    severity_filter = fcols[1].selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    project_status_filter = fcols[2].selectbox("Estado proyecto", ["(todos)", "on_track", "at_risk", "off_track", "on_hold", "none"])
    sponsor_query = fcols[3].text_input("Sponsor contiene", value="Abrigo")
    resp_query = fcols[4].text_input("Responsable contiene")

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
            where.append("p.status = :pstatus")
            params["pstatus"] = project_status_filter
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

    def _rule_message(r, p):
        rule = r.get("rule_id")
        details = r.get("details") or {}
        if rule == "no_status_update":
            days = details.get("days_since_last_status_update", "N/A")
            return f"Actualización de estado con más de {days} días. El JP debe publicar un status update (cada 7 días)."
        if rule == "no_activity":
            return "Sin actividad reciente. El JP debe crear o completar tareas esta semana."
        if rule == "schedule_risk":
            return "Riesgo de calendario. El JP debe ajustar plan o acelerar ejecución para cumplir el progreso esperado."
        return "Revisar el hallazgo y tomar acción correctiva."

    fdf = pd.DataFrame([{
        "select": False,
        "id": r.get("id"),
        "pmo_id": _cf_value_from_project_row(projects_map.get(r.get("project_gid")) or {}, "PMO ID"),
        "proyecto": f"{(projects_map.get(r.get('project_gid')) or {}).get('name') or ''} ({_fmt_status((projects_map.get(r.get('project_gid')) or {}).get('status'))})".strip(),
        "cliente": _cf_value_from_project_row(projects_map.get(r.get("project_gid")) or {}, "cliente_nuevo"),
        "responsable": _cf_value_from_project_row(projects_map.get(r.get("project_gid")) or {}, "Responsable Proyecto"),
        "sponsor": _cf_value_from_project_row(projects_map.get(r.get("project_gid")) or {}, "Sponsor"),
        "regla": r.get("rule_id"),
        "severidad": r.get("severity"),
        "motivo": _rule_message(r, projects_map.get(r.get("project_gid")) or {}),
    } for r in rows])

    edited = st.data_editor(
        fdf,
        use_container_width=True,
        height=400,
        column_config={"select": st.column_config.CheckboxColumn("")},
        disabled=["id", "pmo_id", "proyecto", "cliente", "responsable", "sponsor", "regla", "severidad", "motivo"],
        hide_index=True,
    )
    st.caption(f"Total registros: {len(fdf)}")

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
                    "Severidad": r.get("severity"),
                    "Motivo": _rule_message(r, p),
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
        rule_cols = ["no_status_update", "no_activity", "amount_of_tasks"]

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

elif page == "Seguimiento":
    st.subheader("Seguimiento")
    monitoring_cfg = cfg.get("monitoring", {}) if isinstance(cfg, dict) else {}
    new_days_default = int(monitoring_cfg.get("new_projects_days", 7))
    closing_days_default = int(monitoring_cfg.get("closing_soon_days", 15))

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

    st.markdown(f"**Proyectos por cerrar (atrasados o en los próximos {closing_days} días)**")
    today = date.today()
    end_date = today + timedelta(days=closing_days)
    with engine.begin() as conn:
        closing_projects = conn.execute(text("""
            SELECT gid, name, owner_name, due_date, status, raw_data, last_status_update_at
            FROM projects
            WHERE due_date IS NOT NULL
              AND due_date <= :end_date
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
              )
              AND (:sponsor = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf2
                WHERE cf2->>'name' = 'Sponsor' AND COALESCE(cf2->>'display_value','') ILIKE :sponsor_like
              ))
              AND (:resp = '' OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(raw_data->'project'->'custom_fields') cf3
                WHERE cf3->>'name' = 'Responsable Proyecto' AND COALESCE(cf3->>'display_value','') ILIKE :resp_like
              ))
            ORDER BY due_date ASC
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
            "Sponsor": _cf_value_from_project_row(p, "Sponsor"),
            "Estado": _fmt_status(p.get("status")),
            "Fecha término": _fmt_date(p.get("due_date")),
            "Días a cierre": (p.get("due_date") - today).days if p.get("due_date") else "",
        } for p in closing_projects])
        st.dataframe(df_close, use_container_width=True, height=260, hide_index=True)
        st.caption(f"Total: {len(df_close)}")
    else:
        st.info("No hay proyectos con cierre próximo.")

import streamlit as st
import json
import pandas as pd
from sqlalchemy import text
from controltower.db.connection import get_engine

st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
engine = get_engine()

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

# Sidebar filters
with st.sidebar:
    st.header("Filtros")
    rule_filter = st.selectbox("Regla", ["(todas)", "no_status_update", "no_activity", "schedule_risk"])
    severity_filter = st.selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    status_filter = st.selectbox("Estado", ["open", "acknowledged", "resolved", "(todas)"])
    project_query = st.text_input("Proyecto contiene")
    owner_query = st.text_input("JP / Owner contiene")
    limit = st.number_input("Limite", min_value=20, max_value=200, value=20, step=20)
    show_raw = st.checkbox("Mostrar raw del proyecto", value=False)


tab1, tab2, tab3 = st.tabs(["Dashboard", "Projects", "Findings"])

with tab1:
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

with tab2:
    st.subheader("Proyectos")
    if "page_projects" not in st.session_state:
        st.session_state["page_projects"] = 1
    page = int(st.session_state["page_projects"])
    offset = (page - 1) * int(limit)
    with engine.begin() as conn:
        projects = conn.execute(text("""
            SELECT gid, name, owner_name, due_date, calculated_progress,
                   total_tasks, completed_tasks, last_status_update_at, last_activity_at, status, raw_data
            FROM projects
            ORDER BY name ASC
            LIMIT :limit OFFSET :offset
        """), {"limit": int(limit), "offset": int(offset)}).mappings().all()

    nav_cols = st.columns([1, 1, 2])
    if nav_cols[0].button("Pagina anterior") and page > 1:
        st.session_state["page_projects"] = page - 1
        st.rerun()
    nav_cols[1].button("Pagina siguiente", on_click=lambda: st.session_state.__setitem__("page_projects", page + 1))
    nav_cols[2].markdown(f"**Pagina:** {page}  |  **Pagina size:** {int(limit)}")

    header_cols = st.columns([4, 2, 2, 2, 2, 1])
    header_cols[0].markdown("**Proyecto**")
    header_cols[1].markdown("**Owner**")
    header_cols[2].markdown("**Status**")
    header_cols[3].markdown("**Due date**")
    header_cols[4].markdown("**Ult. actividad**")
    header_cols[5].markdown("**Ver**")

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
        cols = st.columns([4, 2, 2, 2, 2, 1])
        cols[0].write(p.get("name") or "(sin nombre)")
        cols[1].write(p.get("owner_name") or "")
        cols[2].write(p.get("status") or "")
        cols[3].write(p.get("due_date") or "")
        cols[4].write(p.get("last_activity_at") or "")
        if cols[5].button("Ver", key=f"view_{p.get('gid')}"):
            st.session_state["show_project_gid"] = p.get("gid")
            st.session_state["show_project_row"] = p
            _show_project_dialog(p)

    if st.session_state.get("show_project_gid") and st.session_state.get("show_project_row"):
        _show_project_dialog(st.session_state["show_project_row"])

with tab3:
    st.subheader("Hallazgos")
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
    if owner_query.strip():
        where.append("(details->>'owner_name') ILIKE :oname")
        params["oname"] = f"%{owner_query.strip()}%"

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
        "id": r.get("id"),
        "project_gid": r.get("project_gid"),
        "rule_id": r.get("rule_id"),
        "severity": r.get("severity"),
        "status": r.get("status"),
        "created_at": r.get("created_at"),
    } for r in rows])
    st.dataframe(fdf, use_container_width=True, height=400)

    for r in rows:
        details = r["details"] or {}
        title = f"[{r['severity'].upper()}] {details.get('project_name','(sin nombre)')} - {r['rule_id']}"
        with st.expander(title):
            st.json(_jsonable(details))
            ack = st.text_input("Comentario para Acknowledge (obligatorio)", key=f"ack_{r['id']}")
            ack_by = st.text_input("Acknowledged by", key=f"ackby_{r['id']}", value="PMO")
            if st.button("Acknowledge", key=f"btn_{r['id']}"):
                if not ack.strip():
                    st.error("Comentario obligatorio.")
                else:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE findings
                            SET status='acknowledged',
                                acknowledged_at=NOW(),
                                acknowledged_by=:by,
                                ack_comment=:c
                            WHERE id=:id
                        """), {"id": r["id"], "c": ack, "by": ack_by or "PMO"})
                    st.success("Hallazgo acknowledged.")
                    st.rerun()

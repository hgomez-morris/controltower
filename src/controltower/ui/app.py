import streamlit as st
import json
from sqlalchemy import text
from controltower.db.connection import get_engine

st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
engine = get_engine()

st.title("PMO Control Tower - MVP")

def _jsonable(obj):
    return json.loads(json.dumps(obj, default=str))

# Sidebar filters
with st.sidebar:
    st.header("Filtros")
    rule_filter = st.selectbox("Regla", ["(todas)", "no_status_update", "no_activity", "schedule_risk"])
    severity_filter = st.selectbox("Severidad", ["(todas)", "low", "medium", "high"])
    status_filter = st.selectbox("Estado", ["open", "acknowledged", "resolved", "(todas)"])
    project_query = st.text_input("Proyecto contiene")
    owner_query = st.text_input("JP / Owner contiene")
    limit = st.number_input("Limite", min_value=50, max_value=2000, value=500, step=50)


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
    with engine.begin() as conn:
        projects = conn.execute(text("""
            SELECT gid, name, owner_name, due_date, calculated_progress,
                   total_tasks, completed_tasks, last_status_update_at, last_activity_at
            FROM projects
            ORDER BY name ASC
            LIMIT :limit
        """), {"limit": int(limit)}).mappings().all()

    for p in projects:
        title = f"{p['name']}" if p["name"] else "(sin nombre)"
        with st.expander(title):
            st.json(_jsonable(p))

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

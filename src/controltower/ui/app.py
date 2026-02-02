import streamlit as st
from sqlalchemy import text
from controltower.db.connection import get_engine

st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
engine = get_engine()

st.title("PMO Control Tower — MVP")

tab1, tab2 = st.tabs(["Dashboard", "Findings"])

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
    st.subheader("Hallazgos abiertos")
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, project_gid, rule_id, severity, status, created_at, details
            FROM findings
            WHERE status='open'
            ORDER BY created_at DESC
            LIMIT 500
        """)).mappings().all()

    for r in rows:
        details = r["details"] or {}
        with st.expander(f"[{r['severity'].upper()}] {details.get('project_name','(sin nombre)')} — {r['rule_id']}"):
            st.json(details)
            ack = st.text_input("Comentario para Acknowledge (obligatorio)", key=f"ack_{r['id']}")
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
                        """), {"id": r["id"], "c": ack, "by": "PMO"})
                    st.success("Hallazgo acknowledged.")
                    st.rerun()

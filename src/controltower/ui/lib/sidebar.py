from datetime import datetime

import streamlit as st
from sqlalchemy import text

from controltower.ui.lib.context import CHILE_TZ, get_engine_cached


def apply_sidebar_style():
    st.markdown(
        """
<style>
section[data-testid="stSidebar"] > div:first-child {
    display: flex;
    flex-direction: column;
    height: 100%;
}
section[data-testid="stSidebar"] > div:first-child > div {
    flex: 1 1 auto;
}
.sidebar-footer {
    margin-top: auto;
    padding-top: 0.5rem;
    font-size: 0.85rem;
    color: #6c757d;
}
</style>
""",
        unsafe_allow_html=True,
    )


def _get_last_sync_label() -> str:
    engine = get_engine_cached()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT started_at, completed_at, status
                FROM sync_log
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
        ).mappings().first()
    if not row:
        return "Sync: sin registros"
    ts = row.get("completed_at") or row.get("started_at")
    if isinstance(ts, datetime):
        label = ts.astimezone(CHILE_TZ).strftime("%Y-%m-%d %H:%M")
    else:
        label = str(ts)[:16] if ts else ""
    return f"Asana Sync: {label}"


def render_sidebar_footer():
    st.sidebar.caption(_get_last_sync_label())

from datetime import datetime

import streamlit as st

from controltower.ui.lib.common import format_datetime_chile


def render_sidebar_brand():
    with st.sidebar:
        # intentionally left blank (no extra header in ControlTower)
        pass


def _format_sync_minute(value) -> str:
    return format_datetime_chile(value)


def render_last_sync_sidebar(last_sync: dict):
    with st.sidebar:
        if last_sync:
            formatted = _format_sync_minute(last_sync.get("synced_at", ""))
            st.caption(f"Clockify Sync: {formatted}")
        else:
            st.caption("Clockify Sync: sin registros")


def percent_cell_style(value):
    if not isinstance(value, (int, float)):
        return ""

    if value > 110:
        return "background-color: #000000; color: #f9fafb;"

    step_colors = [
        "#ffffff",
        "#eff6ff",
        "#dbeafe",
        "#bfdbfe",
        "#93c5fd",
        "#60a5fa",
        "#3b82f6",
        "#2563eb",
        "#1d4ed8",
        "#1e3a8a",
        "#1e3a8a",
    ]

    bounded = max(0.0, min(100.0, float(value)))
    idx = min(int(bounded // 10), len(step_colors) - 1)
    bg = step_colors[idx]
    text = "#f9fafb" if idx >= 8 else "#111827"
    return f"background-color: {bg}; color: {text};"

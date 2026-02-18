import pandas as pd
import streamlit as st

from controltower.ui.lib.sidebar import apply_sidebar_style, render_sidebar_footer


from controltower.clockify.analytics_db import (
    DEFAULT_WEEKLY_HOURS,
    fetch_kpis,
    fetch_last_sync,
    fetch_week_starts,
    fetch_weekly_hours_by_person,
    fetch_weekly_hours_by_person_project,
    get_conn,
    person_percent_rows,
)
from controltower.clockify.page_user_logic import (
    build_user_detail_df,
    build_user_main_df,
    selected_person_from_index,
)
from controltower.clockify.ui_helpers import percent_cell_style, render_last_sync_sidebar, render_sidebar_brand


def detail_total_row_style(row):
    if str(row.get("Proyecto", "")).strip().upper() != "TOTAL":
        return [""] * len(row)
    return ["background-color: #e5e7eb; color: #111827; font-weight: 700;" for _ in row.index]


def render():
    apply_sidebar_style()
    render_sidebar_footer()
    st.title("Clockify - Por usuarios")
    render_sidebar_brand()

    st.caption("Filtros")
    fcol1, fcol2, fcol3 = st.columns([2, 2, 2])
    weeks_count = fcol1.slider(
        "Semanas a mostrar",
        min_value=4,
        max_value=16,
        value=8,
        key="users_weeks_count",
    )
    weekly_hours = fcol2.slider(
        "Horas por semana (100%)",
        min_value=40,
        max_value=48,
        value=DEFAULT_WEEKLY_HOURS,
        step=1,
        key="users_weekly_hours",
    )
    include_current_week = fcol3.checkbox(
        "Incluir semana actual",
        value=False,
        key="include_current_week_users",
    )

    conn = get_conn()
    try:
        last_sync = fetch_last_sync(conn)
        week_starts = fetch_week_starts(conn, weeks_count, include_current_week=include_current_week)
        df_hours = fetch_weekly_hours_by_person(conn, week_starts)
        rows = person_percent_rows(df_hours, week_starts, weekly_hours)
        kpis = fetch_kpis(conn)
    except Exception as exc:
        st.error(f"Error consultando PostgreSQL: {exc}")
        st.stop()
    finally:
        conn.close()

    render_last_sync_sidebar(last_sync)

    if not week_starts:
        st.warning("No hay semanas en calendar_weeks. Ejecuta una sincronización primero.")
        st.stop()

    st.subheader("Porcentaje de Carga por Persona por Semana")
    st.write(
        f"Semanas visibles: **{week_starts[0]}** (más reciente) a **{week_starts[-1]}** (más antigua). "
        f"Base: **{weekly_hours}h = 100%**."
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Time Entries", f"{kpis['time_entries']:,}")
    col2.metric("Personas", f"{kpis['people']:,}")
    col3.metric("Proyectos", f"{kpis['projects']:,}")

    if not rows:
        st.warning("No hay datos de horas para las semanas seleccionadas.")
        st.stop()

    df_raw, df, percentage_cols = build_user_main_df(rows, week_starts)

    styled_df = df.style.format({col: "{:.1f}%" for col in percentage_cols}).map(
        percent_cell_style, subset=percentage_cols
    )

    selection = st.dataframe(
        styled_df,
        use_container_width=True,
        height=700,
        on_select="rerun",
        selection_mode="single-row",
        key="user_weekly_grid",
    )

    selected_rows = []
    if selection is not None:
        if hasattr(selection, "selection") and hasattr(selection.selection, "rows"):
            selected_rows = list(selection.selection.rows)
        elif isinstance(selection, dict):
            selected_rows = list(selection.get("selection", {}).get("rows", []))
    if not selected_rows:
        return

    selected_idx = int(selected_rows[0])
    if selected_idx < 0 or selected_idx >= len(df):
        return
    selected_person = selected_person_from_index(df_raw, df, selected_idx)

    conn = get_conn()
    try:
        df_user_project_hours = fetch_weekly_hours_by_person_project(conn, week_starts, selected_person)
    except Exception as exc:
        st.error(f"Error consultando detalle por proyecto: {exc}")
        return
    finally:
        conn.close()

    st.subheader(f"Detalle de Imputación por Proyecto - {selected_person}")
    if df_user_project_hours.empty:
        st.info("El usuario seleccionado no tiene imputaciones por proyecto en las semanas visibles.")
        return

    detail_df = build_user_detail_df(df_user_project_hours, week_starts)
    detail_styled_df = detail_df.style.format(
        {"Total período": "{:.2f}", **{week: "{:.2f}" for week in week_starts}}
    ).apply(detail_total_row_style, axis=1)
    row_count = len(detail_df)
    row_px = 35
    header_px = 40
    table_height = max(120, min(700, header_px + row_count * row_px))
    st.dataframe(
        detail_styled_df,
        use_container_width=True,
        height=table_height,
    )

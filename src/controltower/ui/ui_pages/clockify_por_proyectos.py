import pandas as pd
import streamlit as st



from controltower.clockify.analytics_db import (
    extract_pmo_id,
    fetch_asana_hours_by_pmo_ids,
    fetch_kpis,
    fetch_time_entries_count_for_weeks,
    fetch_total_hours_by_project_person,
    fetch_total_hours_by_project,
    fetch_week_starts,
    fetch_weekly_hours_by_project_person,
    fetch_weekly_hours_by_project,
    get_conn,
    project_hours_rows,
)
from controltower.clockify.page_project_logic import (
    build_project_main_df,
    build_project_user_detail_df,
    enrich_project_rows,
    is_closed_project,
)
from controltower.clockify.ui_helpers import render_sidebar_brand


def trend_cell_style(value):
    text = str(value).upper()
    if text.startswith("▼ "):
        return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"
    if text.startswith("■ "):
        return "background-color: #fff7ed; color: #9a3412; font-weight: 700;"
    return "color: #1e3a8a;"


def total_vs_planned_style(row):
    styles = [""] * len(row)
    if "HH clock." not in row.index or "HH plan." not in row.index:
        return styles
    planned = row["HH plan."]
    total = row["HH clock."]
    if isinstance(planned, (int, float)) and isinstance(total, (int, float)) and total > planned:
        total_idx = list(row.index).index("HH clock.")
        styles[total_idx] = "background-color: #fee2e2; color: #991b1b; font-weight: 700;"
    return styles


def closed_project_row_style(row, closed_projects: set[str]):
    is_closed = str(row.get("Proyecto", "")) in closed_projects
    if not is_closed:
        return [""] * len(row)
    return ["background-color: #f3f4f6; color: #374151;" for _ in row.index]


def detail_total_row_style(row):
    if str(row.get("Usuario", "")).strip().upper() != "TOTAL":
        return [""] * len(row)
    return ["background-color: #e5e7eb; color: #111827; font-weight: 700;" for _ in row.index]


def render():
    st.title("Clockify - Por proyectos")
    render_sidebar_brand()

    st.caption("Filtros")
    fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns([2, 2, 2, 2, 2])
    weeks_count = fcol1.slider(
        "Semanas a mostrar",
        min_value=4,
        max_value=16,
        value=8,
        key="projects_weeks_count",
    )
    include_current_week = fcol2.checkbox(
        "Incluir semana actual",
        value=False,
        key="include_current_week",
    )
    only_pmo = fcol3.checkbox("Solo PMO", value=True, key="only_pmo")
    add_total_project = fcol4.checkbox("Agregar total proyecto", value=True, key="add_total_project")
    show_closed_projects = fcol5.checkbox(
        "Mostrar proyectos terminados",
        value=False,
        key="show_closed_projects",
    )

    conn = get_conn()
    try:
        week_starts = fetch_week_starts(conn, weeks_count, include_current_week=include_current_week)
        df_hours = fetch_weekly_hours_by_project(conn, week_starts)
        period_time_entries = fetch_time_entries_count_for_weeks(conn, week_starts)
        total_hours_by_project = fetch_total_hours_by_project(conn) if add_total_project else None
        rows = project_hours_rows(df_hours, week_starts, total_hours_by_project=total_hours_by_project)
        kpis = fetch_kpis(conn)
    except Exception as exc:
        st.error(f"Error consultando PostgreSQL: {exc}")
        st.stop()
    finally:
        conn.close()

    if not week_starts:
        st.warning("No hay semanas en calendar_weeks. Ejecuta una sincronización primero.")
        st.stop()

    st.subheader("Clockify - Horas por Proyecto por Semana")
    st.write(
        f"Semanas visibles: **{week_starts[0]}** (más reciente) a **{week_starts[-1]}** (más antigua)."
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Time Entries", f"{period_time_entries:,}")
    col2.metric("Personas", f"{kpis['people']:,}")
    col3.metric("Proyectos", f"{kpis['projects']:,}")
    st.caption("Horas se consolidan en perídos de lunes a domingo.")

    if not rows:
        st.warning("No hay datos de horas para las semanas seleccionadas.")
        st.stop()

    if only_pmo:
        rows = [row for row in rows if extract_pmo_id(str(row.get("Proyecto", "")))]
        if not rows:
            st.warning("No hay proyectos con PMO-ID para las semanas seleccionadas.")
            st.stop()

    pmo_ids = [extract_pmo_id(str(row.get("Proyecto", ""))) for row in rows]
    asana_by_pmo = fetch_asana_hours_by_pmo_ids([p for p in pmo_ids if p])
    if pmo_ids and not asana_by_pmo:
        st.warning(
            "No se obtuvieron datos de Asana para el cruce PMO-ID. "
            "Revisa conexión PostgreSQL (DB_*) y driver psycopg/psycopg2 en el entorno de Streamlit."
        )
    rows, closed_projects = enrich_project_rows(rows, asana_by_pmo, extract_pmo_id)

    if not show_closed_projects:
        rows = [row for row in rows if str(row.get("Proyecto", "")) not in closed_projects]
        if not rows:
            st.warning("No hay proyectos activos para los filtros seleccionados.")
            st.stop()

    df = build_project_main_df(rows, week_starts, add_total_project=add_total_project)

    numeric_cols = [
        col
        for col in df.columns
        if col
        not in (
            "Proyecto",
            "Responsable",
            "Tendencia acumulada",
            "HH plan.",
            "% cump.",
        )
    ]
    format_map = {col: "{:.2f}" for col in numeric_cols}
    format_map["HH plan."] = lambda v: "NA" if pd.isna(v) else f"{float(v):.2f}"
    format_map["% cump."] = lambda v: "NA" if pd.isna(v) else f"{float(v):.1f}%"
    styled_df = (
        df.style.format(format_map)
        .map(trend_cell_style, subset=["Tendencia acumulada"])
        .apply(total_vs_planned_style, axis=1)
        .apply(lambda row: closed_project_row_style(row, closed_projects), axis=1)
    )

    selection = st.dataframe(
        styled_df,
        use_container_width=True,
        height=700,
        column_config={
            "Proyecto": st.column_config.TextColumn(width="medium"),
            "Responsable": st.column_config.TextColumn(width="small"),
        },
        on_select="rerun",
        selection_mode="single-row",
        key="project_weekly_grid",
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
    selected_project = str(df.iloc[selected_idx]["Proyecto"])

    conn = get_conn()
    try:
        df_project_user_hours = fetch_weekly_hours_by_project_person(conn, week_starts, selected_project)
        total_hours_by_user = fetch_total_hours_by_project_person(conn, selected_project)
    except Exception as exc:
        st.error(f"Error consultando detalle por usuario: {exc}")
        return
    finally:
        conn.close()

    st.subheader(f"Detalle de Carga por Usuario - {selected_project}")
    if df_project_user_hours.empty:
        st.info("El proyecto seleccionado no tiene imputaciones por usuario en las semanas visibles.")
        return

    detail_df = build_project_user_detail_df(df_project_user_hours, total_hours_by_user, week_starts)
    detail_styled_df = detail_df.style.format(
        {"Total": "{:.2f}", "Total período": "{:.2f}", **{week: "{:.2f}" for week in week_starts}}
    ).apply(detail_total_row_style, axis=1)
    row_count = len(detail_df)
    row_px = 35
    header_px = 40
    table_height = max(120, min(700, header_px + row_count * row_px))
    st.dataframe(detail_styled_df, use_container_width=True, height=table_height)

from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from sqlalchemy import text

from controltower.ui.lib.common import _fmt_status, format_datetime_chile
from controltower.ui.lib.context import CHILE_TZ, get_engine_cached


def render():
    engine = get_engine_cached()

    with engine.begin() as conn:
        last_score = conn.execute(
            text(
                """
                SELECT MAX(scoring_date) AS last_scoring_date
                FROM ml.ml_project_scores
                """
            )
        ).mappings().first()

    title_cols = st.columns([3, 2])
    title_cols[0].title("Predicción de Riesgo")
    last_date = last_score.get("last_scoring_date") if last_score else None
    if last_date:
        title_cols[1].caption(f"Último scoring date: {format_datetime_chile(last_date)}")
    st.caption("Se usan las últimas k semanas completas (se excluye la semana en curso).")

    fcols = st.columns(3)
    k_filter = fcols[0].selectbox("Ventana (k semanas)", [2, 3, 4], index=0)

    today_chile = datetime.now(CHILE_TZ).date()

    with engine.begin() as conn:
        latest_score = conn.execute(
            text(
                """
                SELECT MAX(scoring_date) AS last_scoring_date
                FROM ml.ml_project_scores
                WHERE k = :k
                """
            ),
            {"k": k_filter},
        ).mappings().first()
        last_scoring_date = latest_score.get("last_scoring_date") if latest_score else None

        filter_rows = conn.execute(
            text(
                """
                SELECT DISTINCT p.responsable_proyecto AS responsable, p.sponsor
                FROM ml.ml_project_scores s
                JOIN projects p ON p.pmo_id = s.pmo_id
                WHERE s.k = :k
                  AND (:last_score IS NULL OR s.scoring_date = :last_score)
                  AND (
                    COALESCE(p.planned_end_date, p.due_date) IS NULL
                    OR COALESCE(p.planned_end_date, p.due_date) >= :today
                  )
                """
            ),
            {"k": k_filter, "today": today_chile, "last_score": last_scoring_date},
        ).mappings().all()
    responsables = sorted({(r.get("responsable") or "").strip() for r in filter_rows if r.get("responsable")})
    sponsors = sorted({(r.get("sponsor") or "").strip() for r in filter_rows if r.get("sponsor")})

    resp_options = ["(todos)"] + responsables
    sponsor_options = ["(todos)"] + sponsors

    responsable_filter = fcols[1].selectbox("Responsable", resp_options, index=0)
    sponsor_filter = fcols[2].selectbox("Sponsor", sponsor_options, index=0)

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT s.pmo_id, s.k, s.probability, s.scoring_date,
                       p.name, p.responsable_proyecto, p.status, p.sponsor
                FROM ml.ml_project_scores s
                JOIN projects p ON p.pmo_id = s.pmo_id
                WHERE s.k = :k
                  AND (:last_score IS NULL OR s.scoring_date = :last_score)
                  AND p.completed_flag = false
                  AND COALESCE(upper(p.fase_proyecto), '') NOT IN ('TERMINADO','CANCELADO')
                  AND (
                    COALESCE(p.planned_end_date, p.due_date) IS NULL
                    OR COALESCE(p.planned_end_date, p.due_date) >= :today
                  )
                  AND (:resp = '' OR p.responsable_proyecto ILIKE :resp_like)
                  AND (:sponsor = '' OR p.sponsor ILIKE :sponsor_like)
                ORDER BY s.probability DESC
                LIMIT 20
                """
            ),
            {
                "k": k_filter,
                "today": today_chile,
                "last_score": last_scoring_date,
                "resp": "" if responsable_filter == "(todos)" else responsable_filter.strip(),
                "resp_like": f"%{'' if responsable_filter == '(todos)' else responsable_filter.strip()}%",
                "sponsor": "" if sponsor_filter == "(todos)" else sponsor_filter.strip(),
                "sponsor_like": f"%{'' if sponsor_filter == '(todos)' else sponsor_filter.strip()}%",
            },
        ).mappings().all()

        features = conn.execute(
            text(
                """
                SELECT pmo_id, k, ratio_burn, slope_hours_week, volatility_hours,
                       active_people_k, jp_active_projects_k
                FROM ml.ml_project_features
                WHERE k = :k
                """
            ),
            {"k": k_filter},
        ).mappings().all()

    if not rows:
        st.info("No hay proyectos para los filtros seleccionados.")
        return

    df = pd.DataFrame(rows)
    feat_df = pd.DataFrame(features)
    if not feat_df.empty:
        df = df.merge(feat_df, on=["pmo_id", "k"], how="left")

    df["Estado"] = df["status"].apply(_fmt_status)
    df["Probabilidad"] = (df["probability"] * 100).round(1)

    df = df.rename(
        columns={
            "pmo_id": "PMO-ID",
            "name": "Proyecto",
            "responsable_proyecto": "Responsable",
            "sponsor": "Sponsor",
            "ratio_burn": "Ratio burn",
            "slope_hours_week": "Pendiente horas",
            "volatility_hours": "Volatilidad",
            "active_people_k": "Personas activas",
            "jp_active_projects_k": "Promedio proyectos activos JP (k)",
        }
    )
    display_cols = [
        "PMO-ID",
        "Proyecto",
        "Responsable",
        "Sponsor",
        "Estado",
        "Probabilidad",
        "Ratio burn",
        "Pendiente horas",
        "Volatilidad",
        "Personas activas",
        "Promedio proyectos activos JP (k)",
    ]
    df = df[[c for c in display_cols if c in df.columns]]

    selection = st.dataframe(
        df,
        use_container_width=True,
        height=500,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="ml_risk_grid",
        column_config={
            "Probabilidad": st.column_config.NumberColumn(format="%.1f %%"),
            "Ratio burn": st.column_config.NumberColumn(format="%.2f"),
            "Pendiente horas": st.column_config.NumberColumn(format="%.2f"),
            "Volatilidad": st.column_config.NumberColumn(format="%.2f"),
            "Promedio proyectos activos JP (k)": st.column_config.NumberColumn(format="%.1f"),
        },
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

    selected_pmo = df.iloc[selected_idx]["PMO-ID"]
    st.markdown("**Detalle del proyecto seleccionado**")
    with engine.begin() as conn:
        project_row = conn.execute(
            text(
                """
                SELECT p.pmo_id, p.name, p.start_date, p.planned_end_date, p.planned_hours_total
                FROM projects p
                WHERE p.pmo_id = :pmo
                LIMIT 1
                """
            ),
            {"pmo": selected_pmo},
        ).mappings().first()

        weekly_rows = conn.execute(
            text(
                """
                SELECT week_start, hours_week, active_users_week
                FROM ml.weekly_fact
                WHERE pmo_id = :pmo
                  AND week_start < date_trunc('week', NOW())::date
                ORDER BY week_start
                """
            ),
            {"pmo": selected_pmo},
        ).mappings().all()

        clockify_project_id = conn.execute(
            text(
                """
                SELECT clockify_project_id
                FROM ml.project_id_map
                WHERE pmo_id = :pmo
                LIMIT 1
                """
            ),
            {"pmo": selected_pmo},
        ).scalar()

        totals = None
        by_person = []
        if clockify_project_id:
            totals = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(te.hours), 0) AS total_hours,
                           COUNT(DISTINCT te.person_id) AS people_count
                    FROM clockify.time_entries te
                    WHERE te.project_id = :pid
                    """
                ),
                {"pid": clockify_project_id},
            ).mappings().first()

            by_person = conn.execute(
                text(
                    """
                    SELECT pe.name AS persona, cw.week_start, SUM(te.hours) AS horas
                    FROM clockify.time_entries te
                    JOIN clockify.people pe ON pe.person_id = te.person_id
                    JOIN clockify.calendar_weeks cw
                      ON te.entry_date BETWEEN cw.week_start AND cw.week_end
                    WHERE te.project_id = :pid
                      AND cw.week_start < date_trunc('week', NOW())::date
                    GROUP BY pe.name, cw.week_start
                    ORDER BY pe.name, cw.week_start
                    """
                ),
                {"pid": clockify_project_id},
            ).mappings().all()

    if project_row:
        meta_cols = st.columns(4)
        meta_cols[0].metric("Proyecto", project_row.get("name") or "")
        start_val = project_row.get("start_date")
        end_val = project_row.get("planned_end_date")
        meta_cols[1].metric("Inicio", start_val.isoformat() if hasattr(start_val, "isoformat") else (start_val or "-"))
        meta_cols[2].metric("Término planificado", end_val.isoformat() if hasattr(end_val, "isoformat") else (end_val or "-"))
        planned_val = project_row.get("planned_hours_total")
        if planned_val is None:
            planned_label = "-"
        else:
            try:
                planned_label = f"{float(planned_val):.2f}"
            except Exception:
                planned_label = str(planned_val)
        meta_cols[3].metric("Horas planificadas", planned_label)

    ratio_detail = None
    slope_detail = None
    volatility_detail = None
    active_people_detail = None
    expected_hours_week = None
    hours_k_total = None
    hours_k = []
    users_k = []

    if project_row and weekly_rows:
        start_date = project_row.get("start_date")
        end_date = project_row.get("planned_end_date")
        planned_hours_total = project_row.get("planned_hours_total")

        try:
            planned_hours_total = float(planned_hours_total) if planned_hours_total is not None else None
        except Exception:
            planned_hours_total = None

        if start_date and end_date and planned_hours_total:
            duration_days = (end_date - start_date).days
            duration_weeks = max(1, int((duration_days + 6) // 7))
            expected_hours_week = planned_hours_total / float(duration_weeks)

        hours_series = [float(r.get("hours_week") or 0.0) for r in weekly_rows]
        users_series = [int(r.get("active_users_week") or 0) for r in weekly_rows]
        hours_k = hours_series[-k_filter:]
        users_k = users_series[-k_filter:]
        hours_k_total = sum(hours_k) if hours_k else 0.0

        if expected_hours_week:
            ratio_detail = hours_k_total / (k_filter * expected_hours_week)
        if len(hours_k) >= 2:
            x = pd.Series(range(len(hours_k)))
            slope_detail = float((x * pd.Series(hours_k)).cov(x) / x.var()) if x.var() else 0.0
        if hours_k:
            mean_k = sum(hours_k) / len(hours_k)
            variance = sum((v - mean_k) ** 2 for v in hours_k) / len(hours_k)
            volatility_detail = (variance ** 0.5) / mean_k if mean_k else 0.0
        if users_k:
            active_people_detail = max(users_k)

    if totals:
        tcols = st.columns(2)
        tcols[0].metric("Horas imputadas (Clockify)", f"{float(totals.get('total_hours') or 0):.2f}")
        tcols[1].metric("Personas imputando", f"{int(totals.get('people_count') or 0)}")

    selected_row = None
    if selected_pmo in df["PMO-ID"].values:
        selected_row = df[df["PMO-ID"] == selected_pmo].iloc[0]
    ratio_grid = selected_row.get("Ratio burn") if selected_row is not None else None
    slope_grid = selected_row.get("Pendiente horas") if selected_row is not None else None
    volatility_grid = selected_row.get("Volatilidad") if selected_row is not None else None
    people_grid = selected_row.get("Personas activas") if selected_row is not None else None
    jp_projects_grid = selected_row.get("Promedio proyectos activos JP (k)") if selected_row is not None else None

    if hours_k and expected_hours_week:
        st.markdown("**Fórmulas con datos reales**")
        st.write(
            f"Ratio burn = horas reales acumuladas / (k * horas esperadas por semana) = "
            f"{hours_k_total:.2f} / ({k_filter} * {expected_hours_week:.2f}) = "
            f"{(float(ratio_grid) if ratio_grid is not None else ratio_detail):.2f}"
        )
        if slope_detail is not None:
            st.write(
                f"Pendiente horas = tendencia (regresión lineal) de horas por semana = "
                f"{(float(slope_grid) if slope_grid is not None else slope_detail):.2f} "
                f"(horas: {', '.join(f'{v:.2f}' for v in hours_k)})"
            )
        if volatility_detail is not None:
            st.write(
                f"Volatilidad = desviación estándar / media = "
                f"{(float(volatility_grid) if volatility_grid is not None else volatility_detail):.2f} "
                f"(media: {sum(hours_k) / len(hours_k):.2f})"
            )
        if active_people_detail is not None:
            st.write(
                f"Personas activas = máximo de personas imputando en las últimas k semanas = "
                f"max({', '.join(str(v) for v in users_k)}) = "
                f"{int(people_grid) if people_grid is not None else active_people_detail}"
            )
        st.write(
            "Promedio proyectos activos JP (k) = promedio de proyectos activos del JP en las últimas k semanas "
            "(según fechas de inicio y término planificado, no por horas imputadas)."
        )
        st.write(
            "La probabilidad es la salida del modelo de clasificación entrenado con proyectos cerrados. "
            "Variables consideradas: ratio_burn, pendiente horas, volatilidad, personas activas, "
            "crecimiento de personas, concentración de horas (top1), proyectos activos del JP y horas planificadas (log)."
        )
        st.write(
            "k = número de semanas completas más recientes usadas para calcular las variables y el riesgo. "
            "Se excluye la semana en curso."
        )
        st.write(
            "Parámetros de entrenamiento actuales: k en {2, 3, 4}, "
            "recalc-weeks=9, weekly-capacity-hours=45, "
            "split temporal por fecha de cierre (último mes como test), "
            "baseline cuando no hay suficientes datos."
        )

    if by_person:
        df_person = pd.DataFrame(by_person)
        df_person["horas"] = df_person["horas"].apply(lambda v: float(v) if v is not None else 0.0)
        df_person["week_start"] = pd.to_datetime(df_person["week_start"]).dt.date
        pivot = (
            df_person.pivot_table(
                index="persona",
                columns="week_start",
                values="horas",
                aggfunc="sum",
                fill_value=0.0,
            )
            .sort_index(axis=1)
            .reset_index()
        )
        if not pivot.empty:
            week_cols = [c for c in pivot.columns if c != "persona"]
            pivot["Total"] = pivot[week_cols].sum(axis=1)
            if week_cols:
                week_cols_sorted = sorted(week_cols, reverse=True)
                highlight_weeks = set(week_cols_sorted[:k_filter])
            else:
                highlight_weeks = set()

            # Convert date column names to strings for Streamlit column_config compatibility
            rename_map = {}
            for col in week_cols:
                if hasattr(col, "strftime"):
                    rename_map[col] = col.strftime("%Y-%m-%d")
                else:
                    rename_map[col] = str(col)
            pivot = pivot.rename(columns=rename_map)

            highlight_weeks_str = {rename_map[w] for w in highlight_weeks if w in rename_map}

            def _highlight_k(col):
                if col.name == "persona" or col.name == "Total":
                    return [""] * len(col)
                if col.name in highlight_weeks_str:
                    return ["background-color: #fbe4d5"] * len(col)
                return [""] * len(col)

            ordered_cols = ["persona", "Total"] + [rename_map[c] for c in week_cols_sorted]
            ordered_cols = [c for c in ordered_cols if c in pivot.columns]
            pivot = pivot[ordered_cols]

            number_cols = [c for c in pivot.columns if c != "persona"]
            styled = (
                pivot.style.apply(_highlight_k, axis=0)
                .format({col: "{:.1f}" for col in number_cols})
            )
            st.markdown("**Horas imputadas por persona (todas las semanas)**")
            st.dataframe(
                styled,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "persona": st.column_config.TextColumn("Persona"),
                    "Total": st.column_config.NumberColumn(format="%.1f"),
                    **{rename_map[c]: st.column_config.NumberColumn(format="%.1f") for c in week_cols},
                },
            )

    # Charts
    if weekly_rows:
        weekly_df = pd.DataFrame(weekly_rows)
        weekly_df["week_start"] = pd.to_datetime(weekly_df["week_start"]).dt.date
        weekly_df = weekly_df.sort_values("week_start")
        weekly_df["hours_week"] = weekly_df["hours_week"].apply(lambda v: float(v) if v is not None else 0.0)
        weekly_df["active_users_week"] = weekly_df["active_users_week"].apply(lambda v: int(v) if v is not None else 0)

        weeks = list(weekly_df["week_start"])
        hours = list(weekly_df["hours_week"])
        cumulative_actual = pd.Series(hours).cumsum()
        if expected_hours_week:
            cumulative_planned = [expected_hours_week * (i + 1) for i in range(len(weeks))]
        else:
            cumulative_planned = None

        fig_burndown = go.Figure()
        fig_burndown.add_trace(go.Scatter(x=weeks, y=cumulative_actual, mode="lines+markers", name="Horas reales acumuladas"))
        if cumulative_planned:
            fig_burndown.add_trace(go.Scatter(x=weeks, y=cumulative_planned, mode="lines", name="Horas planificadas acumuladas"))
        fig_burndown.update_layout(title="Burndown: real vs plan", xaxis_title="Semana", yaxis_title="Horas")

        fig_weekly = go.Figure()
        fig_weekly.add_trace(go.Bar(x=weeks, y=hours, name="Horas reales"))
        if expected_hours_week:
            fig_weekly.add_trace(go.Scatter(x=weeks, y=[expected_hours_week] * len(weeks), mode="lines", name="Horas esperadas"))
        fig_weekly.update_layout(title="Horas semanales vs esperado", xaxis_title="Semana", yaxis_title="Horas")

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=weeks, y=hours, mode="lines+markers", name="Horas semanales"))
        if len(hours) >= 2:
            x_idx = np.arange(len(hours))
            slope, intercept = np.polyfit(x_idx, hours, 1)
            trend = [slope * i + intercept for i in x_idx]
            fig_trend.add_trace(go.Scatter(x=weeks, y=trend, mode="lines", name="Tendencia"))
        fig_trend.update_layout(title="Tendencia de horas", xaxis_title="Semana", yaxis_title="Horas")

        fig_users = px.line(
            weekly_df,
            x="week_start",
            y="active_users_week",
            title="Personas activas por semana",
            labels={"week_start": "Semana", "active_users_week": "Personas"},
        )

        scatter_df = None
        with engine.begin() as conn:
            scatter_df = conn.execute(
                text(
                    """
                    WITH latest AS (
                      SELECT MAX(scoring_date) AS scoring_date
                      FROM ml.ml_project_scores
                      WHERE k = :k
                    )
                    SELECT f.pmo_id, f.ratio_burn, f.volatility_hours, s.probability
                    FROM ml.ml_project_features f
                    JOIN latest l ON true
                    LEFT JOIN ml.ml_project_scores s
                      ON s.pmo_id = f.pmo_id AND s.k = :k AND s.scoring_date = l.scoring_date
                    WHERE f.k = :k
                    """
                ),
                {"k": k_filter},
            ).mappings().all()
        scatter_df = pd.DataFrame(scatter_df) if scatter_df else pd.DataFrame()
        fig_scatter = None
        if not scatter_df.empty:
            scatter_df["probability"] = scatter_df["probability"].fillna(0.0)
            scatter_df["Risk Band"] = scatter_df["probability"].apply(
                lambda v: "Riesgo > 80%" if float(v) >= 0.8 else "Riesgo <= 80%"
            )
            scatter_df.loc[scatter_df["pmo_id"] == selected_pmo, "Risk Band"] = "Proyecto seleccionado"
            fig_scatter = px.scatter(
                scatter_df,
                x="volatility_hours",
                y="ratio_burn",
                color="Risk Band",
                title="Volatilidad vs ratio burn",
                labels={"volatility_hours": "Volatilidad", "ratio_burn": "Ratio burn"},
                color_discrete_map={
                    "Proyecto seleccionado": "#dc2626",
                    "Riesgo > 80%": "#2563eb",
                    "Riesgo <= 80%": "#94a3b8",
                },
            )
            fig_scatter.update_traces(marker=dict(size=9, opacity=0.9))
            fig_scatter.update_layout(legend_title_text="Grupo")

        charts = [fig_burndown, fig_weekly, fig_trend, fig_users]
        if fig_scatter is not None:
            charts.append(fig_scatter)
        jp_projects_value = None
        jp_name_selected = None
        if selected_pmo in df["PMO-ID"].values:
            row = df[df["PMO-ID"] == selected_pmo].iloc[0]
            jp_projects_value = row.get("Promedio proyectos activos JP (k)")
            jp_name_selected = row.get("Responsable")

        fig_jp_projects = None
        with engine.begin() as conn:
            jp_rows = conn.execute(
                text(
                    """
                    SELECT p.responsable_proyecto AS responsable,
                           AVG(f.jp_active_projects_k) AS avg_projects
                    FROM ml.ml_project_features f
                    JOIN projects p ON p.pmo_id = f.pmo_id
                    WHERE f.k = :k
                      AND p.completed_flag = false
                      AND COALESCE(upper(p.fase_proyecto), '') NOT IN ('TERMINADO','CANCELADO')
                    GROUP BY p.responsable_proyecto
                    """
                ),
                {"k": k_filter},
            ).mappings().all()

        if jp_rows:
            jp_df = pd.DataFrame(jp_rows)
            jp_df["avg_projects"] = jp_df["avg_projects"].apply(lambda v: float(v) if v is not None else 0.0)
            jp_df["Grupo"] = "Otros JP"
            if jp_name_selected:
                jp_df.loc[jp_df["responsable"] == jp_name_selected, "Grupo"] = "JP seleccionado"
            jp_df = jp_df.sort_values("avg_projects", ascending=False)
            fig_jp_projects = px.bar(
                jp_df,
                x="responsable",
                y="avg_projects",
                color="Grupo",
                color_discrete_map={"JP seleccionado": "#dc2626", "Otros JP": "#94a3b8"},
                title="Carga del JP vs resto (promedio proyectos activos)",
                labels={"responsable": "Responsable", "avg_projects": "Promedio proyectos activos"},
            )
            fig_jp_projects.update_layout(xaxis_tickangle=-45)
            charts.append(fig_jp_projects)

        chart_cols = st.columns(3)
        for i, fig in enumerate(charts):
            chart_cols[i % 3].plotly_chart(fig, use_container_width=True)

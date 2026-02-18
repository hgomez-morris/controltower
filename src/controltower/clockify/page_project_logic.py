from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


def is_closed_project(status, phase, completed) -> bool:
    status_value = str(status or "").strip().lower()
    phase_value = str(phase or "").strip().lower()
    completed_value = bool(completed)
    return completed_value or status_value == "completed" or phase_value in {"cancelado", "terminado"}


def enrich_project_rows(rows: list[dict], asana_by_pmo: Dict[str, Dict], extract_pmo_id_fn) -> Tuple[list[dict], set[str]]:
    closed_projects: set[str] = set()
    for row in rows:
        pmo_id = extract_pmo_id_fn(str(row.get("Proyecto", "")))
        asana_data = asana_by_pmo.get(pmo_id or "", {})
        planned = asana_data.get("horas_planificadas")
        row["Responsable"] = asana_data.get("responsable_proyecto") or "NA"
        row["Horas planificadas"] = round(float(planned), 2) if planned is not None else float("nan")

        closed = is_closed_project(
            asana_data.get("status"),
            asana_data.get("fase_proyecto"),
            asana_data.get("completed", False),
        )
        if closed:
            closed_projects.add(str(row.get("Proyecto", "")))

        total_project = row.get("Total proyecto")
        if isinstance(total_project, (int, float)) and isinstance(row["Horas planificadas"], (int, float)):
            planned_hours = float(row["Horas planificadas"])
            row["Imputación total (%)"] = (
                round((float(total_project) / planned_hours) * 100.0, 1)
                if planned_hours > 0
                else float("nan")
            )
        else:
            row["Imputación total (%)"] = float("nan")

    return rows, closed_projects


def build_project_main_df(rows: list[dict], week_starts: List[str], add_total_project: bool) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    ordered_cols = ["Proyecto", "Responsable", "Tendencia acumulada", "Horas planificadas"]
    if add_total_project:
        ordered_cols.extend(["Total proyecto", "Imputación total (%)"])
    ordered_cols.append("Total (semanas visibles)")
    ordered_cols.extend(week_starts)
    df = df[ordered_cols]

    week_display_map = {
        week: week[5:] if isinstance(week, str) and len(week) >= 10 else week for week in week_starts
    }
    rename_map = {
        "Horas planificadas": "HH plan.",
        "Total proyecto": "HH clock.",
        "Imputación total (%)": "% cump.",
        "Total (semanas visibles)": "HH período",
        **week_display_map,
    }
    df = df.rename(columns=rename_map)

    for metric_col in ("HH plan.", "% cump."):
        if metric_col in df.columns:
            df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
    if "% cump." in df.columns:
        df = df.sort_values(by="% cump.", ascending=False, na_position="last", kind="mergesort")
    return df


def build_project_user_detail_df(
    df_project_user_hours: pd.DataFrame,
    total_hours_by_user: Dict[str, float],
    week_starts: List[str],
) -> pd.DataFrame:
    df_project_user_hours = df_project_user_hours.copy()
    df_project_user_hours["week_start"] = df_project_user_hours["week_start"].astype(str)
    pivot = (
        df_project_user_hours.pivot(index="person_name", columns="week_start", values="total_hours")
        .fillna(0.0)
        .astype(float)
    )
    for week in week_starts:
        if week not in pivot.columns:
            pivot[week] = 0.0
    pivot = pivot[week_starts]

    detail_df = pivot.reset_index().rename(columns={"person_name": "Usuario"})
    detail_df["Total"] = detail_df["Usuario"].map(lambda u: float(total_hours_by_user.get(str(u), 0.0)))
    detail_df["Total período"] = detail_df[week_starts].sum(axis=1).round(2)

    total_row = {
        "Usuario": "TOTAL",
        "Total": round(float(detail_df["Total"].sum()), 2),
        "Total período": round(float(detail_df["Total período"].sum()), 2),
    }
    for week in week_starts:
        total_row[week] = round(float(detail_df[week].sum()), 2)

    detail_df = pd.concat([detail_df, pd.DataFrame([total_row])], ignore_index=True)
    ordered_detail_cols = ["Usuario", "Total", "Total período"] + week_starts
    return detail_df[ordered_detail_cols]

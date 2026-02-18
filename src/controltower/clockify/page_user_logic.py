from __future__ import annotations

from typing import List, Tuple

import pandas as pd


def normalize_person_display(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return raw
    parts = [p.strip() for p in raw.split("-", 1)]
    if len(parts) != 2:
        return raw
    left, right = parts
    if left.isdigit() and right:
        return f"{right} - {left}"
    if right.isdigit() and left:
        return f"{left} - {right}"
    return raw


def build_user_main_df(rows: list[dict], week_starts: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    df_raw = pd.DataFrame(rows)
    df = df_raw.copy()
    df["Persona"] = df["Persona"].map(normalize_person_display)
    df = df.sort_values(by="Persona", ascending=True, kind="mergesort")
    ordered_cols = ["Persona", "Tendencia"] + week_starts + ["Promedio"]
    df = df[ordered_cols]
    percentage_cols = week_starts + ["Promedio"]
    return df_raw, df, percentage_cols


def selected_person_from_index(df_raw: pd.DataFrame, df_display: pd.DataFrame, selected_idx: int) -> str:
    return str(df_raw.loc[df_display.index[selected_idx], "Persona"])


def build_user_detail_df(df_user_project_hours: pd.DataFrame, week_starts: List[str]) -> pd.DataFrame:
    df_user_project_hours = df_user_project_hours.copy()
    df_user_project_hours["week_start"] = df_user_project_hours["week_start"].astype(str)
    pivot = (
        df_user_project_hours.pivot(index="project_name", columns="week_start", values="total_hours")
        .fillna(0.0)
        .astype(float)
    )
    for week in week_starts:
        if week not in pivot.columns:
            pivot[week] = 0.0
    pivot = pivot[week_starts]
    detail_df = pivot.reset_index().rename(columns={"project_name": "Proyecto"})
    detail_df["Total período"] = detail_df[week_starts].sum(axis=1).round(2)

    total_row = {"Proyecto": "TOTAL", "Total período": round(float(detail_df["Total período"].sum()), 2)}
    for week in week_starts:
        total_row[week] = round(float(detail_df[week].sum()), 2)

    detail_df = pd.concat([detail_df, pd.DataFrame([total_row])], ignore_index=True)
    ordered_cols_detail = ["Proyecto", "Total período"] + week_starts
    return detail_df[ordered_cols_detail]

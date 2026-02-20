#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


PMO_REGEX = re.compile(r"(PMO-\d+)", re.IGNORECASE)

CATEGORICAL_COLS = [
    "business_vertical",
    "tipo_proyecto",
    "clasificacion",
    "segmento_empresa",
    "pais",
    "cliente_nuevo",
    "sponsor",
]


def _db_url_from_env() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    name = os.getenv("DB_NAME", "controltower")
    user = os.getenv("DB_USER", "controltower")
    password = os.getenv("DB_PASSWORD", "controltower")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


def _load_env(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            value = value.strip().strip("'").strip('"')
            os.environ[key] = value


def _ensure_ml_schema(engine) -> None:
    sql = Path("src/controltower/db/schema.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))


def _extract_pmo_id(name: str) -> str | None:
    if not name:
        return None
    m = PMO_REGEX.search(name.upper())
    return m.group(1).upper().strip() if m else None


def _build_project_id_map(engine) -> None:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT project_id, name FROM clockify.projects")
        ).mappings().all()
        payload = []
        for r in rows:
            pmo = _extract_pmo_id(r.get("name") or "")
            if not pmo:
                continue
            payload.append((pmo, r.get("project_id")))
        if not payload:
            return
        for pmo, clockify_project_id in payload:
            conn.execute(
                text(
                    """
                    INSERT INTO ml.project_id_map (pmo_id, asana_project_gid, clockify_project_id, updated_at)
                    VALUES (
                        :pmo_id,
                        (SELECT gid FROM projects WHERE pmo_id = CAST(:pmo_id AS varchar) LIMIT 1),
                        :clockify_project_id,
                        NOW()
                    )
                    ON CONFLICT (pmo_id)
                    DO UPDATE SET clockify_project_id = EXCLUDED.clockify_project_id, updated_at = NOW()
                    """
                ),
                {"pmo_id": pmo, "clockify_project_id": clockify_project_id},
            )


def _normalize_ml_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE ml.project_id_map SET pmo_id = UPPER(TRIM(pmo_id))"))
        conn.execute(text("UPDATE ml.weekly_fact SET pmo_id = UPPER(TRIM(pmo_id))"))


def _recalc_weekly_fact(engine, recalc_weeks: int) -> None:
    with engine.begin() as conn:
        cutoff = conn.execute(
            text(
                """
                SELECT (date_trunc('week', NOW())::date - (:weeks * 7))::date AS cutoff
                """
            ),
            {"weeks": recalc_weeks},
        ).scalar()

        conn.execute(
            text(
                """
                DELETE FROM ml.weekly_fact
                WHERE week_start >= :cutoff
                """
            ),
            {"cutoff": cutoff},
        )

        conn.execute(
            text(
                """
                INSERT INTO ml.weekly_fact (pmo_id, week_start, hours_week, active_users_week, updated_at)
                SELECT
                    map.pmo_id,
                    cw.week_start,
                    COALESCE(SUM(te.hours), 0) AS hours_week,
                    COUNT(DISTINCT te.person_id) AS active_users_week,
                    NOW()
                FROM clockify.time_entries te
                JOIN clockify.calendar_weeks cw
                  ON te.entry_date BETWEEN cw.week_start AND cw.week_end
                JOIN ml.project_id_map map
                  ON map.clockify_project_id = te.project_id
                WHERE cw.week_start >= :cutoff
                GROUP BY map.pmo_id, cw.week_start
                """
            ),
            {"cutoff": cutoff},
        )


def _compute_labels(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM ml.ml_project_labels
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO ml.ml_project_labels (pmo_id, closed_date, final_deviation, label, computed_at)
                SELECT
                    UPPER(TRIM(h.pmo_id)) AS pmo_id,
                    COALESCE(
                      h.planned_end_date,
                      (
                        SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                        FROM jsonb_array_elements(h.raw_data->'project'->'custom_fields') cf
                        WHERE cf->>'gid' = '1207505889399790'
                        LIMIT 1
                      ),
                      (
                        SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                        FROM jsonb_array_elements(h.raw_data->'project'->'custom_fields') cf
                        WHERE cf->>'gid' = '1207505889399731'
                        LIMIT 1
                      )
                    ) AS closed_date,
                    CASE
                        WHEN h.planned_hours_total IS NULL OR h.planned_hours_total = 0 THEN NULL
                        ELSE (COALESCE(e.total_hours, 0) - h.planned_hours_total) / h.planned_hours_total
                    END AS final_deviation,
                    CASE
                        WHEN h.planned_hours_total IS NULL OR h.planned_hours_total = 0 THEN NULL
                        WHEN (COALESCE(e.total_hours, 0) - h.planned_hours_total) / h.planned_hours_total > 0.20 THEN 1
                        ELSE 0
                    END AS label,
                    NOW()
                FROM projects_history h
                LEFT JOIN (
                    SELECT map.pmo_id, COALESCE(SUM(te.hours), 0) AS total_hours
                    FROM clockify.time_entries te
                    JOIN ml.project_id_map map
                      ON map.clockify_project_id = te.project_id
                    GROUP BY map.pmo_id
                ) e ON e.pmo_id = UPPER(TRIM(h.pmo_id))
                WHERE h.pmo_id IS NOT NULL
                  AND h.pmo_id <> ''
                  AND h.planned_hours_total IS NOT NULL
                  AND h.planned_hours_total > 0
                  AND h.completed_flag = true
                  AND upper(COALESCE(h.fase_proyecto, '')) = 'TERMINADO'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%bolsa%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%reseller%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%soporte recurrente%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%servicio soporte%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%soporte, seguridad y monitoreo%'
                """
            )
        )


def _fetch_weekly_series(engine, pmo_ids: List[str]) -> Dict[str, List[Tuple[datetime, float, int]]]:
    if not pmo_ids:
        return {}
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pmo_id, week_start, hours_week, active_users_week
                FROM ml.weekly_fact
                WHERE pmo_id = ANY(:pmo_ids)
                  AND week_start < date_trunc('week', NOW())::date
                ORDER BY pmo_id, week_start
                """
            ),
            {"pmo_ids": pmo_ids},
        ).mappings().all()
    grouped: Dict[str, List[Tuple[datetime, float, int]]] = {}
    for r in rows:
        grouped.setdefault(r["pmo_id"], []).append(
            (r["week_start"], float(r["hours_week"]), int(r["active_users_week"]))
        )
    return grouped


def _linear_slope(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    x = np.arange(len(values))
    y = np.array(values, dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def _normalize_cat(value) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip().lower()
    return text if text else "unknown"


def _prepare_feature_matrix(
    df: pd.DataFrame,
    numeric_cols: List[str],
    categorical_cols: List[str],
    expected_columns: List[str] | None = None,
) -> tuple[pd.DataFrame, List[str]]:
    work = df.copy()
    for col in categorical_cols:
        if col in work.columns:
            work[col] = work[col].apply(_normalize_cat)
        else:
            work[col] = "unknown"
    dummies = pd.get_dummies(work[categorical_cols], prefix=categorical_cols, dtype=float)
    X = pd.concat([work[numeric_cols].astype(float), dummies], axis=1)
    if expected_columns is not None:
        for col in expected_columns:
            if col not in X.columns:
                X[col] = 0.0
        X = X[expected_columns]
        return X, expected_columns
    return X, list(X.columns)


def _baseline_probability(row: pd.Series) -> float:
    ratio = float(row.get("ratio_burn") or 0.0)
    volatility = float(row.get("volatility_hours") or 0.0)
    slope = float(row.get("slope_hours_week") or 0.0)

    ratio_norm = min(1.5, max(0.0, ratio)) / 1.5
    vol_norm = min(1.0, max(0.0, volatility))
    slope_norm = 1.0 / (1.0 + math.exp(-slope / 5.0))

    score = 0.6 * ratio_norm + 0.3 * vol_norm + 0.1 * slope_norm
    return max(0.0, min(1.0, score))


def _temporal_split(df: pd.DataFrame, date_col: str, test_months: int = 1):
    if date_col not in df.columns:
        return None, None, None
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    if df.empty:
        return None, None, None
    max_date = df[date_col].max()
    cutoff = max_date.replace(day=1)
    if test_months > 1:
        cutoff = cutoff - pd.DateOffset(months=test_months - 1)
    train = df[df[date_col] < cutoff]
    test = df[df[date_col] >= cutoff]
    return train, test, cutoff


def _build_features(engine, k_values: List[int], weekly_capacity_hours: int) -> None:
    with engine.begin() as conn:
        projects = conn.execute(
            text(
                """
                SELECT
                  UPPER(TRIM(p.pmo_id)) AS pmo_id,
                  COALESCE(
                    p.due_date,
                    (
                      SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                      FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                      WHERE cf->>'gid' = '1207505889399731'
                      LIMIT 1
                    )
                  ) AS planned_end_date,
                  p.start_date,
                  p.planned_hours_total,
                  p.owner_name,
                  p.responsable_proyecto,
                  p.name AS project_name,
                  p.business_vertical,
                  p.tipo_proyecto,
                  p.clasificacion,
                  p.segmento_empresa,
                  p.pais,
                  p.cliente_nuevo,
                  p.sponsor,
                  0 AS source_priority
                FROM projects p
                WHERE p.pmo_id IS NOT NULL AND p.pmo_id <> ''
                  AND COALESCE(p.name, '') NOT ILIKE '%bolsa%'
                  AND COALESCE(p.name, '') NOT ILIKE '%reseller%'
                  AND COALESCE(p.name, '') NOT ILIKE '%soporte recurrente%'
                  AND COALESCE(p.name, '') NOT ILIKE '%servicio soporte%'
                  AND COALESCE(p.name, '') NOT ILIKE '%soporte, seguridad y monitoreo%'
                UNION ALL
                SELECT
                  UPPER(TRIM(h.pmo_id)) AS pmo_id,
                  h.planned_end_date AS planned_end_date,
                  h.start_date,
                  h.planned_hours_total,
                  h.responsable_proyecto AS owner_name,
                  h.responsable_proyecto,
                  COALESCE(h.raw_data->'project'->>'name','') AS project_name,
                  h.business_vertical,
                  h.tipo_proyecto,
                  h.clasificacion,
                  h.segmento_empresa,
                  h.pais,
                  h.cliente_nuevo,
                  h.sponsor,
                  1 AS source_priority
                FROM projects_history h
                WHERE h.pmo_id IS NOT NULL AND h.pmo_id <> ''
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%bolsa%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%reseller%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%soporte recurrente%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%servicio soporte%'
                  AND COALESCE(h.raw_data->'project'->>'name','') NOT ILIKE '%soporte, seguridad y monitoreo%'
                """
            )
        ).mappings().all()

    # Prefer current projects over history for each pmo_id
    projects_sorted = sorted(projects, key=lambda r: (r.get("pmo_id") or "", r.get("source_priority", 9)))
    projects_by_pmo = {}
    for row in projects_sorted:
        pid = row.get("pmo_id")
        if not pid:
            continue
        if pid not in projects_by_pmo:
            projects_by_pmo[pid] = row

    projects = list(projects_by_pmo.values())
    pmo_ids = [p["pmo_id"] for p in projects if p.get("pmo_id")]
    weekly_map = _fetch_weekly_series(engine, pmo_ids)

    # Precompute JP load using responsable_proyecto (not owner_name)
    with engine.begin() as conn:
        jp_projects = conn.execute(
            text(
                """
                SELECT pmo_id, responsable_proyecto, start_date, planned_end_date
                FROM projects
                WHERE pmo_id IS NOT NULL AND pmo_id <> ''
                  AND pmo_id ILIKE 'PMO-%'
                  AND completed_flag = false
                  AND COALESCE(upper(fase_proyecto), '') NOT IN ('TERMINADO','CANCELADO')
                """
            )
        ).mappings().all()
    jp_map = {}
    jp_project_ranges = {}
    for r in jp_projects:
        jp = (r.get("responsable_proyecto") or "").strip()
        if not jp:
            continue
        jp_map.setdefault(jp, []).append(r["pmo_id"])
        jp_project_ranges.setdefault(jp, []).append(
            (r.get("start_date"), r.get("planned_end_date"))
        )

    issues = []
    feature_rows = []
    for p in projects:
        pmo_id = p.get("pmo_id")
        if not pmo_id:
            continue
        series = weekly_map.get(pmo_id, [])
        if not series:
            issues.append((pmo_id, "no_time_entries", "No imputations in weekly_fact"))
            continue

        start_date = p.get("start_date")
        if not start_date:
            start_date = series[0][0]
        due_date = p.get("planned_end_date")
        if not due_date:
            issues.append((pmo_id, "missing_due_date", "due_date is null"))
            continue

        planned_hours_raw = p.get("planned_hours_total")
        if planned_hours_raw is None or planned_hours_raw == 0:
            issues.append((pmo_id, "missing_planned_hours", "planned_hours_total is null or 0"))
            continue
        try:
            planned_hours = float(planned_hours_raw)
        except Exception:
            issues.append((pmo_id, "invalid_planned_hours", f"planned_hours_total={planned_hours_raw}"))
            continue

        duration_days = (due_date - start_date).days if due_date and start_date else 0
        duration_weeks = max(1, math.ceil(duration_days / 7))
        expected_hours_week = planned_hours / float(duration_weeks)

        hours_series = [h for _, h, _ in series]
        active_users_series = [u for _, _, u in series]

        for k in k_values:
            if len(hours_series) < k:
                issues.append((pmo_id, "insufficient_weeks", f"need {k} weeks, have {len(hours_series)}"))
                continue
            hours_k = hours_series[-k:]
            users_k = active_users_series[-k:]

            ratio_burn = _safe_div(sum(hours_k), k * expected_hours_week)
            slope = _linear_slope(hours_k)
            volatility = _safe_div(float(np.std(hours_k)), float(np.mean(hours_k)) if np.mean(hours_k) else 0.0)
            active_people_k = int(max(users_k))
            people_growth = _safe_div(users_k[-1] - users_k[0], users_k[0] if users_k[0] else 1)
            top1_share = _safe_div(max(hours_k), sum(hours_k) if sum(hours_k) else 1)

            # JP utilization
            jp_active_projects_k = None
            jp_name = (p.get("responsable_proyecto") or "").strip()
            if jp_name and jp_name in jp_project_ranges:
                weeks_sorted = [w for w, _, _ in series]
                last_weeks = weeks_sorted[-k:] if weeks_sorted else []
                if last_weeks:
                    counts = []
                    for week_start in last_weeks:
                        week_end = week_start + timedelta(days=6)
                        active_count = 0
                        for start_date, end_date in jp_project_ranges.get(jp_name, []):
                            if start_date and start_date > week_end:
                                continue
                            if end_date and end_date < week_start:
                                continue
                            active_count += 1
                        counts.append(active_count)
                    if counts:
                        jp_active_projects_k = round(sum(counts) / float(len(counts)), 2)

            feature_rows.append(
                {
                    "pmo_id": pmo_id,
                    "k": k,
                    "ratio_burn": ratio_burn,
                    "slope_hours_week": slope,
                    "volatility_hours": volatility,
                    "active_people_k": active_people_k,
                    "people_growth": people_growth,
                    "hours_top1_share": top1_share,
                    "jp_active_projects_k": jp_active_projects_k,
                    "log_planned_hours": math.log(max(planned_hours, 1)),
                    "business_vertical": p.get("business_vertical"),
                    "tipo_proyecto": p.get("tipo_proyecto"),
                    "clasificacion": p.get("clasificacion"),
                    "segmento_empresa": p.get("segmento_empresa"),
                    "pais": p.get("pais"),
                    "cliente_nuevo": p.get("cliente_nuevo"),
                    "sponsor": p.get("sponsor"),
                }
            )

    if not feature_rows:
        _write_quality_issues(engine, issues)
        return

    df = pd.DataFrame(feature_rows)
    df = df.drop_duplicates(subset=["pmo_id", "k"])
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ml.ml_project_features"))
        df.to_sql("ml_project_features", conn, schema="ml", if_exists="append", index=False)
    _write_quality_issues(engine, issues)


def _train_models(engine, k_values: List[int], model_dir: Path) -> Dict[int, str]:
    model_dir.mkdir(parents=True, exist_ok=True)
    model_versions: Dict[int, str] = {}

    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score
    from sklearn.linear_model import LogisticRegression

    try:
        import xgboost as xgb
        use_xgb = True
    except Exception:
        use_xgb = False

    with engine.begin() as conn:
        labels = pd.read_sql(
            text(
                """
                SELECT pmo_id, label, closed_date
                FROM ml.ml_project_labels
                WHERE label IS NOT NULL
                """
            ),
            conn,
        )
        feats = pd.read_sql(
            text("SELECT * FROM ml.ml_project_features"),
            conn,
        )

    if labels.empty or feats.empty:
        return model_versions

    data = feats.merge(labels, on="pmo_id", how="inner")
    feature_cols = [
        "ratio_burn",
        "slope_hours_week",
        "volatility_hours",
        "active_people_k",
        "jp_active_projects_k",
        "people_growth",
        "hours_top1_share",
        "log_planned_hours",
    ]

    for k in k_values:
        df_k = data[data["k"] == k].dropna(subset=feature_cols + ["label"])
        if df_k.empty or df_k["label"].nunique() < 2:
            continue

        temporal_train, temporal_test, cutoff = _temporal_split(df_k, "closed_date", test_months=1)
        if temporal_train is not None and temporal_test is not None and not temporal_train.empty and not temporal_test.empty:
            if temporal_train["label"].nunique() < 2 or temporal_test["label"].nunique() < 2:
                X_all, feature_names = _prepare_feature_matrix(df_k, feature_cols, CATEGORICAL_COLS)
                y = df_k["label"].astype(int)
                X_train, X_test, y_train, y_test = train_test_split(X_all, y, test_size=0.2, random_state=42)
                split_tag = "random"
            else:
                X_train, feature_names = _prepare_feature_matrix(temporal_train, feature_cols, CATEGORICAL_COLS)
                X_test, _ = _prepare_feature_matrix(temporal_test, feature_cols, CATEGORICAL_COLS, feature_names)
                y_train = temporal_train["label"].astype(int)
                y_test = temporal_test["label"].astype(int)
                split_tag = f"tsplit{cutoff.strftime('%Y%m')}"
        else:
            X_all, feature_names = _prepare_feature_matrix(df_k, feature_cols, CATEGORICAL_COLS)
            y = df_k["label"].astype(int)
            X_train, X_test, y_train, y_test = train_test_split(X_all, y, test_size=0.2, random_state=42)
            split_tag = "random"

        if use_xgb:
            model = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.1,
                subsample=0.9,
                colsample_bytree=0.9,
                objective="binary:logistic",
                eval_metric="logloss",
            )
        else:
            model = LogisticRegression(max_iter=1000)

        model.fit(X_train, y_train)
        preds = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, preds)
        version = f"k{k}-{split_tag}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-auc{auc:.3f}"
        path = model_dir / f"model_{version}.json"
        if use_xgb:
            model.save_model(path.as_posix())
        else:
            import joblib

            path = model_dir / f"model_{version}.joblib"
            joblib.dump(model, path)
        meta_path = model_dir / f"{path.name}.meta.json"
        meta_payload = {"feature_columns": feature_names}
        meta_path.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        model_versions[k] = path.name

    return model_versions


def _write_quality_issues(engine, issues: List[Tuple[str, str, str]]) -> None:
    if not issues:
        return
    df = pd.DataFrame(issues, columns=["pmo_id", "issue_type", "details"])
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ml.ml_data_quality_issues"))
        df.to_sql("ml_data_quality_issues", conn, schema="ml", if_exists="append", index=False)


def _score_active_projects(engine, k_values: List[int], model_dir: Path, model_versions: Dict[int, str]) -> None:
    from sklearn.linear_model import LogisticRegression

    try:
        import xgboost as xgb
        use_xgb = True
    except Exception:
        use_xgb = False

    with engine.begin() as conn:
        feats = pd.read_sql(
            text("SELECT * FROM ml.ml_project_features"),
            conn,
        )
        actives = pd.read_sql(
            text(
                """
                SELECT pmo_id
                FROM projects
                WHERE completed_flag = false
                  AND COALESCE(upper(fase_proyecto), '') NOT IN ('TERMINADO','CANCELADO')
                """
            ),
            conn,
        )

    if feats.empty or actives.empty:
        return

    feature_cols = [
        "ratio_burn",
        "slope_hours_week",
        "volatility_hours",
        "active_people_k",
        "jp_active_projects_k",
        "people_growth",
        "hours_top1_share",
        "log_planned_hours",
    ]

    feats = feats.merge(actives, on="pmo_id", how="inner")
    now = datetime.now(timezone.utc)
    rows = []

    for k in k_values:
        df_k = feats[feats["k"] == k].dropna(subset=feature_cols)
        if df_k.empty:
            continue
        model_file = model_versions.get(k)
        if not model_file:
            for _, row in df_k.iterrows():
                rows.append(
                    {
                        "pmo_id": row["pmo_id"],
                        "k": k,
                        "probability": float(_baseline_probability(row)),
                        "scoring_date": now,
                        "model_version": "baseline",
                    }
                )
            continue

        path = model_dir / model_file
        meta_path = model_dir / f"{path.name}.meta.json"
        if meta_path.exists():
            meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
            feature_columns = meta_payload.get("feature_columns") or []
        else:
            feature_columns = []
        if use_xgb and path.suffix == ".json":
            model = xgb.XGBClassifier()
            model.load_model(path.as_posix())
        else:
            import joblib

            model = joblib.load(path)

        if feature_columns:
            X, _ = _prepare_feature_matrix(df_k, feature_cols, CATEGORICAL_COLS, feature_columns)
        else:
            X = df_k[feature_cols].astype(float)
        probs = model.predict_proba(X)[:, 1]
        for pmo_id, prob in zip(df_k["pmo_id"], probs):
            rows.append(
                {
                    "pmo_id": pmo_id,
                    "k": k,
                    "probability": float(prob),
                    "scoring_date": now,
                    "model_version": model_file,
                }
            )

    if not rows:
        return

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["pmo_id", "k", "scoring_date"])
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ml.ml_project_scores WHERE scoring_date = :scoring_date"), {"scoring_date": now})
        df.to_sql("ml_project_scores", conn, schema="ml", if_exists="append", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="ML risk prediction pipeline")
    parser.add_argument("--recalc-weeks", type=int, default=9)
    parser.add_argument("--weekly-capacity-hours", type=int, default=45)
    parser.add_argument("--model-dir", type=str, default="models")
    args = parser.parse_args()

    _load_env()
    engine = create_engine(_db_url_from_env())
    _ensure_ml_schema(engine)

    print("== ML pipeline start ==")
    _build_project_id_map(engine)
    _normalize_ml_tables(engine)
    _recalc_weekly_fact(engine, recalc_weeks=args.recalc_weeks)
    _compute_labels(engine)
    _build_features(engine, k_values=[2, 3, 4], weekly_capacity_hours=args.weekly_capacity_hours)

    model_dir = Path(args.model_dir)
    model_versions = _train_models(engine, k_values=[2, 3, 4], model_dir=model_dir)
    _score_active_projects(engine, k_values=[2, 3, 4], model_dir=model_dir, model_versions=model_versions)

    with engine.begin() as conn:
        counts = conn.execute(
            text(
                """
                SELECT
                  (SELECT COUNT(*) FROM ml.project_id_map) AS map_count,
                  (SELECT COUNT(*) FROM ml.weekly_fact) AS weekly_fact_count,
                  (SELECT COUNT(*) FROM ml.ml_project_labels) AS label_count,
                  (SELECT COUNT(*) FROM ml.ml_project_features) AS feature_count,
                  (SELECT COUNT(*) FROM ml.ml_project_scores) AS score_count,
                  (SELECT COUNT(*) FROM ml.ml_data_quality_issues) AS issues_count
                """
            )
        ).mappings().one()
        eligible = conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT p.pmo_id) AS eligible
                FROM projects p
                JOIN ml.weekly_fact wf ON wf.pmo_id = p.pmo_id
                WHERE p.planned_hours_total IS NOT NULL
                  AND p.planned_hours_total > 0
                  AND COALESCE(
                    p.due_date,
                    (
                      SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                      FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                      WHERE cf->>'gid' = '1207505889399731'
                      LIMIT 1
                    )
                  ) IS NOT NULL
                """
            )
        ).mappings().one()

    print("Counts:", dict(counts))
    print("Eligible projects for features:", eligible["eligible"])
    if counts["feature_count"] == 0:
        print("No features generated. Check planned_hours_total, planned_end_date, and weekly_fact coverage.")
    if counts["score_count"] == 0:
        print("No scores generated. Check that models were trained and features exist.")
    print("== ML pipeline end ==")


if __name__ == "__main__":
    main()

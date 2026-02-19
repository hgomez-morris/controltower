import json
import re
import unicodedata
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo

import streamlit as st


def _jsonable(obj):
    if hasattr(obj, "items"):
        obj = dict(obj)
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return {"value": obj}
    return json.loads(json.dumps(obj, default=str))


def _extract_custom_fields(project_raw):
    try:
        fields = (project_raw or {}).get("custom_fields") or []
    except Exception:
        return []
    out = []
    for f in fields:
        name = f.get("name") or "(sin nombre)"
        if f.get("display_value") is not None:
            val = f.get("display_value")
        else:
            val = (
                f.get("text_value")
                or f.get("number_value")
                or f.get("enum_value")
                or f.get("multi_enum_values")
                or f.get("date_value")
            )
        out.append({"name": name, "value": val})
    return out


def _custom_field_map(project_raw):
    fields = _extract_custom_fields(project_raw)
    return {f["name"]: f["value"] for f in fields}


def _humanize_last_update(ts):
    if not ts:
        return ""
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return str(ts)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - ts.astimezone(timezone.utc)
        days = delta.days
        if days <= 0:
            hours = int(delta.total_seconds() // 3600)
            if hours <= 0:
                return "hoy"
            return f"hace {hours}h"
        if days == 1:
            return "ayer"
        return f"hace {days}d"
    return str(ts)


def _days_since_last_update(ts, today_date=None):
    if not ts:
        return None
    if today_date is None:
        today_date = date.today()
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None
    try:
        return (today_date - dt.date()).days
    except Exception:
        return None


def _fmt_date(val):
    if not val:
        return ""
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            return val.split("T")[0] if "T" in val else val
    return str(val)


def _cf_value_from_project_row(p, field_name):
    project_raw = (p.get("raw_data") or {}).get("project") if hasattr(p, "get") else None
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    return cf.get(field_name, "")


def _cf_value_from_project_raw(project_raw: dict, field_name: str) -> str:
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    return cf.get(field_name, "")


def _cf_first_value(project_raw: dict, field_names: list[str]) -> str:
    if not project_raw:
        return ""
    cf = _custom_field_map(project_raw)
    for n in field_names:
        v = cf.get(n, "")
        if v:
            return v
    return ""


def _fmt_status(val):
    if not val:
        return "-"
    mapping = {
        "on_track": "On track",
        "on_hold": "On hold",
        "off_track": "Off track",
        "at_risk": "At risk",
        "green": "On track",
        "yellow": "At risk",
        "red": "Off track",
        "blue": "On hold",
    }
    return mapping.get(str(val), str(val).replace("_", " ").title())


def _fmt_task_progress(completed, total):
    try:
        total_i = int(total) if total is not None else 0
        completed_i = int(completed) if completed is not None else 0
    except Exception:
        return ""
    if total_i <= 0:
        return "0 de 0 tareas (0%)"
    pct = int(round((completed_i / total_i) * 100))
    return f"{completed_i} de {total_i} tareas ({pct}%)"


def _parse_budget(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val)
    s = s.replace(" ", "")
    if "," in s and "." not in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _get_query_params():
    try:
        return dict(st.query_params)
    except Exception:
        return st.experimental_get_query_params()


def _set_query_params(**kwargs):
    try:
        st.query_params.clear()
        st.query_params.update(kwargs)
    except Exception:
        st.experimental_set_query_params(**kwargs)


def _extract_next_steps(raw: dict) -> str:
    if not raw:
        return ""
    for key in ("next_steps", "next_steps_text", "next_steps_html", "next_steps_summary"):
        if raw.get(key):
            return str(raw.get(key))
    return ""


def _normalize_email_from_name(name: str) -> str:
    if not name:
        return ""
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    parts = [p for p in n.replace("-", " ").split() if p.strip()]
    if len(parts) == 0:
        return ""
    first = parts[0].lower()
    last = parts[-1].lower()
    return f"{first[:1]}{last}@morrisopazo.com"


def _truncate_text(s: str, n: int = 10) -> str:
    if not s:
        return ""
    s = str(s)
    return s[:n] + "..." if len(s) > n else s


def _selected_row_index(selection):
    if selection is None:
        return None
    rows = []
    if hasattr(selection, "selection") and hasattr(selection.selection, "rows"):
        rows = list(selection.selection.rows)
    elif isinstance(selection, dict):
        rows = list(selection.get("selection", {}).get("rows", []))
    if not rows:
        return None
    try:
        return int(rows[0])
    except Exception:
        return None


def _table_height(row_count: int, min_height: int = 120, max_height: int = 700, row_px: int = 35, header_px: int = 40) -> int:
    return max(min_height, min(max_height, header_px + row_count * row_px))


def format_datetime_chile(value) -> str:
    if not value:
        return ""
    try:
        chile_tz = ZoneInfo("America/Santiago")
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(chile_tz).strftime("%Y-%m-%d %H:%M")
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(chile_tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]

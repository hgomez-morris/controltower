from __future__ import annotations

import pytest

from controltower.clockify.analytics_db import fetch_last_sync, fetch_week_starts, get_conn


def _table_exists(conn, schema: str, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table),
    ).fetchone()
    return bool(row)


def test_clockify_queries_smoke():
    try:
        conn = get_conn()
    except Exception as exc:
        pytest.skip(f"Clockify DB not available: {exc}")

    try:
        if not _table_exists(conn, "clockify", "sync_history"):
            pytest.skip("clockify.sync_history not found")
        last = fetch_last_sync(conn)
        assert isinstance(last, dict)

        if not _table_exists(conn, "clockify", "calendar_weeks"):
            pytest.skip("clockify.calendar_weeks not found")
        weeks = fetch_week_starts(conn, 2, include_current_week=True)
        assert isinstance(weeks, list)
    finally:
        conn.close()

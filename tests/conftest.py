from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _db_url_from_env() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    name = os.getenv("DB_NAME", "controltower")
    user = os.getenv("DB_USER", "controltower")
    password = os.getenv("DB_PASSWORD", "controltower")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


@pytest.fixture(scope="session")
def db_engine():
    url = _db_url_from_env()
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"Database not available for tests: {exc}")
    return engine

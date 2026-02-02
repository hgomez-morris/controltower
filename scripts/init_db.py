from __future__ import annotations
from pathlib import Path
from sqlalchemy import text
from controltower.db.connection import get_engine
from controltower.utils.logging import configure_logging
import logging

def main() -> None:
    configure_logging()
    log = logging.getLogger("init_db")
    engine = get_engine()
    schema_path = Path(__file__).resolve().parents[1] / "src" / "controltower" / "db" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    log.info("DB schema applied successfully.")

if __name__ == "__main__":
    main()

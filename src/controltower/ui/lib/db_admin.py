from sqlalchemy import text

from controltower.ui.lib.context import get_engine_cached


def _ensure_kpi_tables() -> None:
    engine = get_engine_cached()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS kpi_snapshots (
                id SERIAL PRIMARY KEY,
                kpi_id VARCHAR(50) NOT NULL,
                scope_type VARCHAR(20) NOT NULL,
                scope_value VARCHAR(200) NOT NULL,
                as_of TIMESTAMP NOT NULL,
                total_projects INTEGER NOT NULL,
                compliant_projects INTEGER NOT NULL,
                kpi_value DECIMAL(5,2) NOT NULL
            )
            """
            )
        )
        conn.execute(
            text(
                """
            ALTER TABLE projects
            ADD COLUMN IF NOT EXISTS tasks_modified_last_7d INTEGER
            """
            )
        )


def _ensure_payments_tables() -> None:
    engine = get_engine_cached()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                project_gid VARCHAR(50),
                pmo_id VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL,
                payment_date DATE NOT NULL,
                glosa TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
            )
        )
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS payment_estimate_history (
                id SERIAL PRIMARY KEY,
                payment_id INTEGER NOT NULL,
                old_date DATE,
                new_date DATE,
                changed_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (payment_id) REFERENCES payments(id)
            )
            """
            )
        )
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_payments_pmo_id ON payments(pmo_id)"""))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_payments_project ON payments(project_gid)"""))

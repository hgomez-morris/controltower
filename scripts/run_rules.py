from controltower.config import load_config
from controltower.rules.engine import evaluate_rules
from controltower.db.connection import get_engine
from sqlalchemy import text
import os


def main() -> None:
    cfg = load_config(os.path.join("config", "config.yaml")) if os.path.exists(os.path.join("config", "config.yaml")) else load_config(os.path.join("config", "config.example.yaml"))
    engine = get_engine()
    with engine.begin() as conn:
        sync_id = conn.execute(text("select sync_id from sync_log order by started_at desc limit 1")).scalar()
    if not sync_id:
        raise RuntimeError("No sync_id found. Run sync first.")
    created = evaluate_rules(cfg, sync_id)
    print(f"sync_id={sync_id} findings_created={created}")


if __name__ == "__main__":
    main()

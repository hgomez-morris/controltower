import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from controltower.actions.slack import post_new_findings_to_slack
from controltower.config import load_config
from controltower.rules.engine import evaluate_rules
from controltower.sync.sync_runner import main_sync
from controltower.utils.logging import configure_logging

def main():
    configure_logging()
    cfg = load_config(os.path.join("config","config.example.yaml"))
    sync_id = main_sync(cfg)
    evaluate_rules(cfg, sync_id)

    # Clockify sync: 90 days only on the 09:00 (Chile) run; otherwise 7 days
    chile_now = datetime.now(ZoneInfo("America/Santiago"))
    morning = chile_now.hour == 9
    incremental_days = 90 if morning else 7
    try:
        subprocess.run(
            [sys.executable, "scripts/run_clockify_sync.py", "--incremental-days", str(incremental_days)],
            check=True,
        )
    except Exception as exc:
        print(f"[clockify] sync failed: {exc}")

    # Slack disabled for now. Should run after Clockify when re-enabled.
    # if (cfg.get("slack", {}) or {}).get("enabled", False):
    #     post_new_findings_to_slack(cfg)

if __name__ == "__main__":
    main()

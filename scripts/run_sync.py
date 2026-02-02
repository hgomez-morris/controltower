from controltower.config import load_config
from controltower.utils.logging import configure_logging
from controltower.sync.sync_runner import main_sync
from controltower.rules.engine import evaluate_rules
from controltower.actions.slack import post_new_findings_to_slack
import os

def main():
    configure_logging()
    cfg = load_config(os.path.join("config","config.example.yaml"))
    sync_id = main_sync(cfg)
    evaluate_rules(cfg, sync_id)
    post_new_findings_to_slack(cfg)

if __name__ == "__main__":
    main()

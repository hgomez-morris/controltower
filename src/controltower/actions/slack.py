from __future__ import annotations
import os, json, logging
import requests
from sqlalchemy import text
from controltower.db.connection import get_engine

def post_new_findings_to_slack(channel: str | None = None) -> int:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL not set")

    channel = channel or os.getenv("SLACK_CHANNEL", "#pmo-status")
    engine = get_engine()
    sent = 0
    log = logging.getLogger("slack")

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, project_gid, rule_id, severity, details
            FROM findings
            WHERE status='open' AND (details->>'slack_sent') IS DISTINCT FROM 'true'
            ORDER BY created_at ASC
        """)).mappings().all()

        for r in rows:
            details = r["details"] or {}
            msg = {
                "text": f"[{r['severity'].upper()}] {details.get('project_name','(sin nombre)')} — {r['rule_id']}",
            }
            resp = requests.post(webhook, data=json.dumps(msg), headers={"Content-Type":"application/json"})
            if resp.status_code >= 300:
                log.error("Slack webhook failed status=%s body=%s", resp.status_code, resp.text)
                continue
            # mark sent
            conn.execute(text("""
                UPDATE findings
                SET details = jsonb_set(COALESCE(details,'{}'::jsonb), '{slack_sent}', 'true'::jsonb, true)
                WHERE id = :id
            """), {"id": r["id"]})
            sent += 1
    log.info("Slack sent messages=%s", sent)
    return sent

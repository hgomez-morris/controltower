from __future__ import annotations
import os, json, logging
import requests
from sqlalchemy import text
from controltower.db.connection import get_engine

def post_new_findings_to_slack(config: dict, channel: str | None = None) -> int:
    webhook = (config.get("slack", {}) or {}).get("webhook_url") or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL not set")

    channel = channel or (config.get("slack", {}) or {}).get("channel") or os.getenv("SLACK_CHANNEL", "#pmo-status")
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
                "text": f"[{r['severity'].upper()}] {details.get('project_name','(sin nombre)')} - {r['rule_id']}",
            }
            if channel:
                msg["channel"] = channel
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

def post_findings_to_slack_by_ids(config: dict, ids: list[int], channel: str | None = None) -> int:
    if not ids:
        return 0
    webhook = (config.get("slack", {}) or {}).get("webhook_url") or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL not set")

    channel = channel or (config.get("slack", {}) or {}).get("channel") or os.getenv("SLACK_CHANNEL", "#pmo-status")
    engine = get_engine()
    sent = 0
    log = logging.getLogger("slack")

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, project_gid, rule_id, severity, details
            FROM findings
            WHERE id = ANY(:ids)
            ORDER BY created_at ASC
        """), {"ids": ids}).mappings().all()

        for r in rows:
            details = r["details"] or {}
            msg = {
                "text": f"[{r['severity'].upper()}] {details.get('project_name','(sin nombre)')} - {r['rule_id']}",
            }
            if channel:
                msg["channel"] = channel
            resp = requests.post(webhook, data=json.dumps(msg), headers={"Content-Type":"application/json"})
            if resp.status_code >= 300:
                log.error("Slack webhook failed status=%s body=%s", resp.status_code, resp.text)
                continue
            sent += 1
    log.info("Slack sent messages=%s", sent)
    return sent

def post_slack_message(config: dict, text: str, channel: str | None = None, blocks: list[dict] | None = None) -> None:
    webhook = (config.get("slack", {}) or {}).get("webhook_url") or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL not set")
    channel = channel or (config.get("slack", {}) or {}).get("channel") or os.getenv("SLACK_CHANNEL", "#pmo-status")
    msg = {"text": text}
    if blocks:
        msg["blocks"] = blocks
    if channel:
        msg["channel"] = channel
    resp = requests.post(webhook, data=json.dumps(msg), headers={"Content-Type":"application/json"})
    if resp.status_code >= 300:
        raise RuntimeError(f"Slack webhook failed status={resp.status_code} body={resp.text}")

def _slack_api_request(token: str, method: str, payload: dict, use_form: bool = False) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if use_form:
        resp = requests.post(
            f"https://slack.com/api/{method}",
            headers=headers,
            data=payload,
        )
    else:
        headers["Content-Type"] = "application/json"
        resp = requests.post(
            f"https://slack.com/api/{method}",
            headers=headers,
            data=json.dumps(payload),
        )
    if resp.status_code >= 300:
        raise RuntimeError(f"Slack API failed status={resp.status_code} body={resp.text}")
    data = resp.json()
    if not data.get("ok"):
        meta = data.get("response_metadata") or {}
        msgs = meta.get("messages")
        raise RuntimeError(f"Slack API error: {data.get('error')} {msgs or ''} payload={payload}".strip())
    return data

def post_dm_by_email(config: dict, email: str, text: str, blocks: list[dict] | None = None) -> None:
    token = (config.get("slack", {}) or {}).get("bot_token") or os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN not set")
    email = (email or "").strip().lower()
    if not email:
        raise RuntimeError("Email requerido para DM")
    user = _slack_api_request(token, "users.lookupByEmail", {"email": email}, use_form=True)
    user_id = (user.get("user") or {}).get("id")
    if not user_id:
        raise RuntimeError("No se pudo resolver user_id para el email")
    # Open conversation (DM) first
    convo = _slack_api_request(token, "conversations.open", {"users": user_id}, use_form=True)
    channel_id = (convo.get("channel") or {}).get("id")
    if not channel_id:
        raise RuntimeError("No se pudo abrir conversación DM")
    payload = {"channel": channel_id, "text": text}
    if blocks:
        payload["blocks"] = blocks
    _slack_api_request(token, "chat.postMessage", payload)

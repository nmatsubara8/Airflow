"""通知連携（DDD-08 §6）。

Slack webhook + SMTP email + CLI（bell）で needs_human / critical / バッチ完了を通知する。
"""
from __future__ import annotations

import logging
import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import httpx

from ..common.io import safe_yaml_load
from ..common.url_validator import validate_slack_webhook_url

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(os.environ.get("AIRFLOW_CONFIG", ".airflow/config/auth.yaml"))

_TEMPLATES = {
    "needs_human": (
        ":warning: *Escalation*: `{task_id}` needs human review\n"
        "*Title*: {title}\n*Section*: {section}\n*Agent*: {agent_id}\n*Reason*: {reason}"
    ),
    "critical": (
        ":rotating_light: *CRITICAL*: {message}\n"
        "*audit_id*: {audit_id}\n*source*: {source}"
    ),
    "starvation_force": (
        ":hourglass: *Starvation force-run*: `{task_id}`\n"
        "*Waited*: {waited_s}s"
    ),
    "gpu_oom": (
        ":fire: *GPU OOM*: model={model_id}\n"
        "*Free VRAM*: {free_vram_gb} GB"
    ),
    "audit_tamper": (
        ":lock: *Audit chain broken* at `{date}`\n"
        "*Expected*: {expected_hash}\n*Got*: {actual_hash}"
    ),
    "batch_complete": (
        ":white_check_mark: *Nightly batch done*: {name}\n"
        "*Success*: {success}/{total} | *Duration*: {duration_s}s"
    ),
}


def _load_config() -> dict[str, Any]:
    return safe_yaml_load(_CONFIG_PATH).get("notifications", {})


_EMAIL_EVENTS = {"critical", "audit_tamper"}


def _slack_webhook_url() -> str | None:
    raw = os.environ.get("AIRFLOW_SLACK_WEBHOOK")
    if raw is None:
        return None
    return validate_slack_webhook_url(raw)


def send_email(
    subject: str,
    body: str,
    email_cfg: dict[str, Any],
) -> bool:
    """SMTP でメールを送信する（DDD-08 §6.1）。

    email_cfg キー: smtp_host, smtp_port (default 587), smtp_user,
    smtp_password_env, from_addr, to_addrs (list or str)。
    """
    smtp_host = email_cfg.get("smtp_host", "")
    if not smtp_host:
        return False
    smtp_port = int(email_cfg.get("smtp_port", 587))
    smtp_user = email_cfg.get("smtp_user", "")
    password = os.environ.get(email_cfg.get("smtp_password_env", ""), "")
    from_addr = email_cfg.get("from_addr", smtp_user)
    to_raw = email_cfg.get("to_addrs", [])
    to_addrs = [to_raw] if isinstance(to_raw, str) else list(to_raw)
    if not to_addrs:
        logger.warning("send_email: to_addrs is empty, skipping")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
            smtp.ehlo()
            if smtp_port != 25:
                smtp.starttls()
                smtp.ehlo()
            if smtp_user:
                smtp.login(smtp_user, password)
            smtp.sendmail(from_addr, to_addrs, msg.as_string())
        logger.info("email sent: subject=%r to=%s", subject, to_addrs)
        return True
    except (smtplib.SMTPException, OSError) as exc:
        logger.warning("send_email failed: %s", exc)
        return False


def send_notification(
    event: str,
    data: dict[str, Any],
    *,
    channel: str | None = None,
) -> bool:
    """指定イベントの通知を送信する（DDD-08 §6.2）。

    Returns:
        True if sent successfully, False otherwise.
    """
    cfg = _load_config()
    slack_cfg = cfg.get("channels", {}).get("slack", {})
    email_cfg = cfg.get("channels", {}).get("email", {})
    cli_cfg = cfg.get("channels", {}).get("cli", {})

    template = _TEMPLATES.get(event, "{event}: {data}")
    try:
        message = template.format(event=event, data=str(data), **data)
    except KeyError as exc:
        message = f"[{event}] {data}"
        logger.warning("notification template key missing: %s", exc)

    sent = False

    # CLI 通知
    if cli_cfg.get("enabled", True):
        bell = "\a" if cli_cfg.get("bell_on_critical") and event == "critical" else ""
        logger.info("NOTIFY [%s] %s%s", event, bell, message.replace("\n", " | "))
        sent = True

    # Slack webhook
    webhook_url = _slack_webhook_url()
    if slack_cfg.get("enabled", False) and webhook_url:
        target_channel = channel or slack_cfg.get("default_channel", "#airflow-alerts")
        payload = {"text": message, "channel": target_channel}
        try:
            resp = httpx.post(webhook_url, json=payload, timeout=10.0)
            resp.raise_for_status()
            sent = True
            logger.info("slack notification sent: event=%s", event)
        except httpx.HTTPError as exc:
            sent = False
            logger.warning("slack notification failed: %s", exc)

    # Email（critical / audit_tamper のみ）
    if email_cfg.get("enabled", False) and event in _EMAIL_EVENTS:
        subject = f"[airflow] {event}: {data.get('audit_id', data.get('task_id', ''))}"
        if send_email(subject, message, email_cfg):
            sent = True

    return sent


def notify_needs_human(
    task_id: str,
    title: str,
    section: str,
    agent_id: str,
    reason: str,
) -> None:
    send_notification("needs_human", {
        "task_id": task_id, "title": title,
        "section": section, "agent_id": agent_id, "reason": reason,
    })


def notify_batch_complete(
    name: str, success: int, total: int, duration_s: float
) -> None:
    send_notification("batch_complete", {
        "name": name, "success": success,
        "total": total, "duration_s": round(duration_s, 1),
    })


def notify_audit_tamper(date: str, expected_hash: str, actual_hash: str) -> None:
    send_notification("audit_tamper", {
        "date": date, "expected_hash": expected_hash, "actual_hash": actual_hash,
    })

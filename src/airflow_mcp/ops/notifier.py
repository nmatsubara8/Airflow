"""通知連携（DDD-08 §6）。

Slack webhook + CLI（bell）で needs_human / critical / バッチ完了を通知する。
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx
import yaml

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
    if _CONFIG_PATH.exists():
        cfg = yaml.safe_load(_CONFIG_PATH.read_text())
        return cfg.get("notifications", {})
    return {}


def _slack_webhook_url() -> str | None:
    return os.environ.get("AIRFLOW_SLACK_WEBHOOK")


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
            logger.warning("slack notification failed: %s", exc)

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

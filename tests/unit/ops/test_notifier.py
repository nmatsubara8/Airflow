"""ops/notifier.py の単体テスト（DDD-08 §6）。"""
from __future__ import annotations

import pytest

from airflow_mcp.ops.notifier import (
    notify_audit_tamper,
    notify_batch_complete,
    notify_needs_human,
    send_notification,
)


@pytest.fixture(autouse=True)
def _disable_slack(monkeypatch):
    """Slack 送信を常に無効化してテストを独立させる。"""
    monkeypatch.setenv("AIRFLOW_SLACK_WEBHOOK", "")
    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {"slack": {"enabled": False}, "cli": {"enabled": True}}},
    )


# ── send_notification ─────────────────────────────────

def test_send_notification_returns_true_on_cli():
    result = send_notification("batch_complete", {
        "name": "nightly", "success": 5, "total": 5, "duration_s": 10.0,
    })
    assert result is True


def test_send_notification_unknown_event_does_not_raise():
    result = send_notification("unknown_event", {"key": "value"})
    assert result is True  # CLI は常に True


def test_send_notification_missing_template_key_does_not_raise():
    result = send_notification("needs_human", {"task_id": "t1"})  # 必須キーが欠如
    assert result is True  # フォールバック文字列で送信


# ── notify_* ────────────────────────────────────────

def test_notify_needs_human_does_not_raise():
    notify_needs_human("t1", "テストタスク", "engineering", "agent_qa", "理由")


def test_notify_batch_complete_does_not_raise():
    notify_batch_complete("nightly_qa", 10, 10, 42.3)


def test_notify_audit_tamper_does_not_raise():
    notify_audit_tamper("2026-01-01", "abc123", "deadbeef")

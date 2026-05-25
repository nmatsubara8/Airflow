"""ops/notifier.py の単体テスト（DDD-08 §6）。"""
from __future__ import annotations

import pytest

from airflow_mcp.ops.notifier import (
    notify_audit_tamper,
    notify_batch_complete,
    notify_needs_human,
    send_email,
    send_notification,
)

# autouse fixture がパッチする前にモジュールレベルで実関数を保持する
import airflow_mcp.ops.notifier as _notifier_mod
_real_load_config = _notifier_mod._load_config
_real_slack_webhook_url = _notifier_mod._slack_webhook_url


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


# ── _load_config both branches (line 51-54) ───────────

def test_load_config_reads_yaml_file(tmp_path, monkeypatch):
    """実の _load_config をモジュールレベルの参照で呼ぶ（autouse patch を回避）。"""
    cfg = tmp_path / "auth.yaml"
    cfg.write_text(
        "notifications:\n  channels:\n    slack:\n      enabled: false\n"
    )
    monkeypatch.setattr(_notifier_mod, "_CONFIG_PATH", cfg)
    result = _real_load_config()
    assert "channels" in result


def test_load_config_returns_empty_when_no_file(monkeypatch):
    """config ファイルが存在しないとき {} を返すこと（line 54）。"""
    from pathlib import Path
    monkeypatch.setattr(_notifier_mod, "_CONFIG_PATH", Path("/nonexistent/auth.yaml"))
    result = _real_load_config()
    assert result == {}


# ── _slack_webhook_url return None (line 60) ──────────

def test_slack_webhook_url_returns_none_when_env_not_set(monkeypatch):
    """AIRFLOW_SLACK_WEBHOOK が未設定のとき None を返すこと（line 60）。"""
    monkeypatch.delenv("AIRFLOW_SLACK_WEBHOOK", raising=False)
    result = _real_slack_webhook_url()
    assert result is None


# ── Slack webhook path (lines 97-105) ─────────────────

def test_send_notification_slack_success(monkeypatch):
    """Slack enabled + valid webhook → POST が呼ばれ sent=True（lines 97-103）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {
            "slack": {"enabled": True, "default_channel": "#test"},
            "cli": {"enabled": False},
        }},
    )
    monkeypatch.setenv("AIRFLOW_SLACK_WEBHOOK", "https://hooks.slack.com/services/test")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None
    with patch("airflow_mcp.ops.notifier.httpx.post", return_value=mock_resp):
        result = send_notification("batch_complete", {
            "name": "nightly", "success": 5, "total": 5, "duration_s": 10.0,
        })
    assert result is True


def test_send_notification_slack_http_error(monkeypatch):
    """Slack POST が HTTPError → ログ警告のみ、クラッシュしないこと（lines 104-105）。"""
    import httpx
    from unittest.mock import patch
    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {
            "slack": {"enabled": True, "default_channel": "#test"},
            "cli": {"enabled": False},
        }},
    )
    monkeypatch.setenv("AIRFLOW_SLACK_WEBHOOK", "https://hooks.slack.com/services/test")
    with patch("airflow_mcp.ops.notifier.httpx.post",
               side_effect=httpx.ConnectError("refused")):
        result = send_notification("batch_complete", {
            "name": "nightly", "success": 5, "total": 5, "duration_s": 10.0,
        })
    assert result is False  # CLI も disabled なので False


def test_send_notification_slack_with_explicit_channel(monkeypatch):
    """channel= 引数が Slack payload に渡されること（line 97-98）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {
            "slack": {"enabled": True, "default_channel": "#default"},
            "cli": {"enabled": False},
        }},
    )
    monkeypatch.setenv("AIRFLOW_SLACK_WEBHOOK", "https://hooks.slack.com/services/test")
    captured = {}
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None

    def fake_post(url, json, timeout):
        captured["channel"] = json["channel"]
        return mock_resp

    with patch("airflow_mcp.ops.notifier.httpx.post", side_effect=fake_post):
        send_notification("batch_complete", {
            "name": "n", "success": 1, "total": 1, "duration_s": 1.0,
        }, channel="#override")
    assert captured["channel"] == "#override"


# ── 追加テスト: _load_config YAML non-dict ガード ─────
# _real_load_config を使って autouse patch を回避する

def test_load_config_non_dict_yaml_returns_empty(tmp_path, monkeypatch):
    """YAML が list などの非dict を返した場合 {} を返すこと。"""
    cfg = tmp_path / "ops.yaml"
    cfg.write_text("- item1\n- item2\n")
    monkeypatch.setattr(_notifier_mod, "_CONFIG_PATH", cfg)
    result = _real_load_config()
    assert result == {}


def test_load_config_empty_file_returns_empty(tmp_path, monkeypatch):
    """YAML が空ファイルの場合 {} を返すこと。"""
    cfg = tmp_path / "ops.yaml"
    cfg.write_text("")
    monkeypatch.setattr(_notifier_mod, "_CONFIG_PATH", cfg)
    result = _real_load_config()
    assert result == {}


def test_load_config_comment_only_yaml_returns_empty(tmp_path, monkeypatch):
    """yaml.safe_load が None を返すコメントのみの YAML でも {} を返すこと。"""
    cfg = tmp_path / "ops.yaml"
    cfg.write_text("# comment only\n")
    monkeypatch.setattr(_notifier_mod, "_CONFIG_PATH", cfg)
    result = _real_load_config()
    assert result == {}


# ── send_email ────────────────────────────────────────

def test_send_email_returns_false_when_no_smtp_host():
    result = send_email("subject", "body", {})
    assert result is False


def test_send_email_sends_via_smtp(monkeypatch):
    """SMTP が正常に送信できる場合 True を返すこと。"""
    from unittest.mock import MagicMock, patch

    mock_smtp = MagicMock()
    mock_smtp.__enter__ = lambda s: s
    mock_smtp.__exit__ = MagicMock(return_value=False)

    with patch("airflow_mcp.ops.notifier.smtplib.SMTP", return_value=mock_smtp):
        result = send_email(
            "Test Subject",
            "Test body",
            {
                "smtp_host": "smtp.example.com",
                "smtp_port": 587,
                "smtp_user": "user@example.com",
                "smtp_password_env": "TEST_SMTP_PASS",
                "from_addr": "user@example.com",
                "to_addrs": ["dest@example.com"],
            },
        )
    assert result is True
    mock_smtp.sendmail.assert_called_once()


def test_send_email_returns_false_on_smtp_exception(monkeypatch):
    """SMTP 例外が発生しても False を返しクラッシュしないこと。"""
    import smtplib
    from unittest.mock import patch

    with patch("airflow_mcp.ops.notifier.smtplib.SMTP", side_effect=smtplib.SMTPConnectError(421, "down")):
        result = send_email(
            "Subject",
            "body",
            {"smtp_host": "bad.host", "to_addrs": ["x@y.com"]},
        )
    assert result is False


def test_send_email_returns_false_when_no_to_addrs():
    result = send_email("subj", "body", {"smtp_host": "smtp.example.com", "to_addrs": []})
    assert result is False


def test_send_email_accepts_string_to_addr(monkeypatch):
    """to_addrs が str の場合もリスト変換して送信すること。"""
    from unittest.mock import MagicMock, patch

    mock_smtp = MagicMock()
    mock_smtp.__enter__ = lambda s: s
    mock_smtp.__exit__ = MagicMock(return_value=False)

    with patch("airflow_mcp.ops.notifier.smtplib.SMTP", return_value=mock_smtp):
        result = send_email(
            "subj", "body",
            {"smtp_host": "s", "smtp_port": 25, "to_addrs": "single@example.com"},
        )
    assert result is True


# ── send_notification email integration ───────────────

def test_send_notification_sends_email_for_critical(monkeypatch):
    """critical イベントで email が enabled なら send_email が呼ばれること。"""
    from unittest.mock import patch

    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {
            "email": {"enabled": True, "smtp_host": "s", "to_addrs": ["a@b.com"]},
            "cli": {"enabled": False},
            "slack": {"enabled": False},
        }},
    )
    with patch("airflow_mcp.ops.notifier.send_email", return_value=True) as mock_email:
        result = send_notification("critical", {
            "message": "crash", "audit_id": "aid1", "source": "runner",
        })
    mock_email.assert_called_once()
    assert result is True


def test_send_notification_does_not_email_non_email_events(monkeypatch):
    """needs_human は email 対象外なので send_email が呼ばれないこと。"""
    from unittest.mock import patch

    monkeypatch.setattr(
        "airflow_mcp.ops.notifier._load_config",
        lambda: {"channels": {
            "email": {"enabled": True, "smtp_host": "s", "to_addrs": ["a@b.com"]},
            "cli": {"enabled": True},
            "slack": {"enabled": False},
        }},
    )
    with patch("airflow_mcp.ops.notifier.send_email") as mock_email:
        send_notification("needs_human", {
            "task_id": "t1", "title": "x", "section": "eng",
            "agent_id": "agent_docs", "reason": "r",
        })
    mock_email.assert_not_called()

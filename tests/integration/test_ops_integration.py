"""ops モジュール結合テスト（DDD-08 §4.5, §5, §6）。

結合対象:
  ops/monitor.py        (get_status_dict / print_status)
  ops/notifier.py       (send_notification / notify_* helpers)
  ops/audit_integrity.py (compute_chain_hash / run_chain_hash / verify_chain)

検証シナリオ:
  1. monitor — taskboard 状態を正確に読み取る
  2. notifier — テンプレート展開・CLI 通知・Slack webhook の動作
  3. audit_integrity — ハッシュチェーン構築・検証・改ざん検出
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("OLLAMA_STUB", "true")

# ── フィクスチャ ──────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_taskboard(tmp_path: Path, monkeypatch):
    """テストごとに独立した TaskStore ディレクトリを使用する。"""
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    monkeypatch.setenv("AIRFLOW_TASKBOARD_DIR", str(tasks_dir))
    yield tasks_dir


@pytest.fixture
def integrity_dir(tmp_path: Path) -> Path:
    """audit_integrity テスト用の一時ディレクトリ。"""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    integrity = logs_dir / "integrity"
    integrity.mkdir()
    return logs_dir


# ── シナリオ 1: monitor ────────────────────────────────────────────

class TestMonitor:

    def test_get_status_dict_returns_required_keys(self):
        """get_status_dict() が必須キーを持つ辞書を返すこと。"""
        from airflow_mcp.ops.monitor import get_status_dict
        result = get_status_dict()
        for key in ("gpu", "tasks", "sections", "profile", "timestamp"):
            assert key in result, f"キー '{key}' が存在しない"

    def test_task_counts_reflect_created_tasks(self, isolated_taskboard):
        """TaskStore に作成したタスクが task_counts に反映される。"""
        from airflow_mcp.taskboard.store import TaskStore
        from airflow_mcp.ops.monitor import get_status_dict

        store = TaskStore()
        store.create({"title": "t1", "section": "engineering"})
        store.create({"title": "t2", "section": "content"})

        result = get_status_dict()
        # inbox 件数が増えていること
        assert result["tasks"].get("inbox", 0) >= 2

    def test_in_progress_task_counted_in_sections(self, isolated_taskboard):
        """in_progress のタスクがセクション別にカウントされること。"""
        from airflow_mcp.taskboard.store import TaskStore
        from airflow_mcp.ops.monitor import get_status_dict

        store = TaskStore()
        t = store.create({"title": "eng_task", "section": "engineering", "owner": None})
        store.claim(t["id"], "agent_docs")  # in_progress へ遷移

        result = get_status_dict()
        sections = result.get("sections", {})
        eng = sections.get("engineering", {})
        assert eng.get("active", 0) >= 1

    def test_timestamp_is_iso_format(self):
        """timestamp フィールドが ISO 8601 形式であること。"""
        from airflow_mcp.ops.monitor import get_status_dict
        result = get_status_dict()
        ts = result["timestamp"]
        # ISO 8601: "2024-01-01T00:00:00Z" 形式
        assert "T" in ts and ts.endswith("Z")

    def test_print_status_produces_output(self, capsys):
        """print_status() がターミナルに出力を生成すること。"""
        from airflow_mcp.ops.monitor import print_status
        print_status(degraded=False)
        captured = capsys.readouterr()
        assert len(captured.out) > 0
        assert "Airflow Status" in captured.out

    def test_print_status_degraded_mode_shown(self, capsys):
        """degraded=True のとき 'DEGRADED' が出力に含まれること。"""
        from airflow_mcp.ops.monitor import print_status
        print_status(degraded=True)
        captured = capsys.readouterr()
        assert "DEGRADED" in captured.out

    def test_needs_human_tasks_reflected_in_counts(self, isolated_taskboard):
        """needs_human ステータスのタスクが task_counts に反映される。"""
        from airflow_mcp.taskboard.store import TaskStore
        from airflow_mcp.ops.monitor import get_status_dict

        store = TaskStore()
        t = store.create({"title": "escalated", "section": "engineering", "owner": None})
        store.claim(t["id"], "agent_docs")
        store.transition(t["id"], "needs_human", reason="test")

        result = get_status_dict()
        assert result["tasks"].get("needs_human", 0) >= 1


# ── シナリオ 2: notifier ──────────────────────────────────────────

class TestNotifier:

    def test_send_notification_cli_returns_true(self):
        """CLI 通知は常に True を返す（デフォルト enabled）。"""
        from airflow_mcp.ops.notifier import send_notification
        result = send_notification(
            "needs_human",
            {"task_id": "t1", "title": "test", "section": "eng",
             "agent_id": "a1", "reason": "test"},
        )
        assert result is True

    def test_notify_needs_human_formats_correctly(self, caplog):
        """notify_needs_human() がテンプレート展開したメッセージをログ出力する。"""
        import logging
        from airflow_mcp.ops.notifier import notify_needs_human

        with caplog.at_level(logging.INFO, logger="airflow_mcp.ops.notifier"):
            notify_needs_human(
                task_id="task_001",
                title="テストタスク",
                section="engineering",
                agent_id="agent_docs",
                reason="schema invalid",
            )

        assert any("task_001" in r.message for r in caplog.records)

    def test_notify_batch_complete_logs_correct_info(self, caplog):
        """notify_batch_complete() がバッチ名・成功数・合計数をログ出力する。"""
        import logging
        from airflow_mcp.ops.notifier import notify_batch_complete

        with caplog.at_level(logging.INFO, logger="airflow_mcp.ops.notifier"):
            notify_batch_complete("nightly_qa", success=3, total=3, duration_s=1.5)

        combined = " ".join(r.message for r in caplog.records)
        assert "nightly_qa" in combined

    def test_send_notification_with_missing_template_key(self):
        """テンプレートキーが欠けてもクラッシュせず True を返す。"""
        from airflow_mcp.ops.notifier import send_notification
        # needs_human テンプレートに必須の task_id を省略
        result = send_notification("needs_human", {"title": "only title"})
        assert result is True

    def test_notify_audit_tamper_formats_hashes(self, caplog):
        """notify_audit_tamper() が expected/actual ハッシュをログ出力する。"""
        import logging
        from airflow_mcp.ops.notifier import notify_audit_tamper

        with caplog.at_level(logging.INFO, logger="airflow_mcp.ops.notifier"):
            notify_audit_tamper(
                date="2024-01-01",
                expected_hash="abc123",
                actual_hash="def456",
            )

        combined = " ".join(r.message for r in caplog.records)
        assert "abc123" in combined or "2024-01-01" in combined

    def test_slack_not_called_without_webhook_env(self):
        """AIRFLOW_SLACK_WEBHOOK が未設定なら Slack は呼ばれない。"""
        from airflow_mcp.ops.notifier import send_notification

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AIRFLOW_SLACK_WEBHOOK", None)
            with patch("httpx.post") as mock_post:
                send_notification(
                    "needs_human",
                    {"task_id": "t1", "title": "x", "section": "e",
                     "agent_id": "a", "reason": "r"},
                )
                mock_post.assert_not_called()

    def test_slack_called_when_webhook_configured(self):
        """AIRFLOW_SLACK_WEBHOOK が設定されていて enabled=True なら Slack に POST する。"""
        from airflow_mcp.ops.notifier import send_notification

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        slack_url = "https://hooks.slack.com/services/T00/B00/XXXXXXXXX"
        with (
            patch.dict(os.environ, {"AIRFLOW_SLACK_WEBHOOK": slack_url}),
            patch("airflow_mcp.ops.notifier._load_config",
                  return_value={
                      "channels": {
                          "slack": {"enabled": True, "default_channel": "#test"},
                          "cli": {"enabled": False},
                      }
                  }),
            patch("httpx.post", return_value=mock_resp) as mock_post,
        ):
            send_notification(
                "needs_human",
                {"task_id": "t1", "title": "x", "section": "e",
                 "agent_id": "a", "reason": "r"},
            )
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert slack_url in str(call_kwargs)


# ── シナリオ 3: audit_integrity ───────────────────────────────────

class TestAuditIntegrity:

    def test_compute_chain_hash_is_deterministic(self, integrity_dir: Path, monkeypatch):
        """同じ入力で同じハッシュが返ること。"""
        from airflow_mcp.ops.audit_integrity import compute_chain_hash
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)

        h1 = compute_chain_hash("2024-01-01", prev_hash="abc")
        h2 = compute_chain_hash("2024-01-01", prev_hash="abc")
        assert h1 == h2

    def test_different_prev_hash_gives_different_result(self, integrity_dir: Path, monkeypatch):
        """prev_hash が異なると別のチェーンハッシュになる。"""
        from airflow_mcp.ops.audit_integrity import compute_chain_hash
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)

        h1 = compute_chain_hash("2024-01-01", prev_hash="aaa")
        h2 = compute_chain_hash("2024-01-01", prev_hash="bbb")
        assert h1 != h2

    def test_run_chain_hash_creates_hash_file(self, integrity_dir: Path, monkeypatch):
        """run_chain_hash() が integrity ディレクトリにハッシュファイルを作成する。"""
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)
        monkeypatch.setattr(
            "airflow_mcp.ops.audit_integrity._INTEGRITY_DIR",
            integrity_dir / "integrity",
        )
        (integrity_dir / "integrity").mkdir(exist_ok=True)

        from airflow_mcp.ops.audit_integrity import run_chain_hash
        result = run_chain_hash("2024-01-01")

        assert "hash" in result
        assert result["date"] == "2024-01-01"
        hash_file = integrity_dir / "integrity" / "2024-01-01.hash"
        assert hash_file.exists()

    def test_verify_chain_passes_for_untampered_log(self, integrity_dir: Path, monkeypatch):
        """改ざんのないチェーンは verify_chain() が空リストを返す。"""
        int_dir = integrity_dir / "integrity"
        int_dir.mkdir(exist_ok=True)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._INTEGRITY_DIR", int_dir)

        from airflow_mcp.ops.audit_integrity import run_chain_hash, verify_chain

        run_chain_hash("2024-01-01")
        run_chain_hash("2024-01-02")

        failures = verify_chain()
        assert failures == []

    def test_verify_chain_detects_tampering(self, integrity_dir: Path, monkeypatch):
        """ハッシュファイルを改ざんすると verify_chain() が検出する。"""
        int_dir = integrity_dir / "integrity"
        int_dir.mkdir(exist_ok=True)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._INTEGRITY_DIR", int_dir)

        from airflow_mcp.ops.audit_integrity import run_chain_hash, verify_chain

        run_chain_hash("2024-01-01")

        # ハッシュを改ざん
        hash_file = int_dir / "2024-01-01.hash"
        record = json.loads(hash_file.read_text())
        record["hash"] = "tampered_hash_value_0000000000000000"
        hash_file.write_text(json.dumps(record))

        failures = verify_chain()
        assert len(failures) == 1
        assert failures[0]["date"] == "2024-01-01"
        assert failures[0]["actual_hash"] == "tampered_hash_value_0000000000000000"

    def test_run_chain_hash_result_has_required_fields(self, integrity_dir: Path, monkeypatch):
        """run_chain_hash() の戻り値が必須フィールドを持つ。"""
        int_dir = integrity_dir / "integrity"
        int_dir.mkdir(exist_ok=True)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._INTEGRITY_DIR", int_dir)

        from airflow_mcp.ops.audit_integrity import run_chain_hash
        result = run_chain_hash("2024-03-15")

        for field in ("date", "hash", "prev_hash", "computed_at"):
            assert field in result, f"フィールド '{field}' が存在しない"
        assert result["date"] == "2024-03-15"
        assert len(result["hash"]) == 64  # SHA256 = 64 hex chars

    def test_chain_links_previous_hash(self, integrity_dir: Path, monkeypatch):
        """2 日目のハッシュが 1 日目のハッシュを prev_hash として持つ。"""
        int_dir = integrity_dir / "integrity"
        int_dir.mkdir(exist_ok=True)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._LOGS_DIR", integrity_dir)
        monkeypatch.setattr("airflow_mcp.ops.audit_integrity._INTEGRITY_DIR", int_dir)

        from airflow_mcp.ops.audit_integrity import run_chain_hash
        day1 = run_chain_hash("2024-01-01")
        day2 = run_chain_hash("2024-01-02")

        assert day2["prev_hash"] == day1["hash"]

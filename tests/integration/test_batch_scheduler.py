"""batch_scheduler × TaskStore × notifier 結合テスト（DDD-08 §7）。

結合対象:
  orchestrator/batch_scheduler.py  × taskboard/store.py  × ops/notifier.py

検証シナリオ:
  1. cron パーサー — 分・時・曜日のマッチ判定
  2. run_schedule() — TaskStore へのタスク生成・audit アクション
  3. run_due_schedules() — 現在時刻にマッチするスケジュールのみ実行
  4. notifier 連携 — バッチ完了後に notify_batch_complete() が呼ばれる
  5. 複数アクション — dispatch + audit + cleaner の複合スケジュール
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from airflow_mcp.orchestrator.batch_scheduler import (
    _parse_cron,
    run_due_schedules,
    run_schedule,
)

os.environ.setdefault("OLLAMA_STUB", "true")

# ── フィクスチャ ──────────────────────────────────────────────────

@pytest.fixture
def schedules_file(tmp_path: Path, monkeypatch) -> Path:
    """一時 schedules.yaml を作成し環境変数で参照させる。"""
    path = tmp_path / "schedules.yaml"
    monkeypatch.setenv("AIRFLOW_SCHEDULES", str(path))
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._SCHEDULES_PATH", path
    )
    return path


def _write_schedules(path: Path, schedules: list[dict[str, Any]]) -> None:
    path.write_text(yaml.dump({"schedules": schedules}))


# ── シナリオ 1: cron パーサー ─────────────────────────────────────

class TestCronParser:

    def _dt(self, hour: int, minute: int, weekday: int) -> datetime:
        """テスト用 datetime を生成（weekday: 0=月曜）。"""
        from datetime import timedelta
        base = datetime(2024, 1, 1, hour, minute, tzinfo=timezone.utc)  # 月曜
        return base + timedelta(days=weekday)

    def test_wildcard_matches_any(self):
        """* は任意の値にマッチする。"""
        assert _parse_cron("* * * * *", self._dt(3, 30, 0)) is True

    def test_exact_hour_and_minute_match(self):
        """時・分が一致するとマッチする。"""
        assert _parse_cron("30 2 * * *", self._dt(2, 30, 0)) is True

    def test_exact_hour_mismatch(self):
        """時が異なればマッチしない。"""
        assert _parse_cron("30 3 * * *", self._dt(2, 30, 0)) is False

    def test_exact_minute_mismatch(self):
        """分が異なればマッチしない。"""
        assert _parse_cron("0 2 * * *", self._dt(2, 30, 0)) is False

    def test_weekday_match(self):
        """曜日が一致するとマッチする（POSIX: 1=月曜）。"""
        assert _parse_cron("0 0 * * 1", self._dt(0, 0, 0)) is True

    def test_weekday_mismatch(self):
        """曜日が異なればマッチしない。"""
        assert _parse_cron("0 0 * * 5", self._dt(0, 0, 0)) is False  # 金曜(5) vs 月曜(1)

    def test_comma_separated_hours(self):
        """カンマ区切りの複数値にマッチする。"""
        assert _parse_cron("0 2,14 * * *", self._dt(14, 0, 0)) is True

    def test_invalid_cron_returns_false(self):
        """不正な cron 式は False を返す。"""
        assert _parse_cron("invalid", datetime.now(timezone.utc)) is False

    def test_five_field_required(self):
        """フィールドが 5 つでないと False。"""
        assert _parse_cron("0 2 * *", datetime.now(timezone.utc)) is False


# ── シナリオ 2: run_schedule() ────────────────────────────────────

class TestRunSchedule:

    def test_dispatch_action_creates_tasks_in_store(self):
        """dispatch アクションが TaskStore にタスクを作成すること。"""
        schedule = {
            "name": "nightly_qa",
            "actions": [{
                "dispatch": {
                    "tasks": ["qa_review"],
                    "targets": ["agent_qa"],
                    "tag": "nightly",
                }
            }],
        }
        result = run_schedule(schedule)
        assert result["success"] == 1
        assert result["total"] == 1

        from airflow_mcp.taskboard.store import TaskStore
        store = TaskStore()
        tasks = store.list_all()
        assert any("nightly_qa" in t.get("title", "") for t in tasks)

    def test_dispatch_creates_one_task_per_target(self):
        """targets が複数の場合、タスク×ターゲット数だけ作成される。"""
        from airflow_mcp.taskboard.store import TaskStore
        store = TaskStore()
        before = len([t for t in store.list_all() if "multi_target" in t.get("title", "")])

        schedule = {
            "name": "multi_target",
            "actions": [{
                "dispatch": {
                    "tasks": ["review"],
                    "targets": ["agent_a", "agent_b", "agent_c"],
                }
            }],
        }
        run_schedule(schedule)

        after = len([t for t in store.list_all() if "multi_target" in t.get("title", "")])
        assert after - before == 3

    def test_audit_action_executes_without_error(self):
        """audit アクションがエラーなく実行されること。"""
        schedule = {
            "name": "audit_check",
            "actions": [{"audit": {}}],
        }
        result = run_schedule(schedule)
        assert result["success"] == 1

    def test_cleaner_action_executes_without_error(self):
        """cleaner アクションがエラーなく実行されること（stub）。"""
        schedule = {
            "name": "nightly_clean",
            "actions": [{"cleaner": {}}],
        }
        result = run_schedule(schedule)
        assert result["success"] == 1

    def test_result_contains_required_fields(self):
        """run_schedule() の戻り値が必須フィールドを含む。"""
        schedule = {"name": "test_sched", "actions": [{"cleaner": {}}]}
        result = run_schedule(schedule)
        for field in ("name", "success", "total", "duration_s"):
            assert field in result

    def test_duration_s_is_non_negative(self):
        """duration_s は 0 以上の数値であること。"""
        schedule = {"name": "quick", "actions": [{"cleaner": {}}]}
        result = run_schedule(schedule)
        assert result["duration_s"] >= 0.0

    def test_legacy_agents_format_creates_tasks(self):
        """旧形式（agents フィールド）のスケジュールもタスクを生成する。"""
        from airflow_mcp.taskboard.store import TaskStore
        store = TaskStore()
        before = len([t for t in store.list_all() if "legacy_batch" in t.get("title", "")])

        schedule = {
            "name": "legacy_batch",
            "agents": ["agent_docs", "agent_qa"],
            "task_template": "weekly_report",
            "category": "content",
        }
        run_schedule(schedule)

        after = len([t for t in store.list_all() if "legacy_batch" in t.get("title", "")])
        assert after - before == 2


# ── シナリオ 3: run_due_schedules() ──────────────────────────────

class TestRunDueSchedules:

    def test_matching_schedule_is_executed(self, schedules_file: Path):
        """現在時刻にマッチするスケジュールが実行される。"""
        now = datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc)  # 月曜 02:00
        _write_schedules(schedules_file, [{
            "name": "match_test",
            "cron": "0 2 * * 1",  # 月曜 02:00 (POSIX: 1=月)
            "actions": [{"cleaner": {}}],
        }])

        results = run_due_schedules(now=now)
        assert len(results) == 1
        assert results[0]["name"] == "match_test"

    def test_non_matching_schedule_is_skipped(self, schedules_file: Path):
        """現在時刻にマッチしないスケジュールは実行されない。"""
        now = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)  # 03:00
        _write_schedules(schedules_file, [{
            "name": "skip_test",
            "cron": "0 2 * * *",  # 02:00 のみ
            "actions": [{"cleaner": {}}],
        }])

        results = run_due_schedules(now=now)
        assert results == []

    def test_multiple_matching_schedules_all_run(self, schedules_file: Path):
        """複数のマッチするスケジュールがすべて実行される。"""
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        _write_schedules(schedules_file, [
            {"name": "sched_a", "cron": "0 0 * * *", "actions": [{"cleaner": {}}]},
            {"name": "sched_b", "cron": "0 0 * * *", "actions": [{"cleaner": {}}]},
            {"name": "sched_c", "cron": "0 1 * * *", "actions": [{"cleaner": {}}]},
        ])

        results = run_due_schedules(now=now)
        names = [r["name"] for r in results]
        assert "sched_a" in names
        assert "sched_b" in names
        assert "sched_c" not in names

    def test_empty_schedules_file_returns_empty(self, schedules_file: Path):
        """スケジュールが空の場合は空リストを返す。"""
        _write_schedules(schedules_file, [])
        results = run_due_schedules()
        assert results == []

    def test_missing_schedules_file_returns_empty(self, monkeypatch, tmp_path):
        """schedules.yaml が存在しない場合は空リストを返す。"""
        missing = tmp_path / "nonexistent.yaml"
        monkeypatch.setattr(
            "airflow_mcp.orchestrator.batch_scheduler._SCHEDULES_PATH", missing
        )
        results = run_due_schedules()
        assert results == []


# ── シナリオ 4: notifier 連携 ─────────────────────────────────────

class TestNotifierIntegration:

    def test_notify_batch_complete_called_after_run_schedule(self):
        """run_schedule() 完了後に notify_batch_complete() が呼ばれること。"""
        schedule = {"name": "notify_test", "actions": [{"cleaner": {}}]}

        with patch(
            "airflow_mcp.orchestrator.batch_scheduler.notify_batch_complete"
        ) as mock_notify:
            run_schedule(schedule)

        mock_notify.assert_called_once()
        args = mock_notify.call_args[0]
        assert args[0] == "notify_test"   # name
        assert args[1] == 1               # success
        assert args[2] == 1               # total
        assert args[3] >= 0.0             # duration_s

    def test_notify_receives_correct_success_count(self):
        """成功アクション数が正確に通知される。"""
        schedule = {
            "name": "multi_action",
            "actions": [{"cleaner": {}}, {"cleaner": {}}, {"cleaner": {}}],
        }

        with patch(
            "airflow_mcp.orchestrator.batch_scheduler.notify_batch_complete"
        ) as mock_notify:
            run_schedule(schedule)

        args = mock_notify.call_args[0]
        assert args[1] == 3  # 3 アクションすべて成功
        assert args[2] == 3  # total=3

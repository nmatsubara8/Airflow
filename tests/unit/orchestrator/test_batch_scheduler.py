"""orchestrator/batch_scheduler.py の単体テスト（DDD-08 §7）。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from airflow_mcp.orchestrator.batch_scheduler import _parse_cron, run_due_schedules, run_schedule


# ── _parse_cron ───────────────────────────────────────

def _dt(hour: int, minute: int, weekday: int = 0) -> datetime:
    return datetime(2026, 5, 25, hour, minute, tzinfo=timezone.utc).replace(
        # weekday: 0=Mon, 1=Tue, ... Using datetime directly
    )


def test_parse_cron_matches_exact_time():
    now = datetime(2026, 5, 25, 2, 0, tzinfo=timezone.utc)  # Mon 02:00
    assert _parse_cron("0 2 * * *", now) is True


def test_parse_cron_no_match_wrong_hour():
    now = datetime(2026, 5, 25, 3, 0, tzinfo=timezone.utc)
    assert _parse_cron("0 2 * * *", now) is False


def test_parse_cron_no_match_wrong_minute():
    now = datetime(2026, 5, 25, 2, 1, tzinfo=timezone.utc)
    assert _parse_cron("0 2 * * *", now) is False


def test_parse_cron_weekday_filter():
    # POSIX cron: 0=Sun, 1=Mon, ..., 6=Sat
    mon = datetime(2026, 5, 25, 9, 0, tzinfo=timezone.utc)  # Monday
    assert mon.weekday() == 0
    assert _parse_cron("0 9 * * 1", mon) is True  # cron 1=Mon


def test_parse_cron_comma_separated_weekdays():
    # POSIX cron: Tue=2, Fri=5
    tue = datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)  # Tuesday
    fri = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)  # Friday
    assert tue.weekday() == 1
    assert fri.weekday() == 4
    assert _parse_cron("0 9 * * 2,5", tue) is True
    assert _parse_cron("0 9 * * 2,5", fri) is True


def test_parse_cron_wildcard_matches_all():
    now = datetime(2026, 5, 25, 15, 30, tzinfo=timezone.utc)
    assert _parse_cron("30 15 * * *", now) is True


def test_parse_cron_invalid_format():
    now = datetime(2026, 5, 25, 2, 0, tzinfo=timezone.utc)
    assert _parse_cron("invalid", now) is False


# ── run_schedule ──────────────────────────────────────

def test_run_schedule_returns_result_dict(tmp_path, monkeypatch):
    schedule = {
        "name": "test_sched",
        "cron": "0 2 * * *",
        "actions": [{"cleaner": {"mode": "scan_and_dedupe"}}],
    }
    result = run_schedule(schedule)
    assert result["name"] == "test_sched"
    assert "success" in result
    assert "total" in result
    assert "duration_s" in result


def test_run_schedule_audit_action(monkeypatch):
    called = []
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._run_audit_action",
        lambda: called.append(1),
    )
    schedule = {
        "name": "audit_sched",
        "cron": "0 3 * * *",
        "actions": [{"audit": {"mode": "chain_hash"}}],
    }
    result = run_schedule(schedule)
    assert result["success"] == 1
    assert len(called) == 1


# ── run_due_schedules ─────────────────────────────────

def test_run_due_schedules_returns_empty_when_nothing_due(monkeypatch):
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._load_schedules",
        lambda: [{"name": "nightly", "cron": "0 2 * * *", "actions": [{"cleaner": {}}]}],
    )
    # 02:01 → not due
    now = datetime(2026, 5, 25, 2, 1, tzinfo=timezone.utc)
    results = run_due_schedules(now=now)
    assert results == []


def test_run_due_schedules_triggers_matching(monkeypatch):
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._load_schedules",
        lambda: [{"name": "nightly", "cron": "0 2 * * *", "actions": [{"cleaner": {}}]}],
    )
    # 02:00 → due
    now = datetime(2026, 5, 25, 2, 0, tzinfo=timezone.utc)
    results = run_due_schedules(now=now)
    assert len(results) == 1
    assert results[0]["name"] == "nightly"


# ── _load_schedules ───────────────────────────────────

def test_load_schedules_returns_empty_when_no_file(monkeypatch):
    import airflow_mcp.orchestrator.batch_scheduler as bs
    from pathlib import Path
    monkeypatch.setattr(bs, "_SCHEDULES_PATH", Path("/nonexistent/schedules.yaml"))
    from airflow_mcp.orchestrator.batch_scheduler import _load_schedules
    assert _load_schedules() == []


def test_load_schedules_normalizes_dict_format(tmp_path, monkeypatch):
    import airflow_mcp.orchestrator.batch_scheduler as bs
    f = tmp_path / "schedules.yaml"
    f.write_text("schedules:\n  nightly:\n    cron: '0 2 * * *'\n    agents: [agent_docs]\n")
    monkeypatch.setattr(bs, "_SCHEDULES_PATH", f)
    from airflow_mcp.orchestrator.batch_scheduler import _load_schedules
    schedules = _load_schedules()
    assert len(schedules) == 1
    assert schedules[0]["name"] == "nightly"


def test_load_schedules_returns_list_format(tmp_path, monkeypatch):
    import airflow_mcp.orchestrator.batch_scheduler as bs
    import yaml
    f = tmp_path / "schedules.yaml"
    f.write_text(yaml.dump({"schedules": [{"name": "daily", "cron": "0 9 * * *"}]}))
    monkeypatch.setattr(bs, "_SCHEDULES_PATH", f)
    from airflow_mcp.orchestrator.batch_scheduler import _load_schedules
    schedules = _load_schedules()
    assert schedules[0]["name"] == "daily"


# ── _parse_cron comma/wildcard edge cases ─────────────

def test_parse_cron_comma_minute():
    from airflow_mcp.orchestrator.batch_scheduler import _parse_cron
    now = datetime(2026, 5, 25, 9, 30, tzinfo=timezone.utc)
    assert _parse_cron("0,30 9 * * *", now) is True


# ── _run_dispatch_action ──────────────────────────────

def test_run_dispatch_action_creates_tasks(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.orchestrator.batch_scheduler import _run_dispatch_action
        action = {"dispatch": {"tasks": ["qa_review"], "targets": ["agent_qa"], "tag": "nightly"}}
        created = _run_dispatch_action(action, "nightly_batch")
    assert created == 1


# ── _run_schedule_agents ──────────────────────────────

def test_run_schedule_agents_creates_tasks_per_agent(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.orchestrator.batch_scheduler import _run_schedule_agents
        schedule = {
            "name": "nightly",
            "agents": ["agent_docs", "agent_qa"],
            "task_template": "review",
            "category": "engineering",
        }
        created = _run_schedule_agents(schedule)
    assert created == 2


# ── run_schedule with agents format ──────────────────

def test_run_schedule_agents_format(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.orchestrator.batch_scheduler import run_schedule
        schedule = {
            "name": "weekly_review",
            "cron": "0 9 * * 1",
            "agents": ["agent_docs"],
            "task_template": "weekly_review",
        }
        result = run_schedule(schedule)
    assert result["name"] == "weekly_review"
    assert result["success"] >= 1


# ── _parse_cron ValueError branch ────────────────────

def test_parse_cron_invalid_field_returns_false():
    from airflow_mcp.orchestrator.batch_scheduler import _parse_cron
    now = datetime(2026, 5, 25, 9, 0, tzinfo=timezone.utc)
    # "abc" is not "*", not comma-separated, and not parseable as int → ValueError → False
    assert _parse_cron("abc 9 * * *", now) is False


# ── _run_dispatch_action exception path ──────────────

def test_run_dispatch_action_exception_returns_zero():
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", side_effect=RuntimeError("db down")):
        from airflow_mcp.orchestrator.batch_scheduler import _run_dispatch_action
        action = {"dispatch": {"tasks": ["qa_review"], "targets": ["agent_qa"]}}
        created = _run_dispatch_action(action, "failing_batch")
    assert created == 0


# ── _run_audit_action ─────────────────────────────────

def test_run_audit_action_success(monkeypatch):
    import unittest.mock as mock
    with mock.patch(
        "airflow_mcp.orchestrator.batch_scheduler.run_chain_hash",
        return_value={"hash": "abcdef1234567890", "chain_length": 3},
    ):
        from airflow_mcp.orchestrator.batch_scheduler import _run_audit_action
        _run_audit_action()  # should not raise


def test_run_audit_action_exception_is_swallowed(monkeypatch):
    import unittest.mock as mock
    with mock.patch(
        "airflow_mcp.orchestrator.batch_scheduler.run_chain_hash",
        side_effect=RuntimeError("audit db error"),
    ):
        from airflow_mcp.orchestrator.batch_scheduler import _run_audit_action
        _run_audit_action()  # should not raise


# ── _run_schedule_agents exception path ──────────────

def test_run_schedule_agents_exception_returns_zero():
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", side_effect=RuntimeError("store error")):
        from airflow_mcp.orchestrator.batch_scheduler import _run_schedule_agents
        schedule = {"name": "nightly", "agents": ["agent_docs"], "task_template": "review"}
        created = _run_schedule_agents(schedule)
    assert created == 0


# ── run_schedule dispatch branch (no agents field) ───

def test_run_schedule_dispatch_without_agents(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    import unittest.mock as mock
    with mock.patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.orchestrator.batch_scheduler import run_schedule
        schedule = {
            "name": "direct_dispatch",
            "actions": [{"dispatch": {"tasks": ["qa_review"], "targets": ["agent_qa"], "tag": "nightly"}}],
        }
        result = run_schedule(schedule)
    assert result["name"] == "direct_dispatch"
    assert result["success"] == 1


# ── run_schedule per-action exception path ───────────

def test_run_schedule_action_exception_is_caught(monkeypatch):
    import unittest.mock as mock
    with mock.patch(
        "airflow_mcp.orchestrator.batch_scheduler._run_audit_action",
        side_effect=RuntimeError("audit exploded"),
    ):
        from airflow_mcp.orchestrator.batch_scheduler import run_schedule
        schedule = {
            "name": "failing_audit",
            "actions": [{"audit": {"mode": "chain_hash"}}],
        }
        result = run_schedule(schedule)
    # exception is caught; success stays 0, total is 1
    assert result["total"] == 1
    assert result["success"] == 0


# ── run_due_schedules logger.info line ───────────────

def test_run_due_schedules_logger_triggered(monkeypatch):
    import unittest.mock as mock
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._load_schedules",
        lambda: [{"name": "nightly", "cron": "0 2 * * *", "actions": [{"cleaner": {}}]}],
    )
    now = datetime(2026, 5, 25, 2, 0, tzinfo=timezone.utc)
    with mock.patch("airflow_mcp.orchestrator.batch_scheduler.logger") as mock_logger:
        from airflow_mcp.orchestrator.batch_scheduler import run_due_schedules
        results = run_due_schedules(now=now)
    assert len(results) == 1
    mock_logger.info.assert_any_call(
        "batch.triggered schedule=%s cron=%s", "nightly", "0 2 * * *"
    )


def test_run_due_schedules_uses_utcnow_when_no_arg(monkeypatch):
    """Cover the `now = datetime.now(timezone.utc)` default branch (line 175)."""
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler._load_schedules",
        lambda: [],
    )
    from airflow_mcp.orchestrator.batch_scheduler import run_due_schedules
    results = run_due_schedules()  # no now= argument → takes default branch
    assert results == []


# ── run_loop ──────────────────────────────────────────

def test_run_loop_runs_once_then_stops(monkeypatch):
    """run_loop is an infinite loop; return a result first (covers lines 196-197), then stop."""

    call_count = 0

    def fake_run_due_schedules():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [{"name": "nightly", "success": 1, "total": 1, "duration_s": 0.1}]
        raise SystemExit(0)

    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler.run_due_schedules",
        fake_run_due_schedules,
    )
    monkeypatch.setattr("airflow_mcp.orchestrator.batch_scheduler.time.sleep", lambda s: None)

    from airflow_mcp.orchestrator.batch_scheduler import run_loop
    with pytest.raises(SystemExit):
        run_loop(poll_interval_s=0.001)
    assert call_count == 2


def test_run_loop_handles_exception(monkeypatch):
    """run_loop catches exceptions in the main loop body."""

    call_count = 0

    def fake_run_due_schedules():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient error")
        raise SystemExit(0)

    monkeypatch.setattr(
        "airflow_mcp.orchestrator.batch_scheduler.run_due_schedules",
        fake_run_due_schedules,
    )
    monkeypatch.setattr("airflow_mcp.orchestrator.batch_scheduler.time.sleep", lambda s: None)

    from airflow_mcp.orchestrator.batch_scheduler import run_loop
    with pytest.raises(SystemExit):
        run_loop(poll_interval_s=0.001)
    assert call_count == 2

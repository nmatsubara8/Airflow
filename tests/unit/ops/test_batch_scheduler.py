"""ops/batch_scheduler.py の単体テスト（DDD-08 §7）。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from airflow_mcp.ops.batch_scheduler import _parse_cron, run_due_schedules, run_schedule


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
    # 0=Mon in python weekday(), cron dow 1=Mon
    mon = datetime(2026, 5, 25, 9, 0, tzinfo=timezone.utc)  # Monday
    assert mon.weekday() == 0
    assert _parse_cron("0 9 * * 0", mon) is True


def test_parse_cron_comma_separated_weekdays():
    # Tuesday (weekday=1) and Friday (weekday=4)
    tue = datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)  # Tuesday
    fri = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)  # Friday
    assert tue.weekday() == 1
    assert fri.weekday() == 4
    assert _parse_cron("0 9 * * 1,4", tue) is True
    assert _parse_cron("0 9 * * 1,4", fri) is True


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
        "airflow_mcp.ops.batch_scheduler._run_audit_action",
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
        "airflow_mcp.ops.batch_scheduler._load_schedules",
        lambda: [{"name": "nightly", "cron": "0 2 * * *", "actions": [{"cleaner": {}}]}],
    )
    # 02:01 → not due
    now = datetime(2026, 5, 25, 2, 1, tzinfo=timezone.utc)
    results = run_due_schedules(now=now)
    assert results == []


def test_run_due_schedules_triggers_matching(monkeypatch):
    monkeypatch.setattr(
        "airflow_mcp.ops.batch_scheduler._load_schedules",
        lambda: [{"name": "nightly", "cron": "0 2 * * *", "actions": [{"cleaner": {}}]}],
    )
    # 02:00 → due
    now = datetime(2026, 5, 25, 2, 0, tzinfo=timezone.utc)
    results = run_due_schedules(now=now)
    assert len(results) == 1
    assert results[0]["name"] == "nightly"

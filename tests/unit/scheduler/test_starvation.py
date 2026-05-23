"""scheduler/starvation.py の単体テスト（DDD-07 §6）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from airflow_mcp.scheduler.starvation import (
    check_starvation_flags,
    compute_aging_score,
    is_force_priority,
)

_CONFIG = {
    "max_wait_before_boost_s": 600,
    "aging_factor": 0.1,
    "force_run_s": 3600,
    "deadline_warn_s": 300,
}

_NOW = datetime.now(timezone.utc)


def _ago(seconds: float) -> datetime:
    return _NOW - timedelta(seconds=seconds)


def _future(seconds: float) -> datetime:
    return _NOW + timedelta(seconds=seconds)


# ── compute_aging_score ──────────────────────────────

def test_aging_score_zero_before_threshold():
    score = compute_aging_score(_ago(300), _CONFIG)
    assert score == 0.0


def test_aging_score_zero_at_threshold():
    score = compute_aging_score(_ago(600), _CONFIG)
    assert score == pytest.approx(0.0, abs=0.05)  # 実行時間ズレを許容


def test_aging_score_positive_after_threshold():
    score = compute_aging_score(_ago(660), _CONFIG)  # 60秒超過 → 0.1
    assert score == pytest.approx(0.1, abs=0.02)


def test_aging_score_increases_with_time():
    score_10min = compute_aging_score(_ago(600 + 600), _CONFIG)   # 10分超過
    score_5min = compute_aging_score(_ago(600 + 300), _CONFIG)    # 5分超過
    assert score_10min > score_5min


# ── check_starvation_flags ───────────────────────────

def test_no_flags_for_fresh_task():
    flags = check_starvation_flags(_ago(30), None, _CONFIG)
    assert flags == []


def test_aging_flag_after_threshold():
    flags = check_starvation_flags(_ago(700), None, _CONFIG)
    assert "aging" in flags


def test_force_run_flag_after_max_wait():
    flags = check_starvation_flags(_ago(3700), None, _CONFIG)
    assert "force_run" in flags
    # force_run は最上位 → aging は含まれない
    assert "aging" not in flags


def test_deadline_near_flag_when_close():
    flags = check_starvation_flags(_ago(10), _future(200), _CONFIG)
    assert "deadline_near" in flags


def test_no_deadline_flag_when_far():
    flags = check_starvation_flags(_ago(10), _future(3600), _CONFIG)
    assert "deadline_near" not in flags


def test_force_run_overrides_deadline_flag():
    # force_run 発動中は deadline_near は返さない
    flags = check_starvation_flags(_ago(3700), _future(100), _CONFIG)
    assert "force_run" in flags
    assert "deadline_near" not in flags


# ── is_force_priority ────────────────────────────────

def test_is_force_priority_true_for_force_run():
    assert is_force_priority(["force_run"]) is True


def test_is_force_priority_true_for_deadline_near():
    assert is_force_priority(["deadline_near"]) is True


def test_is_force_priority_false_for_aging_only():
    assert is_force_priority(["aging"]) is False


def test_is_force_priority_false_for_empty():
    assert is_force_priority([]) is False

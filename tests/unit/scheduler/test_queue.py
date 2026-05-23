"""scheduler/queue.py の単体テスト（DDD-07 §5）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from airflow_mcp.scheduler.queue import QueueEntry, SchedulerQueue

_CONFIG = {
    "max_consecutive_same_model": 3,
    "max_wait_before_boost_s": 600,
    "aging_factor": 0.1,
    "force_run_s": 3600,
    "deadline_warn_s": 300,
}


def _entry(task_id: str, model_id: str, priority: int = 5,
           wait_s: float = 0) -> QueueEntry:
    t = datetime.now(timezone.utc) - timedelta(seconds=wait_s)
    return QueueEntry(task_id=task_id, model_id=model_id, base_priority=priority, enqueued_at=t)


# ── enqueue / depth ──────────────────────────────────

def test_enqueue_increases_depth():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t1", "model_14b"))
    assert q.depth("model_14b") == 1
    assert q.depth() == 1


def test_enqueue_multiple_models():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t1", "model_14b"))
    q.enqueue(_entry("t2", "model_32b"))
    assert q.depth() == 2
    assert q.model_depths() == {"model_14b": 1, "model_32b": 1}


# ── dequeue 基本 ──────────────────────────────────────

def test_dequeue_prefers_loaded_model():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t_14b", "model_14b"))
    q.enqueue(_entry("t_32b", "model_32b"))

    entry = q.dequeue(loaded_model_ids=["model_14b"])
    assert entry is not None
    assert entry.model_id == "model_14b"


def test_dequeue_returns_none_when_empty():
    q = SchedulerQueue(_CONFIG)
    assert q.dequeue([]) is None


def test_dequeue_reduces_depth():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t1", "model_14b"))
    q.dequeue(["model_14b"])
    assert q.depth() == 0


def test_dequeue_returns_unloaded_when_no_loaded_queued():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t1", "model_32b"))
    entry = q.dequeue(loaded_model_ids=["model_14b"])  # 14b は loaded だがキュー空
    assert entry is not None
    assert entry.model_id == "model_32b"


# ── 優先度 ────────────────────────────────────────────

def test_dequeue_respects_priority():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("low", "model_14b", priority=2))
    q.enqueue(_entry("high", "model_14b", priority=8))
    entry = q.dequeue(["model_14b"])
    assert entry.task_id == "high"


# ── 連続実行上限 ──────────────────────────────────────

def test_dequeue_switches_after_consecutive_limit():
    q = SchedulerQueue(_CONFIG)
    # max_consecutive = 3
    q.enqueue(_entry("t1", "model_14b"))
    q.enqueue(_entry("t2", "model_14b"))
    q.enqueue(_entry("t3", "model_14b"))
    q.enqueue(_entry("t4", "model_14b"))
    q.enqueue(_entry("t5", "model_32b"))

    # 3 回連続 14b を dequeue した後、32b に切り替わるはず
    for _ in range(3):
        e = q.dequeue(["model_14b", "model_32b"])
        assert e.model_id == "model_14b"

    e = q.dequeue(["model_14b", "model_32b"])
    assert e.model_id == "model_32b"


# ── スターベーション ──────────────────────────────────

def test_dequeue_force_priority_overrides_loaded_preference():
    q = SchedulerQueue(_CONFIG)
    # 14b はロード済みで通常優先、32b は古くて force_run
    q.enqueue(_entry("normal", "model_14b", priority=5))
    old_entry = _entry("starved", "model_32b", priority=1, wait_s=3700)
    q.enqueue(old_entry)

    entry = q.dequeue(loaded_model_ids=["model_14b"])
    assert entry.task_id == "starved"


# ── all_entries ───────────────────────────────────────

def test_all_entries_returns_all():
    q = SchedulerQueue(_CONFIG)
    q.enqueue(_entry("t1", "model_14b"))
    q.enqueue(_entry("t2", "model_32b"))
    entries = q.all_entries()
    assert len(entries) == 2
    ids = {e.task_id for e in entries}
    assert ids == {"t1", "t2"}

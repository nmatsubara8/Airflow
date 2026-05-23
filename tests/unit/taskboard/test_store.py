"""TaskStore の単体テスト（DDD-04 §3）。"""
from __future__ import annotations

import threading
from pathlib import Path

import pytest

from airflow_mcp.common.errors import (
    TaskAlreadyClaimed,
    TaskInvalidTransition,
    TaskLockConflict,
    TaskNotFound,
)
from airflow_mcp.taskboard.store import TaskStore


# ── CRUD ─────────────────────────────────────────────────

def test_create_returns_inbox_task(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    assert task["status"] == "inbox"
    assert task["title"] == "テストタスク"
    assert task["lock"]["version"] == 1
    assert task["lock"]["etag"].startswith("sha256:")


def test_get_returns_created_task(task_store: TaskStore, sample_task_fields: dict):
    created = task_store.create(sample_task_fields)
    fetched = task_store.get(created["id"])
    assert fetched["id"] == created["id"]


def test_get_raises_task_not_found(task_store: TaskStore):
    with pytest.raises(TaskNotFound):
        task_store.get("task_nonexistent")


def test_list_by_status_returns_correct_tasks(task_store: TaskStore, sample_task_fields: dict):
    task_store.create(sample_task_fields)
    task_store.create({**sample_task_fields, "title": "タスク2"})
    inbox = task_store.list_by_status("inbox")
    assert len(inbox) == 2
    assert all(t["status"] == "inbox" for t in inbox)


def test_list_by_status_empty_when_no_tasks(task_store: TaskStore):
    assert task_store.list_by_status("inbox") == []


# ── 楽観的ロック ──────────────────────────────────────────

def test_update_succeeds_with_correct_version(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    updated = task_store.update(task["id"], 1, {"description": "更新済み"})
    assert updated["description"] == "更新済み"
    assert updated["lock"]["version"] == 2


def test_update_raises_lock_conflict_on_stale_version(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.update(task["id"], 1, {"description": "first"})
    with pytest.raises(TaskLockConflict):
        task_store.update(task["id"], 1, {"description": "conflict"})


def test_update_ignores_immutable_fields(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    original_id = task["id"]
    updated = task_store.update(task["id"], 1, {
        "id": "tampered_id",
        "schema_version": "9.9.9",
        "created_at": "1970-01-01T00:00:00Z",
        "title": "正当な変更",
    })
    assert updated["id"] == original_id
    assert updated["schema_version"] == "1.0.0"
    assert updated["title"] == "正当な変更"


def test_concurrent_update_only_one_succeeds(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_id = task["id"]
    results: list[Exception | None] = [None, None]

    def try_update(idx: int) -> None:
        try:
            task_store.update(task_id, 1, {"description": f"writer-{idx}"})
            results[idx] = None
        except TaskLockConflict as e:
            results[idx] = e

    t0 = threading.Thread(target=try_update, args=(0,))
    t1 = threading.Thread(target=try_update, args=(1,))
    t0.start(); t1.start()
    t0.join(); t1.join()

    conflicts = sum(1 for r in results if isinstance(r, TaskLockConflict))
    assert conflicts == 1, "並行更新で片方だけが成功すること"


# ── ステータス遷移 ────────────────────────────────────────

def test_transition_inbox_to_in_progress(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    transitioned = task_store.transition(task["id"], "in_progress")
    assert transitioned["status"] == "in_progress"


def test_transition_raises_invalid_transition(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    with pytest.raises(TaskInvalidTransition):
        task_store.transition(task["id"], "done")  # inbox → done は不正


def test_transition_done_moves_to_monthly_subdir(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_store.transition(task["id"], "done")
    done = task_store.list_by_status("done")
    assert len(done) == 1
    assert done[0]["status"] == "done"


def test_transition_to_needs_human_preserves_notes(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_store.add_handoff_note(task["id"], "処理失敗", author="agent_docs")
    task_store.transition(task["id"], "needs_human", reason="schema invalid")
    fetched = task_store.get(task["id"])
    assert fetched["status"] == "needs_human"
    assert len(fetched["handoff_notes"]) == 1


# ── claim ─────────────────────────────────────────────────

def test_claim_moves_task_to_in_progress(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    claimed = task_store.claim(task["id"], "agent_docs")
    assert claimed["status"] == "in_progress"
    assert claimed["owner"] == "agent_docs"


def test_claim_raises_invalid_transition_when_already_in_progress(
    task_store: TaskStore, sample_task_fields: dict
):
    # in_progress 状態での再 claim は TaskInvalidTransition（DDD-04 §3.3）
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    with pytest.raises(TaskInvalidTransition):
        task_store.claim(task["id"], "agent_docs")


def test_claim_raises_already_claimed_by_other(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    with pytest.raises(TaskAlreadyClaimed):
        task_store.claim(task["id"], "agent_architect")


# ── handoff_note ──────────────────────────────────────────

def test_add_handoff_note_appends_to_list(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_store.add_handoff_note(task["id"], "最初のノート", author="agent_docs")
    task_store.add_handoff_note(task["id"], "2番目のノート", author="agent_docs")
    fetched = task_store.get(task["id"])
    assert len(fetched["handoff_notes"]) == 2
    assert fetched["handoff_notes"][0]["body"] == "最初のノート"


def test_add_handoff_note_has_required_fields(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    updated = task_store.add_handoff_note(task["id"], "内容", author="agent_docs")
    note = updated["handoff_notes"][0]
    assert "id" in note
    assert "body" in note
    assert "author" in note
    assert "timestamp" in note
    assert note["author"]["type"] == "agent"
    assert note["author"]["id"] == "agent_docs"


# ── quarantine ────────────────────────────────────────────

def test_quarantine_moves_task_to_corrupt_dir(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.quarantine(task["id"], reason="破損ファイル検出")
    # quarantine 後は通常のステータスでは見つからない
    assert task_store.list_by_status("inbox") == []


# ── etag の一貫性 ─────────────────────────────────────────

def test_etag_changes_after_update(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    original_etag = task["lock"]["etag"]
    updated = task_store.update(task["id"], 1, {"description": "変更"})
    assert updated["lock"]["etag"] != original_etag


# ── assign (M-6 TOCTOU 対策) ──────────────────────────────

def test_assign_changes_owner_atomically(task_store: TaskStore, sample_task_fields: dict):
    """inbox タスクのオーナー変更がアトミックに行われること。"""
    task = task_store.create(sample_task_fields)
    updated = task_store.assign(task["id"], "human_reviewer_01")
    assert updated["owner"] == "human_reviewer_01"
    assert updated["lock"]["version"] == 2


def test_assign_raises_invalid_transition_when_in_progress(
    task_store: TaskStore, sample_task_fields: dict
):
    """in_progress タスクへの assign はロック内で TaskInvalidTransition を raise すること（M-6）。"""
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    with pytest.raises(TaskInvalidTransition):
        task_store.assign(task["id"], "human_reviewer_01")


def test_assign_toctou_claim_wins(task_store: TaskStore, sample_task_fields: dict):
    """claim と assign が競合した場合、claim 後の assign は TaskInvalidTransition になること（M-6）。

    これは旧実装の TOCTOU ウィンドウに相当するシナリオ:
      スレッド A が get() でステータス確認（inbox）
      スレッド B が claim() で in_progress へ遷移
      スレッド A が assign() しようとすると → TaskInvalidTransition
    """
    task = task_store.create(sample_task_fields)
    task_id = task["id"]

    barrier = threading.Barrier(2)
    results: list[Exception | None] = [None, None]

    def do_claim():
        barrier.wait()
        try:
            task_store.claim(task_id, "agent_docs")
        except Exception as e:
            results[0] = e

    def do_assign():
        barrier.wait()
        try:
            task_store.assign(task_id, "human_reviewer_01")
        except (TaskInvalidTransition, TaskLockConflict) as e:
            results[1] = e

    t_claim = threading.Thread(target=do_claim)
    t_assign = threading.Thread(target=do_assign)
    t_claim.start(); t_assign.start()
    t_claim.join(); t_assign.join()

    # claim か assign のどちらか一方だけが成功する
    failures = sum(1 for r in results if r is not None)
    assert failures == 1, "並行 claim/assign で片方だけが成功すること"
    # claim が成功した場合、assign は TaskInvalidTransition になること
    if results[0] is None:
        assert isinstance(results[1], TaskInvalidTransition)

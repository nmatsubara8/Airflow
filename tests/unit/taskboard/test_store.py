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
    t0.start()
    t1.start()
    t0.join()
    t1.join()

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
    t_claim.start()
    t_assign.start()
    t_claim.join()
    t_assign.join()

    # claim か assign のどちらか一方だけが成功する
    failures = sum(1 for r in results if r is not None)
    assert failures == 1, "並行 claim/assign で片方だけが成功すること"
    # claim が成功した場合、assign は TaskInvalidTransition になること
    if results[0] is None:
        assert isinstance(results[1], TaskInvalidTransition)


# ── 欠損行カバレッジ追加 ──────────────────────────────────
# 既存テストで使う "task_nonexistent" は _TASK_ID_RE に合致せず
# _validate_task_id で TaskNotFound が raise される（line 44）ため
# 各メソッド内部の TaskNotFound raise（lines 96,144,166,183,203,228,243）が未カバー。
# 有効フォーマット（^task_[a-z0-9]{16,32}$）で存在しない ID を使う。
_VALID_MISSING = "task_" + "a" * 16  # 存在しない有効 task_id


def test_find_task_path_returns_none_for_valid_missing_id(task_store):
    """_find_task_path が有効フォーマットかつ存在しない ID で None を返すこと（line 96）。"""
    path = task_store._find_task_path(_VALID_MISSING)
    assert path is None


def test_get_raises_task_not_found_for_valid_missing_id(task_store):
    """get() が有効フォーマットの存在しない ID で TaskNotFound（line 144）。"""
    with pytest.raises(TaskNotFound):
        task_store.get(_VALID_MISSING)


def test_update_raises_task_not_found_for_valid_missing_id(task_store):
    """update() が有効フォーマットの存在しない ID で TaskNotFound（line 166）。"""
    with pytest.raises(TaskNotFound):
        task_store.update(_VALID_MISSING, 1, {"title": "x"})


def test_transition_raises_task_not_found_for_valid_missing_id(task_store):
    """transition() が有効フォーマットの存在しない ID で TaskNotFound（line 183）。"""
    with pytest.raises(TaskNotFound):
        task_store.transition(_VALID_MISSING, "in_progress")


def test_claim_raises_task_not_found_for_valid_missing_id(task_store):
    """claim() が有効フォーマットの存在しない ID で TaskNotFound（line 203）。"""
    with pytest.raises(TaskNotFound):
        task_store.claim(_VALID_MISSING, "agent_docs")


def test_assign_raises_task_not_found_for_valid_missing_id(task_store):
    """assign() が有効フォーマットの存在しない ID で TaskNotFound（line 228）。"""
    with pytest.raises(TaskNotFound):
        task_store.assign(_VALID_MISSING, "human_reviewer")


def test_add_handoff_note_raises_task_not_found_for_valid_missing_id(task_store):
    """add_handoff_note() が有効フォーマットの存在しない ID で TaskNotFound（line 243）。"""
    with pytest.raises(TaskNotFound):
        task_store.add_handoff_note(_VALID_MISSING, "note", author="agent_docs")


def test_task_path_raises_value_error_for_invalid_status(task_store, sample_task_fields):
    """_task_path が STATUS_ALLOWLIST 外のステータスで ValueError（line 76）。"""
    task = task_store.create(sample_task_fields)
    with pytest.raises(ValueError, match="Invalid status"):
        task_store._task_path(task["id"], "invalid_status")


def test_list_by_status_skips_corrupt_json(task_store):
    """list_by_status が破損 JSON をスキップして残りを返すこと（lines 156-157）。"""
    # 正常タスクを作成
    task_store.create({"title": "正常", "section": "engineering",
                       "description": "", "tags": [], "priority": 5, "owner": None})
    # inbox/ 配下に破損 JSON を直接書く
    corrupt = task_store.base / "inbox" / "task_aaaaaaaaaaaaaaaa.json"
    corrupt.write_text("{ broken json }")
    result = task_store.list_by_status("inbox")
    assert len(result) == 1  # 正常な1件のみ返る


def test_find_task_path_finds_done_in_monthly_subdir(task_store, sample_task_fields):
    """done/ の月別サブディレクトリにある task_id を _find_task_path が返すこと（lines 93-95）。"""
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_store.transition(task["id"], "done")
    # done 遷移後に get() → _find_task_path が done/<month>/ を検索してヒット
    fetched = task_store.get(task["id"])
    assert fetched["status"] == "done"


def test_lock_cleanup_suppresses_oserror(task_store, sample_task_fields):
    """_lock が unlink で OSError を raise してもクラッシュしないこと（lines 113-114）。"""
    from unittest.mock import patch
    task = task_store.create(sample_task_fields)
    # unlink を OSError に差し替え → except OSError: pass が実行される
    with patch.object(Path, "unlink", side_effect=OSError("busy")):
        # _lock コンテキスト内では unlink が呼ばれる → suppressed
        with task_store._lock(task["id"]):
            pass  # 正常終了（ロック解放後の unlink が OSError になる）


def test_list_all_returns_tasks_across_all_statuses(task_store, sample_task_fields):
    """list_all() が全ステータスのタスクを返すこと。"""
    t1 = task_store.create(sample_task_fields)
    t2 = task_store.create({**sample_task_fields, "title": "別タスク"})
    task_store.claim(t2["id"], "agent_docs")  # → in_progress

    all_tasks = task_store.list_all()
    ids = [t["id"] for t in all_tasks]
    assert t1["id"] in ids
    assert t2["id"] in ids


def test_list_all_excludes_corrupt_directory(task_store, sample_task_fields):
    """list_all() が _corrupt ディレクトリを含まないこと。"""
    task_store.create(sample_task_fields)
    # _corrupt/ に直接ファイルを置いても list_all には出ない
    corrupt_dir = task_store.base / "_corrupt"
    corrupt_dir.mkdir(exist_ok=True)
    (corrupt_dir / "task_fake.json").write_text('{"id": "task_fake"}')

    all_tasks = task_store.list_all()
    assert all(t.get("id") != "task_fake" for t in all_tasks)


def test_quarantine_logs_reason(task_store: TaskStore, sample_task_fields: dict, caplog):
    """quarantine() が reason を logger.warning で出力すること。"""
    import logging
    task = task_store.create(sample_task_fields)
    with caplog.at_level(logging.WARNING, logger="airflow_mcp.taskboard.store"):
        task_store.quarantine(task["id"], reason="スキーマ不正")
    assert "スキーマ不正" in caplog.text


def test_quarantine_nonexistent_task_logs_warning(task_store: TaskStore, caplog):
    """存在しない task_id を quarantine しても例外が出ず warning が記録されること。"""
    import logging
    with caplog.at_level(logging.WARNING, logger="airflow_mcp.taskboard.store"):
        task_store.quarantine("task_zzzzzzzzzzzzzzzz", reason="テスト")
    assert "not found" in caplog.text


def test_quarantine_moves_file_with_shutil(task_store: TaskStore, sample_task_fields: dict):
    """quarantine() 後にファイルが _corrupt/ に移動していること。"""
    task = task_store.create(sample_task_fields)
    task_id = task["id"]
    task_store.quarantine(task_id, reason="テスト移動")
    corrupt_dir = task_store.base / "_corrupt"
    moved_files = list(corrupt_dir.glob(f"{task_id}.json"))
    assert len(moved_files) == 1


# ── 追加テスト: store.get() TOCTOU 保護 ───────────────

def test_get_raises_task_not_found_on_file_deleted_after_find(
    task_store: TaskStore, sample_task_fields: dict
):
    """_find_task_path() 後にファイルが削除された場合 TaskNotFound が発生すること（TOCTOU）。"""
    from unittest.mock import patch
    from airflow_mcp.common.errors import TaskNotFound
    task = task_store.create(sample_task_fields)
    task_id = task["id"]

    def fake_read_text(*args, **kwargs):
        raise OSError("simulated file deleted")

    with patch("pathlib.Path.read_text", side_effect=fake_read_text):
        with pytest.raises(TaskNotFound):
            task_store.get(task_id)


def test_get_raises_task_not_found_on_corrupt_json(
    task_store: TaskStore, sample_task_fields: dict, tmp_path
):
    """タスクファイルが破損 JSON の場合 TaskNotFound が発生すること。"""
    from airflow_mcp.common.errors import TaskNotFound
    task = task_store.create(sample_task_fields)
    task_id = task["id"]

    # ファイルを直接破損させる
    path = task_store._find_task_path(task_id)
    assert path is not None
    path.write_text("{ this is not valid json }")

    with pytest.raises(TaskNotFound):
        task_store.get(task_id)


def test_quarantine_oserror_on_move_is_logged(task_store: TaskStore, sample_task_fields: dict, caplog):
    """quarantine() で shutil.move が OSError のときエラーログが出てクラッシュしないこと（lines 293-294）。"""
    import logging
    from unittest.mock import patch
    task = task_store.create(sample_task_fields)
    task_id = task["id"]
    with patch("shutil.move", side_effect=OSError("no space")):
        with caplog.at_level(logging.ERROR, logger="airflow_mcp.taskboard.store"):
            task_store.quarantine(task_id, reason="test-error")
    assert "quarantine" in caplog.text

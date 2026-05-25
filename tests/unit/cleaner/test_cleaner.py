"""cleaner/main.py の単体テスト（TF-1）。"""
from __future__ import annotations

import gzip
import json
import time
from pathlib import Path


from airflow_mcp.cleaner.main import (
    _archive_old_tasks,
    _days_old,
    _purge_archived_tasks,
    _rotate_execution_log,
    load_config,
    run_once,
)


# ── load_config ───────────────────────────────────────────────

def test_load_config_reads_yaml(tmp_path):
    cfg = tmp_path / "cleaner.yaml"
    cfg.write_text("cleaner:\n  dry_run: true\n  tasks:\n    archive_done_after_days: 10\n")
    result = load_config(cfg)
    assert result["dry_run"] is True
    assert result["tasks"]["archive_done_after_days"] == 10


def test_load_config_returns_empty_when_missing(tmp_path):
    result = load_config(tmp_path / "nonexistent.yaml")
    assert result == {}


def test_load_config_returns_empty_when_no_cleaner_key(tmp_path):
    cfg = tmp_path / "cleaner.yaml"
    cfg.write_text("other: value\n")
    result = load_config(cfg)
    assert result == {}


# ── _days_old ─────────────────────────────────────────────────

def test_days_old_returns_float(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("x")
    assert isinstance(_days_old(f), float)
    assert _days_old(f) < 1.0  # just created


# ── _archive_old_tasks ────────────────────────────────────────

def _make_task_file(directory: Path, name: str, age_days: float) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    f = directory / name
    f.write_text(json.dumps({"id": name.replace(".json", ""), "status": "done"}))
    # 古い mtime を設定
    mtime = time.time() - age_days * 86400
    import os
    os.utime(f, (mtime, mtime))
    return f


def test_archive_done_tasks_older_than_threshold(tmp_path):
    tasks_dir = tmp_path / "tasks"
    done_dir = tasks_dir / "done" / "2025-01"
    _make_task_file(done_dir, "task_aaa1234567890123.json", age_days=31)

    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)

    assert moved == 1
    archive = tasks_dir / "_archive" / "done"
    assert any(archive.rglob("task_aaa1234567890123.json"))


def test_archive_skips_recent_tasks(tmp_path):
    tasks_dir = tmp_path / "tasks"
    done_dir = tasks_dir / "done" / "2025-01"
    _make_task_file(done_dir, "task_recent1234567.json", age_days=5)

    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_archive_dry_run_does_not_move(tmp_path):
    tasks_dir = tmp_path / "tasks"
    done_dir = tasks_dir / "done" / "2025-01"
    task_file = _make_task_file(done_dir, "task_dry1234567890.json", age_days=60)

    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=True)

    assert moved == 1
    assert task_file.exists()  # dry_run なので移動されない


def test_archive_skips_files_already_in_archive(tmp_path):
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2025-01"
    _make_task_file(archive_dir, "task_already1234567.json", age_days=60)

    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_archive_handles_missing_status_dir(tmp_path):
    tasks_dir = tmp_path / "tasks"
    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_archive_cancelled_tasks(tmp_path):
    tasks_dir = tmp_path / "tasks"
    cancelled_dir = tasks_dir / "cancelled"
    _make_task_file(cancelled_dir, "task_canc1234567890.json", age_days=10)

    config = {"tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 1


# ── _purge_archived_tasks ─────────────────────────────────────

def test_purge_old_archived_tasks(tmp_path):
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2024-01"
    _make_task_file(archive_dir, "task_old1234567890.json", age_days=366)

    config = {"tasks": {"purge_archive_after_days": 365}}
    purged = _purge_archived_tasks(config, tasks_dir, dry_run=False)
    assert purged == 1
    assert not (archive_dir / "task_old1234567890.json").exists()


def test_purge_skips_recent_archived_tasks(tmp_path):
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2025-01"
    _make_task_file(archive_dir, "task_keep1234567890.json", age_days=100)

    config = {"tasks": {"purge_archive_after_days": 365}}
    purged = _purge_archived_tasks(config, tasks_dir, dry_run=False)
    assert purged == 0


def test_purge_dry_run_does_not_delete(tmp_path):
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2024-01"
    task_file = _make_task_file(archive_dir, "task_purge_dry123.json", age_days=366)

    config = {"tasks": {"purge_archive_after_days": 365}}
    purged = _purge_archived_tasks(config, tasks_dir, dry_run=True)
    assert purged == 1
    assert task_file.exists()


def test_purge_returns_zero_when_no_archive_dir(tmp_path):
    config = {"tasks": {"purge_archive_after_days": 365}}
    assert _purge_archived_tasks(config, tmp_path / "tasks", dry_run=False) == 0


# ── _rotate_execution_log ─────────────────────────────────────

def _make_exec_log(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(e) for e in entries]
    path.write_text("\n".join(lines) + "\n")


def _old_ts(days: float) -> str:
    from datetime import datetime, timezone, timedelta
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.isoformat()


def test_rotate_compresses_old_log_entries(tmp_path):
    log = tmp_path / "exec.jsonl"
    _make_exec_log(log, [
        {"ts": _old_ts(20), "model_id": "m", "duration_s": 1.0},
        {"ts": _old_ts(1),  "model_id": "m", "duration_s": 2.0},
    ])
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)

    assert freed > 0
    # 古い行は gz ファイルに移動、新しい行は残る
    gz_files = list(tmp_path.glob("exec-*.jsonl.gz"))
    assert len(gz_files) == 1
    remaining = log.read_text()
    assert _old_ts(20)[:10] not in remaining  # 古い行は削除済み


def test_rotate_dry_run_does_not_compress(tmp_path):
    log = tmp_path / "exec.jsonl"
    _make_exec_log(log, [{"ts": _old_ts(20), "model_id": "m", "duration_s": 1.0}])
    original_content = log.read_text()

    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=True)

    assert freed > 0
    assert log.read_text() == original_content
    assert not list(tmp_path.glob("exec-*.jsonl.gz"))


def test_rotate_returns_zero_when_no_old_entries(tmp_path):
    log = tmp_path / "exec.jsonl"
    _make_exec_log(log, [{"ts": _old_ts(1), "model_id": "m", "duration_s": 1.0}])
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed == 0


def test_rotate_returns_zero_when_log_missing(tmp_path):
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, tmp_path / "nonexistent.jsonl", dry_run=False)
    assert freed == 0


def test_rotate_purges_old_gz_files(tmp_path):
    log = tmp_path / "exec.jsonl"
    log.write_text("")
    # 古い gz ファイルを作成
    gz = tmp_path / "exec-20240101T000000.jsonl.gz"
    with gzip.open(gz, "wt") as f:
        f.write("old\n")
    import os
    os.utime(gz, (time.time() - 100 * 86400,) * 2)

    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed > 0
    assert not gz.exists()


def test_rotate_handles_oserror_on_read(tmp_path, monkeypatch):
    log = tmp_path / "exec.jsonl"
    log.write_text('{"ts": "2020-01-01T00:00:00+00:00", "model_id": "m"}\n')
    monkeypatch.setattr(Path, "read_text", lambda *a, **kw: (_ for _ in ()).throw(OSError("unreadable")))
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed == 0


def test_rotate_keeps_lines_with_unparseable_ts(tmp_path):
    log = tmp_path / "exec.jsonl"
    _make_exec_log(log, [{"ts": "not-a-timestamp", "model_id": "m"}])
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed == 0  # パース不能なタイムスタンプは保持


def test_rotate_handles_invalid_json_lines(tmp_path):
    log = tmp_path / "exec.jsonl"
    log.write_text("bad-json\n")
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed == 0


# ── run_once ──────────────────────────────────────────────────

def test_run_once_returns_summary(tmp_path):
    tasks_dir = tmp_path / "tasks"
    exec_log = tmp_path / "exec.jsonl"
    exec_log.parent.mkdir(parents=True, exist_ok=True)
    exec_log.write_text("")

    config = {
        "dry_run": False,
        "tasks": {"archive_done_after_days": 30, "archive_failed_after_days": 7,
                  "purge_archive_after_days": 365},
        "execution_logs": {"archive_after_days": 14, "purge_after_days": 90},
    }
    result = run_once(config=config, tasks_dir=tasks_dir, exec_log=exec_log)

    assert "archived" in result
    assert "purged" in result
    assert "log_bytes_freed" in result
    assert result["dry_run"] is False


def test_run_once_dry_run_flag(tmp_path):
    config = {"dry_run": True}
    result = run_once(config=config, tasks_dir=tmp_path / "tasks",
                      exec_log=tmp_path / "exec.jsonl")
    assert result["dry_run"] is True


def test_run_once_uses_defaults_when_no_config(tmp_path, monkeypatch):
    """config=None のとき load_config() が呼ばれてもクラッシュしないこと。"""
    monkeypatch.setattr("airflow_mcp.cleaner.main._CONFIG_PATH", tmp_path / "none.yaml")
    monkeypatch.setattr("airflow_mcp.cleaner.main._TASKS_DIR", tmp_path / "tasks")
    monkeypatch.setattr("airflow_mcp.cleaner.main._EXEC_LOG", tmp_path / "exec.jsonl")
    result = run_once()
    assert isinstance(result["archived"], int)


# ── additional coverage ────────────────────────────────────────

def test_archive_handles_oserror_on_move(tmp_path):
    """move が OSError → warning ログのみ・クラッシュしない (line 74-75)。"""
    from unittest.mock import patch
    tasks_dir = tmp_path / "tasks"
    _make_task_file(tasks_dir / "done", "task_x.json", age_days=60)
    config = {"tasks": {"archive_after_days": 30}}
    with patch("shutil.move", side_effect=OSError("disk full")):
        moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_purge_handles_oserror_on_unlink(tmp_path):
    """unlink が OSError → warning ログのみ・クラッシュしない (line 97-98)。"""
    from unittest.mock import patch
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2020-01"
    archive_dir.mkdir(parents=True)
    f = archive_dir / "task_old.json"
    f.write_text('{"id":"old","status":"done"}')
    import os
    os.utime(f, (time.time() - 400 * 86400,) * 2)
    config = {"tasks": {"purge_archive_after_days": 365}}
    with patch("pathlib.Path.unlink", side_effect=OSError("perm denied")):
        purged = _purge_archived_tasks(config, tasks_dir, dry_run=False)
    assert purged == 0


def test_rotate_log_purge_gz_oserror(tmp_path):
    """gz の unlink が OSError → warning ログのみ・クラッシュしない (line 121-122)。"""
    from unittest.mock import patch
    log = tmp_path / "exec.jsonl"
    log.write_text("")
    gz = tmp_path / "exec-20240101T000000.jsonl.gz"
    with gzip.open(gz, "wt") as gf:
        gf.write("old\n")
    import os
    os.utime(gz, (time.time() - 100 * 86400,) * 2)
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 90}}
    with patch("pathlib.Path.unlink", side_effect=OSError("locked")):
        freed = _rotate_execution_log(config, log, dry_run=False)
    # freed == 0 (unlink failed) and no crash
    assert freed == 0


def test_archive_skips_non_matching_task_files(tmp_path):
    """threshold に達していないファイルはスキップ (line 65-66)。"""
    tasks_dir = tmp_path / "tasks"
    _make_task_file(tasks_dir / "done", "task_new.json", age_days=5)
    config = {"tasks": {"archive_after_days": 30}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_purge_skips_recent_files(tmp_path):
    """purge_archive_after_days に達していないファイルはスキップ (line 91-92)。"""
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "_archive" / "done" / "2025-01"
    archive_dir.mkdir(parents=True)
    f = archive_dir / "task_recent.json"
    f.write_text('{"id":"x","status":"done"}')
    config = {"tasks": {"purge_archive_after_days": 365}}
    purged = _purge_archived_tasks(config, tasks_dir, dry_run=False)
    assert purged == 0


def test_archive_skips_files_already_in_archive(tmp_path):
    """_archive 配下のファイルはスキップすること (line 63)。"""
    tasks_dir = tmp_path / "tasks"
    archive_dir = tasks_dir / "done" / "_archive" / "2024-01"
    archive_dir.mkdir(parents=True)
    f = archive_dir / "task_archived.json"
    f.write_text('{"id":"x","status":"done"}')
    import os
    os.utime(f, (time.time() - 200 * 86400,) * 2)
    config = {"tasks": {"archive_after_days": 30}}
    moved = _archive_old_tasks(config, tasks_dir, dry_run=False)
    assert moved == 0


def test_rotate_skips_blank_lines_in_jsonl(tmp_path):
    """jsonl に空行が含まれていてもスキップして正常に処理すること (line 138)。"""
    log = tmp_path / "exec.jsonl"
    old_ts = "2020-01-01T00:00:00Z"
    log.write_text(f'\n{{"ts": "{old_ts}", "event": "x"}}\n\n')
    config = {"execution_logs": {"archive_after_days": 14, "purge_after_days": 365}}
    freed = _rotate_execution_log(config, log, dry_run=False)
    assert freed > 0

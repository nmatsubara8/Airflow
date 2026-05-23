"""ingester/main.py の単体テスト（DDD-04 §5）。"""
from __future__ import annotations

import time
from pathlib import Path

import pytest
import yaml

from airflow_mcp.ingester.main import (
    _is_excluded,
    _is_stable,
    load_config,
    run_once,
)


# ── load_config ───────────────────────────────────────

def test_load_config_returns_empty_when_no_file():
    assert load_config("/nonexistent/ingester.yaml") == {}


def test_load_config_reads_yaml(tmp_path):
    cfg = tmp_path / "ingester.yaml"
    cfg.write_text("watch:\n  glob: '**/*.md'\n")
    result = load_config(str(cfg))
    assert result["watch"]["glob"] == "**/*.md"


# ── _is_stable ────────────────────────────────────────

def test_is_stable_returns_true_for_old_file(tmp_path):
    f = tmp_path / "old.md"
    f.write_text("content")
    # settle_ms=0 → always stable
    assert _is_stable(f, settle_ms=0) is True


def test_is_stable_returns_false_for_very_recent_file(tmp_path):
    f = tmp_path / "new.md"
    f.write_text("content")
    # settle_ms = 999999 → not settled yet
    assert _is_stable(f, settle_ms=999_999_000) is False


def test_is_stable_returns_false_for_nonexistent_file(tmp_path):
    f = tmp_path / "ghost.md"
    assert _is_stable(f, settle_ms=0) is False


# ── _is_excluded ──────────────────────────────────────

def test_is_excluded_matches_pattern(tmp_path):
    f = tmp_path / "archive" / "old.md"
    f.parent.mkdir()
    f.touch()
    assert _is_excluded(f, tmp_path, ["archive/*"]) is True


def test_is_excluded_no_match(tmp_path):
    f = tmp_path / "inbox" / "new.md"
    f.parent.mkdir()
    f.touch()
    assert _is_excluded(f, tmp_path, ["archive/*"]) is False


def test_is_excluded_empty_patterns(tmp_path):
    f = tmp_path / "any.md"
    f.touch()
    assert _is_excluded(f, tmp_path, []) is False


# ── run_once ──────────────────────────────────────────

def test_run_once_returns_zero_with_no_watch_paths(task_store):
    n = run_once(task_store, {})
    assert n == 0


def test_run_once_uses_vault_root_env(tmp_path, task_store, monkeypatch):
    # VAULT_ROOT を設定してもファイルがなければ 0
    monkeypatch.setenv("VAULT_ROOT", str(tmp_path))
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()
    n = run_once(task_store, {})
    assert n == 0


def test_run_once_ingests_markdown_file(tmp_path, task_store, monkeypatch):
    monkeypatch.setenv("VAULT_ROOT", str(tmp_path))
    md = tmp_path / "task.md"
    md.write_text("---\ntitle: テストタスク\nsection: engineering\n---\n本文")

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}
    n = run_once(task_store, config)
    assert n == 1
    tasks = task_store.list_by_status("inbox")
    assert len(tasks) == 1
    assert tasks[0]["title"] == "テストタスク"


def test_run_once_deduplicates_on_second_call(tmp_path, task_store, monkeypatch):
    md = tmp_path / "task.md"
    md.write_text("---\ntitle: 重複テスト\nsection: engineering\n---\n本文")

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}
    n1 = run_once(task_store, config)
    n2 = run_once(task_store, config)
    assert n1 == 1
    assert n2 == 0  # 重複排除


def test_run_once_skips_oversized_file(tmp_path, task_store):
    md = tmp_path / "large.md"
    md.write_text("x" * 100)
    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 10}}  # 10 bytes limit

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    n = run_once(task_store, config)
    assert n == 0


def test_run_once_handles_parse_error_gracefully(tmp_path, task_store):
    # 無効 UTF-8 バイト列 → read_text(encoding="utf-8") が UnicodeDecodeError を raise する
    md = tmp_path / "bad.md"
    md.write_bytes(b"\xff\xfe invalid utf-8 \x80\x81")
    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    # パース例外をキャッチして 0 を返すこと（クラッシュしない）
    n = run_once(task_store, config)
    assert n == 0

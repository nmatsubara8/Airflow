"""ingester/main.py の単体テスト（DDD-04 §5）。"""
from __future__ import annotations


import pytest

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


def test_run_once_skips_nonexistent_watch_path(task_store):
    """存在しない watch パスは quietly スキップすること（line 53）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()
    config = {"watch": {"paths": ["/nonexistent/vault/path"], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}
    n = run_once(task_store, config)
    assert n == 0


def test_run_once_skips_excluded_files(tmp_path, task_store):
    """exclude パターンにマッチするファイルをスキップすること（line 56）。"""
    archive = tmp_path / "archive"
    archive.mkdir()
    md = archive / "old_task.md"
    md.write_text("---\ntitle: 旧タスク\nsection: engineering\n---\n本文")

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    config = {"watch": {"paths": [str(tmp_path)], "glob": "**/*.md",
                        "exclude": ["archive/*"]},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}
    n = run_once(task_store, config)
    assert n == 0


def test_run_once_skips_unstable_file(tmp_path, task_store):
    """settle_ms が大きいと直後に作成したファイルはスキップされること（line 61）。"""
    md = tmp_path / "new.md"
    md.write_text("---\ntitle: 新規\nsection: engineering\n---\n本文")

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 999_999_000, "max_file_size_bytes": 524288}}
    n = run_once(task_store, config)
    assert n == 0


def test_run_once_dedup_from_store(tmp_path, task_store):
    """_seen_hashes をクリアしてもストア内に同一 content_hash があれば再取り込みしないこと（lines 84-85）。"""
    md = tmp_path / "task.md"
    md.write_text("---\ntitle: 重複テスト\nsection: engineering\n---\n本文")

    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    config = {"watch": {"paths": [str(tmp_path)], "glob": "*.md", "exclude": []},
              "stability": {"settle_ms": 0, "max_file_size_bytes": 524288}}

    # 1回目：正常に ingested
    n1 = run_once(task_store, config)
    assert n1 == 1

    # _seen_hashes をクリアして「プロセス再起動後」をシミュレート
    im._seen_hashes.clear()

    # 2回目：ストアに既存のハッシュがあるので再取り込みしない
    n2 = run_once(task_store, config)
    assert n2 == 0


# ── run_loop ───────────────────────────────────────────

def test_run_loop_runs_once_then_stops(task_store, monkeypatch):
    """run_loop が run_once を呼び n>0 のときログを出力すること（lines 125-134）。"""
    call_count = {"n": 0}

    def fake_run_once(store, config):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return 1  # n>0 → logger.info branch
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.ingester.main.run_once", fake_run_once)
    monkeypatch.setattr("airflow_mcp.ingester.main.time.sleep", lambda s: None)

    from airflow_mcp.ingester.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(task_store, {}, poll_interval_ms=100)

    assert call_count["n"] == 2


def test_run_loop_catches_run_once_exception(task_store, monkeypatch):
    """run_loop が run_once の例外を catch してループを継続すること（lines 132-133）。"""
    call_count = {"n": 0}

    def fake_run_once(store, config):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("store temporarily unavailable")
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.ingester.main.run_once", fake_run_once)
    monkeypatch.setattr("airflow_mcp.ingester.main.time.sleep", lambda s: None)

    from airflow_mcp.ingester.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(task_store, {})

    assert call_count["n"] == 2


# ── _cli ───────────────────────────────────────────────

def test_cli_entry_point(tmp_path, monkeypatch):
    """_cli が TaskStore と load_config を呼んで run_loop に渡すこと（lines 138-143）。"""
    call_args = {}

    def fake_run_loop(store, config):
        call_args["store"] = store
        call_args["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.ingester.main.run_loop", fake_run_loop)
    monkeypatch.setenv("AIRFLOW_TASKBOARD_DIR", str(tmp_path))

    from airflow_mcp.ingester.main import _cli
    with pytest.raises(SystemExit):
        _cli()

    assert "store" in call_args


def test_run_once_skips_task_creation_when_auto_dispatch_false(tmp_path, task_store):
    """auto_dispatch=False のパスではノートはインデックスされるがタスクは生成されない（DDD-04 §5.1）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    md = tmp_path / "note.md"
    md.write_text("---\ntitle: 参照ノート\nsection: engineering\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": False}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    n = run_once(task_store, config)
    assert n == 0
    assert task_store.list_by_status("inbox") == []


def test_run_once_skips_task_creation_for_confidential_notes(tmp_path, task_store):
    """confidential: true のノートはタスク生成しない（DDD-04 §5.5）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    md = tmp_path / "secret.md"
    md.write_text("---\ntitle: 機密ノート\nsection: engineering\nconfidential: true\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": True}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    n = run_once(task_store, config)
    assert n == 0
    assert task_store.list_by_status("inbox") == []


def test_run_once_writes_processed_at_to_frontmatter(tmp_path, task_store):
    """タスク生成後に processed_at をフロントマターに追記する（F-L28）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    md = tmp_path / "inbox.md"
    md.write_text("---\ntitle: Inbox ノート\nsection: engineering\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": True}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    n = run_once(task_store, config)
    assert n == 1
    updated = md.read_text(encoding="utf-8")
    assert "processed_at:" in updated


def test_run_once_sets_source_note_id_on_task(tmp_path, task_store):
    """タスクに source_note_id が設定されること（DDD-04 §6.2）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    md = tmp_path / "note.md"
    md.write_text("---\ntitle: ノート\nsection: engineering\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": True}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    run_once(task_store, config)
    tasks = task_store.list_by_status("inbox")
    assert tasks[0].get("source_note_id", "").startswith("note_")


def test_run_once_updates_notes_index(tmp_path, task_store, monkeypatch):
    """notes/index.json が更新されること（DDD-04 §7）。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    index_path = tmp_path / "notes_index.json"
    monkeypatch.setattr(im, "_notes_index_path", index_path)

    md = tmp_path / "note.md"
    md.write_text("---\ntitle: インデックステスト\nsection: engineering\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": True}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    run_once(task_store, config)
    assert index_path.exists()
    import json
    idx = json.loads(index_path.read_text())
    assert len(idx["notes"]) == 1


def test_run_once_indexes_confidential_note_without_creating_task(tmp_path, task_store, monkeypatch):
    """confidential ノートはインデックスには記録されるがタスクは生成しない。"""
    import airflow_mcp.ingester.main as im
    im._seen_hashes.clear()

    index_path = tmp_path / "notes_index.json"
    monkeypatch.setattr(im, "_notes_index_path", index_path)

    md = tmp_path / "secret.md"
    md.write_text("---\ntitle: 機密\nsection: engineering\nconfidential: true\n---\n本文")

    config = {
        "watch_paths": [{"path": str(tmp_path), "auto_dispatch": True}],
        "watch": {"glob": "*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    n = run_once(task_store, config)
    assert n == 0
    import json
    idx = json.loads(index_path.read_text())
    note = list(idx["notes"].values())[0]
    assert note["confidential"] is True


def test_write_processed_at_adds_to_existing_frontmatter(tmp_path):
    """_write_processed_at が既存フロントマターに processed_at を追記する。"""
    from airflow_mcp.ingester.main import _write_processed_at
    md = tmp_path / "note.md"
    md.write_text("---\ntitle: テスト\n---\n本文")
    _write_processed_at(md, "2026-05-23T00:00:00Z")
    content = md.read_text()
    assert "processed_at: 2026-05-23T00:00:00Z" in content
    assert "title: テスト" in content


def test_write_processed_at_is_idempotent(tmp_path):
    """processed_at が既に存在する場合は書き込みをスキップすること（冪等性）。"""
    from airflow_mcp.ingester.main import _write_processed_at
    md = tmp_path / "note.md"
    md.write_text("---\ntitle: テスト\nprocessed_at: 2026-05-01T00:00:00Z\n---\n本文")
    _write_processed_at(md, "2026-05-23T00:00:00Z")
    content = md.read_text()
    # 元の processed_at が保持され、新しい値で上書きされないこと
    assert "processed_at: 2026-05-01T00:00:00Z" in content
    assert "2026-05-23" not in content


def test_write_processed_at_handles_file_without_frontmatter(tmp_path):
    """フロントマターなしのファイルでも crash しないこと。"""
    from airflow_mcp.ingester.main import _write_processed_at
    md = tmp_path / "plain.md"
    md.write_text("# 本文のみ\n内容")
    _write_processed_at(md, "2026-05-23T00:00:00Z")
    # crash しないことを確認


def test_write_processed_at_suppresses_os_error(tmp_path):
    """OSError を抑制すること。"""
    from unittest.mock import patch
    from airflow_mcp.ingester.main import _write_processed_at
    md = tmp_path / "note.md"
    md.write_text("---\ntitle: T\n---\n本文")
    with patch.object(md.__class__, "write_text", side_effect=OSError("disk full")):
        _write_processed_at(md, "2026-05-23T00:00:00Z")  # must not raise


# ── 追加テスト: YAML non-dict ガード ──────────────────

def test_load_config_non_dict_yaml_returns_empty(tmp_path):
    """ingester.yaml が list などの非dict を返した場合 {} を返すこと。"""
    from airflow_mcp.ingester.main import load_config
    cfg_file = tmp_path / "ingester.yaml"
    cfg_file.write_text("- watch\n- paths\n")
    result = load_config(str(cfg_file))
    assert result == {}


def test_load_config_empty_yaml_returns_empty(tmp_path):
    """ingester.yaml が空ファイルの場合 {} を返すこと。"""
    from airflow_mcp.ingester.main import load_config
    cfg_file = tmp_path / "ingester.yaml"
    cfg_file.write_text("")
    result = load_config(str(cfg_file))
    assert result == {}


def test_write_processed_at_returns_false_when_no_closing_delimiter(tmp_path):
    """frontmatter の閉じ '---' がない場合に False を返すこと（main.py line 51）。"""
    from airflow_mcp.ingester.main import _write_processed_at
    md = tmp_path / "test.md"
    # '---' で始まるが閉じていない（len(parts) < 3）
    md.write_text("---\ntitle: test\n", encoding="utf-8")
    result = _write_processed_at(md, "2026-05-25T00:00:00Z")
    assert result is False

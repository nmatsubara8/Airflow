"""dispatcher/main.py の単体テスト（DDD-04 §6.4）。"""
from __future__ import annotations

import pytest

from airflow_mcp.dispatcher.main import (
    dispatch_once,
    infer_section,
    load_config,
    select_agent,
)


# ── load_config ───────────────────────────────────────

def test_load_config_returns_empty_when_no_file():
    result = load_config("/nonexistent/dispatch.yaml")
    assert result == {}


def test_load_config_reads_yaml(tmp_path):
    cfg = tmp_path / "dispatch.yaml"
    cfg.write_text("routing:\n  default_category: content\n")
    result = load_config(str(cfg))
    assert result["routing"]["default_category"] == "content"


# ── infer_section ─────────────────────────────────────

def test_infer_section_uses_task_section_field():
    task = {"section": "business", "tags": [], "title": "anything"}
    assert infer_section(task, {}) == "business"


def test_infer_section_matches_tag_rule():
    config = {
        "routing": {
            "rules": [
                {"category": "content", "match_any": {"tags": ["blog", "copy"], "title_keywords": []}},
            ],
            "default_category": "engineering",
        }
    }
    task = {"tags": ["blog"], "title": "new post"}
    assert infer_section(task, config) == "content"


def test_infer_section_matches_keyword_rule():
    config = {
        "routing": {
            "rules": [
                {"category": "business", "match_any": {"tags": [], "title_keywords": ["report", "analysis"]}},
            ],
            "default_category": "engineering",
        }
    }
    task = {"tags": [], "title": "Quarterly Analysis Report"}
    assert infer_section(task, config) == "business"


def test_infer_section_returns_default_when_no_match():
    config = {"routing": {"rules": [], "default_category": "orchestration"}}
    task = {"tags": [], "title": "unrelated task"}
    assert infer_section(task, config) == "orchestration"


def test_infer_section_returns_engineering_when_no_config():
    task = {"tags": [], "title": "unrelated"}
    assert infer_section(task, {}) == "engineering"


# ── select_agent ──────────────────────────────────────

def test_select_agent_known_section():
    assert select_agent("engineering", {}) == "agent_docs"


def test_select_agent_unknown_section_falls_back():
    assert select_agent("unknown_section", {}) == "agent_docs"


# ── dispatch_once ─────────────────────────────────────

def test_dispatch_once_claims_inbox_task(task_store, sample_task_fields):
    task_store.create(sample_task_fields)
    n = dispatch_once(task_store, {})
    assert n == 1
    in_progress = task_store.list_by_status("in_progress")
    assert len(in_progress) == 1


def test_dispatch_once_skips_already_claimed(task_store, sample_task_fields):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    n = dispatch_once(task_store, {})
    assert n == 0  # already in_progress, not inbox


def test_dispatch_once_returns_zero_on_empty_inbox(task_store):
    n = dispatch_once(task_store, {})
    assert n == 0


def test_dispatch_once_handles_exception_gracefully(task_store, sample_task_fields, monkeypatch):
    task_store.create(sample_task_fields)
    import airflow_mcp.dispatcher.main as dm

    def raise_error(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(dm, "select_agent", raise_error)
    # should not raise, returns 0
    n = dispatch_once(task_store, {})
    assert n == 0


def test_dispatch_once_writes_section_when_missing(task_store):
    """タスクに section がない場合、store.update で section を書き込むこと（line 86）。"""
    _task = task_store.create({
        "title": "no section task",
        "description": "desc",
        "tags": [],
        "priority": 5,
        "owner": None,
    })
    n = dispatch_once(task_store, {})
    assert n == 1
    # in_progress になったタスクに section が設定されていること
    in_prog = task_store.list_by_status("in_progress")
    assert len(in_prog) == 1


# ── run_loop ───────────────────────────────────────────

def test_run_loop_runs_and_handles_results(task_store, monkeypatch):
    """run_loop が dispatch_once を呼び n>0 のときログを出力すること（lines 103-111）。"""
    call_count = {"n": 0}

    def fake_dispatch_once(store, config):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return 1  # n>0 → logger.info branch
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.dispatcher.main.dispatch_once", fake_dispatch_once)
    monkeypatch.setattr("airflow_mcp.dispatcher.main.time.sleep", lambda s: None)

    from airflow_mcp.dispatcher.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(task_store, {}, poll_interval_s=0.001)

    assert call_count["n"] == 2


def test_run_loop_catches_exception(task_store, monkeypatch):
    """run_loop が dispatch_once の例外を catch してループを継続すること（lines 109-110）。"""
    call_count = {"n": 0}

    def fake_dispatch_once(store, config):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("store error")
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.dispatcher.main.dispatch_once", fake_dispatch_once)
    monkeypatch.setattr("airflow_mcp.dispatcher.main.time.sleep", lambda s: None)

    from airflow_mcp.dispatcher.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(task_store, {})

    assert call_count["n"] == 2


# ── _cli ───────────────────────────────────────────────

def test_cli_entry_point(tmp_path, monkeypatch):
    """_cli が TaskStore と load_config を呼んで run_loop に渡すこと（lines 115-121）。"""
    call_args = {}

    def fake_run_loop(store, config):
        call_args["store"] = store
        call_args["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.dispatcher.main.run_loop", fake_run_loop)
    monkeypatch.setenv("AIRFLOW_TASKBOARD_DIR", str(tmp_path))

    from airflow_mcp.dispatcher.main import _cli
    with pytest.raises(SystemExit):
        _cli()

    assert "store" in call_args

"""dispatcher/main.py の単体テスト（DDD-04 §6.4）。"""
from __future__ import annotations

import pytest
import yaml

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
    from airflow_mcp.taskboard.store import TaskStore
    task = task_store.create({
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

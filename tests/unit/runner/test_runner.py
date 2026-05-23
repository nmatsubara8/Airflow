"""runner/main.py の単体テスト（DDD-05 §9）。"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_mcp.orchestrator.recommender import simple_rule_select
from airflow_mcp.runner.main import (
    _build_messages,
    _call_ollama,
    _model_id_to_ollama_tag,
    _select_model,
    process_task,
    run_once,
)
from airflow_mcp.taskboard.store import TaskStore

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


# ── simple_rule_select (コールドスタートフォールバック) ────

def test_select_model_high_accuracy_batch_returns_32b():
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    assert simple_rule_select(task) == "model_qwen25_32b_q3"


def test_select_model_medium_accuracy_returns_14b():
    task = {"features": {"required_accuracy": "medium", "latency_sensitivity": "batch"}}
    assert simple_rule_select(task) == "model_qwen25_14b_q4"


def test_select_model_high_accuracy_interactive_returns_14b():
    # interactive は latency 優先なので 14B
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "interactive"}}
    assert simple_rule_select(task) == "model_qwen25_14b_q4"


def test_select_model_empty_features_returns_14b():
    assert simple_rule_select({}) == "model_qwen25_14b_q4"


# ── _select_model (registry なし → simple_rule フォールバック) ─

def test_runner_select_model_no_registry_falls_back_to_simple_rule():
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    model_id, etype = _select_model(task, {}, registry=None)
    assert model_id == "model_qwen25_32b_q3"
    assert etype is None


# ── _model_id_to_ollama_tag ───────────────────────────────

def test_model_id_to_ollama_tag_uses_registry():
    registry = {"model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b-instruct-q4_K_M"}}
    tag = _model_id_to_ollama_tag("model_qwen25_14b_q4", registry)
    assert tag == "qwen2.5:14b-instruct-q4_K_M"


def test_model_id_to_ollama_tag_fallback_converts_underscores():
    tag = _model_id_to_ollama_tag("model_qwen25_14b_q4", {})
    assert ":" in tag  # フォールバックは model_ を除去して _ → :


# ── _build_messages ────────────────────────────────────────

def test_build_messages_returns_system_and_user():
    task = {"title": "テスト", "description": "説明", "tags": ["a"], "section": "engineering"}
    msgs = _build_messages(task, "システムプロンプト")
    assert msgs[0]["role"] == "system"
    assert msgs[0]["content"] == "システムプロンプト"
    assert msgs[1]["role"] == "user"
    assert "テスト" in msgs[1]["content"]


def test_build_messages_includes_tags_in_user_content():
    task = {"title": "T", "description": "", "tags": ["api", "backend"], "section": "engineering"}
    msgs = _build_messages(task, "sys")
    assert "api" in msgs[1]["content"]
    assert "backend" in msgs[1]["content"]


# ── _call_ollama (STUB mode) ──────────────────────────────

def test_call_ollama_stub_returns_valid_schema():
    os.environ["OLLAMA_STUB"] = "true"
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "user content"},
    ]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    content, parsed, valid = _call_ollama(
        "http://localhost:11434", "stub-model", messages, schema, 0.3, 512, 0
    )
    assert valid is True
    assert "summary" in parsed


def test_call_ollama_stub_schema_valid_is_true():
    os.environ["OLLAMA_STUB"] = "true"
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    _, _, valid = _call_ollama("http://x", "model", messages, schema, 0.3, 512, 0)
    assert valid is True


# ── process_task ──────────────────────────────────────────

def test_process_task_stub_transitions_to_done(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    result = process_task(task_in_progress, task_store, {}, {}, "agent_docs", "http://localhost")
    assert result is True
    done = task_store.get(task["id"])
    assert done["status"] == "done"


def test_process_task_stub_adds_handoff_note(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    process_task(task_in_progress, task_store, {}, {}, "agent_docs", "http://localhost")
    done = task_store.get(task["id"])
    assert len(done["handoff_notes"]) >= 1
    assert "要約" in done["handoff_notes"][-1]["content"]


def test_process_task_invalid_schema_transitions_to_needs_human(
    task_store: TaskStore, sample_task_fields: dict
):
    """schema 不適合が _MAX_RETRY 回続いたら needs_human に遷移すること。"""
    os.environ["OLLAMA_STUB"] = "false"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    invalid_json = json.dumps({"wrong_field": "invalid"})

    def fake_call_ollama(*args, **kwargs):
        return invalid_json, {"wrong_field": "invalid"}, False

    with patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call_ollama):
        result = process_task(task_in_progress, task_store, {}, {}, "agent_docs", "http://localhost")

    assert result is False
    failed_task = task_store.get(task["id"])
    assert failed_task["status"] == "needs_human"


# ── run_once ──────────────────────────────────────────────

def test_run_once_processes_only_owned_tasks(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    # agent_docs 所有のタスク
    t1 = task_store.create(sample_task_fields)
    task_store.claim(t1["id"], "agent_docs")
    # agent_other 所有のタスク
    t2 = task_store.create({**sample_task_fields, "title": "他エージェントのタスク"})
    task_store.claim(t2["id"], "agent_other")

    n = run_once(task_store, {}, {}, "agent_docs", "http://localhost")
    assert n == 1

    # agent_other のタスクは untouched
    assert task_store.get(t2["id"])["status"] == "in_progress"


def test_run_once_returns_zero_when_no_owned_tasks(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_other")
    n = run_once(task_store, {}, {}, "agent_docs", "http://localhost")
    assert n == 0

"""パイプラインエラーパス統合テスト。

対象シナリオ:
  - スキーマ不適合によるneeds_human遷移
  - N タスク並列処理
  - fallback モデルへの reroute
  - ディスパッチャのセクション未定義タスク
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch


os.environ.setdefault("OLLAMA_STUB", "true")

from airflow_mcp.dispatcher.main import dispatch_once, load_config as load_dispatch_config
from airflow_mcp.ingester.main import run_once as ingester_run_once
from airflow_mcp.runner.main import (
    ExecutionContext,
    _load_registry,
    load_model_config,
    process_task,
    run_once as runner_run_once,
)
from airflow_mcp.taskboard.store import TaskStore


def _runner_ctx(store, model_config=None, registry=None, agent_id="agent_docs",
                ollama_url="http://localhost"):
    return ExecutionContext(
        store=store,
        model_config=model_config or {},
        registry=registry or {},
        agent_id=agent_id,
        ollama_url=ollama_url,
    )


# ── ヘルパー ──────────────────────────────────────────────────────────────


def _create_in_progress(store: TaskStore, title: str = "テストタスク", section: str = "engineering") -> dict:
    task = store.create({
        "title": title,
        "section": section,
        "description": "統合テスト用",
        "tags": ["test"],
        "priority": 3,
        "features": {"required_accuracy": "medium", "latency_sensitivity": "batch"},
        "owner": None,
    })
    return store.claim(task["id"], "agent_docs")


def _invalid_stub() -> tuple[str, dict | None, bool]:
    """スキーマ不適合の応答を返すスタブ。"""
    return "not valid json at all", None, False


# ── needs_human エスカレーション ────────────────────────────────────────────


def test_needs_human_on_schema_invalid(tmp_tasks_dir: Path):
    """スキーマ不適合が _MAX_RETRY 回続いた後、タスクが needs_human に遷移すること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    task = _create_in_progress(store)
    task_id = task["id"]

    model_config: dict[str, Any] = {}
    registry: dict[str, dict] = {}

    with patch("airflow_mcp.runner.main._call_ollama", return_value=_invalid_stub()), \
         patch("airflow_mcp.runner.main.time.sleep"):
        result = process_task(task, _runner_ctx(store, model_config, registry))

    assert result is False
    nh = store.list_by_status("needs_human")
    assert len(nh) == 1
    assert nh[0]["id"] == task_id

    notes = nh[0]["handoff_notes"]
    assert any("[FAILED]" in n["body"] for n in notes)


def test_needs_human_has_failure_detail_in_handoff_note(tmp_tasks_dir: Path):
    """needs_human 遷移時の handoff_note にリトライ回数と生の出力が含まれること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    task = _create_in_progress(store)

    with patch("airflow_mcp.runner.main._call_ollama", return_value=("raw output xyz", None, False)), \
         patch("airflow_mcp.runner.main.time.sleep"):
        process_task(task, _runner_ctx(store))

    nh_task = store.list_by_status("needs_human")[0]
    note_body = nh_task["handoff_notes"][-1]["body"]
    assert "raw output xyz" in note_body or "[FAILED]" in note_body


def test_done_count_zero_when_all_fail(tmp_tasks_dir: Path):
    """全タスクが schema 不適合になると done が 0 件のままであること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    for i in range(3):
        _create_in_progress(store, title=f"タスク{i}")

    with patch("airflow_mcp.runner.main._call_ollama", return_value=_invalid_stub()), \
         patch("airflow_mcp.runner.main.time.sleep"):
        n = runner_run_once(_runner_ctx(store))

    assert n == 3
    assert store.list_by_status("done") == []
    assert len(store.list_by_status("needs_human")) == 3


# ── 並列タスク処理 ─────────────────────────────────────────────────────────


def test_run_once_processes_all_in_progress_tasks(tmp_tasks_dir: Path):
    """run_once が in_progress タスクを N 件まとめて処理すること。"""
    N = 5
    store = TaskStore(base_dir=tmp_tasks_dir)
    for i in range(N):
        _create_in_progress(store, title=f"並列タスク{i}")

    n = runner_run_once(_runner_ctx(store))

    assert n == N
    assert len(store.list_by_status("done")) == N
    assert store.list_by_status("in_progress") == []


def test_run_once_skips_other_agents_tasks(tmp_tasks_dir: Path):
    """run_once は自分以外の agent がオーナーのタスクを処理しないこと。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    mine = _create_in_progress(store, title="自分のタスク")
    other = store.create({
        "title": "他エージェントのタスク",
        "section": "engineering", "description": "", "tags": [],
        "priority": 3, "features": {}, "owner": None,
    })
    store.claim(other["id"], "agent_other")

    n = runner_run_once(_runner_ctx(store))

    assert n == 1
    assert store.get(mine["id"])["status"] == "done"
    assert store.get(other["id"])["status"] == "in_progress"


# ── Reroute（fallback モデル） ─────────────────────────────────────────────


def test_reroute_to_fallback_on_primary_failure(tmp_tasks_dir: Path):
    """プライマリ失敗時に fallback_chain のモデルで再試行し、成功すること。

    simple_rule_select は medium accuracy タスクで model_qwen25_14b_q4 を選択するため、
    fallback_chain のキーも model_qwen25_14b_q4 にする。
    """
    store = TaskStore(base_dir=tmp_tasks_dir)
    task = _create_in_progress(store)

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "fallback_chain": {
                "model_qwen25_14b_q4": ["model_qwen25_32b_q3"],
            },
        },
    }
    registry = {
        "model_qwen25_14b_q4": {"id": "model_qwen25_14b_q4", "ollama_tag": "qwen2.5:14b-q4", "vram_gb": 8.5},
        "model_qwen25_32b_q3": {"id": "model_qwen25_32b_q3", "ollama_tag": "qwen2.5:32b-q3", "vram_gb": 14.0},
    }
    stub_success = json.dumps({
        "summary": "fallback 成功",
        "key_points": ["pt1", "pt2", "pt3"],
        "confidence": 0.9,
        "suggested_tags": [],
    })

    call_count = {"n": 0}

    def _side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 3:  # primary の _MAX_RETRY 回
            return ("bad", None, False)
        return (stub_success, json.loads(stub_success), True)

    with patch("airflow_mcp.runner.main._call_ollama", side_effect=_side_effect):
        result = process_task(task, _runner_ctx(store, model_config, registry))

    assert result is True
    done = store.list_by_status("done")
    assert len(done) == 1
    notes = done[0]["handoff_notes"]
    assert any("fallback 成功" in n["body"] or "reroute" in n["body"].lower() for n in notes)


def test_needs_human_when_both_primary_and_fallback_fail(tmp_tasks_dir: Path):
    """プライマリも fallback も失敗した場合に needs_human に遷移すること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    task = _create_in_progress(store)

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "fallback_chain": {
                "model_qwen25_14b_q4": ["model_qwen25_32b_q3"],
            },
        },
    }
    registry = {
        "model_qwen25_14b_q4": {"id": "model_qwen25_14b_q4", "ollama_tag": "qwen2.5:14b-q4", "vram_gb": 8.5},
        "model_qwen25_32b_q3": {"id": "model_qwen25_32b_q3", "ollama_tag": "qwen2.5:32b-q3", "vram_gb": 14.0},
    }

    with patch("airflow_mcp.runner.main._call_ollama", return_value=_invalid_stub()):
        result = process_task(task, _runner_ctx(store, model_config, registry))

    assert result is False
    nh = store.list_by_status("needs_human")
    assert len(nh) == 1
    # reroute 情報が handoff_note に含まれること
    note_body = nh[0]["handoff_notes"][-1]["body"]
    assert "reroute" in note_body.lower() or "[FAILED]" in note_body


# ── ディスパッチャのエラーパス ────────────────────────────────────────────


def test_dispatcher_handles_unknown_section_gracefully(tmp_tasks_dir: Path):
    """未知のセクションのタスクが agent_docs にフォールバックすること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    _task = store.create({
        "title": "未知セクションタスク",
        "section": "unknown_section_xyz",
        "description": "", "tags": [], "priority": 5, "features": {}, "owner": None,
    })

    n = dispatch_once(store, {})

    assert n == 1
    in_progress = store.list_by_status("in_progress")
    assert len(in_progress) == 1
    assert in_progress[0]["owner"] == "agent_docs"


def test_full_pipeline_multiple_tasks(tmp_tasks_dir: Path):
    """Vault → ingester → dispatch → runner で複数タスクが全て done になること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    vault = tmp_tasks_dir.parent / "vault"
    vault.mkdir()
    for i in range(3):
        (vault / f"task_{i}.md").write_text(f"""---
title: パイプラインテスト{i}
tags: [engineering, test]
section: engineering
priority: {i + 1}
features:
  required_accuracy: medium
  latency_sensitivity: batch
---
# テスト {i}
""")

    ingester_cfg = {
        "watch": {"paths": [str(vault)], "glob": "**/*.md", "exclude": []},
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }

    import airflow_mcp.ingester.main as ingester_mod
    ingester_mod._seen_hashes.clear()

    n_ing = ingester_run_once(store, ingester_cfg)
    assert n_ing == 3

    n_disp = dispatch_once(store, load_dispatch_config())
    assert n_disp == 3

    model_config = load_model_config()
    registry = _load_registry()
    n_run = runner_run_once(_runner_ctx(store, model_config, registry))
    assert n_run == 3

    assert len(store.list_by_status("done")) == 3
    assert store.list_by_status("inbox") == []
    assert store.list_by_status("in_progress") == []

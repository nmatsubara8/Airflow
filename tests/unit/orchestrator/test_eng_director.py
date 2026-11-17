"""orchestrator/eng_director.py の単体テスト。"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock


from airflow_mcp.orchestrator.eng_director import (
    _collect_stats,
    _build_context,
    _call_llm,
    _format_plan_description,
    _stub_plan,
    run_briefing,
)


# ── フィクスチャ ──────────────────────────────────────

def _make_task(
    task_id: str,
    status: str = "inbox",
    section: str = "engineering",
    priority: int = 3,
    owner: str = "agent_docs",
    title: str | None = None,
    updated_at: str | None = None,
) -> dict:
    return {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "status": status,
        "section": section,
        "priority": priority,
        "owner": owner,
        "updated_at": updated_at or datetime.now(timezone.utc).isoformat(),
    }


# ── _collect_stats ────────────────────────────────────

def test_collect_stats_empty():
    stats = _collect_stats([])
    assert stats["total"] == 0
    assert stats["blocked"] == []
    assert stats["needs_human"] == []
    assert stats["in_progress_long"] == []


def test_collect_stats_basic_counts():
    tasks = [
        _make_task("t1", status="inbox"),
        _make_task("t2", status="inbox"),
        _make_task("t3", status="in_progress"),
        _make_task("t4", status="done"),
        _make_task("t5", status="blocked"),
        _make_task("t6", status="needs_human"),
    ]
    stats = _collect_stats(tasks)
    assert stats["total"] == 6
    assert stats["by_status"]["inbox"] == 2
    assert stats["by_status"]["in_progress"] == 1
    assert stats["by_status"]["done"] == 1
    assert stats["by_status"]["blocked"] == 1
    assert stats["by_status"]["needs_human"] == 1


def test_collect_stats_blocked_list():
    tasks = [
        _make_task("t1", status="blocked", title="Blocker A"),
        _make_task("t2", status="blocked", title="Blocker B"),
    ]
    stats = _collect_stats(tasks)
    assert len(stats["blocked"]) == 2
    ids = {b["id"] for b in stats["blocked"]}
    assert "t1" in ids
    assert "t2" in ids


def test_collect_stats_needs_human_list():
    tasks = [_make_task("nh1", status="needs_human", title="Human needed")]
    stats = _collect_stats(tasks)
    assert len(stats["needs_human"]) == 1
    assert stats["needs_human"][0]["id"] == "nh1"


def test_collect_stats_stale_in_progress():
    stale_time = datetime.fromtimestamp(time.time() - 80 * 3600, tz=timezone.utc).isoformat()
    fresh_time = datetime.now(timezone.utc).isoformat()
    tasks = [
        _make_task("stale", status="in_progress", updated_at=stale_time),
        _make_task("fresh", status="in_progress", updated_at=fresh_time),
    ]
    stats = _collect_stats(tasks)
    assert len(stats["in_progress_long"]) == 1
    assert stats["in_progress_long"][0]["id"] == "stale"


def test_collect_stats_invalid_updated_at_ignored():
    tasks = [_make_task("t1", status="in_progress", updated_at="not-a-date")]
    stats = _collect_stats(tasks)
    assert stats["in_progress_long"] == []


def test_collect_stats_no_updated_at():
    t = _make_task("t1", status="in_progress")
    t.pop("updated_at")
    stats = _collect_stats([t])
    assert stats["in_progress_long"] == []


def test_collect_stats_section_counts():
    tasks = [
        _make_task("t1", section="engineering"),
        _make_task("t2", section="engineering"),
        _make_task("t3", section="content"),
    ]
    stats = _collect_stats(tasks)
    assert stats["by_section"]["engineering"] == 2
    assert stats["by_section"]["content"] == 1


# ── _build_context ────────────────────────────────────

def test_build_context_includes_stats():
    tasks = [_make_task("t1", title="My Task")]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "タスクボード統計" in ctx
    assert "総タスク数: 1" in ctx


def test_build_context_lists_active_tasks():
    tasks = [_make_task("t1", status="inbox", title="Active task")]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "Active task" in ctx


def test_build_context_no_blockers_shows_none():
    tasks = [_make_task("t1")]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "なし" in ctx


def test_build_context_lists_blockers():
    tasks = [_make_task("b1", status="blocked", title="The Blocker")]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "The Blocker" in ctx


def test_build_context_lists_needs_human():
    tasks = [_make_task("nh1", status="needs_human", title="Human task")]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "Human task" in ctx


def test_build_context_lists_stale_in_progress():
    stale_time = datetime.fromtimestamp(time.time() - 80 * 3600, tz=timezone.utc).isoformat()
    tasks = [_make_task("s1", status="in_progress", title="Stale task", updated_at=stale_time)]
    stats = _collect_stats(tasks)
    ctx = _build_context(tasks, stats)
    assert "Stale task" in ctx


# ── _stub_plan ────────────────────────────────────────

def test_stub_plan_structure():
    stats = {"total": 5, "blocked": [], "needs_human": [], "by_status": {"done": 2}}
    plan = _stub_plan(stats)
    assert "week_goal" in plan
    assert "summary" in plan
    assert "top_priorities" in plan
    assert "agent_actions" in plan
    assert "blockers" in plan
    assert "risks" in plan
    assert plan["done_count_last_week"] == 2


# ── _format_plan_description ──────────────────────────

def test_format_plan_description_contains_goal():
    stats = {"total": 10, "blocked": [], "needs_human": [], "by_status": {}}
    plan = {
        "week_goal": "今週はテスト完了",
        "summary": "現状良好",
        "top_priorities": [
            {"task_id": "t1", "title": "重要タスク", "reason": "期限間近"}
        ],
        "agent_actions": {"agent_docs": "ドキュメント更新"},
        "blockers": [],
        "done_count_last_week": 3,
        "risks": ["リスクA"],
    }
    desc = _format_plan_description(plan, stats)
    assert "今週はテスト完了" in desc
    assert "重要タスク" in desc
    assert "agent_docs" in desc
    assert "リスクA" in desc
    assert "総タスク数: 10" in desc


def test_format_plan_description_with_blockers():
    stats = {"total": 5, "blocked": [], "needs_human": [], "by_status": {}}
    plan = {
        "week_goal": "目標",
        "summary": "概要",
        "top_priorities": [],
        "agent_actions": {},
        "blockers": [{"description": "DB接続エラー", "action": "再起動する"}],
        "done_count_last_week": 0,
        "risks": [],
    }
    desc = _format_plan_description(plan, stats)
    assert "DB接続エラー" in desc
    assert "再起動する" in desc


# ── run_briefing ──────────────────────────────────────

def _make_mock_store(tasks: list[dict] | None = None):
    store = MagicMock()
    tasks = tasks or []
    store.list_all.return_value = tasks
    created_task = {"id": "sprint-001", "title": "Sprint plan"}
    store.create.return_value = created_task
    return store


def test_run_briefing_stub_mode(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = _make_mock_store([_make_task("t1")])
    result = run_briefing(store, ollama_url="http://localhost:11434")
    assert result["task_id"] == "sprint-001"
    assert "stats" in result
    assert "plan" in result
    store.create.assert_called_once()


def test_run_briefing_creates_needs_human_task(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = _make_mock_store([_make_task("t1"), _make_task("t2", status="blocked")])
    run_briefing(store)
    call_kwargs = store.create.call_args[0][0]
    assert call_kwargs["status"] == "needs_human"
    assert "sprint-plan" in call_kwargs["tags"]
    assert "週次ブリーフィング" in call_kwargs["title"]


def test_run_briefing_without_list_all(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = MagicMock(spec=["list_by_status", "create"])
    store.list_by_status.return_value = [_make_task("t1")]
    store.create.return_value = {"id": "new-001", "title": "Plan"}
    result = run_briefing(store)
    assert result["task_id"] == "new-001"
    assert store.list_by_status.call_count >= 1


def test_run_briefing_empty_taskboard(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = _make_mock_store([])
    result = run_briefing(store)
    assert result["stats"]["total"] == 0
    store.create.assert_called_once()


def test_run_briefing_list_by_status_exception_tolerated(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = MagicMock(spec=["list_by_status", "create"])
    store.list_by_status.side_effect = RuntimeError("db error")
    store.create.return_value = {"id": "x1", "title": "Plan"}
    result = run_briefing(store)
    assert result["stats"]["total"] == 0


# ── _call_llm non-stub paths ──────────────────────────

def test_call_llm_returns_parsed_json(monkeypatch, tmp_path):
    monkeypatch.delenv("OLLAMA_STUB", raising=False)
    plan_json = json.dumps({
        "week_goal": "goal",
        "summary": "ok",
        "top_priorities": [{"task_id": "t1", "title": "T", "reason": "r"}],
        "agent_actions": {},
        "blockers": [],
        "done_count_last_week": 0,
        "risks": [],
    })
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director.call_chat",
        lambda *a, **kw: plan_json,
    )
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director._OUTPUT_SCHEMA_PATH",
        tmp_path / "nonexistent.json",
    )
    stats = {"total": 0, "blocked": [], "needs_human": [], "in_progress_long": [],
             "by_status": {}, "by_section": {}}
    result = _call_llm("http://localhost:11434", [], stats)
    assert result["week_goal"] == "goal"


def test_call_llm_invalid_json_falls_back(monkeypatch, tmp_path):
    monkeypatch.delenv("OLLAMA_STUB", raising=False)
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director.call_chat",
        lambda *a, **kw: "NOT JSON AT ALL",
    )
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director._OUTPUT_SCHEMA_PATH",
        tmp_path / "nonexistent.json",
    )
    stats = {"total": 0, "blocked": [], "needs_human": [], "in_progress_long": [],
             "by_status": {}, "by_section": {}}
    result = _call_llm("http://localhost:11434", [], stats)
    assert "LLM出力パースエラー" in result["summary"]


def test_call_llm_uses_registry_tag(monkeypatch, tmp_path):
    monkeypatch.delenv("OLLAMA_STUB", raising=False)
    plan_json = json.dumps({
        "week_goal": "g",
        "summary": "s",
        "top_priorities": [],
        "agent_actions": {},
        "blockers": [],
        "done_count_last_week": 0,
        "risks": [],
    })
    captured = {}

    def fake_chat(url, payload, timeout):
        captured["model"] = payload["model"]
        return plan_json

    monkeypatch.setattr("airflow_mcp.orchestrator.eng_director.call_chat", fake_chat)
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director._OUTPUT_SCHEMA_PATH",
        tmp_path / "nonexistent.json",
    )
    fake_registry = {"model_qwen25_14b_q4": {"ollama_tag": "custom-model:latest"}}
    monkeypatch.setattr(
        "airflow_mcp.common.io.load_model_registry",
        lambda *a, **kw: fake_registry,
    )
    stats = {"total": 0, "blocked": [], "needs_human": [], "in_progress_long": [],
             "by_status": {}, "by_section": {}}
    _call_llm("http://localhost:11434", [], stats)
    assert captured.get("model") == "custom-model:latest"


def test_call_llm_registry_exception_uses_default(monkeypatch, tmp_path):
    """registry 読み込み失敗時はデフォルトモデルタグにフォールバックする。"""
    monkeypatch.delenv("OLLAMA_STUB", raising=False)
    plan_json = json.dumps({
        "week_goal": "g", "summary": "s", "top_priorities": [],
        "agent_actions": {}, "blockers": [], "done_count_last_week": 0, "risks": [],
    })
    captured = {}

    def fake_chat(url, payload, timeout):
        captured["model"] = payload["model"]
        return plan_json

    monkeypatch.setattr("airflow_mcp.orchestrator.eng_director.call_chat", fake_chat)
    monkeypatch.setattr(
        "airflow_mcp.orchestrator.eng_director._OUTPUT_SCHEMA_PATH",
        tmp_path / "nonexistent.json",
    )
    monkeypatch.setattr(
        "airflow_mcp.common.io.load_model_registry",
        lambda *a, **kw: (_ for _ in ()).throw(OSError("registry missing")),
    )
    stats = {"total": 0, "blocked": [], "needs_human": [], "in_progress_long": [],
             "by_status": {}, "by_section": {}}
    _call_llm("http://localhost:11434", [], stats)
    assert captured.get("model") == "qwen2.5:14b-instruct-q4_K_M"


def test_run_briefing_plan_metadata_stored(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    store = _make_mock_store([_make_task("t1")])
    run_briefing(store)
    task_data = store.create.call_args[0][0]
    assert "metadata" in task_data
    assert "plan" in task_data["metadata"]
    assert "stats" in task_data["metadata"]

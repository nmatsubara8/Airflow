"""orchestrator/handoff_chain.py の単体テスト（DDD-06 §3 / DDD-08 §3.1）。"""
from __future__ import annotations

from pathlib import Path

import pytest

from airflow_mcp.orchestrator.handoff_chain import (
    _task_matches_trigger,
    find_next_steps,
    load_chains,
    record_chain_step,
    spawn_handoff_tasks,
)


# ── load_chains ───────────────────────────────────────────

def test_load_chains_returns_empty_when_no_file(tmp_path, monkeypatch):
    """ファイルが存在しない場合は空リストを返すこと。"""
    import airflow_mcp.orchestrator.handoff_chain as mod
    monkeypatch.setattr(mod, "_DEFAULT_CONFIG", tmp_path / "nonexistent.yaml")
    assert load_chains() == []


def test_load_chains_reads_config(tmp_path):
    """chains.yaml を読み込んでリストを返すこと。"""
    cfg = tmp_path / "chains.yaml"
    cfg.write_text(
        "chains:\n"
        "  - name: eng_qa\n"
        "    trigger:\n"
        "      section: engineering\n"
        "    steps:\n"
        "      - owner: qa\n"
        "        title_prefix: '[QA] '\n"
    )
    result = load_chains(cfg)
    assert len(result) == 1
    assert result[0]["name"] == "eng_qa"


def test_load_chains_non_dict_returns_empty(tmp_path):
    """chains.yaml が list など非dict の場合は空リストを返すこと。"""
    cfg = tmp_path / "chains.yaml"
    cfg.write_text("- item1\n- item2\n")
    assert load_chains(cfg) == []


def test_load_chains_missing_chains_key_returns_empty(tmp_path):
    """chains キーが無い場合は空リストを返すこと。"""
    cfg = tmp_path / "chains.yaml"
    cfg.write_text("other_key: value\n")
    assert load_chains(cfg) == []


def test_load_chains_chains_not_list_returns_empty(tmp_path):
    """chains キーが list でない場合は空リストを返すこと。"""
    cfg = tmp_path / "chains.yaml"
    cfg.write_text("chains: not-a-list\n")
    assert load_chains(cfg) == []


def test_load_chains_oserror_returns_empty(tmp_path, monkeypatch):
    """OSError 発生時は空リストを返すこと。"""
    from unittest.mock import patch
    cfg = tmp_path / "chains.yaml"
    cfg.write_text("chains: []\n")
    with patch.object(Path, "read_text", side_effect=OSError("unreadable")):
        assert load_chains(cfg) == []


# ── _task_matches_trigger ─────────────────────────────────

def test_matches_trigger_section_match():
    task = {"section": "engineering"}
    assert _task_matches_trigger(task, {"section": "engineering"}) is True


def test_matches_trigger_section_no_match():
    task = {"section": "content"}
    assert _task_matches_trigger(task, {"section": "engineering"}) is False


def test_matches_trigger_task_type_from_features():
    task = {"features": {"task_type": "code_gen"}}
    assert _task_matches_trigger(task, {"task_type": "code_gen"}) is True


def test_matches_trigger_task_type_fallback():
    task = {"task_type": "review", "features": {}}
    assert _task_matches_trigger(task, {"task_type": "review"}) is True


def test_matches_trigger_task_type_no_match():
    task = {"features": {"task_type": "summarize"}}
    assert _task_matches_trigger(task, {"task_type": "code_gen"}) is False


def test_matches_trigger_tags_match():
    task = {"tags": ["batch", "review"]}
    assert _task_matches_trigger(task, {"tags": ["review"]}) is True


def test_matches_trigger_tags_no_match():
    task = {"tags": ["batch"]}
    assert _task_matches_trigger(task, {"tags": ["review"]}) is False


def test_matches_trigger_multi_condition_all_match():
    task = {"section": "engineering", "features": {"task_type": "code_gen"}}
    assert _task_matches_trigger(task, {"section": "engineering", "task_type": "code_gen"}) is True


def test_matches_trigger_multi_condition_partial_fail():
    task = {"section": "content", "features": {"task_type": "code_gen"}}
    assert _task_matches_trigger(task, {"section": "engineering", "task_type": "code_gen"}) is False


def test_matches_trigger_empty_trigger_returns_false():
    assert _task_matches_trigger({"section": "engineering"}, {}) is False


# ── find_next_steps ───────────────────────────────────────

_CHAINS = [
    {
        "name": "eng_qa",
        "trigger": {"section": "engineering"},
        "steps": [{"owner": "qa", "title_prefix": "[QA] ", "priority": 6}],
    },
    {
        "name": "content_review",
        "trigger": {"section": "content"},
        "steps": [{"owner": "tech-lead", "title_prefix": "[Review] "}],
    },
]


def test_find_next_steps_returns_matching_steps():
    task = {"section": "engineering"}
    steps = find_next_steps(task, _CHAINS)
    assert len(steps) == 1
    assert steps[0]["owner"] == "qa"
    assert steps[0]["chain_name"] == "eng_qa"


def test_find_next_steps_no_match_returns_empty():
    task = {"section": "business"}
    steps = find_next_steps(task, _CHAINS)
    assert steps == []


def test_find_next_steps_multiple_chains_match():
    chains = [
        {"name": "chain_a", "trigger": {"section": "engineering"},
         "steps": [{"owner": "qa"}]},
        {"name": "chain_b", "trigger": {"section": "engineering"},
         "steps": [{"owner": "security"}]},
    ]
    task = {"section": "engineering"}
    steps = find_next_steps(task, chains)
    assert len(steps) == 2
    owners = {s["owner"] for s in steps}
    assert owners == {"qa", "security"}


def test_find_next_steps_skips_non_dict_chains():
    chains = [None, "bad", {"name": "ok", "trigger": {"section": "x"}, "steps": [{"owner": "a"}]}]
    task = {"section": "x"}
    steps = find_next_steps(task, chains)
    assert len(steps) == 1


def test_find_next_steps_skips_non_dict_steps():
    chains = [{"name": "c", "trigger": {"section": "x"}, "steps": ["bad", {"owner": "ok"}]}]
    task = {"section": "x"}
    steps = find_next_steps(task, chains)
    assert len(steps) == 1
    assert steps[0]["owner"] == "ok"


# ── spawn_handoff_tasks ───────────────────────────────────

@pytest.fixture
def task_store(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    return TaskStore(base_dir=str(tmp_path / "taskboard"))


@pytest.fixture
def done_task(task_store):
    t = task_store.create({
        "title": "実装タスク",
        "description": "コードを実装する",
        "section": "engineering",
        "tags": ["code_gen"],
        "priority": 5,
        "owner": None,
    })
    task_store.claim(t["id"], "agent_docs")
    task_store.add_handoff_note(t["id"], "実装完了。テストが必要です。", author="agent_docs")
    task_store.transition(t["id"], "done")
    return task_store.get(t["id"])


def test_spawn_handoff_tasks_creates_child_task(task_store, done_task):
    """chain 設定に一致するタスクが生成されること。"""
    chains = [{"name": "eng_qa", "trigger": {"section": "engineering"},
               "steps": [{"owner": "qa", "title_prefix": "[QA] ", "priority": 6}]}]
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    assert len(ids) == 1
    child = task_store.get(ids[0])
    assert child["title"].startswith("[QA] ")
    assert child["owner"] == "qa"
    assert "handoff" in child.get("tags", [])


def test_spawn_handoff_tasks_inherits_parent_context(task_store, done_task):
    """子タスクの description に親タスク ID と handoff_note の body が含まれること。"""
    chains = [{"name": "c", "trigger": {"section": "engineering"},
               "steps": [{"owner": "qa"}]}]
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    child = task_store.get(ids[0])
    assert done_task["id"] in child["description"]
    assert "実装完了。テストが必要です。" in child["description"]


def test_spawn_handoff_tasks_records_parent_task_id(task_store, done_task):
    """子タスクに parent_task_id が設定されること。"""
    chains = [{"name": "c", "trigger": {"section": "engineering"},
               "steps": [{"owner": "qa"}]}]
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    child = task_store.get(ids[0])
    assert child.get("parent_task_id") == done_task["id"]


def test_spawn_handoff_tasks_no_match_returns_empty(task_store, done_task):
    """chain に一致しない場合は空リストを返すこと。"""
    chains = [{"name": "c", "trigger": {"section": "content"},
               "steps": [{"owner": "writer"}]}]
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    assert ids == []


def test_spawn_handoff_tasks_empty_chains_returns_empty(task_store, done_task):
    """chain が空の場合は空リストを返すこと。"""
    assert spawn_handoff_tasks(done_task, task_store, []) == []


def test_spawn_handoff_tasks_exception_in_create_is_caught(task_store, done_task):
    """store.create が例外を投げてもクラッシュせず残りの step を続けること。"""
    chains = [{"name": "c", "trigger": {"section": "engineering"},
               "steps": [{"owner": "qa"}, {"owner": "tech-lead"}]}]
    call_count = [0]
    original_create = task_store.create

    def patched_create(data):
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("store error")
        return original_create(data)

    task_store.create = patched_create
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    assert len(ids) == 1  # 2 件中 1 件だけ成功


def test_spawn_handoff_tasks_uses_default_title_prefix(task_store, done_task):
    """title_prefix 未指定時は '[Handoff] ' が付くこと。"""
    chains = [{"name": "c", "trigger": {"section": "engineering"},
               "steps": [{"owner": "qa"}]}]
    ids = spawn_handoff_tasks(done_task, task_store, chains)
    child = task_store.get(ids[0])
    assert child["title"].startswith("[Handoff] ")


# ── record_chain_step ─────────────────────────────────────

def test_record_chain_step_appends_to_chain(task_store):
    """record_chain_step が task.chain フィールドにエントリを追記すること。"""
    task = task_store.create({
        "title": "chain test",
        "section": "engineering",
        "tags": [],
        "priority": 5,
        "owner": None,
    })
    task_store.claim(task["id"], "agent_docs")
    task_store.transition(task["id"], "done")

    record_chain_step(task["id"], "agent_docs", "log_abc123", task_store)
    updated = task_store.get(task["id"])
    chain = updated.get("chain", [])
    assert len(chain) == 1
    assert chain[0]["agent_id"] == "agent_docs"
    assert chain[0]["result_ref"] == "log_abc123"
    assert "completed_at" in chain[0]


def test_record_chain_step_accumulates_multiple_steps(task_store):
    """複数エージェントが順に完了した場合、chain に複数エントリが積まれること。"""
    task = task_store.create({
        "title": "multi step",
        "section": "engineering",
        "tags": [],
        "priority": 5,
        "owner": None,
    })
    task_store.claim(task["id"], "agent_a")
    task_store.transition(task["id"], "done")

    record_chain_step(task["id"], "agent_a", None, task_store)
    record_chain_step(task["id"], "agent_b", "log_xyz", task_store)
    updated = task_store.get(task["id"])
    chain = updated.get("chain", [])
    assert len(chain) == 2
    assert chain[0]["agent_id"] == "agent_a"
    assert chain[1]["agent_id"] == "agent_b"


def test_record_chain_step_handles_task_not_found(task_store):
    """存在しない task_id を渡してもクラッシュしないこと。"""
    record_chain_step("nonexistent_id", "agent_docs", None, task_store)


# ── tasks.spawn_handoff MCP ツール ────────────────────────

@pytest.mark.asyncio
async def test_tasks_spawn_handoff_mcp_tool(tmp_path, monkeypatch):
    """tasks.spawn_handoff MCP ツールが子タスクを生成して IDs を返すこと。"""
    import airflow_mcp.taskboard.server as sv
    from tests.helpers import make_mcp_caller
    import airflow_mcp.orchestrator.handoff_chain as hc_mod

    _call = make_mcp_caller(sv.call_tool)

    store = sv.TaskStore(base_dir=str(tmp_path / "tb"))
    sv._store = store
    monkeypatch.setattr(sv, "_get_store", lambda: store)

    t = store.create({"title": "元タスク", "section": "engineering",
                      "tags": [], "priority": 5, "owner": None})
    store.claim(t["id"], "agent_docs")
    store.transition(t["id"], "done")

    chains_cfg = tmp_path / "chains.yaml"
    chains_cfg.write_text(
        "chains:\n"
        "  - name: eng_qa\n"
        "    trigger:\n"
        "      section: engineering\n"
        "    steps:\n"
        "      - owner: qa\n"
        "        title_prefix: '[QA] '\n"
    )
    monkeypatch.setattr(hc_mod, "_DEFAULT_CONFIG", chains_cfg)
    monkeypatch.setattr(sv, "load_chains", lambda: hc_mod.load_chains(chains_cfg))

    result = await _call("tasks.spawn_handoff", {"task_id": t["id"]})
    assert result["count"] == 1
    assert len(result["spawned"]) == 1

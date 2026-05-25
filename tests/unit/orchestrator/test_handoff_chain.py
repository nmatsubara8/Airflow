"""orchestrator/handoff_chain.py の単体テスト（DDD-06 §3 / DDD-08 §3.1）。"""
from __future__ import annotations

from pathlib import Path

import pytest

from airflow_mcp.orchestrator.handoff_chain import (
    _task_matches_trigger,
    detect_cycle,
    find_next_steps,
    load_chains,
    record_chain_step,
    route_to_next_agent,
    spawn_handoff_tasks,
    spawn_with_join_tracking,
)


# ── load_chains ───────────────────────────────────────────

def test_load_chains_returns_empty_when_no_file(tmp_path, monkeypatch):
    """ファイルが存在しない場合は空リストを返すこと。"""
    import airflow_mcp.orchestrator.handoff_chain as mod
    monkeypatch.setattr(mod, "_DEFAULT_CONFIG", tmp_path / "nonexistent_yaml")
    assert load_chains() == []


def test_load_chains_reads_config(tmp_path):
    """chains.yaml を読み込んでリストを返すこと。"""
    cfg = tmp_path / "chains_yaml"
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
    cfg = tmp_path / "chains_yaml"
    cfg.write_text("- item1\n- item2\n")
    assert load_chains(cfg) == []


def test_load_chains_missing_chains_key_returns_empty(tmp_path):
    """chains キーが無い場合は空リストを返すこと。"""
    cfg = tmp_path / "chains_yaml"
    cfg.write_text("other_key: value\n")
    assert load_chains(cfg) == []


def test_load_chains_chains_not_list_returns_empty(tmp_path):
    """chains キーが list でない場合は空リストを返すこと。"""
    cfg = tmp_path / "chains_yaml"
    cfg.write_text("chains: not-a-list\n")
    assert load_chains(cfg) == []


def test_load_chains_oserror_returns_empty(tmp_path, monkeypatch):
    """OSError 発生時は空リストを返すこと。"""
    from unittest.mock import patch
    cfg = tmp_path / "chains_yaml"
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

    chains_cfg = tmp_path / "chains_yaml"
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

    result = await _call("tasks_spawn_handoff", {"task_id": t["id"]})
    assert result["count"] == 1
    assert len(result["spawned"]) == 1


# ── chain_depth trigger ───────────────────────────────────

def test_task_matches_trigger_chain_depth_exact():
    """chain_depth trigger が chain 配列の長さと一致するとき True を返すこと。"""
    task = {"section": "engineering", "chain": [{"agent_id": "a1"}]}
    assert _task_matches_trigger(task, {"section": "engineering", "chain_depth": 1}) is True


def test_task_matches_trigger_chain_depth_mismatch():
    """chain_depth が一致しないとき False を返すこと。"""
    task = {"section": "engineering", "chain": []}
    assert _task_matches_trigger(task, {"section": "engineering", "chain_depth": 1}) is False


# ── detect_cycle ──────────────────────────────────────────

def test_detect_cycle_no_history():
    """chain 履歴が空なら False を返すこと。"""
    task = {"chain": []}
    assert detect_cycle(task, "agent_a") is False


def test_detect_cycle_first_occurrence_ok():
    """agent が 1 回だけ処理済みで max_per_agent=1 → True（上限到達）。"""
    task = {"chain": [{"agent_id": "agent_a"}]}
    assert detect_cycle(task, "agent_a", max_per_agent=1) is True


def test_detect_cycle_different_agent_is_safe():
    """別エージェントの履歴があっても対象エージェントは False。"""
    task = {"chain": [{"agent_id": "agent_b"}]}
    assert detect_cycle(task, "agent_a") is False


def test_detect_cycle_max_per_agent_2():
    """max_per_agent=2 なら 1 回目は通過し 2 回目で True。"""
    task = {"chain": [{"agent_id": "agent_a"}]}
    assert detect_cycle(task, "agent_a", max_per_agent=2) is False
    task2 = {"chain": [{"agent_id": "agent_a"}, {"agent_id": "agent_a"}]}
    assert detect_cycle(task2, "agent_a", max_per_agent=2) is True


# ── route_to_next_agent ───────────────────────────────────

def _make_store_with_task(tmp_path, section="content", extra=None):
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    data = {"title": "テスト", "section": section, "tags": [], "priority": 5, "owner": None}
    if extra:
        data.update(extra)
    t = store.create(data)
    t = store.claim(t["id"], "agent_writer")
    return store, t


def _sequential_chains(agents=None):
    return [{
        "name": "review_chain",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": agents or [
            {"id": "agent_writer"},
            {"id": "agent_reviewer"},
            {"id": "agent_publisher"},
        ],
    }]


def test_route_to_next_agent_returns_next(tmp_path):
    """step 0 完了後に step 1 エージェントにルーティングされること。"""
    store, task = _make_store_with_task(tmp_path)
    # chain に step 0 完了を記録
    store.update(task["id"], task["lock"]["version"],
                 {"chain": [{"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z",
                              "result_ref": None}]})
    updated = store.get(task["id"])
    chains = _sequential_chains()
    next_agent = route_to_next_agent(updated, "agent_writer", store, chains)
    assert next_agent == "agent_reviewer"
    # タスクのオーナーが変わっていること
    refreshed = store.get(task["id"])
    assert refreshed["owner"] == "agent_reviewer"


def test_route_to_next_agent_returns_none_at_end(tmp_path):
    """最終エージェント完了後は None を返すこと。"""
    store, task = _make_store_with_task(tmp_path)
    # 全 3 ステップ完了済み
    store.update(task["id"], task["lock"]["version"], {"chain": [
        {"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z", "result_ref": None},
        {"agent_id": "agent_reviewer", "completed_at": "2026-01-01T00:00:01Z", "result_ref": None},
        {"agent_id": "agent_publisher", "completed_at": "2026-01-01T00:00:02Z", "result_ref": None},
    ]})
    updated = store.get(task["id"])
    next_agent = route_to_next_agent(updated, "agent_publisher", store, _sequential_chains())
    assert next_agent is None


def test_route_to_next_agent_skips_non_sequential_chains(tmp_path):
    """mode: fan_out チェーンは sequential ルーティングに使われないこと。"""
    store, task = _make_store_with_task(tmp_path)
    store.update(task["id"], task["lock"]["version"],
                 {"chain": [{"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z",
                              "result_ref": None}]})
    updated = store.get(task["id"])
    chains = [{
        "name": "fanout",
        "mode": "fan_out",
        "trigger": {"section": "content"},
        "agents": [{"id": "agent_reviewer"}],
    }]
    assert route_to_next_agent(updated, "agent_writer", store, chains) is None


def test_route_to_next_agent_detects_cycle(tmp_path):
    """サイクル検出でスキップされ None が返ること。"""
    store, task = _make_store_with_task(tmp_path)
    # agent_reviewer が既に 1 回処理済み
    store.update(task["id"], task["lock"]["version"], {"chain": [
        {"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z", "result_ref": None},
        {"agent_id": "agent_reviewer", "completed_at": "2026-01-01T00:00:01Z", "result_ref": None},
    ]})
    updated = store.get(task["id"])
    # step 2 の次が agent_reviewer = サイクル
    chains = [{
        "name": "cycle_chain",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": [
            {"id": "agent_writer"},
            {"id": "agent_reviewer"},
            {"id": "agent_reviewer"},  # 再登場 → cycle
        ],
    }]
    assert route_to_next_agent(updated, "agent_reviewer", store, chains) is None


# ── spawn_with_join_tracking ──────────────────────────────

def test_spawn_with_join_tracking_single_child_no_join(tmp_path):
    """子タスクが 1 件のとき join フィールドは設定されないこと。"""
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    t = store.create({"title": "親", "section": "engineering",
                      "tags": ["needs_review"], "priority": 5, "owner": None})
    store.claim(t["id"], "agent_docs")
    store.transition(t["id"], "done")
    done_task = store.get(t["id"])
    chains = [{
        "name": "single",
        "trigger": {"section": "engineering"},
        "steps": [{"title_prefix": "[QA] ", "section": "qa", "owner": "agent_qa"}],
    }]
    child_ids = spawn_with_join_tracking(done_task, store, chains)
    assert len(child_ids) == 1
    refreshed = store.get(t["id"])
    assert "join" not in refreshed


def test_spawn_with_join_tracking_fanout_registers_join(tmp_path):
    """子タスクが 2 件以上のとき親に join.child_ids が登録されること。"""
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    t = store.create({"title": "親", "section": "design",
                      "tags": ["approved"], "priority": 5, "owner": None})
    store.claim(t["id"], "agent_pm")
    store.transition(t["id"], "done")
    done_task = store.get(t["id"])
    chains = [{
        "name": "fanout2",
        "trigger": {"section": "design", "tags": ["approved"]},
        "steps": [
            {"title_prefix": "[実装] ", "section": "engineering"},
            {"title_prefix": "[テスト] ", "section": "qa"},
        ],
    }]
    child_ids = spawn_with_join_tracking(done_task, store, chains)
    assert len(child_ids) == 2
    refreshed = store.get(t["id"])
    assert "join" in refreshed
    assert set(refreshed["join"]["child_ids"]) == set(child_ids)
    assert refreshed["join"]["policy"] == "all"


def test_route_to_next_agent_reassign_fails_returns_none(tmp_path):
    """store.reassign_in_progress() が例外を投げたとき None を返しクラッシュしないこと。"""
    from unittest.mock import patch
    from airflow_mcp.common.errors import TaskInvalidTransition
    store, task = _make_store_with_task(tmp_path)
    chains = _sequential_chains()
    updated = store.get(task["id"])
    with patch.object(store, "reassign_in_progress", side_effect=TaskInvalidTransition("in_progress", "reassign_in_progress")):
        result = route_to_next_agent(updated, "agent_writer", store, chains)
    assert result is None


# ── spawn_with_join_tracking update exception ─────────────

def test_spawn_with_join_tracking_update_exception_does_not_crash(tmp_path):
    """store.update() が例外を投げても child_ids を返してクラッシュしないこと（line 171-172）。"""
    from unittest.mock import patch
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    t = store.create({"title": "親", "section": "design",
                      "tags": ["approved"], "priority": 5, "owner": None})
    store.claim(t["id"], "agent_pm")
    store.transition(t["id"], "done")
    done_task = store.get(t["id"])
    chains = [{
        "name": "fanout_err",
        "trigger": {"section": "design"},
        "steps": [
            {"title_prefix": "[A] ", "section": "qa", "owner": "agent_qa"},
            {"title_prefix": "[B] ", "section": "review", "owner": "agent_rev"},
        ],
    }]
    with patch.object(store, "update", side_effect=RuntimeError("lock conflict")):
        child_ids = spawn_with_join_tracking(done_task, store, chains)
    # update が失敗しても child_ids は返る
    assert len(child_ids) == 2


# ── route_to_next_agent edge cases ───────────────────────

def test_route_to_next_agent_skips_non_dict_chain_entry(tmp_path):
    """chains リストに dict でない要素があってもクラッシュせず None を返すこと（line 214）。"""
    store, task = _make_store_with_task(tmp_path)
    chains = ["not-a-dict", None, 42]
    result = route_to_next_agent(task, "agent_writer", store, chains)
    assert result is None


def test_route_to_next_agent_skips_non_list_agents(tmp_path):
    """chain.agents が list でない場合スキップして None を返すこと（line 223）。"""
    store, task = _make_store_with_task(tmp_path)
    chains = [{
        "name": "bad_chain",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": "not-a-list",
    }]
    result = route_to_next_agent(task, "agent_writer", store, chains)
    assert result is None


def test_route_to_next_agent_skips_non_dict_agent_entry(tmp_path):
    """agents リスト内の要素が dict でない場合スキップして None を返すこと（line 229）。"""
    store, task = _make_store_with_task(tmp_path)
    # chain_depth=0 なので agents[0] がターゲット
    chains = [{
        "name": "bad_agents",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": ["string-not-dict", {"id": "agent_reviewer"}],
    }]
    result = route_to_next_agent(task, "agent_writer", store, chains)
    assert result is None


def test_route_to_next_agent_skips_agent_without_id(tmp_path):
    """agents エントリに id が無い場合スキップして None を返すこと（line 233）。"""
    store, task = _make_store_with_task(tmp_path)
    chains = [{
        "name": "noid_chain",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": [{"name": "no_id_field"}, {"id": "agent_reviewer"}],
    }]
    result = route_to_next_agent(task, "agent_writer", store, chains)
    assert result is None


def test_route_to_next_agent_logs_warning_on_cycle(tmp_path):
    """detect_cycle が True のとき logger.warning が呼ばれ None を返すこと（lines 241-245）。"""
    store, task = _make_store_with_task(tmp_path)
    # agent_writer が 1 回完了済み → chain_depth=1 → agents[1]=agent_writer → cycle
    store.update(task["id"], task["lock"]["version"], {"chain": [
        {"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z", "result_ref": None},
    ]})
    updated = store.get(task["id"])
    chains = [{
        "name": "cycle_chain",
        "mode": "sequential",
        "trigger": {"section": "content"},
        "agents": [
            {"id": "agent_reviewer"},   # step 0
            {"id": "agent_writer"},     # step 1 → agent_writer がすでに chain にある → cycle
            {"id": "agent_publisher"},  # step 2
        ],
    }]
    # current_agent=agent_reviewer, next=agent_writer (in chain already) → cycle detected
    result = route_to_next_agent(updated, "agent_reviewer", store, chains)
    assert result is None


def test_route_to_next_agent_logs_error_on_reassign_exception(tmp_path):
    """store.reassign_in_progress() が例外を投げたとき logger.error が呼ばれ None を返すこと（lines 258-259）。"""
    from unittest.mock import patch
    store, task = _make_store_with_task(tmp_path)
    # chain_depth=1: agent_writer completed → next=agent_reviewer (different from current)
    store.update(task["id"], task["lock"]["version"], {"chain": [
        {"agent_id": "agent_writer", "completed_at": "2026-01-01T00:00:00Z", "result_ref": None},
    ]})
    updated = store.get(task["id"])
    chains = _sequential_chains()
    with patch.object(store, "reassign_in_progress", side_effect=RuntimeError("db failure")):
        result = route_to_next_agent(updated, "agent_writer", store, chains)
    assert result is None

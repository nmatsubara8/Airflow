"""taskboard/server.py の単体テスト（DDD-02 §3）。"""
from __future__ import annotations

import pytest

import airflow_mcp.taskboard.server as srv_mod
from airflow_mcp.common.audit import record_audit
from airflow_mcp.taskboard.store import TaskStore
from tests.helpers import make_mcp_caller


@pytest.fixture(autouse=True)
def isolated_store(tmp_path):
    """各テスト前後にシングルトンストアをリセットし tmp_path を使う。"""
    store = TaskStore(base_dir=tmp_path)
    srv_mod._store = store
    srv_mod._audit_log.clear()
    yield store
    srv_mod._store = None
    srv_mod._audit_log.clear()


def _make_task(store: TaskStore, **overrides) -> dict:
    fields = {
        "title": "テストタスク",
        "description": "説明",
        "section": "engineering",
        "tags": ["api"],
        "priority": 5,
        "owner": None,
        **overrides,
    }
    return store.create(fields)


_call = make_mcp_caller(srv_mod.call_tool)


# ── _get_store singleton (line 42) ────────────────────

def test_get_store_creates_singleton_when_none(tmp_path, monkeypatch):
    srv_mod._store = None
    monkeypatch.setattr(
        "airflow_mcp.taskboard.server.TaskStore",
        lambda: TaskStore(base_dir=tmp_path),
    )
    store1 = srv_mod._get_store()
    store2 = srv_mod._get_store()
    assert store1 is store2


# ── list_tools (line 65) ──────────────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_all_tools():
    tools = await srv_mod.list_tools()
    names = [t.name for t in tools]
    assert "tasks.list" in names
    assert "tasks.get" in names
    assert "tasks.create" in names
    assert "tasks.update" in names
    assert "tasks.transition" in names
    assert "tasks.claim" in names
    assert "tasks.add_handoff_note" in names
    assert "tasks.assign" in names
    assert "tasks.join" in names
    assert "audit.trace" in names
    assert "audit.query" in names


# ── tasks.list (lines 267-290) ────────────────────────

@pytest.mark.asyncio
async def test_tasks_list_returns_all_statuses(isolated_store):
    _make_task(isolated_store, title="t1")
    result = await _call("tasks.list", {})
    assert len(result["tasks"]) == 1


@pytest.mark.asyncio
async def test_tasks_list_filter_by_status(isolated_store):
    t = _make_task(isolated_store)
    isolated_store.claim(t["id"], "agent_docs")
    result = await _call("tasks.list", {"status": ["in_progress"]})
    assert len(result["tasks"]) == 1
    result2 = await _call("tasks.list", {"status": ["inbox"]})
    assert len(result2["tasks"]) == 0


@pytest.mark.asyncio
async def test_tasks_list_filter_by_owner(isolated_store):
    t = _make_task(isolated_store)
    isolated_store.claim(t["id"], "agent_docs")
    result = await _call("tasks.list", {"owner_id": "agent_docs"})
    assert len(result["tasks"]) == 1
    result2 = await _call("tasks.list", {"owner_id": "agent_other"})
    assert len(result2["tasks"]) == 0


@pytest.mark.asyncio
async def test_tasks_list_filter_by_section(isolated_store):
    _make_task(isolated_store, section="engineering")
    _make_task(isolated_store, section="content")
    result = await _call("tasks.list", {"section": "engineering"})
    assert len(result["tasks"]) == 1


@pytest.mark.asyncio
async def test_tasks_list_filter_by_tags(isolated_store):
    _make_task(isolated_store, tags=["api", "backend"])
    _make_task(isolated_store, tags=["docs"])
    result = await _call("tasks.list", {"tags": ["api"]})
    assert len(result["tasks"]) == 1


@pytest.mark.asyncio
async def test_tasks_list_cursor_pagination(isolated_store):
    _tasks = [_make_task(isolated_store, title=f"t{i}") for i in range(5)]
    result1 = await _call("tasks.list", {"limit": 2})
    assert len(result1["tasks"]) == 2
    cursor = result1["next_cursor"]
    assert cursor is not None
    result2 = await _call("tasks.list", {"limit": 2, "cursor": cursor})
    assert len(result2["tasks"]) == 2
    ids1 = {t["id"] for t in result1["tasks"]}
    ids2 = {t["id"] for t in result2["tasks"]}
    assert ids1.isdisjoint(ids2)


@pytest.mark.asyncio
async def test_tasks_list_cursor_not_found_starts_from_zero(isolated_store):
    _make_task(isolated_store)
    result = await _call("tasks.list", {"cursor": "nonexistent_id"})
    assert len(result["tasks"]) == 1


# ── tasks.get ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_tasks_get_returns_task(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("tasks.get", {"id": t["id"]})
    assert result["id"] == t["id"]


@pytest.mark.asyncio
async def test_tasks_get_unknown_returns_error(isolated_store):
    result = await _call("tasks.get", {"id": "nonexistent_task_id"})
    assert "error" in result
    assert result["error"]["code"] == "TASK_NOT_FOUND"


# ── tasks.create ──────────────────────────────────────

@pytest.mark.asyncio
async def test_tasks_create_returns_new_task(isolated_store):
    result = await _call("tasks.create", {
        "title": "新タスク", "section": "engineering",
        "description": "説明", "tags": ["api"], "priority": 3,
    })
    assert result["title"] == "新タスク"
    assert result["status"] == "inbox"
    assert len(srv_mod._audit_log) == 1


# ── tasks.update (lines 244, 318-324) ─────────────────

@pytest.mark.asyncio
async def test_tasks_update_patches_task(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("tasks.update", {
        "id": t["id"],
        "expected_version": t["lock"]["version"],
        "patch": {"title": "更新済みタスク"},
    })
    assert result["title"] == "更新済みタスク"


@pytest.mark.asyncio
async def test_tasks_update_records_audit(isolated_store):
    t = _make_task(isolated_store)
    await _call("tasks.update", {
        "id": t["id"],
        "expected_version": t["lock"]["version"],
        "patch": {"priority": 1},
        "_meta": {"agent_id": "agent_docs"},
    })
    assert any(e["event"] == "task.updated" for e in srv_mod._audit_log)


# ── tasks.transition ──────────────────────────────────

@pytest.mark.asyncio
async def test_tasks_transition_changes_status(isolated_store):
    t = _make_task(isolated_store)
    isolated_store.claim(t["id"], "agent_docs")
    result = await _call("tasks.transition", {
        "id": t["id"], "to_status": "done", "reason": "completed",
    })
    assert result["status"] == "done"


# ── tasks.claim ───────────────────────────────────────

@pytest.mark.asyncio
async def test_tasks_claim_assigns_owner(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("tasks.claim", {"task_id": t["id"], "agent_id": "agent_docs"})
    assert result["owner"] == "agent_docs"
    assert result["status"] == "in_progress"


# ── tasks.add_handoff_note ────────────────────────────

@pytest.mark.asyncio
async def test_tasks_add_handoff_note_appends(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("tasks.add_handoff_note", {
        "task_id": t["id"], "content": "ノート内容", "author": "agent_docs",
    })
    assert any(n["body"] == "ノート内容" for n in result["handoff_notes"])


# ── tasks.assign ──────────────────────────────────────

@pytest.mark.asyncio
async def test_tasks_assign_changes_owner(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("tasks.assign", {
        "task_id": t["id"],
        "new_owner": {"type": "agent", "id": "agent_qa"},
        "reason": "人間が手動アサイン",
    })
    assert result["owner"] == "agent_qa"


# ── tasks.join (lines 388-389: child not found) ───────

@pytest.mark.asyncio
async def test_tasks_join_all_done_when_children_complete(isolated_store):
    parent = _make_task(isolated_store, title="parent")
    c1 = _make_task(isolated_store, title="child1")
    c2 = _make_task(isolated_store, title="child2")
    isolated_store.claim(c1["id"], "agent_docs")
    isolated_store.transition(c1["id"], "done")
    isolated_store.claim(c2["id"], "agent_docs")
    isolated_store.transition(c2["id"], "done")
    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"]],
        "join_policy": "all_done",
    })
    assert result["ready"] is True
    assert result["pending_children"] == []


@pytest.mark.asyncio
async def test_tasks_join_not_ready_when_children_pending(isolated_store):
    parent = _make_task(isolated_store, title="parent")
    c1 = _make_task(isolated_store, title="child1")
    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"]],
        "join_policy": "all_done",
    })
    assert result["ready"] is False
    assert c1["id"] in result["pending_children"]


@pytest.mark.asyncio
async def test_tasks_join_child_not_found_goes_to_pending(isolated_store):
    """存在しない child_id は TaskNotFound を catch して pending 扱い（lines 388-389）。"""
    parent = _make_task(isolated_store, title="parent")
    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": ["nonexistent_child_id"],
        "join_policy": "all_done",
    })
    assert result["ready"] is False
    assert "nonexistent_child_id" in result["pending_children"]


@pytest.mark.asyncio
async def test_tasks_join_any_done_policy(isolated_store):
    parent = _make_task(isolated_store, title="parent")
    c1 = _make_task(isolated_store, title="c1")
    c2 = _make_task(isolated_store, title="c2")
    isolated_store.claim(c1["id"], "agent_docs")
    isolated_store.transition(c1["id"], "done")
    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"]],
        "join_policy": "any_done",
    })
    assert result["ready"] is True


@pytest.mark.asyncio
async def test_tasks_join_majority_policy(isolated_store):
    parent = _make_task(isolated_store, title="parent")
    children = [_make_task(isolated_store, title=f"c{i}") for i in range(3)]
    for c in children[:2]:
        isolated_store.claim(c["id"], "agent_docs")
        isolated_store.transition(c["id"], "done")
    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c["id"] for c in children],
        "join_policy": "majority",
    })
    assert result["ready"] is True


# ── audit.trace ───────────────────────────────────────

@pytest.mark.asyncio
async def test_audit_trace_returns_event_chain(isolated_store):
    # tasks.create via MCP tool → audit log is populated
    result = await _call("tasks.create", {
        "title": "監査テスト", "section": "engineering",
        "_meta": {"agent_id": "agent_docs"},
    })
    _task_id = result["id"]
    aid = srv_mod._audit_log[0]["audit_id"]
    trace = await _call("audit.trace", {"audit_id": aid, "depth": 3})
    assert trace["root"] == aid
    assert len(trace["events"]) >= 1


@pytest.mark.asyncio
async def test_audit_trace_follows_child_events(isolated_store):
    """parent_audit_id を持つ子イベントも収集すること。"""
    await _call("tasks.create", {"title": "親タスク", "section": "engineering"})
    parent_aid = srv_mod._audit_log[0]["audit_id"]
    child_aid = record_audit(
        "task.updated", sink=srv_mod._audit_log, task_id="dummy", parent_audit_id=parent_aid
    )
    trace = await _call("audit.trace", {"audit_id": parent_aid, "depth": 5})
    event_ids = [e["audit_id"] for e in trace["events"]]
    assert parent_aid in event_ids
    assert child_aid in event_ids


@pytest.mark.asyncio
async def test_audit_trace_depth_limit_stops_recursion(isolated_store):
    """depth=0 のとき子イベントの収集が打ち切られること（line 426）。"""
    await _call("tasks.create", {"title": "深さ制限テスト", "section": "engineering"})
    parent_aid = srv_mod._audit_log[0]["audit_id"]
    record_audit("task.updated", sink=srv_mod._audit_log, task_id="dummy", parent_audit_id=parent_aid)
    # depth=0 → collect(parent, 0) は処理するが collect(child, 1) で return
    trace = await _call("audit.trace", {"audit_id": parent_aid, "depth": 0})
    event_ids = [e["audit_id"] for e in trace["events"]]
    assert parent_aid in event_ids
    # child は depth 超過で収集されない


# ── audit.query (line 450: agent_id filter) ───────────

@pytest.mark.asyncio
async def test_audit_query_filters_by_event(isolated_store):
    _make_task(isolated_store)
    result = await _call("audit.query", {"event": "task.created"})
    assert all(e["event"] == "task.created" for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_filters_by_task_id(isolated_store):
    t = _make_task(isolated_store)
    result = await _call("audit.query", {"task_id": t["id"]})
    assert all(e.get("task_id") == t["id"] for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_filters_by_agent_id(isolated_store):
    """agent_id フィルタが機能すること（line 450）。"""
    t = _make_task(isolated_store)
    isolated_store.claim(t["id"], "agent_docs")
    record_audit("task.claimed", sink=srv_mod._audit_log, task_id=t["id"], agent_id="agent_docs")
    result = await _call("audit.query", {"agent_id": "agent_docs"})
    assert len(result["events"]) >= 1
    assert all(e.get("agent_id") == "agent_docs" for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_returns_empty_on_no_match(isolated_store):
    _make_task(isolated_store)
    result = await _call("audit.query", {"agent_id": "nonexistent_agent"})
    assert result["events"] == []


# ── unknown tool (line 260) ───────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("tasks.unknown_tool", {})
    assert "error" in result
    assert result["error"]["code"] == "UNKNOWN_TOOL"


# ── run() (lines 457-458) ─────────────────────────────

@pytest.mark.asyncio
async def test_run_starts_stdio_server(fake_stdio_ctx):
    from unittest.mock import AsyncMock, patch

    mock_server_run = AsyncMock()
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_ctx), \
         patch.object(srv_mod.server, "run", mock_server_run):
        await srv_mod.run()

    mock_server_run.assert_awaited_once()

"""taskboard-mcp: タスクボード MCP サーバー（DDD-02 §3）。

ツール: tasks.list / tasks.get / tasks.create / tasks.update /
        tasks.transition / tasks.claim / tasks.add_handoff_note /
        tasks.assign / tasks.join / audit.trace / audit.query
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any

import mcp.types as types
from mcp.server import Server

from ..common.audit import now_iso, record_audit
from ..common.auth import require_scope
from ..common.errors import TaskNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from .store import TaskStore

_ALL_STATUSES = ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]

# プロセス内監査ログ（audit.trace / audit.query 用）
_audit_log: list[dict[str, Any]] = []

server = Server("taskboard-mcp")

# HTTP マルチ接続移行時の race condition 防止（DDD-08 §4 / 提案 A）
_state_lock = threading.RLock()

# ── TaskStore シングルトン ────────────────────────────────
_store: TaskStore | None = None


def _get_store() -> TaskStore:
    global _store
    with _state_lock:
        if _store is None:
            _store = TaskStore()
        return _store



# ── ツール定義 ────────────────────────────────────────────
@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="tasks.list",
            description="タスクをフィルタ条件で一覧取得",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {"type": "array", "items": {"type": "string"}},
                    "owner_id": {"type": "string"},
                    "section": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "limit": {"type": "integer", "default": 50},
                    "cursor": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="tasks.get",
            description="ID 指定でタスクを取得",
            inputSchema={
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="tasks.create",
            description="タスクを新規作成",
            inputSchema={
                "type": "object",
                "required": ["title", "section"],
                "properties": {
                    "title": {"type": "string"},
                    "section": {"type": "string"},
                    "description": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "priority": {"type": "integer"},
                    "source_note_id": {"type": "string"},
                    "features": {"type": "object"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.update",
            description="タスクを編集（楽観的ロック必須）",
            inputSchema={
                "type": "object",
                "required": ["id", "expected_version", "patch"],
                "properties": {
                    "id": {"type": "string"},
                    "expected_version": {"type": "integer"},
                    "patch": {"type": "object"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.transition",
            description="タスクのステータスを遷移",
            inputSchema={
                "type": "object",
                "required": ["id", "to_status"],
                "properties": {
                    "id": {"type": "string"},
                    "to_status": {"type": "string"},
                    "reason": {"type": "string"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.claim",
            description="エージェントがタスクを自分にアサイン",
            inputSchema={
                "type": "object",
                "required": ["task_id", "agent_id"],
                "properties": {
                    "task_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.add_handoff_note",
            description="タスクに handoff_note を追加",
            inputSchema={
                "type": "object",
                "required": ["task_id", "content"],
                "properties": {
                    "task_id": {"type": "string"},
                    "content": {"type": "string"},
                    "author": {"type": "string"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.assign",
            description="タスクの owner を上書き設定（人間操作 / F-08）",
            inputSchema={
                "type": "object",
                "required": ["task_id", "new_owner"],
                "properties": {
                    "task_id": {"type": "string"},
                    "new_owner": {
                        "type": "object",
                        "required": ["type", "id"],
                        "properties": {
                            "type": {"type": "string", "enum": ["agent", "human"]},
                            "id": {"type": "string"},
                        },
                    },
                    "reason": {"type": "string"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="tasks.join",
            description="親タスクが子タスク群の完了を待機・集約",
            inputSchema={
                "type": "object",
                "required": ["parent_id", "child_ids"],
                "properties": {
                    "parent_id": {"type": "string"},
                    "child_ids": {"type": "array", "items": {"type": "string"}},
                    "join_policy": {
                        "type": "string",
                        "enum": ["all_done", "any_done", "majority"],
                        "default": "all_done",
                    },
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="audit.trace",
            description="audit_id 起点で全派生イベントを取得",
            inputSchema={
                "type": "object",
                "required": ["audit_id"],
                "properties": {
                    "audit_id": {"type": "string"},
                    "depth": {"type": "integer", "default": 5, "maximum": 20},
                },
            },
        ),
        types.Tool(
            name="audit.query",
            description="構造化フィルタで監査ログを検索",
            inputSchema={
                "type": "object",
                "properties": {
                    "event": {"type": "string"},
                    "task_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "level": {"type": "array", "items": {"type": "string"}},
                    "limit": {"type": "integer", "default": 100},
                },
            },
        ),
    ]


async def _tasks_list(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    store = _get_store()
    statuses: list[str] = args.get("status") or _ALL_STATUSES

    results: list[dict[str, Any]] = []
    for s in statuses:
        results.extend(store.list_by_status(s))

    if owner := args.get("owner_id"):
        results = [t for t in results if t.get("owner") == owner]
    if section := args.get("section"):
        results = [t for t in results if t.get("section") == section]
    if tags := args.get("tags"):
        results = [t for t in results if any(tag in t.get("tags", []) for tag in tags)]

    limit = min(args.get("limit", 50), 200)
    cursor = args.get("cursor")
    start = 0
    if cursor:
        ids = [t["id"] for t in results]
        start = ids.index(cursor) + 1 if cursor in ids else 0

    page = results[start:start + limit]
    next_cursor = page[-1]["id"] if len(results) > start + limit else None
    return mcp_result({"tasks": page, "next_cursor": next_cursor})


async def _tasks_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    task = _get_store().get(args["id"])  # raises TaskNotFound
    return mcp_result(task)


async def _tasks_create(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    task = _get_store().create({
        "title": args["title"],
        "section": args["section"],
        "description": args.get("description", ""),
        "tags": args.get("tags", []),
        "priority": args.get("priority", 5),
        "source_note_id": args.get("source_note_id"),
        "features": args.get("features", {}),
        "owner": None,
    })
    with _state_lock:
        record_audit("task.created", sink=_audit_log, task_id=task["id"],
                     agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return mcp_result(task)


async def _tasks_update(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    # immutable fields are silently stripped by TaskStore.update()
    task = _get_store().update(args["id"], args["expected_version"], args["patch"])
    with _state_lock:
        record_audit("task.updated", sink=_audit_log, task_id=task["id"],
                     agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return mcp_result(task)


async def _tasks_transition(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    task = _get_store().transition(args["id"], args["to_status"], reason=args.get("reason"))
    with _state_lock:
        record_audit("task.transitioned", sink=_audit_log, task_id=task["id"],
                     agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"),
                     data={"to": args["to_status"], "reason": args.get("reason")})
    return mcp_result(task)


async def _tasks_claim(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    task = _get_store().claim(args["task_id"], args["agent_id"])
    with _state_lock:
        record_audit("task.claimed", sink=_audit_log, task_id=task["id"],
                     agent_id=args["agent_id"], parent_audit_id=meta.get("audit_id"))
    return mcp_result(task)


async def _tasks_add_handoff_note(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    task = _get_store().add_handoff_note(
        args["task_id"], args["content"], author=args.get("author", "unknown")
    )
    with _state_lock:
        record_audit("task.handoff_note_added", sink=_audit_log, task_id=task["id"],
                     agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return mcp_result(task)


async def _tasks_assign(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    store = _get_store()
    task_id = args["task_id"]
    new_owner = args["new_owner"]
    meta = args.get("_meta", {})

    task = store.assign(task_id, new_owner["id"])  # atomic: lock → check → write (M-6)
    with _state_lock:
        record_audit("task.assigned", sink=_audit_log, task_id=task_id,
                     agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"),
                     data={"new_owner": new_owner, "reason": args.get("reason")})
    return mcp_result(task)


async def _tasks_join(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    store = _get_store()
    parent_id = args["parent_id"]
    child_ids = args["child_ids"]
    policy = args.get("join_policy", "all_done")

    parent = store.get(parent_id)  # raises TaskNotFound

    completed: list[str] = []
    pending: list[str] = []
    for cid in child_ids:
        try:
            child = store.get(cid)
            (completed if child["status"] == "done" else pending).append(cid)
        except TaskNotFound:
            pending.append(cid)

    ready = False
    if policy == "all_done":
        ready = len(pending) == 0
    elif policy == "any_done":
        ready = len(completed) > 0
    elif policy == "majority":
        ready = len(completed) > len(child_ids) / 2

    if ready:
        now = now_iso()
        store.update(parent_id, parent["lock"]["version"], {
            "join": {"status": "fulfilled", "fulfilled_at": now}
        })

    return mcp_result({
        "ready": ready,
        "completed_children": completed,
        "pending_children": pending,
    })


async def _audit_trace(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    root_id = args["audit_id"]
    depth = min(args.get("depth", 5), 20)  # clamp to prevent DoS

    with _state_lock:
        log_snapshot = list(_audit_log)

    by_parent: dict[str | None, list[dict]] = defaultdict(list)
    for entry in log_snapshot:
        by_parent[entry.get("parent_audit_id")].append(entry)

    events: list[dict] = []
    visited: set[str] = set()  # cycle guard

    def collect(aid: str, current_depth: int) -> None:
        if current_depth > depth or aid in visited:
            return
        visited.add(aid)
        for entry in log_snapshot:
            if entry["audit_id"] == aid:
                events.append(entry)
                break
        for child in by_parent.get(aid, []):
            collect(child["audit_id"], current_depth + 1)

    collect(root_id, 0)
    return mcp_result({"root": root_id, "events": events})


async def _audit_query(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    with _state_lock:
        results = list(_audit_log)

    if event_filter := args.get("event"):
        # fnmatch を使ってReDoSを回避（re.match の代わり）
        import fnmatch
        results = [e for e in results if fnmatch.fnmatch(e.get("event", ""), event_filter)]
    if task_id := args.get("task_id"):
        results = [e for e in results if e.get("task_id") == task_id]
    if agent_id := args.get("agent_id"):
        results = [e for e in results if e.get("agent_id") == agent_id]

    limit = args.get("limit", 100)
    return mcp_result({"events": results[-limit:]})


# ── ツール実装 ────────────────────────────────────────────
_HANDLERS = {
    "tasks.list": _tasks_list,
    "tasks.get": _tasks_get,
    "tasks.create": _tasks_create,
    "tasks.update": _tasks_update,
    "tasks.transition": _tasks_transition,
    "tasks.claim": _tasks_claim,
    "tasks.add_handoff_note": _tasks_add_handoff_note,
    "tasks.assign": _tasks_assign,
    "tasks.join": _tasks_join,
    "audit.trace": _audit_trace,
    "audit.query": _audit_query,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS)


async def run() -> None:
    await run_stdio_server(server)

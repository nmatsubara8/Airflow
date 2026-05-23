"""taskboard-mcp: タスクボード MCP サーバー（DDD-02 §3）。

ツール: tasks.list / tasks.get / tasks.create / tasks.update /
        tasks.transition / tasks.claim / tasks.add_handoff_note /
        tasks.assign / tasks.join / audit.trace / audit.query
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.auth import get_token_from_env, require_scope
from ..common.errors import (
    AirflowError,
    TaskAlreadyClaimed,
    TaskInvalidTransition,
    TaskLockConflict,
    TaskNotFound,
)

# ── 有効なステータス遷移（DDD-02 §3.2）──────────────────
_TRANSITIONS: dict[str, set[str]] = {
    "inbox":       {"in_progress", "cancelled"},
    "in_progress": {"done", "needs_human", "blocked", "cancelled"},
    "needs_human": {"in_progress", "cancelled"},
    "blocked":     {"in_progress", "cancelled"},
    "done":        set(),
    "cancelled":   set(),
}

# ── インメモリストア ──────────────────────────────────────
_tasks: dict[str, dict[str, Any]] = {}
_audit_log: list[dict[str, Any]] = []

server = Server("taskboard-mcp")


def _etag(task: dict[str, Any]) -> str:
    body = json.dumps(task, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(body.encode()).hexdigest()


def _result(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False))]


def _record_audit(event: str, component: str = "taskboard-mcp", **kw: Any) -> str:
    aid = new_audit_id()
    entry = {"audit_id": aid, "event": event, "component": component,
             "timestamp": now_iso(), **kw}
    _audit_log.append(entry)
    log_event("info", event, component, aid, **{
        k: v for k, v in kw.items()
        if k in ("parent_audit_id", "task_id", "agent_id", "data")
    })
    return aid


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
                    "depth": {"type": "integer", "default": 5},
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


# ── ツール実装 ────────────────────────────────────────────
@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    token = get_token_from_env()
    try:
        if name == "tasks.list":
            return await _tasks_list(arguments, token)
        elif name == "tasks.get":
            return await _tasks_get(arguments, token)
        elif name == "tasks.create":
            return await _tasks_create(arguments, token)
        elif name == "tasks.update":
            return await _tasks_update(arguments, token)
        elif name == "tasks.transition":
            return await _tasks_transition(arguments, token)
        elif name == "tasks.claim":
            return await _tasks_claim(arguments, token)
        elif name == "tasks.add_handoff_note":
            return await _tasks_add_handoff_note(arguments, token)
        elif name == "tasks.assign":
            return await _tasks_assign(arguments, token)
        elif name == "tasks.join":
            return await _tasks_join(arguments, token)
        elif name == "audit.trace":
            return await _audit_trace(arguments, token)
        elif name == "audit.query":
            return await _audit_query(arguments, token)
        else:
            return _result({"error": {"code": "UNKNOWN_TOOL", "message": f"未知のツール: {name}"}})
    except AirflowError as e:
        return e.to_mcp_result()


async def _tasks_list(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    results = list(_tasks.values())

    if statuses := args.get("status"):
        results = [t for t in results if t["status"] in statuses]
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
    return _result({"tasks": page, "next_cursor": next_cursor})


async def _tasks_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    task_id = args["id"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)
    return _result(_tasks[task_id])


async def _tasks_create(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    meta = args.get("_meta", {})
    aid = new_audit_id()
    ts = now_iso()
    task_id = "task_" + aid[:16].lower().replace("-", "")

    task: dict[str, Any] = {
        "schema_version": "1.0.0",
        "id": task_id,
        "title": args["title"],
        "section": args["section"],
        "status": "inbox",
        "priority": args.get("priority", 5),
        "tags": args.get("tags", []),
        "description": args.get("description", ""),
        "owner": None,
        "source_note_id": args.get("source_note_id"),
        "features": args.get("features", {}),
        "handoff_notes": [],
        "join": None,
        "lock": {"version": 1, "etag": "sha256:" + "0" * 64},
        "created_at": ts,
        "updated_at": ts,
    }
    task["lock"]["etag"] = _etag(task)
    _tasks[task_id] = task
    _record_audit("task.created", task_id=task_id,
                  agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return _result(task)


async def _tasks_update(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    task_id = args["id"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)

    task = _tasks[task_id]
    if task["lock"]["version"] != args["expected_version"]:
        raise TaskLockConflict(task_id, task["lock"]["version"])

    patch = args["patch"]
    for key, value in patch.items():
        if key not in ("id", "created_at", "lock", "schema_version"):
            task[key] = value
    task["updated_at"] = now_iso()
    task["lock"]["version"] += 1
    task["lock"]["etag"] = _etag(task)

    meta = args.get("_meta", {})
    _record_audit("task.updated", task_id=task_id,
                  agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return _result(task)


async def _tasks_transition(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    task_id = args["id"]
    to_status = args["to_status"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)

    task = _tasks[task_id]
    from_status = task["status"]
    if to_status not in _TRANSITIONS.get(from_status, set()):
        raise TaskInvalidTransition(from_status, to_status)

    task["status"] = to_status
    task["updated_at"] = now_iso()
    task["lock"]["version"] += 1
    task["lock"]["etag"] = _etag(task)

    meta = args.get("_meta", {})
    _record_audit("task.transitioned", task_id=task_id,
                  agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"),
                  data={"from": from_status, "to": to_status, "reason": args.get("reason")})
    return _result(task)


async def _tasks_claim(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    task_id = args["task_id"]
    agent_id = args["agent_id"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)

    task = _tasks[task_id]
    if task["owner"] not in (None, agent_id):
        raise TaskAlreadyClaimed(task_id, task["owner"])
    if task["status"] != "inbox":
        raise TaskInvalidTransition(task["status"], "in_progress")

    task["owner"] = agent_id
    task["status"] = "in_progress"
    task["updated_at"] = now_iso()
    task["lock"]["version"] += 1
    task["lock"]["etag"] = _etag(task)

    meta = args.get("_meta", {})
    _record_audit("task.claimed", task_id=task_id, agent_id=agent_id,
                  parent_audit_id=meta.get("audit_id"))
    return _result(task)


async def _tasks_add_handoff_note(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    task_id = args["task_id"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)

    task = _tasks[task_id]
    note = {
        "id": "hn_" + new_audit_id()[:12].lower(),
        "content": args["content"],
        "author": args.get("author", "unknown"),
        "created_at": now_iso(),
    }
    task.setdefault("handoff_notes", []).append(note)
    task["updated_at"] = now_iso()
    task["lock"]["version"] += 1
    task["lock"]["etag"] = _etag(task)

    meta = args.get("_meta", {})
    _record_audit("task.handoff_note_added", task_id=task_id,
                  agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"))
    return _result(task)


async def _tasks_assign(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    task_id = args["task_id"]
    if task_id not in _tasks:
        raise TaskNotFound(task_id)

    task = _tasks[task_id]
    if task["status"] == "in_progress":
        raise TaskInvalidTransition("in_progress", "reassign")

    new_owner = args["new_owner"]
    task["owner"] = new_owner["id"]
    task["updated_at"] = now_iso()
    task["lock"]["version"] += 1
    task["lock"]["etag"] = _etag(task)

    meta = args.get("_meta", {})
    _record_audit("task.assigned", task_id=task_id,
                  agent_id=meta.get("agent_id"), parent_audit_id=meta.get("audit_id"),
                  data={"new_owner": new_owner, "reason": args.get("reason")})
    return _result(task)


async def _tasks_join(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:write")
    parent_id = args["parent_id"]
    child_ids = args["child_ids"]
    policy = args.get("join_policy", "all_done")

    if parent_id not in _tasks:
        raise TaskNotFound(parent_id)

    completed = [cid for cid in child_ids
                 if _tasks.get(cid, {}).get("status") == "done"]
    pending = [cid for cid in child_ids if cid not in completed]

    ready = False
    if policy == "all_done":
        ready = len(pending) == 0
    elif policy == "any_done":
        ready = len(completed) > 0
    elif policy == "majority":
        ready = len(completed) > len(child_ids) / 2

    if ready:
        parent = _tasks[parent_id]
        parent.setdefault("join", {})["status"] = "fulfilled"
        parent["join"]["fulfilled_at"] = now_iso()

    return _result({
        "ready": ready,
        "completed_children": completed,
        "pending_children": pending,
    })


async def _audit_trace(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    root_id = args["audit_id"]
    depth = args.get("depth", 5)

    # BFS で親子関係を辿る
    by_parent: dict[str | None, list[dict]] = defaultdict(list)
    for entry in _audit_log:
        by_parent[entry.get("parent_audit_id")].append(entry)

    events: list[dict] = []

    def collect(aid: str, current_depth: int) -> None:
        if current_depth > depth:
            return
        for entry in _audit_log:
            if entry["audit_id"] == aid:
                events.append(entry)
                break
        for child in by_parent.get(aid, []):
            collect(child["audit_id"], current_depth + 1)

    collect(root_id, 0)
    return _result({"root": root_id, "events": events})


async def _audit_query(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "tasks:read")
    results = list(_audit_log)

    if event_filter := args.get("event"):
        pattern = event_filter.replace(".", r"\.").replace("*", ".*")
        import re
        results = [e for e in results if re.match(pattern, e.get("event", ""))]
    if task_id := args.get("task_id"):
        results = [e for e in results if e.get("task_id") == task_id]
    if agent_id := args.get("agent_id"):
        results = [e for e in results if e.get("agent_id") == agent_id]

    limit = args.get("limit", 100)
    return _result({"events": results[-limit:]})


async def run() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

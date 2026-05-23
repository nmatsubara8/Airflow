"""notes-mcp: ローカル Markdown ノート MCP サーバー（DDD-02 §4）。

ツール: notes.search / notes.get / notes.list_by_category / notes.related / notes.reindex

Phase 2 MVP: Qdrant 未接続のため search は fixture ベースのスタブ。
"""
from __future__ import annotations

from typing import Any

import mcp.types as types
from mcp.server import Server

from ..common.audit import log_event, new_audit_id
from ..common.auth import require_scope
from ..common.errors import NoteNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server

server = Server("notes-mcp")

# インメモリノートストア（ingester が登録、または fixture から初期化）
_notes: dict[str, dict[str, Any]] = {}


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="notes.search",
            description="ベクトル検索 + メタデータフィルタでノートを検索",
            inputSchema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string"},
                    "filters": {
                        "type": "object",
                        "properties": {
                            "category": {"type": "array", "items": {"type": "string"}},
                            "min_confidence": {"type": "number"},
                            "exclude_confidential": {"type": "boolean", "default": True},
                        },
                    },
                    "top_k": {"type": "integer", "default": 10},
                },
            },
        ),
        types.Tool(
            name="notes.get",
            description="ID 指定でノートを取得",
            inputSchema={
                "type": "object",
                "required": ["note_id"],
                "properties": {"note_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="notes.list_by_category",
            description="カテゴリでノートをフィルタ",
            inputSchema={
                "type": "object",
                "required": ["category"],
                "properties": {
                    "category": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            },
        ),
        types.Tool(
            name="notes.related",
            description="内部リンクから関連ノートを取得",
            inputSchema={
                "type": "object",
                "required": ["note_id"],
                "properties": {
                    "note_id": {"type": "string"},
                    "depth": {"type": "integer", "default": 1},
                },
            },
        ),
        types.Tool(
            name="notes.reindex",
            description="ノートを再インデックス（管理用）",
            inputSchema={
                "type": "object",
                "properties": {"folder": {"type": "string"}},
            },
        ),
    ]


async def _notes_search(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "notes:read")
    query = args["query"]
    filters = args.get("filters", {})
    top_k = min(args.get("top_k", 10), 50)
    exclude_confidential = filters.get("exclude_confidential", True)

    # Phase 2 スタブ: キーワードマッチのみ（Qdrant 未接続）
    results = []
    for note in _notes.values():
        if exclude_confidential and note.get("confidential"):
            continue
        if category := filters.get("category"):
            if note.get("section") not in category:
                continue
        body = (note.get("title", "") + " " + note.get("body", "")).lower()
        if query.lower() in body:
            results.append({
                "note_id": note["id"],
                "title": note.get("title", ""),
                "snippet": note.get("body", "")[:200],
                "score": 0.8,  # stub score
                "section_heading": None,
            })
            if len(results) >= top_k:
                break

    log_event("info", "notes.searched", "notes-mcp", new_audit_id(),
              data={"query_len": len(query), "results": len(results)})
    return mcp_result({"results": results})


async def _notes_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "notes:read")
    note_id = args["note_id"]
    if note_id not in _notes:
        raise NoteNotFound(note_id)
    return mcp_result(_notes[note_id])


async def _notes_list_by_category(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "notes:read")
    category = args["category"]
    limit = min(args.get("limit", 50), 200)
    results = [n for n in _notes.values() if n.get("section") == category][:limit]
    return mcp_result({"notes": results})


async def _notes_related(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "notes:read")
    note_id = args["note_id"]
    if note_id not in _notes:
        raise NoteNotFound(note_id)

    # Obsidian [[内部リンク]] を辿る（Phase 2: タイトル一致のみ）
    note = _notes[note_id]
    import re
    links = re.findall(r"\[\[([^\]]+)\]\]", note.get("body", ""))
    related = [
        n for n in _notes.values()
        if n["id"] != note_id and any(link.lower() in n.get("title", "").lower() for link in links)
    ]
    return mcp_result({"related": related[: args.get("depth", 1) * 5]})


async def _notes_reindex(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "notes:write")
    # Phase 2 スタブ: 実際のインデックス更新はなし
    return mcp_result({"status": "ok", "indexed": len(_notes), "note": "stub — Qdrant not connected"})


_HANDLERS = {
    "notes.search": _notes_search,
    "notes.get": _notes_get,
    "notes.list_by_category": _notes_list_by_category,
    "notes.related": _notes_related,
    "notes.reindex": _notes_reindex,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS)


async def run() -> None:
    await run_stdio_server(server)

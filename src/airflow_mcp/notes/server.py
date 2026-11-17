"""notes-mcp: ローカル Markdown ノート MCP サーバー（DDD-02 §4）。

ツール: notes.search / notes.get / notes.list_by_category / notes.related / notes.reindex

Phase 2 MVP: Qdrant 未接続のため search は fixture ベースのスタブ。
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server

from ..common.audit import log_event, new_audit_id
from ..common.auth import require_scope
from ..common.errors import NoteNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from ..ingester.notes_index import load_index

logger = logging.getLogger(__name__)

server = Server("notes-mcp")

_NOTES_INDEX_PATH = Path(os.environ.get("AIRFLOW_NOTES_INDEX", ".airflow/notes/index.json"))

# インメモリノートストア（起動時に index.json から初期化、reindex で更新）
_notes: dict[str, dict[str, Any]] = {}


def _load_notes() -> None:
    """起動時に notes/index.json からノートをロードする。"""
    index = load_index(_NOTES_INDEX_PATH)
    _notes.clear()
    for note_id, note in index.get("notes", {}).items():
        _notes[note_id] = {"id": note_id, **note}
    logger.info("notes: loaded %d notes from index", len(_notes))


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="notes_search",
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
            name="notes_get",
            description="ID 指定でノートを取得",
            inputSchema={
                "type": "object",
                "required": ["note_id"],
                "properties": {"note_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="notes_list_by_category",
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
            name="notes_related",
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
            name="notes_reindex",
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
    _load_notes()
    return mcp_result({"status": "ok", "indexed": len(_notes)})


_HANDLERS = {
    "notes_search": _notes_search,
    "notes_get": _notes_get,
    "notes_list_by_category": _notes_list_by_category,
    "notes_related": _notes_related,
    "notes_reindex": _notes_reindex,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS)


async def run() -> None:
    await run_stdio_server(server, init_fn=_load_notes)

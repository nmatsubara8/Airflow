"""MCP サーバー共通ユーティリティ。

全 MCP サーバーが共有するヘルパーを集約する：
- mcp_result()         : dict → TextContent リスト変換
- dispatch()           : ツール名ディスパッチ + AirflowError ハンドリング
- run_stdio_server()   : stdio 経由の MCP サーバー起動
"""
from __future__ import annotations

import json
from collections.abc import Callable, Coroutine
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from .auth import get_token_from_env
from .errors import AirflowError
from .rate_limiter import check_rate_limit


def mcp_result(data: Any) -> list[types.TextContent]:
    """dict/list を MCP TextContent リストに変換する。"""
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False))]


# ハンドラー型エイリアス: (args, token) → TextContent リスト
_Handler = Callable[
    [dict[str, Any], str | None],
    Coroutine[Any, Any, list[types.TextContent]],
]


async def dispatch(
    name: str,
    arguments: dict[str, Any],
    handlers: dict[str, _Handler],
    *,
    rate_limit: bool = True,
) -> list[types.TextContent]:
    """ツール名をハンドラーへディスパッチし、AirflowError を MCP エラーに変換する。

    Args:
        name:       MCP ツール名。
        arguments:  ツール引数。
        handlers:   ツール名 → ハンドラー関数のマッピング。
        rate_limit: True のとき呼び出し前にレート制限チェックを行う。
    """
    token = get_token_from_env()
    try:
        if rate_limit:
            check_rate_limit(token)
        handler = handlers.get(name)
        if handler is None:
            return mcp_result({"error": {"code": "UNKNOWN_TOOL", "message": name}})
        return await handler(arguments, token)
    except AirflowError as e:
        return e.to_mcp_result()


async def run_stdio_server(server: Server, *, init_fn: Callable[[], None] | None = None) -> None:
    """stdio 経由で MCP サーバーを起動する。

    Args:
        server:  起動する MCP Server インスタンス。
        init_fn: 接続前に一度だけ呼ぶ初期化関数（省略可）。
    """
    if init_fn is not None:
        init_fn()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

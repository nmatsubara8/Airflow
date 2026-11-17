"""共有テストヘルパー（フィクスチャ以外）。"""
from __future__ import annotations

import json
from typing import Any


def make_mcp_caller(call_tool_fn):
    """MCP サーバーモジュールの call_tool を呼ぶ非同期ヘルパーを返す。

    各サーバーテストファイルで同一コードを重複定義する代わりに使う:

        _call = make_mcp_caller(some_mod.call_tool)

        async def test_something():
            result = await _call("tool.name", {"key": "value"})
    """
    async def _call(name: str, args: dict[str, Any]) -> dict[str, Any]:
        result = await call_tool_fn(name, args)
        return json.loads(result[0].text)
    return _call

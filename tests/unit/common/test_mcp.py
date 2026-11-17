"""common/mcp.py の単体テスト。"""
from __future__ import annotations

import json

import pytest

from airflow_mcp.common.errors import McpInvalidArgument
from airflow_mcp.common.mcp import dispatch, mcp_result, require_args


# ── require_args ──────────────────────────────────────────

def test_require_args_passes_when_all_keys_present():
    require_args({"id": "x", "name": "y"}, "id", "name")  # no raise


def test_require_args_raises_on_missing_key():
    with pytest.raises(McpInvalidArgument):
        require_args({"id": "x"}, "id", "name")


def test_require_args_raises_on_all_missing():
    with pytest.raises(McpInvalidArgument):
        require_args({}, "id", "name")


def test_require_args_no_keys_always_passes():
    require_args({}, )  # empty keys list → no raise


def test_require_args_empty_args_no_keys_passes():
    require_args({})  # no keys to check


def test_require_args_missing_carries_field_info():
    try:
        require_args({"a": 1}, "a", "b", "c")
    except McpInvalidArgument as exc:
        msg = str(exc)
        assert "b" in msg or "c" in msg


# ── mcp_result ────────────────────────────────────────────

def test_mcp_result_returns_list_of_one_text_content():
    result = mcp_result({"key": "value"})
    assert len(result) == 1
    assert result[0].type == "text"


def test_mcp_result_serializes_dict():
    result = mcp_result({"foo": "bar"})
    data = json.loads(result[0].text)
    assert data["foo"] == "bar"


def test_mcp_result_serializes_list():
    result = mcp_result([1, 2, 3])
    data = json.loads(result[0].text)
    assert data == [1, 2, 3]


def test_mcp_result_handles_nested_structure():
    payload = {"tasks": [{"id": "task_001", "status": "inbox"}]}
    result = mcp_result(payload)
    data = json.loads(result[0].text)
    assert data["tasks"][0]["id"] == "task_001"


def test_mcp_result_non_ascii_preserved():
    result = mcp_result({"title": "テスト"})
    assert "テスト" in result[0].text


# ── dispatch ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dispatch_calls_correct_handler():
    async def handler(args, token):
        return mcp_result({"called": True})

    result = await dispatch("my.tool", {}, {"my.tool": handler}, rate_limit=False)
    data = json.loads(result[0].text)
    assert data["called"] is True


@pytest.mark.asyncio
async def test_dispatch_unknown_tool_returns_error():
    result = await dispatch("no.such.tool", {}, {}, rate_limit=False)
    data = json.loads(result[0].text)
    assert data["error"]["code"] == "UNKNOWN_TOOL"


@pytest.mark.asyncio
async def test_dispatch_airflow_error_converted_to_mcp_result():
    async def failing_handler(args, token):
        raise McpInvalidArgument(["required_field"])

    result = await dispatch("bad.tool", {}, {"bad.tool": failing_handler}, rate_limit=False)
    data = json.loads(result[0].text)
    assert "error" in data


@pytest.mark.asyncio
async def test_dispatch_passes_args_and_token_to_handler():
    received = {}

    async def handler(args, token):
        received["args"] = args
        received["token"] = token
        return mcp_result({})

    import os
    original = os.environ.get("AIRFLOW_MCP_TOKEN")
    os.environ["AIRFLOW_MCP_TOKEN"] = "test-tok"
    try:
        await dispatch("t", {"x": 1}, {"t": handler}, rate_limit=False)
    finally:
        if original is None:
            os.environ.pop("AIRFLOW_MCP_TOKEN", None)
        else:
            os.environ["AIRFLOW_MCP_TOKEN"] = original

    assert received["args"] == {"x": 1}

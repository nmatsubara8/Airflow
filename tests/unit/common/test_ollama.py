"""common/ollama.py の単体テスト。"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow_mcp.common.ollama import _extract_json, call_chat


# ── _extract_json ─────────────────────────────────────

def test_extract_json_returns_json_block():
    text = 'some text {"key": "val"} more'
    result = _extract_json(text)
    assert result == '{"key": "val"}'


def test_extract_json_returns_text_when_no_json():
    text = "no json here"
    result = _extract_json(text)
    assert result == text


# ── call_chat ─────────────────────────────────────────

def _mock_response(content="", thinking="", content2="", thinking2=""):
    """Helper to create mock httpx response sequences."""
    resp1 = MagicMock()
    resp1.raise_for_status = MagicMock()
    resp1.json.return_value = {"message": {"content": content, "thinking": thinking}}

    resp2 = MagicMock()
    resp2.raise_for_status = MagicMock()
    resp2.json.return_value = {"message": {"content": content2, "thinking": thinking2}}

    return [resp1, resp2]


def test_call_chat_returns_content_directly():
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"message": {"content": '{"result": "ok"}'}}
    with patch("airflow_mcp.common.ollama.httpx.post", return_value=resp):
        result = call_chat("http://localhost", {"model": "x", "messages": []}, timeout_s=5.0)
    assert result == '{"result": "ok"}'


def test_call_chat_extracts_from_thinking_when_content_empty():
    # thinking contains surrounding text + JSON — _extract_json strips the prefix
    responses = _mock_response(content="", thinking='Let me think... {"from": "thinking"} done')
    with patch("airflow_mcp.common.ollama.httpx.post", side_effect=responses):
        result = call_chat("http://localhost", {"model": "x", "messages": []}, timeout_s=5.0)
    assert '"from"' in result


def test_call_chat_retries_without_format_when_content_and_thinking_empty():
    responses = _mock_response(content="", thinking="", content2='{"retry": true}')
    with patch("airflow_mcp.common.ollama.httpx.post", side_effect=responses):
        result = call_chat("http://localhost",
                           {"model": "x", "messages": [], "format": "json"}, timeout_s=5.0)
    assert '"retry"' in result


def test_call_chat_extracts_from_thinking2_when_content2_empty():
    # thinking2 has surrounding text + JSON
    responses = _mock_response(content="", thinking="plain text no json",
                               content2="", thinking2='retry result: {"from": "thinking2"} end')
    with patch("airflow_mcp.common.ollama.httpx.post", side_effect=responses):
        result = call_chat("http://localhost",
                           {"model": "x", "messages": [], "format": "json"}, timeout_s=5.0)
    assert '"from"' in result


def test_call_chat_raises_when_all_content_empty():
    responses = _mock_response(content="", thinking="not json", content2="", thinking2="not json")
    with patch("airflow_mcp.common.ollama.httpx.post", side_effect=responses):
        with pytest.raises(ValueError, match="empty content"):
            call_chat("http://localhost",
                      {"model": "x", "messages": [], "format": "json"}, timeout_s=5.0)


def test_call_chat_thinking_no_json_falls_through_to_retry():
    """thinking にテキストはあるが JSON なし → retry without format."""
    responses = _mock_response(content="", thinking="plain text no json",
                               content2='{"ok": true}')
    with patch("airflow_mcp.common.ollama.httpx.post", side_effect=responses):
        result = call_chat("http://localhost",
                           {"model": "x", "messages": [], "format": "json"}, timeout_s=5.0)
    assert '"ok"' in result

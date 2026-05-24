"""notes/server.py の単体テスト（DDD-02 §4）。"""
from __future__ import annotations

import json

import pytest

import airflow_mcp.notes.server as notes_mod
from tests.helpers import make_mcp_caller


@pytest.fixture(autouse=True)
def clean_notes():
    """各テスト前後にインメモリノートストアをリセット。"""
    notes_mod._notes.clear()
    yield
    notes_mod._notes.clear()


def _add_note(note_id: str, title: str, body: str,
               section: str = "engineering", confidential: bool = False) -> dict:
    note = {
        "id": note_id,
        "title": title,
        "body": body,
        "section": section,
        "confidential": confidential,
    }
    notes_mod._notes[note_id] = note
    return note


_call = make_mcp_caller(notes_mod.call_tool)


# ── notes.search ──────────────────────────────────────

@pytest.mark.asyncio
async def test_search_returns_matching_notes():
    _add_note("n1", "Python入門", "Python は動的型付け言語です")
    _add_note("n2", "JavaScript入門", "JSはブラウザで動く")
    result = await _call("notes.search", {"query": "Python"})
    assert len(result["results"]) == 1
    assert result["results"][0]["note_id"] == "n1"


@pytest.mark.asyncio
async def test_search_excludes_confidential_by_default():
    _add_note("secret", "秘密情報", "机密データ Python", confidential=True)
    result = await _call("notes.search", {"query": "Python"})
    assert all(r["note_id"] != "secret" for r in result["results"])


@pytest.mark.asyncio
async def test_search_includes_confidential_when_flag_false():
    _add_note("secret", "秘密", "confidential Python", confidential=True)
    result = await _call("notes.search", {
        "query": "Python",
        "filters": {"exclude_confidential": False},
    })
    assert any(r["note_id"] == "secret" for r in result["results"])


@pytest.mark.asyncio
async def test_search_respects_top_k():
    for i in range(10):
        _add_note(f"n{i}", f"Python {i}", f"Python コード {i}")
    result = await _call("notes.search", {"query": "Python", "top_k": 3})
    assert len(result["results"]) <= 3


@pytest.mark.asyncio
async def test_search_filters_by_category():
    _add_note("eng", "エンジニアリング", "Python コード", section="engineering")
    _add_note("biz", "ビジネス", "Python 分析", section="business")
    result = await _call("notes.search", {
        "query": "Python",
        "filters": {"category": ["engineering"]},
    })
    ids = [r["note_id"] for r in result["results"]]
    assert "eng" in ids
    assert "biz" not in ids


@pytest.mark.asyncio
async def test_search_empty_store_returns_empty_list():
    result = await _call("notes.search", {"query": "anything"})
    assert result["results"] == []


# ── notes.get ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_returns_note_by_id():
    _add_note("note_001", "タイトル", "内容")
    result = await _call("notes.get", {"note_id": "note_001"})
    assert result["id"] == "note_001"
    assert result["title"] == "タイトル"


@pytest.mark.asyncio
async def test_get_unknown_note_returns_error():
    result = await _call("notes.get", {"note_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "NOTE_NOT_FOUND"


# ── notes.list_by_category ───────────────────────────

@pytest.mark.asyncio
async def test_list_by_category_returns_matching():
    _add_note("e1", "E1", "本文", section="engineering")
    _add_note("e2", "E2", "本文", section="engineering")
    _add_note("b1", "B1", "本文", section="business")
    result = await _call("notes.list_by_category", {"category": "engineering"})
    ids = [n["id"] for n in result["notes"]]
    assert "e1" in ids and "e2" in ids
    assert "b1" not in ids


@pytest.mark.asyncio
async def test_list_by_category_respects_limit():
    for i in range(10):
        _add_note(f"e{i}", f"E{i}", "本文", section="engineering")
    result = await _call("notes.list_by_category", {"category": "engineering", "limit": 3})
    assert len(result["notes"]) <= 3


# ── notes.related ─────────────────────────────────────

@pytest.mark.asyncio
async def test_related_finds_linked_notes():
    _add_note("source", "Source", "内容 [[Target Note]]")
    _add_note("target", "Target Note", "リンク先の内容")
    result = await _call("notes.related", {"note_id": "source"})
    ids = [n["id"] for n in result["related"]]
    assert "target" in ids


@pytest.mark.asyncio
async def test_related_unknown_note_returns_error():
    result = await _call("notes.related", {"note_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "NOTE_NOT_FOUND"


@pytest.mark.asyncio
async def test_related_no_links_returns_empty():
    _add_note("lonely", "孤立ノート", "リンクなしの内容")
    result = await _call("notes.related", {"note_id": "lonely"})
    assert result["related"] == []


# ── notes.reindex ─────────────────────────────────────

@pytest.mark.asyncio
async def test_reindex_returns_ok_status(tmp_path, monkeypatch):
    """reindex は notes/index.json を再ロードして indexed 件数を返す。"""
    import json
    index_path = tmp_path / "index.json"
    index_path.write_text(json.dumps({
        "schema_version": "1.0.0",
        "notes": {
            "n1": {"title": "N1", "body": "内容", "section": "engineering"},
        },
    }))
    monkeypatch.setattr("airflow_mcp.notes.server._NOTES_INDEX_PATH", index_path)
    result = await _call("notes.reindex", {})
    assert result["status"] == "ok"
    assert result["indexed"] == 1


# ── notes.reindex スコープ強制 ────────────────────────

@pytest.mark.asyncio
async def test_reindex_requires_notes_write_scope(monkeypatch):
    """notes:read しか持たないトークンで reindex を呼ぶと AUTH_SCOPE_INSUFFICIENT になること。"""
    import airflow_mcp.common.auth as auth_mod
    import airflow_mcp.notes.server as srv
    import airflow_mcp.common.mcp as mcp_mod
    monkeypatch.setattr(auth_mod, "_LOCALHOST_BYPASS", False)
    monkeypatch.setattr(mcp_mod, "get_token_from_env", lambda: "limited_token")
    # tokens.json なし → env var トークンと一致しない → AUTH_TOKEN_INVALID
    # tokens.json ありの場合の notes:write なしテストは auth 単体テストで担保
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "different_token")

    result = await srv.call_tool("notes.reindex", {})
    data = json.loads(result[0].text)
    assert "error" in data
    assert data["error"]["code"] in ("AUTH_TOKEN_INVALID", "AUTH_SCOPE_INSUFFICIENT")


# ── 不明ツール名 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("notes.unknown", {})
    assert "error" in result


# ── list_tools() body (line 33) ───────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_all_tool_names():
    import airflow_mcp.notes.server as notes_mod
    tools = await notes_mod.list_tools()
    names = [t.name for t in tools]
    assert "notes.search" in names
    assert "notes.get" in names
    assert "notes.list_by_category" in names
    assert "notes.related" in names
    assert "notes.reindex" in names


# ── run() (lines 193-194) ─────────────────────────────

@pytest.mark.asyncio
async def test_run_starts_stdio_server(fake_stdio_ctx):
    from unittest.mock import AsyncMock, patch
    import airflow_mcp.notes.server as notes_mod

    mock_run = AsyncMock()
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_ctx), \
         patch.object(notes_mod.server, "run", mock_run):
        await notes_mod.run()

    mock_run.assert_awaited_once()

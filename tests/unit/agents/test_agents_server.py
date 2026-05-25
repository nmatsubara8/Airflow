"""agents/server.py の単体テスト（DDD-02 §7）。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import airflow_mcp.agents.server as agents_mod
from tests.helpers import make_mcp_caller


@pytest.fixture(autouse=True)
def reset_agents():
    """各テスト前後に _agents をリセット。"""
    agents_mod._agents.clear()
    yield
    agents_mod._agents.clear()


_call = make_mcp_caller(agents_mod.call_tool)


# ── _load_agents ──────────────────────────────────────

def test_load_agents_reads_skills_md(tmp_path, monkeypatch):
    """skills.md を読み込んで _agents に登録されること。"""
    skills = tmp_path / "agent_docs" / "skills.md"
    skills.parent.mkdir()
    skills.write_text(
        "---\nagent_id: agent_docs\nsection: engineering\n---\n# Docs Agent\n"
    )
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path)
    agents_mod._load_agents()
    assert "agent_docs" in agents_mod._agents
    assert agents_mod._agents["agent_docs"]["section"] == "engineering"


def test_load_agents_skips_nonexistent_dir(monkeypatch):
    """_AGENTS_DIR が存在しない場合は何も登録しないこと。"""
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", Path("/nonexistent/agents"))
    agents_mod._load_agents()
    assert agents_mod._agents == {}


def test_load_agents_skips_no_frontmatter(tmp_path, monkeypatch):
    """フロントマターなしの skills.md は無視されること。"""
    skills = tmp_path / "agent_x" / "skills.md"
    skills.parent.mkdir()
    skills.write_text("# No frontmatter\n")
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path)
    agents_mod._load_agents()
    assert agents_mod._agents == {}


def test_load_agents_skips_missing_agent_id(tmp_path, monkeypatch):
    """agent_id がないフロントマターは無視されること。"""
    skills = tmp_path / "anon" / "skills.md"
    skills.parent.mkdir()
    skills.write_text("---\nsection: engineering\n---\nbody\n")
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path)
    agents_mod._load_agents()
    assert agents_mod._agents == {}


def test_load_agents_suppresses_read_error(tmp_path, monkeypatch):
    """read_text が例外を raise しても crash しないこと。"""
    from unittest.mock import patch
    skills = tmp_path / "bad" / "skills.md"
    skills.parent.mkdir()
    skills.write_text("---\nagent_id: x\n---\nbody\n")
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path)
    with patch.object(Path, "read_text", side_effect=OSError("unreadable")):
        agents_mod._load_agents()
    assert agents_mod._agents == {}


# ── list_tools ────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_all_agent_tools():
    tools = await agents_mod.list_tools()
    names = [t.name for t in tools]
    assert "agents.list" in names
    assert "agents.get" in names
    assert "agents.find_for_task" in names
    assert "agents.extract_features" in names
    assert "agents.register" in names


# ── agents.list ───────────────────────────────────────

@pytest.mark.asyncio
async def test_agents_list_returns_all_agents():
    agents_mod._agents["agent_docs"] = {"agent_id": "agent_docs", "section": "engineering"}
    result = await _call("agents.list", {})
    assert result["total"] == 1
    assert result["agents"][0]["agent_id"] == "agent_docs"


@pytest.mark.asyncio
async def test_agents_list_filters_by_section():
    agents_mod._agents["a1"] = {"agent_id": "a1", "section": "engineering"}
    agents_mod._agents["a2"] = {"agent_id": "a2", "section": "content"}
    result = await _call("agents.list", {"section": "engineering"})
    assert result["total"] == 1


@pytest.mark.asyncio
async def test_agents_list_filters_by_platform():
    agents_mod._agents["a1"] = {"agent_id": "a1", "platform": "linux"}
    agents_mod._agents["a2"] = {"agent_id": "a2", "platform": "mac"}
    result = await _call("agents.list", {"platform": "linux"})
    assert result["total"] == 1


@pytest.mark.asyncio
async def test_agents_list_body_excluded_from_summary():
    agents_mod._agents["a1"] = {"agent_id": "a1", "body": "long content here"}
    result = await _call("agents.list", {})
    assert "body" not in result["agents"][0]


# ── agents.get ────────────────────────────────────────

@pytest.mark.asyncio
async def test_agents_get_returns_agent():
    agents_mod._agents["agent_docs"] = {"agent_id": "agent_docs", "section": "engineering"}
    result = await _call("agents.get", {"agent_id": "agent_docs"})
    assert result["agent_id"] == "agent_docs"


@pytest.mark.asyncio
async def test_agents_get_returns_error_for_unknown():
    result = await _call("agents.get", {"agent_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "AGENT_NOT_FOUND"


# ── agents.find_for_task ──────────────────────────────

@pytest.mark.asyncio
async def test_agents_find_for_task_matches_section():
    agents_mod._agents["a1"] = {
        "agent_id": "a1", "section": "engineering", "responsibilities": ["api", "backend"]
    }
    task = {"section": "engineering", "tags": ["api"]}
    result = await _call("agents.find_for_task", {"task": task})
    assert len(result["candidates"]) == 1
    assert result["candidates"][0]["agent_id"] == "a1"


@pytest.mark.asyncio
async def test_agents_find_for_task_returns_empty_when_no_match():
    agents_mod._agents["a1"] = {"agent_id": "a1", "section": "content", "responsibilities": []}
    task = {"section": "engineering", "tags": []}
    result = await _call("agents.find_for_task", {"task": task})
    assert result["candidates"] == []


# ── agents.extract_features ───────────────────────────

@pytest.mark.asyncio
async def test_agents_extract_features_infers_task_type_review():
    task = {"tags": ["code-review"], "title": "review PR"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["task_type"] == "review"


@pytest.mark.asyncio
async def test_agents_extract_features_infers_task_type_summarize():
    task = {"tags": ["summarize"], "title": "summary"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["task_type"] == "summarize"


@pytest.mark.asyncio
async def test_agents_extract_features_infers_task_type_translate():
    task = {"tags": ["translate"], "title": "translate doc"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["task_type"] == "translate"


@pytest.mark.asyncio
async def test_agents_extract_features_defaults_to_document():
    task = {"tags": [], "title": "unknown task"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["task_type"] == "document"


@pytest.mark.asyncio
async def test_agents_extract_features_detects_japanese():
    task = {"tags": [], "title": "日本語タイトル"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["language"] == "ja"


@pytest.mark.asyncio
async def test_agents_extract_features_preserves_existing():
    task = {"features": {"task_type": "plan", "required_accuracy": "high"}, "tags": [], "title": "t"}
    result = await _call("agents.extract_features", {"task": task})
    assert result["features"]["task_type"] == "plan"
    assert result["features"]["required_accuracy"] == "high"


# ── agents.register ───────────────────────────────────

@pytest.mark.asyncio
async def test_agents_register_adds_agent():
    agent = {"agent_id": "agent_new", "section": "engineering"}
    result = await _call("agents.register", {"agent": agent})
    assert result["registered"] == "agent_new"
    assert "agent_new" in agents_mod._agents


@pytest.mark.asyncio
async def test_agents_register_uses_id_field_as_fallback():
    agent = {"id": "agent_alt", "section": "content"}
    result = await _call("agents.register", {"agent": agent})
    assert result["registered"] == "agent_alt"


# ── unknown tool ──────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("agents.unknown", {})
    assert "error" in result
    assert result["error"]["code"] == "UNKNOWN_TOOL"


# ── AirflowError catch (lines 121-122) ───────────────

@pytest.mark.asyncio
async def test_call_tool_returns_mcp_result_on_airflow_error(monkeypatch):
    """require_scope が AirflowError を raise したとき to_mcp_result() が返ること（lines 121-122）。"""
    from airflow_mcp.common.errors import AuthScopeInsufficient
    from unittest.mock import patch
    # require_scope を強制的に AirflowError を raise させる
    with patch("airflow_mcp.agents.server.require_scope",
               side_effect=AuthScopeInsufficient("agents:read")):
        result = await agents_mod.call_tool("agents.list", {})
    data = json.loads(result[0].text)
    assert "error" in data


# ── run() ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_starts_stdio_server(monkeypatch, fake_stdio_ctx):
    from unittest.mock import AsyncMock, patch

    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", Path("/nonexistent"))
    mock_run = AsyncMock()
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_ctx), \
         patch.object(agents_mod.server, "run", mock_run):
        await agents_mod.run()

    mock_run.assert_awaited_once()


# ── T2-C: agents frontmatter YAML ガードテスト ────────

def test_load_agents_skips_non_dict_frontmatter(tmp_path, monkeypatch):
    """frontmatter YAML が非dict（list など）の場合にスキップすること（T1-D）。"""
    skills_dir = tmp_path / "agents" / "test_agent"
    skills_dir.mkdir(parents=True)
    skills_file = skills_dir / "skills.md"
    # frontmatter に list を書く（yaml.safe_load → list ではなく dict にならない）
    skills_file.write_text(
        "---\n- item1\n- item2\n---\n\n# Body\n"
    )
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path / "agents")
    agents_mod._agents.clear()
    agents_mod._load_agents()
    # list frontmatter のエージェントは登録されないこと
    assert len(agents_mod._agents) == 0


def test_load_agents_registers_valid_frontmatter(tmp_path, monkeypatch):
    """有効な dict frontmatter があるとき agent_id でエージェントが登録されること。"""
    skills_dir = tmp_path / "agents" / "test_agent"
    skills_dir.mkdir(parents=True)
    skills_file = skills_dir / "skills.md"
    skills_file.write_text(
        "---\nagent_id: test_agent_001\nname: Test Agent\n---\n\n# Skills\n"
    )
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path / "agents")
    agents_mod._agents.clear()
    agents_mod._load_agents()
    assert "test_agent_001" in agents_mod._agents


# ── agents.reload ─────────────────────────────────────

@pytest.mark.asyncio
async def test_agents_reload_clears_and_reloads(tmp_path, monkeypatch):
    """agents.reload がキャッシュをクリアして再ロードすること（lines 189-192）。"""
    skills_dir = tmp_path / "agents" / "reload_agent"
    skills_dir.mkdir(parents=True)
    (skills_dir / "skills.md").write_text(
        "---\nagent_id: reload_agent_001\nname: Reload Agent\n---\n\n# Skills\n"
    )
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", tmp_path / "agents")
    agents_mod._agents["stale_agent"] = {"agent_id": "stale_agent"}
    result = await _call("agents.reload", {})
    assert result["reloaded"] == 1
    assert "stale_agent" not in agents_mod._agents
    assert "reload_agent_001" in agents_mod._agents

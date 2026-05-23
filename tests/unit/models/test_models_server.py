"""models/server.py の単体テスト（DDD-02 §5）。"""
from __future__ import annotations

import json

import pytest

import airflow_mcp.models.server as models_mod


@pytest.fixture(autouse=True)
def clean_models():
    """各テスト前後にインメモリモデルストアをリセット。"""
    models_mod._models.clear()
    models_mod._execution_logs.clear()
    yield
    models_mod._models.clear()
    models_mod._execution_logs.clear()


def _add_model(model_id: str, **extra) -> dict:
    model = {
        "id": model_id,
        "name": model_id,
        "ollama_tag": f"{model_id}:latest",
        "vram_gb": 9.0,
        "preferred_tasks": [],
        **extra,
    }
    models_mod._models[model_id] = model
    return model


async def _call(name: str, args: dict) -> dict:
    result = await models_mod.call_tool(name, args)
    return json.loads(result[0].text)


# ── model.list ────────────────────────────────────────

@pytest.mark.asyncio
async def test_model_list_returns_empty_when_no_models():
    result = await _call("model.list", {})
    assert result["models"] == []


@pytest.mark.asyncio
async def test_model_list_returns_registered_models():
    _add_model("model_a")
    _add_model("model_b")
    result = await _call("model.list", {})
    ids = [m["id"] for m in result["models"]]
    assert "model_a" in ids
    assert "model_b" in ids


# ── model.get ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_model_get_returns_correct_model():
    _add_model("model_qwen25_14b_q4", vram_gb=8.5)
    result = await _call("model.get", {"model_id": "model_qwen25_14b_q4"})
    assert result["id"] == "model_qwen25_14b_q4"
    assert result["vram_gb"] == 8.5


@pytest.mark.asyncio
async def test_model_get_unknown_returns_not_found_error():
    result = await _call("model.get", {"model_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "MODEL_NOT_FOUND"


# ── model.recommend ───────────────────────────────────

@pytest.mark.asyncio
async def test_model_recommend_returns_recommendations():
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
    })
    assert "recommendations" in result
    assert len(result["recommendations"]) >= 1


@pytest.mark.asyncio
async def test_model_recommend_high_accuracy_prefers_32b():
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
    })
    # ε-greedy があるためランダム性あり。recommendationsが存在することを確認
    assert len(result["recommendations"]) >= 1
    assert result["recommendations"][0]["model_id"] in models_mod._models


@pytest.mark.asyncio
async def test_model_recommend_manual_mode_returns_first_candidate(monkeypatch):
    _add_model("model_a")
    _add_model("model_b")
    # ε-greedy が上書きしないよう random.random を 1.0 固定
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 1.0)
    result = await _call("model.recommend", {
        "features": {},
        "mode": "manual",
        "candidate_models": ["model_a", "model_b"],
    })
    assert result["recommendations"][0]["model_id"] == "model_a"


# ── model.execute ─────────────────────────────────────

@pytest.mark.asyncio
async def test_model_execute_unknown_model_returns_error():
    result = await _call("model.execute", {
        "model_id": "nonexistent",
        "messages": [{"role": "user", "content": "hello"}],
    })
    assert "error" in result
    assert result["error"]["code"] == "MODEL_NOT_FOUND"


@pytest.mark.asyncio
async def test_model_execute_stub_returns_content(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    result = await _call("model.execute", {
        "model_id": "model_qwen25_14b_q4",
        "messages": [{"role": "user", "content": "テスト"}],
    })
    assert "content" in result
    assert "execution_log_id" in result


# ── model.stats ───────────────────────────────────────

@pytest.mark.asyncio
async def test_model_stats_returns_zero_when_no_executions():
    result = await _call("model.stats", {})
    assert result["total_executions"] == 0
    assert result["success_rate"] == 0


@pytest.mark.asyncio
async def test_model_stats_counts_executions(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    await _call("model.execute", {
        "model_id": "model_qwen25_14b_q4",
        "messages": [{"role": "user", "content": "テスト"}],
    })
    result = await _call("model.stats", {"model_id": "model_qwen25_14b_q4"})
    assert result["total_executions"] == 1
    assert result["success_rate"] == 1.0


# ── model.register ────────────────────────────────────

@pytest.mark.asyncio
async def test_model_register_adds_to_registry():
    new_model = {"id": "model_new", "name": "New Model", "vram_gb": 5.0}
    result = await _call("model.register", {"model": new_model})
    assert result["registered"] == "model_new"
    assert "model_new" in models_mod._models


@pytest.mark.asyncio
async def test_model_register_requires_models_write_scope(monkeypatch):
    """models:execute しか持たないトークンで register を呼ぶと AUTH エラーになること。"""
    import airflow_mcp.common.auth as auth_mod
    import airflow_mcp.models.server as srv
    monkeypatch.setattr(auth_mod, "_LOCALHOST_BYPASS", False)
    import airflow_mcp.common.mcp as mcp_mod
    monkeypatch.setattr(mcp_mod, "get_token_from_env", lambda: "execute_only_token")
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "different_token")

    result = await srv.call_tool("model.register", {"model": {"id": "m_evil", "name": "evil"}})
    data = json.loads(result[0].text)
    assert "error" in data
    assert data["error"]["code"] in ("AUTH_TOKEN_INVALID", "AUTH_SCOPE_INSUFFICIENT")
    assert "m_evil" not in models_mod._models


# ── model.feedback ────────────────────────────────────

@pytest.mark.asyncio
async def test_model_feedback_returns_task_id(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_LOG_DIR", str(tmp_path / "logs"))
    # update_feedback は log_dir なしでデフォルトを使うため、
    # タスクが存在しなくても updated_record=None で正常終了する
    result = await _call("model.feedback", {
        "task_id": "task_nonexistent",
        "human_feedback": "accepted",
    })
    assert result["task_id"] == "task_nonexistent"
    assert result["human_feedback"] == "accepted"
    assert result["updated_record"] is None  # タスクなし → None


# ── 不明ツール名 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("model.unknown", {})
    assert "error" in result


# ── _load_registry (lines 35-38) ─────────────────────

def test_load_registry_reads_file(tmp_path, monkeypatch):
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"models": [
        {"id": "model_from_file", "ollama_tag": "file:latest", "vram_gb": 8.0}
    ]}))
    monkeypatch.setattr(models_mod, "_REGISTRY_PATH", reg)
    models_mod._models.clear()
    models_mod._load_registry()
    assert "model_from_file" in models_mod._models


# ── list_tools (line 47) ──────────────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_all_seven_tools():
    tools = await models_mod.list_tools()
    names = [t.name for t in tools]
    assert set(names) == {
        "model.list", "model.get", "model.recommend",
        "model.execute", "model.feedback", "model.stats", "model.register",
    }


# ── _model_recommend default else branch (lines 187-188) ─

@pytest.mark.asyncio
async def test_model_recommend_medium_accuracy_returns_14b(monkeypatch):
    _add_model("model_qwen25_14b_q4")
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 1.0)
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "medium", "latency_sensitivity": "batch"},
    })
    assert result["recommendations"][0]["model_id"] == "model_qwen25_14b_q4"
    assert "14B" in result["recommendations"][0]["reason"]


@pytest.mark.asyncio
async def test_model_recommend_interactive_latency_returns_14b(monkeypatch):
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 1.0)
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "high", "latency_sensitivity": "interactive"},
    })
    # high accuracy but interactive → else branch (14b)
    assert result["recommendations"][0]["model_id"] == "model_qwen25_14b_q4"


# ── ε-greedy branch (lines 196-197) ──────────────────

@pytest.mark.asyncio
async def test_model_recommend_epsilon_greedy_explores(monkeypatch):
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 0.0)
    monkeypatch.setattr(
        "airflow_mcp.models.server.random.choice",
        lambda lst: lst[0],
    )
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
    })
    assert "ε-greedy" in result["recommendations"][0]["reason"]


# ── _call_ollama non-stub paths (lines 275-317) ───────

@pytest.mark.asyncio
async def test_model_execute_httpx_basic_call(monkeypatch):
    """非スタブ: httpx.post が正常応答を返す基本ケース（lines 275-310）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "some output"}}
    mock_resp.raise_for_status = lambda: None
    with patch("httpx.post", return_value=mock_resp):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hello"}],
        })
    assert result["content"] == "some output"


@pytest.mark.asyncio
async def test_model_execute_with_temperature_and_max_tokens(monkeypatch):
    """options に temperature と num_predict が設定されること（lines 286-289）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    captured = {}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "ok"}}
    mock_resp.raise_for_status = lambda: None

    def fake_post(url, json, timeout):
        captured["payload"] = json
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0.7,
            "max_tokens": 128,
        })
    assert captured["payload"]["options"]["temperature"] == 0.7
    assert captured["payload"]["options"]["num_predict"] == 128


@pytest.mark.asyncio
async def test_model_execute_structured_output_valid_schema(monkeypatch):
    """structured_output.schema が valid JSON のとき parsed が返ること（lines 290-310）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": json.dumps({"summary": "ok"})}}
    mock_resp.raise_for_status = lambda: None
    with patch("httpx.post", return_value=mock_resp):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
        })
    assert result["parsed"]["summary"] == "ok"


@pytest.mark.asyncio
async def test_model_execute_structured_output_schema_validation_fails(monkeypatch):
    """schema 不適合 JSON → ModelStructuredOutputInvalid → AirflowError 応答（lines 303-314）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": json.dumps({"wrong": "field"})}}
    mock_resp.raise_for_status = lambda: None
    with patch("httpx.post", return_value=mock_resp):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
        })
    assert "error" in result
    assert result["error"]["code"] == "MODEL_STRUCTURED_OUTPUT_INVALID"


@pytest.mark.asyncio
async def test_model_execute_structured_output_json_decode_error(monkeypatch):
    """content が JSON でないとき → ModelStructuredOutputInvalid（lines 307-308）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "not json {"}}
    mock_resp.raise_for_status = lambda: None
    with patch("httpx.post", return_value=mock_resp):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
        })
    assert "error" in result
    assert result["error"]["code"] == "MODEL_STRUCTURED_OUTPUT_INVALID"


@pytest.mark.asyncio
async def test_model_execute_ollama_connection_error_falls_back(monkeypatch):
    """httpx 接続失敗 → スタブフォールバック応答（lines 312-317）。"""
    import httpx as _httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    with patch("httpx.post", side_effect=_httpx.ConnectError("refused")):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert "content" in result
    assert "Ollama 接続失敗" in result["content"]


# ── run() entry point (lines 365-367) ─────────────────

@pytest.mark.asyncio
async def test_run_calls_load_registry_and_server(tmp_path, monkeypatch):
    """run() が _load_registry を呼んだうえで stdio_server を起動すること。"""
    from unittest.mock import AsyncMock, MagicMock, patch
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_stdio_server():
        yield MagicMock(), MagicMock()

    mock_server_run = AsyncMock()
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_server), \
         patch.object(models_mod.server, "run", mock_server_run):
        await models_mod.run()

    mock_server_run.assert_awaited_once()

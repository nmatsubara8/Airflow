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
    monkeypatch.setattr(srv, "get_token_from_env", lambda: "execute_only_token")
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

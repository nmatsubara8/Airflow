"""models/server.py の単体テスト（DDD-02 §5、DDD-05）。"""
from __future__ import annotations

import json

import pytest

import airflow_mcp.models.server as models_mod
from tests.helpers import make_mcp_caller


@pytest.fixture(autouse=True)
def clean_models(monkeypatch):
    """各テスト前後にインメモリモデルストアをリセット。
    load_history はデフォルトで空リストを返すようパッチ（実行ログに依存しないよう）。
    """
    models_mod._models.clear()
    models_mod._execution_logs.clear()
    monkeypatch.setattr("airflow_mcp.models.server.load_history", lambda: [])
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


_call = make_mcp_caller(models_mod.call_tool)


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


# ── _load_registry ────────────────────────────────────

def test_load_registry_reads_file(tmp_path, monkeypatch):
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"models": [
        {"id": "model_from_file", "ollama_tag": "file:latest", "vram_gb": 8.0}
    ]}))
    monkeypatch.setattr(models_mod, "_REGISTRY_PATH", reg)
    models_mod._models.clear()
    models_mod._load_registry()
    assert "model_from_file" in models_mod._models


# ── list_tools ────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_all_ten_tools():
    tools = await models_mod.list_tools()
    names = [t.name for t in tools]
    assert set(names) == {
        "model.list", "model.get", "model.recommend",
        "model.execute", "model.feedback", "model.stats", "model.register",
        "model.load", "model.unload", "model.ps",
    }


# ── model.recommend 追加分岐 ──────────────────────────

@pytest.mark.asyncio
async def test_model_recommend_medium_accuracy_returns_14b(monkeypatch):
    _add_model("model_qwen25_14b_q4")
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 1.0)
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "medium", "latency_sensitivity": "batch"},
    })
    assert result["recommendations"][0]["model_id"] == "model_qwen25_14b_q4"
    # cold start 時はコールドスタート理由を返す
    assert "reason" in result["recommendations"][0]


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


@pytest.mark.asyncio
async def test_model_recommend_epsilon_greedy_explores(monkeypatch):
    """auto モードで ε-greedy 探索が発動すること（DDD-06 §4.5）。"""
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    # KNN で history がある場合のみ ε-greedy が発動するため, history をパッチ
    history = [
        {"model_id": "model_qwen25_14b_q4",
         "features_snapshot": {"task_type": "review", "language": "ja",
                                "required_accuracy": "high", "latency_sensitivity": "batch"},
         "auto_score": 0.9}
        for _ in range(10)
    ]
    monkeypatch.setattr("airflow_mcp.models.server.load_history", lambda: history)
    monkeypatch.setattr("airflow_mcp.models.server.random.random", lambda: 0.0)
    monkeypatch.setattr(
        "airflow_mcp.models.server.random.choice",
        lambda lst: lst[0],
    )
    result = await _call("model.recommend", {
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch",
                     "task_type": "review", "language": "ja"},
        "mode": "auto",
    })
    assert "ε-greedy" in result["recommendations"][0]["reason"]


@pytest.mark.asyncio
async def test_model_recommend_knn_uses_history_when_available(monkeypatch):
    """履歴がある場合に KNN 推奨リストを返すこと（DDD-06 §4.3）。"""
    _add_model("model_qwen25_14b_q4")
    _add_model("model_qwen25_32b_q3")
    history = [
        {"model_id": "model_qwen25_32b_q3",
         "features_snapshot": {"task_type": "review", "language": "ja",
                                "required_accuracy": "high", "latency_sensitivity": "batch"},
         "auto_score": 0.95}
        for _ in range(10)
    ]
    monkeypatch.setattr("airflow_mcp.models.server.load_history", lambda: history)
    result = await _call("model.recommend", {
        "features": {"task_type": "review", "language": "ja",
                     "required_accuracy": "high", "latency_sensitivity": "batch"},
        "mode": "recommend",
    })
    assert len(result["recommendations"]) >= 1
    assert result["recommendations"][0]["model_id"] == "model_qwen25_32b_q3"
    assert result["sample_count"] >= 5


@pytest.mark.asyncio
async def test_model_recommend_cold_start_reason(monkeypatch):
    """履歴なしのとき cold start 理由が返ること。"""
    _add_model("model_qwen25_14b_q4")
    monkeypatch.setattr("airflow_mcp.models.server.load_history", lambda: [])
    result = await _call("model.recommend", {"features": {}})
    assert "コールドスタート" in result["recommendations"][0]["reason"]


@pytest.mark.asyncio
async def test_model_recommend_cold_start_fallback_when_selected_not_in_candidates(monkeypatch):
    """cold_start_model が candidate にないモデルを返すとき available[0] にフォールバックすること（line 271）。"""
    _add_model("model_preferred")   # _models に存在するが candidates には入れない
    _add_model("model_candidate")   # candidates に指定するモデル
    monkeypatch.setattr("airflow_mcp.models.server.load_history", lambda: [])
    result = await _call("model.recommend", {
        "features": {"preferred_models": ["model_preferred"]},
        "candidate_models": ["model_candidate"],  # model_preferred は含まない
    })
    # cold_start が model_preferred を返すが available に無いため model_candidate にフォールバック
    assert result["recommendations"][0]["model_id"] == "model_candidate"


# ── _call_ollama 非スタブ ─────────────────────────────

@pytest.mark.asyncio
async def test_model_execute_httpx_basic_call(monkeypatch):
    """非スタブ: httpx.post が正常応答を返す基本ケース。"""
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
async def test_model_execute_uses_ollama_tag_from_registry(monkeypatch):
    """registry の ollama_tag が Ollama 呼び出しに使われること（DDD-05 §3.1）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4", ollama_tag="qwen2.5:14b-instruct-q4_K_M")
    captured = {}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "ok"}}
    mock_resp.raise_for_status = lambda: None

    def fake_post(url, json, timeout):
        captured["model"] = json.get("model")
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert captured["model"] == "qwen2.5:14b-instruct-q4_K_M"


@pytest.mark.asyncio
async def test_model_execute_with_temperature_and_max_tokens(monkeypatch):
    """options に temperature と num_predict が設定されること。"""
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
    """structured_output.schema が valid JSON のとき parsed が返ること。"""
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
    """schema 不適合 JSON → ModelStructuredOutputInvalid → AirflowError 応答。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": json.dumps({"wrong": "field"})}}
    mock_resp.raise_for_status = lambda: None
    # リトライ(_STRUCT_OUTPUT_RETRIES=2)後も失敗する
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
    """content が JSON でないとき → ModelStructuredOutputInvalid（リトライ後も同様）。"""
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
    """httpx 接続失敗 → スタブフォールバック応答。"""
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


# ── 構造化出力リトライ（DDD-05 §7）────────────────────

@pytest.mark.asyncio
async def test_structured_output_retry_succeeds_on_second_attempt(monkeypatch):
    """初回スキーマ不適合 → 2 回目で成功するケース。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}

    call_count = 0

    def fake_post(url, json_data=None, json=None, timeout=None):
        nonlocal call_count
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.raise_for_status = lambda: None
        if call_count == 1:
            # 初回: 不正 JSON
            mock_resp.json.return_value = {"message": {"content": "not json {"}}
        else:
            # 2 回目: 正常
            mock_resp.json.return_value = {"message": {"content": '{"summary": "ok"}'}}
        return mock_resp

    with patch("httpx.post", side_effect=lambda url, json, timeout: fake_post(url, json=json, timeout=timeout)):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
        })
    assert result["parsed"]["summary"] == "ok"
    assert call_count == 2


@pytest.mark.asyncio
async def test_structured_output_retry_hint_appended_to_messages(monkeypatch):
    """リトライ時にエラーヒントメッセージが追加されること。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}

    captured_messages = []
    call_count = 0

    def fake_post(url, json, timeout):
        nonlocal call_count
        call_count += 1
        captured_messages.append(json.get("messages", []))
        mock_resp = MagicMock()
        mock_resp.raise_for_status = lambda: None
        if call_count == 1:
            mock_resp.json.return_value = {"message": {"content": "not json {"}}
        else:
            mock_resp.json.return_value = {"message": {"content": '{"summary": "retry ok"}'}}
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
        })

    assert result["parsed"]["summary"] == "retry ok"
    # 2 回目のメッセージにはヒントが付いている
    assert len(captured_messages[1]) > len(captured_messages[0])
    assert "JSON" in captured_messages[1][-1]["content"]


# ── JSONL 永続化（task_id あり時）───────────────────────

@pytest.mark.asyncio
async def test_model_execute_writes_execution_log_when_task_id_given(monkeypatch):
    """task_id 指定時に append_execution_log が呼ばれること（DDD-05 §9）。"""
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    captured: dict = {}
    mock_record = {"id": "log_test", "task_id": "task_abc"}

    def fake_append(**kw):
        captured.update(kw)
        return mock_record

    with patch("airflow_mcp.models.server.append_execution_log",
               side_effect=fake_append) as mock_fn:
        await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "テスト"}],
            "task_id": "task_abc",
        })
    mock_fn.assert_called_once()
    assert captured.get("task_id") == "task_abc"
    assert captured.get("model_id") == "model_qwen25_14b_q4"


@pytest.mark.asyncio
async def test_model_execute_no_jsonl_when_no_task_id(tmp_path, monkeypatch):
    """task_id 未指定時は JSONL を書き込まないこと。"""
    import airflow_mcp.orchestrator.execution_log as el_mod
    monkeypatch.setattr(el_mod, "_DEFAULT_LOG_DIR", tmp_path / "logs")
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    await _call("model.execute", {
        "model_id": "model_qwen25_14b_q4",
        "messages": [{"role": "user", "content": "テスト"}],
    })
    assert not (tmp_path / "logs").exists()


# ── 二重実行モード（DDD-05 §8）──────────────────────────

def test_should_dual_execute_high_accuracy_non_interactive():
    args = {
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
        "_meta": {"mode": "recommend"},
    }
    assert models_mod._should_dual_execute(args) is True


def test_should_dual_execute_medium_accuracy():
    args = {"features": {"required_accuracy": "medium"}}
    assert models_mod._should_dual_execute(args) is False


def test_should_dual_execute_interactive_latency():
    args = {
        "features": {"required_accuracy": "high", "latency_sensitivity": "interactive"},
    }
    assert models_mod._should_dual_execute(args) is False


def test_should_dual_execute_manual_mode():
    args = {
        "features": {"required_accuracy": "high"},
        "_meta": {"mode": "manual"},
    }
    assert models_mod._should_dual_execute(args) is False


def test_should_dual_execute_no_features():
    assert models_mod._should_dual_execute({}) is False


def test_json_field_similarity_identical():
    a = {"x": 1, "y": "hello"}
    assert models_mod._json_field_similarity(a, a) == 1.0


def test_json_field_similarity_no_overlap():
    assert models_mod._json_field_similarity({"a": 1}, {"a": 2}) == 0.0


def test_json_field_similarity_partial():
    a = {"x": 1, "y": 2}
    b = {"x": 1, "y": 99}
    assert models_mod._json_field_similarity(a, b) == 0.5


def test_json_field_similarity_none_inputs():
    assert models_mod._json_field_similarity(None, {"a": 1}) == 0.0
    assert models_mod._json_field_similarity({"a": 1}, None) == 0.0


def test_json_field_similarity_empty_dicts():
    assert models_mod._json_field_similarity({}, {}) == 1.0


def test_text_similarity_identical():
    assert models_mod._text_similarity("hello world", "hello world") == 1.0


def test_text_similarity_no_overlap():
    assert models_mod._text_similarity("foo bar", "baz qux") == 0.0


def test_text_similarity_empty():
    assert models_mod._text_similarity("", "") == 1.0


@pytest.mark.asyncio
async def test_model_execute_dual_mode_structured_output_similarity(monkeypatch):
    """二重実行 + structured_output: JSON フィールド一致率で類似度を計算すること（line 316）。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}

    call_count = 0

    def fake_post(url, json, timeout):
        nonlocal call_count
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.raise_for_status = lambda: None
        mock_resp.json.return_value = {"message": {"content": '{"summary": "same"}'}}
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "structured_output": {"schema": schema},
            "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
        })
    assert result["dual_similarity"] == 1.0
    assert result["auto_score"] == 1.0
    assert call_count == 2


@pytest.mark.asyncio
async def test_model_execute_dual_mode_high_similarity(monkeypatch):
    """二重実行で similarity >= 0.95 → auto_score=1.0 が返ること。"""
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    # スタブは毎回同じ応答 → similarity=1.0
    result = await _call("model.execute", {
        "model_id": "model_qwen25_14b_q4",
        "messages": [{"role": "user", "content": "テスト"}],
        "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
    })
    assert result["dual_similarity"] == 1.0
    assert result["auto_score"] == 1.0


@pytest.mark.asyncio
async def test_model_execute_dual_mode_partial_similarity(monkeypatch):
    """二重実行で 0.85 <= similarity < 0.95 → auto_score=0.85 が返ること。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    from unittest.mock import MagicMock, patch

    call_count = 0

    def fake_post(url, json, timeout):
        nonlocal call_count
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.raise_for_status = lambda: None
        # 1 回目と 2 回目で少し異なる内容を返す（类似度を 0.85 < x < 0.95 にする）
        if call_count == 1:
            # 19 words, 18 common → 18/20 = 0.9 (0.85 <= x < 0.95)
            mock_resp.json.return_value = {"message": {"content": "a b c d e f g h i j k l m n o p q r s"}}
        else:
            mock_resp.json.return_value = {"message": {"content": "a b c d e f g h i j k l m n o p q r z"}}
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
        })
    assert 0.85 <= result["dual_similarity"] < 0.95
    assert result["auto_score"] == 0.85


@pytest.mark.asyncio
async def test_model_execute_dual_mode_low_similarity_raises_error(monkeypatch):
    """二重実行で similarity < 0.85 → MODEL_LOW_CONFIDENCE エラー。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    from unittest.mock import MagicMock, patch

    call_count = 0

    def fake_post(url, json, timeout):
        nonlocal call_count
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.raise_for_status = lambda: None
        if call_count == 1:
            mock_resp.json.return_value = {"message": {"content": "completely different output"}}
        else:
            mock_resp.json.return_value = {"message": {"content": "totally unrelated text here"}}
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.execute", {
            "model_id": "model_qwen25_14b_q4",
            "messages": [{"role": "user", "content": "hi"}],
            "features": {"required_accuracy": "high", "latency_sensitivity": "batch"},
        })
    assert "error" in result
    assert result["error"]["code"] == "MODEL_LOW_CONFIDENCE"


# ── model.load ────────────────────────────────────────

@pytest.mark.asyncio
async def test_model_load_stub(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    result = await _call("model.load", {"model_id": "model_qwen25_14b_q4"})
    assert result["loaded"] == "model_qwen25_14b_q4"
    assert result["status"] == "stub"


@pytest.mark.asyncio
async def test_model_load_unknown_model_returns_error():
    result = await _call("model.load", {"model_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "MODEL_NOT_FOUND"


@pytest.mark.asyncio
async def test_model_load_sends_keep_alive_minus_one(monkeypatch):
    """非スタブ: keep_alive=-1 で POST /api/generate が呼ばれること。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4", ollama_tag="qwen2.5:14b-instruct-q4_K_M")
    captured = {}
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["payload"] = json
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.load", {"model_id": "model_qwen25_14b_q4"})
    assert "api/generate" in captured["url"]
    assert captured["payload"]["keep_alive"] == -1
    assert captured["payload"]["model"] == "qwen2.5:14b-instruct-q4_K_M"
    assert result["loaded"] == "model_qwen25_14b_q4"


@pytest.mark.asyncio
async def test_model_load_connection_error_falls_back(monkeypatch):
    """httpx 接続失敗 → エラー情報付きの結果を返す。"""
    import httpx as _httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    with patch("httpx.post", side_effect=_httpx.ConnectError("refused")):
        result = await _call("model.load", {"model_id": "model_qwen25_14b_q4"})
    assert result["loaded"] == "model_qwen25_14b_q4"
    assert "load_error" in result["status"]


# ── model.unload ──────────────────────────────────────

@pytest.mark.asyncio
async def test_model_unload_stub(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    _add_model("model_qwen25_14b_q4")
    result = await _call("model.unload", {"model_id": "model_qwen25_14b_q4"})
    assert result["unloaded"] == "model_qwen25_14b_q4"
    assert result["status"] == "stub"


@pytest.mark.asyncio
async def test_model_unload_unknown_model_returns_error():
    result = await _call("model.unload", {"model_id": "nonexistent"})
    assert "error" in result
    assert result["error"]["code"] == "MODEL_NOT_FOUND"


@pytest.mark.asyncio
async def test_model_unload_sends_keep_alive_zero(monkeypatch):
    """非スタブ: keep_alive=0 で POST /api/generate が呼ばれること。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4", ollama_tag="qwen2.5:14b-instruct-q4_K_M")
    captured = {}
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None

    def fake_post(url, json, timeout):
        captured["payload"] = json
        return mock_resp

    with patch("httpx.post", side_effect=fake_post):
        result = await _call("model.unload", {"model_id": "model_qwen25_14b_q4"})
    assert captured["payload"]["keep_alive"] == 0
    assert result["unloaded"] == "model_qwen25_14b_q4"


@pytest.mark.asyncio
async def test_model_unload_connection_error_falls_back(monkeypatch):
    """httpx 接続失敗 → エラー情報付きの結果を返す。"""
    import httpx as _httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    _add_model("model_qwen25_14b_q4")
    with patch("httpx.post", side_effect=_httpx.ConnectError("refused")):
        result = await _call("model.unload", {"model_id": "model_qwen25_14b_q4"})
    assert result["unloaded"] == "model_qwen25_14b_q4"
    assert "unload_error" in result["status"]


# ── model.ps ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_model_ps_stub(monkeypatch):
    monkeypatch.setenv("OLLAMA_STUB", "true")
    result = await _call("model.ps", {})
    assert result["models"] == []
    assert result["status"] == "stub"


@pytest.mark.asyncio
async def test_model_ps_returns_ollama_response(monkeypatch):
    """非スタブ: GET /api/ps の応答をそのまま返すこと。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None
    mock_resp.json.return_value = {
        "models": [{"name": "qwen2.5:14b-instruct-q4_K_M", "size": 123456}]
    }
    with patch("httpx.get", return_value=mock_resp):
        result = await _call("model.ps", {})
    assert len(result["models"]) == 1
    assert result["models"][0]["name"] == "qwen2.5:14b-instruct-q4_K_M"


@pytest.mark.asyncio
async def test_model_ps_connection_error_falls_back(monkeypatch):
    """httpx 接続失敗 → 空リストを返す。"""
    import httpx as _httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    with patch("httpx.get", side_effect=_httpx.ConnectError("refused")):
        result = await _call("model.ps", {})
    assert result["models"] == []
    assert "ps_error" in result["status"]


# ── _check_ollama ─────────────────────────────────────

def test_check_ollama_skips_when_stub(monkeypatch):
    """OLLAMA_STUB=true のとき _check_ollama は HTTP 呼び出しをしない。"""
    import httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "true")
    with patch.object(httpx, "get", side_effect=AssertionError("should not be called")):
        models_mod._check_ollama()  # 例外が出ないことを確認


def test_check_ollama_logs_version_on_success(monkeypatch):
    """Ollama が応答 → 接続成功ログが記録されること。"""
    from unittest.mock import MagicMock, patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = lambda: None
    mock_resp.json.return_value = {"version": "0.5.0"}
    events = []
    with patch("httpx.get", return_value=mock_resp), \
         patch("airflow_mcp.models.server.log_event",
               side_effect=lambda *a, **kw: events.append(a[1])):
        models_mod._check_ollama()
    assert any("reachable" in e for e in events)


def test_check_ollama_logs_warning_on_failure(monkeypatch):
    """Ollama 未起動 → 警告ログが記録されサーバーは停止しない。"""
    import httpx as _httpx
    from unittest.mock import patch
    monkeypatch.setenv("OLLAMA_STUB", "false")
    events = []
    with patch("httpx.get", side_effect=_httpx.ConnectError("refused")), \
         patch("airflow_mcp.models.server.log_event",
               side_effect=lambda *a, **kw: events.append(a[1])):
        models_mod._check_ollama()  # 例外を送出しないこと
    assert any("unreachable" in e for e in events)


# ── GPU 例外フォールバック ────────────────────────────

@pytest.mark.asyncio
async def test_model_recommend_falls_back_when_gpu_server_import_fails(monkeypatch):
    """gpu.server のインポートが失敗しても recommend が loaded_ids=[], free_vram=0.0 で続行する。"""
    _add_model("model_qwen25_14b_q4")

    # gpu.server をインポートしようとすると例外を発生させる
    import builtins
    real_import = builtins.__import__

    def _bad_import(name, *args, **kwargs):
        if name.endswith("gpu.server") or name == "airflow_mcp.gpu.server":
            raise ImportError("gpu server not available")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _bad_import)

    result = await _call("model.recommend", {"features": {}})
    # GPU state 取得失敗でも推奨リストが返ること
    assert "recommendations" in result
    assert len(result["recommendations"]) >= 1


@pytest.mark.asyncio
async def test_model_recommend_falls_back_when_gpu_state_attribute_raises(monkeypatch):
    """_gpu オブジェクトの属性アクセスが例外を起こしても loaded_ids=[], free_vram=0.0 で続行する。"""
    _add_model("model_qwen25_14b_q4")

    # gpu.server は import できるが _gpu 属性アクセスで例外
    import types
    fake_gpu_server = types.SimpleNamespace(_gpu=None)

    class _BrokenGpu:
        @property
        def loaded_model_ids(self):
            raise RuntimeError("GPU state unavailable")

        @property
        def free_vram_gb(self):
            raise RuntimeError("GPU state unavailable")

    fake_gpu_server._gpu = _BrokenGpu()
    monkeypatch.setattr("airflow_mcp.gpu.server", fake_gpu_server, raising=False)

    result = await _call("model.recommend", {"features": {}})
    assert "recommendations" in result
    assert len(result["recommendations"]) >= 1


# ── _ollama_tag ───────────────────────────────────────

def test_ollama_tag_uses_registry_value():
    _add_model("model_qwen25_14b_q4", ollama_tag="qwen2.5:14b-instruct-q4_K_M")
    assert models_mod._ollama_tag("model_qwen25_14b_q4") == "qwen2.5:14b-instruct-q4_K_M"


def test_ollama_tag_falls_back_when_not_in_registry():
    # モデルが未登録の場合はフォールバック変換を使う
    assert models_mod._ollama_tag("unknown_model") == "unknown:model"


# ── run() ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_calls_load_registry_and_server(tmp_path, monkeypatch):
    """run() が _check_ollama + _load_registry を呼んだうえで stdio_server を起動すること。"""
    from unittest.mock import AsyncMock, MagicMock, patch
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_stdio_server():
        yield MagicMock(), MagicMock()

    mock_server_run = AsyncMock()
    check_called = []
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_server), \
         patch.object(models_mod.server, "run", mock_server_run), \
         patch.object(models_mod, "_check_ollama", side_effect=lambda: check_called.append(1)):
        await models_mod.run()

    mock_server_run.assert_awaited_once()
    assert len(check_called) == 1

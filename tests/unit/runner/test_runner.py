"""runner/main.py の単体テスト（DDD-05 §9）。"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_mcp.orchestrator.recommender import simple_rule_select
from airflow_mcp.runner.main import (
    ExecutionContext,
    _build_handoff_note,
    _build_messages,
    _call_ollama,
    _execute_with_retry,
    _execute_with_reroute,
    _extract_human_instructions,
    _get_fallback_model,
    _model_id_to_ollama_tag,
    _select_model,
    process_task,
    run_once,
)
from airflow_mcp.taskboard.store import TaskStore


def _ctx(store, model_config=None, registry=None, agent_id="agent_docs", ollama_url="http://localhost"):
    return ExecutionContext(
        store=store,
        model_config=model_config or {},
        registry=registry or {},
        agent_id=agent_id,
        ollama_url=ollama_url,
    )

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


# ── simple_rule_select (コールドスタートフォールバック) ────

def test_select_model_high_accuracy_batch_returns_32b():
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    assert simple_rule_select(task) == "model_qwen3_35b_a3b"


def test_select_model_medium_accuracy_returns_default():
    task = {"features": {"required_accuracy": "medium", "latency_sensitivity": "batch"}}
    assert simple_rule_select(task) == "model_qwen3_35b_a3b"


def test_select_model_high_accuracy_interactive_returns_default():
    # interactive は latency 優先なのでデフォルトモデル
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "interactive"}}
    assert simple_rule_select(task) == "model_qwen3_35b_a3b"


def test_select_model_empty_features_returns_moe():
    assert simple_rule_select({}) == "model_qwen3_35b_a3b"


# ── _select_model (registry なし → simple_rule フォールバック) ─

def test_runner_select_model_no_registry_falls_back_to_simple_rule():
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    model_id, etype = _select_model(task, {}, registry=None)
    assert model_id == "model_qwen3_35b_a3b"
    assert etype is None


# ── _model_id_to_ollama_tag ───────────────────────────────

def test_model_id_to_ollama_tag_uses_registry():
    registry = {"model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b-instruct-q4_K_M"}}
    tag = _model_id_to_ollama_tag("model_qwen25_14b_q4", registry)
    assert tag == "qwen2.5:14b-instruct-q4_K_M"


def test_model_id_to_ollama_tag_fallback_converts_underscores():
    tag = _model_id_to_ollama_tag("model_qwen25_14b_q4", {})
    assert ":" in tag  # フォールバックは model_ を除去して _ → :


# ── _build_messages ────────────────────────────────────────

def test_build_messages_returns_system_and_user():
    task = {"title": "テスト", "description": "説明", "tags": ["a"], "section": "engineering"}
    msgs = _build_messages(task, "システムプロンプト")
    assert msgs[0]["role"] == "system"
    assert msgs[0]["content"] == "システムプロンプト"
    assert msgs[1]["role"] == "user"
    assert "テスト" in msgs[1]["content"]


def test_build_messages_includes_tags_in_user_content():
    task = {"title": "T", "description": "", "tags": ["api", "backend"], "section": "engineering"}
    msgs = _build_messages(task, "sys")
    assert "api" in msgs[1]["content"]
    assert "backend" in msgs[1]["content"]


# ── _call_ollama (STUB mode) ──────────────────────────────

def test_call_ollama_stub_returns_valid_schema():
    os.environ["OLLAMA_STUB"] = "true"
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "user content"},
    ]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    content, parsed, valid = _call_ollama(
        "http://localhost:11434", "stub-model", messages, schema, 0.3, 512, 0
    )
    assert valid is True
    assert "summary" in parsed


def test_call_ollama_stub_schema_valid_is_true():
    os.environ["OLLAMA_STUB"] = "true"
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    _, _, valid = _call_ollama("http://x", "model", messages, schema, 0.3, 512, 0)
    assert valid is True


# ── process_task ──────────────────────────────────────────

def test_process_task_stub_transitions_to_done(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    result = process_task(task_in_progress, _ctx(task_store))
    assert result is True
    done = task_store.get(task["id"])
    assert done["status"] == "done"


def test_process_task_stub_adds_handoff_note(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    process_task(task_in_progress, _ctx(task_store))
    done = task_store.get(task["id"])
    assert len(done["handoff_notes"]) >= 1
    assert "要約" in done["handoff_notes"][-1]["body"]


def test_process_task_invalid_schema_transitions_to_needs_human(
    task_store: TaskStore, sample_task_fields: dict
):
    """schema 不適合が _MAX_RETRY 回続いたら needs_human に遷移すること。"""
    os.environ["OLLAMA_STUB"] = "false"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    invalid_json = json.dumps({"wrong_field": "invalid"})

    def fake_call_ollama(*args, **kwargs):
        return invalid_json, {"wrong_field": "invalid"}, False

    with patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call_ollama):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is False
    failed_task = task_store.get(task["id"])
    assert failed_task["status"] == "needs_human"


# ── run_once ──────────────────────────────────────────────

def test_run_once_processes_only_owned_tasks(task_store: TaskStore, sample_task_fields: dict):
    os.environ["OLLAMA_STUB"] = "true"
    # agent_docs 所有のタスク
    t1 = task_store.create(sample_task_fields)
    task_store.claim(t1["id"], "agent_docs")
    # agent_other 所有のタスク
    t2 = task_store.create({**sample_task_fields, "title": "他エージェントのタスク"})
    task_store.claim(t2["id"], "agent_other")

    n = run_once(_ctx(task_store))
    assert n == 1

    # agent_other のタスクは untouched
    assert task_store.get(t2["id"])["status"] == "in_progress"


def test_run_once_returns_zero_when_no_owned_tasks(task_store: TaskStore, sample_task_fields: dict):
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_other")
    n = run_once(_ctx(task_store))
    assert n == 0


# ── _execute_with_retry ────────────────────────────────────

def test_execute_with_retry_stub_returns_valid():
    os.environ["OLLAMA_STUB"] = "true"
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    content, parsed, valid, retries = _execute_with_retry(
        "http://x", "model", messages, schema
    )
    assert valid is True
    assert retries == 0


def test_execute_with_retry_stops_on_success():
    call_count = 0

    def fake_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return '{"summary":"ok"}', {"summary": "ok"}, True

    with patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        _, _, valid, retries = _execute_with_retry("http://x", "m", [], {})
    assert valid is True
    assert call_count == 1  # 最初の成功で停止


# ── _get_fallback_model ────────────────────────────────────

def test_get_fallback_model_returns_first_in_chain():
    config = {"reroute": {"fallback_chain": {
        "model_qwen25_14b_q4": ["model_qwen25_32b_q3"]
    }}}
    assert _get_fallback_model("model_qwen25_14b_q4", config) == "model_qwen25_32b_q3"


def test_get_fallback_model_returns_none_when_no_chain():
    config = {"reroute": {"fallback_chain": {"model_qwen25_32b_q3": []}}}
    assert _get_fallback_model("model_qwen25_32b_q3", config) is None


def test_get_fallback_model_returns_none_when_model_not_in_chain():
    assert _get_fallback_model("unknown_model", {}) is None


# ── reroute in process_task ───────────────────────────────

def test_process_task_reroute_succeeds_with_fallback(
    task_store: TaskStore, sample_task_fields: dict, monkeypatch
):
    """プライマリ失敗 → fallback_chain で再試行 → 成功 → done。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    call_count = {"n": 0}

    def fake_call(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 3:  # プライマリ: 3回失敗
            return '{"bad":"field"}', {"bad": "field"}, False
        # フォールバック: 成功
        return '{"summary":"ok","key_points":["p1"]}', \
               {"summary": "ok", "key_points": ["p1"]}, True

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b", "preferred_tasks": []},
    }

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        result = process_task(task_in_progress, _ctx(task_store, model_config, registry))

    assert result is True
    done = task_store.get(task["id"])
    assert done["status"] == "done"
    note = done["handoff_notes"][-1]["body"]
    assert "reroute" in note  # reroute 情報が handoff_note に含まれる


def test_process_task_reroute_also_fails_transitions_to_needs_human(
    task_store: TaskStore, sample_task_fields: dict, monkeypatch
):
    """プライマリ + fallback 両方失敗 → needs_human に遷移。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    def always_fail(*args, **kwargs):
        return '{"bad":"x"}', {"bad": "x"}, False

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b", "preferred_tasks": []},
    }

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=always_fail):
        result = process_task(task_in_progress, _ctx(task_store, model_config, registry))

    assert result is False
    assert task_store.get(task["id"])["status"] == "needs_human"


def test_process_task_no_reroute_when_max_attempts_zero(
    task_store: TaskStore, sample_task_fields: dict, monkeypatch
):
    """max_reroute_attempts=0 なら reroute しない。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    calls: list[str] = []

    def fake_call(url, model_tag, *args, **kwargs):
        calls.append(model_tag)
        return '{"bad":"x"}', {"bad": "x"}, False

    model_config = {
        "reroute": {
            "max_reroute_attempts": 0,
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b-q4", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b-q3", "preferred_tasks": []},
    }

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        process_task(task_in_progress, _ctx(task_store, model_config, registry))

    # fallback モデルの tag が呼ばれていないこと
    assert not any("32b" in c for c in calls), f"fallback が呼ばれた: {calls}"


# ── load_model_config / _load_registry ───────────────

def test_load_model_config_returns_empty_when_no_file():
    from airflow_mcp.runner.main import load_model_config
    assert load_model_config("/nonexistent/config.yaml") == {}


def test_load_model_config_reads_existing_yaml(tmp_path):
    from airflow_mcp.runner.main import load_model_config
    cfg = tmp_path / "model-config.yaml"
    cfg.write_text("knn:\n  k: 5\n")
    result = load_model_config(str(cfg))
    assert result["knn"]["k"] == 5


def test_load_registry_returns_empty_when_no_file():
    from airflow_mcp.runner.main import _load_registry
    assert _load_registry("/nonexistent/registry.json") == {}


def test_load_registry_reads_existing_json(tmp_path):
    import json
    from airflow_mcp.runner.main import _load_registry
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"models": [{"id": "model_test", "ollama_tag": "test:latest"}]}))
    result = _load_registry(str(reg))
    assert "model_test" in result


# ── _call_ollama non-stub paths ───────────────────────

def test_call_ollama_json_decode_error(monkeypatch):
    import os
    from unittest.mock import MagicMock, patch
    os.environ["OLLAMA_STUB"] = "false"
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "not json {"}}
    mock_resp.raise_for_status = lambda: None
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post", return_value=mock_resp):
        content, parsed, valid = _call_ollama(
            "http://localhost:11434", "model", messages, schema, 0.3, 512, 0
        )
    assert valid is False
    assert parsed is None


def test_call_ollama_schema_mismatch(monkeypatch):
    import os
    import json as _json
    from unittest.mock import MagicMock, patch
    os.environ["OLLAMA_STUB"] = "false"
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": _json.dumps({"wrong": "field"})}}
    mock_resp.raise_for_status = lambda: None
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post", return_value=mock_resp):
        content, parsed, valid = _call_ollama(
            "http://localhost:11434", "model", messages, schema, 0.3, 512, 0
        )
    assert valid is False


# ── _execute_with_retry ───────────────────────────────

def test_execute_with_retry_http_error_exhausts_retries():
    import os
    from unittest.mock import patch
    import httpx
    os.environ["OLLAMA_STUB"] = "false"
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    with patch("airflow_mcp.runner.main.httpx.post",
               side_effect=httpx.ConnectError("refused")), \
         patch("airflow_mcp.runner.main.time.sleep"):
        content, parsed, valid, retries = _execute_with_retry(
            "http://bad-host", "model", messages, schema
        )
    assert valid is False


# ── run_once ─────────────────────────────────────────

def test_run_once_processes_owned_in_progress_tasks(task_store, sample_task_fields):
    import os
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    n = run_once(_ctx(task_store, ollama_url="http://localhost:11434"))
    assert n >= 1


def test_run_once_skips_tasks_owned_by_other_agent(task_store, sample_task_fields):
    import os
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_other")
    n = run_once(_ctx(task_store, ollama_url="http://localhost:11434"))
    assert n == 0


def test_run_once_returns_zero_with_empty_store(task_store):
    n = run_once(_ctx(task_store, ollama_url="http://localhost:11434"))
    assert n == 0


# ── _get_fallback_model ───────────────────────────────

def test_get_fallback_model_uses_first_in_chain():
    config = {"reroute": {"fallback_chain": {"model_a": ["model_b", "model_c"]}}}
    assert _get_fallback_model("model_a", config) == "model_b"


def test_get_fallback_model_returns_none_when_not_in_chain():
    assert _get_fallback_model("model_unknown", {}) is None


def test_call_ollama_non_stub_valid_schema(monkeypatch):
    """非 stub で schema 適合 JSON を返す場合 schema_valid=True になること（line 143）。"""
    import os
    import json as _json
    from unittest.mock import MagicMock, patch
    os.environ["OLLAMA_STUB"] = "false"
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": _json.dumps({"summary": "ok"})}}
    mock_resp.raise_for_status = lambda: None
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post", return_value=mock_resp):
        content, parsed, valid = _call_ollama(
            "http://localhost:11434", "model", messages, schema, 0.3, 512, 0
        )
    assert valid is True
    assert parsed["summary"] == "ok"


def test_process_task_execution_log_failure_still_completes(
    task_store, sample_task_fields
):
    """execution_log の書き込み失敗でもタスク処理が完了すること（lines 232-234）。"""
    import os
    from unittest.mock import patch
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    with patch("airflow_mcp.runner.main.append_execution_log",
               side_effect=IOError("disk full")):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True
    assert task_store.get(task["id"])["status"] == "done"


# ── _select_model KNN path (lines 69-70) ─────────────

def test_select_model_uses_knn_when_registry_and_config_present():
    """registry と model_config が両方あるとき select_model (KNN) を呼ぶこと。"""
    from unittest.mock import patch
    from airflow_mcp.runner.main import _select_model
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    registry = {"model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []}}
    config = {"knn": {"k": 3}}
    with patch("airflow_mcp.runner.main.select_model",
               return_value=("model_qwen25_14b_q4", "exploit")) as mock_select:
        model_id, etype = _select_model(task, config, registry=registry, history=[])
    mock_select.assert_called_once()
    assert model_id == "model_qwen25_14b_q4"
    assert etype == "exploit"


def test_select_model_calls_load_history_when_history_is_none():
    """history=None のとき load_history() を呼んで履歴を補完すること（line 69）。"""
    from unittest.mock import patch
    from airflow_mcp.runner.main import _select_model
    task = {"features": {}}
    registry = {"model_a": {"ollama_tag": "a:latest", "preferred_tasks": []}}
    config = {"knn": {"k": 3}}
    with patch("airflow_mcp.runner.main.load_history", return_value=[]) as mock_hist, \
         patch("airflow_mcp.runner.main.select_model", return_value=("model_a", None)):
        _select_model(task, config, registry=registry, history=None)
    mock_hist.assert_called_once()


# ── execution_log failure during reroute (lines 265-266) ─

def test_process_task_reroute_execution_log_failure_still_completes(
    task_store, sample_task_fields, monkeypatch
):
    """reroute 後の execution_log 書き込み失敗でもタスクが完了すること（lines 265-266）。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    call_count = {"n": 0}

    def fake_call(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 3:
            return '{"bad":"x"}', {"bad": "x"}, False
        return '{"summary":"ok","key_points":[]}', {"summary": "ok", "key_points": []}, True

    log_call_count = {"n": 0}

    def fake_log(*args, **kwargs):
        log_call_count["n"] += 1
        if log_call_count["n"] == 2:  # reroute の二回目の呼び出しで失敗
            raise IOError("disk full during reroute")
        return {"id": "log_test_001"}

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b", "preferred_tasks": []},
    }

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call), \
         patch("airflow_mcp.runner.main.append_execution_log", side_effect=fake_log):
        result = process_task(task_in_progress, _ctx(task_store, model_config, registry))

    assert result is True
    assert task_store.get(task["id"])["status"] == "done"


# ── run_once exception handler (lines 333-334) ───────

def test_run_once_catches_process_task_exception(task_store, sample_task_fields):
    """process_task が例外を投げても run_once がクラッシュしないこと（lines 333-334）。"""
    import os
    os.environ["OLLAMA_STUB"] = "false"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")

    with patch("airflow_mcp.runner.main.process_task",
               side_effect=RuntimeError("unexpected crash")):
        n = run_once(_ctx(task_store))

    assert n == 0  # 例外は catch されカウントされない


# ── run_loop (lines 346-357) ─────────────────────────

def test_run_loop_runs_and_handles_results(task_store, monkeypatch):
    """run_loop が run_once を呼び出し、処理数 > 0 のときログを出力すること。"""
    call_count = {"n": 0}

    def fake_run_once(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return 1  # 処理あり → logger.info が呼ばれる
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.runner.main.run_once", fake_run_once)
    monkeypatch.setattr("airflow_mcp.runner.main.time.sleep", lambda s: None)

    from airflow_mcp.runner.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(_ctx(task_store, ollama_url="http://localhost:11434"), poll_interval_s=0.001)

    assert call_count["n"] == 2


def test_run_loop_catches_run_once_exception(task_store, monkeypatch):
    """run_loop が run_once の例外を catch してループを継続すること（lines 355-356）。"""
    call_count = {"n": 0}

    def fake_run_once(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient store error")
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.runner.main.run_once", fake_run_once)
    monkeypatch.setattr("airflow_mcp.runner.main.time.sleep", lambda s: None)

    from airflow_mcp.runner.main import run_loop
    with pytest.raises(SystemExit):
        run_loop(_ctx(task_store, ollama_url="http://localhost:11434"), poll_interval_s=0.001)

    assert call_count["n"] == 2


# ── reroute.skip_for interactive (DDD-06 §7.3) ───────

def test_process_task_reroute_skips_for_interactive_latency(
    task_store: TaskStore, sample_task_fields: dict, monkeypatch
):
    """interactive latency タスクは reroute せず即 needs_human に escalate すること。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    fields = {**sample_task_fields, "features": {
        **sample_task_fields.get("features", {}),
        "latency_sensitivity": "interactive",
    }}
    task = task_store.create(fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "skip_for": ["interactive"],
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b", "preferred_tasks": []},
    }

    call_models: list[str] = []

    def fake_call(url, model_tag, *args, **kwargs):
        call_models.append(model_tag)
        return '{"bad":"x"}', {"bad": "x"}, False

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        result = process_task(task_in_progress, _ctx(task_store, model_config, registry))

    assert result is False
    assert task_store.get(task["id"])["status"] == "needs_human"
    # fallback モデルは呼ばれていないこと
    assert not any("32b" in m for m in call_models)


def test_process_task_reroute_on_low_auto_score(
    task_store: TaskStore, sample_task_fields: dict, monkeypatch
):
    """schema_valid=True でも auto_score < 0.7 のとき reroute を試みること（DDD-06 §7.3）。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    call_models: list[str] = []

    def fake_call(url, model_tag, messages, output_schema, temperature, max_tokens, attempt):
        call_models.append(model_tag)
        if "14b" in model_tag:
            # プライマリ: schema_valid=True だが retry_count=2（auto_score≈0.5 < 0.7）
            if attempt < 2:
                return '{"bad":"field"}', {"bad": "field"}, False
            return '{"summary":"ok","key_points":[]}', \
                   {"summary": "ok", "key_points": []}, True
        # フォールバック: 成功（attempt=0 で True）
        return '{"summary":"fallback ok","key_points":[]}', \
               {"summary": "fallback ok", "key_points": []}, True

    model_config = {
        "reroute": {
            "max_reroute_attempts": 1,
            "skip_for": [],
            "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
        }
    }
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b-q4", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b-q3", "preferred_tasks": []},
    }

    with patch("airflow_mcp.runner.main._select_model",
               return_value=("model_qwen25_14b_q4", None)), \
         patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        result = process_task(task_in_progress, _ctx(task_store, model_config, registry))

    assert result is True
    done = task_store.get(task["id"])
    assert done["status"] == "done"
    # fallback モデルが呼ばれたこと
    assert any("32b" in m for m in call_models)


# ── _cli (lines 361-368) ─────────────────────────────

def test_cli_entry_point(monkeypatch, tmp_path):
    """_cli が ExecutionContext を組み立てて run_loop に渡すこと。"""
    call_args = {}

    def fake_run_loop(ctx, *args, **kwargs):
        call_args["ctx"] = ctx
        raise SystemExit(0)

    monkeypatch.setattr("airflow_mcp.runner.main.run_loop", fake_run_loop)
    monkeypatch.setenv("AIRFLOW_TASKBOARD_DIR", str(tmp_path))

    from airflow_mcp.runner.main import _cli
    with pytest.raises(SystemExit):
        _cli()

    assert "ctx" in call_args
    assert call_args["ctx"].agent_id == "agent_docs"


# ── _build_handoff_note ───────────────────────────────

def test_build_handoff_note_includes_summary_and_model():
    note = _build_handoff_note(
        parsed={"summary": "テスト要約", "key_points": ["p1", "p2"], "confidence": 0.92},
        content="raw",
        model_id="model_qwen25_14b_q4",
        latency_ms=1200,
        retry_count=0,
        rerouted_from=None,
        log_id="log_abc",
    )
    assert "テスト要約" in note
    assert "model_qwen25_14b_q4" in note
    assert "log_abc" in note
    assert "reroute" not in note


def test_build_handoff_note_includes_reroute_info():
    note = _build_handoff_note(
        parsed={"summary": "ok", "key_points": [], "confidence": 0.8},
        content="raw",
        model_id="model_qwen25_32b_q3",
        latency_ms=3000,
        retry_count=1,
        rerouted_from="model_qwen25_14b_q4",
        log_id="log_xyz",
    )
    assert "reroute" in note
    assert "model_qwen25_14b_q4" in note
    assert "model_qwen25_32b_q3" in note


def test_build_handoff_note_falls_back_when_parsed_none():
    note = _build_handoff_note(
        parsed=None,
        content="raw content fallback",
        model_id="model_qwen25_14b_q4",
        latency_ms=500,
        retry_count=0,
        rerouted_from=None,
        log_id="log_001",
    )
    assert "raw content fallback" in note


# ── _execute_with_reroute ─────────────────────────────

def test_execute_with_reroute_stub_returns_exec_result(task_store, sample_task_fields):
    """STUB モードで _ExecResult が返りスキーマ有効になること。"""
    import os
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    from airflow_mcp.common.audit import new_audit_id
    r = _execute_with_reroute(
        task, "model_qwen25_14b_q4", None,
        _ctx(task_store, ollama_url="http://localhost:11434"),
        messages, schema, new_audit_id(),
    )
    assert r.schema_valid is True
    assert r.rerouted_from is None
    assert r.model_id == "model_qwen25_14b_q4"


def test_execute_with_reroute_uses_fallback_on_primary_failure(
    task_store, sample_task_fields, monkeypatch
):
    """プライマリ失敗時に fallback_chain のモデルで再試行すること。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    call_models: list[str] = []

    def fake_call(url, model_tag, *args, **kwargs):
        call_models.append(model_tag)
        if "14b" in model_tag:
            return '{"bad":"x"}', {"bad": "x"}, False
        return '{"summary":"fb ok","key_points":[]}', {"summary": "fb ok", "key_points": []}, True

    model_config = {"reroute": {
        "max_reroute_attempts": 1,
        "fallback_chain": {"model_qwen25_14b_q4": ["model_qwen25_32b_q3"]},
    }}
    registry = {
        "model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []},
        "model_qwen25_32b_q3": {"ollama_tag": "qwen2.5:32b", "preferred_tasks": []},
    }
    from airflow_mcp.common.audit import new_audit_id
    with patch("airflow_mcp.runner.main._call_ollama", side_effect=fake_call):
        r = _execute_with_reroute(
            task, "model_qwen25_14b_q4", None,
            _ctx(task_store, model_config, registry),
            messages, schema, new_audit_id(),
        )
    assert r.schema_valid is True
    assert r.rerouted_from == "model_qwen25_14b_q4"
    assert r.model_id == "model_qwen25_32b_q3"


# ── T2-A: 追加テスト ──────────────────────────────────

def test_call_ollama_timeout_error_propagates(monkeypatch):
    """httpx.TimeoutError が _call_ollama から伝播して _execute_with_retry に catch されること。"""
    import os
    import httpx
    from unittest.mock import patch
    os.environ["OLLAMA_STUB"] = "false"
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post",
               side_effect=httpx.TimeoutException("timed out")), \
         patch("airflow_mcp.runner.main.time.sleep"):
        content, parsed, valid, retries = _execute_with_retry(
            "http://localhost:11434", "model", messages, schema
        )
    assert valid is False
    assert parsed is None


def test_call_ollama_read_timeout_propagates(monkeypatch):
    """httpx.ReadTimeout が _execute_with_retry に catch されること。"""
    import os
    import httpx
    from unittest.mock import patch
    os.environ["OLLAMA_STUB"] = "false"
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post",
               side_effect=httpx.ReadTimeout("read timed out")), \
         patch("airflow_mcp.runner.main.time.sleep"):
        _, _, valid, _ = _execute_with_retry(
            "http://localhost:11434", "model", messages, schema
        )
    assert valid is False


def test_call_ollama_missing_message_key_raises_value_error(monkeypatch):
    """Ollama レスポンスに 'message' キーが無い場合 ValueError が raise されること。"""
    import os
    import pytest
    from unittest.mock import MagicMock, patch
    os.environ["OLLAMA_STUB"] = "false"
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"done": True}  # "message" キーなし
    mock_resp.raise_for_status = lambda: None
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]
    with patch("airflow_mcp.runner.main.httpx.post", return_value=mock_resp):
        with pytest.raises(ValueError, match="empty content"):
            _call_ollama("http://localhost:11434", "model", messages, schema, 0.3, 512, 0)


def test_call_ollama_stub_single_message_no_index_error(monkeypatch):
    """messages が 1 要素以下でもスタブモードが IndexError を起こさないこと（T1-A）。"""
    import os
    os.environ["OLLAMA_STUB"] = "true"
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "only system"}]  # user メッセージなし
    content, parsed, valid = _call_ollama(
        "http://localhost:11434", "stub-model", messages, schema, 0.3, 512, 0
    )
    assert valid is True
    assert "summary" in parsed


def test_process_task_uses_fallback_prompt_when_file_missing(
    task_store, sample_task_fields, monkeypatch, tmp_path
):
    """システムプロンプトファイルが存在しない場合にデフォルトプロンプトを使うこと。"""
    monkeypatch.setenv("OLLAMA_STUB", "true")
    # _SYSTEM_PROMPT_PATH を存在しないパスにパッチ
    monkeypatch.setattr("airflow_mcp.runner.main._SYSTEM_PROMPT_PATH",
                        tmp_path / "nonexistent-prompt.md")
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    built_messages: list = []

    original_build = __import__(
        "airflow_mcp.runner.main", fromlist=["_build_messages"]
    )._build_messages

    def capturing_build(task, system_prompt, context="", no_think_prefix=False):
        built_messages.append(system_prompt)
        return original_build(task, system_prompt, context)

    with patch("airflow_mcp.runner.main._build_messages", side_effect=capturing_build):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True
    # デフォルトプロンプトが使われていること
    assert len(built_messages) == 1
    assert "JSON" in built_messages[0]


def test_execute_with_reroute_skips_when_fallback_not_in_registry(
    task_store, sample_task_fields, monkeypatch
):
    """fallback_chain に fallback_id があっても registry に無い場合は reroute しないこと。"""
    monkeypatch.setenv("OLLAMA_STUB", "false")
    task = task_store.create(sample_task_fields)
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]

    def always_fail(*args, **kwargs):
        return '{"bad":"x"}', {"bad": "x"}, False

    model_config = {"reroute": {
        "max_reroute_attempts": 1,
        "fallback_chain": {"model_qwen25_14b_q4": ["model_nonexistent"]},
    }}
    # model_nonexistent は registry に存在しない
    registry = {"model_qwen25_14b_q4": {"ollama_tag": "qwen2.5:14b", "preferred_tasks": []}}

    from airflow_mcp.common.audit import new_audit_id
    with patch("airflow_mcp.runner.main._call_ollama", side_effect=always_fail):
        r = _execute_with_reroute(
            task, "model_qwen25_14b_q4", None,
            _ctx(task_store, model_config, registry),
            messages, schema, new_audit_id(),
        )
    # reroute されずプライマリ結果のまま
    assert r.rerouted_from is None
    assert r.schema_valid is False


def test_load_model_config_non_dict_yaml_returns_empty(tmp_path):
    """YAML が list などの非dict を返した場合 {} を返すこと（T1-A）。"""
    from airflow_mcp.runner.main import load_model_config
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("- item1\n- item2\n")  # list YAML
    result = load_model_config(str(cfg_file))
    assert result == {}


def test_build_handoff_note_with_none_log_id():
    """log_id が None の場合でも handoff_note を生成できること（T1-B）。"""
    note = _build_handoff_note(
        parsed={"summary": "テスト", "key_points": [], "confidence": 0.9},
        content="raw",
        model_id="model_qwen25_14b_q4",
        latency_ms=500,
        retry_count=0,
        rerouted_from=None,
        log_id=None,
    )
    assert "テスト" in note
    assert "実行ログ" not in note  # log_id なし → 実行ログ行は省略


def test_load_registry_non_dict_json_returns_empty(tmp_path):
    """registry.json が配列など非 dict の場合 {} を返すこと（runner/_load_registry line 61）。"""
    import json as _json
    from airflow_mcp.runner.main import _load_registry
    reg = tmp_path / "registry.json"
    reg.write_text(_json.dumps([{"id": "model_x"}]))  # list, not dict
    result = _load_registry(str(reg))
    assert result == {}


def test_process_task_output_schema_oserror_uses_fallback(task_store, sample_task_fields, monkeypatch, tmp_path):
    """output_schema ファイルが JSON として不正なとき fallback schema を使うこと（lines 355-356）。"""
    monkeypatch.setenv("OLLAMA_STUB", "true")
    # 不正な JSON ファイルにパッチ（JSONDecodeError → fallback）
    bad_schema = tmp_path / "bad_schema.json"
    bad_schema.write_text("{ not valid json }")
    monkeypatch.setattr("airflow_mcp.runner.main._OUTPUT_SCHEMA_PATH", bad_schema)

    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    result = process_task(task_in_progress, _ctx(task_store))
    # fallback schema が使われて正常終了すること
    assert result is not None


def test_process_task_logs_when_handoff_spawned(task_store, sample_task_fields, monkeypatch):
    """spawn_with_join_tracking が ID を返す場合に handoff.spawned がログ出力されること。"""
    import os
    from unittest.mock import patch
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    with patch("airflow_mcp.runner.main.spawn_with_join_tracking", return_value=["child_task_1"]):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True


# ── model selection exception → needs_human (lines 354-360) ──

def test_process_task_model_selection_exception_transitions_to_needs_human(
    task_store, sample_task_fields, monkeypatch
):
    """_select_model が例外を投げたとき needs_human に遷移し notify が呼ばれること。"""
    import os
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    with patch("airflow_mcp.runner.main._select_model",
               side_effect=RuntimeError("registry unavailable")), \
         patch("airflow_mcp.runner.main.notify_needs_human") as mock_notify:
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is False
    updated = task_store.get(task["id"])
    assert updated["status"] == "needs_human"
    mock_notify.assert_called_once()
    _, _, _, _, reason = mock_notify.call_args[0]
    assert "MODEL_SELECTION_ERROR" in reason


# ── sequential handoff early return True (lines 412-416) ──

def test_process_task_sequential_handoff_returns_true(
    task_store, sample_task_fields, monkeypatch
):
    """route_to_next_agent が next_agent を返したとき process_task が True で返ること。"""
    import os
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create(sample_task_fields)
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    with patch("airflow_mcp.runner.main.route_to_next_agent",
               return_value="agent_reviewer"):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True
    # done に遷移していないこと（オーナー変遷前の状態を確認）
    updated = task_store.get(task["id"])
    assert updated["status"] == "in_progress"


# ── _extract_human_instructions ──────────────────────────────────────────────

def test_extract_human_instructions_returns_dashboard_notes():
    task = {
        "handoff_notes": [
            {"author": {"type": "agent", "id": "agent_docs"}, "body": "エージェントの要約"},
            {"author": "dashboard", "body": "📝 [手動指示] 詳細化して"},
            {"author": {"type": "agent", "id": "dashboard"}, "body": "🔍 [再検討指示] 再確認"},
        ]
    }
    result = _extract_human_instructions(task)
    assert "📝 [手動指示] 詳細化して" in result
    assert "エージェントの要約" not in result


def test_extract_human_instructions_excludes_system_notes():
    task = {
        "handoff_notes": [
            {"author": "dashboard", "body": "✏️ [description 更新] 人間が直接編集"},
            {"author": "dashboard", "body": "📥 [description 更新] エージェント回答を反映"},
        ]
    }
    result = _extract_human_instructions(task)
    assert result == []


def test_extract_human_instructions_empty_task():
    assert _extract_human_instructions({}) == []


# ── _build_messages with human instructions ──────────────────────────────────

def test_build_messages_includes_human_instructions():
    task = {
        "id": "task_test001",
        "title": "テストタスク",
        "description": "元の説明",
        "tags": ["test"],
        "section": "engineering",
        "handoff_notes": [
            {"author": "dashboard", "body": "🔍 [再検討指示] オッズ情報を追加して"},
        ],
    }
    messages = _build_messages(task, "system prompt")
    user_content = messages[1]["content"]
    assert "人間からの追加指示" in user_content
    assert "オッズ情報を追加して" in user_content
    assert "updated_description" in user_content


def test_build_messages_no_instructions_no_block():
    task = {
        "id": "task_test002",
        "title": "テストタスク",
        "description": "説明",
        "tags": [],
        "section": "engineering",
        "handoff_notes": [],
    }
    messages = _build_messages(task, "system prompt")
    user_content = messages[1]["content"]
    assert "人間からの追加指示" not in user_content


# ── process_task: updated_description auto-update ────────────────────────────

def test_process_task_auto_updates_description_when_present(task_store, sample_task_fields):
    """parsed に updated_description があれば description が自動更新されること。"""
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create({**sample_task_fields, "description": "元の説明"})
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    stub_parsed = {
        "summary": "テスト要約テスト要約テスト要約テスト",
        "key_points": ["ポイント1", "ポイント2", "ポイント3"],
        "confidence": 0.9,
        "suggested_tags": ["test"],
        "updated_description": "## 更新済み説明\n新しい内容",
    }
    with patch("airflow_mcp.runner.main._execute_with_retry",
               return_value=(json.dumps(stub_parsed), stub_parsed, True, 0)):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True
    updated = task_store.get(task["id"])
    assert updated["description"] == "## 更新済み説明\n新しい内容"


def test_process_task_skips_description_update_when_null(task_store, sample_task_fields):
    """updated_description が null の場合は description を変更しないこと。"""
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create({**sample_task_fields, "description": "元の説明"})
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    stub_parsed = {
        "summary": "テスト要約テスト要約テスト要約テスト",
        "key_points": ["ポイント1", "ポイント2", "ポイント3"],
        "confidence": 0.9,
        "suggested_tags": ["test"],
        "updated_description": None,
    }
    with patch("airflow_mcp.runner.main._execute_with_retry",
               return_value=(json.dumps(stub_parsed), stub_parsed, True, 0)):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True
    updated = task_store.get(task["id"])
    assert updated["description"] == "元の説明"


def test_process_task_description_update_exception_is_swallowed(task_store, sample_task_fields):
    """updated_description auto-update が例外でも process_task は成功すること。"""
    os.environ["OLLAMA_STUB"] = "true"
    task = task_store.create({**sample_task_fields, "description": "元の説明"})
    task_store.claim(task["id"], "agent_docs")
    task_in_progress = task_store.get(task["id"])

    stub_parsed = {
        "summary": "テスト要約テスト要約テスト要約テスト",
        "key_points": ["ポイント1", "ポイント2", "ポイント3"],
        "confidence": 0.9,
        "suggested_tags": ["test"],
        "updated_description": "## 新説明",
    }
    with patch("airflow_mcp.runner.main._execute_with_retry",
               return_value=(json.dumps(stub_parsed), stub_parsed, True, 0)), \
         patch.object(task_store, "update", side_effect=RuntimeError("lock conflict")):
        result = process_task(task_in_progress, _ctx(task_store))

    assert result is True

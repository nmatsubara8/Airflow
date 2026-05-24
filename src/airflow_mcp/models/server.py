"""models-mcp: モデルレジストリ・推論 MCP サーバー（DDD-02 §5、DDD-05）。

ツール: model.list / model.get / model.recommend / model.execute /
        model.feedback / model.stats / model.register /
        model.load / model.unload / model.ps
"""
from __future__ import annotations

import json
import os
import random
import threading
import time
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.auth import require_scope
from ..common.errors import (
    AirflowError,
    ModelLowConfidence,
    ModelNotFound,
    ModelStructuredOutputInvalid,
)
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from ..common.url_validator import validate_ollama_url
from ..common.thresholds import (
    DEFAULT_MODEL_ID,
    DUAL_SCORE_PARTIAL,
    DUAL_SIM_HIGH,
    DUAL_SIM_LOW,
    EPSILON_GREEDY_RATE,
    KNN_COLD_START_MIN_SAMPLES,
)
from ..orchestrator.execution_log import append_execution_log, load_history, update_feedback
from ..orchestrator.recommender import _cold_start_model, knn_recommend
from ..scheduler.affinity import compute_swap_cost

server = Server("models-mcp")

_REGISTRY_PATH = Path(os.environ.get("AIRFLOW_REGISTRY", ".airflow/models/registry.json"))
_models: dict[str, dict[str, Any]] = {}
_execution_logs: dict[str, dict[str, Any]] = {}

# HTTP マルチ接続移行時の race condition 防止（DDD-08 §4 / 提案 A）
_state_lock = threading.RLock()

# リトライ設定（DDD-05 §7）
_STRUCT_OUTPUT_RETRIES = 2


def _load_registry() -> None:
    if _REGISTRY_PATH.exists():
        data = json.loads(_REGISTRY_PATH.read_text())
        for m in data.get("models", []):
            _models[m["id"]] = m


def _ollama_base_url() -> str:
    return validate_ollama_url(
        os.environ.get("OLLAMA_URL", "http://localhost:11434")
    )


def _ollama_tag(model_id: str) -> str:
    """registry の ollama_tag を使用。未登録時はフォールバック変換（DDD-05 §3.1）。"""
    return (
        _models.get(model_id, {}).get("ollama_tag")
        or model_id.replace("model_", "").replace("_", ":")
    )


def _check_ollama() -> None:
    """起動時 Ollama 疎通確認（DDD-05 §4.2）。STUB 環境ではスキップ。"""
    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        return
    try:
        import httpx
        resp = httpx.get(f"{_ollama_base_url()}/api/version", timeout=5.0)
        resp.raise_for_status()
        version = resp.json().get("version", "unknown")
        log_event("info", "ollama.reachable", "models-mcp", new_audit_id(),
                  data={"version": version})
    except Exception as exc:
        log_event("warn", "ollama.unreachable", "models-mcp", new_audit_id(),
                  data={"error": str(exc)})


def _should_dual_execute(args: dict[str, Any]) -> bool:
    """二重実行モードの起動条件を確認する（DDD-05 §8.1）。"""
    features = args.get("features") or {}
    mode = (args.get("_meta") or {}).get("mode", "recommend")
    return (
        features.get("required_accuracy") == "high"
        and mode != "manual"
        and features.get("latency_sensitivity") != "interactive"
    )


def _json_field_similarity(a: dict | None, b: dict | None) -> float:
    """構造化出力の JSON フィールド完全一致率を計算する（DDD-05 §8.2）。"""
    if a is None or b is None:
        return 0.0
    all_keys = set(a.keys()) | set(b.keys())
    if not all_keys:
        return 1.0
    matches = sum(1 for k in all_keys if a.get(k) == b.get(k))
    return matches / len(all_keys)


def _text_similarity(a: str, b: str) -> float:
    """自由文の単語単位 Jaccard 類似度（Phase 2 では bge-m3 の代替）。"""
    if not a and not b:
        return 1.0
    set_a = set(a.split())
    set_b = set(b.split())
    union = len(set_a | set_b)
    return len(set_a & set_b) / union if union else 0.0


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="model.list",
            description="登録済みモデル一覧を返す",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="model.get",
            description="ID 指定でモデルを取得",
            inputSchema={
                "type": "object",
                "required": ["model_id"],
                "properties": {"model_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="model.recommend",
            description="タスク特徴から推奨モデルを返す",
            inputSchema={
                "type": "object",
                "required": ["features"],
                "properties": {
                    "features": {"type": "object"},
                    "mode": {"type": "string", "enum": ["manual", "recommend", "auto"],
                             "default": "recommend"},
                    "candidate_models": {"type": "array", "items": {"type": "string"}},
                },
            },
        ),
        types.Tool(
            name="model.execute",
            description="指定モデルで推論実行",
            inputSchema={
                "type": "object",
                "required": ["model_id", "messages"],
                "properties": {
                    "model_id": {"type": "string"},
                    "messages": {"type": "array"},
                    "structured_output": {"type": "object"},
                    "max_tokens": {"type": "integer"},
                    "temperature": {"type": "number"},
                    "task_id": {"type": "string"},
                    "features": {"type": "object"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="model.feedback",
            description="実行結果に対するフィードバックを登録",
            inputSchema={
                "type": "object",
                "required": ["task_id", "human_feedback"],
                "properties": {
                    "task_id": {"type": "string"},
                    "execution_log_id": {"type": "string"},
                    "human_feedback": {
                        "type": "string",
                        "enum": ["accepted", "modified", "rejected"],
                    },
                    "comment": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="model.stats",
            description="モデル別統計を返す",
            inputSchema={
                "type": "object",
                "properties": {"model_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="model.register",
            description="新規モデルを registry に登録",
            inputSchema={
                "type": "object",
                "required": ["model"],
                "properties": {"model": {"type": "object"}},
            },
        ),
        types.Tool(
            name="model.load",
            description="モデルを明示的に VRAM にロード（keep_alive=-1）",
            inputSchema={
                "type": "object",
                "required": ["model_id"],
                "properties": {"model_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="model.unload",
            description="モデルを明示的に VRAM からアンロード（keep_alive=0）",
            inputSchema={
                "type": "object",
                "required": ["model_id"],
                "properties": {"model_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="model.ps",
            description="現在 VRAM にロードされているモデル一覧を返す（/api/ps）",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


async def _model_list(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    return mcp_result({"models": list(_models.values())})


async def _model_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)
    return mcp_result(_models[model_id])


async def _model_recommend(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    features = args.get("features", {})
    mode = args.get("mode", "recommend")
    candidates = args.get("candidate_models") or list(_models.keys())

    # GPU 状態取得（gpu/server._gpu から遅延参照）
    try:
        from ..gpu import server as _gpu_server  # noqa: PLC0415
        _gpu = _gpu_server._gpu
        loaded_ids: list[str] = _gpu.loaded_model_ids if _gpu else []
        free_vram: float = _gpu.free_vram_gb if _gpu else 0.0
    except Exception:
        loaded_ids = []
        free_vram = 0.0

    def _swap_ms(model_id: str) -> int:
        return round(compute_swap_cost(model_id, loaded_ids, free_vram, _models))

    # manual モード: 候補の先頭を返す（DDD-06 §4.1）
    if mode == "manual":
        selected = candidates[0] if candidates else DEFAULT_MODEL_ID
        return mcp_result({
            "recommendations": [{
                "model_id": selected,
                "score": 1.0,
                "expected_accuracy": 1.0,
                "swap_cost_ms": _swap_ms(selected),
                "confidence": 1.0,
                "reason": "手動指定（manual モード）",
            }],
            "sample_count": 0,
        })

    # KNN 推奨（DDD-06 §4.3）
    history = load_history()
    knn_config: dict[str, Any] = {}
    available = [c for c in candidates if c in _models]
    recommendations = knn_recommend(
        features, history, available, knn_config,
        loaded_model_ids=loaded_ids,
        free_vram_gb=free_vram,
        registry=_models,
    )
    total_samples = sum(r["sample_count"] for r in recommendations)

    # コールドスタート（サンプル不足）
    if not recommendations or total_samples < KNN_COLD_START_MIN_SAMPLES:
        selected = _cold_start_model(features, _models, {})
        if selected not in available:
            selected = available[0] if available else (candidates[0] if candidates else DEFAULT_MODEL_ID)
        return mcp_result({
            "recommendations": [{
                "model_id": selected,
                "score": 0.5,
                "expected_accuracy": 0.5,
                "swap_cost_ms": _swap_ms(selected),
                "confidence": 0.0,
                "reason": "コールドスタート（履歴不足）",
            }],
            "sample_count": total_samples,
        })

    # ε-greedy 探索（auto モードのみ、DDD-06 §4.5）
    if mode == "auto" and random.random() < EPSILON_GREEDY_RATE:
        others = [m for m in available if m != recommendations[0]["model_id"]]
        if others:
            selected = random.choice(others)
            return mcp_result({
                "recommendations": [{
                    "model_id": selected,
                    "score": recommendations[0]["score"],
                    "expected_accuracy": recommendations[0]["expected_accuracy"],
                    "swap_cost_ms": _swap_ms(selected),
                    "confidence": 0.0,
                    "reason": f"ε-greedy 探索（ε={EPSILON_GREEDY_RATE}）",
                }],
                "sample_count": total_samples,
            })

    return mcp_result({
        "recommendations": recommendations,
        "sample_count": total_samples,
    })


async def _model_execute(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:execute")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)

    start_ms = int(time.time() * 1000)
    meta = args.get("_meta", {}) or {}
    aid = new_audit_id()
    messages = args["messages"]
    structured_output = args.get("structured_output")
    temperature = args.get("temperature")
    max_tokens = args.get("max_tokens")
    task_id = args.get("task_id")
    dual = _should_dual_execute(args)

    # 1 回目実行（二重実行時は temperature=0.3 固定）
    content_a, parsed_a, schema_valid_a, retry_count_a = _call_ollama_with_retry(
        model_id=model_id,
        messages=messages,
        structured_output=structured_output,
        temperature=0.3 if dual else temperature,
        max_tokens=max_tokens,
    )

    # 二重実行モード（DDD-05 §8）
    auto_score: float | None = None
    dual_similarity: float | None = None
    if dual:
        content_b, parsed_b, _schema_valid_b, _retry_b = _call_ollama_with_retry(
            model_id=model_id,
            messages=messages,
            structured_output=structured_output,
            temperature=0.5,
            max_tokens=max_tokens,
        )
        if structured_output:
            dual_similarity = _json_field_similarity(parsed_a, parsed_b)
        else:
            dual_similarity = _text_similarity(content_a, content_b)

        if dual_similarity < DUAL_SIM_LOW:
            raise ModelLowConfidence(model_id, dual_similarity)
        auto_score = 1.0 if dual_similarity >= DUAL_SIM_HIGH else DUAL_SCORE_PARTIAL

    latency_ms = int(time.time() * 1000) - start_ms
    log_id = "log_" + aid[:16].lower()

    # インメモリ記録（model.stats 用）
    with _state_lock:
        _execution_logs[log_id] = {
            "schema_version": "1.0.0",
            "id": log_id,
            "timestamp": now_iso(),
            "task_id": task_id,
            "agent_id": meta.get("agent_id"),
            "model_id": model_id,
            "latency_ms": latency_ms,
            "success": schema_valid_a,
            "schema_valid": schema_valid_a,
            "audit_id": aid,
            "parent_audit_id": meta.get("audit_id"),
        }

    # JSONL 永続化（DDD-05 §9）。task_id がある場合のみ学習データとして記録。
    if task_id:
        append_execution_log(
            task_id=task_id,
            agent_id=meta.get("agent_id") or "",
            model_id=model_id,
            features_snapshot=args.get("features") or {},
            schema_valid=schema_valid_a,
            retry_count=retry_count_a,
            latency_ms=latency_ms,
        )

    log_event("info", "model.executed", "models-mcp", aid,
              model_id=model_id, task_id=task_id,
              duration_ms=latency_ms, parent_audit_id=meta.get("audit_id"))

    result: dict[str, Any] = {
        "content": content_a,
        "parsed": parsed_a,
        "execution_log_id": log_id,
    }
    if dual:
        result["dual_similarity"] = dual_similarity
        result["auto_score"] = auto_score
    return mcp_result(result)


def _call_ollama_with_retry(
    model_id: str,
    messages: list[dict],
    structured_output: dict | None,
    temperature: float | None,
    max_tokens: int | None,
) -> tuple[str, dict | None, bool, int]:
    """構造化出力リトライ付きで Ollama を呼び出す（DDD-05 §7）。

    Returns:
        (content, parsed, schema_valid, retry_count)
    """
    current_messages = list(messages)
    last_exc: ModelStructuredOutputInvalid | None = None

    for attempt in range(_STRUCT_OUTPUT_RETRIES + 1):
        try:
            content, parsed, schema_valid = _call_ollama(
                model_id=model_id,
                messages=current_messages,
                structured_output=structured_output,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return content, parsed, schema_valid, attempt
        except ModelStructuredOutputInvalid as exc:
            last_exc = exc
            if attempt < _STRUCT_OUTPUT_RETRIES:
                errors = exc.data.get("schema_errors", [])
                hint = (
                    f"（前回の出力は JSON として不正でした：{errors}）"
                    "必ず有効な JSON オブジェクト 1 つのみを返してください。"
                )
                current_messages = list(messages) + [{"role": "user", "content": hint}]

    assert last_exc is not None
    raise last_exc


def _call_ollama(
    model_id: str,
    messages: list[dict],
    structured_output: dict | None,
    temperature: float | None,
    max_tokens: int | None,
) -> tuple[str, dict | None, bool]:
    """Ollama を呼び出す。OLLAMA_STUB=true の場合はスタブ応答を返す。"""
    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        stub_content = json.dumps({
            "summary": "スタブ応答: Ollama 未接続",
            "output": "Phase 2 スタブ — 実 Ollama 未接続",
        }, ensure_ascii=False)
        return stub_content, json.loads(stub_content), True

    try:
        import httpx
        model_tag = _ollama_tag(model_id)
        payload: dict[str, Any] = {
            "model": model_tag,
            "messages": messages,
            "stream": False,
        }
        if temperature is not None:
            payload.setdefault("options", {})["temperature"] = temperature
        if max_tokens is not None:
            payload.setdefault("options", {})["num_predict"] = max_tokens
        if structured_output and structured_output.get("schema"):
            payload["format"] = structured_output["schema"]

        resp = httpx.post(f"{_ollama_base_url()}/api/chat", json=payload, timeout=120.0)
        resp.raise_for_status()
        content = resp.json()["message"]["content"]

        parsed = None
        schema_valid = True
        if structured_output and structured_output.get("schema"):
            try:
                parsed = json.loads(content)
                from jsonschema import Draft202012Validator
                errors = list(Draft202012Validator(structured_output["schema"]).iter_errors(parsed))
                if errors:
                    schema_valid = False
                    raise ModelStructuredOutputInvalid([e.message for e in errors])
            except json.JSONDecodeError as exc:
                raise ModelStructuredOutputInvalid([f"JSON decode error: {exc}"]) from exc

        return content, parsed, schema_valid

    except Exception as exc:
        if isinstance(exc, AirflowError):
            raise
        # Ollama 接続失敗はスタブ応答にフォールバック
        stub = {"summary": f"Ollama 接続失敗: {exc}", "output": ""}
        return json.dumps(stub, ensure_ascii=False), stub, True


async def _model_feedback(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    """人間フィードバックを ExecutionLog に登録し KNN 学習データを更新（DDD-06 §5.3）。"""
    require_scope(token, "models:execute")
    task_id = args["task_id"]
    human_feedback = args["human_feedback"]

    updated = update_feedback(task_id, human_feedback)
    aid = new_audit_id()
    log_event("info", "model.feedback_recorded", "models-mcp", aid,
              task_id=task_id,
              data={"human_feedback": human_feedback,
                    "auto_score": updated.get("auto_score") if updated else None})

    return mcp_result({
        "task_id": task_id,
        "human_feedback": human_feedback,
        "updated_record": updated,
        "recorded_at": now_iso(),
    })


async def _model_stats(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    model_id = args.get("model_id")
    with _state_lock:
        logs = [log for log in _execution_logs.values()
                if not model_id or log.get("model_id") == model_id]
    total = len(logs)
    success = sum(1 for log in logs if log.get("success"))
    avg_latency = sum(log.get("latency_ms", 0) for log in logs) / total if total else 0
    return mcp_result({
        "model_id": model_id or "all",
        "total_executions": total,
        "success_rate": success / total if total else 0,
        "avg_latency_ms": int(avg_latency),
    })


async def _model_register(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:write")
    model = args["model"]
    with _state_lock:
        _models[model["id"]] = model
    return mcp_result({"registered": model["id"]})


async def _model_load(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    """モデルを明示的に VRAM にロードする（keep_alive=-1）（DDD-05 §4.3）。"""
    require_scope(token, "models:execute")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)

    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        return mcp_result({"loaded": model_id, "status": "stub"})

    try:
        import httpx
        model_tag = _ollama_tag(model_id)
        resp = httpx.post(
            f"{_ollama_base_url()}/api/generate",
            json={"model": model_tag, "prompt": "", "keep_alive": -1},
            timeout=120.0,
        )
        resp.raise_for_status()
        return mcp_result({"loaded": model_id, "ollama_tag": model_tag})
    except Exception as exc:
        stub = {"loaded": model_id, "status": f"load_error: {exc}"}
        return mcp_result(stub)


async def _model_unload(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    """モデルを明示的に VRAM からアンロードする（keep_alive=0）（DDD-05 §4.3）。"""
    require_scope(token, "models:execute")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)

    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        return mcp_result({"unloaded": model_id, "status": "stub"})

    try:
        import httpx
        model_tag = _ollama_tag(model_id)
        resp = httpx.post(
            f"{_ollama_base_url()}/api/generate",
            json={"model": model_tag, "prompt": "", "keep_alive": 0},
            timeout=30.0,
        )
        resp.raise_for_status()
        return mcp_result({"unloaded": model_id, "ollama_tag": model_tag})
    except Exception as exc:
        stub = {"unloaded": model_id, "status": f"unload_error: {exc}"}
        return mcp_result(stub)


async def _model_ps(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    """現在ロードされているモデル一覧を Ollama /api/ps から取得する（DDD-05 §4.3）。"""
    require_scope(token, "models:read")

    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        return mcp_result({"models": [], "status": "stub"})

    try:
        import httpx
        resp = httpx.get(f"{_ollama_base_url()}/api/ps", timeout=10.0)
        resp.raise_for_status()
        return mcp_result(resp.json())
    except Exception as exc:
        return mcp_result({"models": [], "status": f"ps_error: {exc}"})


_HANDLERS = {
    "model.list": _model_list,
    "model.get": _model_get,
    "model.recommend": _model_recommend,
    "model.execute": _model_execute,
    "model.feedback": _model_feedback,
    "model.stats": _model_stats,
    "model.register": _model_register,
    "model.load": _model_load,
    "model.unload": _model_unload,
    "model.ps": _model_ps,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS)


async def run() -> None:
    _check_ollama()
    await run_stdio_server(server, init_fn=_load_registry)

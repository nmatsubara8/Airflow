"""models-mcp: モデルレジストリ・推論 MCP サーバー（DDD-02 §5）。

ツール: model.list / model.get / model.recommend / model.execute /
        model.feedback / model.stats / model.register
"""
from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.auth import get_token_from_env, require_scope
from ..common.rate_limiter import check_rate_limit
from ..common.errors import AirflowError, ModelNotFound, ModelStructuredOutputInvalid
from ..orchestrator.execution_log import update_feedback

server = Server("models-mcp")

_REGISTRY_PATH = Path(os.environ.get("AIRFLOW_REGISTRY", ".airflow/models/registry.json"))
_models: dict[str, dict[str, Any]] = {}
_execution_logs: dict[str, dict[str, Any]] = {}
_feedback_store: list[dict[str, Any]] = []


def _load_registry() -> None:
    if _REGISTRY_PATH.exists():
        data = json.loads(_REGISTRY_PATH.read_text())
        for m in data.get("models", []):
            _models[m["id"]] = m


def _result(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False))]


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
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    token = get_token_from_env()
    check_rate_limit(token)
    try:
        if name == "model.list":
            return await _model_list(arguments, token)
        elif name == "model.get":
            return await _model_get(arguments, token)
        elif name == "model.recommend":
            return await _model_recommend(arguments, token)
        elif name == "model.execute":
            return await _model_execute(arguments, token)
        elif name == "model.feedback":
            return await _model_feedback(arguments, token)
        elif name == "model.stats":
            return await _model_stats(arguments, token)
        elif name == "model.register":
            return await _model_register(arguments, token)
        else:
            return _result({"error": {"code": "UNKNOWN_TOOL", "message": name}})
    except AirflowError as e:
        return e.to_mcp_result()


async def _model_list(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    return _result({"models": list(_models.values())})


async def _model_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)
    return _result(_models[model_id])


async def _model_recommend(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    features = args.get("features", {})
    mode = args.get("mode", "recommend")
    candidates = args.get("candidate_models") or list(_models.keys())

    # Phase 2 スタブ: KNN 未実装。features ベースの単純ルール。
    # required_accuracy=high → 32b, それ以外 → 14b
    required_accuracy = features.get("required_accuracy", "medium")
    latency = features.get("latency_sensitivity", "batch")

    if mode == "manual":
        # manual モードは候補の先頭を返す
        selected = candidates[0] if candidates else "model_qwen25_14b_q4"
        reason = "手動指定（manual モード）"
    elif required_accuracy == "high" and latency != "interactive":
        selected = "model_qwen25_32b_q3"
        reason = "required_accuracy=high → 32B 推奨"
    else:
        selected = "model_qwen25_14b_q4"
        reason = "デフォルト 14B（Phase 2 MVP）"

    # ε-greedy: 10% でランダム探索（DDD-06 §4.5）
    model_config_path = Path(
        os.environ.get("AIRFLOW_MODEL_CONFIG", ".airflow/config/model-config.yaml")
    )
    epsilon = 0.1
    if random.random() < epsilon:
        selected = random.choice([m for m in candidates if m in _models] or [selected])
        reason = f"ε-greedy 探索（ε={epsilon}）"

    model = _models.get(selected, {})
    return _result({
        "recommendations": [{
            "model_id": selected,
            "score": 0.85,
            "expected_accuracy": 0.85 if selected == "model_qwen25_32b_q3" else 0.75,
            "swap_cost_ms": model.get("avg_load_time_ms", 10000),
            "confidence": 0.5,  # Phase 2: 履歴なしのため低め
            "reason": reason,
        }],
        "sample_count": 0,  # Phase 2: 履歴未蓄積
    })


async def _model_execute(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:execute")
    model_id = args["model_id"]
    if model_id not in _models:
        raise ModelNotFound(model_id)

    start_ms = int(time.time() * 1000)
    meta = args.get("_meta", {})
    aid = new_audit_id()

    # Ollama 呼び出し
    content, parsed, schema_valid = _call_ollama(
        model_id=model_id,
        messages=args["messages"],
        structured_output=args.get("structured_output"),
        temperature=args.get("temperature"),
        max_tokens=args.get("max_tokens"),
    )

    latency_ms = int(time.time() * 1000) - start_ms
    log_id = "log_" + aid[:16].lower()

    _execution_logs[log_id] = {
        "schema_version": "1.0.0",
        "id": log_id,
        "timestamp": now_iso(),
        "task_id": args.get("task_id"),
        "agent_id": meta.get("agent_id"),
        "model_id": model_id,
        "latency_ms": latency_ms,
        "success": schema_valid,
        "schema_valid": schema_valid,
        "audit_id": aid,
        "parent_audit_id": meta.get("audit_id"),
    }

    log_event("info", "model.executed", "models-mcp", aid,
              model_id=model_id, task_id=args.get("task_id"),
              duration_ms=latency_ms, parent_audit_id=meta.get("audit_id"))

    return _result({
        "content": content,
        "parsed": parsed,
        "execution_log_id": log_id,
    })


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
        ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
        model_tag = model_id.replace("model_", "").replace("_", ":")
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

        resp = httpx.post(f"{ollama_url}/api/chat", json=payload, timeout=120.0)
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

    return _result({
        "task_id": task_id,
        "human_feedback": human_feedback,
        "updated_record": updated,
        "recorded_at": now_iso(),
    })


async def _model_stats(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:read")
    model_id = args.get("model_id")
    logs = [l for l in _execution_logs.values()
            if not model_id or l.get("model_id") == model_id]
    total = len(logs)
    success = sum(1 for l in logs if l.get("success"))
    avg_latency = sum(l.get("latency_ms", 0) for l in logs) / total if total else 0
    return _result({
        "model_id": model_id or "all",
        "total_executions": total,
        "success_rate": success / total if total else 0,
        "avg_latency_ms": int(avg_latency),
    })


async def _model_register(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "models:execute")
    model = args["model"]
    _models[model["id"]] = model
    return _result({"registered": model["id"]})


async def run() -> None:
    _load_registry()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

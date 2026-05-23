"""gpu-mcp: GPU 状態・スケジューラ MCP サーバー（DDD-02 §6）。

ツール: gpu.state / gpu.reserve / gpu.release /
        scheduler.queue_status / scheduler.profile_switch / scheduler.degrade
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server
import yaml

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.auth import get_token_from_env, require_scope
from ..common.errors import AirflowError, GpuBusy

server = Server("gpu-mcp")

_PARALLEL_CONFIG = Path(os.environ.get("AIRFLOW_PARALLEL_CONFIG", ".airflow/config/parallel.yaml"))

# インメモリ GPU 状態
_state: dict[str, Any] = {
    "vram_total_gb": 16.0,
    "vram_available_gb": 15.0,
    "loaded_models": [],
    "current_profile": None,
    "degraded": False,
}
_task_queues: dict[str, list] = {}  # model_id → pending task list
_profiles: list[dict] = []


def _load_config() -> None:
    if _PARALLEL_CONFIG.exists():
        cfg = yaml.safe_load(_PARALLEL_CONFIG.read_text())
        _state["vram_total_gb"] = cfg.get("scheduler", {}).get("vram_total_gb", 16.0)
        _state["vram_available_gb"] = cfg.get("scheduler", {}).get("vram_available_gb", 15.0)
        _profiles.extend(cfg.get("profiles", []))
        mvp = cfg.get("mvp_profile")
        if mvp:
            _state["current_profile"] = mvp


def _result(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False))]


def _free_vram() -> float:
    used = sum(m.get("vram_gb", 0) for m in _state["loaded_models"])
    return _state["vram_available_gb"] - used


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="gpu.state",
            description="現在の GPU メモリ・ロード済みモデルを返す",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="gpu.reserve",
            description="モデルロードを要求。VRAM 不足時は LRU で退避",
            inputSchema={
                "type": "object",
                "required": ["model_id"],
                "properties": {
                    "model_id": {"type": "string"},
                    "wait": {"type": "boolean", "default": True},
                    "timeout_ms": {"type": "integer", "default": 60000},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="gpu.release",
            description="モデルをアンロード",
            inputSchema={
                "type": "object",
                "required": ["model_id"],
                "properties": {
                    "model_id": {"type": "string"},
                    "_meta": {"type": "object"},
                },
            },
        ),
        types.Tool(
            name="scheduler.queue_status",
            description="各モデルのキュー深度・待機時間を返す",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="scheduler.profile_switch",
            description="スケジューラプロファイルを切替",
            inputSchema={
                "type": "object",
                "required": ["profile_name"],
                "properties": {"profile_name": {"type": "string"}},
            },
        ),
        types.Tool(
            name="scheduler.degrade",
            description="デグラデーションモードを強制 ON/OFF",
            inputSchema={
                "type": "object",
                "required": ["enabled"],
                "properties": {
                    "enabled": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    token = get_token_from_env()
    try:
        if name == "gpu.state":
            return await _gpu_state(arguments, token)
        elif name == "gpu.reserve":
            return await _gpu_reserve(arguments, token)
        elif name == "gpu.release":
            return await _gpu_release(arguments, token)
        elif name == "scheduler.queue_status":
            return await _scheduler_queue_status(arguments, token)
        elif name == "scheduler.profile_switch":
            return await _scheduler_profile_switch(arguments, token)
        elif name == "scheduler.degrade":
            return await _scheduler_degrade(arguments, token)
        else:
            return _result({"error": {"code": "UNKNOWN_TOOL", "message": name}})
    except AirflowError as e:
        return e.to_mcp_result()


async def _gpu_state(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    return _result({
        "schema_version": "1.0.0",
        "snapshot_at": now_iso(),
        "vram_total_gb": _state["vram_total_gb"],
        "vram_available_gb": _state["vram_available_gb"],
        "vram_free_gb": _free_vram(),
        "loaded_models": _state["loaded_models"],
        "current_profile": _state["current_profile"],
        "degraded": _state["degraded"],
    })


async def _gpu_reserve(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    wait = args.get("wait", True)

    # すでにロード済みか確認
    loaded_ids = [m["id"] for m in _state["loaded_models"]]
    if model_id in loaded_ids:
        return _result({"reserved": True, "loaded_at": now_iso(), "evicted_models": []})

    # VRAM チェック（registry.json からサイズを取得できない場合はデフォルト 9GB）
    registry_path = Path(os.environ.get("AIRFLOW_REGISTRY", ".airflow/models/registry.json"))
    vram_needed = 9.0
    if registry_path.exists():
        registry = json.loads(registry_path.read_text())
        for m in registry.get("models", []):
            if m["id"] == model_id:
                vram_needed = m.get("vram_gb", 9.0)
                break

    evicted: list[str] = []
    # LRU 退避: pending_tasks == 0 のモデルを退避（DDD-07 §9.2）
    while _free_vram() < vram_needed:
        evictable = [m for m in _state["loaded_models"] if not m.get("pending_tasks", 0)]
        if not evictable:
            if not wait:
                raise GpuBusy()
            # wait=true でも退避できない場合はエラー
            raise GpuBusy(retry_after_ms=10000)
        lru = evictable[0]
        _state["loaded_models"].remove(lru)
        evicted.append(lru["id"])
        log_event("info", "gpu.model_evicted", "gpu-mcp", new_audit_id(),
                  model_id=lru["id"])

    _state["loaded_models"].append({
        "id": model_id,
        "vram_gb": vram_needed,
        "loaded_at": now_iso(),
        "pending_tasks": 0,
    })

    aid = new_audit_id()
    meta = args.get("_meta", {})
    log_event("info", "gpu.reserved", "gpu-mcp", aid,
              model_id=model_id, parent_audit_id=meta.get("audit_id"),
              data={"evicted": evicted})

    return _result({"reserved": True, "loaded_at": now_iso(), "evicted_models": evicted})


async def _gpu_release(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    before = len(_state["loaded_models"])
    _state["loaded_models"] = [m for m in _state["loaded_models"] if m["id"] != model_id]
    released = len(_state["loaded_models"]) < before

    meta = args.get("_meta", {})
    if released:
        log_event("info", "gpu.released", "gpu-mcp", new_audit_id(),
                  model_id=model_id, parent_audit_id=meta.get("audit_id"))

    return _result({"released": released, "free_vram_gb": _free_vram()})


async def _scheduler_queue_status(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    queues = {
        model_id: {"depth": len(tasks), "oldest_wait_s": 0}
        for model_id, tasks in _task_queues.items()
    }
    return _result({"queues": queues, "current_profile": _state["current_profile"]})


async def _scheduler_profile_switch(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    profile_name = args["profile_name"]
    known = [p["name"] for p in _profiles]
    if profile_name not in known:
        return _result({"error": {"code": "UNKNOWN_PROFILE",
                                  "message": f"'{profile_name}' は未定義。既知: {known}"}})
    old = _state["current_profile"]
    _state["current_profile"] = profile_name
    log_event("info", "scheduler.profile_switched", "gpu-mcp", new_audit_id(),
              data={"from": old, "to": profile_name})
    return _result({"switched": True, "from": old, "to": profile_name})


async def _scheduler_degrade(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    _state["degraded"] = args["enabled"]
    log_event("warn" if args["enabled"] else "info",
              "scheduler.degraded" if args["enabled"] else "scheduler.degraded_recovered",
              "gpu-mcp", new_audit_id(),
              data={"reason": args.get("reason")})
    return _result({"degraded": _state["degraded"]})


async def run() -> None:
    _load_config()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

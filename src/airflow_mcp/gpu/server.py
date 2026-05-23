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
import yaml
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.auth import get_token_from_env, require_scope
from ..common.rate_limiter import check_rate_limit
from ..common.errors import AirflowError, GpuBusy
from ..scheduler.profile import get_profile_models, select_best_profile
from ..scheduler.queue import QueueEntry, SchedulerQueue
from .state_manager import GpuStateManager

server = Server("gpu-mcp")

_PARALLEL_CONFIG = Path(os.environ.get("AIRFLOW_PARALLEL_CONFIG", ".airflow/config/parallel.yaml"))

_gpu: GpuStateManager = GpuStateManager()
_task_queues: dict[str, list] = {}   # legacy: raw list per model (used by tests)
_scheduler_queue: SchedulerQueue = SchedulerQueue()
_profiles: list[dict] = []
_current_profile: str | None = None
_degraded: bool = False
_fair_share: dict[str, float] = {}
_registry: dict[str, dict] = {}


def _load_config() -> None:
    global _current_profile, _scheduler_queue, _gpu
    if _PARALLEL_CONFIG.exists():
        cfg = yaml.safe_load(_PARALLEL_CONFIG.read_text())
        sched = cfg.get("scheduler", {})
        pinned = cfg.get("gpu", {}).get("pinned_models", [])
        _gpu = GpuStateManager(
            vram_available_gb=sched.get("vram_available_gb", 15.0),
            vram_total_gb=sched.get("vram_total_gb", 16.0),
            pinned_models=pinned,
            snapshot_interval_s=5.0,
        )
        _profiles.clear()
        _profiles.extend(cfg.get("profiles", []))
        _current_profile = cfg.get("mvp_profile")

        starvation_cfg = cfg.get("starvation_prevention", {})
        fair_share_cfg = cfg.get("fair_share", {})
        _scheduler_queue = SchedulerQueue(
            starvation_config=starvation_cfg,
            fair_share_config=fair_share_cfg,
        )

        _fair_share.clear()
        _fair_share.update(fair_share_cfg)


def _load_registry_cache() -> dict[str, dict]:
    registry_path = Path(os.environ.get("AIRFLOW_REGISTRY", ".airflow/models/registry.json"))
    if registry_path.exists():
        data = json.loads(registry_path.read_text())
        return {m["id"]: m for m in data.get("models", [])}
    return {}


def _result(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False))]


def _free_vram() -> float:
    return _gpu.free_vram_gb


def get_model_config_with_degraded() -> dict[str, Any]:
    """affinity scorer に渡す model_config を構築する（degraded 状態を反映）。"""
    return {"degraded": _degraded}


def suggest_profile_switch() -> str | None:
    """キュー深度に基づきプロファイル自動選択を実施する（DDD-07 §4.2）。"""
    global _current_profile
    if not _profiles:
        return _current_profile
    depths = _scheduler_queue.model_depths()
    registry = _load_registry_cache()
    current_models = get_profile_models(_profiles, _current_profile)
    best = select_best_profile(
        _profiles, depths, _current_profile, current_models,
        _gpu.free_vram_gb, registry,
    )
    if best and best != _current_profile:
        old = _current_profile
        _current_profile = best
        log_event("info", "scheduler.auto_profile_switch", "gpu-mcp", new_audit_id(),
                  data={"from": old, "to": best})
    return _current_profile


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
        check_rate_limit(token)
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
    snap = _gpu.snapshot()
    snap["active_profile"] = _current_profile
    snap["degraded"] = _degraded
    return _result(snap)


async def _gpu_reserve(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    wait = args.get("wait", True)

    registry = _load_registry_cache()
    result = _gpu.reserve(model_id=model_id, wait=wait, registry=registry)
    _gpu.touch(model_id)
    return _result(result)


async def _gpu_release(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    result = _gpu.release(model_id)
    # GPU 使用時間をフェアシェア計算に記録する（DDD-07 §6.4）
    if result.get("released") and result.get("duration_ms", 0) > 0:
        _scheduler_queue.record_completion(model_id, result["duration_ms"])
    return _result(result)


async def _scheduler_queue_status(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    depths = _scheduler_queue.model_depths()
    queues = {model_id: {"depth": d, "oldest_wait_s": 0} for model_id, d in depths.items()}
    return _result({"queues": queues, "current_profile": _current_profile, "degraded": _degraded})


async def _scheduler_profile_switch(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    global _current_profile
    require_scope(token, "gpu:reserve")
    profile_name = args["profile_name"]
    known = [p["name"] for p in _profiles]
    if profile_name not in known:
        return _result({"error": {"code": "UNKNOWN_PROFILE",
                                  "message": f"'{profile_name}' は未定義。既知: {known}"}})
    old = _current_profile
    _current_profile = profile_name
    log_event("info", "scheduler.profile_switched", "gpu-mcp", new_audit_id(),
              data={"from": old, "to": profile_name})
    return _result({"switched": True, "from": old, "to": profile_name})


async def _scheduler_degrade(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    global _degraded
    require_scope(token, "gpu:reserve")
    _degraded = args["enabled"]
    log_event("warn" if _degraded else "info",
              "scheduler.degraded" if _degraded else "scheduler.degraded_recovered",
              "gpu-mcp", new_audit_id(),
              data={"reason": args.get("reason")})
    return _result({"degraded": _degraded})


async def run() -> None:
    _load_config()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

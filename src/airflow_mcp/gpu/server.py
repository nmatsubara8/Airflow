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

from ..common.audit import log_event, new_audit_id
from ..common.auth import require_scope
from ..common.errors import SchedulerProfileNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from ..scheduler.profile import get_profile_models, select_best_profile
from ..scheduler.queue import SchedulerQueue
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


async def _gpu_state(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    snap = _gpu.snapshot()
    snap["active_profile"] = _current_profile
    snap["degraded"] = _degraded
    return mcp_result(snap)


async def _gpu_reserve(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    wait = args.get("wait", True)

    registry = _load_registry_cache()
    result = _gpu.reserve(model_id=model_id, wait=wait, registry=registry)
    _gpu.touch(model_id)
    return mcp_result(result)


async def _gpu_release(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    result = _gpu.release(model_id)
    # GPU 使用時間をフェアシェア計算に記録する（DDD-07 §6.4）
    if result.get("released") and result.get("duration_ms", 0) > 0:
        _scheduler_queue.record_completion(model_id, result["duration_ms"])
    return mcp_result(result)


async def _scheduler_queue_status(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    depths = _scheduler_queue.model_depths()
    queues = {model_id: {"depth": d, "oldest_wait_s": 0} for model_id, d in depths.items()}
    return mcp_result({"queues": queues, "current_profile": _current_profile, "degraded": _degraded})


async def _scheduler_profile_switch(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    global _current_profile
    require_scope(token, "gpu:reserve")
    profile_name = args["profile_name"]
    known = [p["name"] for p in _profiles]
    if profile_name not in known:
        raise SchedulerProfileNotFound(profile_name, known)
    old = _current_profile
    _current_profile = profile_name
    log_event("info", "scheduler.profile_switched", "gpu-mcp", new_audit_id(),
              data={"from": old, "to": profile_name})
    return mcp_result({"switched": True, "from": old, "to": profile_name})


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
    return mcp_result({"degraded": _degraded})


_HANDLERS = {
    "gpu.state": _gpu_state,
    "gpu.reserve": _gpu_reserve,
    "gpu.release": _gpu_release,
    "scheduler.queue_status": _scheduler_queue_status,
    "scheduler.profile_switch": _scheduler_profile_switch,
    "scheduler.degrade": _scheduler_degrade,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS)


async def run() -> None:
    await run_stdio_server(server, init_fn=_load_config)

"""gpu-mcp: GPU 状態・スケジューラ MCP サーバー（DDD-02 §6）。

ツール: gpu.state / gpu.reserve / gpu.release /
        scheduler.queue_status / scheduler.profile_switch / scheduler.degrade
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

import mcp.types as types
from ..common.io import load_model_registry, safe_yaml_load
from mcp.server import Server

from ..common.audit import log_event, new_audit_id
from ..common.auth import require_scope
from ..common.errors import SchedulerProfileNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from ..scheduler.profile import get_profile_models, select_best_profile
from ..scheduler.queue import SchedulerQueue
from .state_manager import GpuStateManager

logger = logging.getLogger(__name__)
server = Server("gpu-mcp")

_PARALLEL_CONFIG = Path(os.environ.get("AIRFLOW_PARALLEL_CONFIG", ".airflow/config/parallel.yaml"))
_OVERRIDE_FILE = Path(os.environ.get("AIRFLOW_DEGRADATION_OVERRIDE", ".airflow/state/degradation_override.json"))

_gpu: GpuStateManager = GpuStateManager()
_task_queues: dict[str, list] = {}   # legacy: raw list per model (used by tests)
_scheduler_queue: SchedulerQueue = SchedulerQueue()
_profiles: list[dict] = []
_current_profile: str | None = None
_degraded: bool = False
_degradation_mode: str = "auto"  # "on" | "off" | "auto"
_fair_share: dict[str, float] = {}
_registry: dict[str, dict] = {}

# HTTP マルチ接続移行時の race condition 防止（DDD-08 §4 / 提案 A）
_state_lock = threading.RLock()


def _load_degradation_override() -> None:
    """起動時に永続化された手動 override を復元する。"""
    global _degraded, _degradation_mode
    if not _OVERRIDE_FILE.exists():
        return
    try:
        data = json.loads(_OVERRIDE_FILE.read_text(encoding="utf-8"))
        mode = data.get("mode", "auto")
        if mode in ("on", "off"):
            _degradation_mode = mode
            _degraded = mode == "on"
            logger.info("degradation override restored: mode=%s", mode)
    except Exception as exc:
        logger.warning("failed to load degradation override: %s", exc)


def _save_degradation_override(mode: str) -> None:
    """手動 override をディスクに永続化する。mode='auto' の場合はファイルを削除。"""
    if mode == "auto":
        try:
            _OVERRIDE_FILE.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("failed to remove degradation override: %s", exc)
        return
    try:
        _OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _OVERRIDE_FILE.write_text(json.dumps({"mode": mode}), encoding="utf-8")
    except Exception as exc:
        logger.warning("failed to save degradation override: %s", exc)


def _load_config() -> None:
    global _current_profile, _scheduler_queue, _gpu
    cfg = safe_yaml_load(_PARALLEL_CONFIG)
    if cfg:
        sched = cfg.get("scheduler", {})
        pinned = cfg.get("gpu", {}).get("pinned_models", [])
        new_gpu = GpuStateManager(
            vram_available_gb=sched.get("vram_available_gb", 15.0),
            vram_total_gb=sched.get("vram_total_gb", 16.0),
            pinned_models=pinned,
            snapshot_interval_s=5.0,
        )

        starvation_cfg = cfg.get("starvation_prevention", {})
        fair_share_cfg = cfg.get("fair_share", {})
        new_queue = SchedulerQueue(
            starvation_config=starvation_cfg,
            fair_share_config=fair_share_cfg,
        )

        with _state_lock:
            _gpu = new_gpu
            _profiles.clear()
            _profiles.extend(cfg.get("profiles", []))
            _current_profile = cfg.get("mvp_profile")
            _scheduler_queue = new_queue
            _fair_share.clear()
            _fair_share.update(fair_share_cfg)

    with _state_lock:
        _registry.clear()
        _registry.update(load_model_registry())
    _load_degradation_override()


def _get_registry() -> dict[str, dict]:
    """起動時にロード済みのレジストリを返す。未ロードなら on-demand でロード。"""
    if not _registry:
        with _state_lock:
            if not _registry:
                _registry.update(load_model_registry())
    return _registry


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
    with _state_lock:
        depths = _scheduler_queue.model_depths()
        registry = _get_registry()
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
            description="デグラデーションモードを手動制御（on/off/auto）。再起動後も維持される。",
            inputSchema={
                "type": "object",
                "required": ["mode"],
                "properties": {
                    "mode": {"type": "string", "enum": ["on", "off", "auto"]},
                    "reason": {"type": "string"},
                },
            },
        ),
    ]


async def _gpu_state(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    snap = _gpu.snapshot()
    with _state_lock:
        snap["active_profile"] = _current_profile
        snap["degraded"] = _degraded
    return mcp_result(snap)


async def _gpu_reserve(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    wait = args.get("wait", True)

    registry = _get_registry()
    with _state_lock:
        result = _gpu.reserve(model_id=model_id, wait=wait, registry=registry)
        _gpu.touch(model_id)
    return mcp_result(result)


async def _gpu_release(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "gpu:reserve")
    model_id = args["model_id"]
    with _state_lock:
        result = _gpu.release(model_id)
        # GPU 使用時間をフェアシェア計算に記録する（DDD-07 §6.4）
        if result.get("released") and result.get("duration_ms", 0) > 0:
            _scheduler_queue.record_completion(model_id, result["duration_ms"])
    return mcp_result(result)


async def _scheduler_queue_status(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "gpu:read")
    with _state_lock:
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
    with _state_lock:
        old = _current_profile
        _current_profile = profile_name
    log_event("info", "scheduler.profile_switched", "gpu-mcp", new_audit_id(),
              data={"from": old, "to": profile_name})
    return mcp_result({"switched": True, "from": old, "to": profile_name})


async def _scheduler_degrade(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    global _degraded, _degradation_mode
    require_scope(token, "gpu:reserve")
    mode = args.get("mode", "auto")
    with _state_lock:
        _degradation_mode = mode
        if mode == "on":
            _degraded = True
        elif mode == "off":
            _degraded = False
        degraded_val = _degraded
    _save_degradation_override(mode)
    log_event("warn" if degraded_val else "info",
              "scheduler.degraded_on" if degraded_val else "scheduler.degraded_off",
              "gpu-mcp", new_audit_id(),
              data={"mode": mode, "reason": args.get("reason")})
    return mcp_result({"degraded": degraded_val, "mode": mode})


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

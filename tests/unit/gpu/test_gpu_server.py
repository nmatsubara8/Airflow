"""gpu/server.py の単体テスト（DDD-02 §6 / DDD-07）。"""
from __future__ import annotations

import json

import pytest

import airflow_mcp.gpu.server as gpu_mod
from airflow_mcp.gpu.server import _free_vram


@pytest.fixture(autouse=True)
def reset_gpu_state():
    """各テスト前後に GPU インメモリ状態をリセット。"""
    gpu_mod._state.update({
        "vram_total_gb": 16.0,
        "vram_available_gb": 15.0,
        "loaded_models": [],
        "current_profile": None,
        "degraded": False,
    })
    gpu_mod._task_queues.clear()
    gpu_mod._profiles.clear()
    yield
    gpu_mod._state.update({
        "vram_total_gb": 16.0,
        "vram_available_gb": 15.0,
        "loaded_models": [],
        "current_profile": None,
        "degraded": False,
    })
    gpu_mod._task_queues.clear()
    gpu_mod._profiles.clear()


async def _call(name: str, args: dict) -> dict:
    result = await gpu_mod.call_tool(name, args)
    return json.loads(result[0].text)


# ── _free_vram ────────────────────────────────────────

def test_free_vram_with_no_models():
    assert _free_vram() == pytest.approx(15.0)


def test_free_vram_accounts_for_loaded_models():
    gpu_mod._state["loaded_models"] = [{"id": "m1", "vram_gb": 9.0}]
    assert _free_vram() == pytest.approx(6.0)


# ── gpu.state ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_state_returns_vram_fields():
    result = await _call("gpu.state", {})
    assert "vram_total_gb" in result
    assert "vram_available_gb" in result
    assert "loaded_models" in result
    assert result["vram_total_gb"] == pytest.approx(16.0)


@pytest.mark.asyncio
async def test_gpu_state_snapshot_at_is_present():
    result = await _call("gpu.state", {})
    assert "snapshot_at" in result


# ── gpu.reserve ───────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_reserve_loads_model_into_state():
    result = await _call("gpu.reserve", {"model_id": "model_a"})
    assert result["reserved"] is True
    ids = [m["id"] for m in gpu_mod._state["loaded_models"]]
    assert "model_a" in ids


@pytest.mark.asyncio
async def test_gpu_reserve_idempotent_for_already_loaded():
    gpu_mod._state["loaded_models"] = [{"id": "model_a", "vram_gb": 9.0, "pending_tasks": 0}]
    result = await _call("gpu.reserve", {"model_id": "model_a"})
    assert result["reserved"] is True
    # 二重ロードされていない
    count = sum(1 for m in gpu_mod._state["loaded_models"] if m["id"] == "model_a")
    assert count == 1


@pytest.mark.asyncio
async def test_gpu_reserve_evicts_lru_when_vram_full():
    gpu_mod._state["vram_available_gb"] = 9.5
    # 9GB モデルをすでにロード済み（evictable: pending_tasks=0）
    gpu_mod._state["loaded_models"] = [
        {"id": "old_model", "vram_gb": 9.0, "pending_tasks": 0, "loaded_at": "2026-01-01T00:00:00Z"}
    ]
    result = await _call("gpu.reserve", {"model_id": "new_model"})
    assert result["reserved"] is True
    assert "old_model" in result["evicted_models"]
    ids = [m["id"] for m in gpu_mod._state["loaded_models"]]
    assert "old_model" not in ids
    assert "new_model" in ids


@pytest.mark.asyncio
async def test_gpu_reserve_fails_when_all_models_have_pending_tasks():
    gpu_mod._state["vram_available_gb"] = 9.5
    gpu_mod._state["loaded_models"] = [
        {"id": "busy_model", "vram_gb": 9.0, "pending_tasks": 2}
    ]
    result = await _call("gpu.reserve", {"model_id": "new_model", "wait": False})
    assert "error" in result
    assert result["error"]["code"] == "GPU_BUSY"


# ── gpu.release ───────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_release_removes_model_from_state():
    gpu_mod._state["loaded_models"] = [{"id": "model_a", "vram_gb": 9.0, "pending_tasks": 0}]
    result = await _call("gpu.release", {"model_id": "model_a"})
    assert result["released"] is True
    ids = [m["id"] for m in gpu_mod._state["loaded_models"]]
    assert "model_a" not in ids


@pytest.mark.asyncio
async def test_gpu_release_returns_false_for_not_loaded():
    result = await _call("gpu.release", {"model_id": "ghost_model"})
    assert result["released"] is False


@pytest.mark.asyncio
async def test_gpu_release_increases_free_vram():
    gpu_mod._state["loaded_models"] = [{"id": "m1", "vram_gb": 9.0, "pending_tasks": 0}]
    before = _free_vram()
    await _call("gpu.release", {"model_id": "m1"})
    after = _free_vram()
    assert after > before


# ── scheduler.queue_status ────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_queue_status_returns_queues():
    gpu_mod._task_queues["model_a"] = ["task1", "task2"]
    result = await _call("scheduler.queue_status", {})
    assert "queues" in result
    assert "model_a" in result["queues"]
    assert result["queues"]["model_a"]["depth"] == 2


# ── scheduler.profile_switch ──────────────────────────

@pytest.mark.asyncio
async def test_scheduler_profile_switch_updates_profile():
    gpu_mod._profiles.append({"name": "balanced"})
    result = await _call("scheduler.profile_switch", {"profile_name": "balanced"})
    assert result["switched"] is True
    assert result["to"] == "balanced"
    assert gpu_mod._state["current_profile"] == "balanced"


@pytest.mark.asyncio
async def test_scheduler_profile_switch_unknown_profile_returns_error():
    result = await _call("scheduler.profile_switch", {"profile_name": "unknown_profile"})
    assert "error" in result


# ── scheduler.degrade ─────────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_degrade_enables_degraded_mode():
    result = await _call("scheduler.degrade", {"enabled": True, "reason": "高負荷"})
    assert result["degraded"] is True
    assert gpu_mod._state["degraded"] is True


@pytest.mark.asyncio
async def test_scheduler_degrade_disables_degraded_mode():
    gpu_mod._state["degraded"] = True
    result = await _call("scheduler.degrade", {"enabled": False})
    assert result["degraded"] is False
    assert gpu_mod._state["degraded"] is False


# ── 不明ツール名 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("gpu.unknown", {})
    assert "error" in result

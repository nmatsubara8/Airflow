"""gpu/server.py の単体テスト（DDD-02 §6 / DDD-07）。"""
from __future__ import annotations

import json

import pytest

import airflow_mcp.gpu.server as gpu_mod
from airflow_mcp.gpu.server import _free_vram
from airflow_mcp.gpu.state_manager import GpuStateManager
from airflow_mcp.scheduler.queue import QueueEntry, SchedulerQueue


@pytest.fixture(autouse=True)
def reset_gpu_state(tmp_path):
    """各テスト前後に GPU 状態をリセット。"""
    gpu_mod._gpu = GpuStateManager(
        vram_available_gb=15.0,
        vram_total_gb=16.0,
        snapshot_path=tmp_path / "gpu_state.json",
    )
    gpu_mod._task_queues.clear()
    gpu_mod._scheduler_queue = SchedulerQueue()
    gpu_mod._profiles.clear()
    gpu_mod._current_profile = None
    gpu_mod._degraded = False
    yield
    gpu_mod._gpu = GpuStateManager(
        vram_available_gb=15.0,
        vram_total_gb=16.0,
        snapshot_path=tmp_path / "gpu_state.json",
    )
    gpu_mod._task_queues.clear()
    gpu_mod._scheduler_queue = SchedulerQueue()
    gpu_mod._profiles.clear()
    gpu_mod._current_profile = None
    gpu_mod._degraded = False


async def _call(name: str, args: dict) -> dict:
    result = await gpu_mod.call_tool(name, args)
    return json.loads(result[0].text)


# ── _free_vram ────────────────────────────────────────

def test_free_vram_with_no_models():
    assert _free_vram() == pytest.approx(15.0)


def test_free_vram_accounts_for_loaded_models():
    gpu_mod._gpu.reserve("m1", vram_gb=9.0)
    assert _free_vram() == pytest.approx(6.0)


# ── gpu.state ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_state_returns_vram_fields():
    result = await _call("gpu.state", {})
    assert "total_vram_gb" in result
    assert "free_vram_gb" in result
    assert "loaded_models" in result
    assert result["total_vram_gb"] == pytest.approx(16.0)


@pytest.mark.asyncio
async def test_gpu_state_snapshot_at_is_present():
    result = await _call("gpu.state", {})
    assert "snapshot_at" in result


# ── gpu.reserve ───────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_reserve_loads_model_into_state():
    result = await _call("gpu.reserve", {"model_id": "model_a"})
    assert result["reserved"] is True
    assert "model_a" in gpu_mod._gpu.loaded_model_ids


@pytest.mark.asyncio
async def test_gpu_reserve_idempotent_for_already_loaded():
    gpu_mod._gpu.reserve("model_a", vram_gb=9.0)
    result = await _call("gpu.reserve", {"model_id": "model_a"})
    assert result["reserved"] is True
    assert gpu_mod._gpu.loaded_model_ids.count("model_a") == 1


@pytest.mark.asyncio
async def test_gpu_reserve_evicts_lru_when_vram_full():
    # VRAM 9.5GB に制限
    gpu_mod._gpu = GpuStateManager(vram_available_gb=9.5, vram_total_gb=16.0)
    gpu_mod._gpu.reserve("old_model", vram_gb=9.0)
    gpu_mod._gpu._loaded["old_model"]["last_used_at"] = "2020-01-01T00:00:00Z"
    result = await _call("gpu.reserve", {"model_id": "new_model"})
    assert result["reserved"] is True
    assert "old_model" in result["evicted_models"]
    assert "new_model" in gpu_mod._gpu.loaded_model_ids


@pytest.mark.asyncio
async def test_gpu_reserve_fails_when_all_models_have_pending_tasks():
    gpu_mod._gpu = GpuStateManager(vram_available_gb=9.5, vram_total_gb=16.0)
    gpu_mod._gpu.reserve("busy_model", vram_gb=9.0)
    gpu_mod._gpu._loaded["busy_model"]["pending_tasks"] = 2
    result = await _call("gpu.reserve", {"model_id": "new_model", "wait": False})
    assert "error" in result
    assert result["error"]["code"] == "GPU_BUSY"


# ── gpu.release ───────────────────────────────────────

@pytest.mark.asyncio
async def test_gpu_release_removes_model_from_state():
    gpu_mod._gpu.reserve("model_a", vram_gb=9.0)
    result = await _call("gpu.release", {"model_id": "model_a"})
    assert result["released"] is True
    assert "model_a" not in gpu_mod._gpu.loaded_model_ids


@pytest.mark.asyncio
async def test_gpu_release_returns_false_for_not_loaded():
    result = await _call("gpu.release", {"model_id": "ghost_model"})
    assert result["released"] is False


@pytest.mark.asyncio
async def test_gpu_release_increases_free_vram():
    gpu_mod._gpu.reserve("m1", vram_gb=9.0)
    before = _free_vram()
    await _call("gpu.release", {"model_id": "m1"})
    after = _free_vram()
    assert after > before


# ── scheduler.queue_status ────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_queue_status_returns_queues():
    gpu_mod._scheduler_queue.enqueue(QueueEntry(task_id="task1", model_id="model_a"))
    gpu_mod._scheduler_queue.enqueue(QueueEntry(task_id="task2", model_id="model_a"))
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
    assert gpu_mod._current_profile == "balanced"


@pytest.mark.asyncio
async def test_scheduler_profile_switch_unknown_profile_returns_error():
    result = await _call("scheduler.profile_switch", {"profile_name": "unknown_profile"})
    assert "error" in result


# ── scheduler.degrade ─────────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_degrade_enables_degraded_mode():
    result = await _call("scheduler.degrade", {"enabled": True, "reason": "高負荷"})
    assert result["degraded"] is True
    assert gpu_mod._degraded is True


@pytest.mark.asyncio
async def test_scheduler_degrade_disables_degraded_mode():
    gpu_mod._degraded = True
    result = await _call("scheduler.degrade", {"enabled": False})
    assert result["degraded"] is False
    assert gpu_mod._degraded is False


# ── get_model_config_with_degraded ────────────────────

def test_model_config_reflects_degraded_flag():
    gpu_mod._degraded = False
    cfg = gpu_mod.get_model_config_with_degraded()
    assert cfg["degraded"] is False

    gpu_mod._degraded = True
    cfg = gpu_mod.get_model_config_with_degraded()
    assert cfg["degraded"] is True


# ── suggest_profile_switch ────────────────────────────

def test_suggest_profile_switch_returns_current_when_no_profiles():
    gpu_mod._profiles.clear()
    gpu_mod._current_profile = "single-14b"
    result = gpu_mod.suggest_profile_switch()
    assert result == "single-14b"


def test_suggest_profile_switch_auto_selects():
    gpu_mod._profiles.extend([
        {"name": "single-14b", "models": ["m14b"], "vram_required_gb": 10},
        {"name": "single-32b", "models": ["m32b"], "vram_required_gb": 15},
    ])
    gpu_mod._current_profile = None
    gpu_mod._scheduler_queue.enqueue(QueueEntry(task_id="t1", model_id="m32b"))
    result = gpu_mod.suggest_profile_switch()
    assert result == "single-32b"


# ── scheduler.queue_status includes degraded ──────────

@pytest.mark.asyncio
async def test_queue_status_includes_degraded_flag():
    gpu_mod._degraded = True
    result = await _call("scheduler.queue_status", {})
    assert result["degraded"] is True


# ── fair_share config ─────────────────────────────────

def test_fair_share_stored_in_module():
    gpu_mod._fair_share.clear()
    gpu_mod._fair_share.update({"model_a": 0.5, "model_b": 0.5})
    assert gpu_mod._fair_share["model_a"] == 0.5


# ── 不明ツール名 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_unknown_tool_returns_error():
    result = await _call("gpu.unknown", {})
    assert "error" in result


# ── _load_config with existing YAML (lines 42-64) ─────

def test_load_config_reads_yaml_file(tmp_path, monkeypatch):
    """config ファイルが存在するときすべてのフィールドが読み込まれること。"""
    cfg_file = tmp_path / "parallel.yaml"
    cfg_file.write_text(
        "scheduler:\n  vram_available_gb: 12.0\n  vram_total_gb: 16.0\n"
        "gpu:\n  pinned_models: []\n"
        "profiles:\n  - name: test-profile\n    models: []\n    vram_required_gb: 12\n"
        "mvp_profile: test-profile\n"
        "starvation_prevention: {}\n"
        "fair_share:\n  model_a: 0.5\n"
    )
    monkeypatch.setattr(gpu_mod, "_PARALLEL_CONFIG", cfg_file)
    gpu_mod._load_config()
    assert gpu_mod._current_profile == "test-profile"
    assert gpu_mod._fair_share.get("model_a") == pytest.approx(0.5)
    assert any(p["name"] == "test-profile" for p in gpu_mod._profiles)


# ── _load_registry_cache return {} (line 72) ──────────

def test_load_registry_cache_returns_empty_when_no_file(monkeypatch):
    """AIRFLOW_REGISTRY が存在しないとき {} を返すこと。"""
    monkeypatch.setenv("AIRFLOW_REGISTRY", "/nonexistent/registry.json")
    result = gpu_mod._load_registry_cache()
    assert result == {}


# ── list_tools() body (line 110) ──────────────────────

@pytest.mark.asyncio
async def test_list_tools_returns_expected_tool_names():
    tools = await gpu_mod.list_tools()
    names = [t.name for t in tools]
    assert "gpu.state" in names
    assert "gpu.reserve" in names
    assert "gpu.release" in names
    assert "scheduler.queue_status" in names
    assert "scheduler.profile_switch" in names
    assert "scheduler.degrade" in names


# ── run() entry point (lines 263-265) ─────────────────

@pytest.mark.asyncio
async def test_run_starts_stdio_server(monkeypatch):
    """run() が _load_config を呼び stdio_server を起動すること。"""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock, patch

    monkeypatch.setattr(gpu_mod, "_PARALLEL_CONFIG", MagicMock(exists=lambda: False))

    @asynccontextmanager
    async def fake_stdio_server():
        yield MagicMock(), MagicMock()

    mock_run = AsyncMock()
    with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_server), \
         patch.object(gpu_mod.server, "run", mock_run):
        await gpu_mod.run()

    mock_run.assert_awaited_once()

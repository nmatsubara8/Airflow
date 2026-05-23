"""gpu/state_manager.py の単体テスト（DDD-07 §9）。"""
from __future__ import annotations

import json

import pytest

from airflow_mcp.common.errors import GpuBusy
from airflow_mcp.gpu.state_manager import GpuStateManager

_REGISTRY = {
    "model_14b": {"vram_gb": 9.0, "avg_load_time_ms": 20_000},
    "model_32b": {"vram_gb": 14.0, "avg_load_time_ms": 40_000},
    "model_bge": {"vram_gb": 1.0, "avg_load_time_ms": 3_000},
}


@pytest.fixture
def mgr(tmp_path) -> GpuStateManager:
    return GpuStateManager(
        vram_available_gb=15.0,
        vram_total_gb=16.0,
        snapshot_path=tmp_path / "gpu_state.json",
    )


@pytest.fixture
def mgr_pinned(tmp_path) -> GpuStateManager:
    return GpuStateManager(
        vram_available_gb=15.0,
        vram_total_gb=16.0,
        pinned_models=["model_bge"],
        snapshot_path=tmp_path / "gpu_state.json",
    )


# ── free_vram ─────────────────────────────────────────

def test_free_vram_full_when_empty(mgr):
    assert mgr.free_vram_gb == pytest.approx(15.0)


def test_free_vram_decreases_after_reserve(mgr):
    mgr.reserve("model_14b", vram_gb=9.0, registry=_REGISTRY)
    assert mgr.free_vram_gb == pytest.approx(6.0)


# ── reserve ───────────────────────────────────────────

def test_reserve_adds_model_to_loaded(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    assert "model_14b" in mgr.loaded_model_ids


def test_reserve_idempotent(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr.reserve("model_14b", registry=_REGISTRY)
    assert mgr.loaded_model_ids.count("model_14b") == 1


def test_reserve_returns_reserved_true(mgr):
    result = mgr.reserve("model_14b", registry=_REGISTRY)
    assert result["reserved"] is True
    assert "loaded_at" in result
    assert result["evicted_models"] == []


def test_reserve_evicts_lru_when_vram_full(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)  # 9GB
    # 14b の last_used_at を古くする（LRU 対象）
    mgr._loaded["model_14b"]["last_used_at"] = "2020-01-01T00:00:00Z"
    result = mgr.reserve("model_32b", registry=_REGISTRY)  # 14GB → 必ず退避
    assert "model_14b" in result["evicted_models"]
    assert "model_32b" in mgr.loaded_model_ids


def test_reserve_raises_gpu_busy_when_all_pending(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr._loaded["model_14b"]["pending_tasks"] = 2
    with pytest.raises(GpuBusy):
        mgr.reserve("model_32b", wait=False, registry=_REGISTRY)


# ── pinned モデル ──────────────────────────────────────

def test_pinned_model_not_evicted(mgr_pinned):
    mgr_pinned.reserve("model_bge", registry=_REGISTRY)    # 1GB pinned
    mgr_pinned.reserve("model_14b", registry=_REGISTRY)    # 9GB
    mgr_pinned._loaded["model_bge"]["last_used_at"] = "2020-01-01T00:00:00Z"
    mgr_pinned._loaded["model_14b"]["last_used_at"] = "2020-01-01T00:00:01Z"
    # 32b のロードに 14GB 必要 → 15 - 1(bge) - 9(14b) = 5GB 不足
    # bge は pinned → 14b だけ退避
    result = mgr_pinned.reserve("model_32b", registry=_REGISTRY)
    evicted = result["evicted_models"]
    assert "model_bge" not in evicted
    assert "model_14b" in evicted


# ── release ───────────────────────────────────────────

def test_release_removes_model(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    result = mgr.release("model_14b")
    assert result["released"] is True
    assert "model_14b" not in mgr.loaded_model_ids


def test_release_restores_vram(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr.release("model_14b")
    assert mgr.free_vram_gb == pytest.approx(15.0)


def test_release_returns_false_for_not_loaded(mgr):
    result = mgr.release("ghost_model")
    assert result["released"] is False


# ── touch / pending ───────────────────────────────────

def test_touch_updates_last_used_at(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    old_ts = mgr._loaded["model_14b"]["last_used_at"]
    import time; time.sleep(0.01)
    mgr.touch("model_14b")
    new_ts = mgr._loaded["model_14b"]["last_used_at"]
    assert new_ts >= old_ts


def test_set_pending_increments(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr.set_pending("model_14b", +1)
    assert mgr._loaded["model_14b"]["pending_tasks"] == 1


def test_set_pending_clamps_at_zero(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr.set_pending("model_14b", -5)
    assert mgr._loaded["model_14b"]["pending_tasks"] == 0


# ── snapshot ──────────────────────────────────────────

def test_snapshot_schema_fields(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    snap = mgr.snapshot()
    assert snap["schema_version"] == "1.0.0"
    assert "snapshot_at" in snap
    assert "total_vram_gb" in snap
    assert "used_vram_gb" in snap
    assert "free_vram_gb" in snap
    assert "loaded_models" in snap


def test_snapshot_persisted_to_file(tmp_path):
    snap_path = tmp_path / "gpu_state.json"
    mgr = GpuStateManager(snapshot_path=snap_path)
    mgr.reserve("model_14b", registry=_REGISTRY)
    assert snap_path.exists()
    data = json.loads(snap_path.read_text())
    assert data["schema_version"] == "1.0.0"


def test_snapshot_used_vram_is_sum_of_loaded(mgr):
    mgr.reserve("model_14b", registry=_REGISTRY)
    mgr.reserve("model_bge", registry=_REGISTRY)
    snap = mgr.snapshot()
    assert snap["used_vram_gb"] == pytest.approx(10.0)

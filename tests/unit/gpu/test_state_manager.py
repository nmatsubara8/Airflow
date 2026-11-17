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
        snapshot_path=tmp_path / "gpu_state_json",
    )


@pytest.fixture
def mgr_pinned(tmp_path) -> GpuStateManager:
    return GpuStateManager(
        vram_available_gb=15.0,
        vram_total_gb=16.0,
        pinned_models=["model_bge"],
        snapshot_path=tmp_path / "gpu_state_json",
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
    import time
    time.sleep(0.01)
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
    snap_path = tmp_path / "gpu_state_json"
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


# ── periodic snapshot ────────────────────────────────

def test_periodic_snapshot_writes_file(tmp_path):
    import time
    snap_path = tmp_path / "periodic_gpu_json"
    mgr = GpuStateManager(
        snapshot_path=snap_path,
        snapshot_interval_s=0.1,
    )
    mgr.reserve("model_14b", registry=_REGISTRY)
    time.sleep(0.25)
    mgr.stop_periodic_snapshot()
    assert snap_path.exists()
    data = json.loads(snap_path.read_text())
    assert data["schema_version"] == "1.0.0"


def test_stop_periodic_snapshot_is_safe_when_not_started(mgr):
    mgr.stop_periodic_snapshot()  # should not raise


# ── _iso_to_duration_ms except branch (lines 31-32) ──

def test_iso_to_duration_ms_returns_zero_for_invalid_string():
    """不正 ISO 文字列のとき 0.0 を返すこと（lines 31-32）。"""
    from airflow_mcp.gpu.state_manager import _iso_to_duration_ms
    assert _iso_to_duration_ms("not-a-date") == pytest.approx(0.0)
    assert _iso_to_duration_ms("") == pytest.approx(0.0)


# ── eviction break early (line 188) ──────────────────

def test_evict_stops_early_when_enough_vram_freed(tmp_path):
    """必要 VRAM 確保済みなら break して残りの候補を退避しないこと（line 188）。"""
    mgr = GpuStateManager(vram_available_gb=15.0, vram_total_gb=16.0,
                          snapshot_path=tmp_path / "s_json")
    mgr.reserve("model_14b", vram_gb=9.0)
    mgr.reserve("model_bge", vram_gb=1.0)
    # model_14b を LRU 最古に設定 → 先に退避される
    mgr._loaded["model_14b"]["last_used_at"] = "2020-01-01T00:00:00Z"
    mgr._loaded["model_bge"]["last_used_at"] = "2021-01-01T00:00:00Z"
    # 7GB の新モデルをロード: model_14b(9GB)退避で充分 → break → model_bge は残る
    result = mgr.reserve("new_model", vram_gb=7.0)
    assert "model_14b" in result["evicted_models"]
    assert "model_bge" not in result["evicted_models"]
    assert "model_bge" in mgr.loaded_model_ids


# ── GpuBusy with wait=True (line 198) ────────────────

def test_reserve_raises_gpu_busy_with_wait_true_gives_10s(tmp_path):
    """wait=True でも VRAM 不足なら GpuBusy(retry_after_ms=10_000)（line 198）。"""
    mgr = GpuStateManager(vram_available_gb=9.5, vram_total_gb=16.0,
                          snapshot_path=tmp_path / "s_json")
    mgr.reserve("model_14b", vram_gb=9.0)
    mgr._loaded["model_14b"]["pending_tasks"] = 2  # 退避不可
    with pytest.raises(GpuBusy) as exc_info:
        mgr.reserve("model_32b", vram_gb=14.0, wait=True)
    assert exc_info.value.retry_after_ms == 10_000


# ── _write_snapshot exception suppressed (lines 210-211)

def test_write_snapshot_suppresses_write_exception(mgr):
    """スナップショット書き込み失敗時にクラッシュしないこと（lines 210-211）。"""
    from pathlib import Path
    from unittest.mock import patch
    with patch.object(Path, "write_text", side_effect=OSError("disk full")):
        mgr._write_snapshot()  # should not raise


def test_free_vram_gb_clamped_to_zero_when_overcommitted(tmp_path):
    """ロード済みモデルが VRAM を超えた場合 free_vram_gb が 0 にクランプされること（lines 66-68）。"""
    mgr2 = GpuStateManager(snapshot_path=tmp_path / "gpu_json", vram_available_gb=8.0)
    # VRAM 合計が available を超えるモデルを直接登録
    mgr2._loaded["model_big"] = {"id": "model_big", "vram_gb": 10.0}
    assert mgr2.free_vram_gb == 0.0

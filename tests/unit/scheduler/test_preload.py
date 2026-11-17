"""scheduler/preload.py の単体テスト（DDD-07 §7）。"""
from __future__ import annotations

import pytest

from airflow_mcp.scheduler.preload import PreloadManager, PreloadStats

_REGISTRY = {
    "model_14b": {"vram_gb": 9.0, "avg_load_time_ms": 20_000},
    "model_32b": {"vram_gb": 14.0, "avg_load_time_ms": 40_000},
}


# ── PreloadStats ──────────────────────────────────────

def test_preload_stats_initial_hit_rate_is_one():
    s = PreloadStats()
    assert s.hit_rate == 1.0


def test_preload_stats_hit_rate_calculation():
    s = PreloadStats(hits=3, misses=1)
    assert s.hit_rate == pytest.approx(0.75)


def test_preload_stats_total():
    s = PreloadStats(hits=2, misses=3)
    assert s.total == 5


# ── PreloadManager.should_preload ─────────────────────

def test_should_not_preload_when_already_loaded():
    m = PreloadManager()
    assert m.should_preload("model_14b", ["model_14b"], 5.0, _REGISTRY, 60_000) is False


def test_should_preload_when_vram_sufficient():
    m = PreloadManager()
    assert m.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True


def test_should_preload_when_time_sufficient_even_if_vram_tight():
    m = PreloadManager()
    # VRAM 不足だが残り時間 > ロード時間
    assert m.should_preload("model_14b", [], 0.0, _REGISTRY, remaining_ms=25_000) is True


def test_should_not_preload_when_time_insufficient():
    m = PreloadManager()
    # 残り時間 < ロード時間（20000ms）
    assert m.should_preload("model_14b", [], 0.0, _REGISTRY, remaining_ms=5_000) is False


def test_should_not_preload_below_hit_rate_threshold():
    m = PreloadManager()
    # 5回以上 miss → hit 率 0.0 < 0.5 → プリロード停止
    for _ in range(6):
        m.record_miss("model_32b")
    assert m.should_preload("model_32b", [], 15.0, _REGISTRY, 60_000) is False


def test_should_preload_when_not_enough_history():
    m = PreloadManager()
    # 4回 miss は閾値未満（5回以上必要）→ プリロード継続
    for _ in range(4):
        m.record_miss("model_14b")
    assert m.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True


# ── hit/miss 記録 ─────────────────────────────────────

def test_record_hit_increments_hits():
    m = PreloadManager()
    m.record_hit("model_14b")
    assert m.get_stats()["model_14b"]["hits"] == 1


def test_record_miss_increments_misses():
    m = PreloadManager()
    m.record_miss("model_14b")
    assert m.get_stats()["model_14b"]["misses"] == 1


def test_get_stats_returns_hit_rate():
    m = PreloadManager()
    m.record_hit("model_14b")
    m.record_hit("model_14b")
    m.record_miss("model_14b")
    stats = m.get_stats()["model_14b"]
    assert stats["hit_rate"] == pytest.approx(2 / 3, rel=0.01)

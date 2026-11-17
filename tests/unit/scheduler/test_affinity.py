"""scheduler/affinity.py の単体テスト（DDD-07 §3）。"""
from __future__ import annotations

import math

import pytest

from airflow_mcp.scheduler.affinity import (
    compute_affinity_score,
    compute_swap_cost,
    select_best_model,
)

_REGISTRY = {
    "model_14b": {"vram_gb": 9.0, "avg_load_time_ms": 20_000},
    "model_32b": {"vram_gb": 14.0, "avg_load_time_ms": 40_000},
    "model_8b":  {"vram_gb": 5.0,  "avg_load_time_ms": 10_000},
}


# ── compute_swap_cost ────────────────────────────────

def test_swap_cost_zero_when_model_loaded():
    cost = compute_swap_cost("model_14b", ["model_14b", "model_32b"], 5.0, _REGISTRY)
    assert cost == 0.0


def test_swap_cost_load_time_when_vram_sufficient():
    cost = compute_swap_cost("model_8b", [], free_vram_gb=10.0, registry=_REGISTRY)
    assert cost == pytest.approx(10_000)


def test_swap_cost_includes_unload_when_vram_insufficient():
    cost = compute_swap_cost("model_32b", [], free_vram_gb=5.0, registry=_REGISTRY)
    # 退避必要: 1500 + 40000 = 41500
    assert cost == pytest.approx(41_500)


def test_swap_cost_uses_default_when_model_not_in_registry():
    cost = compute_swap_cost("unknown_model", [], free_vram_gb=15.0, registry={})
    assert cost > 0  # デフォルトロード時間を返す


# ── compute_affinity_score ───────────────────────────

def test_affinity_score_higher_for_loaded_model():
    task = {"features": {"latency_sensitivity": "batch"}}
    score_loaded = compute_affinity_score(
        task, "model_14b", ["model_14b"], 5.0, _REGISTRY, {}
    )
    score_unloaded = compute_affinity_score(
        task, "model_14b", [], 5.0, _REGISTRY, {}
    )
    assert score_loaded > score_unloaded


def test_swap_cost_infinity_when_model_exceeds_total_vram():
    # モデルが GPU 総 VRAM を超える → 物理的に不可能
    cost = compute_swap_cost(
        "model_32b", [], free_vram_gb=0.0,
        registry=_REGISTRY, total_vram_gb=10.0  # 32b は 14GB 必要
    )
    assert math.isinf(cost)


def test_affinity_score_interactive_latency_weight_doubled():
    task_batch = {"features": {"latency_sensitivity": "batch"}}
    task_interactive = {"features": {"latency_sensitivity": "interactive"}}

    score_batch = compute_affinity_score(
        task_batch, "model_32b", [], free_vram_gb=15.0, registry=_REGISTRY, model_config={}
    )
    score_interactive = compute_affinity_score(
        task_interactive, "model_32b", [], free_vram_gb=15.0, registry=_REGISTRY, model_config={}
    )
    # interactive は latency_weight 2倍 → swap_cost のペナルティが大きい
    assert score_interactive < score_batch


def test_affinity_score_degraded_mode_penalizes_accuracy():
    task = {"features": {"latency_sensitivity": "batch"}}
    score_normal = compute_affinity_score(
        task, "model_14b", ["model_14b"], 5.0, _REGISTRY,
        model_config={"degraded": False}, expected_accuracy=0.9
    )
    score_degraded = compute_affinity_score(
        task, "model_14b", ["model_14b"], 5.0, _REGISTRY,
        model_config={"degraded": True}, expected_accuracy=0.9
    )
    # degraded では accuracy_weight が低いため score も低い
    assert score_degraded < score_normal


def test_affinity_score_starvation_boost_increases_score():
    task = {"features": {}}
    base = compute_affinity_score(
        task, "model_14b", [], 15.0, _REGISTRY, {}, starvation_boost=0.0
    )
    boosted = compute_affinity_score(
        task, "model_14b", [], 15.0, _REGISTRY, {}, starvation_boost=0.5
    )
    assert boosted > base


# ── select_best_model ────────────────────────────────

def test_select_best_model_prefers_loaded():
    task = {"features": {"latency_sensitivity": "batch"}}
    best = select_best_model(
        task,
        candidate_model_ids=["model_14b", "model_32b"],
        loaded_model_ids=["model_14b"],
        free_vram_gb=5.0,
        registry=_REGISTRY,
        model_config={},
    )
    assert best == "model_14b"


def test_select_best_model_returns_none_for_empty_candidates():
    task = {"features": {}}
    best = select_best_model(task, [], [], 15.0, {}, {})
    assert best is None


def test_select_best_model_uses_accuracy_scores():
    task = {"features": {"latency_sensitivity": "batch"}}
    # 32B の方が accuracy スコアが高い
    best = select_best_model(
        task,
        candidate_model_ids=["model_14b", "model_32b"],
        loaded_model_ids=["model_14b", "model_32b"],
        free_vram_gb=0.0,
        registry=_REGISTRY,
        model_config={},
        accuracy_scores={"model_14b": 0.6, "model_32b": 0.95},
    )
    assert best == "model_32b"


# ── compute_affinity_score: swap_ms == inf (line 81) ─

def test_affinity_score_returns_neg_infinity_when_swap_cost_is_infinite():
    """compute_swap_cost が inf を返すときスコアが -inf になること（line 81）。"""
    from unittest.mock import patch
    from airflow_mcp.scheduler.affinity import compute_affinity_score, _INFINITY
    task = {"features": {}}
    with patch("airflow_mcp.scheduler.affinity.compute_swap_cost", return_value=_INFINITY):
        score = compute_affinity_score(
            task,
            model_id="model_32b",
            loaded_model_ids=[],
            free_vram_gb=8.0,
            registry={},
            model_config={},
        )
    assert math.isinf(score) and score < 0


def test_select_best_model_returns_none_when_compute_raises(monkeypatch):
    """compute_affinity_score が例外を送出した場合 select_best_model が None を返すこと（lines 130-132）。"""
    from unittest.mock import patch
    task = {"features": {}}
    with patch("airflow_mcp.scheduler.affinity.compute_affinity_score", side_effect=ValueError("bad")):
        result = select_best_model(
            task,
            candidate_model_ids=["model_14b"],
            loaded_model_ids=[],
            free_vram_gb=15.0,
            registry=_REGISTRY,
            model_config={},
        )
    assert result is None

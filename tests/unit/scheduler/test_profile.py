"""scheduler/profile.py の単体テスト（DDD-07 §4）。"""
from __future__ import annotations

import pytest

from airflow_mcp.scheduler.profile import (
    compute_switch_cost,
    evaluate_profile,
    get_profile_models,
    select_best_profile,
)

_PROFILES = [
    {"name": "single-14b", "models": ["model_14b", "model_bge"], "vram_required_gb": 10},
    {"name": "single-32b", "models": ["model_32b", "model_bge"], "vram_required_gb": 15},
    {"name": "dual-8b",    "models": ["model_8b_a", "model_8b_b"], "vram_required_gb": 8},
]

_REGISTRY = {
    "model_14b": {"vram_gb": 9.0, "avg_load_time_ms": 20_000},
    "model_32b": {"vram_gb": 14.0, "avg_load_time_ms": 40_000},
    "model_bge": {"vram_gb": 1.0, "avg_load_time_ms": 3_000},
    "model_8b_a": {"vram_gb": 4.5, "avg_load_time_ms": 8_000},
    "model_8b_b": {"vram_gb": 4.5, "avg_load_time_ms": 8_000},
}


# ── compute_switch_cost ──────────────────────────────

def test_switch_cost_zero_for_same_profile():
    cost = compute_switch_cost(["model_14b", "model_bge"], ["model_14b", "model_bge"], _REGISTRY)
    assert cost == 0.0


def test_switch_cost_covers_load_plus_unload():
    # 14b → 32b に切り替え: 14b アンロード(1500) + 32b ロード(40000) = 41500
    cost = compute_switch_cost(
        ["model_14b", "model_bge"], ["model_32b", "model_bge"], _REGISTRY
    )
    assert cost == pytest.approx(41_500)


def test_switch_cost_shared_models_not_counted():
    # bge は共通 → アンロード/ロードされない
    cost = compute_switch_cost(["model_14b", "model_bge"], ["model_32b", "model_bge"], _REGISTRY)
    # only: unload 14b (1500) + load 32b (40000) = 41500
    assert cost == pytest.approx(41_500)


# ── evaluate_profile ─────────────────────────────────

def test_evaluate_profile_returns_covered_queue_depth():
    queue_depths = {"model_14b": 5, "model_bge": 2, "model_32b": 3}
    profile = {"name": "single-14b", "models": ["model_14b", "model_bge"]}
    score = evaluate_profile(profile, queue_depths)
    assert score == 7  # 5 + 2


def test_evaluate_profile_zero_when_no_queued_tasks():
    score = evaluate_profile({"models": ["model_14b"]}, {})
    assert score == 0


# ── select_best_profile ──────────────────────────────

def test_select_best_profile_chooses_most_coverage():
    queue_depths = {"model_32b": 10, "model_bge": 1}
    # current=None で切替コスト比較をスキップ → カバレッジ最大のプロファイルを選ぶ
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name=None,
        current_model_ids=[],
        available_vram_gb=15.0,
        registry=_REGISTRY,
    )
    assert best == "single-32b"


def test_select_best_profile_defers_when_switch_too_costly():
    # single-14b → single-32b は 41500ms > 30000ms 閾値 → 現プロファイル継続
    queue_depths = {"model_32b": 10, "model_bge": 1}
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name="single-14b",
        current_model_ids=["model_14b", "model_bge"],
        available_vram_gb=15.0,
        registry=_REGISTRY,
    )
    assert best == "single-14b"


def test_select_best_profile_stays_on_current_when_switch_too_costly():
    queue_depths = {"model_32b": 1}  # 切替の価値が小さい
    # コスト > 30秒 なら現プロファイル継続（32b への切替は 41500ms > 30000ms）
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name="single-14b",
        current_model_ids=["model_14b", "model_bge"],
        available_vram_gb=15.0,
        registry=_REGISTRY,
    )
    assert best == "single-14b"


def test_select_best_profile_excludes_vram_insufficient():
    queue_depths = {"model_32b": 100, "model_14b": 0}
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name=None,
        current_model_ids=[],
        available_vram_gb=10.0,  # 32b プロファイルには 15GB 必要
        registry=_REGISTRY,
    )
    # single-32b は除外 → single-14b か dual-8b から選ぶ
    assert best != "single-32b"


def test_select_best_profile_returns_current_when_no_viable():
    best = select_best_profile(
        _PROFILES, {},
        current_profile_name="single-14b",
        current_model_ids=[],
        available_vram_gb=1.0,  # どのプロファイルも VRAM 不足
        registry=_REGISTRY,
    )
    assert best == "single-14b"


# ── get_profile_models ────────────────────────────────

def test_get_profile_models_returns_correct_list():
    models = get_profile_models(_PROFILES, "single-32b")
    assert "model_32b" in models
    assert "model_bge" in models


def test_get_profile_models_returns_empty_for_unknown():
    models = get_profile_models(_PROFILES, "nonexistent")
    assert models == []


def test_get_profile_models_returns_empty_for_none():
    models = get_profile_models(_PROFILES, None)
    assert models == []


# ── select_best_profile: no switch needed (line 78) ──

def test_select_best_profile_returns_current_when_already_best():
    """best が current と同じときそのまま返すこと（line 78）。"""
    # single-14b が single-32b より深いキューを持つ → best=single-14b == current
    queue_depths = {"model_14b": 10, "model_32b": 0}
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name="single-14b",
        current_model_ids=["model_14b", "model_bge"],
        available_vram_gb=15.0,
        registry=_REGISTRY,
    )
    assert best == "single-14b"


# ── select_best_profile: actual switch (lines 95-99) ─

def test_select_best_profile_switches_when_cost_is_low():
    """スイッチコストが閾値以下のとき実際に切り替わること（lines 95-99）。
    current=single-14b, best=dual-8b, dual-8b のモデルは既にロード済み → cost=0 ≤ 30_000。"""
    queue_depths = {"model_8b_a": 10, "model_8b_b": 8}
    best = select_best_profile(
        _PROFILES, queue_depths,
        current_profile_name="single-14b",
        current_model_ids=["model_8b_a", "model_8b_b"],  # 既にロード済み → cost=0
        available_vram_gb=15.0,
        registry=_REGISTRY,
    )
    assert best == "dual-8b"


# ── 追加テスト: name キー欠損ガード ──────────────────

def test_select_best_profile_profile_without_name_key():
    """profiles に 'name' キーが無い場合も KeyError が起きないこと。"""
    profiles = [
        # "name" キー欠損
        {"vram_required_gb": 8.0, "models": ["model_8b"], "task_types": ["summarize"]},
    ]
    result = select_best_profile(
        profiles=profiles,
        queue_depths={"summarize": 2},
        current_profile_name=None,
        current_model_ids=[],
        available_vram_gb=16.0,
        registry={},
    )
    # name が None でも返り値が str | None の範囲内
    # None が返るか、あるいは何らかの値が返るかどうかに関わらずクラッシュしないこと
    assert result is None or isinstance(result, str)


def test_get_profile_models_profile_without_name_key():
    """profiles に 'name' キーが無い場合も KeyError が起きないこと。"""
    profiles = [
        {"models": ["model_a"], "vram_required_gb": 8.0},  # "name" キーなし
        {"name": "found-profile", "models": ["model_b"], "vram_required_gb": 8.0},
    ]
    result = get_profile_models(profiles, "found-profile")
    assert result == ["model_b"]


def test_get_profile_models_all_without_name_returns_empty():
    """すべての profiles に 'name' キーが無い場合 [] を返すこと。"""
    profiles = [
        {"models": ["model_a"], "vram_required_gb": 8.0},
    ]
    result = get_profile_models(profiles, "target-profile")
    assert result == []


# ── select_best_profile: MoE / RAM offload ───────────────────────

_MOE_PROFILES = [
    {"name": "moe-35b-q3", "models": ["model_moe"], "vram_required_gb": 14, "ram_offload_gb": 0},
    {"name": "moe-35b-q4", "models": ["model_moe_q4"], "vram_required_gb": 16, "ram_offload_gb": 3},
    {"name": "single-14b", "models": ["model_14b"], "vram_required_gb": 10, "ram_offload_gb": 0},
]

_MOE_REGISTRY = {
    "model_moe":    {"vram_gb": 13, "avg_load_time_ms": 12_000, "architecture": "moe"},
    "model_moe_q4": {"vram_gb": 15, "avg_load_time_ms": 20_000, "architecture": "moe"},
    "model_14b":    {"vram_gb": 9,  "avg_load_time_ms": 8_000},
}


def test_moe_q3_selected_when_vram_sufficient():
    """VRAM 15 GB あれば MoE Q3 (14 GB) が選ばれること。"""
    queue_depths = {"model_moe": 5, "model_14b": 2}
    best = select_best_profile(
        _MOE_PROFILES, queue_depths,
        current_profile_name=None, current_model_ids=[],
        available_vram_gb=15.0, registry=_MOE_REGISTRY,
    )
    assert best == "moe-35b-q3"


def test_moe_q4_excluded_when_ram_insufficient():
    """RAM が 2 GB しかない場合、RAM 3 GB オフロード要求の Q4 は除外されること。"""
    queue_depths = {"model_moe_q4": 100, "model_moe": 5}
    best = select_best_profile(
        _MOE_PROFILES, queue_depths,
        current_profile_name=None, current_model_ids=[],
        available_vram_gb=16.0, registry=_MOE_REGISTRY,
        available_ram_gb=2.0,   # RAM 不足
    )
    assert best != "moe-35b-q4"


def test_moe_q4_selected_when_vram_and_ram_sufficient():
    """VRAM 16 GB + RAM 10 GB あれば MoE Q4 (15 VRAM + 3 RAM) が選べること。"""
    queue_depths = {"model_moe_q4": 10, "model_moe": 0, "model_14b": 0}
    best = select_best_profile(
        _MOE_PROFILES, queue_depths,
        current_profile_name=None, current_model_ids=[],
        available_vram_gb=16.0, registry=_MOE_REGISTRY,
        available_ram_gb=10.0,
    )
    assert best == "moe-35b-q4"


def test_fallback_to_14b_when_vram_insufficient_for_moe():
    """VRAM が 12 GB の場合、MoE 系は除外されて single-14b が選ばれること。"""
    queue_depths = {"model_moe": 10, "model_14b": 1}
    best = select_best_profile(
        _MOE_PROFILES, queue_depths,
        current_profile_name=None, current_model_ids=[],
        available_vram_gb=12.0, registry=_MOE_REGISTRY,
    )
    assert best == "single-14b"

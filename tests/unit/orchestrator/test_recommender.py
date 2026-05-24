"""recommender.py の単体テスト（DDD-06 §4〜6）。"""
from __future__ import annotations

import math

import pytest

from airflow_mcp.orchestrator.recommender import (
    _DEFAULT_WEIGHTS,
    _TASK_TYPES,
    _LANGUAGES,
    _cold_start_model,
    encode_features,
    infer_features,
    knn_recommend,
    select_model,
    simple_rule_select,
    weighted_distance,
)
from airflow_mcp.orchestrator.execution_log import compute_auto_score


# ── encode_features ───────────────────────────────────────

def test_encode_features_returns_16_dimensions():
    vec = encode_features({})
    assert len(vec) == len(_TASK_TYPES) + len(_LANGUAGES) + 4


def test_encode_features_task_type_one_hot():
    vec = encode_features({"task_type": "review"})
    idx = _TASK_TYPES.index("review")
    # review の位置が 1.0、他が 0.0
    assert vec[idx] == 1.0
    assert sum(vec[:len(_TASK_TYPES)]) == 1.0


def test_encode_features_unknown_task_type_is_all_zeros():
    vec = encode_features({"task_type": "unknown_type"})
    assert sum(vec[:len(_TASK_TYPES)]) == 0.0


def test_encode_features_language_one_hot():
    vec = encode_features({"language": "en"})
    base = len(_TASK_TYPES)
    idx = _LANGUAGES.index("en")
    assert vec[base + idx] == 1.0
    assert sum(vec[base:base + len(_LANGUAGES)]) == 1.0


def test_encode_features_accuracy_high_maps_to_1():
    vec = encode_features({"required_accuracy": "high"})
    assert vec[-2] == pytest.approx(1.0)


def test_encode_features_accuracy_low_maps_to_0():
    vec = encode_features({"required_accuracy": "low"})
    assert vec[-2] == pytest.approx(0.0)


def test_encode_features_latency_interactive_maps_to_1():
    vec = encode_features({"latency_sensitivity": "interactive"})
    assert vec[-1] == pytest.approx(1.0)


def test_encode_features_input_tokens_log_normalized():
    # 1000トークン → log10(1000)/6 ≈ 0.5
    vec = encode_features({"input_size_tokens": 1000})
    base = len(_TASK_TYPES) + len(_LANGUAGES)
    assert vec[base] == pytest.approx(math.log10(1000) / 6.0, abs=1e-6)


def test_encode_features_same_features_deterministic():
    f = {"task_type": "summarize", "language": "ja", "required_accuracy": "medium"}
    assert encode_features(f) == encode_features(f)


# ── weighted_distance ─────────────────────────────────────

def test_weighted_distance_identical_vectors_is_zero():
    v = [1.0, 0.0, 0.5]
    w = [1.0, 2.0, 0.5]
    assert weighted_distance(v, v, w) == pytest.approx(0.0)


def test_weighted_distance_orthogonal_one_hot():
    # task_type が異なる場合の距離 ≥ 0
    v1 = encode_features({"task_type": "review"})
    v2 = encode_features({"task_type": "summarize"})
    d = weighted_distance(v1, v2, _DEFAULT_WEIGHTS)
    assert d > 0


def test_weighted_distance_same_task_type_closer_than_different():
    base = {"language": "ja", "required_accuracy": "medium"}
    v_ref = encode_features({**base, "task_type": "review"})
    v_same = encode_features({**base, "task_type": "review"})
    v_diff = encode_features({**base, "task_type": "code_gen"})
    d_same = weighted_distance(v_ref, v_same, _DEFAULT_WEIGHTS)
    d_diff = weighted_distance(v_ref, v_diff, _DEFAULT_WEIGHTS)
    assert d_same < d_diff


# ── knn_recommend ─────────────────────────────────────────

def _make_history(model_id: str, task_type: str, count: int, score: float) -> list[dict]:
    return [
        {
            "model_id": model_id,
            "features_snapshot": {"task_type": task_type, "language": "ja",
                                   "required_accuracy": "medium", "latency_sensitivity": "batch"},
            "auto_score": score,
            "success": True,
        }
        for _ in range(count)
    ]


REGISTRY = {
    "model_qwen25_14b_q4": {"preferred_tasks": ["summarize", "review"]},
    "model_qwen25_32b_q3": {"preferred_tasks": ["review", "plan"]},
}

KNN_CFG = {"k": 5, "weights": {
    "task_type": 2.0, "language": 1.5, "required_accuracy": 1.0,
    "latency_sensitivity": 0.5, "input_size_tokens": 0.3, "expected_output_size_tokens": 0.3
}}


def test_knn_recommend_empty_history_returns_empty():
    recs = knn_recommend({"task_type": "summarize"}, [], list(REGISTRY.keys()), KNN_CFG)
    assert recs == []


def test_knn_recommend_returns_higher_score_model_first():
    # k=10 を使い両モデルが必ず top-k に入るようにする
    cfg_k10 = {**KNN_CFG, "k": 10}
    history = (
        _make_history("model_qwen25_32b_q3", "review", 5, 0.9)
        + _make_history("model_qwen25_14b_q4", "review", 5, 0.7)
    )
    recs = knn_recommend({"task_type": "review"}, history, list(REGISTRY.keys()), cfg_k10)
    assert len(recs) == 2, f"両モデルが推奨されるべき: {recs}"
    assert recs[0]["model_id"] == "model_qwen25_32b_q3"
    assert recs[0]["score"] > recs[1]["score"]


def test_knn_recommend_confidence_proportional_to_sample_count():
    history = _make_history("model_qwen25_14b_q4", "summarize", 10, 0.8)
    recs = knn_recommend({"task_type": "summarize"}, history, list(REGISTRY.keys()), KNN_CFG)
    model_rec = next(r for r in recs if r["model_id"] == "model_qwen25_14b_q4")
    assert model_rec["confidence"] == pytest.approx(min(1.0, model_rec["sample_count"] / 10.0))


def test_knn_recommend_k_limits_neighbors():
    # k=3 の場合、最近傍 3 件のみ使う
    cfg = {**KNN_CFG, "k": 3}
    history = (
        _make_history("model_qwen25_14b_q4", "summarize", 5, 1.0)
        + _make_history("model_qwen25_32b_q3", "review", 5, 0.1)
    )
    recs = knn_recommend({"task_type": "summarize"}, history, list(REGISTRY.keys()), cfg)
    assert recs[0]["model_id"] == "model_qwen25_14b_q4"


# ── compute_auto_score ────────────────────────────────────

def test_auto_score_success_no_retry_is_0_7():
    assert compute_auto_score(True, 0) == pytest.approx(0.7)


def test_auto_score_failure_is_0():
    assert compute_auto_score(False, 0) == pytest.approx(0.0)


def test_auto_score_retry_reduces_score():
    score_0 = compute_auto_score(True, 0)
    score_2 = compute_auto_score(True, 2)
    assert score_2 < score_0
    assert score_2 == pytest.approx(0.5, abs=0.01)


def test_auto_score_accepted_adds_0_1():
    assert compute_auto_score(True, 0, "accepted") == pytest.approx(0.8)


def test_auto_score_rejected_subtracts_0_5():
    assert compute_auto_score(True, 0, "rejected") == pytest.approx(0.2)


def test_auto_score_clamped_to_0_1():
    # 多数のリトライで 0 以下になっても clamp
    assert compute_auto_score(True, 10) == pytest.approx(0.0)
    assert compute_auto_score(True, 0, "accepted") <= 1.0


# ── select_model ──────────────────────────────────────────

MODEL_CONFIG = {
    "selection": {"default_mode": "recommend", "exploration_rate": 0.1,
                  "min_samples_for_auto": 20},
    "knn": KNN_CFG,
}


def test_select_model_cold_start_returns_valid_model():
    task = {"features": {"task_type": "summarize", "required_accuracy": "medium"}}
    model_id, etype = select_model(task, MODEL_CONFIG, REGISTRY, history=[])
    assert model_id in REGISTRY
    assert etype == "cold_start_fallback"


def test_select_model_recommend_mode_uses_knn():
    history = _make_history("model_qwen25_32b_q3", "review", 10, 0.95)
    task = {"features": {"task_type": "review", "language": "ja",
                          "required_accuracy": "medium", "latency_sensitivity": "batch"}}
    model_id, etype = select_model(task, MODEL_CONFIG, REGISTRY, history=history)
    assert model_id == "model_qwen25_32b_q3"
    assert etype is None


def test_select_model_epsilon_greedy_sometimes_explores():
    # k=20 で全履歴が近傍に入り、total_samples >= min_samples_for_auto を確保
    cfg_k20 = {**KNN_CFG, "k": 20}
    history = (
        _make_history("model_qwen25_14b_q4", "summarize", 10, 0.9)
        + _make_history("model_qwen25_32b_q3", "summarize", 10, 0.5)
    )
    task = {"features": {"task_type": "summarize", "language": "ja",
                          "required_accuracy": "medium", "latency_sensitivity": "batch"}}
    auto_config = {
        "selection": {
            "default_mode": "auto",
            "exploration_rate": 1.0,   # 必ず探索
            "min_samples_for_auto": 5, # total_samples(20) > 5 → 降格しない
        },
        "knn": cfg_k20,
    }
    model_id, etype = select_model(task, auto_config, REGISTRY, history=history)
    assert etype == "epsilon_greedy"
    assert model_id != "model_qwen25_14b_q4"  # top model 以外が選ばれる


def test_select_model_auto_downgraded_when_insufficient_samples():
    # 5〜19 件のみ → auto が recommend に降格
    history = _make_history("model_qwen25_14b_q4", "summarize", 10, 0.8)
    task = {"features": {"task_type": "summarize"}}
    auto_config = {**MODEL_CONFIG, "selection": {
        **MODEL_CONFIG["selection"], "default_mode": "auto",
        "exploration_rate": 0.0,  # greedy 発動しない
        "min_samples_for_auto": 20,
    }}
    # 降格後は recommend として動作 → exploration_type は None
    model_id, etype = select_model(task, auto_config, REGISTRY, history=history)
    assert etype is None


# ── simple_rule_select ────────────────────────────────────

def test_simple_rule_high_accuracy_batch_returns_32b():
    task = {"features": {"required_accuracy": "high", "latency_sensitivity": "batch"}}
    assert simple_rule_select(task) == "model_qwen25_32b_q3"


def test_simple_rule_medium_accuracy_returns_14b():
    task = {"features": {"required_accuracy": "medium"}}
    assert simple_rule_select(task) == "model_qwen25_14b_q4"


def test_simple_rule_empty_features_returns_14b():
    assert simple_rule_select({}) == "model_qwen25_14b_q4"


# ── knn_recommend: empty features_snapshot (line 96) ─

def test_knn_recommend_skips_records_with_empty_snapshot():
    """features_snapshot が空のレコードをスキップすること（line 96）。"""
    history = [
        {"features_snapshot": {}, "model_id": "model_qwen25_14b_q4", "auto_score": 0.8},
        *_make_history("model_qwen25_14b_q4", "summarize", count=5, score=0.8),
    ]
    recs = knn_recommend(
        {"task_type": "summarize"}, history, list(REGISTRY.keys()), KNN_CFG
    )
    # 空スナップショットはスキップ → 残り5件で推薦が返ること
    assert any(r["model_id"] == "model_qwen25_14b_q4" for r in recs)


# ── _cold_start_model branches (lines 144-145, 153) ──

def test_cold_start_model_returns_preferred_model_in_registry():
    """preferred_models に registry 存在モデルがあれば即返すこと（lines 144-145）。"""
    features = {"preferred_models": ["model_qwen25_32b_q3"]}
    result = _cold_start_model(features, REGISTRY, MODEL_CONFIG)
    assert result == "model_qwen25_32b_q3"


# ── infer_features ────────────────────────────────────────

def test_infer_features_task_type_from_tag():
    task = {"tags": ["review", "api"], "features": {}}
    result = infer_features(task)
    assert result["task_type"] == "review"


def test_infer_features_code_tag_maps_to_code_gen():
    task = {"tags": ["code", "backend"], "features": {}}
    result = infer_features(task)
    assert result["task_type"] == "code_gen"


def test_infer_features_required_accuracy_from_section_business():
    task = {"section": "business", "features": {}}
    result = infer_features(task)
    assert result["required_accuracy"] == "high"


def test_infer_features_required_accuracy_from_section_legal():
    task = {"section": "legal", "features": {}}
    result = infer_features(task)
    assert result["required_accuracy"] == "high"


# ── knn_recommend: swap_cost_ms ─────────────────────────────

_REGISTRY_WITH_VRAM = {
    "model_qwen25_14b_q4": {"vram_gb": 9.0, "avg_load_time_ms": 20_000, "preferred_tasks": ["summarize"]},
    "model_qwen25_32b_q3": {"vram_gb": 14.0, "avg_load_time_ms": 40_000, "preferred_tasks": ["review"]},
}


def test_knn_recommend_swap_cost_zero_when_model_loaded():
    """ロード済みモデルは swap_cost_ms=0 で返る。"""
    history = _make_history("model_qwen25_14b_q4", "summarize", 5, 0.8)
    recs = knn_recommend(
        {"task_type": "summarize"}, history,
        list(_REGISTRY_WITH_VRAM.keys()), KNN_CFG,
        loaded_model_ids=["model_qwen25_14b_q4"],
        free_vram_gb=6.0,
        registry=_REGISTRY_WITH_VRAM,
    )
    rec = next(r for r in recs if r["model_id"] == "model_qwen25_14b_q4")
    assert rec["swap_cost_ms"] == 0


def test_knn_recommend_swap_cost_nonzero_when_model_not_loaded():
    """未ロードモデルは avg_load_time_ms 相当のコストが返る。"""
    history = _make_history("model_qwen25_14b_q4", "summarize", 5, 0.8)
    recs = knn_recommend(
        {"task_type": "summarize"}, history,
        list(_REGISTRY_WITH_VRAM.keys()), KNN_CFG,
        loaded_model_ids=[],
        free_vram_gb=15.0,
        registry=_REGISTRY_WITH_VRAM,
    )
    rec = next(r for r in recs if r["model_id"] == "model_qwen25_14b_q4")
    assert rec["swap_cost_ms"] > 0


def test_knn_recommend_swap_cost_defaults_to_zero_without_registry():
    """registry を渡さない場合は後方互換で swap_cost_ms=0。"""
    history = _make_history("model_qwen25_14b_q4", "summarize", 5, 0.8)
    recs = knn_recommend(
        {"task_type": "summarize"}, history,
        list(REGISTRY.keys()), KNN_CFG,
    )
    for r in recs:
        assert r["swap_cost_ms"] == 0


def test_select_model_passes_gpu_state_to_knn():
    """loaded_model_ids を渡すと推奨結果の swap_cost_ms に反映される。"""
    history = _make_history("model_qwen25_14b_q4", "summarize", 10, 0.8)
    model_id, _ = select_model(
        {"features": {"task_type": "summarize"}},
        MODEL_CONFIG,
        _REGISTRY_WITH_VRAM,
        history,
        loaded_model_ids=["model_qwen25_14b_q4"],
        free_vram_gb=6.0,
    )
    assert model_id == "model_qwen25_14b_q4"


def test_infer_features_latency_from_urgent_tag():
    task = {"tags": ["urgent"], "features": {}}
    result = infer_features(task)
    assert result["latency_sensitivity"] == "interactive"


def test_infer_features_does_not_overwrite_existing():
    task = {"tags": ["review"], "section": "business",
            "features": {"task_type": "summarize", "required_accuracy": "medium"}}
    result = infer_features(task)
    assert result["task_type"] == "summarize"
    assert result["required_accuracy"] == "medium"


def test_infer_features_empty_task_returns_empty_extra():
    task = {}
    result = infer_features(task)
    assert isinstance(result, dict)


def test_cold_start_model_falls_back_to_default_when_no_match():
    """preferred_models なし + task_type 不一致のときデフォルトを返すこと（line 153）。"""
    features = {"task_type": "nonexistent_type_xyz"}
    cfg = {**MODEL_CONFIG, "selection": {"default_model": "model_qwen25_14b_q4"}}
    # REGISTRY に "nonexistent_type_xyz" を preferred_tasks に持つモデルはない
    registry_no_match = {k: {**v, "preferred_tasks": ["review"]} for k, v in REGISTRY.items()}
    result = _cold_start_model(features, registry_no_match, cfg)
    assert result == "model_qwen25_14b_q4"

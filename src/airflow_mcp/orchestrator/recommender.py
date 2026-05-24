"""DDD-06 §4: KNN ベースモデル推奨・選択ロジック。

エントリポイント:
    select_model(task, model_config, registry, history, log_dir) -> (model_id, exploration_type)

KNN 特徴ベクトル（16 次元）:
    task_type        ×8  (one-hot)
    language         ×4  (one-hot)
    input_size_tokens    ×1  (log10 正規化)
    expected_output_size_tokens ×1  (log10 正規化)
    required_accuracy    ×1  (0.0/0.5/1.0)
    latency_sensitivity  ×1  (0.0/0.5/1.0)
"""
from __future__ import annotations

import logging
import math
import random
from typing import Any

from ..common.audit import log_event, new_audit_id
from ..common.feature_extractor import (
    ACCURACY_MAP as _ACCURACY_MAP,
    LANGUAGES as _LANGUAGES,
    LATENCY_MAP as _LATENCY_MAP,
    TASK_TYPES as _TASK_TYPES,
    extract_features,
)
from ..common.thresholds import (
    DEFAULT_MODEL_ID,
    DEFAULT_MODEL_32B_ID,
    EPSILON_GREEDY_RATE,
    KNN_COLD_START_MIN_SAMPLES,
    KNN_CONFIDENCE_SCALE,
)
from ..scheduler.affinity import compute_swap_cost

logger = logging.getLogger(__name__)

# 特徴ベクトルの各次元に対応するデフォルト重み
_DEFAULT_WEIGHTS: list[float] = (
    [2.0] * len(_TASK_TYPES)      # task_type (8次元)
    + [1.5] * len(_LANGUAGES)     # language  (4次元)
    + [0.3, 0.3]                   # input/output size
    + [1.0, 0.5]                   # required_accuracy, latency_sensitivity
)


def encode_features(features: dict[str, Any]) -> list[float]:
    """タスク特徴を 16 次元の数値ベクトルに変換（DDD-06 §4.2）。"""
    vec: list[float] = []
    task_type = features.get("task_type", "document")
    vec.extend(1.0 if t == task_type else 0.0 for t in _TASK_TYPES)
    language = features.get("language", "ja")
    vec.extend(1.0 if la == language else 0.0 for la in _LANGUAGES)
    input_tokens = max(1, features.get("input_size_tokens", 1000))
    vec.append(math.log10(input_tokens) / 6.0)
    output_tokens = max(1, features.get("expected_output_size_tokens", 200))
    vec.append(math.log10(output_tokens) / 6.0)
    vec.append(_ACCURACY_MAP.get(features.get("required_accuracy", "medium"), 0.5))
    vec.append(_LATENCY_MAP.get(features.get("latency_sensitivity", "batch"), 0.5))
    return vec


def weighted_distance(v1: list[float], v2: list[float], weights: list[float]) -> float:
    """重み付きユークリッド距離（DDD-06 §4.2）。"""
    return math.sqrt(sum(w * (a - b) ** 2 for w, a, b in zip(weights, v1, v2)))


def _build_weights(knn_config: dict[str, Any]) -> list[float]:
    """model-config.yaml の knn.weights をベクトル重みに展開。"""
    w = knn_config.get("weights", {})
    return (
        [w.get("task_type", 2.0)] * len(_TASK_TYPES)
        + [w.get("language", 1.5)] * len(_LANGUAGES)
        + [w.get("input_size_tokens", 0.3), w.get("expected_output_size_tokens", 0.3)]
        + [w.get("required_accuracy", 1.0), w.get("latency_sensitivity", 0.5)]
    )


def knn_recommend(
    features: dict[str, Any],
    history: list[dict[str, Any]],
    available_model_ids: list[str],
    knn_config: dict[str, Any],
    *,
    loaded_model_ids: list[str] | None = None,
    free_vram_gb: float = 0.0,
    registry: dict[str, dict] | None = None,
    total_vram_gb: float | None = None,
) -> list[dict[str, Any]]:
    """KNN で推奨モデルリストを返す（DDD-06 §4.3）。

    loaded_model_ids / free_vram_gb / registry を渡すと swap_cost_ms を
    compute_swap_cost() で正確に計算する。省略時は 0。

    Returns:
        List of {"model_id", "score", "confidence", "sample_count", "reason"}
        score 降順。履歴がない場合は空リスト。
    """
    if not history:
        return []

    k = knn_config.get("k", 5)
    weights = _build_weights(knn_config)
    query_vec = encode_features(features)

    # 距離計算
    neighbors: list[tuple[float, dict]] = []
    for record in history:
        snap = record.get("features_snapshot", {})
        if not snap:
            continue
        hist_vec = encode_features(snap)
        dist = weighted_distance(query_vec, hist_vec, weights)
        neighbors.append((dist, record))

    neighbors.sort(key=lambda x: x[0])
    top_k = [rec for _, rec in neighbors[:k]]

    # モデルごとに集計
    model_scores: dict[str, list[float]] = {}
    for rec in top_k:
        mid = rec.get("model_id")
        score = rec.get("auto_score", 0.0)
        if mid:
            model_scores.setdefault(mid, []).append(score)

    recommendations: list[dict[str, Any]] = []
    for model_id in available_model_ids:
        scores = model_scores.get(model_id, [])
        if not scores:
            continue
        mean_score = sum(scores) / len(scores)
        confidence = min(1.0, len(scores) / KNN_CONFIDENCE_SCALE)
        swap_ms = (
            compute_swap_cost(model_id, loaded_model_ids or [], free_vram_gb, registry,
                              total_vram_gb=total_vram_gb)
            if registry is not None
            else 0
        )
        recommendations.append({
            "model_id": model_id,
            "score": round(mean_score, 4),
            "expected_accuracy": round(mean_score, 4),
            "swap_cost_ms": round(swap_ms),
            "confidence": round(confidence, 4),
            "sample_count": len(scores),
            "reason": f"過去 {len(scores)} 件の類似タスクで平均スコア {mean_score:.2f}",
        })

    recommendations.sort(key=lambda x: x["score"], reverse=True)
    return recommendations


def _cold_start_model(
    features: dict[str, Any],
    registry: dict[str, dict],
    model_config: dict[str, Any],
) -> str:
    """コールドスタート時のフォールバック選択（DDD-06 §6.2）。

    features.preferred_models → registry preferred_tasks マッチ → デフォルト 14B。
    """
    preferred = features.get("preferred_models", [])
    for mid in preferred:
        if mid in registry:
            return mid

    # task_type マッチング
    task_type = features.get("task_type", "document")
    for model_id, meta in registry.items():
        if task_type in meta.get("preferred_tasks", []):
            return model_id

    return model_config.get("selection", {}).get("default_model", DEFAULT_MODEL_ID)


def select_model(
    task: dict[str, Any],
    model_config: dict[str, Any],
    registry: dict[str, dict],
    history: list[dict[str, Any]],
    *,
    loaded_model_ids: list[str] | None = None,
    free_vram_gb: float = 0.0,
    total_vram_gb: float | None = None,
) -> tuple[str, str | None]:
    """タスクに対して最適モデルを選択する（DDD-06 §4〜6 を統合）。

    loaded_model_ids / free_vram_gb / total_vram_gb を渡すと swap_cost_ms が正確に計算される。

    Returns:
        (model_id, exploration_type)
        exploration_type: None | "epsilon_greedy" | "cold_start_fallback"
    """
    features = task.get("features", {})
    sel_config = model_config.get("selection", {})
    knn_config = model_config.get("knn", {})
    mode = sel_config.get("default_mode", "recommend")
    exploration_rate = sel_config.get("exploration_rate", EPSILON_GREEDY_RATE)
    min_samples = sel_config.get("min_samples_for_auto", 20)

    # embedding モデルを除外（推薦対象外）
    available = [
        mid for mid, meta in registry.items()
        if "embed" not in meta.get("preferred_tasks", [])
    ]

    # KNN 推奨
    recommendations = knn_recommend(
        features, history, available, knn_config,
        loaded_model_ids=loaded_model_ids,
        free_vram_gb=free_vram_gb,
        registry=registry,
        total_vram_gb=total_vram_gb,
    )
    total_samples = sum(r["sample_count"] for r in recommendations)

    # コールドスタート（サンプル不足）
    if total_samples < KNN_COLD_START_MIN_SAMPLES or not recommendations:
        fallback_id = _cold_start_model(features, registry, model_config)
        aid = new_audit_id()
        log_event("info", "model.cold_start_fallback", "orchestrator", aid,
                  data={"model_id": fallback_id, "sample_count": total_samples,
                        "features": features})
        logger.info("cold_start_fallback → %s (samples=%d)", fallback_id, total_samples)
        return fallback_id, "cold_start_fallback"

    top_model = recommendations[0]["model_id"]

    # Auto モード降格チェック
    if mode == "auto" and total_samples < min_samples:
        aid = new_audit_id()
        log_event("warn", "model.auto_downgraded", "orchestrator", aid,
                  data={"reason": f"insufficient_samples ({total_samples} < {min_samples})"})
        logger.warning("auto_downgraded to recommend (samples=%d)", total_samples)
        mode = "recommend"

    # ε-greedy 探索（Auto モード）
    if mode == "auto" and random.random() < exploration_rate:
        others = [mid for mid in available if mid != top_model]
        if others:
            chosen = random.choice(others)
            aid = new_audit_id()
            log_event("info", "model.explored", "orchestrator", aid,
                      data={"model_id": chosen, "reason": "epsilon_greedy",
                            "exploration_rate": exploration_rate})
            logger.info("epsilon_greedy exploration → %s", chosen)
            return chosen, "epsilon_greedy"

    return top_model, None


def infer_features(task: dict[str, Any]) -> dict[str, Any]:
    """タスクフィールドから不足特徴を自動推論する（DDD-06 §3.2）。

    common.feature_extractor.extract_features への互換シム。
    features dict のみを返す（inferred_fields は捨てる）。
    """
    features, _ = extract_features(task)
    return features


def simple_rule_select(task: dict[str, Any]) -> str:
    """履歴なし・Phase 2 固定ルール（コールドスタート最終フォールバック）。

    DDD-06 §6.1: サンプル数 < 5 のコールドスタートで preferred_models もなければここに来る。
    """
    features = task.get("features", {})
    accuracy = features.get("required_accuracy", "medium")
    latency = features.get("latency_sensitivity", "batch")
    if accuracy == "high" and latency != "interactive":
        return DEFAULT_MODEL_32B_ID
    return DEFAULT_MODEL_ID

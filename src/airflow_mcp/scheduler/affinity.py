"""親和性スコア計算（DDD-07 §3）。

total_score = expected_accuracy × accuracy_weight
            - swap_cost_normalized × latency_weight
            + starvation_boost
"""
from __future__ import annotations

import math
from typing import Any

_INFINITY = math.inf


def compute_swap_cost(
    model_id: str,
    loaded_model_ids: list[str],
    free_vram_gb: float,
    registry: dict[str, dict],
    total_vram_gb: float | None = None,
) -> float:
    """swap_cost_ms を算出する（DDD-07 §3.2）。

    Returns:
        ミリ秒。ロード済みなら 0。物理的に実行不可なら inf。
    """
    if model_id in loaded_model_ids:
        return 0.0

    model_meta = registry.get(model_id, {})
    vram_needed = model_meta.get("vram_gb", 9.0)
    load_time = model_meta.get("avg_load_time_ms", 30_000)

    # プロファイル上で物理的に不可能（モデル自体が総 VRAM を超える）
    if total_vram_gb is not None and vram_needed > total_vram_gb:
        return _INFINITY

    if free_vram_gb >= vram_needed:
        return float(load_time)

    # 退避が必要な場合: 退避時間 + ロード時間
    UNLOAD_TIME_MS = 1_500
    return float(UNLOAD_TIME_MS + load_time)


def compute_affinity_score(
    task: dict[str, Any],
    model_id: str,
    loaded_model_ids: list[str],
    free_vram_gb: float,
    registry: dict[str, dict],
    model_config: dict[str, Any],
    expected_accuracy: float = 0.75,
    starvation_boost: float = 0.0,
    total_vram_gb: float | None = None,
) -> float:
    """タスク×モデルの親和性スコア（DDD-07 §3.1）。

    スコアが高いほど優先される。
    """
    features = task.get("features", {})
    is_interactive = features.get("latency_sensitivity") == "interactive"
    degraded = model_config.get("degraded", False)

    if degraded:
        accuracy_w = 0.3
        latency_w = 0.7
    else:
        accuracy_w = 0.7
        latency_w = 0.3

    if is_interactive:
        latency_w *= 2.0
        # 正規化のため accuracy_w も調整
        total = accuracy_w + latency_w
        accuracy_w /= total
        latency_w /= total

    swap_ms = compute_swap_cost(model_id, loaded_model_ids, free_vram_gb, registry,
                                total_vram_gb=total_vram_gb)

    if swap_ms == _INFINITY:
        return -_INFINITY

    # swap_cost を 0-1 に正規化（60秒 = 1.0 とする）
    swap_normalized = min(swap_ms / 60_000, 1.0)

    return (
        expected_accuracy * accuracy_w
        - swap_normalized * latency_w
        + starvation_boost
    )


def select_best_model(
    task: dict[str, Any],
    candidate_model_ids: list[str],
    loaded_model_ids: list[str],
    free_vram_gb: float,
    registry: dict[str, dict],
    model_config: dict[str, Any],
    accuracy_scores: dict[str, float] | None = None,
    starvation_boosts: dict[str, float] | None = None,
    total_vram_gb: float | None = None,
) -> str | None:
    """candidate_model_ids の中で最高親和性スコアのモデルを返す。"""
    if not candidate_model_ids:
        return None

    acc = accuracy_scores or {}
    boosts = starvation_boosts or {}

    scored = [
        (
            model_id,
            compute_affinity_score(
                task=task,
                model_id=model_id,
                loaded_model_ids=loaded_model_ids,
                free_vram_gb=free_vram_gb,
                registry=registry,
                model_config=model_config,
                expected_accuracy=acc.get(model_id, 0.75),
                starvation_boost=boosts.get(model_id, 0.0),
                total_vram_gb=total_vram_gb,
            ),
        )
        for model_id in candidate_model_ids
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    best_id, best_score = scored[0]
    return best_id if best_score > -_INFINITY else None

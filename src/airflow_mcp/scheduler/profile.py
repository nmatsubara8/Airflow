"""プロファイル管理（DDD-07 §4）。

利用可能なモデルプロファイルの中から最適なものを選択する。
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_SWITCH_COST_THRESHOLD_MS = 30_000  # 30 秒


def compute_switch_cost(
    current_model_ids: list[str],
    target_model_ids: list[str],
    registry: dict[str, dict],
) -> float:
    """プロファイル切替コスト（ms）を算出する（DDD-07 §4.2）。

    共通モデルはコスト 0。差分モデルは unload + load。
    """
    to_unload = set(current_model_ids) - set(target_model_ids)
    to_load = set(target_model_ids) - set(current_model_ids)

    cost_ms = 0.0
    UNLOAD_MS = 1_500

    for model_id in to_unload:
        cost_ms += UNLOAD_MS

    for model_id in to_load:
        meta = registry.get(model_id, {})
        cost_ms += meta.get("avg_load_time_ms", 30_000)

    return cost_ms


def evaluate_profile(
    profile: dict[str, Any],
    queue_depths: dict[str, int],
) -> float:
    """プロファイルのカバー件数スコアを算出する（DDD-07 §4.2）。"""
    return sum(queue_depths.get(m, 0) for m in profile.get("models", []))


def select_best_profile(
    profiles: list[dict[str, Any]],
    queue_depths: dict[str, int],
    current_profile_name: str | None,
    current_model_ids: list[str],
    available_vram_gb: float,
    registry: dict[str, dict],
) -> str | None:
    """最適プロファイル名を返す（DDD-07 §4.2）。

    切替コストが閾値を超える場合は現プロファイルを継続。
    Returns:
        プロファイル名。変更不要なら current_profile_name を返す。
    """
    viable = [
        p for p in profiles
        if p.get("vram_required_gb", 0) <= available_vram_gb
    ]
    if not viable:
        return current_profile_name

    scored = sorted(
        viable,
        key=lambda p: evaluate_profile(p, queue_depths),
        reverse=True,
    )
    best = scored[0]
    best_name = best.get("name")

    if best_name == current_profile_name:
        return current_profile_name

    # 初回選択（current=None）はコストチェックなし
    if current_profile_name is None:
        logger.info("profile.initial_select → %s", best_name)
        return best_name

    switch_cost = compute_switch_cost(
        current_model_ids, best.get("models", []), registry
    )
    if switch_cost > _SWITCH_COST_THRESHOLD_MS:
        logger.info(
            "profile.switch_deferred current=%s candidate=%s cost_ms=%.0f",
            current_profile_name, best_name, switch_cost,
        )
        return current_profile_name

    logger.info(
        "profile.switch %s → %s (cost_ms=%.0f)",
        current_profile_name, best_name, switch_cost,
    )
    return best_name


def get_profile_models(
    profiles: list[dict[str, Any]], profile_name: str | None
) -> list[str]:
    """プロファイル名に対応するモデルリストを返す。"""
    if not profile_name:
        return []
    for p in profiles:
        if p.get("name") == profile_name:
            return list(p.get("models", []))
    return []

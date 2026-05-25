"""スターベーション防止（DDD-07 §6）。

Aging / Deadline / force_run の判定と aging スコア算出。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _wait_seconds(enqueued_at: datetime) -> float:
    return max(0.0, (_now() - enqueued_at).total_seconds())


def compute_aging_score(enqueued_at: datetime, config: dict[str, Any]) -> float:
    """待機時間ブースト値を返す（DDD-07 §6.1）。

    max_wait_before_boost_s 秒以上待っていれば、1 分ごとに aging_factor ずつ増加。
    """
    boost_threshold = max(0.0, config.get("max_wait_before_boost_s", 600))
    aging_factor = max(0.0, config.get("aging_factor", 0.1))

    wait = _wait_seconds(enqueued_at)
    if wait < boost_threshold:
        return 0.0
    return (wait - boost_threshold) / 60.0 * aging_factor


def check_starvation_flags(
    enqueued_at: datetime,
    deadline: datetime | None,
    config: dict[str, Any],
) -> list[str]:
    """スターベーションフラグを返す（DDD-07 §6.2 / §6.3）。

    Returns:
        フラグのリスト: "force_run" / "deadline_near" / "aging"
    """
    flags: list[str] = []
    wait = _wait_seconds(enqueued_at)

    max_wait = max(0.0, config.get("force_run_s", 3600))
    if wait >= max_wait:
        flags.append("force_run")
        return flags  # force_run は最上位、以下チェック不要

    if deadline is not None:
        deadline_threshold = max(0.0, config.get("deadline_warn_s", 300))
        time_to_deadline = (deadline - _now()).total_seconds()
        if time_to_deadline <= deadline_threshold:
            flags.append("deadline_near")

    boost_threshold = max(0.0, config.get("max_wait_before_boost_s", 600))
    if wait >= boost_threshold:
        flags.append("aging")

    return flags


def is_force_priority(flags: list[str]) -> bool:
    """force_run または deadline_near なら swap_cost を無視して優先実行すべきか。"""
    return "force_run" in flags or "deadline_near" in flags

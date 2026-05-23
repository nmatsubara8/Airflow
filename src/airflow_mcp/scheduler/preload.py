"""投機的プリロード（DDD-07 §7）。

hit/miss 統計とプリロード判定ロジック。
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_MIN_HIT_RATE = 0.5  # hit 率がこれを下回るパターンはプリロード停止


@dataclass
class PreloadStats:
    """モデル別プリロード統計。"""
    hits: int = 0
    misses: int = 0

    @property
    def total(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        return self.hits / self.total if self.total > 0 else 1.0


class PreloadManager:
    """投機的プリロードの判定・統計管理（DDD-07 §7）。"""

    def __init__(self) -> None:
        self._stats: dict[str, PreloadStats] = defaultdict(PreloadStats)
        self._pending_preloads: dict[str, str] = {}  # model_id → "for whom" (task context)

    def should_preload(
        self,
        model_id: str,
        loaded_model_ids: list[str],
        free_vram_gb: float,
        registry: dict[str, dict],
        remaining_ms: float,
    ) -> bool:
        """プリロードすべきか判定する（DDD-07 §7.1）。"""
        if model_id in loaded_model_ids:
            return False  # すでにロード済み

        # hit 率が低いパターンは停止
        stats = self._stats[model_id]
        if stats.total >= 5 and stats.hit_rate < _MIN_HIT_RATE:
            return False

        model_meta = registry.get(model_id, {})
        vram_needed = model_meta.get("vram_gb", 9.0)
        load_time = model_meta.get("avg_load_time_ms", 30_000)

        if free_vram_gb >= vram_needed:
            return True  # VRAM 余裕あり → 即プリロード

        # VRAM 不足 → 残り時間でロードが間に合うか
        return remaining_ms >= load_time

    def record_hit(self, model_id: str) -> None:
        self._stats[model_id].hits += 1
        logger.info("preload.hit model=%s rate=%.2f",
                    model_id, self._stats[model_id].hit_rate)

    def record_miss(self, model_id: str) -> None:
        self._stats[model_id].misses += 1
        logger.info("preload.miss model=%s rate=%.2f",
                    model_id, self._stats[model_id].hit_rate)

    def get_stats(self) -> dict[str, dict[str, Any]]:
        return {
            mid: {"hits": s.hits, "misses": s.misses, "hit_rate": round(s.hit_rate, 3)}
            for mid, s in self._stats.items()
        }

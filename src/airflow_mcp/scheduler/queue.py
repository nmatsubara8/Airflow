"""マルチキュースケジューラ（DDD-07 §5）。

モデル別サブキューを持ち、親和性スコア・スターベーション・連続実行上限を考慮して
次の実行タスクを選択する。
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .starvation import check_starvation_flags, compute_aging_score, is_force_priority

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_DEFAULT = 10


@dataclass
class QueueEntry:
    task_id: str
    model_id: str
    base_priority: int = 5            # 0（低）〜 10（高）
    enqueued_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    deadline: datetime | None = None
    starvation_flags: list[str] = field(default_factory=list)

    def effective_priority(self, starvation_config: dict[str, Any]) -> float:
        """aging ブーストを加算した実効優先度。"""
        aging = compute_aging_score(self.enqueued_at, starvation_config)
        return self.base_priority + aging


class SchedulerQueue:
    """モデル別マルチキュー（DDD-07 §5）。

    スレッドセーフではない（呼び出し元で同期すること）。
    """

    def __init__(
        self,
        starvation_config: dict[str, Any] | None = None,
        fair_share_config: dict[str, Any] | None = None,
    ) -> None:
        self._queues: dict[str, list[QueueEntry]] = defaultdict(list)
        self._starvation_cfg = starvation_config or {}
        self._fair_share_cfg = fair_share_config or {}
        self._consecutive: dict[str, int] = defaultdict(int)
        self._last_model: str | None = None
        self._gpu_time_ms: dict[str, float] = {}

    # ── public API ─────────────────────────────────────────

    def enqueue(self, entry: QueueEntry) -> None:
        """タスクをモデル別サブキューに追加する（DDD-07 §5.2）。"""
        self._queues[entry.model_id].append(entry)
        logger.info("scheduler.enqueued task=%s model=%s", entry.task_id, entry.model_id)

    def dequeue(self, loaded_model_ids: list[str]) -> QueueEntry | None:
        """次に実行すべきエントリを選択して取り出す（DDD-07 §5.3）。

        優先順序:
          1. force_run / deadline_near フラグ付き（スワップコスト無視）
          2. ロード済みモデルのキューから高優先度順
          3. 未ロードモデルのキューを探索
        """
        self._refresh_starvation_flags()

        # 1. 強制優先エントリ
        forced = self._pop_forced()
        if forced:
            self._update_consecutive(forced.model_id)
            return forced

        # 1.5. フェアシェア: 最低保証を下回るモデルを優先（DDD-07 §6.4）
        fs_model = self._get_fair_share_boost_model()
        if fs_model:
            entry = self._pop_highest_priority(fs_model)
            if entry:
                self._update_consecutive(fs_model)
                logger.info("scheduler.fair_share_boost model=%s", fs_model)
                return entry

        max_consec = self._starvation_cfg.get(
            "max_consecutive_same_model", _MAX_CONSECUTIVE_DEFAULT
        )

        # 2. ロード済みモデルから（consecutive 上限を尊重）
        for model_id in loaded_model_ids:
            if not self._queues[model_id]:
                continue
            if (
                self._last_model == model_id
                and self._consecutive[model_id] >= max_consec
            ):
                # 連続上限: 他のキューを先に探す
                continue
            entry = self._pop_highest_priority(model_id)
            if entry:
                self._update_consecutive(model_id)
                return entry

        # 3. 未ロードモデルのキューを探索
        for model_id, entries in self._queues.items():
            if not entries or model_id in loaded_model_ids:
                continue
            entry = self._pop_highest_priority(model_id)
            if entry:
                self._update_consecutive(model_id)
                return entry

        # 4. 連続上限で飛ばしたモデルを改めて試す（デッドロック防止）
        for model_id in loaded_model_ids:
            entry = self._pop_highest_priority(model_id)
            if entry:
                self._consecutive[model_id] = 1
                self._last_model = model_id
                return entry

        return None

    def depth(self, model_id: str | None = None) -> int:
        """指定モデル（または全体）のキュー深度を返す。"""
        if model_id:
            return len(self._queues.get(model_id, []))
        return sum(len(q) for q in self._queues.values())

    def model_depths(self) -> dict[str, int]:
        """モデル別キュー深度マップを返す。"""
        return {mid: len(entries) for mid, entries in self._queues.items() if entries}

    def all_entries(self) -> list[QueueEntry]:
        """全エントリのフラットリストを返す（スターベーション監視用）。"""
        return [e for entries in self._queues.values() for e in entries]

    def record_completion(self, model_id: str, duration_ms: float) -> None:
        """GPU 使用時間を累積記録する（フェアシェア計算用）。"""
        self._gpu_time_ms[model_id] = self._gpu_time_ms.get(model_id, 0.0) + max(0.0, duration_ms)

    def gpu_time_stats(self) -> dict[str, float]:
        """モデル別 GPU 使用時間 (ms) を返す（モニタリング用）。"""
        return dict(self._gpu_time_ms)

    # ── private helpers ────────────────────────────────────

    def _refresh_starvation_flags(self) -> None:
        """全エントリのスターベーションフラグを更新する。"""
        for entries in self._queues.values():
            for entry in entries:
                entry.starvation_flags = check_starvation_flags(
                    entry.enqueued_at, entry.deadline, self._starvation_cfg
                )

    def _pop_forced(self) -> QueueEntry | None:
        """force_run / deadline_near フラグ付きの最古エントリを取り出す。"""
        forced: list[tuple[datetime, str, QueueEntry]] = []
        for model_id, entries in self._queues.items():
            for entry in entries:
                if is_force_priority(entry.starvation_flags):
                    forced.append((entry.enqueued_at, model_id, entry))

        if not forced:
            return None

        forced.sort(key=lambda x: x[0])  # 最古を優先
        _, model_id, entry = forced[0]
        self._queues[model_id].remove(entry)
        logger.warning(
            "scheduler.force_priority task=%s flags=%s",
            entry.task_id, entry.starvation_flags,
        )
        return entry

    def _pop_highest_priority(self, model_id: str) -> QueueEntry | None:
        entries = self._queues.get(model_id)
        if not entries:
            return None
        entries.sort(
            key=lambda e: e.effective_priority(self._starvation_cfg),
            reverse=True,
        )
        return entries.pop(0)

    def _get_fair_share_boost_model(self) -> str | None:
        """フェアシェア設定で最低保証を下回るモデルを返す（DDD-07 §6.4）。

        enabled_on_high_load=true かつ GPU 使用実績がある場合のみ発動。
        """
        cfg = self._fair_share_cfg
        if not cfg.get("enabled_on_high_load"):
            return None

        min_pct_map: dict[str, float] = cfg.get("minimum_gpu_time_pct", {})
        if not min_pct_map:
            return None

        total_ms = sum(self._gpu_time_ms.values())
        if total_ms <= 0:
            return None  # 実績なし → フェアシェア判定不要

        deficit: list[tuple[float, str]] = []
        for model_id, entries in self._queues.items():
            if not entries:
                continue
            # "32b" / "14b" / "8b" などのキーをモデル ID にサブストリングマッチ
            key = next((k for k in min_pct_map if k in model_id), None)
            if key is None:
                continue
            min_pct = float(min_pct_map[key])
            actual_pct = self._gpu_time_ms.get(model_id, 0.0) / total_ms * 100.0
            if actual_pct < min_pct:
                deficit.append((min_pct - actual_pct, model_id))

        if not deficit:
            return None

        deficit.sort(reverse=True)
        boosted_model = deficit[0][1]
        logger.debug(
            "scheduler.fair_share deficit=%s model=%s",
            round(deficit[0][0], 1), boosted_model,
        )
        return boosted_model

    def _update_consecutive(self, model_id: str) -> None:
        if self._last_model == model_id:
            self._consecutive[model_id] += 1
        else:
            self._consecutive[model_id] = 1
            self._last_model = model_id

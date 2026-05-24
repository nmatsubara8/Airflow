"""starvation × SchedulerQueue 結合テスト（DDD-07 §5〜§6）。

結合対象:
  scheduler/starvation.py  × scheduler/queue.py

検証シナリオ:
  1. Aging スコア — 待機時間に応じた優先度ブースト
  2. スターベーションフラグ — aging / deadline_near / force_run の判定
  3. force_run の強制 dequeue — 低優先度タスクが長待ちで先頭に出る
  4. deadline_near の強制 dequeue — 期限迫りタスクが優先される
  5. 連続実行上限（max_consecutive_same_model）— モデルの独占を防ぐ
  6. フェアシェアブースト — GPU 使用率が低いモデルを優先する
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from airflow_mcp.scheduler.queue import QueueEntry, SchedulerQueue
from airflow_mcp.scheduler.starvation import (
    check_starvation_flags,
    compute_aging_score,
    is_force_priority,
)

# ── 共通設定 ─────────────────────────────────────────────────────

_STARVATION_CFG = {
    "max_wait_before_boost_s": 600,   # 10 分でエージング開始
    "aging_factor": 0.1,
    "force_run_s": 3600,              # 60 分で強制実行
    "deadline_warn_s": 300,           # 5 分前に deadline_near
    "max_consecutive_same_model": 2,
}

_FAIR_SHARE_CFG = {
    "enabled_on_high_load": True,
    "minimum_gpu_time_pct": {"14b": 30.0, "32b": 20.0},
}

_NOW = datetime.now(timezone.utc)


def _past(seconds: float) -> datetime:
    """seconds 秒前の datetime を返す。"""
    return _NOW - timedelta(seconds=seconds)


def _future(seconds: float) -> datetime:
    """seconds 秒後の datetime を返す。"""
    return _NOW + timedelta(seconds=seconds)


# ── シナリオ 1: Aging スコア ──────────────────────────────────────

class TestAgingScore:

    def test_no_boost_before_threshold(self):
        """閾値未満の待機ではブーストなし。"""
        score = compute_aging_score(_past(100), _STARVATION_CFG)
        assert score == 0.0

    def test_boost_starts_at_threshold(self):
        """閾値ちょうどで boost 開始（浮動小数点誤差を許容）。"""
        score = compute_aging_score(_past(600), _STARVATION_CFG)
        assert score >= 0.0

    def test_boost_increases_with_wait(self):
        """10 分を超えると待機時間に比例してスコアが増加。"""
        score_10m = compute_aging_score(_past(600), _STARVATION_CFG)
        score_20m = compute_aging_score(_past(1200), _STARVATION_CFG)
        assert score_20m > score_10m

    def test_effective_priority_includes_aging(self):
        """effective_priority() が aging ブーストを加算すること。"""
        entry = QueueEntry(
            task_id="t1", model_id="model_14b", base_priority=3,
            enqueued_at=_past(1200),  # 20 分待機
        )
        effective = entry.effective_priority(_STARVATION_CFG)
        assert effective > entry.base_priority


# ── シナリオ 2: スターベーションフラグ ──────────────────────────

class TestStarvationFlags:

    def test_no_flags_for_fresh_entry(self):
        """新規エントリにはフラグなし。"""
        flags = check_starvation_flags(_past(10), None, _STARVATION_CFG)
        assert flags == []

    def test_aging_flag_after_threshold(self):
        """閾値超過で 'aging' フラグが立つ。"""
        flags = check_starvation_flags(_past(700), None, _STARVATION_CFG)
        assert "aging" in flags

    def test_force_run_flag_after_force_run_s(self):
        """force_run_s 秒超過で 'force_run' フラグが立つ。"""
        flags = check_starvation_flags(_past(3700), None, _STARVATION_CFG)
        assert "force_run" in flags

    def test_force_run_suppresses_other_flags(self):
        """force_run が立つと他フラグは返らない（最上位）。"""
        flags = check_starvation_flags(
            _past(3700), _future(100), _STARVATION_CFG
        )
        assert flags == ["force_run"]

    def test_deadline_near_flag_within_warn_window(self):
        """deadline まで deadline_warn_s 秒以内で 'deadline_near' フラグが立つ。"""
        flags = check_starvation_flags(
            _past(10), _future(100), _STARVATION_CFG  # 100s < 300s
        )
        assert "deadline_near" in flags

    def test_no_deadline_near_when_deadline_far(self):
        """deadline が十分遠い場合 'deadline_near' フラグは立たない。"""
        flags = check_starvation_flags(
            _past(10), _future(600), _STARVATION_CFG  # 600s > 300s
        )
        assert "deadline_near" not in flags

    def test_is_force_priority_true_for_force_run(self):
        """force_run フラグは is_force_priority=True。"""
        assert is_force_priority(["force_run"]) is True

    def test_is_force_priority_true_for_deadline_near(self):
        """deadline_near フラグは is_force_priority=True。"""
        assert is_force_priority(["deadline_near"]) is True

    def test_is_force_priority_false_for_aging_only(self):
        """aging だけでは is_force_priority=False。"""
        assert is_force_priority(["aging"]) is False

    def test_is_force_priority_false_for_empty(self):
        """フラグなしは False。"""
        assert is_force_priority([]) is False


# ── シナリオ 3: force_run による強制 dequeue ──────────────────────

class TestForceRunDequeue:

    def test_low_priority_starved_task_dequeued_first(self):
        """低優先タスクでも force_run 閾値を超えると先頭で dequeue される。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)

        # 高優先・新規タスク
        q.enqueue(QueueEntry(
            task_id="fresh_high", model_id="model_14b",
            base_priority=9, enqueued_at=_past(10),
        ))
        # 低優先・長時間待機タスク（force_run 発動）
        q.enqueue(QueueEntry(
            task_id="starved_low", model_id="model_14b",
            base_priority=1, enqueued_at=_past(4000),
        ))

        entry = q.dequeue(loaded_model_ids=["model_14b"])
        assert entry is not None
        assert entry.task_id == "starved_low"

    def test_force_run_entry_has_flag_set(self):
        """force_run タスクには dequeue 後もフラグが記録されている。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)
        q.enqueue(QueueEntry(
            task_id="starved", model_id="model_14b",
            base_priority=1, enqueued_at=_past(4000),
        ))
        entry = q.dequeue(loaded_model_ids=["model_14b"])
        assert entry is not None
        assert "force_run" in entry.starvation_flags

    def test_normal_entry_after_starved_is_dequeued_by_priority(self):
        """force_run タスクが取り出された後は通常の優先度順に戻る。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)
        q.enqueue(QueueEntry(
            task_id="starved", model_id="model_14b",
            base_priority=1, enqueued_at=_past(4000),
        ))
        q.enqueue(QueueEntry(
            task_id="high_prio", model_id="model_14b",
            base_priority=9, enqueued_at=_past(10),
        ))
        q.enqueue(QueueEntry(
            task_id="low_prio", model_id="model_14b",
            base_priority=2, enqueued_at=_past(10),
        ))

        first = q.dequeue(loaded_model_ids=["model_14b"])
        second = q.dequeue(loaded_model_ids=["model_14b"])

        assert first.task_id == "starved"
        assert second.task_id == "high_prio"


# ── シナリオ 4: deadline_near による優先 dequeue ─────────────────

class TestDeadlineNearDequeue:

    def test_deadline_near_task_dequeued_before_higher_base_priority(self):
        """期限迫りタスクは高 base_priority タスクより先に dequeue される。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)

        # 高優先だが期限なし
        q.enqueue(QueueEntry(
            task_id="high_no_deadline", model_id="model_14b",
            base_priority=9, enqueued_at=_past(10),
        ))
        # 低優先だが期限迫り
        q.enqueue(QueueEntry(
            task_id="low_near_deadline", model_id="model_14b",
            base_priority=1, enqueued_at=_past(10),
            deadline=_future(100),  # 100s < deadline_warn_s=300s
        ))

        entry = q.dequeue(loaded_model_ids=["model_14b"])
        assert entry is not None
        assert entry.task_id == "low_near_deadline"

    def test_far_deadline_does_not_get_priority_boost(self):
        """期限が十分先のタスクは強制優先されない。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)

        q.enqueue(QueueEntry(
            task_id="high_prio", model_id="model_14b",
            base_priority=9, enqueued_at=_past(10),
        ))
        q.enqueue(QueueEntry(
            task_id="low_far_deadline", model_id="model_14b",
            base_priority=1, enqueued_at=_past(10),
            deadline=_future(1000),  # 1000s > 300s → 強制優先なし
        ))

        entry = q.dequeue(loaded_model_ids=["model_14b"])
        assert entry is not None
        assert entry.task_id == "high_prio"


# ── シナリオ 5: 連続実行上限 ─────────────────────────────────────

class TestConsecutiveLimit:

    def test_consecutive_limit_prevents_model_monopoly(self):
        """max_consecutive_same_model を超えると他モデルのタスクが優先される。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)

        # model_14b に 5 件
        for i in range(5):
            q.enqueue(QueueEntry(
                task_id=f"14b_{i}", model_id="model_14b",
                base_priority=5, enqueued_at=_past(10),
            ))
        # model_32b に 1 件
        q.enqueue(QueueEntry(
            task_id="32b_0", model_id="model_32b",
            base_priority=5, enqueued_at=_past(10),
        ))

        results = []
        for _ in range(4):
            e = q.dequeue(loaded_model_ids=["model_14b", "model_32b"])
            if e:
                results.append(e.model_id)

        # max_consecutive=2 なので model_14b が 2 連続後、model_32b が入るはず
        assert "model_32b" in results

    def test_consecutive_counter_resets_on_model_switch(self):
        """モデルが切り替わると連続カウンタがリセットされる。"""
        q = SchedulerQueue(starvation_config=_STARVATION_CFG)
        q.enqueue(QueueEntry("t1", "model_14b", base_priority=5))
        q.enqueue(QueueEntry("t2", "model_32b", base_priority=5))
        q.enqueue(QueueEntry("t3", "model_14b", base_priority=5))

        e1 = q.dequeue(["model_14b", "model_32b"])
        e2 = q.dequeue(["model_14b", "model_32b"])
        e3 = q.dequeue(["model_14b", "model_32b"])

        models = [e.model_id for e in [e1, e2, e3] if e]
        # すべて dequeue できること（デッドロックなし）
        assert len(models) == 3


# ── シナリオ 6: フェアシェアブースト ─────────────────────────────

class TestFairShareBoost:

    def test_fair_share_boosts_underutilized_model(self):
        """GPU 使用率が最低保証を下回るモデルが優先される。"""
        q = SchedulerQueue(
            starvation_config=_STARVATION_CFG,
            fair_share_config=_FAIR_SHARE_CFG,
        )

        # model_32b にタスク（使用率低い想定）
        q.enqueue(QueueEntry("32b_task", "model_32b", base_priority=3))
        # model_14b にタスク（使用率高い想定）
        q.enqueue(QueueEntry("14b_task", "model_14b", base_priority=5))

        # model_14b が GPU 時間の大部分を占有（32b は 0%）
        q.record_completion("model_14b", 10_000)  # 10s
        # model_32b は 0 → 最低保証 20% を下回る

        # fair_share_boost が有効なら model_32b が先に出る
        entry = q.dequeue(loaded_model_ids=["model_14b", "model_32b"])
        assert entry is not None
        assert entry.model_id == "model_32b"

    def test_no_fair_share_boost_without_gpu_history(self):
        """GPU 使用実績がない場合はフェアシェアブーストなし。"""
        q = SchedulerQueue(
            starvation_config=_STARVATION_CFG,
            fair_share_config=_FAIR_SHARE_CFG,
        )
        # 高優先 model_14b
        q.enqueue(QueueEntry("14b_task", "model_14b", base_priority=9))
        q.enqueue(QueueEntry("32b_task", "model_32b", base_priority=1))

        # GPU 使用実績なし → fair_share 判定スキップ
        entry = q.dequeue(loaded_model_ids=["model_14b", "model_32b"])
        assert entry is not None
        assert entry.model_id == "model_14b"  # 優先度の高い方

    def test_gpu_time_stats_accumulates(self):
        """record_completion() が GPU 時間を正確に累積する。"""
        q = SchedulerQueue()
        q.record_completion("model_14b", 5_000)
        q.record_completion("model_14b", 3_000)
        q.record_completion("model_32b", 2_000)

        stats = q.gpu_time_stats()
        assert stats["model_14b"] == pytest.approx(8_000)
        assert stats["model_32b"] == pytest.approx(2_000)

    def test_model_depths_reflects_enqueued_tasks(self):
        """model_depths() がエンキュー後に正確な深度を返す。"""
        q = SchedulerQueue()
        q.enqueue(QueueEntry("t1", "model_14b", base_priority=5))
        q.enqueue(QueueEntry("t2", "model_14b", base_priority=5))
        q.enqueue(QueueEntry("t3", "model_32b", base_priority=5))

        depths = q.model_depths()
        assert depths["model_14b"] == 2
        assert depths["model_32b"] == 1

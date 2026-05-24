"""scheduler/preload.py 結合テスト（DDD-07 §7）。

結合対象:
  - PreloadManager × GpuStateManager  (GPU 状態ブリッジ)
  - PreloadManager × affinity.compute_affinity_score  (プリロード後の swap_cost=0 効果)
  - PreloadManager × SchedulerQueue  (キュー深度に基づくプリロード判断)
  - multi-cycle フィードバックループ (hit/miss → hit_rate → 抑制 → 回復)
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from airflow_mcp.gpu.state_manager import GpuStateManager
from airflow_mcp.scheduler.affinity import compute_affinity_score, compute_swap_cost
from airflow_mcp.scheduler.preload import PreloadManager
from airflow_mcp.scheduler.queue import QueueEntry, SchedulerQueue

# ── 共通テストデータ ──────────────────────────────────────────────────────

_REGISTRY = {
    "model_14b": {"vram_gb": 9.0,  "avg_load_time_ms": 20_000},
    "model_32b": {"vram_gb": 14.0, "avg_load_time_ms": 40_000},
}

_VRAM_TOTAL = 16.0
_VRAM_AVAIL = 15.0  # システム予約 1GB


@pytest.fixture
def gpu():
    return GpuStateManager(
        vram_available_gb=_VRAM_AVAIL,
        vram_total_gb=_VRAM_TOTAL,
        snapshot_interval_s=0,  # スナップショット無効
    )


@pytest.fixture
def preload():
    return PreloadManager()


# ── シナリオ 1: GPU 状態ブリッジ ─────────────────────────────────────────
# GpuStateManager の loaded_model_ids を PreloadManager に渡し、
# reserve / release に追従してプリロード判断が変化することを確認。

class TestGpuStateBridge:

    def test_should_preload_when_model_not_loaded(self, gpu, preload):
        """未ロードモデルは VRAM 余裕があればプリロード対象。"""
        assert preload.should_preload(
            "model_14b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 60_000
        ) is True

    def test_should_not_preload_after_reserve(self, gpu, preload):
        """reserve 後は loaded_model_ids に含まれるのでプリロード不要。"""
        gpu.reserve("model_14b", registry=_REGISTRY)
        assert preload.should_preload(
            "model_14b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 60_000
        ) is False

    def test_should_preload_again_after_release(self, gpu, preload):
        """release 後は再度プリロード対象に戻る。"""
        gpu.reserve("model_14b", registry=_REGISTRY)
        gpu.release("model_14b")
        assert preload.should_preload(
            "model_14b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 60_000
        ) is True

    def test_free_vram_decreases_after_reserve(self, gpu, preload):
        """reserve 後に free_vram_gb が減少し、大きなモデルのプリロード可否が変化する。"""
        assert preload.should_preload(
            "model_32b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 60_000
        ) is True  # 15GB 空き → 14GB モデルもプリロード可

        gpu.reserve("model_14b", registry=_REGISTRY)  # 9GB 消費 → 残 6GB

        # 6GB < 14GB なので VRAM だけでは無理、remaining_ms で判断
        assert gpu.free_vram_gb == pytest.approx(6.0)
        # remaining_ms=50_000 >= 40_000 (avg_load_time_ms) → 間に合う
        assert preload.should_preload(
            "model_32b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 50_000
        ) is True
        # remaining_ms=30_000 < 40_000 → 間に合わない
        assert preload.should_preload(
            "model_32b", gpu.loaded_model_ids, gpu.free_vram_gb, _REGISTRY, 30_000
        ) is False

    def test_both_models_loaded_neither_should_preload(self, gpu, preload):
        """両モデルがロード済みなら、どちらもプリロード不要。"""
        gpu.reserve("model_14b", registry=_REGISTRY)
        # model_32b を強引にロード（VRAM eviction が起きる設定）
        gpu_small = GpuStateManager(vram_available_gb=_VRAM_AVAIL, vram_total_gb=_VRAM_TOTAL)
        gpu_small.reserve("model_14b", registry=_REGISTRY)
        gpu_small.reserve("model_32b", registry=_REGISTRY)
        for mid in gpu_small.loaded_model_ids:
            assert preload.should_preload(
                mid, gpu_small.loaded_model_ids, gpu_small.free_vram_gb, _REGISTRY, 60_000
            ) is False


# ── シナリオ 2: 親和性スコアへの波及効果 ─────────────────────────────────
# プリロード済みモデルは swap_cost=0 → affinity スコアが高くなることを確認。

class TestAffinityImpact:

    def test_preloaded_model_has_zero_swap_cost(self):
        """ロード済みモデルの swap_cost は 0。"""
        cost = compute_swap_cost("model_14b", ["model_14b"], 6.0, _REGISTRY)
        assert cost == 0.0

    def test_unloaded_model_has_nonzero_swap_cost(self):
        """未ロードモデルは load_time_ms 分のコストを持つ。"""
        cost = compute_swap_cost("model_14b", [], 15.0, _REGISTRY)
        assert cost == pytest.approx(20_000.0)

    def test_preloaded_model_scores_higher_than_unloaded(self):
        """ロード済みモデルは未ロードより affinity スコアが高い。"""
        task = {"features": {"latency_sensitivity": "batch"}}
        config = {}

        score_loaded = compute_affinity_score(
            task, "model_14b", loaded_model_ids=["model_14b"],
            free_vram_gb=6.0, registry=_REGISTRY, model_config=config,
        )
        score_unloaded = compute_affinity_score(
            task, "model_14b", loaded_model_ids=[],
            free_vram_gb=6.0, registry=_REGISTRY, model_config=config,
        )
        assert score_loaded > score_unloaded

    def test_affinity_gap_interactive_vs_batch(self):
        """interactive タスクはレイテンシ重みが大きいため、プリロードの恩恵がより大きい。"""
        task_interactive = {"features": {"latency_sensitivity": "interactive"}}
        task_batch = {"features": {"latency_sensitivity": "batch"}}
        config = {}
        loaded = ["model_14b"]

        gap_interactive = (
            compute_affinity_score(task_interactive, "model_14b", loaded, 0.0, _REGISTRY, config)
            - compute_affinity_score(task_interactive, "model_14b", [], 0.0, _REGISTRY, config)
        )
        gap_batch = (
            compute_affinity_score(task_batch, "model_14b", loaded, 0.0, _REGISTRY, config)
            - compute_affinity_score(task_batch, "model_14b", [], 0.0, _REGISTRY, config)
        )
        assert gap_interactive > gap_batch


# ── シナリオ 3: SchedulerQueue との連携 ──────────────────────────────────
# キューにタスクが積まれた状態で PreloadManager の判断がキュー深度と整合するか確認。

class TestQueueIntegration:

    def test_preload_decision_consistent_with_queue_depth(self, preload):
        """キューに 3 件のタスクが並ぶモデルはプリロード優先度が高い。"""
        q = SchedulerQueue()
        for i in range(3):
            q.enqueue(QueueEntry(task_id=f"task_{i}", model_id="model_14b", base_priority=5))

        depths = q.model_depths()
        # 最も深いキューのモデルが VRAM 余裕あり → プリロード推奨
        target = max(depths, key=depths.get)
        assert preload.should_preload(
            target, [], free_vram_gb=15.0, registry=_REGISTRY, remaining_ms=60_000
        ) is True

    def test_no_preload_when_queue_empty_and_vram_tight(self, preload):
        """キューが空で VRAM も残り時間も不足 → プリロード不要。"""
        assert preload.should_preload(
            "model_32b", [], free_vram_gb=0.0, registry=_REGISTRY, remaining_ms=5_000
        ) is False

    def test_dequeue_loaded_model_preferred(self):
        """ロード済みモデルのタスクが dequeue で優先されること（スワップ不要）。"""
        q = SchedulerQueue()
        q.enqueue(QueueEntry(task_id="t_32b", model_id="model_32b", base_priority=5))
        q.enqueue(QueueEntry(task_id="t_14b", model_id="model_14b", base_priority=5))

        # model_14b だけロード済み
        entry = q.dequeue(loaded_model_ids=["model_14b"])
        assert entry is not None
        assert entry.model_id == "model_14b"


# ── シナリオ 4: multi-cycle フィードバックループ ─────────────────────────
# hit/miss の蓄積 → hit_rate 変化 → 抑制 → 回復 の完全サイクルを検証。

class TestFeedbackLoop:

    def test_initial_state_allows_preload(self, preload):
        """履歴ゼロでは hit_rate=1.0 として扱い、プリロードを許可。"""
        stats = preload.get_stats()
        assert stats == {}  # 統計なし
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True

    def test_five_hits_keeps_preload_active(self, preload):
        """5 hit → hit_rate=1.0 → プリロード継続。"""
        for _ in range(5):
            preload.record_hit("model_14b")
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True

    def test_suppression_after_enough_misses(self, preload):
        """5 miss → total=5, hit_rate=0.0 < 0.5 → プリロード抑制。"""
        for _ in range(5):
            preload.record_miss("model_14b")
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is False

    def test_below_5_samples_not_suppressed(self, preload):
        """4 miss は閾値 (total >= 5) 未満 → 抑制されない。"""
        for _ in range(4):
            preload.record_miss("model_14b")
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True

    def test_recovery_when_hit_rate_restored_above_threshold(self, preload):
        """6 miss 後に 7 hit → hit_rate = 7/13 ≈ 0.54 ≥ 0.5 → プリロード回復。"""
        for _ in range(6):
            preload.record_miss("model_14b")
        # この時点で抑制されている
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is False

        for _ in range(7):
            preload.record_hit("model_14b")

        stats = preload.get_stats()["model_14b"]
        assert stats["hits"] == 7
        assert stats["misses"] == 6
        assert stats["hit_rate"] == pytest.approx(7 / 13, rel=0.01)
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True

    def test_suppression_at_exactly_threshold(self, preload):
        """hit_rate が ちょうど 0.5 (5hit / 10total) は抑制されない（< 0.5 が条件）。"""
        for _ in range(5):
            preload.record_hit("model_14b")
        for _ in range(5):
            preload.record_miss("model_14b")
        stats = preload.get_stats()["model_14b"]
        assert stats["hit_rate"] == pytest.approx(0.5)
        # hit_rate < 0.5 は False なのでプリロード許可
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True

    def test_two_models_independent_suppression(self, preload):
        """2 モデルの hit/miss は独立して管理される。"""
        for _ in range(6):
            preload.record_miss("model_14b")   # model_14b を抑制
        for _ in range(5):
            preload.record_hit("model_32b")    # model_32b は高 hit_rate

        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is False
        assert preload.should_preload("model_32b", [], 15.0, _REGISTRY, 60_000) is True

    def test_realistic_workload_simulation(self, preload):
        """現実的なワークロードシミュレーション: 70% hit rate でプリロード継続。"""
        results = []
        for i in range(20):
            # 7 hit / 3 miss (70%) のパターンを繰り返す
            if i % 10 < 7:
                preload.record_hit("model_14b")
                results.append("hit")
            else:
                preload.record_miss("model_14b")
                results.append("miss")

        stats = preload.get_stats()["model_14b"]
        assert stats["hits"] == 14
        assert stats["misses"] == 6
        assert stats["hit_rate"] == pytest.approx(14 / 20)
        # 70% hit rate → プリロード継続
        assert preload.should_preload("model_14b", [], 15.0, _REGISTRY, 60_000) is True


# ── シナリオ 5: スレッドセーフ（_state_lock 相当の直列化を前提とした確認） ──
# gpu/server.py は _state_lock でシリアライズするが、
# ロック内部では PreloadManager が正しく累積することを確認。

class TestSerializedAccess:

    def test_sequential_hits_accumulate_correctly(self, preload):
        """直列呼び出しで hit が正確に累積すること。"""
        for _ in range(10):
            preload.record_hit("model_14b")
        assert preload.get_stats()["model_14b"]["hits"] == 10

    def test_interleaved_hit_miss_correct_total(self, preload):
        """hit/miss を交互に記録したとき total が正確。"""
        for i in range(10):
            if i % 2 == 0:
                preload.record_hit("model_14b")
            else:
                preload.record_miss("model_14b")
        stats = preload.get_stats()["model_14b"]
        assert stats["hits"] == 5
        assert stats["misses"] == 5
        assert stats["hits"] + stats["misses"] == 10

    def test_lock_serialized_concurrent_updates(self, preload):
        """ロックで直列化した並列スレッドから hit/miss を記録しても統計が壊れない。"""
        lock = threading.Lock()
        errors = []

        def record(n: int) -> None:
            try:
                for _ in range(n):
                    with lock:  # gpu/server.py の _state_lock を模倣
                        preload.record_hit("model_14b")
            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(record, 25) for _ in range(4)]
            for f in futures:
                f.result()

        assert errors == []
        assert preload.get_stats()["model_14b"]["hits"] == 100

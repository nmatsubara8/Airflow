"""KNN フィードバックループ結合テスト（DDD-06 §5）。

結合対象:
  runner.run_once()
    → append_execution_log()  (JSONL 書込)
    → load_history()          (JSONL 読込)
    → knn_recommend()         (KNN 推奨)
    → select_model()          (モデル選択)

サイクル全体を通じて「実行履歴が次の推奨に反映される」ことを検証する。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from airflow_mcp.orchestrator.execution_log import (
    append_execution_log,
    compute_auto_score,
    load_history,
    update_feedback,
)
from airflow_mcp.orchestrator.recommender import (
    knn_recommend,
    select_model,
)
from airflow_mcp.common.thresholds import KNN_COLD_START_MIN_SAMPLES

os.environ.setdefault("OLLAMA_STUB", "true")

# ── 共通テストデータ ──────────────────────────────────────────────

_REGISTRY = {
    "model_14b": {
        "vram_gb": 9.0,
        "avg_load_time_ms": 20_000,
        "preferred_tasks": ["summarize", "review"],
    },
    "model_32b": {
        "vram_gb": 14.0,
        "avg_load_time_ms": 40_000,
        "preferred_tasks": ["review", "plan"],
    },
}

_KNN_CFG = {
    "k": 5,
    "weights": {
        "task_type": 2.0, "language": 1.5,
        "required_accuracy": 1.0, "latency_sensitivity": 0.5,
        "input_size_tokens": 0.3, "expected_output_size_tokens": 0.3,
    },
}

_MODEL_CONFIG = {
    "selection": {"default_mode": "recommend", "default_model": "model_14b"},
    "knn": _KNN_CFG,
}

_FEATURES_SUMMARIZE = {
    "task_type": "summarize", "language": "ja",
    "required_accuracy": "medium", "latency_sensitivity": "batch",
}

_FEATURES_REVIEW = {
    "task_type": "review", "language": "ja",
    "required_accuracy": "high", "latency_sensitivity": "interactive",
}


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    """テスト専用の一時 ExecutionLog ディレクトリ。"""
    d = tmp_path / "execution_logs"
    d.mkdir()
    return d


def _write_log(
    model_id: str,
    features: dict[str, Any],
    log_dir: Path,
    *,
    schema_valid: bool = True,
    retry_count: int = 0,
    count: int = 1,
) -> list[dict[str, Any]]:
    """ヘルパー: 指定モデルの ExecutionLog を count 件書き込む。"""
    return [
        append_execution_log(
            task_id=f"task_{model_id}_{i}",
            agent_id="agent_docs",
            model_id=model_id,
            features_snapshot=features,
            schema_valid=schema_valid,
            retry_count=retry_count,
            latency_ms=1000,
            log_dir=log_dir,
        )
        for i in range(count)
    ]


# ── シナリオ 1: 基本サイクル ─────────────────────────────────────

class TestBasicCycle:

    def test_append_log_creates_jsonl(self, log_dir: Path):
        """append_execution_log() が JSONL ファイルを生成すること。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        jsonl_files = list(log_dir.glob("*.jsonl"))
        assert len(jsonl_files) == 1

    def test_load_history_returns_written_record(self, log_dir: Path):
        """書き込んだレコードを load_history() で読み返せること。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        history = load_history(log_dir=log_dir)
        assert len(history) == 1
        assert history[0]["model_id"] == "model_14b"
        assert history[0]["features_snapshot"] == _FEATURES_SUMMARIZE

    def test_knn_uses_history_after_write(self, log_dir: Path):
        """ExecutionLog を書いた後、knn_recommend() が結果を返すこと。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir, count=5)
        history = load_history(log_dir=log_dir)
        recs = knn_recommend(
            _FEATURES_SUMMARIZE, history,
            list(_REGISTRY.keys()), _KNN_CFG,
        )
        assert any(r["model_id"] == "model_14b" for r in recs)

    def test_record_contains_required_fields(self, log_dir: Path):
        """ExecutionLog レコードが必須フィールドを持つこと。"""
        records = _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        rec = records[0]
        for field in ("id", "timestamp", "task_id", "agent_id", "model_id",
                      "features_snapshot", "schema_valid", "retry_count",
                      "latency_ms", "auto_score", "success"):
            assert field in rec, f"フィールド '{field}' が存在しない"
        assert rec["id"].startswith("log_")


# ── シナリオ 2: モデル選好の蓄積 ──────────────────────────────────

class TestModelPreferenceAccumulation:

    def test_high_score_model_preferred_after_accumulation(self, log_dir: Path):
        """高スコア履歴が積まれたモデルが knn_recommend() で先頭になる。"""
        # model_32b を 5 件 (score 0.7)、model_14b を 5 件 (score 0.7) 書き込む
        # → 同スコアなら両方が推奨される
        _write_log("model_32b", _FEATURES_REVIEW, log_dir, count=5)
        _write_log("model_14b", _FEATURES_REVIEW, log_dir, schema_valid=False, count=5)

        history = load_history(log_dir=log_dir)
        recs = knn_recommend(
            _FEATURES_REVIEW, history,
            list(_REGISTRY.keys()), {**_KNN_CFG, "k": 10},
        )
        # model_32b は schema_valid=True → auto_score=0.7
        # model_14b は schema_valid=False → auto_score=0.0
        top = recs[0]["model_id"] if recs else None
        assert top == "model_32b", f"高スコアモデルが先頭にならない: {recs}"

    def test_select_model_picks_better_history_model(self, log_dir: Path):
        """select_model() が蓄積された高スコア履歴のモデルを選ぶ。

        k を全レコード数以上にすることで両モデルが評価対象に入り、
        スコアだけで順位が決まることを確認する。
        """
        # model_14b に十分な高スコア履歴
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir, count=10)
        # model_32b に低スコア履歴
        _write_log("model_32b", _FEATURES_SUMMARIZE, log_dir,
                   schema_valid=False, count=10)

        history = load_history(log_dir=log_dir)
        task = {"features": _FEATURES_SUMMARIZE}
        # k=20 で両モデルのレコードを全件 top-k に含める
        config = {**_MODEL_CONFIG, "knn": {**_KNN_CFG, "k": 20}}
        model_id, etype = select_model(task, config, _REGISTRY, history)
        assert model_id == "model_14b"
        assert etype is None  # cold_start ではない

    def test_multiple_cycles_accumulate_correctly(self, log_dir: Path):
        """複数サイクルを経てもレコード数が正確に累積する。"""
        for i in range(3):
            _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir, count=5)

        history = load_history(log_dir=log_dir)
        assert len(history) == 15


# ── シナリオ 3: コールドスタート → ウォームスタート ─────────────

class TestColdStartToWarmStart:

    def test_empty_history_triggers_cold_start_fallback(self, log_dir: Path):
        """履歴ゼロでは cold_start_fallback になる。"""
        task = {"features": _FEATURES_SUMMARIZE}
        _, etype = select_model(task, _MODEL_CONFIG, _REGISTRY, history=[])
        assert etype == "cold_start_fallback"

    def test_below_threshold_still_cold_start(self, log_dir: Path):
        """KNN_COLD_START_MIN_SAMPLES 未満では cold_start_fallback が継続する。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir,
                   count=KNN_COLD_START_MIN_SAMPLES - 1)
        history = load_history(log_dir=log_dir)
        task = {"features": _FEATURES_SUMMARIZE}
        _, etype = select_model(task, _MODEL_CONFIG, _REGISTRY, history)
        assert etype == "cold_start_fallback"

    def test_at_threshold_exits_cold_start(self, log_dir: Path):
        """KNN_COLD_START_MIN_SAMPLES 件に達すると cold_start_fallback を脱する。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir,
                   count=KNN_COLD_START_MIN_SAMPLES)
        history = load_history(log_dir=log_dir)
        task = {"features": _FEATURES_SUMMARIZE}
        _, etype = select_model(task, _MODEL_CONFIG, _REGISTRY, history)
        assert etype != "cold_start_fallback"

    def test_cold_start_returns_valid_model_id(self, log_dir: Path):
        """コールドスタート時も registry 内の有効なモデル ID が返る。"""
        task = {"features": _FEATURES_SUMMARIZE}
        model_id, _ = select_model(task, _MODEL_CONFIG, _REGISTRY, history=[])
        assert model_id in _REGISTRY


# ── シナリオ 4: human_feedback による auto_score 変動 ────────────

class TestHumanFeedbackEffect:

    def test_accepted_feedback_raises_score(self, log_dir: Path):
        """accepted フィードバックで auto_score が上昇する。"""
        records = _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        task_id = records[0]["task_id"]
        base_score = records[0]["auto_score"]

        updated = update_feedback(task_id, "accepted", log_dir=log_dir)
        assert updated is not None
        assert updated["auto_score"] > base_score

    def test_rejected_feedback_lowers_score(self, log_dir: Path):
        """rejected フィードバックで auto_score が低下する。"""
        records = _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        task_id = records[0]["task_id"]
        base_score = records[0]["auto_score"]

        updated = update_feedback(task_id, "rejected", log_dir=log_dir)
        assert updated is not None
        assert updated["auto_score"] < base_score

    def test_feedback_persists_in_load_history(self, log_dir: Path):
        """フィードバックが JSONL に永続化され load_history() で読み返せる。"""
        records = _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir)
        task_id = records[0]["task_id"]
        update_feedback(task_id, "accepted", log_dir=log_dir)

        history = load_history(log_dir=log_dir)
        rec = next(r for r in history if r["task_id"] == task_id)
        assert rec["human_feedback"] == "accepted"

    def test_rejected_model_ranks_lower_in_knn(self, log_dir: Path):
        """rejected フィードバックを受けたモデルが KNN で下位になる。"""
        # model_14b を 5 件書いて全件 rejected
        records_14b = _write_log("model_14b", _FEATURES_REVIEW, log_dir, count=5)
        for r in records_14b:
            update_feedback(r["task_id"], "rejected", log_dir=log_dir)

        # model_32b を 5 件書いてそのまま（accepted なし）
        _write_log("model_32b", _FEATURES_REVIEW, log_dir, count=5)

        history = load_history(log_dir=log_dir)
        recs = knn_recommend(
            _FEATURES_REVIEW, history,
            list(_REGISTRY.keys()), {**_KNN_CFG, "k": 10},
        )
        if len(recs) >= 2:
            scores = {r["model_id"]: r["score"] for r in recs}
            assert scores.get("model_32b", 0) >= scores.get("model_14b", 0)


# ── シナリオ 5: リトライペナルティ ──────────────────────────────

class TestRetryPenalty:

    def test_auto_score_decreases_with_retries(self):
        """retry_count が増えると auto_score が下がる。"""
        score_0 = compute_auto_score(schema_valid=True, retry_count=0)
        score_1 = compute_auto_score(schema_valid=True, retry_count=1)
        score_2 = compute_auto_score(schema_valid=True, retry_count=2)
        assert score_0 > score_1 > score_2

    def test_failed_execution_has_zero_base_score(self):
        """schema_valid=False の実行は auto_score のベースが 0。"""
        score = compute_auto_score(schema_valid=False, retry_count=0)
        assert score == 0.0

    def test_high_retry_model_ranks_lower(self, log_dir: Path):
        """リトライ多いモデルは同タスク種別で低スコアになり下位推奨。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir,
                   retry_count=0, count=5)   # 高スコア
        _write_log("model_32b", _FEATURES_SUMMARIZE, log_dir,
                   retry_count=2, count=5)   # ペナルティあり

        history = load_history(log_dir=log_dir)
        recs = knn_recommend(
            _FEATURES_SUMMARIZE, history,
            list(_REGISTRY.keys()), {**_KNN_CFG, "k": 10},
        )
        scores = {r["model_id"]: r["score"] for r in recs}
        assert scores.get("model_14b", 0) > scores.get("model_32b", 0)

    def test_log_window_limits_history(self, log_dir: Path):
        """load_history(window=N) は直近 N 件のみ返す。"""
        _write_log("model_14b", _FEATURES_SUMMARIZE, log_dir, count=20)
        history = load_history(log_dir=log_dir, window=10)
        assert len(history) == 10

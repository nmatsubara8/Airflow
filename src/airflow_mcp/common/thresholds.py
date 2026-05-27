"""システム全体の定数（DDD-05 / DDD-06）。

モデル名・スコア閾値・KNN パラメータ等のマジック値を一元管理する。
各モジュールはここから import して使用すること。
"""
from __future__ import annotations

# ── デフォルトモデル ID（DDD-06 §6）────────────────────────────────────
DEFAULT_MODEL_ID = "model_gemma4_26b_a4b"
DEFAULT_MODEL_32B_ID = "model_qwen3_35b_a3b"

# ── auto_score 算出パラメータ（DDD-06 §5.2）──────────────────────────────
# compute_auto_score() のベーススコアと補正値
AUTO_SCORE_BASE_SUCCESS = 0.7      # schema 検証成功時のベース
AUTO_SCORE_RETRY_PENALTY = 0.1     # リトライ 1 回あたりの減点
AUTO_SCORE_FEEDBACK_ACCEPTED = 0.1
AUTO_SCORE_FEEDBACK_MODIFIED = 0.2
AUTO_SCORE_FEEDBACK_REJECTED = 0.5

# runner がリルートを発動する auto_score 下限（DDD-06 §7.3）
REROUTE_SCORE_THRESHOLD = 0.7

# ── 二重実行類似度閾値（DDD-05 §8.2）──────────────────────────────────────
DUAL_SIM_HIGH = 0.95    # この値以上 → auto_score=1.0（完全合意）
DUAL_SIM_LOW = 0.85     # この値未満 → ModelLowConfidence 例外
DUAL_SCORE_PARTIAL = 0.85  # DUAL_SIM_LOW〜DUAL_SIM_HIGH の場合の auto_score

# ── KNN 推奨パラメータ（DDD-06 §4）──────────────────────────────────────
KNN_COLD_START_MIN_SAMPLES = 5   # この件数未満ならコールドスタート扱い
KNN_CONFIDENCE_SCALE = 10.0      # confidence = min(1.0, sample_count / scale)
EPSILON_GREEDY_RATE = 0.1        # auto モードの探索率 ε

# ── エージェントスコアリング（DDD-02 §5）────────────────────────────────
AGENT_SECTION_MATCH_SCORE = 0.5
AGENT_TAG_MATCH_SCORE = 0.1

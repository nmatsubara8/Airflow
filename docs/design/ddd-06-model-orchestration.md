# DDD-06：モデル動的選択・学習 + 精度補完統合設計

> Phase 3。DDD-05 の単一モデル基盤の上に、**タスク特徴に応じた動的モデル選択**と **精度補完機構** を統合する。
>
> 参照要件：`requirements-model-orchestration.md` 全般、`requirements-local-oss-config.md` §3.2「精度補完メカニズム」
> 前提：[[DDD-01：共通データスキーマ仕様書]]、[[DDD-02：MCP インターフェース仕様書]]、[[DDD-03：エラー処理・ログ設計]]、[[DDD-05：ローカル LLM 実行基盤]]

---

## 1. スコープ

### 1.1 含む

- 3 モード（Manual / Recommend / Auto）の動作仕様
- タスク特徴ベクトル抽出ロジック
- KNN ベースの推奨アルゴリズム
- フィードバックループとオンライン学習
- 精度補完機構の統合（二重実行、自動評価、再ルート）
- コールドスタート対策

### 1.2 含まない（後続 DDD）

- 並列実行・親和性スコア → DDD-07
- 複数モデル同時起動 → DDD-07

Phase 3 は **依然としてシリアル実行**を前提とする。動的選択はモデル切替頻度を増やすが、並列化は次フェーズ。

---

## 2. 3 モードの動作仕様

### 2.1 モード設定

```yaml
# .airflow/config/model-config.yaml
selection:
  default_mode: recommend       # manual | recommend | auto
  exploration_rate: 0.1         # ε-greedy
  min_samples_for_auto: 20      # この件数未満なら auto を recommend に降格
```

### 2.2 各モードの動作

| モード | 選択ロジック | 人間関与 |
|-------|-----------|--------|
| **Manual** | タスクの `selected_model_id` を必ず使う | 完全に人間が決定 |
| **Recommend** | KNN で推奨モデルを提示、`selected_model_id` 未設定なら推奨を採用、設定済みなら指定を優先 | 必要に応じて人間が override |
| **Auto** | KNN 推奨 + ε-greedy 探索で完全自動選択 | 介入なし（フィードバックのみ） |

### 2.3 モード上書きの粒度

| レベル | 設定箇所 |
|------|---------|
| グローバル | `model-config.yaml` の `default_mode` |
| エージェント | `skills.md` の `selection_mode` |
| タスク個別 | タスク作成時の `_meta.selection_mode` |
| CLI 個別 | `airflow task run --mode manual --model X` |

下位の指定が優先される。

### 2.4 Auto モードの自動降格

`min_samples_for_auto` 未満のサンプル数 → Auto 指定でも内部で Recommend として動作。
ログ：warn `model.auto_downgraded`、`reason: insufficient_samples (N < 20)`。

---

## 3. タスク特徴ベクトル抽出

### 3.1 必要な 6 軸

DDD-01 §2.1 で定義済み（`task.features`）：

```yaml
task_type:        code_gen | review | summarize | classify | extract | chat | test | document
language:         ja | en | code | mixed
input_size_tokens:    integer
expected_output_size_tokens: integer
required_accuracy:    low | medium | high
latency_sensitivity:  none | batch | interactive
```

### 3.2 抽出ロジック

#### 3.2.1 明示指定優先

タスク作成時に `features` フィールドが明示されていれば、そのまま採用。

#### 3.2.2 自動推定（明示指定なしの場合）

```
task_type:
  - エージェントの skills.md から推定（agent.outputs に summarize があれば summarize）
  - タスクタグから推定（"review" タグ → review）
  - デフォルト: document

language:
  - description / source ノートを bge-m3 で埋め込み → 日本語クラスタとの類似度で判定
  - .py/.ts/.json などのファイル拡張子が含まれれば code
  - デフォルト: ja

input_size_tokens:
  - description のトークン数 + source ノートのトークン数を tiktoken でカウント
  - 不明: 推定値 1000

expected_output_size_tokens:
  - task_type ごとのデフォルト（summarize: 200, classify: 50, code_gen: 800, document: 1000）

required_accuracy:
  - section が business/legal → high
  - section が orchestration の分類タスク → low
  - デフォルト: medium

latency_sensitivity:
  - tags に "urgent" or "interactive" → interactive
  - 夜間バッチ生成タスク → batch
  - デフォルト: batch
```

#### 3.2.3 抽出タイミング

タスクが `inbox` に登録された直後、taskboard-mcp 内で同期的に実行。
失敗時はデフォルト値を入れて `warn` ログ。

### 3.3 特徴抽出 API

```yaml
# 内部 API（agents-mcp に同居）
agents.extract_features:
  input: Task object (features 未設定可)
  output: TaskFeatures
```

---

## 4. 推奨アルゴリズム（KNN）

### 4.1 アプローチ選定

| 手法 | Phase 3 | Phase 5+ |
|------|:---:|:---:|
| **KNN（k=5）** | ✅ 採用 | — |
| Decision Tree | — | 候補 |
| Multi-Armed Bandit | — | 候補 |

理由：データ少でも動く・解釈可能・実装単純・Cold Start に弱いが §6 の対策で補完。

### 4.2 KNN の距離計算

特徴ベクトルを以下のように数値化：

```
task_type:        one-hot encoding (8 次元)
language:         one-hot encoding (4 次元)
input_size_tokens:    log10 スケール正規化 (1 次元)
expected_output_tokens: log10 スケール正規化 (1 次元)
required_accuracy:    [low=0.0, medium=0.5, high=1.0]
latency_sensitivity:  [none=0.0, batch=0.5, interactive=1.0]
```

距離：重み付きユークリッド距離。

```
weights = {
  task_type:        2.0,    # 最重要
  language:         1.5,
  required_accuracy: 1.0,
  latency_sensitivity: 0.5,
  input_size_tokens:    0.3,
  expected_output_tokens: 0.3
}
```

### 4.3 推奨スコア算出

```
1. 入力特徴ベクトル v_in を計算
2. ExecutionLog から過去 N=200 件の (features_snapshot, model_id, auto_score, success) を取得
3. v_in と各サンプルの距離を計算
4. 最近傍 k=5 件を抽出
5. モデルごとに平均 auto_score を集計:
   - score(model_X) = mean(auto_score_i for i in neighbors if model_id_i == model_X)
   - sample_count(model_X) = count(...)
6. score 降順で並べる
7. 各モデルに confidence を付与:
   - confidence = min(1.0, sample_count / 10)
```

### 4.4 出力（`model.recommend` 応答）

```json
{
  "recommendations": [
    {
      "model_id": "model_qwen25_32b_q3",
      "score": 0.91,
      "expected_accuracy": 0.91,
      "swap_cost_ms": 0,
      "confidence": 0.8,
      "reason": "過去 8 件の類似タスクで平均スコア 0.91"
    },
    {
      "model_id": "model_qwen25_14b_q4",
      "score": 0.76,
      "expected_accuracy": 0.76,
      "swap_cost_ms": 0,
      "confidence": 0.5,
      "reason": "過去 5 件の類似タスクで平均スコア 0.76"
    }
  ],
  "sample_count": 13
}
```

`swap_cost_ms` は Phase 3 では常に 0（並列なし）。Phase 4 で DDD-07 が計算式を入れる。

### 4.5 ε-greedy 探索（Auto モード）

```
1. KNN 推奨を取得（top model）
2. random < exploration_rate (0.1) → 別モデルをランダム選択
3. 否 → top model を採用
4. 新規登録モデルは 10 件処理されるまで強制的に探索対象に含める
```

ログ：`model.explored` info、`reason: epsilon_greedy` or `new_model_exploration`。

---

## 5. フィードバックループ

### 5.1 フィードバックの 3 源泉

| 源泉 | 取得タイミング | 反映先 |
|------|------------|-------|
| **自動評価**（schema_valid, retry_count） | 実行直後 | ExecutionLog（自動） |
| **二重実行類似度**（auto_score） | required_accuracy=high で常時 | ExecutionLog（自動） |
| **人間フィードバック**（accepted/modified/rejected） | needs_human 解決時 or 任意 | `model.feedback` MCP 呼出 |

### 5.2 `auto_score` の算出（自動評価）

二重実行を行わないタスクでは、以下から自動算出：

```
score = base
  - 0.3  if not schema_valid
  - 0.1 * retry_count
  + 0.1  if human_feedback == "accepted"
  - 0.2  if human_feedback == "modified"
  - 0.5  if human_feedback == "rejected"

base:
  - 1.0  if 二重実行類似度 ≥ 0.95
  - 0.85 if 0.85 <= 類似度 < 0.95
  - 0.7  デフォルト（二重実行なしで成功）
  - 0.0  失敗（success=false）

clamp to [0, 1]
```

### 5.3 人間フィードバック登録

```
1. needs_human のタスクが人間によって解決される
2. tasks.transition(done, handoff_note=...) が呼ばれる
3. 同時に MCP: model.feedback({
     task_id: ...,
     human_feedback: "accepted" | "modified" | "rejected"
   })
4. 対応する ExecutionLog レコードを UPDATE
5. CLI: airflow task feedback <task_id> --rating accepted
```

### 5.4 反映タイミング

KNN は **クエリ時にリアルタイム計算**するため、フィードバック反映は即座。バッチ再学習は不要。

---

## 6. コールドスタート対策

### 6.1 段階定義

| 段階 | 条件 | 動作 |
|------|------|------|
| **Cold Start** | サンプル数 < 5（タスク種別ごと） | Recommend 強制、`preferred_models` 優先 |
| **探索期** | 5 ≤ サンプル数 < 20 | Recommend、Auto 指定でも降格 |
| **収束期** | サンプル数 ≥ 20 | Auto 解禁 |

### 6.2 Cold Start 時の挙動

```
1. KNN で近傍検索 → sample_count < 5
2. fallback として agent.preferred_models を参照
3. weight 降順で top model を採用
4. ログ: info "model.cold_start_fallback"
```

### 6.3 新規モデル追加時の探索強制

```
1. 新規モデルを model.register で追加
2. ExecutionLog でそのモデルの実行件数 = 0
3. ε-greedy の探索対象に強制追加（10 件達成まで）
4. ログ: info "model.new_model_exploring"
```

---

## 7. 精度補完機構の統合

### 7.1 メカニズム一覧と起動条件

| 機構 | 自動起動条件 | 実装場所 |
|------|----------|--------|
| 構造化出力強制 | structured_output 指定時 | DDD-05 §6 |
| リトライ（recoverable） | recoverable エラー | DDD-03 §3 |
| **二重実行** | `required_accuracy = high` & `mode != manual` | 本 DDD §7.2 |
| **再ルート** | 二重実行不一致 or low confidence | 本 DDD §7.3 |
| Few-shot 自動構築 | （Phase 5 で検討） | — |

### 7.2 二重実行の Phase 3 での ON 化

Phase 2 では OFF だった二重実行を、`required_accuracy = high` タスクで自動 ON：

```yaml
# .airflow/config/model-config.yaml
accuracy_compensation:
  dual_execution:
    enabled: true
    trigger:
      - required_accuracy: high
    skip_for:
      - latency_sensitivity: interactive  # 速度優先タスクは除外
    temperature_variation:
      first: 0.3
      second: 0.5
    similarity_thresholds:
      pass: 0.95
      partial: 0.85
```

実装は DDD-05 §8 のロジックをそのまま流用。

### 7.3 再ルート（fallback chain）

二重実行不一致 or 推奨信頼度低い場合、**より上位モデルに自動切替** して再実行：

```yaml
reroute:
  enabled: true
  fallback_chain:
    model_qwen25_14b_q4:    [model_qwen25_32b_q3]
    model_qwen25_32b_q3:    []   # 終端
  max_reroute_attempts: 1
```

実装：

```
1. model.execute 完了
2. auto_score < 0.7 OR error_class == "MODEL_LOW_CONFIDENCE"
3. reroute 設定確認 → fallback_chain の次モデルがあれば再実行
4. 元実行と再実行の両方を ExecutionLog に記録
5. 再実行の結果も auto_score < 0.7 → escalate (needs_human)
6. ログ: info "model.rerouted"
```

### 7.4 再ルートと並列実行の関係（DDD-07 への接続）

Phase 4 で並列化が入ると、再ルートは「親和性 vs 精度」のトレードオフを生む。
本 DDD では再ルート発動時は **親和性を無視して fallback model を使う** とのみ規定。
コスト計算は DDD-07 §3.3 で詳細化。

---

## 8. `model.recommend` の実装詳細

DDD-02 §5.2 で定義したツールの内部処理。

### 8.1 処理フロー

```
入力:
  features: TaskFeatures
  mode: manual | recommend | auto
  candidate_models: [model_id, ...]  optional

1. mode == manual:
   - features.preferred_models から最上位を返す（recommendations 配列に 1 件）
   - reason: "Manual mode: top of preferred_models"

2. mode == recommend or auto:
   a. ExecutionLog から過去 N=200 件取得（直近のもの優先）
   b. KNN 計算（§4.2-4.3）
   c. sample_count < min_samples_for_auto:
      - mode を recommend に降格
      - reason 末尾に "(downgraded from auto)"
   d. candidate_models が指定されていれば、その中から選ぶ
   e. recommendations 配列を score 降順で返却

3. Auto モードかつ ε-greedy 発動:
   - random < 0.1 → top 以外をランダム選択
   - reason: "epsilon_greedy exploration"

4. ログ: info "model.recommended"
```

### 8.2 candidate_models の用途

並列実行（DDD-07）で「現在ロード済みモデル」だけを候補に絞る用途。
Phase 3 では使用しないが、インターフェースは確保。

---

## 9. 設定ファイル統合

### 9.1 完成形 `model-config.yaml`

```yaml
schema_version: "1.0.0"

selection:
  default_mode: recommend
  exploration_rate: 0.1
  min_samples_for_auto: 20

knn:
  k: 5
  history_window: 200
  weights:
    task_type: 2.0
    language: 1.5
    required_accuracy: 1.0
    latency_sensitivity: 0.5
    input_size_tokens: 0.3
    expected_output_tokens: 0.3

accuracy_compensation:
  dual_execution:
    enabled: true
    trigger:
      - required_accuracy: high
    skip_for:
      - latency_sensitivity: interactive
    temperature_variation:
      first: 0.3
      second: 0.5
    similarity_thresholds:
      pass: 0.95
      partial: 0.85

reroute:
  enabled: true
  fallback_chain:
    model_qwen25_14b_q4: [model_qwen25_32b_q3]
    model_qwen25_32b_q3: []
  max_reroute_attempts: 1
```

---

## 10. CLI 拡張

```bash
# モデル推奨を見る（実行はしない）
airflow model recommend --task <task_id>
airflow model recommend --features-file features.json

# 統計
airflow model stats
# => qwen25-14b-q4: 156 runs, 89% schema_valid, avg_score 0.78, avg 2.1s
#    qwen25-32b-q3:  42 runs, 95% schema_valid, avg_score 0.91, avg 5.5s

# サンプル数チェック
airflow model coverage
# => task_type=summarize: 45 samples (auto OK)
#    task_type=review: 8 samples (recommend only)
#    task_type=classify: 2 samples (cold start)

# フィードバック登録
airflow task feedback <task_id> --rating accepted
```

---

## 11. シナリオ

### 11.1 シナリオ A：契約書レビュー（high accuracy）

```
タスク投入:
  features: { task_type: "review", language: "ja", required_accuracy: "high",
              latency_sensitivity: "none" }

1. agent.extract_features → そのまま採用
2. model.recommend (mode=recommend):
   - KNN: 過去 12 件の類似タスク
   - top: qwen25_32b_q3 (score=0.91, confidence=0.8)
3. ユーザー override なし → 32b 採用
4. model.execute(qwen25_32b_q3):
   - required_accuracy=high → 二重実行 ON
   - 実行 1: temp=0.3
   - 実行 2: temp=0.5
   - 類似度 0.92 → 部分一致、auto_score=0.85
5. ExecutionLog: 3 行記録（実行 1、実行 2、集約）
6. agent: handoff_note 付きで done 遷移
```

### 11.2 シナリオ B：タスク分類（low accuracy）

```
タスク投入:
  features: { task_type: "classify", language: "ja", required_accuracy: "low",
              latency_sensitivity: "interactive" }

1. model.recommend:
   - KNN: 過去 3 件 → cold start
   - fallback: preferred_models top = qwen25_14b_q4
2. model.execute(qwen25_14b_q4):
   - required_accuracy=low → 二重実行 OFF
   - latency_sensitivity=interactive → temperature 低め
   - 単発実行成功
3. auto_score = 0.7（デフォルト、補正なし）
```

### 11.3 シナリオ C：再ルート発動

```
1. model.execute(qwen25_14b_q4): auto_score = 0.5（low）
2. reroute 発動 → qwen25_32b_q3 で再実行
3. auto_score = 0.88
4. 結果: qwen25_32b_q3 の出力を採用
5. ExecutionLog: 14b 1 行 (success but low) + 32b 1 行 (success)
6. ログ: info "model.rerouted", from=14b, to=32b
```

### 11.4 シナリオ D：新規モデル探索

```
1. ユーザー: airflow model add model_gemma_27b_q4
2. その後 20 件のタスクで、ε-greedy + 新規探索強制で gemma が選ばれる
3. 実行結果が ExecutionLog に蓄積
4. 10 件達成 → 強制探索終了、KNN の通常評価に組み込まれる
```

---

## 12. 監視・可視化

### 12.1 メトリクス

```
- model_recommendations_total{model_id, mode, downgraded}
- model_execution_score_histogram{model_id, task_type}
- model_reroute_total{from, to}
- model_cold_start_fallback_total{task_type}
- model_exploration_total{model_id, reason}
```

Phase 3 では SQLite クエリで集計、Phase 5 で Prometheus エクスポート検討。

### 12.2 ダッシュボード（CLI 出力）

```bash
$ airflow model dashboard

=== Model Performance (last 7 days) ===
Model                      Runs  Success  AvgScore  AvgLatency
qwen25_14b_q4                89     85%      0.76        2.3s
qwen25_32b_q3                42     93%      0.89        5.5s
gemma_27b_q4                  5    100%      0.82        4.1s  [new]

=== Recommendation Coverage ===
task_type=summarize     [████████░░] 45 samples  ← auto OK
task_type=review        [██░░░░░░░░]  8 samples  ← recommend only
task_type=classify      [░░░░░░░░░░]  2 samples  ← cold start

=== Recent Reroutes (last 24h) ===
14b → 32b: 5 cases (avg score improvement: +0.18)
```

---

## 13. 障害シナリオ

| シナリオ | 検出 | 対応 |
|---------|------|------|
| ExecutionLog 破損 | SQL エラー | logs/errors に記録、KNN は preferred_models フォールバック |
| KNN 計算で全モデル score=0 | 集計後 | preferred_models フォールバック、warn ログ |
| 推奨モデルが unload 済み | model.execute 内でロード | DDD-05 §4.3 のロード処理 |
| reroute 連鎖が止まらない | max_reroute_attempts=1 で制限 | 1 回で escalate |
| 二重実行が連続失敗 | 2 回とも error | recoverable リトライ、それでも失敗 → escalate |

---

## 14. パフォーマンス目標と実測

### 14.1 目標

| 指標 | 目標 |
|------|-----|
| `model.recommend` レイテンシ | < 100ms（200 件 KNN） |
| Phase 3 全体のタスク成功率（Phase 2 比） | +10pt |
| 高精度タスクの auto_score 平均 | ≥ 0.85 |
| 再ルート発動率 | < 20% |

### 14.2 ベースライン比較

Phase 2 baseline（DDD-05 §14）と本フェーズ完了時の指標を比較し、`docs/baseline/phase3-orchestration.md` に記録。

---

## 15. 検証チェックリスト

Phase 3 完了時：

- [ ] 3 モードがすべて動作
- [ ] タスク特徴の自動抽出が機能
- [ ] KNN 推奨レイテンシ < 100ms
- [ ] Cold Start fallback が preferred_models を使う
- [ ] ε-greedy が指定確率で別モデルを選ぶ
- [ ] 二重実行 ON で auto_score が記録される
- [ ] 再ルートが auto_score < 0.7 で発動
- [ ] フィードバック登録が KNN にリアルタイム反映
- [ ] ExecutionLog に parent_audit_id が正しく連鎖
- [ ] `airflow model dashboard` で統計が見える

---

## 16. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| Multi-Armed Bandit への移行 | Phase 5 |
| Decision Tree によるモデル選択 | Phase 5 |
| Few-shot プロンプトの自動構築 | Phase 5 |
| `swap_cost_ms` の実装（並列化前提） | DDD-07 |
| 候補モデル絞り込み（candidate_models 活用） | DDD-07 |
| 学習データの decay_factor 実装 | Phase 5 |
| 二重実行の温度バリエーション最適化 | Phase 5 |

---

*次：DDD-07（並列スケジューラ）で、本 DDD で増える「モデル切替頻度」を吸収する並列実行と親和性スコアを設計する。*

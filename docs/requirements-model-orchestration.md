# モデル動的選択・学習機能 — 概要要件定義

> タスクに応じて使用 LLM を入れ替え可能にし、Hermes Agent がタスクとモデルの組み合わせを **学習・最適化** していく仕組み。
>
> 前提：[[完全無料・完全ローカル構成]]（VRAM 16GB / Ollama / Hermes Agent）

---

## 1. 機能概要

### 1.1 目的

- VRAM 16GB の制約下で、**複数モデル**（軽量〜大型量子化）を使い分けて最大の費用対精度を得る
- ユーザーが明示的にモデル指定できる
- **使用実績データから自動学習** し、タスクに最適なモデルを推奨・自動選択する

### 1.2 3 つの選択モード

| モード | 説明 | 想定ユーザー |
|-------|------|------------|
| **Manual** | タスクごとに明示指定 | チューニング期・実験用 |
| **Recommend** | エージェントが推奨表示、人間が承認 | 通常運用 |
| **Auto** | 学習結果に基づき自動選択 | 安定運用フェーズ |

---

## 2. 機能要件

### 2.1 モデル登録・管理

| # | 要件 |
|---|------|
| F-M01 | 使用可能なモデルを **モデルレジストリ** に登録できる（Ollama / vLLM / クラウド API 等） |
| F-M02 | 各モデルに以下のメタデータを保持：`name`, `provider`, `vram_gb`, `quantization`, `context_length`, `strengths`, `cost_per_1k_tokens`, `avg_latency_ms` |
| F-M03 | モデルの追加・削除を CLI / 設定ファイルで実施できる |
| F-M04 | 同時起動不可なモデルの組み合わせを VRAM 制約から自動判定する |

### 2.2 タスク → モデル割当

| # | 要件 |
|---|------|
| F-M05 | エージェント定義（`skills.md`）に **`preferred_models`** フィールドを追加できる |
| F-M06 | タスク単位でユーザーが `--model` オプションでオーバーライドできる |
| F-M07 | 指定モデルが利用不可（VRAM 不足 / 停止中）の場合は **フォールバック順序** に従って代替モデルで実行 |
| F-M08 | フォールバック発生時は warning ログを残し、ユーザー通知 |

### 2.3 タスク分類

| # | 要件 |
|---|------|
| F-M09 | タスクを以下の **特徴ベクトル** に変換： |
|     | - `task_type`（code_gen / review / summarize / classify / extract / chat） |
|     | - `language`（ja / en / code） |
|     | - `input_size_tokens` |
|     | - `expected_output_size_tokens` |
|     | - `required_accuracy`（low / medium / high） |
|     | - `latency_sensitivity`（none / batch / interactive） |
| F-M10 | タスク特徴は自動抽出し、明示指定でオーバーライド可能 |

### 2.4 学習データ収集

| # | 要件 |
|---|------|
| F-M11 | 各タスク実行で以下を **実績ログ** に保存： |
|     | - タスク特徴ベクトル |
|     | - 使用モデル |
|     | - 実行時間（ms） |
|     | - 出力トークン数 |
|     | - 成功 / 失敗（パース成否、リトライ回数） |
|     | - 人間レビュー結果（採用 / 修正 / 却下） |
|     | - 自動評価スコア（後述） |
| F-M12 | 実績ログはローカル DB（SQLite）に保存し、外部送信しない |
| F-M13 | 人間レビュー結果は **明示フィードバック UI** から入力可能（👍/👎 / 採用率） |

### 2.5 自動評価

| # | 要件 |
|---|------|
| F-M14 | 構造化出力の **JSON Schema 適合率** を自動計測 |
| F-M15 | 出力に含まれる ハルシネーション指標（存在しない関数名、引用元矛盾等）を簡易検出 |
| F-M16 | 二重実行モード時の **一致率** をモデル性能指標として記録 |
| F-M17 | テスト実行系タスクは **実テスト結果** をフィードバックとして直接利用 |

### 2.6 学習・推奨ロジック

| # | 要件 |
|---|------|
| F-M18 | 蓄積実績から **タスク特徴 × モデル → 期待スコア** のマッピングを学習 |
| F-M19 | 学習アルゴリズム： |
|     | - Phase 1：単純集計（特徴ベクトル近傍 K=5 件の平均成功率） |
|     | - Phase 2：軽量モデル（決定木 / ロジスティック回帰）でモデル選択を予測 |
|     | - Phase 3：Multi-Armed Bandit でモデル探索と活用のバランス |
| F-M20 | 新規モデル追加時は **探索期間**（ε-greedy で 10% 強制選択）を設けて学習データを蓄積 |
| F-M21 | 学習はクラウド送信せず、ローカルで完結 |

### 2.7 推奨提示

| # | 要件 |
|---|------|
| F-M22 | タスク投入時に **推奨モデル + 信頼度 + 代替候補** を表示 |
| F-M23 | 信頼度が低い場合（過去実績不足）は警告を出し、ユーザー判断を促す |
| F-M24 | 推奨理由を可視化する（例：「過去に同種タスクで Qwen 2.5 32B が 85% 成功」） |

### 2.8 オーバーライドと学習の関係

| # | 要件 |
|---|------|
| F-M25 | ユーザーが推奨と異なるモデルを選択した場合、その実績も学習に組み込む |
| F-M26 | ユーザーが連続して特定モデルを選んでいる場合、推奨ロジックに重み付け |
| F-M27 | グローバル設定で **「推奨を無視する」** モードに切り替え可能 |

---

## 3. アーキテクチャ

```
┌────────────────────────────────────────────────┐
│         タスク投入（ユーザー or エージェント）   │
└──────────────────────┬─────────────────────────┘
                       │ タスク特徴抽出
                       ▼
        ┌──────────────────────────────┐
        │   Model Selector              │
        │   - レジストリ参照             │
        │   - 学習履歴参照（SQLite）     │
        │   - VRAM / 起動状態確認        │
        └──┬──────────────────────────┬─┘
           │ 推奨 / Auto              │ Manual
           ▼                          ▼
   ┌─────────────┐         ┌─────────────────┐
   │ ユーザー承認 │◄────────│ ユーザー指定モデル │
   └─────┬───────┘         └─────────┬───────┘
         │                            │
         └────────────┬───────────────┘
                      ▼
        ┌──────────────────────────────┐
        │   Model Runtime               │
        │   - Ollama / vLLM 切替        │
        │   - VRAM 確保・モデルロード    │
        └──────────────┬───────────────┘
                       │ 実行
                       ▼
        ┌──────────────────────────────┐
        │   Execution Logger            │
        │   - 実績 + 評価記録            │
        │   - 人間フィードバック取り込み  │
        └──────────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │   Learner                     │
        │   - 推奨モデル更新             │
        │   - ε-greedy 探索              │
        └──────────────────────────────┘
```

---

## 4. データモデル

### 4.1 モデルレジストリ

```yaml
models:
  - id: qwen2.5-32b-instruct-q3
    name: "Qwen 2.5 32B Instruct (Q3_K_M)"
    provider: ollama
    vram_gb: 14
    quantization: Q3_K_M
    context_length: 32768
    strengths: [japanese, reasoning, document]
    avg_latency_ms_per_100tok: 5000
    cost_per_1k_tokens: 0.0

  - id: qwen2.5-coder-32b-q3
    name: "Qwen 2.5 Coder 32B (Q3_K_M)"
    provider: ollama
    vram_gb: 14
    quantization: Q3_K_M
    context_length: 32768
    strengths: [code_generation, code_review]
    avg_latency_ms_per_100tok: 5000
    cost_per_1k_tokens: 0.0

  - id: qwen2.5-14b-q4
    name: "Qwen 2.5 14B (Q4_K_M)"
    provider: ollama
    vram_gb: 9
    quantization: Q4_K_M
    context_length: 32768
    strengths: [classify, extract, summarize]
    avg_latency_ms_per_100tok: 2000
    cost_per_1k_tokens: 0.0

  - id: llama3.1-8b-q4
    name: "Llama 3.1 8B (Q4_K_M)"
    provider: ollama
    vram_gb: 5
    quantization: Q4_K_M
    context_length: 8192
    strengths: [summarize, light_task]
    avg_latency_ms_per_100tok: 1000
    cost_per_1k_tokens: 0.0
```

### 4.2 タスク実績ログ（SQLite）

```sql
CREATE TABLE task_executions (
  id INTEGER PRIMARY KEY,
  timestamp TEXT NOT NULL,
  task_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  task_type TEXT,
  language TEXT,
  input_size_tokens INTEGER,
  output_size_tokens INTEGER,
  required_accuracy TEXT,
  latency_sensitivity TEXT,
  model_id TEXT NOT NULL,
  latency_ms INTEGER,
  success BOOLEAN,
  retry_count INTEGER,
  schema_valid BOOLEAN,
  human_feedback TEXT,  -- 'accepted' | 'modified' | 'rejected' | NULL
  auto_score REAL,      -- 0.0 - 1.0
  notes TEXT
);

CREATE INDEX idx_features ON task_executions(task_type, language, required_accuracy);
CREATE INDEX idx_model ON task_executions(model_id);
```

### 4.3 エージェント定義の拡張

```yaml
---
agent_id: brand-voice
section: §02
platform: hermes-agent
preferred_models:
  - id: qwen2.5-32b-instruct-q3
    weight: 1.0
  - id: qwen2.5-14b-q4
    weight: 0.5         # フォールバック用
context_isolation: subagent
required_accuracy: high
language: ja
---
```

### 4.4 設定ファイル `.airflow/model-config.yaml`

```yaml
selection:
  mode: recommend   # manual | recommend | auto
  exploration_rate: 0.1   # ε-greedy: 10% は別モデルを試す
  min_samples_for_auto: 20  # この件数を超えるまで auto は推奨にダウングレード

learning:
  algorithm: knn   # knn | decision_tree | bandit
  knn_k: 5
  retrain_interval: daily
  decay_factor: 0.95  # 古いデータの重み減衰

vram_limit_gb: 16
allow_cpu_offload: false  # true にすると低速だが大型モデルも動作
```

---

## 5. CLI / API

### 5.1 CLI コマンド例

```bash
# モデル登録
airflow model add qwen2.5-32b-instruct-q3 \
  --provider ollama --vram 14 --strengths japanese,reasoning

# 一覧確認
airflow model list

# 推奨モデルを表示
airflow model recommend \
  --task-type code_gen --language code --accuracy high
# => Recommended: qwen2.5-coder-32b-q3 (confidence: 87%, based on 42 samples)

# タスク実行（手動指定）
airflow task run task-123 --model qwen2.5-14b-q4

# 学習統計
airflow model stats
# => qwen2.5-32b-instruct-q3: 156 runs, 89% success, avg 5.2s
#    qwen2.5-14b-q4         :  89 runs, 76% success, avg 2.1s

# フィードバック登録
airflow task feedback task-123 --rating accepted
```

### 5.2 MCP ツール公開

| ツール名 | 機能 |
|---------|------|
| `model.recommend` | タスク特徴から推奨モデルを返す |
| `model.list` | 利用可能モデル一覧 |
| `model.stats` | モデル別統計 |
| `model.feedback` | フィードバック登録 |

---

## 6. 段階的学習戦略

| Phase | 期間目安 | 状態 | 動作 |
|-------|---------|------|------|
| **Cold Start** | 0-1 週 | データ不足 | デフォルト設定（`preferred_models`）に従う。Recommend モードで人間判断を促す |
| **探索期** | 1-4 週 | データ蓄積中 | ε-greedy で 10% 別モデルを強制選択。学習データを増やす |
| **収束期** | 4 週〜 | 充分データ蓄積 | KNN / Bandit で自動選択。Auto モード解禁 |
| **継続改善** | 常時 | 新モデル追加時 | 該当モデルのみ探索期へ戻す |

---

## 7. ユースケース

### 7.1 シナリオ A：契約書レビュー（§03）

```
タスク特徴：
  task_type=review, language=ja, accuracy=high, latency=none

→ 推奨：qwen2.5-32b-instruct-q3（過去 12 件で全件採用、信頼度 91%）
→ 代替：qwen2.5-14b-q4（VRAM 不足時のフォールバック）
```

### 7.2 シナリオ B：タスクのタグ分類（調整層）

```
タスク特徴：
  task_type=classify, language=ja, accuracy=low, latency=interactive

→ 推奨：llama3.1-8b-q4（高速・低精度許容）
→ 32B は使用しない（過剰）
```

### 7.3 シナリオ C：新規モデル追加

```
ユーザーが gemma3-27b-q4 を追加
→ 探索期に入る（10% 強制選択）
→ 20 件実行後、qwen 2.5 32B より日本語スコアが高ければ推奨を切替
```

---

## 8. 想定リスクと対策

| リスク | 影響 | 対策 |
|------|------|------|
| 初期データ不足での誤推奨 | 性能低下 | Cold Start 期は Recommend モード固定 |
| 局所最適への収束 | 新モデルが選ばれない | ε-greedy 探索を常時有効化 |
| 人間フィードバック未入力 | 学習材料不足 | 自動評価指標で代替 |
| モデル切替のオーバーヘッド | レイテンシ増 | バッチ運用ではモデルごとにタスクをまとめる |
| VRAM 競合 | OOM | 起動前に VRAM 残量確認、必要に応じてアンロード |
| 学習データの偏り | 特定モデルへ過剰集中 | decay_factor で古いデータの重み減衰 |

---

## 9. 実装フェーズ

| Phase | 内容 | 期間 |
|-------|------|-----|
| **M-Phase 1** | モデルレジストリ + Manual 指定 + 実行ログ | 1 週間 |
| **M-Phase 2** | タスク特徴抽出 + Recommend モード（KNN） | 2 週間 |
| **M-Phase 3** | 人間フィードバック UI + 自動評価 | 1 週間 |
| **M-Phase 4** | Auto モード + ε-greedy 探索 | 2 週間 |
| **M-Phase 5** | Multi-Armed Bandit への移行 | 3 週間（将来） |

---

## 10. 関連要件との接続

| 関連要件 | 接続点 |
|---------|--------|
| [[完全無料・完全ローカル構成]] | VRAM 16GB 制約・モデル一覧の前提を共有 |
| [[AI チーム構成 — 詳細要件定義]] | エージェント定義の `preferred_models` 拡張 |
| [[ローカル Markdown 取り込み機能]] | フィードバックノートからの学習データ抽出も可能 |
| [[概要要件定義]] | タスクボードのタスク特徴を入力として利用 |

---

*ユーザー指定と Hermes Agent の学習を併用することで、ローカル運用の費用対精度を継続改善する仕組み。*

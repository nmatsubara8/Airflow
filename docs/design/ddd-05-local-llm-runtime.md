# DDD-05：ローカル LLM 実行基盤

> Phase 2 MVP コアの第 2 弾（最終）。Ollama を中心とした **ローカル LLM 推論の実装詳細**。VRAM 16GB 制約下での単一モデル運用、構造化出力強制、精度補完の基礎を確定する。
>
> 参照要件：`requirements-local-oss-config.md` 全般、§3.2「精度補完メカニズム」、`requirements-model-orchestration.md` の `model.execute` 部分
> 前提：[[DDD-01：共通データスキーマ仕様書]]、[[DDD-02：MCP インターフェース仕様書]]、[[DDD-03：エラー処理・ログ設計]]

---

## 1. スコープ

### 1.1 含む

- Ollama プロセス管理とモデルロードの手順
- VRAM 制約下のシリアル実行モデル
- `model.execute` MCP ツールの実装詳細
- 構造化出力（JSON Schema）の強制機構
- 推論失敗時のリトライ
- ExecutionLog への自動記録

### 1.2 含まない（後続 DDD）

- 複数モデル並列・親和性スコア → DDD-07
- Recommend/Auto モデル選択ロジック → DDD-06
- 学習データを使った推奨 → DDD-06

Phase 2 MVP の前提：**1 度に 1 モデルだけがロードされ、シリアルに実行される**。

---

## 2. 推論バックエンドの選定

### 2.1 Ollama を採用する理由

| 観点 | Ollama | vLLM | llama.cpp 単独 |
|------|:---:|:---:|:---:|
| セットアップ容易性 | ◎ | △ | △ |
| Modelfile による設定 | ◎ | ✗ | ✗ |
| 自動 VRAM 管理（GGUF） | ◎ | ✗ | ○ |
| 同時リクエスト | ○（v0.5+） | ◎ | △ |
| OpenAI 互換 API | ◎ | ◎ | △ |
| 量子化サポート | ◎（Q3〜Q8） | ○ | ◎ |

**結論**：Phase 2〜3 は **Ollama 単独**。Phase 4 で並列スループット要求が顕在化したら、vLLM を追加する余地を残す（同じ OpenAI 互換インターフェースなのでスイッチ容易）。

### 2.2 Ollama バージョン要件

- 最低：v0.5.0（`OLLAMA_NUM_PARALLEL` 対応）
- 推奨：最新安定版
- 起動コマンド：`ollama serve`（systemd 経由で常駐）

---

## 3. モデル管理

### 3.1 利用モデルの確定（VRAM 16GB）

#### 3.1.1 MVP 必須セット（Phase 2 で DL 必須）

| ID | Ollama tag | VRAM | 量子化 | 用途 |
|----|-----------|------|------|------|
| `model_qwen25_14b_q4` | `qwen2.5:14b-instruct-q4_K_M` | ~9GB | Q4_K_M | **MVP デフォルト**（高速・中精度・日本語） |
| `model_qwen25_32b_q3` | `qwen2.5:32b-instruct-q3_K_M` | ~14GB | Q3_K_M | 高精度タスク用（reroute fallback、§02/§03 既定） |
| `model_bge_m3` | `bge-m3` | ~1GB | FP16 | 埋め込み（Vault Ingestion + 二重実行類似度） |

**Phase 2 MVP は `qwen2.5:14b-q4` をデフォルトに**：
- 32B Q3 より精度は落ちるが、ロード時間・推論速度の安定性を優先
- VRAM 余裕があるため、bge-m3 と同居可能（埋め込みと推論の同時並行）

#### 3.1.2 拡張セット（Phase 3-4 で DL）

| ID | Ollama tag | VRAM | 量子化 | 用途 |
|----|-----------|------|------|------|
| `model_qwen25_coder_32b_q3` | `qwen2.5-coder:32b-instruct-q3_K_M` | ~14GB | Q3_K_M | コード生成・レビュー専用（§01 主力） |
| `model_qwen25_coder_14b_q4` | `qwen2.5-coder:14b-instruct-q4_K_M` | ~9GB | Q4_K_M | コード軽量タスク（高負荷時の coder fallback） |
| `model_llama31_8b_q4` | `llama3.1:8b-instruct-q4_K_M` | ~5GB | Q4_K_M | 軽量分類・要約（並列起動用） |
| `model_qwen25_7b_q4` | `qwen2.5:7b-instruct-q4_K_M` | ~5GB | Q4_K_M | 軽量日本語タスク（dual-7b/8b プロファイル用） |

> **Phase 2 MVP では拡張セットは DL 不要**。Phase 3 で `model_qwen25_coder_32b_q3` から段階的に追加。
> Phase 4 で `model_llama31_8b_q4` / `model_qwen25_7b_q4` を追加し、`14b-plus-8b` / `dual-7b-plus-emb` 等のプロファイル運用を有効化。

#### 3.1.3 モデル登録手順

Phase 2 のとき：

```bash
# 必須モデル DL（10-30分）
ollama pull qwen2.5:14b-instruct-q4_K_M
ollama pull qwen2.5:32b-instruct-q3_K_M
ollama pull bge-m3

# レジストリに登録
airflow model register --config models/registry-mvp.json
```

Phase 3 以降：

```bash
ollama pull qwen2.5-coder:32b-instruct-q3_K_M
airflow model register --id model_qwen25_coder_32b_q3 --ollama-tag qwen2.5-coder:32b-instruct-q3_K_M
```

### 3.2 Modelfile によるカスタマイズ

`.airflow/models/Modelfile.qwen25-14b`：

```
FROM qwen2.5:14b-instruct-q4_K_M

# システムプロンプトテンプレート
SYSTEM """
あなたは Airflow タスク管理システムで動作する AI エージェントです。
出力はすべて構造化された JSON で返してください。
スコープ外の判断が必要な場合は escalate=true を返してください。
"""

# 推論パラメータの既定値
PARAMETER temperature 0.3
PARAMETER top_p 0.9
PARAMETER num_ctx 16384

# 出力フォーマット強制（structured_output 利用時に追加適用）
PARAMETER stop "<<END>>"
```

カスタムモデルを `airflow-qwen25-14b` として登録：

```bash
ollama create airflow-qwen25-14b -f .airflow/models/Modelfile.qwen25-14b
```

同様の Modelfile を全モデルに用意（命名規則 `Modelfile.<model_id短縮形>`）：

| Modelfile | 対応 model_id | 主な差別化 |
|----|----|----|
| `Modelfile.qwen25-14b` | `model_qwen25_14b_q4` | 標準。日本語向け system prompt |
| `Modelfile.qwen25-32b` | `model_qwen25_32b_q3` | 高精度タスク向け、temperature=0.2 |
| `Modelfile.qwen25-coder-32b` | `model_qwen25_coder_32b_q3` | コード向け、num_ctx=16384、コード規約 system prompt |
| `Modelfile.llama31-8b` | `model_llama31_8b_q4` | 軽量、分類タスク向けに temperature=0.1 |
| `Modelfile.qwen25-7b` | `model_qwen25_7b_q4` | 並列起動向け軽量設定 |

### 3.3 モデルレジストリへの登録

DDD-01 §2.3 の Model スキーマに従い `.airflow/models/registry.json` に記録：

```json
[
  {
    "schema_version": "1.0.0",
    "id": "model_qwen25_14b_q4",
    "name": "Qwen 2.5 14B Instruct (Q4_K_M)",
    "provider": "ollama",
    "vram_gb": 9,
    "quantization": "Q4_K_M",
    "context_length": 16384,
    "strengths": ["japanese", "reasoning", "summarize", "extract"],
    "avg_latency_ms_per_100tok": 2000,
    "cost_per_1k_tokens": 0.0,
    "endpoint": "http://localhost:11434"
  }
]
```

---

## 4. プロセス・ライフサイクル

### 4.1 起動順序

```
1. systemd: ollama.service 起動（OS ブート時）
   - ローカルポート 11434
2. systemd: airflow-models-mcp.service 起動
   - models-mcp サーバー（DDD-02 §5）
   - Ollama への HTTP クライアントを初期化
3. systemd: airflow-ingester.service 起動（DDD-04）
4. ユーザーまたはスケジュールでエージェント起動
```

### 4.2 起動時自己診断

models-mcp は起動時に：

1. Ollama HTTP `/api/version` で疎通確認
2. `registry.json` の全モデルに対し `/api/show` で存在確認
3. 未取得モデルがあれば warn ログ（DL は手動）
4. GPU 情報を `nvidia-smi --query-gpu=memory.total,memory.used` で取得

失敗時：
- Ollama 不在 → `INTERNAL_UNEXPECTED` fatal、サービス起動失敗
- モデル不在 → 起動継続（実行時にエラー）

### 4.3 モデルのロード管理

Ollama は **自動的に最終アクセスから 5 分でモデルをアンロード**する。
本基盤では以下のように制御：

| アクション | 実装 |
|----------|------|
| 明示的ロード | `POST /api/generate` を空 prompt で送信（OR `keep_alive` 指定） |
| 明示的アンロード | `POST /api/generate` に `keep_alive: 0` |
| ロード状態確認 | `GET /api/ps` |
| アイドルアンロード | Ollama デフォルト 5 分（設定変更可） |

Phase 2 MVP では：
- 単一モデルが常駐する想定 → `keep_alive: -1`（手動アンロードまで保持）
- 別モデル要求時は明示的に先にアンロード

---

## 5. `model.execute` の実装詳細

DDD-02 §5.3 で定義したツールの内部処理を詳細化。

### 5.1 処理フロー

```
入力受信:
  {
    "model_id": "model_qwen25_14b_q4",
    "messages": [{"role": "user", "content": "..."}],
    "structured_output": { "schema": {...} },
    "max_tokens": 2000,
    "temperature": 0.3,
    "task_id": "task_abc"
  }

1. audit_id 派生（DDD-03 §5）

2. モデル存在確認
   - registry にあるか → なければ MODEL_NOT_FOUND (fatal)

3. GPU 確保（DDD-07 で並列化、Phase 2 はシリアル）
   - 別モデルがロード中 → アンロード待ち or LRU 退避
   - VRAM 不足検知 → GPU_INSUFFICIENT_VRAM (recoverable)

4. プロンプト構築
   - structured_output あり → §6 の手順を適用
   - なし → そのまま messages を渡す

5. Ollama 呼出: POST /api/chat
   - 推論実行
   - タイムアウト: max(60秒, expected_output_tokens * 100ms)

6. 結果パース
   - structured_output あり → JSON パース
     失敗 → リトライ §7 へ

7. ExecutionLog 記録
   - DDD-01 §2.5 の形式で SQLite に INSERT
   - audit_id, latency_ms, success, retry_count を記録

8. レスポンス返却
```

### 5.2 タイムアウト戦略

| 段階 | デフォルト | オーバーライド |
|------|---------|------------|
| Ollama HTTP 接続 | 5 秒 | — |
| モデル初回ロード | 120 秒 | `model.load_timeout_sec` 設定 |
| 推論実行 | `60 + expected_tokens × 0.1` 秒 | リクエストの `timeout_ms` |

タイムアウト発生時：`MODEL_EXECUTION_FAILED` recoverable → リトライ対象。

### 5.3 ストリーミング対応（Phase 2 では非対応）

`/api/chat` の `stream: false` のみ実装。Phase 4 でインタラクティブ要件が出たら拡張。

---

## 6. 構造化出力の強制（精度補完の中核）

### 6.1 採用方針

Ollama は **JSON モード**（`format: "json"`）をネイティブサポート。これに加えて、**JSON Schema を渡してプロンプト内で明示** することで二重に強制する。

### 6.2 実装テンプレート

`structured_output.schema` が指定された場合、プロンプトに以下を自動追加：

```
あなたの出力は以下の JSON Schema に厳密に従ってください。

```json
{ ...ユーザー指定の schema... }
```

出力は JSON オブジェクト 1 つのみとし、説明文や前置きは含めないでください。
すべての required フィールドを必ず含めてください。
```

Ollama 呼出時：
```json
{
  "model": "airflow-qwen25-14b",
  "messages": [...],
  "format": "json",
  "options": { "temperature": 0.3 }
}
```

### 6.3 検証フロー

```
1. Ollama 応答受信
2. JSON.parse 試行
   失敗 → MODEL_STRUCTURED_OUTPUT_INVALID (recoverable)
3. jsonschema バリデート
   失敗 → MODEL_STRUCTURED_OUTPUT_INVALID (recoverable)
4. 成功 → parsed フィールドに格納して返却
```

### 6.4 schema_valid フラグ

ExecutionLog の `schema_valid` フィールドに常に boolean を記録。
学習データとして「どのモデルが構造化出力に強いか」の判定材料になる。

---

## 7. リトライ実装

### 7.1 リトライ対象

DDD-03 §3 の `recoverable` クラスのうち、本基盤で発生しうるもの：

| エラーコード | 対応 |
|------------|------|
| `MODEL_EXECUTION_FAILED` | 200ms × 2^n バックオフ、最大 3 回 |
| `MODEL_STRUCTURED_OUTPUT_INVALID` | プロンプトに警告を追加して 2 回まで再試行 |
| `INTERNAL_TIMEOUT` | バックオフ後 3 回まで |

### 7.2 構造化出力リトライ時の追加プロンプト

初回失敗時、2 回目以降のリクエストには以下を追加：

```
（前回の出力 "..." は JSON として不正でした：<エラー詳細>）
必ず有効な JSON オブジェクト 1 つのみを返してください。
```

これでも失敗する場合は `escalate` に昇格。

### 7.3 リトライ回数の記録

ExecutionLog の `retry_count` に最終的なリトライ回数を記録。
学習時にこの値が多いモデルは「指示追従性が低い」と判定される。

---

## 8. 二重実行モード（精度補完）

DDD-03 §3.4 で定義したルールの実装。

### 8.1 起動条件

タスクの `features.required_accuracy = "high"` かつ `mode != manual` の場合、`model.execute` は内部で 2 回実行する。

### 8.2 実装

```
1. 1 回目実行: result_a = ollama.chat(messages, temperature=0.3)
2. 2 回目実行: result_b = ollama.chat(messages, temperature=0.5)
   ↑ 異なる temperature で実行することで stochastic な揺らぎを評価

3. 類似度算出:
   - 構造化出力時 → JSON フィールドごとに完全一致率
   - 自由文時 → bge-m3 で埋め込み類似度（Cosine）

4. 判定:
   - similarity >= 0.95 → 完全一致扱い、auto_score = 1.0、result_a を採用
   - 0.85 <= similarity < 0.95 → 部分一致、auto_score = 0.85、result_a を採用
   - similarity < 0.85 → MODEL_LOW_CONFIDENCE (escalate)

5. ExecutionLog に両実行を記録（2 行）+ 集約スコアを 1 行追加
```

### 8.3 Phase 2 MVP での扱い

Phase 2 MVP（単一エージェント）では基本的に `required_accuracy = "medium"` を採用し、二重実行は **インフラとしては実装するが、デフォルト OFF**。
Phase 3 で精度測定後に `high` タスクに段階適用する。

---

## 9. ExecutionLog の自動記録

### 9.1 記録タイミング

`model.execute` の各実行ごとに 1 行記録。二重実行モードでは 2 + 1 = 3 行（個別実行 + 集約）。

### 9.2 SQLite スキーマ（DDD-01 §2.5 の実装）

```sql
CREATE TABLE execution_logs (
  id              TEXT PRIMARY KEY,              -- log_<ULID>
  schema_version  TEXT NOT NULL,
  timestamp       TEXT NOT NULL,
  task_id         TEXT,
  agent_id        TEXT,
  model_id        TEXT NOT NULL,
  features_json   TEXT,                          -- features_snapshot を JSON 文字列
  latency_ms      INTEGER,
  input_tokens    INTEGER,
  output_tokens   INTEGER,
  success         INTEGER NOT NULL,              -- bool: 0/1
  retry_count     INTEGER DEFAULT 0,
  schema_valid    INTEGER,
  auto_score      REAL,
  human_feedback  TEXT,
  error_json      TEXT,                          -- error オブジェクト JSON
  audit_id        TEXT NOT NULL,
  parent_audit_id TEXT,
  notes           TEXT
);

CREATE INDEX idx_exec_features ON execution_logs(model_id, success);
CREATE INDEX idx_exec_task ON execution_logs(task_id);
CREATE INDEX idx_exec_audit ON execution_logs(audit_id);
CREATE INDEX idx_exec_timestamp ON execution_logs(timestamp);
```

### 9.3 書き込みポリシー

- **同期書き込み**：レスポンス返却前に INSERT 完了を待つ（耐障害性優先）
- **WAL モード有効**：`PRAGMA journal_mode=WAL` で並行読み込み可能
- **バッチ書き込みは Phase 4 で検討**（並列化時のスループット改善時）

---

## 10. プロンプトテンプレート管理

### 10.1 配置

`.airflow/prompts/` 配下に役割別 Markdown：

```
.airflow/prompts/
├── system/
│   ├── default.md
│   ├── code-review.md
│   └── document-summary.md
└── structured/
    └── escalation_check.md
```

### 10.2 利用方法

エージェント `skills.md` で参照：

```yaml
---
agent_id: agent_docs
preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0
prompt_templates:
  system: "system/document-summary.md"
---
```

`model.execute` は messages にこのテンプレートを system role として挿入。

---

## 11. CLI 実装

### 11.1 必要なコマンド

```bash
# モデル管理
airflow model list
airflow model add <id> --ollama-tag qwen2.5:14b-instruct-q4_K_M
airflow model load <id>        # 明示ロード
airflow model unload <id>      # 明示アンロード
airflow model ps               # 現在ロード済み一覧（/api/ps 相当）

# 直接実行（デバッグ用）
airflow model exec <id> --prompt "test" [--schema-file path/to/schema.json]

# Ollama 疎通
airflow doctor                  # 起動時自己診断と同じチェックを手動実行
```

### 11.2 例：直接実行

```bash
$ airflow model exec model_qwen25_14b_q4 \
    --prompt "次の文章を 100 字以内で要約: ..." \
    --schema-file schemas/summary.json

{
  "summary": "...",
  "key_points": [...]
}

Execution: 4.2s, 850 tokens, schema_valid=true, log_id=log_01HK...
```

---

## 12. シナリオ：MVP の end-to-end

DDD-04 §11 で示した MVP 検証シナリオの **LLM 部分** を本 DDD で具体化：

```
[前提]
- agent_docs エージェントが task_abc (summarize) を claim 済み
- task.features = { task_type: "summarize", language: "ja",
                    required_accuracy: "medium", latency_sensitivity: "batch" }

[実行]
1. agent_docs が prompt を構築:
   - system プロンプト = prompts/system/document-summary.md
   - user content = ノート本文

2. agent_docs が models-mcp に呼出:
   {
     "tool": "model.execute",
     "args": {
       "model_id": "model_qwen25_14b_q4",
       "messages": [...],
       "structured_output": {
         "schema": {
           "type": "object",
           "required": ["summary", "key_points"],
           "properties": {
             "summary": { "type": "string", "maxLength": 200 },
             "key_points": { "type": "array", "items": { "type": "string" } }
           }
         }
       },
       "task_id": "task_abc"
     },
     "_meta": { "audit_id": "log_01HM..." }
   }

3. models-mcp:
   a. ollama /api/ps → qwen25-14b 既にロード済み
   b. プロンプトに JSON Schema 強制テキスト追加
   c. /api/chat 呼出 (format: json)
   d. 応答 JSON.parse + jsonschema バリデート → OK
   e. ExecutionLog INSERT (schema_valid=true, auto_score=NULL)
   f. レスポンス返却 (parsed フィールドに JSON オブジェクト)

4. agent_docs:
   - parsed.summary を handoff_note に格納
   - tasks.transition(done, handoff_note=...)

[計測]
- 推論時間: 4 秒
- ログ: 1 行（success=true）
- 全体経過: 5 秒以内
```

---

## 13. 障害シナリオと対応

| シナリオ | 検出 | 対応 |
|---------|------|------|
| Ollama プロセスダウン | HTTP 接続拒否 | `MODEL_EXECUTION_FAILED` recoverable、3 回リトライ後 fatal |
| モデル未取得（未 DL） | `/api/show` 404 | `MODEL_NOT_FOUND` fatal、CLI で取得を促す |
| VRAM 不足 | nvidia-smi で検出 | `GPU_INSUFFICIENT_VRAM` recoverable（Phase 2 は単純待機） |
| 推論が JSON を返さない | パース失敗 | `MODEL_STRUCTURED_OUTPUT_INVALID` 2 回まで再試行 |
| 推論が極端に長い | タイムアウト | `INTERNAL_TIMEOUT` recoverable、3 回リトライ |
| ExecutionLog SQLite ロック | sqlite3.OperationalError | `INTERNAL_TIMEOUT` recoverable、`retry_max_attempts=5` を応答に明示（DDD-03 §3.2.1） |
| Ollama がモデルを勝手にアンロード | 次回呼出で再ロード（120秒） | warn ログ、`keep_alive: -1` を再設定 |

---

## 14. パフォーマンス目標と実測計画

### 14.1 目標（Phase 2 MVP 完了時）

| 指標 | 目標値 | 測定方法 |
|------|------|---------|
| qwen25-14b-q4 推論レイテンシ（500 tok 入力 / 200 tok 出力） | < 5 秒 | `airflow model exec` 5 回平均 |
| 構造化出力成功率 | ≥ 90% | 同上、`schema_valid=true` の割合 |
| ExecutionLog 書き込みオーバーヘッド | < 50ms | INSERT 前後の差分 |
| 起動時自己診断時間 | < 10 秒 | systemd start → ready |

### 14.2 ベースライン取得

Phase 2 完了時に上記指標を実測 → `docs/baseline/phase2-llm.md` に記録。
Phase 3 以降の改善判定の基準とする。

---

## 15. 検証チェックリスト

Phase 2 MVP 完了時に以下を満たすこと：

- [ ] Ollama が systemd 配下で常駐起動
- [ ] `qwen2.5:14b-instruct-q4_K_M` と `bge-m3` が pull 済み
- [ ] `.airflow/models/registry.json` に MVP 必須セットが登録
- [ ] `airflow doctor` で全項目グリーン
- [ ] `model.execute` が構造化出力で 90%+ 成功
- [ ] ExecutionLog が SQLite に正しく蓄積（audit_id 付き）
- [ ] §12 シナリオが 5 秒以内に完了
- [ ] §13 の障害シナリオが全てエラー処理されている

---

## 16. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| vLLM 切替の互換層実装 | Phase 4 |
| ストリーミング出力（SSE） | Phase 4（インタラクティブ要件発生時） |
| プロンプトテンプレートのバージョニング | Phase 5 |
| Few-shot プロンプトの動的構築（取り込みノートから） | Phase 3 |
| 二重実行モードのデフォルト ON 化（精度測定後） | Phase 3 |
| モデルウォームアップ（起動直後の初回が遅い） | Phase 2 実装中に検討 |

---

## 17. Phase 2 完了サマリ

| DDD | 内容 | 状態 |
|-----|------|------|
| DDD-04 | タスクボード + Vault Ingestion | ✅ |
| DDD-05 | ローカル LLM 実行基盤 | ✅ |

→ Phase 2 MVP 実装に着手可能。
→ MVP 完了基準：DDD-04 §11 シナリオが 30 秒以内、本 DDD §12 シナリオが 5 秒以内、全検証チェックリストグリーン。

---

*次：DDD-06（モデル動的選択・学習）で、本基盤の上に Recommend/Auto モードと精度補完を載せる。*

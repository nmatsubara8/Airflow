# DDD-01：共通データスキーマ仕様書

> Phase 1 横断基盤の中核。全コンポーネントが参照する **JSON Schema 統一定義 + バージョニング規約**。
>
> 参照要件：`requirements-overview.md`, `requirements-ai-team-composition.md`, `requirements-model-orchestration.md`, `requirements-parallel-execution.md`, `requirements-local-markdown-ingestion.md` の全体。

---

## 1. 共通設計原則

### 1.1 全スキーマに必須のメタフィールド

| フィールド | 型 | 必須 | 説明 |
|-----------|---|:---:|------|
| `schema_version` | string (semver) | ✓ | 例 `"1.0.0"`。後方互換破壊で MAJOR を上げる |
| `id` | string (UUID v4 or hash) | ✓ | エンティティの一意識別子 |
| `created_at` | string (RFC3339) | ✓ | 作成時刻（UTC） |
| `updated_at` | string (RFC3339) | ✓ | 最終更新時刻（UTC） |

### 1.2 命名規約

- フィールド名は **`snake_case`**
- 列挙型の値も `snake_case`（例：`in_progress`, `needs_human`）
- ID プレフィックスでエンティティ種別を判別可能に：`task_`, `agent_`, `model_`, `log_`, `note_`

### 1.3 バージョニング規約（NF-10 対応）

- すべてのスキーマファイルに `$id` と `$schema` を明記
- `schema_version` は **semver**：
  - MAJOR：互換性のない変更（必須フィールド削除等）
  - MINOR：後方互換のあるフィールド追加
  - PATCH：説明文・制約値の微修正
- 旧バージョンは **2 つ前まで** 読み取り対応（migration ロジックは Phase 2 で詳細化）

---

## 2. コアスキーマ

### 2.1 Task（タスク）

タスクボードの中心エンティティ。

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://airflow.local/schemas/task/v1.0.0",
  "title": "Task",
  "type": "object",
  "required": [
    "schema_version", "id", "created_at", "updated_at",
    "title", "status", "owner", "priority"
  ],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "id": { "type": "string", "pattern": "^task_[a-f0-9]{8,}$" },
    "created_at": { "type": "string", "format": "date-time" },
    "updated_at": { "type": "string", "format": "date-time" },
    "title": { "type": "string", "minLength": 1, "maxLength": 200 },
    "description": { "type": "string" },
    "status": {
      "type": "string",
      "enum": ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]
    },
    "owner": {
      "type": "object",
      "required": ["type", "id"],
      "properties": {
        "type": { "type": "string", "enum": ["agent", "human"] },
        "id": { "type": "string" }
      }
    },
    "priority": { "type": "integer", "minimum": 1, "maximum": 10 },
    "deadline": { "type": ["string", "null"], "format": "date-time" },
    "tags": { "type": "array", "items": { "type": "string" } },
    "section": {
      "type": "string",
      "enum": ["orchestration", "engineering", "content", "business"]
    },

    "features": {
      "type": "object",
      "description": "モデル選択用タスク特徴ベクトル",
      "properties": {
        "task_type": {
          "type": "string",
          "enum": ["code_gen", "review", "summarize", "classify", "extract", "chat", "test", "document"]
        },
        "language": { "type": "string", "enum": ["ja", "en", "code", "mixed"] },
        "input_size_tokens": { "type": "integer", "minimum": 0 },
        "expected_output_size_tokens": { "type": "integer", "minimum": 0 },
        "required_accuracy": { "type": "string", "enum": ["low", "medium", "high"] },
        "latency_sensitivity": { "type": "string", "enum": ["none", "batch", "interactive"] }
      }
    },

    "scheduling": {
      "type": "object",
      "properties": {
        "enqueued_at": { "type": "string", "format": "date-time" },
        "selected_model_id": { "type": ["string", "null"] },
        "effective_priority": { "type": "number" },
        "starvation_flags": {
          "type": "array",
          "items": { "type": "string", "enum": ["aged", "deadline_near", "force_run"] }
        }
      }
    },

    "handoff_notes": {
      "type": "array",
      "items": { "$ref": "#/$defs/HandoffNote" }
    },

    "source": {
      "type": "object",
      "description": "タスク発生源（Inbox markdown など）",
      "properties": {
        "kind": { "type": "string", "enum": ["inbox_note", "issue", "manual", "schedule", "agent_emitted"] },
        "ref": { "type": "string", "description": "note_id / issue URL / cron name など" }
      }
    },

    "lock": {
      "type": "object",
      "description": "楽観的ロック",
      "properties": {
        "version": { "type": "integer", "minimum": 0 },
        "etag": { "type": "string" }
      }
    }
  },

  "$defs": {
    "HandoffNote": {
      "type": "object",
      "required": ["id", "author", "timestamp", "body"],
      "properties": {
        "id": { "type": "string" },
        "author": {
          "type": "object",
          "required": ["type", "id"],
          "properties": {
            "type": { "type": "string", "enum": ["agent", "human"] },
            "id": { "type": "string" }
          }
        },
        "timestamp": { "type": "string", "format": "date-time" },
        "body": { "type": "string" },
        "kind": {
          "type": "string",
          "enum": ["progress", "result", "escalation", "question", "decision"]
        },
        "references": {
          "type": "array",
          "items": { "type": "string", "description": "note_id / file path / URL" }
        }
      }
    }
  }
}
```

### 2.2 Agent（エージェント定義）

`skills.md` の YAML フロントマターに対応。

```json
{
  "$id": "https://airflow.local/schemas/agent/v1.0.0",
  "type": "object",
  "required": ["schema_version", "id", "section", "platform", "role"],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "id": { "type": "string", "pattern": "^agent_[a-z0-9_-]+$" },
    "section": {
      "type": "string",
      "enum": ["orchestration", "engineering", "content", "business"]
    },
    "platform": {
      "type": "string",
      "enum": ["claude-code", "claude-cowork", "hermes-agent", "custom"]
    },
    "role": { "type": "string" },
    "responsibilities": { "type": "array", "items": { "type": "string" } },
    "inputs": { "type": "array", "items": { "type": "string" } },
    "outputs": { "type": "array", "items": { "type": "string" } },
    "triggers": { "type": "array", "items": { "type": "string" } },
    "escalation_criteria": { "type": "array", "items": { "type": "string" } },
    "preferred_models": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "weight"],
        "properties": {
          "id": { "type": "string" },
          "weight": { "type": "number", "minimum": 0, "maximum": 1 }
        }
      }
    },
    "context_isolation": {
      "type": "string",
      "enum": ["subagent", "separate_session", "shared"]
    },
    "required_accuracy": { "type": "string", "enum": ["low", "medium", "high"] },
    "language": { "type": "string", "enum": ["ja", "en", "code", "mixed"] }
  }
}
```

### 2.3 Model（モデルレジストリ）

```json
{
  "$id": "https://airflow.local/schemas/model/v1.0.0",
  "type": "object",
  "required": [
    "schema_version", "id", "name", "provider", "vram_gb"
  ],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "id": { "type": "string", "pattern": "^model_[a-z0-9._-]+$" },
    "name": { "type": "string" },
    "provider": {
      "type": "string",
      "enum": ["ollama", "vllm", "llama_cpp", "openai", "anthropic", "openrouter"]
    },
    "vram_gb": { "type": "number", "minimum": 0 },
    "quantization": {
      "type": ["string", "null"],
      "enum": ["FP16", "Q8_0", "Q6_K", "Q5_K_M", "Q4_K_M", "Q3_K_M", "Q2_K", null]
    },
    "context_length": { "type": "integer", "minimum": 512 },
    "strengths": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "japanese", "english", "reasoning", "code_generation",
          "code_review", "document", "classify", "extract",
          "summarize", "light_task", "embedding"
        ]
      }
    },
    "avg_latency_ms_per_100tok": { "type": "number", "minimum": 0 },
    "cost_per_1k_tokens": { "type": "number", "minimum": 0 },
    "endpoint": { "type": ["string", "null"], "format": "uri" }
  }
}
```

### 2.4 GpuState（GPU 状態）

スケジューラがリアルタイム参照。

```json
{
  "$id": "https://airflow.local/schemas/gpu_state/v1.0.0",
  "type": "object",
  "required": ["schema_version", "snapshot_at", "total_vram_gb", "used_vram_gb"],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "snapshot_at": { "type": "string", "format": "date-time" },
    "total_vram_gb": { "type": "number" },
    "used_vram_gb": { "type": "number" },
    "free_vram_gb": { "type": "number" },
    "loaded_models": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["model_id", "vram_gb", "loaded_at"],
        "properties": {
          "model_id": { "type": "string" },
          "vram_gb": { "type": "number" },
          "loaded_at": { "type": "string", "format": "date-time" },
          "last_used_at": { "type": "string", "format": "date-time" },
          "pending_tasks": { "type": "integer", "minimum": 0 }
        }
      }
    },
    "active_profile": { "type": ["string", "null"] }
  }
}
```

### 2.5 ExecutionLog（実行ログ）

モデル学習データの源泉。SQLite テーブルの 1 行に相当。

```json
{
  "$id": "https://airflow.local/schemas/execution_log/v1.0.0",
  "type": "object",
  "required": [
    "schema_version", "id", "timestamp", "task_id", "agent_id",
    "model_id", "success"
  ],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "id": { "type": "string", "pattern": "^log_[a-f0-9]{8,}$" },
    "timestamp": { "type": "string", "format": "date-time" },
    "task_id": { "type": "string" },
    "agent_id": { "type": "string" },
    "model_id": { "type": "string" },

    "features_snapshot": {
      "description": "実行時のタスク特徴（Task.features と同形式）",
      "$ref": "task.json#/properties/features"
    },

    "latency_ms": { "type": "integer", "minimum": 0 },
    "input_tokens": { "type": "integer", "minimum": 0 },
    "output_tokens": { "type": "integer", "minimum": 0 },

    "success": { "type": "boolean" },
    "retry_count": { "type": "integer", "minimum": 0 },
    "schema_valid": { "type": "boolean" },

    "auto_score": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
    "human_feedback": {
      "type": ["string", "null"],
      "enum": ["accepted", "modified", "rejected", null]
    },

    "error": {
      "type": ["object", "null"],
      "properties": {
        "class": { "type": "string", "enum": ["recoverable", "escalate", "fatal"] },
        "code": { "type": "string" },
        "message": { "type": "string" }
      }
    },

    "audit_id": { "type": "string", "description": "DDD-03 で定義する監査追跡 ID" }
  }
}
```

### 2.6 Note（取り込み済み Markdown ノート）

ローカル Markdown 取り込み機能用。

```json
{
  "$id": "https://airflow.local/schemas/note/v1.0.0",
  "type": "object",
  "required": ["schema_version", "id", "absolute_path", "content_hash", "last_indexed"],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "id": { "type": "string", "pattern": "^note_[a-f0-9]{16,}$" },
    "absolute_path": { "type": "string" },
    "relative_path": { "type": "string" },
    "title": { "type": ["string", "null"] },
    "category": { "type": ["string", "null"] },
    "confidence": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
    "review_status": { "type": ["string", "null"] },
    "last_verified": { "type": ["string", "null"], "format": "date" },
    "last_indexed": { "type": "string", "format": "date-time" },
    "content_hash": { "type": "string", "pattern": "^sha256:[a-f0-9]{64}$" },
    "confidential": { "type": "boolean", "default": false },
    "sources": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "video_id": { "type": "string" },
          "channel": { "type": "string" },
          "timestamp": { "type": "string" },
          "url": { "type": "string", "format": "uri" }
        }
      }
    },
    "sections": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "heading": { "type": "string" },
          "chunk_ids": { "type": "array", "items": { "type": "string" } }
        }
      }
    },
    "internal_links": { "type": "array", "items": { "type": "string" } }
  }
}
```

---

## 3. スキーマ間の参照関係

```
Task
 ├─ owner ──→ Agent.id
 ├─ scheduling.selected_model_id ──→ Model.id
 ├─ source.ref ──→ Note.id（inbox_note の場合）
 └─ handoff_notes[].references ──→ Note.id / Task.id / file path

ExecutionLog
 ├─ task_id ──→ Task.id
 ├─ agent_id ──→ Agent.id
 ├─ model_id ──→ Model.id
 └─ features_snapshot ←── Task.features のコピー（実行時点の値を凍結）

Agent
 └─ preferred_models[].id ──→ Model.id

GpuState
 └─ loaded_models[].model_id ──→ Model.id
```

---

## 4. ストレージレイアウト

```
.airflow/
├── tasks/                          # Task エンティティ
│   ├── inbox/
│   │   └── task_<id>.json
│   ├── in_progress/
│   ├── done/
│   └── needs_human/
├── agents/                         # Agent 定義
│   ├── tech-lead/skills.md         # YAML frontmatter で Agent スキーマ
│   ├── qa/skills.md
│   └── ...
├── models/registry.json            # Model 配列
├── runtime/
│   ├── gpu_state.json              # 最新の GpuState（上書き更新）
│   └── execution_log.db            # SQLite。各行が ExecutionLog
├── notes/
│   └── index.json                  # Note メタデータ（本体は元の vault パス参照）
└── config/
    ├── parallel.yaml
    ├── model-config.yaml
    └── ingester.yaml
```

タスクは **ステータスごとにフォルダ分離** することで、ファイルシステム監視で状態遷移を検知可能にする（Phase 2 で詳細化）。

---

## 5. 楽観的ロック仕様

要件 NF-02 対応。タスク更新時の競合検出。

### 5.1 ロック構造

```json
"lock": {
  "version": 5,
  "etag": "sha256:abc123..."
}
```

### 5.2 更新プロトコル

1. クライアントは `Task` を読み込み、`lock.version` を記憶
2. 編集後、書き戻し時に `If-Match: version=5` 相当のチェック
3. ストレージ層は現行 `lock.version` と一致を確認
   - 一致：`lock.version` を `+1`、`etag` を再計算して書き込み
   - 不一致：`409 Conflict` 相当のエラーを返す
4. クライアントは再読込してマージ or `needs_human` エスカレーション

### 5.3 ファイルシステム実装での具体策

- ロック取得：`<task_id>.json.lock` をアトミック作成（`O_CREAT | O_EXCL`）
- 取得失敗時：100ms バックオフで最大 3 回リトライ、その後 `409` 相当

---

## 6. バリデーション運用

| タイミング | 実装 |
|----------|------|
| エンティティ書き込み前 | JSON Schema バリデート（python: `jsonschema`、JS: `ajv`） |
| エンティティ読み込み後 | `schema_version` チェック、未対応バージョンは migration or reject |
| CI / リポジトリフック | 全 `*.json` を該当スキーマで検証 |
| エージェント MCP 経由書き込み | MCP サーバー側でバリデート（DDD-02 で詳細化） |

---

## 7. マイグレーション方針

### 7.1 後方互換ルール

- MINOR バージョン：旧コードは新しい追加フィールドを **無視して読める** こと
- MAJOR バージョン：明示的な migration スクリプトを `.airflow/migrations/v1_to_v2/` に配置

### 7.2 凍結タイミング

Phase 2 MVP の実データ検証完了後（4 週目）に `v1.0.0` を凍結。
それまでは `v0.x` を許容し、破壊的変更も可。

---

## 8. JSON Schema ファイル配置

| パス | 内容 |
|------|------|
| `schemas/task.schema.json` | Task |
| `schemas/agent.schema.json` | Agent |
| `schemas/model.schema.json` | Model |
| `schemas/gpu_state.schema.json` | GpuState |
| `schemas/execution_log.schema.json` | ExecutionLog |
| `schemas/note.schema.json` | Note |
| `schemas/handoff_note.schema.json` | HandoffNote（Task から `$ref`） |

各ファイルは独立した `$id` を持ち、相互に `$ref` で参照可能。

---

## 9. オープン課題（Phase 2 以降で詳細化）

| 課題 | 検討するフェーズ |
|------|--------------|
| Migration スクリプトの実装言語と実行タイミング | Phase 2 |
| ベクトル DB（Qdrant）に保存するチャンクのスキーマ | Phase 2 (DDD-04) |
| MCP 経由のスキーマ取得 API（`schema.get(name, version)`） | DDD-02 |
| エラーコード体系の具体化（`code` フィールドの enum 化） | DDD-03 |
| `audit_id` の採番方式・伝播ルール | DDD-03 |

---

## 10. 検証チェックリスト

Phase 1 完了時点で以下を満たすこと：

- [ ] 全 7 スキーマファイル（`schemas/*.schema.json`）が存在
- [ ] 各スキーマに対してサンプル JSON を 1 件以上用意（`schemas/examples/`）
- [ ] `python -m jsonschema -i <example> <schema>` で全例がパス
- [ ] `requirements-*.md` 内に出てくる YAML/JSON 例が本スキーマと整合
- [ ] スキーマ間の `$ref` リンクが解決可能
- [ ] CI でスキーマバリデート自動化（GitHub Actions）

---

*次：DDD-02（MCP インターフェース仕様）で、本スキーマを操作するツール群を定義する。*

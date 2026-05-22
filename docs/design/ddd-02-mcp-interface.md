# DDD-02：MCP インターフェース仕様書

> Phase 1 横断基盤の第 2 弾。DDD-01 で定義したスキーマを操作する **MCP（Model Context Protocol）ツール群** の仕様。
>
> 参照要件：`requirements-overview.md` F-13〜F-15、`requirements-local-markdown-ingestion.md` §6.2、`requirements-model-orchestration.md` §5.2、`requirements-parallel-execution.md` §7.2
> 前提：[[DDD-01：共通データスキーマ仕様書]]

---

## 1. 設計方針

### 1.1 MCP を採用する理由

| 理由 | 詳細 |
|------|------|
| **AI ツール横断アクセス** | Claude Code / Claude Cowork / Hermes Agent が同一データに統一インターフェースでアクセス |
| **ツール定義の自動配布** | エージェント側にコードを書かずに「使えるツール」を増減可能 |
| **権限制御の集約** | サーバー側で認証・認可を一元管理 |
| **スキーマ駆動** | DDD-01 のスキーマを `inputSchema` / `outputSchema` に直結 |

### 1.2 ドメイン分割

5 つの MCP サーバーに分割し、責務とスケールを分離：

| MCP サーバー | ポート | 担当エンティティ | ツール数（目安） |
|------------|-------|--------------|--------------|
| **`taskboard-mcp`** | 7401 | Task / HandoffNote | 8 |
| **`notes-mcp`** | 7402 | Note | 5 |
| **`models-mcp`** | 7403 | Model / ExecutionLog | 7 |
| **`gpu-mcp`** | 7404 | GpuState / Scheduler | 6 |
| **`agents-mcp`** | 7405 | Agent | 4 |

エージェントは必要な MCP サーバーだけを設定ファイルで指定して接続する。

---

## 2. 共通仕様

### 2.1 トランスポート

- **stdio**：ローカル単一ホスト運用時の主経路（Claude Code / Hermes Agent から起動）
- **HTTP/SSE**：複数ホスト・複数エージェントからの同時接続時
- Phase 1 は **stdio + 単一ホスト** を前提とする。HTTP モードは Phase 4 で詳細化。

### 2.2 認証（F-15 対応）

| 項目 | 仕様 |
|------|------|
| 方式 | API Key（環境変数 `AIRFLOW_MCP_TOKEN`） |
| トークン形式 | `airflow_<env>_<24 byte random>`（例：`airflow_local_a1b2c3...`） |
| 検証 | 各 MCP サーバー起動時に共通トークンファイル `.airflow/auth/tokens.json` を参照 |
| ローカル運用 | デフォルトで `localhost` のみ受付。リモート許可は明示設定 |
| ローテーション | `airflow auth rotate` で再生成（Phase 4 で詳細化） |

トークンには **スコープ** を付与可能：

```json
{
  "token": "airflow_local_a1b2c3...",
  "scopes": ["tasks:read", "tasks:write", "notes:read"],
  "agent_id": "agent_qa",
  "expires_at": null
}
```

### 2.3 エラーレスポンス

MCP のエラーは DDD-03 のエラー分類と直結：

```json
{
  "isError": true,
  "content": [{
    "type": "text",
    "text": "{...JSON encoded error...}"
  }],
  "_meta": {
    "error": {
      "class": "recoverable | escalate | fatal",
      "code": "TASK_LOCK_CONFLICT",
      "message": "Task task_abc123 was updated by another process",
      "retry_after_ms": 200,
      "audit_id": "log_xyz789"
    }
  }
}
```

`code` の詳細体系は DDD-03 で定義。MCP 層はそれを透過的に返すだけ。

### 2.4 共通入出力ヘッダ

すべてのツールの入力に以下を許可：

```json
{
  "_meta": {
    "audit_id": "log_xyz789",
    "agent_id": "agent_qa",
    "trace_parent": "..."
  }
}
```

`audit_id` は呼び出し側で生成し、関連ログの追跡に使う（DDD-03 で詳細化）。

### 2.5 ページング

リスト系ツールは以下を共通サポート：

```json
{
  "limit": 50,
  "cursor": "opaque_string"
}
```

レスポンスに `next_cursor`（次ページがあれば）を含める。

### 2.6 バージョニング

- ツール名にバージョンは付けない（互換性は MCP サーバー側で保証）
- `inputSchema` / `outputSchema` は DDD-01 のスキーマを `$ref` で参照
- 破壊的変更時は **新ツール名で並行公開**（例：`tasks.read` と `tasks.read_v2`）

---

## 3. taskboard-mcp（タスクボード）

### 3.1 ツール一覧

| ツール名 | 目的 | スコープ |
|---------|------|---------|
| `tasks.list` | フィルタ付き一覧 | `tasks:read` |
| `tasks.get` | ID 指定で取得 | `tasks:read` |
| `tasks.create` | 新規作成 | `tasks:write` |
| `tasks.update` | 編集（楽観的ロック） | `tasks:write` |
| `tasks.transition` | ステータス遷移 | `tasks:write` |
| `tasks.claim` | エージェントが自分にアサイン | `tasks:write` |
| `tasks.add_handoff_note` | コメント追加 | `tasks:write` |
| `tasks.subscribe` | 状態変化通知（SSE） | `tasks:read` |

### 3.2 個別仕様（抜粋）

#### `tasks.list`

```json
{
  "name": "tasks.list",
  "description": "タスクをフィルタ条件で一覧取得",
  "inputSchema": {
    "type": "object",
    "properties": {
      "status": {
        "type": "array",
        "items": { "type": "string", "enum": ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"] }
      },
      "owner_id": { "type": "string" },
      "section": { "type": "string", "enum": ["orchestration", "engineering", "content", "business"] },
      "tags": { "type": "array", "items": { "type": "string" } },
      "limit": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 },
      "cursor": { "type": "string" }
    }
  },
  "outputSchema": {
    "type": "object",
    "required": ["tasks"],
    "properties": {
      "tasks": { "type": "array", "items": { "$ref": "task.schema.json" } },
      "next_cursor": { "type": ["string", "null"] }
    }
  }
}
```

#### `tasks.update`（楽観的ロック必須）

```json
{
  "name": "tasks.update",
  "inputSchema": {
    "type": "object",
    "required": ["id", "expected_version", "patch"],
    "properties": {
      "id": { "type": "string" },
      "expected_version": { "type": "integer" },
      "patch": {
        "type": "object",
        "description": "JSON Patch (RFC 6902) または部分オブジェクト"
      }
    }
  },
  "outputSchema": { "$ref": "task.schema.json" }
}
```

ロック衝突時は `TASK_LOCK_CONFLICT` エラー + 現行 `lock.version` を返す。

#### `tasks.transition`

ステータス遷移のみを安全に扱う専用ツール。遷移可否は以下：

```
inbox        → in_progress, cancelled
in_progress  → done, needs_human, blocked, cancelled
needs_human  → in_progress, cancelled
blocked      → in_progress, cancelled
done         → (終端)
cancelled    → (終端)
```

不正遷移は `TASK_INVALID_TRANSITION` エラー。

#### `tasks.claim`

エージェントが「自分が処理する」と宣言する操作。

```json
{
  "name": "tasks.claim",
  "inputSchema": {
    "type": "object",
    "required": ["task_id", "agent_id"],
    "properties": {
      "task_id": { "type": "string" },
      "agent_id": { "type": "string" }
    }
  }
}
```

サーバー側で：
1. `owner` が `null` または同じ `agent_id` でなければ拒否
2. `status` が `inbox` でなければ拒否
3. アトミックに `owner` 更新 + `status → in_progress` 遷移

#### `tasks.subscribe`

状態変化のストリーミング通知（SSE）。Phase 4 でリアルタイム性が必要になった時に有効化。Phase 1〜3 は **ポーリング前提**。

---

## 4. notes-mcp（ローカル Markdown）

### 4.1 ツール一覧

| ツール名 | 目的 |
|---------|------|
| `notes.search` | セマンティック + メタデータフィルタ検索 |
| `notes.get` | ID 指定で全文取得 |
| `notes.list_by_category` | カテゴリでフィルタ |
| `notes.related` | 内部リンクから関連取得 |
| `notes.reindex` | フォルダ再インデックス（管理用） |

### 4.2 個別仕様（抜粋）

#### `notes.search`

```json
{
  "name": "notes.search",
  "description": "ベクトル検索 + メタデータフィルタでノートを検索",
  "inputSchema": {
    "type": "object",
    "required": ["query"],
    "properties": {
      "query": { "type": "string", "minLength": 1 },
      "filters": {
        "type": "object",
        "properties": {
          "category": { "type": "array", "items": { "type": "string" } },
          "min_confidence": { "type": "number", "minimum": 0, "maximum": 1 },
          "exclude_confidential": { "type": "boolean", "default": true }
        }
      },
      "top_k": { "type": "integer", "minimum": 1, "maximum": 50, "default": 10 }
    }
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "results": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "note_id": { "type": "string" },
            "title": { "type": "string" },
            "snippet": { "type": "string" },
            "score": { "type": "number" },
            "section_heading": { "type": ["string", "null"] }
          }
        }
      }
    }
  }
}
```

#### `notes.related`

```json
{
  "name": "notes.related",
  "inputSchema": {
    "type": "object",
    "required": ["note_id"],
    "properties": {
      "note_id": { "type": "string" },
      "depth": { "type": "integer", "minimum": 1, "maximum": 3, "default": 1 }
    }
  }
}
```

Obsidian `[[内部リンク]]` を辿って関連ノートを返す。

---

## 5. models-mcp（モデルレジストリ・学習）

### 5.1 ツール一覧

| ツール名 | 目的 |
|---------|------|
| `model.list` | 登録済みモデル一覧 |
| `model.get` | ID 指定 |
| `model.recommend` | タスク特徴から推奨モデル |
| `model.execute` | 指定モデルで推論実行（薄いラッパー） |
| `model.feedback` | 実行結果フィードバック登録 |
| `model.stats` | モデル別統計 |
| `model.register` | 新規モデル登録（管理用） |

### 5.2 `model.recommend`

```json
{
  "name": "model.recommend",
  "description": "タスク特徴から推奨モデルと信頼度を返す",
  "inputSchema": {
    "type": "object",
    "required": ["features"],
    "properties": {
      "features": { "$ref": "task.schema.json#/properties/features" },
      "mode": { "type": "string", "enum": ["manual", "recommend", "auto"], "default": "recommend" },
      "candidate_models": { "type": "array", "items": { "type": "string" } }
    }
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "recommendations": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "model_id": { "type": "string" },
            "score": { "type": "number" },
            "expected_accuracy": { "type": "number" },
            "swap_cost_ms": { "type": "integer" },
            "confidence": { "type": "number", "description": "推奨自体の信頼度（過去サンプル数依存）" },
            "reason": { "type": "string" }
          }
        }
      },
      "sample_count": { "type": "integer", "description": "推奨根拠となった過去実績数" }
    }
  }
}
```

### 5.3 `model.execute`

**重要**：MCP は **薄いラッパー** に徹し、実際の推論は Ollama/vLLM に委譲。

```json
{
  "name": "model.execute",
  "inputSchema": {
    "type": "object",
    "required": ["model_id", "messages"],
    "properties": {
      "model_id": { "type": "string" },
      "messages": { "type": "array" },
      "structured_output": {
        "type": "object",
        "description": "JSON Schema を渡すと構造化出力を強制",
        "properties": {
          "schema": { "type": "object" }
        }
      },
      "max_tokens": { "type": "integer" },
      "temperature": { "type": "number" },
      "task_id": { "type": ["string", "null"], "description": "実行ログ紐付け用" }
    }
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "content": { "type": "string" },
      "parsed": { "type": ["object", "null"], "description": "structured_output 指定時にパース済み" },
      "execution_log_id": { "type": "string" }
    }
  }
}
```

実行ログは自動的に `ExecutionLog` として記録される。

---

## 6. gpu-mcp（GPU 状態・スケジューラ）

### 6.1 ツール一覧

| ツール名 | 目的 |
|---------|------|
| `gpu.state` | 現在の GPU 状態スナップショット |
| `gpu.reserve` | モデルロード要求 |
| `gpu.release` | モデルアンロード |
| `scheduler.queue_status` | 各キューの深度・待機時間 |
| `scheduler.profile_switch` | プロファイル切替 |
| `scheduler.degrade` | デグラデーションモード強制 |

### 6.2 `gpu.state`

```json
{
  "name": "gpu.state",
  "description": "現在の GPU メモリ・ロード済みモデルを返す",
  "inputSchema": { "type": "object" },
  "outputSchema": { "$ref": "gpu_state.schema.json" }
}
```

### 6.3 `gpu.reserve`

```json
{
  "name": "gpu.reserve",
  "description": "モデルロードを要求。VRAM 不足時は LRU で退避",
  "inputSchema": {
    "type": "object",
    "required": ["model_id"],
    "properties": {
      "model_id": { "type": "string" },
      "wait": { "type": "boolean", "default": true, "description": "false なら即座に GPU_BUSY を返す" },
      "timeout_ms": { "type": "integer", "default": 60000 }
    }
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "reserved": { "type": "boolean" },
      "loaded_at": { "type": "string", "format": "date-time" },
      "evicted_models": { "type": "array", "items": { "type": "string" } }
    }
  }
}
```

---

## 7. agents-mcp（エージェント管理）

### 7.1 ツール一覧

| ツール名 | 目的 |
|---------|------|
| `agents.list` | 登録済みエージェント一覧 |
| `agents.get` | ID 指定で skills.md 取得 |
| `agents.find_for_task` | タスク特徴から担当候補を推定 |
| `agents.register` | エージェント新規登録 |

### 7.2 `agents.find_for_task`

タスクディスパッチが利用。タスク特徴・タグから担当エージェント候補を返す。

```json
{
  "name": "agents.find_for_task",
  "inputSchema": {
    "type": "object",
    "required": ["task"],
    "properties": {
      "task": { "$ref": "task.schema.json" }
    }
  },
  "outputSchema": {
    "type": "object",
    "properties": {
      "candidates": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "agent_id": { "type": "string" },
            "score": { "type": "number" },
            "reason": { "type": "string" }
          }
        }
      }
    }
  }
}
```

---

## 8. スコープと権限マトリクス

| エージェント種別 | 推奨スコープ |
|--------------|------------|
| Engineering エージェント（実装系） | `tasks:read`, `tasks:write`, `notes:read`, `models:execute`, `gpu:reserve` |
| Reviewer / QA | `tasks:read`, `tasks:write`, `models:execute` |
| Cleaner Agent | `tasks:read`, `tasks:write` (delete 含む), `notes:read` |
| ディスパッチエージェント | `tasks:write`, `notes:read`, `agents:read` |
| 管理用 CLI | `*:*` |

各 MCP ツールは `requiredScope` を持ち、認証時に検証する。

---

## 9. サーバー構成例

### 9.1 stdio モード（Phase 1〜3）

エージェント設定ファイル（例：`.airflow/agents/qa/mcp_config.json`）：

```json
{
  "mcpServers": {
    "taskboard": {
      "command": "airflow-mcp",
      "args": ["taskboard"],
      "env": { "AIRFLOW_MCP_TOKEN": "${TOKEN_QA}" }
    },
    "models": {
      "command": "airflow-mcp",
      "args": ["models"],
      "env": { "AIRFLOW_MCP_TOKEN": "${TOKEN_QA}" }
    },
    "gpu": {
      "command": "airflow-mcp",
      "args": ["gpu"],
      "env": { "AIRFLOW_MCP_TOKEN": "${TOKEN_QA}" }
    }
  }
}
```

### 9.2 HTTP モード（Phase 4 以降）

複数ホスト構成時に追加。詳細は DDD-07 で扱う。

---

## 10. 検証チェックリスト

Phase 1 完了時点で以下を満たすこと：

- [ ] 5 つの MCP サーバーが stdio で起動可能（スタブ実装で OK）
- [ ] 主要ツール（`tasks.create/update/transition/list`, `notes.search`, `model.recommend`, `gpu.state`）の `inputSchema` が DDD-01 スキーマと整合
- [ ] 認証トークン未指定でエラーになる
- [ ] スコープ違反でエラーになる
- [ ] 楽観的ロック衝突が正しく検出される
- [ ] エラーレスポンスが DDD-03 形式に整合（DDD-03 完成後に再検証）

---

## 11. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| HTTP/SSE モードの詳細実装 | Phase 4 |
| `audit_id` の自動採番ルール | DDD-03 |
| エラーコード列挙の完全化 | DDD-03 |
| ベクトル検索バックエンド（Qdrant vs Chroma）の選定 | DDD-04 |
| `model.execute` の構造化出力強制実装 | DDD-05 |
| トークンローテーション運用 | Phase 5 |

---

*次：DDD-03（エラー処理・ログ設計）で、本仕様の `_meta.error` と `audit_id` を具体化する。*

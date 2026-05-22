# DDD-03：エラー処理・ログ設計

> Phase 1 横断基盤の第 3 弾（最終）。エラー分類・リトライ規約・`needs_human` 遷移条件・構造化ログ・監査 ID を統一定義。
>
> 参照要件：`requirements-overview.md` F-04/F-10/NF-12、`requirements-ai-team-composition.md` クロスセクション・エスカレーションルール、`requirements-local-oss-config.md` §3.2「精度補完メカニズム」
> 前提：[[DDD-01：共通データスキーマ仕様書]]、[[DDD-02：MCP インターフェース仕様書]]

---

## 1. 設計方針

### 1.1 3 つの分離原則

| 分離 | 内容 |
|------|------|
| **エラー分類とハンドリングの分離** | エラー発生コードは「何が起きたか」だけを返し、リトライ/エスカレーション判断は呼出側 or ポリシー層が決める |
| **ログとタスクボードの分離** | タスクボード（Task の handoff_notes）は **業務的な引き継ぎ**、ログは **実行・監査** 用（NF-12） |
| **トレースとログの分離** | `audit_id` は実行単位の追跡 ID、ログはイベント単位。N:1 で紐付く |

### 1.2 既存要件との対応

| 既存要件 | 本 DDD での扱い |
|---------|--------------|
| F-04（needs-human エスカレーション） | §5 で遷移条件を確定 |
| F-10（スコープ外判断の停止） | §5 のエスカレーション基準に含む |
| NF-12（ログを独立ストレージへ） | §7 ストレージ分離 |
| 精度補完メカニズム（リトライ・二重実行） | §4 リトライポリシーで吸収 |

---

## 2. エラー分類体系

### 2.1 3 クラス分類

| クラス | 意味 | デフォルト対応 |
|-------|------|------------|
| **`recoverable`** | 一時的・自動回復可能（ネットワーク瞬断、ロック競合、GPU 一時占有） | 自動リトライ |
| **`escalate`** | 業務判断が必要（スコープ外、信頼度低、ポリシー違反疑い） | `needs_human` に遷移 |
| **`fatal`** | 回復不能（スキーマ違反、認証失敗、設定不整合） | タスクを `blocked` に遷移、運用者通知 |

呼出側はクラスだけ見れば一次対応を判断できる。コード詳細は監査・分析用。

### 2.2 エラーレスポンス構造

DDD-02 §2.3 で示した形式をここで完成させる：

```json
{
  "class": "recoverable",
  "code": "TASK_LOCK_CONFLICT",
  "message": "Task task_abc123 was updated by another process (expected v5, got v7)",
  "details": {
    "task_id": "task_abc123",
    "expected_version": 5,
    "current_version": 7
  },
  "retry_after_ms": 200,
  "retry_max_attempts": 3,
  "audit_id": "log_xyz789",
  "occurred_at": "2026-05-22T15:30:00Z",
  "source": {
    "component": "taskboard-mcp",
    "tool": "tasks.update"
  }
}
```

### 2.3 エラーコード命名規約

- 全大文字 `SNAKE_CASE`
- プレフィックスでドメイン判別：`TASK_*`, `NOTE_*`, `MODEL_*`, `GPU_*`, `AUTH_*`, `SCHEMA_*`, `AGENT_*`, `INTERNAL_*`

### 2.4 標準エラーコード一覧

#### TASK_*
| コード | クラス | 意味 |
|-------|------|------|
| `TASK_NOT_FOUND` | escalate | 指定 ID のタスクが存在しない |
| `TASK_LOCK_CONFLICT` | recoverable | 楽観的ロック衝突 |
| `TASK_INVALID_TRANSITION` | fatal | 不正なステータス遷移要求 |
| `TASK_ALREADY_CLAIMED` | recoverable | 他エージェントが既に claim 済み |
| `TASK_SCOPE_VIOLATION` | escalate | エージェントがスコープ外判断を要求された |

#### MODEL_*
| コード | クラス | 意味 |
|-------|------|------|
| `MODEL_NOT_FOUND` | fatal | レジストリ未登録モデル |
| `MODEL_EXECUTION_FAILED` | recoverable | 推論実行失敗（タイムアウト含む） |
| `MODEL_STRUCTURED_OUTPUT_INVALID` | recoverable | JSON Schema 不適合 |
| `MODEL_LOW_CONFIDENCE` | escalate | 推奨信頼度が閾値未満 |

#### GPU_*
| コード | クラス | 意味 |
|-------|------|------|
| `GPU_INSUFFICIENT_VRAM` | recoverable | VRAM 不足（LRU 退避で解消可能性あり） |
| `GPU_RESERVE_TIMEOUT` | recoverable | リザーブ待機がタイムアウト |
| `GPU_OOM` | fatal | 実行中に OOM 発生 |

#### NOTE_*
| コード | クラス | 意味 |
|-------|------|------|
| `NOTE_NOT_FOUND` | escalate | 指定 ID のノートが存在しない |
| `NOTE_PARSE_ERROR` | recoverable | YAML フロントマター解析失敗 |
| `NOTE_CONFIDENTIAL_ACCESS_DENIED` | fatal | confidential ノートへの不正アクセス |

#### AUTH_*
| コード | クラス | 意味 |
|-------|------|------|
| `AUTH_TOKEN_MISSING` | fatal | トークン未指定 |
| `AUTH_TOKEN_INVALID` | fatal | トークン不正 |
| `AUTH_SCOPE_INSUFFICIENT` | fatal | 必要スコープ不足 |

#### SCHEMA_*
| コード | クラス | 意味 |
|-------|------|------|
| `SCHEMA_VALIDATION_FAILED` | fatal | 入出力スキーマ違反 |
| `SCHEMA_VERSION_UNSUPPORTED` | fatal | 未対応バージョン |

#### INTERNAL_*
| コード | クラス | 意味 |
|-------|------|------|
| `INTERNAL_TIMEOUT` | recoverable | 内部処理タイムアウト |
| `INTERNAL_UNEXPECTED` | fatal | 想定外のサーバーエラー |

---

## 3. リトライポリシー

### 3.1 リトライ判定マトリクス

| エラークラス | 既定動作 | バックオフ | 最大試行回数 |
|-----------|---------|----------|-----------|
| `recoverable` | 自動リトライ | exponential（200ms 起点、×2） | 3 |
| `escalate` | リトライしない | — | — |
| `fatal` | リトライしない | — | — |

### 3.2 エラーごとのオーバーライド

エラーレスポンスの `retry_after_ms` と `retry_max_attempts` が指定されていれば優先：

```json
{
  "class": "recoverable",
  "code": "GPU_INSUFFICIENT_VRAM",
  "retry_after_ms": 5000,
  "retry_max_attempts": 5
}
```

### 3.3 全試行失敗時の振る舞い

リトライ上限到達後の元クラスを **昇格** する：

```
recoverable × 試行回数超過  →  escalate に昇格
escalate                    →  needs_human 遷移
fatal                        →  blocked 遷移 + 運用者通知
```

### 3.4 二重実行モード（精度補完）

`required_accuracy = high` のタスクは MCP 層で **自動的に 2 回実行** し、結果を比較：

| 結果 | 動作 |
|------|------|
| 一致 | 成功扱い、`auto_score = 1.0` |
| 部分一致（類似度 ≥ 0.85） | 成功扱い、`auto_score = 0.85` |
| 不一致 | `MODEL_LOW_CONFIDENCE` で `escalate` |

実装は DDD-06（モデルオーケストレーション）で詳細化。本 DDD ではエラー伝播ルールのみ定義。

---

## 4. `needs_human` 遷移条件（F-04, F-10 対応）

### 4.1 遷移トリガー（自動）

以下のいずれかで自動的に `needs_human` に遷移：

1. エラークラスが `escalate` のエラーが発生
2. `recoverable` エラーがリトライ上限を超過
3. エージェントの `escalation_criteria` に合致（skills.md で個別定義）
4. 二重実行モードで結果不一致
5. `model.recommend` の `confidence < 0.5` かつ `mode != manual`

### 4.2 遷移時の必須情報

`tasks.transition` で `needs_human` に遷移させる際は、handoff_note を **必須**：

```json
{
  "tasks.transition": {
    "id": "task_abc",
    "to_status": "needs_human",
    "handoff_note": {
      "kind": "escalation",
      "body": "契約書 §3.2 の損害賠償条項に判例適合性の判断が必要。AI の判断信頼度が 0.4 と低いため人間レビューを要求。",
      "references": ["note_def456"]
    }
  }
}
```

`handoff_note` 不在の `needs_human` 遷移は `TASK_INVALID_TRANSITION` エラーで拒否。

### 4.3 通知

`needs_human` 遷移と同時に以下を実施：

- 構造化ログに `escalation` イベント記録
- 設定済み通知チャンネル（Slack / メール / CLI）に通知（Phase 5 で実装）
- Phase 1〜3 はログ + ファイル監視で代用

---

## 5. `audit_id` 仕様

### 5.1 採番

| 採番者 | 形式 | タイミング |
|-------|------|----------|
| タスク発生時に **自動採番** | `log_<ULID>` | タスク `create` 時に MCP サーバーが採番 |
| エージェント実行ごとに **派生 ID** 採番 | `log_<ULID>` + `parent_audit_id` | エージェントが MCP 呼出を開始する時 |

ULID 採用理由：時系列ソート可能 + UUID 並みの一意性。

### 5.2 伝播ルール

```
タスク作成
   audit_id = log_01HK...   ← トップレベル

   ↓ エージェントが処理開始
   audit_id = log_01HM...
   parent_audit_id = log_01HK...

   ↓ 内部で MCP 呼出
   audit_id = log_01HN...
   parent_audit_id = log_01HM...

   ↓ さらにモデル推論
   audit_id = log_01HP...
   parent_audit_id = log_01HN...
```

各 MCP ツールは `_meta.audit_id` を受け取り：
- 新規 ID を生成して `parent_audit_id` に元 ID を記録
- ログ・ExecutionLog にすべての階層 ID を記録

### 5.3 横串検索

`audit_id` をキーに：
- そのタスクから派生した全実行ログを取得
- 失敗した実行を遡って原因タスクに到達
- 監査時に「いつ・誰が・何のために」を再現

---

## 6. 構造化ログ仕様

### 6.1 ログレコードスキーマ

```json
{
  "$id": "https://airflow.local/schemas/log_event/v1.0.0",
  "type": "object",
  "required": ["timestamp", "level", "event", "audit_id", "component"],
  "properties": {
    "schema_version": { "type": "string", "const": "1.0.0" },
    "timestamp": { "type": "string", "format": "date-time" },
    "level": { "type": "string", "enum": ["debug", "info", "warn", "error", "critical"] },
    "event": { "type": "string", "description": "ドット区切りイベント名 例: task.created, model.executed" },
    "audit_id": { "type": "string" },
    "parent_audit_id": { "type": ["string", "null"] },
    "component": { "type": "string", "enum": ["taskboard-mcp", "notes-mcp", "models-mcp", "gpu-mcp", "agents-mcp", "scheduler", "dispatcher", "cleaner", "ingester", "cli"] },
    "task_id": { "type": ["string", "null"] },
    "agent_id": { "type": ["string", "null"] },
    "model_id": { "type": ["string", "null"] },
    "duration_ms": { "type": ["integer", "null"] },
    "error": { "type": ["object", "null"], "description": "§2.2 のエラーレスポンス構造" },
    "data": { "type": "object", "description": "イベント固有のペイロード" }
  }
}
```

### 6.2 標準イベント名

ドット区切りで階層化：

| 名前空間 | 例 |
|--------|-----|
| `task.*` | `task.created`, `task.claimed`, `task.transitioned`, `task.handoff_added` |
| `model.*` | `model.recommended`, `model.executed`, `model.feedback_received` |
| `gpu.*` | `gpu.reserved`, `gpu.evicted`, `gpu.oom` |
| `note.*` | `note.indexed`, `note.searched` |
| `scheduler.*` | `scheduler.profile_switched`, `scheduler.degraded`, `scheduler.starvation_detected` |
| `escalation.*` | `escalation.triggered`, `escalation.resolved` |
| `agent.*` | `agent.started`, `agent.finished`, `agent.escalated` |

### 6.3 ログレベル指針

| レベル | 用途 | 想定頻度 |
|-------|------|--------|
| `debug` | 開発時のみ。本番では無効 | 高 |
| `info` | 通常運用のマイルストーン（作成・完了等） | 中 |
| `warn` | リトライ発生、フォールバック発動 | 低 |
| `error` | エラー発生（`recoverable`/`escalate`） | 低 |
| `critical` | `fatal` エラー、システム継続不能 | 極低 |

---

## 7. ストレージ分離（NF-12 対応）

### 7.1 物理分離

| 種別 | 保存先 | 理由 |
|------|-------|------|
| **タスク・handoff_notes** | `.airflow/tasks/*.json` | 業務データ。Git 管理可能 |
| **構造化ログ** | `.airflow/logs/<YYYY-MM-DD>.jsonl` | 実行イベント。ローテーション対象 |
| **ExecutionLog（SQLite）** | `.airflow/runtime/execution_log.db` | 学習データ。クエリ性能優先 |
| **エラーログ（重要のみ）** | `.airflow/logs/errors-<YYYY-MM>.jsonl` | 監査時の高速参照 |

### 7.2 ローテーション

- 構造化ログ：日次ローテーション、`gzip` 圧縮、90 日後に削除（設定変更可）
- ExecutionLog DB：無期限保持。サイズが 1GB を超えたら古いデータをパーティション分離

### 7.3 改ざん耐性（Phase 5 で詳細化）

- 監査要件が出た場合、日次ログのハッシュチェーンを別途記録
- Phase 1〜3 では実装しない

---

## 8. CLI / API

### 8.1 ログ参照 CLI

```bash
# 直近イベント
airflow logs tail

# audit_id で横串検索
airflow logs trace log_01HK...

# エラーのみ
airflow logs filter --level error --since 1h

# 構造化検索
airflow logs query --event 'task.*' --task-id task_abc
```

### 8.2 MCP ツール追加（taskboard-mcp に統合）

| ツール名 | 目的 |
|---------|------|
| `audit.trace` | `audit_id` 起点で関連イベント全件取得 |
| `audit.query` | 構造化フィルタによる検索 |

---

## 9. シナリオ例

### 9.1 ロック衝突 → リトライ成功

```
[t=0] qa エージェントが tasks.update(task_abc, expected_version=5)
[t=0] サーバー: 現在 v=5、更新成功、v=6 返却

[t=10] reviewer エージェントが同じタスクを tasks.update(task_abc, expected_version=5)
[t=10] サーバー: TASK_LOCK_CONFLICT (current=6)
[t=10] reviewer: recoverable なので 200ms 待ってリトライ
[t=210] reviewer: tasks.get で最新 (v=6) を取得 → マージ → tasks.update(expected_version=6)
[t=210] サーバー: 更新成功、v=7

→ ログ: warn level "task.update_retry" 1 件
```

### 9.2 二重実行不一致 → エスカレーション

```
[t=0] brand-voice エージェントが model.execute(required_accuracy=high)
[t=0] サーバー: 2 回実行 → 類似度 0.6
[t=0] サーバー: MODEL_LOW_CONFIDENCE escalate エラー

[t=0] エージェント: escalate なのでリトライしない
[t=0] エージェント: tasks.transition(needs_human, handoff_note="二重実行で結果が大きく異なるため人間レビュー要求")

→ ログ: error "escalation.triggered", task.transition needs_human
```

### 9.3 GPU OOM → 致命

```
[t=0] qa エージェントが gpu.reserve(qwen-32b)
[t=0] 推論実行中に GPU_OOM 発生
[t=0] gpu-mcp: critical "gpu.oom" ログ
[t=0] エージェント: fatal なので tasks.transition(blocked, handoff_note="GPU OOM")
[t=0] 運用者通知（Phase 5 では Slack）
```

---

## 10. 検証チェックリスト

Phase 1 完了時点で以下を満たすこと：

- [ ] エラーレスポンスのスキーマが全 MCP ツールで統一
- [ ] 標準エラーコード一覧が DDD-02 各ツールにマッピング済み
- [ ] `audit_id` が ULID で採番され、parent 伝播が動作する
- [ ] 構造化ログが JSONL で `.airflow/logs/` に出力される
- [ ] `needs_human` 遷移時に handoff_note 必須が強制される
- [ ] `airflow logs trace` で audit_id 起点の追跡ができる
- [ ] ストレージ分離（タスク / ログ / 学習データ）が物理的に分離されている

---

## 11. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| 通知チャンネル（Slack/メール）統合 | Phase 5 |
| ログのハッシュチェーン改ざん耐性 | Phase 5（監査要件発生時） |
| トレース可視化 UI（OpenTelemetry 連携） | Phase 5 |
| 二重実行の類似度計算方法詳細 | DDD-06 |
| エスカレーション解決後の再開フロー | DDD-08 |

---

## 12. Phase 1 完了サマリ

Phase 1（横断基盤）は本 DDD-03 をもって完了：

| DDD | 内容 | 状態 |
|-----|------|------|
| DDD-01 | 共通データスキーマ（Task/Agent/Model/GpuState/ExecutionLog/Note/HandoffNote） | ✅ |
| DDD-02 | MCP インターフェース（5 サーバー、約 30 ツール） | ✅ |
| DDD-03 | エラー処理・ログ設計（3 クラス分類、audit_id、構造化ログ） | ✅ |

→ Phase 2（MVP コア）に着手可能。

---

*次：DDD-04（タスクボード + Vault Ingestion 詳細）で、本基盤の上に MVP の実装設計を載せる。*

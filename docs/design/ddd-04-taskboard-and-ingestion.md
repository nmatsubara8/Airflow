# DDD-04：タスクボード + Vault Ingestion 詳細設計

> Phase 2 MVP コア。Git+JSON によるタスクボード実装と、Obsidian Vault からのローカル `.md` 取り込みを **動かせる粒度** で設計する。
>
> 参照要件：`requirements-overview.md` F-01〜F-05、`requirements-local-markdown-ingestion.md` 全般（特に F-L01〜L11、F-L26〜L28）
> 前提：[[DDD-01：共通データスキーマ仕様書]]、[[DDD-02：MCP インターフェース仕様書]]、[[DDD-03：エラー処理・ログ設計]]

---

## 1. スコープ

### 1.1 含む

- タスクボードのストレージレイアウトとファイル操作
- 楽観的ロックの実装詳細
- ファイル監視によるステータス遷移検知
- Vault ディレクトリ監視と `.md` ファイルの取り込み
- フロントマター解析・チャンク化・ベクトル DB 連携
- Inbox からのタスク自動生成

### 1.2 含まない（後続 DDD）

- LLM 実行ラッパー → DDD-05
- モデル選択ロジック → DDD-06
- 並列スケジューラ → DDD-07

---

## 2. タスクボードのストレージ実装

### 2.1 ディレクトリレイアウト

```
.airflow/
└── tasks/
    ├── _index.json                # 全タスク一覧（高速参照用）
    ├── inbox/
    │   └── task_<ulid>.json
    ├── in_progress/
    │   └── task_<ulid>.json
    ├── done/
    │   └── YYYY-MM/               # 月別アーカイブ
    │       └── task_<ulid>.json
    ├── needs_human/
    │   └── task_<ulid>.json
    ├── blocked/
    │   └── task_<ulid>.json
    └── cancelled/
        └── YYYY-MM/
            └── task_<ulid>.json
```

**設計理由**：
- ステータスをフォルダで物理分離 → `ls tasks/inbox/` で in-flight タスクが即時把握
- `done` / `cancelled` は月別アーカイブで肥大化防止
- `_index.json` は全件サマリのインメモリキャッシュとしても活用

### 2.2 タスクファイルの実体

`task_<ulid>.json` の中身は DDD-01 §2.1 の Task スキーマに準拠。例：

```json
{
  "schema_version": "1.0.0",
  "id": "task_01HK8E5J7M3N4Q5R6S7T8V9W0X",
  "created_at": "2026-05-22T14:00:00Z",
  "updated_at": "2026-05-22T14:05:00Z",
  "title": "Obsidian ノートを要約",
  "status": "in_progress",
  "owner": { "type": "agent", "id": "agent_docs" },
  "priority": 5,
  "section": "engineering",
  "features": {
    "task_type": "summarize",
    "language": "ja",
    "input_size_tokens": 2400,
    "required_accuracy": "medium",
    "latency_sensitivity": "batch"
  },
  "source": {
    "kind": "inbox_note",
    "ref": "note_a1b2c3d4e5f6g7h8"
  },
  "handoff_notes": [],
  "lock": { "version": 1, "etag": "sha256:..." }
}
```

### 2.3 状態遷移のファイル操作

ステータス遷移は **ファイル移動** で実装：

| 遷移 | 実装 |
|------|------|
| `inbox → in_progress` | `mv tasks/inbox/task_X.json tasks/in_progress/task_X.json` |
| `* → done` | `mv tasks/<src>/task_X.json tasks/done/<YYYY-MM>/task_X.json` |
| `* → needs_human` | `mv tasks/<src>/task_X.json tasks/needs_human/task_X.json` |

ファイル移動は **同一ファイルシステム内で atomic**（POSIX `rename(2)`）。

### 2.4 `_index.json` の運用

```json
{
  "schema_version": "1.0.0",
  "updated_at": "2026-05-22T15:00:00Z",
  "tasks": [
    {
      "id": "task_01HK...",
      "title": "Obsidian ノートを要約",
      "status": "in_progress",
      "owner_id": "agent_docs",
      "priority": 5,
      "updated_at": "2026-05-22T14:05:00Z"
    }
  ]
}
```

- 更新は taskboard-mcp サーバーが一元管理
- 起動時にディレクトリスキャンで再構築可能
- 読み取り専用クライアントはこれを参照することでファイル全読込を回避

### 2.5 Git 連携

- `.airflow/tasks/` 配下は **Git 管理対象**
- コミット粒度：原則 1 タスク 1 コミット（フックで自動化、Phase 5）
- Phase 2 MVP では手動コミット可。`.gitignore` に `.airflow/runtime/` `.airflow/logs/` を含める

---

## 3. 楽観的ロック実装（NF-02）

### 3.1 ロックの 3 層構造

| 層 | 役割 |
|---|------|
| **L1：プロセス内ロック** | 同一 MCP サーバー内の goroutine/thread 衝突防止（`sync.Mutex` 等） |
| **L2：ファイルロック** | 別プロセスからの同時書き込み防止（`<task_id>.json.lock` ファイル） |
| **L3：楽観的バージョン** | クライアント間の論理的競合検出（`lock.version`） |

### 3.2 更新フロー（taskboard-mcp `tasks.update`）

```
1. クライアント: tasks.get(task_X) → version=5 取得
2. クライアント: ローカルで編集
3. クライアント: tasks.update(task_X, expected_version=5, patch=...)
4. サーバー:
   a. L2 ロック取得: open(task_X.json.lock, O_CREAT|O_EXCL)
      失敗時 → 100ms バックオフ ×3 → TASK_LOCK_CONFLICT
   b. ディスクから現行 task_X.json を読み込み
   c. current_version 確認
      不一致 → TASK_LOCK_CONFLICT (current_version=N を含める)
   d. patch 適用、version+=1、etag 再計算
   e. tasks/<status>/task_X.json.new に書き込み
   f. atomic rename: task_X.json.new → task_X.json
   g. _index.json 更新
   h. L2 ロックファイル削除
5. ログ: info "task.updated"
```

### 3.3 ロックタイムアウトとクラッシュ復旧

- `.lock` ファイルには採番プロセスの PID と取得時刻を記録
- 起動時クリーンアップ：`now - created_at > 30 秒` かつ PID が存在しないロックは削除
- このスキャンは `airflow taskboard repair` でも手動実行可能

### 3.4 etag の計算

```python
etag = "sha256:" + sha256(canonical_json(task_without_lock_field))
```

- `lock` フィールドを除外して計算
- canonical JSON：キーをアルファベットソート、空白なし

---

## 4. ポーリング機構

### 4.1 エージェントのポーリングフロー

```
loop:
  tasks = taskboard.tasks.list(
    status=["inbox"],
    owner_id=self.agent_id OR null,
    limit=10
  )

  for task in tasks:
    if matches_my_skills(task):
      result = taskboard.tasks.claim(task.id, self.agent_id)
      if result.success:
        process(task)
        break

  sleep(poll_interval)
```

### 4.2 ポーリング間隔の指針

| 環境 | 間隔 |
|------|------|
| 開発・MVP | 5 秒 |
| バッチエージェント（夜間 QA 等） | 30 秒 |
| インタラクティブエージェント | 2 秒 |

Phase 4 で `tasks.subscribe`（SSE）に置き換え可能だが、Phase 2 MVP はポーリングで十分。

### 4.3 ファイル監視による補助

taskboard-mcp サーバーは内部で `inotify` / `fsnotify` で `.airflow/tasks/` を監視し、`_index.json` を即時更新。これにより：

- エージェントのポーリングが常に最新ビューを取得
- 外部ツール（Git pull で他人の変更を取り込んだ場合等）にも追従

---

## 5. Vault Ingestion 実装

### 5.1 監視対象の設定

`.airflow/config/ingester.yaml`：

```yaml
schema_version: "1.0.0"
watch_paths:
  - path: "/mnt/d/vaults/youtube-knowledge/notes/Airflow"
    recursive: true
    mode: realtime              # realtime | scheduled
    auto_dispatch: false

  - path: "/mnt/d/vaults/youtube-knowledge/notes/Airflow/Inbox"
    recursive: false
    mode: realtime
    auto_dispatch: true          # Inbox は新規ファイル → タスク自動生成

exclude:
  - ".obsidian/**"
  - "**/.trash/**"
  - "**/*.tmp"
  - "**/~$*"

vector_db:
  backend: qdrant                # qdrant | chroma
  endpoint: "http://localhost:6333"
  collection: "airflow_notes"
  embedding_model: "bge-m3"

schema_keys:
  confidential_field: "confidential"
  required_fields: []

stability:
  size_stable_check_ms: 500       # ファイルサイズが 500ms 安定するまで待つ
  min_age_seconds: 1              # 作成直後のファイルは少し待つ
```

### 5.2 ingester プロセスのフロー

```
[起動]
  1. config 読み込み
  2. 既存ファイルの初回スキャン（_index.json と比較して差分のみ処理）
  3. fsnotify でリアルタイム監視開始

[ファイル変更検知時]
  4. ファイルが安定するまで待機（size_stable_check_ms）
  5. .airflowignore + exclude パターンで除外判定
  6. content_hash 計算
  7. 既存 Note レコードと hash 比較
     - 一致 → スキップ
     - 不一致 or 新規 → 処理続行
  8. YAML フロントマター解析
  9. 本文をセクションごとにチャンク化
 10. ベクトル DB に embedding + メタデータ保存
 11. .airflow/notes/index.json を更新
 12. auto_dispatch=true のパスなら、Inbox タスクとして taskboard に登録
 13. ログ: info "note.indexed"

[ファイル削除検知時]
 14. ベクトル DB から該当 note_id のレコード削除
 15. notes/index.json から削除
 16. ログ: info "note.deleted"
```

### 5.3 フロントマター解析の堅牢化

ノートが以下のパターンを取りうることを前提に：

| パターン | 対応 |
|---------|------|
| YAML フロントマターあり（標準） | スキーマ通り解析 |
| フロントマターなし | 全文を本文として扱う、メタデータは初期値 |
| YAML 構文エラー | `NOTE_PARSE_ERROR` recoverable ログ、本文のみ取り込み |
| 未知フィールド | そのまま保持（拡張性） |

### 5.4 セクションチャンク化

Markdown 見出し（`#`, `##`）で分割し、各セクションを独立チャンクに：

```
チャンク粒度:
  - 1 セクション = 1 チャンク
  - セクションが 1000 トークン超 → サブ分割（段落単位）
  - セクションが 100 トークン未満 → 親セクションにマージ

オーバーラップ: 隣接チャンク間で 100 トークン重複（コンテキスト保持）
```

### 5.5 Confidential ノートの扱い

```yaml
---
confidential: true
---
```

→ ベクトル DB **には保存しない**
→ `notes/index.json` にはメタデータのみ保存（本文ハッシュは保持）
→ `notes.search` から除外（明示的に `exclude_confidential: false` 指定時のみ含む）

### 5.6 パス正規化（NF-L11〜L13）

| 入力 | 正規化結果 |
|-----|---------|
| `D:\vaults\notes\file.md` (Windows) | `/mnt/d/vaults/notes/file.md` (WSL内) |
| `D:/vaults/notes/file.md` (Win mixed) | `/mnt/d/vaults/notes/file.md` |
| `~/notes/file.md` | `$HOME/notes/file.md` |

- 内部表現は常に **POSIX 絶対パス**
- 設定ファイルでは Windows / POSIX どちらの形式も受け付ける
- 改行コード：読み取り時に CRLF → LF 正規化、UTF-8 BOM は除去

---

## 6. Inbox 自動タスク生成（F-L26〜L28）

### 6.1 Inbox サブフォルダ規約

```
<vault>/Airflow/Inbox/        # 新規ノート投入先
<vault>/Airflow/Processed/    # 処理済み移動先（オプション）
```

### 6.2 タスク化ロジック

```
[新規 .md を Inbox/ で検知]
  1. ingester が通常通り Note として取り込み
  2. auto_dispatch=true → タスク生成フロー起動

[タスク生成フロー]
  3. フロントマターから初期メタデータ抽出:
     - title → task.title
     - category → task.tags + section 推定
     - confidence → priority 調整
  4. 本文先頭からタスク内容を要約（または本文をそのまま description に）
  5. agents.find_for_task を呼び、担当エージェント候補を取得
  6. 候補トップが confidence ≥ 0.7 なら owner として設定
     未満 → owner=null（inbox に置いたまま、エージェントが自発的に claim）
  7. task.source = { kind: "inbox_note", ref: <note_id> }
  8. taskboard.tasks.create で登録
  9. ノートのフロントマターに processed_at を追記（Vault ファイル直接編集）
     ※ 編集失敗時もタスクは登録済み（idempotency は task.source.ref で担保）
 10. ログ: info "task.created" with source=inbox
```

### 6.3 重複タスク防止

ingester は `note.id` を `task.source.ref` でタスクボードに事前検索：

- 既に同じ `source.ref` のタスクが存在 → 新規作成しない（idempotency）
- ノート更新時（hash 不一致）でも、既存タスクが `in_progress` 以降なら新規作成しない

### 6.4 セクション推定ロジック

`category` フィールドから section へのマッピング：

| category 値 | 推定 section |
|-----------|-----------|
| `architecture`, `code`, `tech` | engineering |
| `content`, `script`, `blog` | content |
| `business`, `marketing`, `legal` | business |
| `workflow`, `tool_X`, `concept` | orchestration |
| 未設定 / 不明 | orchestration（デフォルト） |

このマッピングは設定ファイルで上書き可能（`.airflow/config/dispatch.yaml`）。

---

## 7. 初回スキャンと再インデックス

### 7.1 起動時の初回スキャン

```
1. .airflow/notes/index.json を読み込み
2. watch_paths を再帰的にスキャン
3. 既存ファイルごとに content_hash を比較
   - hash 一致 + last_indexed 新しい → スキップ
   - hash 不一致 → 再インデックス
   - index に存在するが実ファイルなし → 削除処理
4. 完了後、リアルタイム監視に移行
```

### 7.2 `airflow notes reindex` CLI

```bash
airflow notes reindex --path /mnt/d/vaults/youtube-knowledge/notes/Airflow
airflow notes reindex --all
airflow notes reindex --force   # hash 一致でも再処理
```

### 7.3 パフォーマンス目標（NF-L05/06）

| 指標 | 目標 |
|------|-----|
| 初回 1000 ファイル / 10MB | 10 分以内 |
| 増分更新 1 ファイル | 5 秒以内 |
| ベクトル検索（top_k=10） | 1 秒以内 |

実測は Phase 2 MVP の検証で取得し、Phase 3 で改善判断。

---

## 8. ベクトル DB 連携

### 8.1 Qdrant コレクションスキーマ

```yaml
collection: airflow_notes
vectors:
  size: 1024                  # bge-m3 の次元
  distance: Cosine
payload_schema:
  note_id: keyword
  section_heading: keyword
  category: keyword
  confidence: float
  language: keyword
  confidential: bool
  chunk_index: integer
  source_url: keyword
```

### 8.2 検索クエリ（`notes.search`）の実装

```python
def search(query: str, filters: dict, top_k: int) -> list[Result]:
    embedding = bge_m3.embed(query)

    qdrant_filter = {
        "must": [{"key": "confidential", "match": {"value": False}}]
    }
    if "category" in filters:
        qdrant_filter["must"].append(
            {"key": "category", "match": {"any": filters["category"]}}
        )
    if "min_confidence" in filters:
        qdrant_filter["must"].append(
            {"key": "confidence", "range": {"gte": filters["min_confidence"]}}
        )

    results = qdrant.search(
        collection="airflow_notes",
        vector=embedding,
        filter=qdrant_filter,
        limit=top_k
    )
    return [Result(note_id=r.payload["note_id"], score=r.score, ...) for r in results]
```

### 8.3 内部リンクグラフ

Obsidian の `[[内部リンク]]` を別途グラフ DB ではなく、`notes/index.json` の `internal_links` フィールドで管理：

```json
{
  "note_id": "note_abc",
  "internal_links": ["note_def", "note_ghi"]
}
```

`notes.related(note_id, depth=2)` は BFS で展開。Phase 2 では depth=1 のみ実装可。

---

## 9. CLI 実装

### 9.1 必要なコマンド

```bash
# タスクボード
airflow task list [--status inbox]
airflow task create --title "..." --section engineering
airflow task show <task_id>
airflow task transition <task_id> --to done --note "完了"

# Ingester 制御
airflow ingest start [--daemon]
airflow ingest stop
airflow ingest status
airflow ingest add-path /mnt/d/vaults/...
airflow ingest reindex [--all]

# Vault 内容確認
airflow notes search "Cowork とは"
airflow notes show <note_id>
```

### 9.2 デーモン運用

`airflow ingest start --daemon` で systemd / launchd 配下に常駐。

```ini
# systemd/airflow-ingester.service
[Service]
ExecStart=/usr/local/bin/airflow ingest start
Restart=on-failure
Environment="AIRFLOW_MCP_TOKEN=..."
```

---

## 10. 障害シナリオと対応

| シナリオ | 検出 | 対応 |
|---------|------|------|
| ingester 異常終了 | systemd Restart=on-failure | 自動再起動 + 初回スキャン |
| Qdrant ダウン | search 時に接続エラー | recoverable リトライ、5回失敗で fatal |
| Vault フォルダごと消失 | 起動時 stat 失敗 | fatal ログ、設定確認を促す |
| タスクファイル破損（不正 JSON） | バリデーション失敗 | `tasks/_corrupt/` に隔離、ログに記録 |
| ロックファイル残留（プロセス強制終了） | 30 秒経過 + PID 不在 | 起動時クリーンアップで削除 |
| Inbox ノートのフロントマター不正 | YAML パース失敗 | NOTE_PARSE_ERROR ログ、本文のみ取り込み + タスク化はスキップ |

---

## 11. MVP 検証シナリオ

Phase 2 完了判定のための end-to-end テスト：

```
[準備]
  1. .airflow/ 初期化（airflow init）
  2. Vault パス設定: /mnt/d/vaults/youtube-knowledge/notes/Airflow
  3. agent_docs エージェント登録（skills.md）
  4. Ollama で qwen2.5:14b 起動確認

[シナリオ]
  5. Vault/Airflow/Inbox/ に test-note.md を投入:
     ---
     title: "テストノート"
     category: concept
     ---
     # 概要
     これは MVP 検証用のテストノートです。要約してください。

  6. 期待動作:
     a. ingester が 1 秒以内に検知
     b. Note レコードが index に追加
     c. Inbox タスクが自動生成（section=orchestration）
     d. agents.find_for_task で agent_docs が候補に
     e. agent_docs がポーリングで取得 → claim
     f. status=in_progress に遷移
     g. agent_docs が Ollama 呼出 → 要約生成
     h. handoff_note を追加し、status=done に遷移
     i. tasks/done/2026-05/ にアーカイブ

[合格基準]
  - 全工程が 30 秒以内に完了
  - ログに critical/error なし
  - タスクボードと Vault が整合
```

---

## 12. 検証チェックリスト

Phase 2 着手時の DDD-04 完了条件：

- [ ] タスクボードのディレクトリレイアウトが確定
- [ ] 楽観的ロックの 3 層実装が明文化
- [ ] ファイル監視・ポーリングの責任分担が明確
- [ ] Vault パス正規化（Windows/POSIX）の動作が定義
- [ ] フロントマター解析の堅牢化方針が確定
- [ ] Inbox 自動タスク生成の idempotency が保証されている
- [ ] Confidential ノートの除外フローが定義
- [ ] §11 MVP 検証シナリオが実行可能

---

## 13. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| ベクトル DB バックエンド最終選定（Qdrant vs Chroma） | Phase 2 実装時に実測比較 |
| Git コミット自動化（タスクごと） | Phase 5 |
| Vault 編集競合（Obsidian 同時編集） | Phase 2 実装で検証 |
| 大量ファイル一括追加時のレート制御 | Phase 2 実装で検証 |
| 内部リンク BFS の深い depth 対応 | Phase 3 |

---

*次：DDD-05（ローカル LLM 実行基盤）で、本タスクボード上のタスクを実際に処理する Ollama ラッパー実装を設計する。*

# ローカル Markdown 取り込み機能 — 概要要件定義

> ローカル PC の指定フォルダに保存された `.md` ファイル群を、Airflow システムの **要件定義・タスク生成・知識ベース** の入力として直接利用できるようにする機能。
>
> 想定ユースケース：Obsidian Vault（例：`D:\vaults\youtube-knowledge\notes\Airflow\`）に YouTube 動画から起こしたメモを蓄積し、それを AI エージェントが自動的に読み込んで要件定義やタスクを生成する。

---

## 1. 機能概要

### 1.1 目的

人間が手作業でファイル内容をコピー＆ペーストして AI に渡している現状を自動化し、**ローカルの Markdown 資産を一次データソース** としてシステムに組み込む。

### 1.2 ユースケース

| # | シナリオ |
|---|---------|
| UC-01 | Obsidian Vault に新規 `.md` を追加 → 自動でタスクボードにタスク生成 |
| UC-02 | Vault の既存ノート群を要件定義エージェントに入力 → 要件定義書を自動草案 |
| UC-03 | 過去ノートを RAG 検索 → 既存設計との整合性チェック |
| UC-04 | YouTube 動画から起こしたメモ群 → 同テーマで章別の要件定義書を生成 |

---

## 2. 機能要件

### 2.1 入力ソース

| # | 要件 |
|---|------|
| F-L01 | ユーザーが指定したローカルフォルダパスを **監視対象** として登録できる |
| F-L02 | 複数フォルダの登録に対応する |
| F-L03 | 再帰的探索（サブフォルダ含む）の有無を設定できる |
| F-L04 | ファイル形式は **`.md` / `.markdown` / `.txt`** をサポート |
| F-L05 | バイナリ・画像ファイルは無視する |
| F-L06 | `.obsidian/` 等のシステムフォルダは除外 |
| F-L07 | `.gitignore` 風の除外パターン（`.airflowignore`）を設定可能 |

### 2.2 ファイル変更検知

| # | 要件 |
|---|------|
| F-L08 | ファイル追加・更新・削除を **リアルタイムで検知** する（fswatch / inotify / FileSystemWatcher） |
| F-L09 | バッチモード（cron 等で定期スキャン）も併用可能 |
| F-L10 | ファイル更新時は内容ハッシュを比較し、実質的な変更がある場合のみ再処理 |
| F-L11 | 一時ファイル（`*.tmp`、`~$*`、`.swp`）は無視する |

### 2.3 YAML フロントマター解析

セッション前半で確認した実ファイル例：

```yaml
---
title: "AI エージェント組織設計の 3 点セット"
category: architecture
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-05-23
derived_from:
  - sem_0027
sources:
  - video_id: youtube_K1JBWvTIc2Y
    channel: クウキデザイン |  Kuuki Design
    timestamp: "1802s – 2399s"
    url: "https://..."
---
```

| # | 要件 |
|---|------|
| F-L12 | `---` で囲まれた YAML フロントマターを構造化データとして抽出 |
| F-L13 | フロントマターが無い場合もエラーにせず、本文のみを処理対象とする |
| F-L14 | 既知メタデータキー（`title`, `category`, `confidence`, `sources` 等）はスキーマに従って正規化 |
| F-L15 | 未知のキーはそのまま保持（拡張性のため） |
| F-L16 | `sources.url`, `sources.video_id`, `sources.timestamp` は別途インデックス化（YouTube 動画起点の追跡用） |

### 2.4 本文解析

| # | 要件 |
|---|------|
| F-L17 | Markdown 見出し（`#`, `##`, ...）でセクション分割 |
| F-L18 | Obsidian の `[[内部リンク]]` 記法を解析し、ノート間の関連グラフを構築 |
| F-L19 | コードブロック・表・リストを構造化して保持 |
| F-L20 | 画像参照（`![alt](path)`）はファイルパスのみ記録（画像解析は対象外） |

### 2.5 知識ベース統合

| # | 要件 |
|---|------|
| F-L21 | 取り込んだファイルは **ベクトル DB**（Qdrant / Chroma）に埋め込み保存 |
| F-L22 | ファイル単位 + セクション単位の二段階チャンクで埋め込み |
| F-L23 | エージェントは MCP 経由で「ノート検索ツール」として利用可能 |
| F-L24 | メタデータフィルタ（`category=architecture` 等）を併用した検索をサポート |
| F-L25 | ファイル削除時は対応するベクトルも削除 |

### 2.6 タスク自動生成（Inbox 連携）

| # | 要件 |
|---|------|
| F-L26 | 監視フォルダ内の **Inbox サブフォルダ**（例：`notes/Inbox/`）に新規ファイルが追加されたら、タスクディスパッチが自動起動 |
| F-L27 | フロントマターの `category` フィールドからタスクのタグ・担当エージェントを推定 |
| F-L28 | 処理済みファイルは `notes/Processed/` に移動するか、フロントマターに `processed_at` を追記 |

### 2.7 要件定義エージェント連携

| # | 要件 |
|---|------|
| F-L29 | 「指定フォルダ配下のノート群から要件定義を生成」コマンドをサポート |
| F-L30 | 入力ファイル一覧と参照箇所を **出力ドキュメントの末尾に明示** する（トレーサビリティ確保） |
| F-L31 | `sources` フィールドが存在する場合、出力に YouTube URL 等の出典を引用する |
| F-L32 | `confidence` が低いノート（< 0.7 等）は信頼度を下げて扱う、または除外オプションを提供 |

---

## 3. 非機能要件

### 3.1 セキュリティ・プライバシー

| # | 要件 |
|---|------|
| NF-L01 | ローカルファイルは **クラウドにアップロードしない**（完全ローカル原則） |
| NF-L02 | 監視対象フォルダ外への意図せぬアクセスを禁止（パストラバーサル対策） |
| NF-L03 | ファイル権限はシステム実行ユーザーに準じ、不要な昇格を行わない |
| NF-L04 | 機密ノート（フロントマター `confidential: true` 等）はベクトル DB に保存せずローカルメモリのみで扱う |

### 3.2 パフォーマンス

| # | 要件 |
|---|------|
| NF-L05 | 初回インデックス：1,000 ファイル / 10MB を **10 分以内** に処理 |
| NF-L06 | 増分更新：ファイル 1 件あたり **5 秒以内** に再インデックス完了 |
| NF-L07 | 検索クエリのレスポンス：**1 秒以内**（VRAM 16GB 想定） |

### 3.3 同時アクセス・整合性

| # | 要件 |
|---|------|
| NF-L08 | Obsidian アプリ等が編集中のファイルもロックせず読み取れる（共有読み取りモード） |
| NF-L09 | エディタ保存中の途中状態を読まないよう、ファイルサイズ安定確認を行ってから取り込む |
| NF-L10 | 複数 AI エージェントが同時にベクトル DB を参照しても整合性を保つ |

### 3.4 移植性・OS 対応

| # | 要件 |
|---|------|
| NF-L11 | Windows・macOS・Linux で動作（WSL2 経由のパス変換も対応：`/mnt/d/...` ⇔ `D:\...`） |
| NF-L12 | パス区切り文字の差異（`/` vs `\`）を吸収 |
| NF-L13 | UTF-8 BOM・改行コード（CRLF/LF）の混在を許容 |

---

## 4. アーキテクチャ

```
┌──────────────────────────────────────────────┐
│  ローカルフォルダ                              │
│  例: D:\vaults\youtube-knowledge\notes\Airflow │
│        ├── *.md（既存ノート）                  │
│        └── Inbox/*.md（新規ノート）            │
└──────────────────┬───────────────────────────┘
                   │ ファイル監視（inotify/fswatch）
                   ▼
        ┌──────────────────────┐
        │  Markdown Ingester    │
        │  - フロントマター解析  │
        │  - 本文チャンク化      │
        │  - ハッシュ比較        │
        └────┬───────────────┬─┘
             │               │
             ▼               ▼
   ┌─────────────────┐  ┌──────────────┐
   │  ベクトル DB     │  │ タスクボード │
   │  (Qdrant)       │  │ (Git+JSON)   │
   └────┬────────────┘  └──────┬───────┘
        │                      │
        ▼                      ▼
   ┌──────────────────────────────┐
   │  AI エージェント群             │
   │  - 要件定義エージェント        │
   │  - タスクディスパッチ          │
   │  - ナレッジ検索               │
   │  （Hermes Agent + Ollama）    │
   └──────────────────────────────┘
```

---

## 5. データモデル

### 5.1 取り込み済みノートのスキーマ

```yaml
# ベクトル DB に格納するメタデータ
note_id: <SHA256 of absolute path>
absolute_path: "D:/vaults/youtube-knowledge/notes/Airflow/airflow-ai-人間ハイブリッドタスク管理システム.md"
relative_path: "Airflow/airflow-ai-人間ハイブリッドタスク管理システム.md"
title: "Airflow: AI 人間ハイブリッドタスク管理システム"
category: tool_X
confidence: 0.98
review_status: auto_accepted
last_verified: 2026-05-23
last_indexed: 2026-05-22T15:00:00Z
content_hash: <SHA256 of file body>
sources:
  - video_id: youtube_a0c0If_2X1o
    url: "https://..."
sections:
  - heading: "仕組み"
    chunk_ids: [chunk_001, chunk_002]
  - heading: "特徴"
    chunk_ids: [chunk_003]
internal_links:
  - "[[AI 社員の構成要素]]"
```

### 5.2 設定ファイル例 `.airflow/config.yaml`

```yaml
ingester:
  watch_paths:
    - path: "D:/vaults/youtube-knowledge/notes/Airflow"
      recursive: true
      mode: realtime
    - path: "D:/vaults/youtube-knowledge/notes/Airflow/Inbox"
      recursive: false
      mode: realtime
      auto_dispatch: true   # 新規ファイルをタスク化

  exclude:
    - ".obsidian/**"
    - "**/.trash/**"
    - "**/*.tmp"

  vector_db:
    backend: qdrant
    endpoint: "http://localhost:6333"
    collection: "airflow_notes"
    embedding_model: "bge-m3"

  schema:
    required_fields: []
    sensitive_field: "confidential"   # true なら DB 保存しない
```

---

## 6. CLI / API

### 6.1 CLI コマンド例

```bash
# フォルダを監視対象に追加
airflow ingest add /mnt/d/vaults/youtube-knowledge/notes/Airflow

# 全件再インデックス
airflow ingest reindex --all

# 状態確認
airflow ingest status
# => 13 files indexed, 0 pending, last update: 2026-05-22 15:00

# ノート検索（CLI からの動作確認用）
airflow knowledge search "Cowork とは何か"

# 要件定義生成
airflow generate requirements \
  --from "D:/vaults/youtube-knowledge/notes/Airflow" \
  --output docs/requirements-from-notes.md
```

### 6.2 MCP ツールとしての公開

エージェント側からは以下の MCP ツールとして見える：

| ツール名 | 機能 |
|---------|------|
| `notes.search` | クエリ + メタデータフィルタでノート検索 |
| `notes.get` | `note_id` で全文取得 |
| `notes.list_by_category` | カテゴリでフィルタリング |
| `notes.related` | 内部リンクから関連ノート取得 |

---

## 7. 想定リスクと対策

| リスク | 影響 | 対策 |
|------|------|------|
| Obsidian の同時編集競合 | ファイル取り込み失敗 | 共有読み取り + リトライ |
| Vault サイズ肥大化 | 初回インデックス遅延 | ストリーミング処理 + 進捗表示 |
| 機密情報の誤取り込み | 情報漏洩リスク | `confidential` フラグ + 除外パターン |
| WSL/Windows パス混乱 | 取り込み失敗 | パス正規化レイヤー |
| フロントマター不在ノート | メタデータ欠落 | デフォルト値補完 + 警告ログ |
| 大量ファイル同時更新 | CPU 負荷スパイク | キューイング + バックプレッシャ |

---

## 8. 実装フェーズ

| Phase | 内容 | 期間 |
|-------|------|-----|
| **L-Phase 1** | フォルダ監視 + フロントマター解析（最小実装） | 1 週間 |
| **L-Phase 2** | ベクトル DB 連携 + MCP ツール公開 | 1 週間 |
| **L-Phase 3** | Inbox 自動タスク生成 | 1 週間 |
| **L-Phase 4** | 要件定義生成エージェント連携 | 2 週間 |

---

## 9. 関連要件との接続

| 関連要件 | 接続点 |
|---------|--------|
| [[概要要件定義]] | F-06「インボックスに入ったノート・Issue・PR を読み込み」の実装本体 |
| [[AI チーム構成 — 詳細要件定義]] | タスクディスパッチ・Cleaner Agent の入力源 |
| [[完全無料・完全ローカル構成]] | クラウド非依存・完全ローカルの原則を継承 |

---

*セッション前半でユーザーが `D:\vaults\youtube-knowledge\notes\Airflow\*.md` の内容を手動コピペした作業を自動化する目的の要件定義。*

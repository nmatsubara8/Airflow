# Airflow MCP

Obsidian Vault の Markdown ファイルを inbox として読み込み、ローカル LLM（Ollama）で AI チームがタスクを自律処理するオーケストレーションシステム。

## 特徴

- **26 AI エージェント** — Engineering / Content / Business / Orchestration の 4 セクション
- **ローカル LLM 完結** — Ollama + Qwen2.5（VRAM 16GB、インターネット不要）
- **5 つの MCP サーバー** — taskboard / notes / models / gpu / agents
- **JWT EdDSA 認証 + per-action レート制限** — 本番グレードのセキュリティ
- **Prometheus メトリクス + Web ダッシュボード** — リアルタイム監視
- **systemd 管理** — 全サービスの自動起動・再起動

---

## 仕様概要

### アーキテクチャ

```
Obsidian Vault (.md)
       │ watch
       ▼
  Ingester ──────► TaskStore (JSON + 楽観的ロック)
                        │
              ┌─────────┼─────────┐
              ▼         ▼         ▼
         Dispatcher  Scheduler  Cleaner
              │         │
              ▼         ▼
          Runner ──► Ollama (Qwen2.5)
              │
              ▼
     26 AI Agents (MCP)
```

### エージェント構成（26体）

| セクション | エージェント数 | 主な役割 |
|-----------|-------------|---------|
| Orchestration | 3 | dispatcher, cleaner, feature-extractor |
| Engineering (§01) | 14 | tech-lead, qa, security, frontend, backend, devops 等 |
| Content (§02) | 5 | content-director, brand-voice, root-cause, anti-ai-slop, morning-brainstorm |
| Business (§03) | 4 | marketing-director, business-strategy, partnership-manage, legal-review |

### タスクライフサイクル

```
inbox → in_progress → done
                  └→ needs_human  (人間エスカレーション)
                  └→ blocked      (依存待ち)
```

### 対応ハンドオフパターン

- **シーケンシャル**: `agent_a → agent_b → agent_c`
- **4段階コンテンツレビュー**: content-director → brand-voice → root-cause → anti-ai-slop
- **並列集約 (fan-out/fan-in)**: `tasks.join` による PR レビュー並列化

---

## セットアップ

### 必要環境

| 要件 | バージョン |
|-----|---------|
| Python | 3.11 以上 |
| Ollama | 最新版 |
| GPU (推奨) | VRAM 16GB（RTX 4060 Ti 等） |
| OS | Linux（本番）/ macOS（開発） |

### 1. Ollama セットアップ

```bash
# Ollama インストール
curl -fsSL https://ollama.ai/install.sh | sh

# 推奨モデルをダウンロード
ollama pull qwen2.5:14b-instruct-q4_K_M
ollama pull qwen2.5-coder:7b-instruct-q4_K_M
```

### 2. パッケージインストール

```bash
git clone https://github.com/nmatsubara8/airflow
cd airflow
pip install -e ".[dev]"
```

### 3. 環境変数設定

```bash
cp .env.example .env
# .env を編集（最低限必要なもの）
```

**.env の主要項目**:

| 変数 | 必須 | 説明 |
|-----|------|------|
| `VAULT_ROOT` | ✅ | Obsidian Vault のルートパス |
| `AIRFLOW_MCP_TOKEN_ADMIN` | ✅ | 管理者 API トークン（ランダム文字列） |
| `AIRFLOW_MCP_TOKEN_AGENT` | ✅ | エージェント API トークン |
| `OLLAMA_BASE_URL` | — | Ollama エンドポイント（デフォルト: `http://localhost:11434`） |
| `AIRFLOW_SLACK_WEBHOOK` | — | Slack 通知 webhook URL |
| `AIRFLOW_HTTP_AUTH_TOKEN` | — | ダッシュボード Bearer 認証トークン |
| `AIRFLOW_AUTH_LOCALHOST_BYPASS` | — | `true` で localhost 認証スキップ（開発用のみ） |

### 4. 設定ファイル検証

```bash
python -m airflow_mcp config
# → All configs valid. が出ればOK
```

### 5. JWT キーペア生成（本番用）

```bash
python -m airflow_mcp auth keygen
# .airflow/auth/ed25519.pem と ed25519.pub が生成される
```

---

## 起動方法

### Docker（推奨）

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull qwen2.5:14b-instruct-q4_K_M
docker compose up -d
```

### ローカル（開発）

```bash
# MCP サーバーをそれぞれ別ターミナルで起動
python -m airflow_mcp taskboard   # タスクボード（必須）
python -m airflow_mcp models      # モデル管理
python -m airflow_mcp gpu         # GPU 状態
python -m airflow_mcp notes       # ノート検索
python -m airflow_mcp agents      # エージェント登録
```

### 本番（systemd）

```bash
# ユニットファイルをインストール
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload

# 全サービス有効化・起動
sudo systemctl enable --now \
  airflow-taskboard airflow-models airflow-gpu \
  airflow-notes airflow-agents airflow-dashboard \
  airflow-dispatcher airflow-batch.timer

# 毎朝 03:00 に自動バッチ実行（cleaner + audit hash）
```

詳細は `deploy/systemd/README.md` を参照。

---

## 利用方法

### タスク投入

1. Obsidian Vault の `Inbox/` フォルダに `.md` ファイルを置く
2. Ingester が自動検出してタスクボードに登録
3. Dispatcher がエージェントへ割り当て
4. エージェントが Ollama で処理し `done` へ遷移

### 運用 CLI

```bash
# リアルタイムステータス表示
python -m airflow_mcp status
python -m airflow_mcp status --watch              # 5秒ごとに更新
python -m airflow_mcp status --watch --interval 10

# バッチ実行
python -m airflow_mcp batch                       # 即時1回実行
python -m airflow_mcp batch --loop                # ループ（デーモン）

# 監査
python -m airflow_mcp audit verify                # ハッシュチェーン整合性確認
python -m airflow_mcp audit verify 2026-01-01     # 特定日以降を検証
python -m airflow_mcp audit hash                  # 当日ハッシュ計算

# 設定検証
python -m airflow_mcp config

# トークン管理
python -m airflow_mcp auth keygen
python -m airflow_mcp auth issue --agent agent_qa --scopes tasks:read,models:execute
python -m airflow_mcp auth list
python -m airflow_mcp auth revoke --agent agent_qa
python -m airflow_mcp auth rotate --agent agent_qa --scopes tasks:read
```

### Web ダッシュボード

起動後 `http://localhost:7400` でアクセス。

| URL | 内容 |
|-----|------|
| `/` | タスク件数サマリー・GPU 状態 |
| `/tasks` | タスク一覧（ステータス・セクション絞り込み） |
| `/tasks/<id>` | タスク詳細・ハンドオフ履歴 |
| `/scheduler` | スケジューラキュー・プロファイル |
| `/api/status` | JSON API |

`AIRFLOW_HTTP_AUTH_TOKEN` を設定した場合は `Authorization: Bearer <token>` ヘッダーが必要。

### Prometheus メトリクス

`http://localhost:9090/metrics` でメトリクス取得（Grafana 連携可）。

主なメトリクス:
- `airflow_tasks_total{status, section}` — タスク件数
- `airflow_model_execution_duration_seconds` — LLM 実行時間
- `airflow_gpu_vram_used_gb` — VRAM 使用量

### エスカレーション対応

`needs_human` に遷移したタスクは Slack 通知で到着。

```bash
# ダッシュボードで内容確認
open http://localhost:7400/tasks?status=needs_human

# 対応後: handoff_note を追加して done に遷移
# （MCP ツール tasks.add_handoff_note + tasks.transition を使用）
```

---

## 開発

```bash
# 高速ユニットテストのみ
pytest -m "not integration and not slow"

# 全テスト（統合テスト含む）
pytest

# カバレッジ付き
pytest --cov=src/airflow_mcp --cov-report=term-missing -q

# Lint
ruff check src/ tests/
```

### テストマーカー

| マーカー | 説明 |
|---------|------|
| *(なし)* | 全テスト（1,337件） |
| `not integration and not slow` | 高速ユニットのみ |
| `integration` | 統合テスト（実サービス不要、モック使用） |
| `slow` | HTTP サーバー起動テスト |

---

## ディレクトリ構成

```
.
├── src/airflow_mcp/
│   ├── common/          # 共通（auth, errors, rate_limiter, config_validator）
│   ├── taskboard/       # TaskStore + MCP サーバー
│   ├── dispatcher/      # inbox タスク → エージェント割り当て
│   ├── runner/          # Ollama 呼び出し・実行ログ
│   ├── orchestrator/    # ハンドオフチェーン・バッチスケジューラ
│   ├── cleaner/         # タスクアーカイブ・ログローテーション
│   ├── models/          # モデル管理 MCP（KNN 推薦 + ε-greedy）
│   ├── gpu/             # GPU 状態管理 MCP
│   ├── scheduler/       # マルチキュースケジューラ（親和性・スターベーション防止）
│   ├── ingester/        # Vault .md → タスク変換
│   ├── notes/           # ノート検索 MCP
│   ├── ops/             # メトリクス・通知・監査ログ
│   └── web/             # ダッシュボード・Prometheus エンドポイント
├── agents/              # 26エージェントの skills.md
│   ├── orchestration/
│   ├── engineering/
│   ├── content/
│   └── business/
├── docs/design/         # DDD-01〜08 詳細設計書
├── deploy/systemd/      # 本番用 systemd ユニットファイル
├── tests/
│   ├── unit/
│   └── integration/
├── .airflow/config/     # YAML 設定ファイル群
└── .env.example
```

---

## 設計ドキュメント

`docs/design/` に 8 本の詳細設計書（DDD）があります。

| ドキュメント | 内容 |
|------------|------|
| DDD-01 | 共通データスキーマ（タスク・ログ・GPU 状態） |
| DDD-02 | MCP インターフェース仕様（5サーバー・ツール定義） |
| DDD-03 | エラー処理・ログ設計（リトライ・監査 ID） |
| DDD-04 | タスクボード + Vault Ingestion |
| DDD-05 | ローカル LLM 実行基盤（Ollama 統合） |
| DDD-06 | モデル動的選択・学習（KNN + ε-greedy） |
| DDD-07 | 並列スケジューラ（GPU 管理・スターベーション防止） |
| DDD-08 | チーム拡張・本番化・systemd・セキュリティ |

---

## ライセンス

MIT

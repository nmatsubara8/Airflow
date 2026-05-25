# Airflow MCP

Obsidian Vault の Markdown ファイルを inbox として読み込み、ローカル LLM（Ollama）で AI チームがタスクを自律処理するオーケストレーションシステム。

## 特徴

- **26 AI エージェント** — Engineering / Content / Business / Orchestration の 4 セクション
- **ローカル LLM 完結** — Ollama + Qwen2.5（VRAM 16GB、インターネット不要）
- **5 つの MCP サーバー** — taskboard / notes / models / gpu / agents
- **JWT EdDSA 認証 + per-action レート制限** — 本番グレードのセキュリティ
- **Prometheus メトリクス + Web ダッシュボード** — リアルタイム監視
- **Docker Compose / systemd** — 全サービスの自動起動・再起動

---

## アーキテクチャ

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

### Docker Compose サービス構成

| サービス | 役割 | ポート |
|---------|------|--------|
| `ollama` | LLM バックエンド | 127.0.0.1:11434 |
| `dispatcher` | inbox タスクのエージェント割り当てループ | — |
| `dashboard` | Web ダッシュボード | 127.0.0.1:7400 |
| `metrics` | Prometheus メトリクスエクスポータ | 127.0.0.1:9090 |

> **MCP サーバー**（taskboard / notes / models / gpu / agents）は stdio ベースのため  
> Claude Desktop が直接起動します。`deploy/mcp-start.sh` で設定します。

---

## クイックスタート（Docker Compose）

### 必要環境

| 要件 | バージョン |
|-----|---------|
| Docker | 24 以上 |
| Docker Compose | v2.20 以上 |
| GPU（推奨） | VRAM 16GB（Qwen2.5:14b 用） |

### 1. リポジトリ取得 & 環境変数設定

```bash
git clone https://github.com/nmatsubara8/airflow
cd airflow

cp .env.example .env
```

`.env` を開いて以下を編集します（`← 要変更` のもの）:

```bash
VAULT_ROOT=/path/to/your/obsidian/vault   # Obsidian Vault のパス

# openssl rand -hex 16 などで生成
AIRFLOW_MCP_TOKEN_ADMIN=airflow_local_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AIRFLOW_MCP_TOKEN_AGENT=airflow_local_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AIRFLOW_MCP_TOKEN_READONLY=airflow_local_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 2. Ollama 起動 & モデル取得

```bash
# Ollama コンテナだけ先に起動
docker compose up -d ollama

# モデルをダウンロード（初回のみ、数 GB）
docker compose exec ollama ollama pull qwen2.5:14b-instruct-q4_K_M
docker compose exec ollama ollama pull qwen2.5-coder:7b-instruct-q4_K_M
```

### 3. 全サービス起動

```bash
docker compose up -d
```

起動後のアクセス先:

| URL | 内容 |
|-----|------|
| http://localhost:7400 | Web ダッシュボード |
| http://localhost:7400/tasks | タスク一覧 |
| http://localhost:9090/metrics | Prometheus メトリクス |

### 4. 状態確認

```bash
# 全サービスの起動状態
docker compose ps

# ログ確認
docker compose logs -f dispatcher
docker compose logs -f dashboard

# コンテナ内から CLI 実行
docker compose exec dashboard python -m airflow_mcp status
```

### 5. 停止

```bash
docker compose down          # コンテナ停止（データ保持）
docker compose down -v       # データボリュームも削除
```

---

## MCP サーバー設定（Claude Desktop）

MCP サーバーは Claude Desktop が stdio 経由で直接起動します。  
`~/Library/Application Support/Claude/claude_desktop_config.json`（Mac）に以下を追加:

```json
{
  "mcpServers": {
    "airflow-taskboard": {
      "command": "/path/to/airflow/deploy/mcp-start.sh",
      "args": ["taskboard"]
    },
    "airflow-notes": {
      "command": "/path/to/airflow/deploy/mcp-start.sh",
      "args": ["notes"]
    },
    "airflow-models": {
      "command": "/path/to/airflow/deploy/mcp-start.sh",
      "args": ["models"]
    },
    "airflow-gpu": {
      "command": "/path/to/airflow/deploy/mcp-start.sh",
      "args": ["gpu"]
    },
    "airflow-agents": {
      "command": "/path/to/airflow/deploy/mcp-start.sh",
      "args": ["agents"]
    }
  }
}
```

`deploy/mcp-start.sh` は `.env` を自動ロードして適切な Python 環境で MCP サーバーを起動します。

---

## 利用方法

### タスク投入

1. Obsidian Vault の `Inbox/` フォルダに `.md` ファイルを置く
2. Ingester が自動検出してタスクボードに登録
3. Dispatcher がエージェントへ割り当て
4. エージェントが Ollama で処理し `done` へ遷移

### タスクライフサイクル

```
inbox → in_progress → done
                  └→ needs_human  (人間エスカレーション)
                  └→ blocked      (依存待ち)
```

### Web ダッシュボード（http://localhost:7400）

| URL | 内容 |
|-----|------|
| `/` | タスク件数サマリー・GPU 状態 |
| `/tasks` | タスク一覧（ステータス・セクション絞り込み） |
| `/tasks/<id>` | タスク詳細・ハンドオフ履歴 |
| `/scheduler` | スケジューラキュー・プロファイル |
| `/api/status` | JSON API |

`AIRFLOW_HTTP_AUTH_TOKEN` を設定した場合は `Authorization: Bearer <token>` ヘッダーが必要。

### Prometheus メトリクス（http://localhost:9090/metrics）

主なメトリクス:

| メトリクス | 内容 |
|-----------|------|
| `airflow_tasks_total{status, section}` | タスク件数 |
| `airflow_model_execution_duration_seconds` | LLM 実行時間 |
| `airflow_gpu_vram_used_gb` | VRAM 使用量 |

### 運用 CLI（コンテナ内から実行）

```bash
# リアルタイムステータス
docker compose exec dashboard python -m airflow_mcp status
docker compose exec dashboard python -m airflow_mcp status --watch

# バッチ実行
docker compose exec dispatcher python -m airflow_mcp batch
docker compose exec dispatcher python -m airflow_mcp batch --loop

# 監査
docker compose exec dashboard python -m airflow_mcp audit verify
docker compose exec dashboard python -m airflow_mcp audit hash

# トークン管理
docker compose exec dashboard python -m airflow_mcp auth list
docker compose exec dashboard python -m airflow_mcp auth issue \
  --agent agent_qa --scopes tasks:read,models:execute
docker compose exec dashboard python -m airflow_mcp auth revoke --agent agent_qa

# 設定検証
docker compose exec dashboard python -m airflow_mcp config
```

### エスカレーション対応

`needs_human` に遷移したタスクは Slack 通知で到着。

```bash
# ダッシュボードで確認
open http://localhost:7400/tasks?status=needs_human

# 対応後: MCP ツールで done へ遷移
# tasks.add_handoff_note + tasks.transition を使用
```

---

## GPU 設定（オプション）

`docker-compose.yml` の `ollama` サービスの `deploy:` セクションをコメントアウト解除します:

```yaml
ollama:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: 1
            capabilities: [gpu]
```

nvidia-container-toolkit が必要です。

---

## 開発

### ローカル直接実行

```bash
pip install -e ".[dev]"
cp .env.example .env  # OLLAMA_BASE_URL=http://localhost:11434 のまま

# MCP サーバー（別ターミナルで）
python -m airflow_mcp taskboard
python -m airflow_mcp notes

# 常駐サービス（別ターミナルで）
python -m airflow_mcp dispatcher
python -m airflow_mcp dashboard
python -m airflow_mcp metrics
```

### テスト

```bash
# 高速ユニットテスト（1秒以内）
python -m pytest -m "not integration and not slow"

# 全テスト（統合テスト含む）
python -m pytest

# カバレッジ付き
python -m pytest --cov=src/airflow_mcp --cov-report=term-missing -q

# Lint
ruff check src/ tests/
```

### テストマーカー

| マーカー | 説明 | テスト数 |
|---------|------|---------|
| *(なし)* | 全テスト | 1,337件 |
| `not integration and not slow` | 高速ユニットのみ | ~1,123件 |
| `integration` | 統合テスト（モック使用） | — |
| `slow` | HTTP サーバー起動テスト | — |

---

## 本番デプロイ（systemd）

Docker を使わずに systemd で直接デプロイする場合:

```bash
# 仮想環境と依存関係
python3 -m venv .venv
.venv/bin/pip install -e .

# systemd ユニットファイルをインストール（install.sh がパスを自動置換）
sudo deploy/systemd/install.sh

# 全サービス有効化・起動
sudo systemctl enable --now \
  airflow-taskboard airflow-models airflow-gpu \
  airflow-notes airflow-agents airflow-dashboard \
  airflow-dispatcher airflow-batch.timer
```

詳細は `deploy/systemd/README.md` を参照。

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
├── deploy/
│   ├── mcp-start.sh     # Claude Desktop 用 MCP 起動スクリプト
│   └── systemd/         # 本番用 systemd ユニットファイル
├── docs/design/         # DDD-01〜08 詳細設計書
├── tests/
│   ├── unit/
│   └── integration/
├── .airflow/config/     # YAML 設定ファイル群
├── docker-compose.yml
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

# Airflow MCP

Obsidian Vault の Markdown ファイルを inbox として読み込み、ローカル LLM（Ollama）で AI チームがタスクを自律処理するオーケストレーションシステム。

## 特徴

- **26 AI エージェント** — Engineering / Content / Business / Orchestration の 4 セクション
- **ローカル LLM 完結** — Ollama + Qwen2.5（VRAM 16GB、インターネット不要）
- **5 つの MCP サーバー** — taskboard / notes / models / gpu / agents（Claude Desktop から直接呼び出し可）
- **JWT EdDSA 認証 + per-action レート制限** — 本番グレードのセキュリティ
- **Prometheus メトリクス + Web ダッシュボード** — リアルタイム監視（port 7400 / 9090）
- **systemd 管理** — 全サービスの自動起動・再起動
- **監査ハッシュチェーン** — 日次 SHA256 チェーンによる改ざん検知

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
              │
     Claude Desktop (MCP クライアント)
```

### エージェント構成（26体）

| セクション | 数 | 主な役割 |
|-----------|---|---------|
| Orchestration | 3 | dispatcher, cleaner, feature-extractor |
| Engineering (§01) | 14 | tech-lead, qa, security, frontend, backend, devops 等 |
| Content (§02) | 5 | content-director, brand-voice, root-cause, anti-ai-slop, morning-brainstorm |
| Business (§03) | 4 | marketing-director, business-strategy, partnership-manage, legal-review |

### タスクライフサイクル

```
inbox → in_progress → done
                  └→ needs_human  (人間エスカレーション)
                  └→ blocked      (依存待ち)
                  └→ cancelled
```

### ハンドオフパターン

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
| OS | Linux / WSL2（Ubuntu 22.04+）/ macOS |

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
# .env を編集
```

**.env の主要項目**:

| 変数 | 必須 | 説明 |
|-----|------|------|
| `VAULT_ROOT` | ✅ | Obsidian Vault のルートパス |
| `AIRFLOW_MCP_TOKEN_ADMIN` | ✅ | 管理者 API トークン |
| `AIRFLOW_MCP_TOKEN_AGENT` | ✅ | エージェント API トークン |
| `OLLAMA_BASE_URL` | — | デフォルト: `http://localhost:11434` |
| `AIRFLOW_SLACK_WEBHOOK` | — | Slack 通知 webhook URL |
| `AIRFLOW_HTTP_AUTH_TOKEN` | — | ダッシュボード Bearer 認証トークン（空で無効） |
| `AIRFLOW_AUTH_LOCALHOST_BYPASS` | — | `true` で localhost 認証スキップ（**開発用のみ**） |

> **セキュリティ注意**: `AIRFLOW_AUTH_LOCALHOST_BYPASS=true` は開発環境専用です。
> 本番では必ず `false` にしてください（`config validate` で警告が出ます）。

### 4. 設定ファイル検証

```bash
python -m airflow_mcp config
# → All configs valid. が出れば OK
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

> **注意**: `dispatcher` と `runner` は `user: "1000:1000"` で起動し、ホストユーザーと
> 同じ UID でファイルを書き込みます。Cleaner（ホスト側で実行）との権限競合を防ぎます。

### WSL2 + Claude Desktop（Windows 環境）

MCP サーバーは Claude Desktop の stdio 経由で起動します。`deploy/mcp-start.sh` が `.env` を
読み込んで各サーバーを起動します。

**Claude Desktop 設定** (`%APPDATA%\Claude\claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "taskboard": {
      "command": "wsl",
      "args": ["-d", "Ubuntu-24.04", "--", "/home/<user>/Airflow/deploy/mcp-start.sh", "taskboard"]
    },
    "notes":   { "command": "wsl", "args": ["-d", "Ubuntu-24.04", "--", "/home/<user>/Airflow/deploy/mcp-start.sh", "notes"] },
    "models":  { "command": "wsl", "args": ["-d", "Ubuntu-24.04", "--", "/home/<user>/Airflow/deploy/mcp-start.sh", "models"] },
    "gpu":     { "command": "wsl", "args": ["-d", "Ubuntu-24.04", "--", "/home/<user>/Airflow/deploy/mcp-start.sh", "gpu"] },
    "agents":  { "command": "wsl", "args": ["-d", "Ubuntu-24.04", "--", "/home/<user>/Airflow/deploy/mcp-start.sh", "agents"] }
  }
}
```

> `<user>` を実際の WSL2 ユーザー名に置換してください。
> 設定ファイルは Claude Desktop が上書きするため、BOM-free UTF-8 で保存し読み取り専用に
> 設定することを推奨します。

### ローカル（開発）

```bash
# 別ターミナルで各サーバーを起動
python -m airflow_mcp taskboard
python -m airflow_mcp models
python -m airflow_mcp gpu
python -m airflow_mcp notes
python -m airflow_mcp agents
```

### 本番（systemd）

```bash
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now \
  airflow-taskboard airflow-models airflow-gpu \
  airflow-notes airflow-agents airflow-dashboard \
  airflow-dispatcher airflow-batch.timer
```

詳細は `deploy/systemd/README.md` を参照。

---

## 利用方法

### タスク投入

1. Obsidian Vault の `Inbox/` フォルダに `.md` ファイルを置く
2. Ingester が自動検出してタスクボードに登録
3. Dispatcher がエージェントへ割り当て（`inbox` → `in_progress`）
4. Runner が Ollama で処理して `done` へ遷移

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
python -m airflow_mcp audit hash                  # 当日ハッシュ計算
python -m airflow_mcp audit verify                # ハッシュチェーン整合性確認
python -m airflow_mcp audit verify 2026-01-01     # 特定日以降を検証

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
| `/api/status` | JSON API |

`AIRFLOW_HTTP_AUTH_TOKEN` 設定時は `Authorization: Bearer <token>` ヘッダーが必要。

### Prometheus メトリクス

`http://localhost:9090/metrics` で取得（Grafana 連携可）。

| メトリクス | 説明 |
|-----------|------|
| `airflow_tasks_total{status,section}` | タスク件数 |
| `airflow_model_execution_duration_seconds` | LLM 実行時間 |
| `airflow_gpu_vram_used_gb` | VRAM 使用量 |

### エスカレーション対応（needs_human）

`needs_human` に遷移したタスクは Slack 通知で到着。

```bash
# ダッシュボードで内容確認
open http://localhost:7400/tasks?status=needs_human

# 対応後: handoff_note を追加して done に遷移
# MCP ツール tasks.add_handoff_note + tasks.transition を使用
```

---

## Cleaner（タスクアーカイブ）

`done` / `cancelled` タスクを `_archive/` へ自動移動し、古い実行ログを gzip 圧縮します。

```bash
# 手動実行（dry_run で事前確認）
python3 - <<'EOF'
from airflow_mcp.cleaner.main import run_once
from pathlib import Path
result = run_once(
    config={"dry_run": True, "tasks": {"archive_done_after_days": 30}},
    tasks_dir=Path(".airflow/tasks"),
)
print(result)
EOF
```

設定は `.airflow/config/cleaner.yaml` で変更可能（`archive_done_after_days` 等）。

> **Docker 環境での注意**: `dispatcher` / `runner` コンテナは `user: "1000:1000"` で
> 動作するため、Cleaner（ホスト実行）との UID が一致し、ファイル削除権限の競合が発生しません。
> 既存の root 所有ファイルがある場合は `sudo chown -R <user>:<user> .airflow/tasks/` を実行してください。

---

## §10 SLA 監視（7日間継続確認）

本番運用基準（DDD-08 §10）を満たすため、日次チェックスクリプトを用意しています。

### セットアップ

```bash
# Windows タスクスケジューラで毎朝 6:00 に自動実行（PowerShell で実行）
$action = New-ScheduledTaskAction -Execute "wsl.exe" `
  -Argument "-d Ubuntu-24.04 -- /home/<user>/Airflow/deploy/ops/check_daily.sh"
$trigger = New-ScheduledTaskTrigger -Daily -At "06:00AM"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskName "Airflow SLA Check" `
  -Action $action -Trigger $trigger -Settings $settings
```

### SLA レポート確認

```bash
# 7日間サマリー
~/Airflow/deploy/ops/sla_report.sh

# 出力例（7日後）
# ========================================
#  §10 SLA レポート（直近 7 日間）
# ========================================
# SLA 合格: 7 / 7 日
# critical ログ累計: 0 件（目標: 0）
# needs_human 最大値: 2 件/日（目標: <10）
# 監査チェーン破断: 0 回（目標: 0）
# ✅ 本番運用基準（§10）達成
```

### SLA 目標値

| 指標 | 目標 |
|------|------|
| critical ログ | 0 件/日 |
| needs_human | < 10 件/日 |
| タスク成功率 | ≥ 95% |
| 監査チェーン | 毎日 `chain ok` |
| 稼働率 | ≥ 99.5%（月次） |

---

## 開発

```bash
# 高速ユニットテストのみ（1秒以内）
pytest -m "not integration and not slow"

# 全テスト
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
| `integration` | 統合テスト（モック使用） |
| `slow` | HTTP サーバー起動テスト |

---

## ディレクトリ構成

```
.
├── src/airflow_mcp/
│   ├── common/          # auth, errors, rate_limiter, config_validator
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
│   ├── mcp-start.sh     # Claude Desktop 用 MCP 起動スクリプト（.env 読み込み）
│   ├── systemd/         # 本番用 systemd ユニットファイル
│   └── ops/
│       ├── check_daily.sh   # §10 SLA 日次チェック
│       └── sla_report.sh    # 7日間 SLA サマリーレポート
├── docs/design/         # DDD-01〜08 詳細設計書
├── tests/
│   ├── unit/
│   └── integration/
├── .airflow/config/     # YAML 設定ファイル群
└── .env.example
```

---

## 設計ドキュメント

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

## フェーズ状況

| フェーズ | 内容 | 状態 |
|---------|------|------|
| Phase 1 | 横断基盤（スキーマ・MCP・エラー処理） | ✅ 完了 |
| Phase 2 | MVP（単一エージェント・Ollama 連携） | ✅ 完了 |
| Phase 3 | Recommend モード・精度補完 | ✅ 完了 |
| Phase 4 | 並列実行・GPU スケジューラ | ✅ 完了 |
| Phase 5 | チーム拡張・JWT・本番化・systemd | ✅ 完了 |

**実装完了**: 1,337 テスト全パス・カバレッジ 100%・ruff clean

### 実機検証済み項目

| 検証 | 内容 | 結果 |
|------|------|------|
| V-2 | Ollama エラー → needs_human 遷移（3回リトライ後） | ✅ |
| V-3 | Cleaner アーカイブ・Docker user 権限修正 | ✅ |
| V-4 | 監査ハッシュチェーン改ざん検知 | ✅ |
| TP-2 | Per-action レート制限（tasks_create/models_execute 等） | ✅ |
| TP-3 | 本番認証ハードニング（localhost_bypass 警告） | ✅ |

残作業: §10 SLA の 7 日間継続確認（`deploy/ops/sla_report.sh` で追跡）。

---

## ライセンス

MIT

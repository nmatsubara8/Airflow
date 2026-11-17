# Airflow MCP — 開発ガイド

Obsidian Vault の Markdown ファイルを inbox として読み込み、ローカル LLM（Ollama）でタスクを処理する AI チームオーケストレーションシステム。

## クイックスタート

```bash
# 依存関係インストール
pip install -e ".[dev]"

# 単体テストのみ（高速・1秒以内）
pytest -m "not integration and not slow"

# 全テスト（統合テスト含む）
pytest

# カバレッジ付き全テスト
pytest --cov=src/airflow_mcp --cov-report=term-missing -q

# lint
ruff check src/ tests/
```

## テストマーカー

| マーカー | 説明 | 実行コマンド |
|---------|------|------------|
| *(なし)* | 全テスト (1337件) | `pytest` |
| `not integration and not slow` | 高速ユニットのみ | `pytest -m "not integration and not slow"` |
| `integration` | 統合テスト | `pytest -m integration` |
| `slow` | HTTPサーバー起動テスト | `pytest -m slow` |

## 設定ファイル検証

```bash
# .airflow/config/*.yaml の全スキーマ検証
python -m airflow_mcp config validate
```

## Docker 起動

```bash
cp .env.example .env
# .env の VAULT_ROOT 等を編集

docker compose up -d ollama           # まず Ollama を起動
docker compose exec ollama ollama pull qwen2.5:14b-instruct-q4_K_M
docker compose up -d                  # 全サービス起動
```

## MCP サーバー（ローカル起動）

```bash
python -m airflow_mcp taskboard   # タスクボード MCP
python -m airflow_mcp models      # モデル管理 MCP
python -m airflow_mcp gpu         # GPU 状態 MCP
python -m airflow_mcp notes       # ノート検索 MCP
python -m airflow_mcp agents      # エージェント登録 MCP
```

## 運用 CLI

```bash
python -m airflow_mcp status                    # ダッシュボード表示
python -m airflow_mcp status --watch            # リアルタイム監視（Ctrl+C で停止）
python -m airflow_mcp status --watch --interval 10  # 10秒ごとに更新
python -m airflow_mcp batch                     # バッチスケジュール 1回実行
python -m airflow_mcp batch --loop              # バッチスケジュール ループ実行
python -m airflow_mcp audit verify              # 監査ログ整合性チェック
python -m airflow_mcp audit hash                # 当日ハッシュ計算
python -m airflow_mcp config                    # 設定ファイル一括検証
python -m airflow_mcp auth keygen               # Ed25519 キーペア生成
python -m airflow_mcp auth issue --agent agent_docs --scopes tasks:read,tasks:write
python -m airflow_mcp auth list                 # 登録済みトークン一覧
python -m airflow_mcp auth revoke --agent agent_docs
python -m airflow_mcp auth rotate --agent agent_docs --scopes tasks:read
```

## ディレクトリ構成

```
src/airflow_mcp/
├── common/          # 共通ユーティリティ（auth, io, errors, config_validator, rate_limiter）
├── taskboard/       # タスクボード MCP サーバー + TaskStore
├── dispatcher/      # inbox タスクをエージェントへ割り当て
├── runner/          # エージェントタスク実行（Ollama 呼び出し）
├── orchestrator/    # ハンドオフチェーン / バッチスケジューラ
├── cleaner/         # Cleaner Agent（タスクアーカイブ・ログローテーション）
├── models/          # モデル管理 MCP (KNN 推薦 + ε-greedy)
├── gpu/             # GPU 状態管理 MCP
├── scheduler/       # マルチキュースケジューラ（親和性・スターベーション）
├── ingester/        # Vault .md → タスク変換
├── notes/           # ノート検索 MCP
├── ops/             # 監視・メトリクス・通知・監査
└── web/             # Prometheus メトリクス / ダッシュボード

agents/              # 26エージェントの skills.md（orchestration/engineering/content/business）
deploy/systemd/      # 本番用 systemd ユニットファイル
```

## 主要環境変数

| 変数 | デフォルト | 説明 |
|-----|---------|------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama エンドポイント |
| `OLLAMA_STUB` | `false` | `true` でモックモード（テスト用） |
| `AIRFLOW_TASKBOARD_DIR` | `.airflow/tasks` | タスク JSON 保存ディレクトリ |
| `VAULT_ROOT` | — | Obsidian Vault のルートパス |
| `AIRFLOW_MCP_TOKEN_ADMIN` | — | 管理者 API トークン |
| `AIRFLOW_MCP_TOKEN_AGENT` | — | エージェント API トークン |
| `AIRFLOW_SLACK_WEBHOOK` | — | Slack webhook URL（空で無効） |
| `AIRFLOW_HTTP_AUTH_TOKEN` | — | メトリクス/ダッシュボードの Bearer 認証（空で無効） |
| `AIRFLOW_AUTH_LOCALHOST_BYPASS` | `false` | `true` で localhost からの認証をスキップ |

詳細は `.env.example` を参照。

## 設計ドキュメント

`docs/design/` に DDD-01〜DDD-08 の詳細設計書があります。

## 本番デプロイ（systemd）

```bash
# ユニットファイルをコピーして有効化
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-taskboard airflow-models airflow-gpu \
  airflow-notes airflow-agents airflow-dashboard airflow-dispatcher airflow-batch.timer
sudo systemctl start airflow-taskboard airflow-models airflow-gpu \
  airflow-notes airflow-agents airflow-dashboard airflow-dispatcher
sudo systemctl start airflow-batch.timer   # 毎朝 03:00 に自動実行

# 詳細手順: deploy/systemd/README.md を参照
```

## フェーズ状況

| フェーズ | 内容 | 状態 |
|---------|------|------|
| Phase 1 | 横断基盤（スキーマ・MCP・エラー処理） | ✅ 完了 |
| Phase 2 | MVP（単一エージェント・Ollama連携） | ✅ 完了 |
| Phase 3 | Recommend モード・精度補完 | ✅ 完了 |
| Phase 4 | 並列実行・GPU スケジューラ | ✅ 完了 |
| Phase 5 | チーム拡張・JWT・本番化・systemd | ✅ 完了 |

**実装完了**: 1,337 テスト全パス・カバレッジ 100%・ruff clean

残作業は実機での運用検証のみ（DDD-08 §8〜§10）。

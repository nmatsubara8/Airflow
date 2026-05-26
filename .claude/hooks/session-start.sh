#!/bin/bash
set -euo pipefail

# リモート環境（Claude Code on the web）でのみ実行
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

echo "[session-start] Installing airflow-mcp dependencies..."

# 本体 + dev 依存を一括インストール（hatchling ビルドバックエンド使用）
pip install --quiet -e ".[dev]"

# インストール確認
echo "[session-start] Verifying key tools..."
python -m ruff --version
python -m pytest --version
python -c "import jwt; print('PyJWT OK:', jwt.__version__)"
python -c "import cryptography; print('cryptography OK:', cryptography.__version__)"

# MCP サーバーが必要とするディレクトリを事前作成
mkdir -p /home/user/Airflow/.airflow/tasks

echo "[session-start] Setup complete."

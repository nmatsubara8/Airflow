#!/usr/bin/env bash
# MCP server launcher — loads .env then starts the specified server.
# Usage: mcp-start.sh <server_name>
#   server_name: taskboard | notes | models | gpu | agents

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_DIR}/.env"

# Load .env (skip comments and empty lines)
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | sed 's/[[:space:]]*#.*$//')
    set +a
fi

VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python"

exec "$VENV_PYTHON" -m airflow_mcp "$1"

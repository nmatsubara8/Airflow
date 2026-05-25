#!/usr/bin/env bash
# MCP server launcher — loads .env then starts the specified server.
# Usage: mcp-start.sh <server_name>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_DIR}/.env"

if [ -f "$ENV_FILE" ]; then
    while IFS= read -r line; do
        case "$line" in
            ""|\#*) continue ;;
        esac
        line="${line%%#*}"
        line="${line%"${line##*[![:space:]]}"}"
        [ -z "$line" ] && continue
        export "$line"
    done < "$ENV_FILE"
fi

# Ensure AIRFLOW_TASKBOARD_DIR is absolute
export AIRFLOW_TASKBOARD_DIR="${PROJECT_DIR}/.airflow/tasks"

cd "${PROJECT_DIR}" || exit 1
exec "${PROJECT_DIR}/.venv/bin/python" -m airflow_mcp "$1"

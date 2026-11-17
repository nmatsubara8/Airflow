#!/usr/bin/env bash
# Install airflow-mcp systemd units with correct paths.
# Usage: sudo bash deploy/systemd/install.sh
set -euo pipefail

INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Prefer venv python if it exists, else system python
VENV_PYTHON="${INSTALL_DIR}/.venv/bin/python"
PYTHON="$([ -x "${VENV_PYTHON}" ] && echo "${VENV_PYTHON}" || command -v python || command -v python3)"
ENV_FILE="${INSTALL_DIR}/.env"

echo "Install dir : ${INSTALL_DIR}"
echo "Python      : ${PYTHON}"
echo "Env file    : ${ENV_FILE}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "WARNING: .env not found at ${ENV_FILE}. Copying from .env.example..."
  cp "${INSTALL_DIR}/.env.example" "${ENV_FILE}"
  echo "         Edit ${ENV_FILE} before starting services."
fi

# Install dashboard wrapper
install -m 755 "${INSTALL_DIR}/deploy/systemd/airflow-dashboard.sh" \
               "${INSTALL_DIR}/deploy/systemd/airflow-dashboard-start.py"

# Copy and patch each .service file
for src in "${INSTALL_DIR}/deploy/systemd/"*.service; do
  name="$(basename "${src}")"
  dest="/etc/systemd/system/${name}"
  sed \
    -e "s|/opt/airflow|${INSTALL_DIR}|g" \
    -e "s|ExecStart=python |ExecStart=${PYTHON} |g" \
    -e "/airflow-dashboard/s|ExecStart=.*|ExecStart=${PYTHON} ${INSTALL_DIR}/deploy/systemd/airflow-dashboard.sh|" \
    "${src}" > "${dest}"
  echo "  installed ${dest}"
done

# Copy timer
cp "${INSTALL_DIR}/deploy/systemd/airflow-batch.timer" /etc/systemd/system/

systemctl daemon-reload
echo ""
echo "Done. Start with:"
echo "  sudo systemctl enable --now airflow-taskboard airflow-models airflow-gpu \\"
echo "    airflow-notes airflow-agents airflow-dashboard airflow-dispatcher airflow-batch.timer"

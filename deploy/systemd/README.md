# Airflow systemd Unit Files

Production deployment using systemd for process management.

## Prerequisites

1. Install the package to `/opt/airflow`:
   ```bash
   cd /opt/airflow
   pip install -e .
   ```

2. Copy your `.env` file:
   ```bash
   cp .env.example /opt/airflow/.env
   # Edit .env — especially set AIRFLOW_AUTH_LOCALHOST_BYPASS=false for production
   ```

3. Generate JWT keypair (if using JWT auth):
   ```bash
   python -m airflow_mcp auth keygen
   ```

## Installation

```bash
# Copy unit files
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable all services (start on boot)
sudo systemctl enable \
  airflow-taskboard \
  airflow-models \
  airflow-gpu \
  airflow-notes \
  airflow-agents \
  airflow-dashboard \
  airflow-dispatcher \
  airflow-batch.timer

# Start MCP servers first
sudo systemctl start \
  airflow-taskboard \
  airflow-models \
  airflow-gpu \
  airflow-notes \
  airflow-agents

# Then start dependent services
sudo systemctl start airflow-dashboard airflow-dispatcher

# Enable nightly batch timer
sudo systemctl start airflow-batch.timer
```

## Service Overview

| Service | Role | Port |
|---------|------|------|
| `airflow-taskboard` | Taskboard MCP server (stdio) | — |
| `airflow-models` | Model management MCP server (stdio) | — |
| `airflow-gpu` | GPU state MCP server (stdio) | — |
| `airflow-notes` | Note search MCP server (stdio) | — |
| `airflow-agents` | Agent registry MCP server (stdio) | — |
| `airflow-dashboard` | Read-only web dashboard | 7400 |
| `airflow-dispatcher` | Inbox polling loop | — |
| `airflow-batch.service` | One-shot batch runner (via timer) | — |
| `airflow-batch.timer` | Nightly 03:00 trigger | — |

## Status / Logs

```bash
# Status overview
sudo systemctl status airflow-taskboard

# Follow logs
sudo journalctl -u airflow-taskboard -f
sudo journalctl -u airflow-batch -f

# Manually run batch (for testing)
sudo systemctl start airflow-batch
sudo journalctl -u airflow-batch --since "1 minute ago"

# Timer status
systemctl list-timers airflow-batch.timer
```

## Production Checklist

Before going live, verify:

- [ ] `AIRFLOW_AUTH_LOCALHOST_BYPASS=false` in `.env`
- [ ] `AIRFLOW_MCP_TOKEN_ADMIN` set to a strong random value
- [ ] Ed25519 keypair generated (`auth keygen`) and keys are not world-readable
- [ ] `AIRFLOW_SLACK_WEBHOOK` configured for alerts
- [ ] `AIRFLOW_HTTP_AUTH_TOKEN` set for dashboard access
- [ ] Run `python -m airflow_mcp config validate` — all configs OK
- [ ] Firewall: port 7400 accessible only from intended hosts

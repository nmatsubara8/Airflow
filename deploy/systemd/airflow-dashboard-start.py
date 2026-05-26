#!/usr/bin/env python3
"""Dashboard startup wrapper for systemd (avoids quoting issues with ExecStart)."""
import os
import time

from airflow_mcp.taskboard.store import TaskStore
from airflow_mcp.web.dashboard import start_dashboard

store = TaskStore(os.environ.get("AIRFLOW_TASKBOARD_DIR", ".airflow/tasks"))
start_dashboard(store)
while True:
    time.sleep(60)

#!/usr/bin/env python3
"""Metrics server startup wrapper for systemd/Docker (avoids inline Python quoting issues)."""
import time

from airflow_mcp.ops.metrics import start_metrics_server

start_metrics_server(store=None, port=9090)
while True:
    time.sleep(60)

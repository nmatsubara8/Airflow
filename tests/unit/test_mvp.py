"""mvp.py の単体テスト。"""
from __future__ import annotations

import threading
import pytest
from unittest.mock import MagicMock, patch


def test_run_all_starts_three_threads_and_stops_on_keyboard_interrupt():
    """run_all() が ingester/dispatcher/runner スレッドを起動し KeyboardInterrupt で停止すること。"""
    import airflow_mcp.mvp as mvp_mod

    started: list[str] = []

    def fake_loop(*args, **kwargs):
        # スレッドが起動したことを記録し、即座に終了
        pass

    with patch("airflow_mcp.mvp.ingester_loop", side_effect=fake_loop), \
         patch("airflow_mcp.mvp.dispatcher_loop", side_effect=fake_loop), \
         patch("airflow_mcp.mvp.runner_loop", side_effect=fake_loop), \
         patch("airflow_mcp.mvp.TaskStore", return_value=MagicMock()), \
         patch("airflow_mcp.mvp.load_ingester_config", return_value={}), \
         patch("airflow_mcp.mvp.load_dispatch_config", return_value={}), \
         patch("airflow_mcp.mvp.load_model_config", return_value={}), \
         patch("airflow_mcp.mvp.validate_ollama_url", return_value="http://localhost:11434"), \
         patch("airflow_mcp.mvp._load_registry", return_value={}), \
         patch("airflow_mcp.mvp.time.sleep", side_effect=KeyboardInterrupt):
        mvp_mod.run_all()  # KeyboardInterrupt で正常終了


def test_run_all_logs_warning_when_vault_root_not_set(monkeypatch, caplog):
    """VAULT_ROOT 未設定のとき警告ログが出ること。"""
    import logging
    import airflow_mcp.mvp as mvp_mod

    monkeypatch.delenv("VAULT_ROOT", raising=False)

    with patch("airflow_mcp.mvp.ingester_loop"), \
         patch("airflow_mcp.mvp.dispatcher_loop"), \
         patch("airflow_mcp.mvp.runner_loop"), \
         patch("airflow_mcp.mvp.TaskStore", return_value=MagicMock()), \
         patch("airflow_mcp.mvp.load_ingester_config", return_value={}), \
         patch("airflow_mcp.mvp.load_dispatch_config", return_value={}), \
         patch("airflow_mcp.mvp.load_model_config", return_value={}), \
         patch("airflow_mcp.mvp.validate_ollama_url", return_value="http://localhost:11434"), \
         patch("airflow_mcp.mvp._load_registry", return_value={}), \
         patch("airflow_mcp.mvp.time.sleep", side_effect=KeyboardInterrupt), \
         caplog.at_level(logging.WARNING, logger="airflow_mcp.mvp"):
        mvp_mod.run_all()

    assert any("VAULT_ROOT" in r.message for r in caplog.records)

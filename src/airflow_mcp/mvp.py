"""MVP オーケストレーター: ingester + dispatcher + runner を同一プロセスで起動。

使い方:
    # 全コンポーネント同時起動（スレッドベース）
    airflow-mvp

    # 個別起動
    airflow-ingester
    airflow-dispatcher
    airflow-runner

環境変数:
    VAULT_ROOT       監視対象 Vault のルートディレクトリ（必須）
    OLLAMA_URL       Ollama エンドポイント (default: http://localhost:11434)
    OLLAMA_STUB      true にすると Ollama を呼ばずスタブ応答を返す
    AIRFLOW_AGENT_ID 実行エージェント ID (default: agent_docs)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

from .dispatcher.main import load_config as load_dispatch_config, run_loop as dispatcher_loop
from .ingester.main import load_config as load_ingester_config, run_loop as ingester_loop
from .runner.main import (
    load_model_config,
    run_loop as runner_loop,
    _load_registry,
)
from .taskboard.store import TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_all() -> None:
    """ingester / dispatcher / runner をスレッドで並列起動。"""
    store = TaskStore()
    ingester_config = load_ingester_config()
    dispatch_config = load_dispatch_config()
    model_config = load_model_config()
    registry = _load_registry()
    agent_id = os.environ.get("AIRFLOW_AGENT_ID", "agent_docs")
    ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")

    vault_root = os.environ.get("VAULT_ROOT", "")
    if not vault_root:
        logger.warning("VAULT_ROOT not set — ingester will not watch any paths")

    threads = [
        threading.Thread(
            target=ingester_loop,
            args=(store, ingester_config),
            name="ingester",
            daemon=True,
        ),
        threading.Thread(
            target=dispatcher_loop,
            args=(store, dispatch_config),
            name="dispatcher",
            daemon=True,
        ),
        threading.Thread(
            target=runner_loop,
            args=(store, model_config, registry, agent_id, ollama_url),
            name="runner",
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()
        logger.info("started thread: %s", t.name)

    logger.info("MVP running — Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("shutting down")

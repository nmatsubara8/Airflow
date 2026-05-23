"""pytest 共有フィクスチャ。"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

# テスト全体で OLLAMA_STUB を有効化
os.environ.setdefault("OLLAMA_STUB", "true")
os.environ.setdefault("AIRFLOW_AUTH_LOCALHOST_BYPASS", "true")

FIXTURES_DIR = Path(__file__).parent / "fixtures"
VAULT_DIR = FIXTURES_DIR / "vaults" / "test-vault"
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def tmp_tasks_dir(tmp_path: Path) -> Path:
    """テスト専用のタスクストアディレクトリを返す。"""
    return tmp_path / "tasks"


@pytest.fixture
def task_store(tmp_tasks_dir: Path):
    """クリーンな TaskStore インスタンスを返す。"""
    from airflow_mcp.taskboard.store import TaskStore
    return TaskStore(base_dir=tmp_tasks_dir)


@pytest.fixture
def sample_task_fields() -> dict:
    return {
        "title": "テストタスク",
        "section": "engineering",
        "description": "テスト用タスクの説明",
        "tags": ["test", "engineering"],
        "priority": 3,
        "owner": None,
        "features": {"required_accuracy": "medium", "latency_sensitivity": "batch"},
    }


@pytest.fixture(autouse=True)
def _restore_ollama_stub():
    """各テスト後に OLLAMA_STUB を元の値に戻す。"""
    original = os.environ.get("OLLAMA_STUB", "true")
    yield
    os.environ["OLLAMA_STUB"] = original


@pytest.fixture
def fake_stdio_ctx():
    """asynccontextmanager として使える fake stdio_server を返す。

    MCP サーバーの run() 関数をテストする際に共通利用する:

        async def test_run(fake_stdio_ctx):
            with patch("airflow_mcp.common.mcp.stdio_server", fake_stdio_ctx):
                ...
    """
    from contextlib import asynccontextmanager
    from unittest.mock import MagicMock

    @asynccontextmanager
    async def _ctx():
        yield MagicMock(), MagicMock()

    return _ctx


@pytest.fixture
def vault_dir() -> Path:
    return VAULT_DIR


@pytest.fixture
def project_root() -> Path:
    return PROJECT_ROOT

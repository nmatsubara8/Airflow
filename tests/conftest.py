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


def pytest_collection_modifyitems(items: list) -> None:
    """tests/integration/ 配下のテストに integration マーカーを自動付与する。"""
    integration_mark = pytest.mark.integration
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent / "integration"):
            item.add_marker(integration_mark)


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


@pytest.fixture
def auth_isolated(monkeypatch, tmp_path):
    """auth モジュールを完全分離した状態で返す。

    - _LOCALHOST_BYPASS = False
    - _token_cache = None（テスト後もキャッシュ汚染防止）
    - _TOKENS_FILE = 存在しない一時パス（偶発的な実ファイル読み込み防止）

    追加変数が必要なテストは monkeypatch.setattr() で上書きできる:
        monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
        monkeypatch.setattr(auth_isolated, "_JWT_PUBLIC_KEY_PATH", pub)

    Yields: airflow_mcp.common.auth モジュール
    """
    import airflow_mcp.common.auth as auth_mod
    monkeypatch.setattr(auth_mod, "_LOCALHOST_BYPASS", False)
    monkeypatch.setattr(auth_mod, "_token_cache", None)
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tmp_path / "no_tokens.json")
    yield auth_mod
    auth_mod._token_cache = None  # monkeypatch 復元後もキャッシュを None に固定

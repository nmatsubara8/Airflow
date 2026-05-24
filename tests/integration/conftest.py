"""結合テスト共通フィクスチャ。"""
from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_taskboard(tmp_path: Path, monkeypatch):
    """テストごとに独立した AIRFLOW_TASKBOARD_DIR を設定する。

    TaskStore() がデフォルトパス (.airflow/tasks) を使う場合には影響しないが、
    AIRFLOW_TASKBOARD_DIR 参照コンポーネント（dispatcher, ingester 等）の
    環境変数ベース設定を分離し、テスト間の環境汚染を防ぐ。
    """
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    monkeypatch.setenv("AIRFLOW_TASKBOARD_DIR", str(tasks_dir))
    yield tasks_dir

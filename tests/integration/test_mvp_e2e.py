"""MVP エンドツーエンド統合テスト（DDD-04 §11）。

Vault .md → ingester → dispatcher → runner → done を 30 秒以内に完了すること。
OLLAMA_STUB=true で実行するため GPU 不要。
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

os.environ["OLLAMA_STUB"] = "true"

from airflow_mcp.dispatcher.main import dispatch_once, load_config as load_dispatch_config
from airflow_mcp.ingester.main import run_once as ingester_run_once
from airflow_mcp.runner.main import (
    _load_registry,
    load_model_config,
    run_once as runner_run_once,
)
from airflow_mcp.taskboard.store import TaskStore

VAULT_FIXTURE = Path(__file__).parent.parent / "fixtures" / "vaults" / "test-vault"


@pytest.fixture
def ingester_config(tmp_path: Path) -> dict:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "task.md").write_text("""---
title: E2Eテストタスク
tags: [engineering, test]
priority: 3
section: engineering
features:
  required_accuracy: medium
  latency_sensitivity: batch
---

# E2Eテストタスク

エンドツーエンドテスト用のサンプルタスクです。
ingester が検出し dispatcher が割り当て、runner が処理して done にします。
""")
    return {
        "watch": {
            "paths": [str(vault)],
            "glob": "**/*.md",
            "exclude": [],
        },
        "stability": {
            "settle_ms": 0,
            "max_file_size_bytes": 524288,
            "poll_interval_ms": 5000,
        },
    }


def test_mvp_e2e_completes_within_30_seconds(
    tmp_tasks_dir: Path, ingester_config: dict
):
    store = TaskStore(base_dir=tmp_tasks_dir)
    dispatch_config = load_dispatch_config()
    model_config = load_model_config()
    registry = _load_registry()

    start = time.time()

    # Step 1: Vault → inbox
    n_ingested = ingester_run_once(store, ingester_config)
    assert n_ingested == 1, "1 つのタスクが inbox に作成されるべき"

    # Step 2: inbox → in_progress (dispatch)
    n_dispatched = dispatch_once(store, dispatch_config)
    assert n_dispatched == 1, "1 つのタスクが dispatch されるべき"

    in_progress = store.list_by_status("in_progress")
    assert len(in_progress) == 1
    assert in_progress[0]["owner"] == "agent_docs"
    assert in_progress[0]["section"] == "engineering"

    # Step 3: in_progress → done (runner)
    n_processed = runner_run_once(store, model_config, registry, "agent_docs", "http://localhost")
    assert n_processed == 1, "1 つのタスクが処理されるべき"

    elapsed = time.time() - start
    assert elapsed < 30.0, f"MVP e2e が {elapsed:.1f}s かかりました（30s 制限超過）"

    # 最終状態検証
    done_tasks = store.list_by_status("done")
    assert len(done_tasks) == 1
    task = done_tasks[0]
    assert task["status"] == "done"
    assert len(task["handoff_notes"]) >= 1
    note = task["handoff_notes"][-1]
    assert "要約" in note["body"]
    assert note["author"]["id"] == "agent_docs"


def test_mvp_e2e_deduplicates_same_file(tmp_tasks_dir: Path, ingester_config: dict):
    """同一ファイルを 2 回スキャンしても重複タスクが生成されないこと。"""
    import airflow_mcp.ingester.main as ingester_mod
    ingester_mod._seen_hashes.clear()  # 他テストの残留ハッシュをリセット

    store = TaskStore(base_dir=tmp_tasks_dir)
    n1 = ingester_run_once(store, ingester_config)
    n2 = ingester_run_once(store, ingester_config)
    assert n1 == 1, f"最初のスキャンで 1 タスク作成されるべき (got {n1})"
    assert n2 == 0, "重複取り込みが発生しました"


def test_mvp_e2e_full_pipeline_with_fixture_vault(tmp_tasks_dir: Path):
    """tests/fixtures/vault のサンプルファイルでパイプラインが動作すること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    config = {
        "watch": {
            "paths": [str(VAULT_FIXTURE / "Inbox")],
            "glob": "**/*.md",
            "exclude": [],
        },
        "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
    }
    dispatch_config = load_dispatch_config()
    model_config = load_model_config()
    registry = _load_registry()

    ingester_run_once(store, config)
    dispatch_once(store, dispatch_config)
    n = runner_run_once(store, model_config, registry, "agent_docs", "http://localhost")
    assert n >= 1
    assert store.list_by_status("done")


def test_mvp_e2e_inbox_empty_after_pipeline(tmp_tasks_dir: Path, ingester_config: dict):
    """パイプライン完了後、inbox が空になること。"""
    store = TaskStore(base_dir=tmp_tasks_dir)
    dispatch_config = load_dispatch_config()
    model_config = load_model_config()
    registry = _load_registry()

    ingester_run_once(store, ingester_config)
    dispatch_once(store, dispatch_config)
    runner_run_once(store, model_config, registry, "agent_docs", "http://localhost")

    assert store.list_by_status("inbox") == []
    assert store.list_by_status("in_progress") == []

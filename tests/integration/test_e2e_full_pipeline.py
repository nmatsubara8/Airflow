"""フルパイプライン E2E 結合テスト。

基本 MVP フロー（test_mvp_e2e.py）を補完し、以下のシナリオを追加検証する:

  1. needs_human 遷移 — runner がリトライ上限に達してエスカレーション
  2. バッチスケジューラ → パイプライン — batch_scheduler が生成したタスクを
     dispatcher → runner がそのまま処理して done になること
  3. モニター整合確認 — パイプライン各段階で get_status_dict() のカウントが
     実際のタスク状態と一致すること

設計上の制約:
  - batch_scheduler.run_schedule() は TaskStore() (デフォルト) を使う
  - ops.monitor._load_task_counts / _load_section_counts も TaskStore() を使う
  - これらと pipeline_env の孤立 TaskStore(base_dir=...) は別インスタンスになる
  - TestBatchSchedulerPipeline と TestMonitorConsistency はデフォルト TaskStore
    を使うか、monitor 関数をパッチして整合性を確保する
"""
from __future__ import annotations

import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("OLLAMA_STUB", "true")

from airflow_mcp.dispatcher.main import dispatch_once
from airflow_mcp.ingester.main import run_once as ingester_run_once
from airflow_mcp.ops.monitor import get_status_dict
from airflow_mcp.orchestrator.batch_scheduler import run_schedule
from airflow_mcp.runner.main import run_once as runner_run_once
from airflow_mcp.taskboard.store import TaskStore


# ── 共通フィクスチャ ──────────────────────────────────────────────

@pytest.fixture
def pipeline_env(tmp_path: Path, monkeypatch):
    """パイプライン全体を分離するディレクトリ群を準備する。

    ingester / dispatcher / runner は store 引数でこのストアを使用する。
    batch_scheduler と monitor は内部で TaskStore() を生成するため別途対応が必要。
    """
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)  # isolated_taskboard (autouse) が先に作成する場合がある
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir(exist_ok=True)

    store = TaskStore(base_dir=tasks_dir)

    return {
        "tasks_dir": tasks_dir,
        "vault_dir": vault_dir,
        "store": store,
        "ingester_config": {
            "watch": {"paths": [str(vault_dir)], "glob": "**/*.md", "exclude": []},
            "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
        },
        "dispatch_config": {},
        "model_config": {},
        "registry": {},
    }


@pytest.fixture
def monitor_pipeline_env(tmp_path: Path, monkeypatch):
    """monitor 関数もパッチして pipeline_env と同じ TaskStore を参照させる。

    ops.monitor._load_task_counts / _load_section_counts を差し替えることで
    get_status_dict() が孤立 TaskStore から正確なカウントを返すようにする。
    """
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir(exist_ok=True)

    store = TaskStore(base_dir=tasks_dir)

    def _task_counts() -> dict[str, int]:
        return {s: len(store.list_by_status(s))
                for s in ("inbox", "in_progress", "done",
                           "needs_human", "blocked", "cancelled")}

    def _section_counts(_counts_by_status: dict) -> dict[str, dict[str, int]]:
        sections: dict[str, dict[str, int]] = {}
        for task in store.list_by_status("in_progress"):
            sec = task.get("section", "engineering")
            sections.setdefault(sec, {"active": 0, "total": 0})
            sections[sec]["active"] += 1
        for status in ("inbox", "in_progress", "done", "needs_human", "blocked"):
            for task in store.list_by_status(status):
                sec = task.get("section", "engineering")
                sections.setdefault(sec, {"active": 0, "total": 0})
                sections[sec]["total"] += 1
        return sections

    monkeypatch.setattr("airflow_mcp.ops.monitor._load_task_counts", _task_counts)
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_section_counts", _section_counts)

    return {
        "tasks_dir": tasks_dir,
        "vault_dir": vault_dir,
        "store": store,
        "ingester_config": {
            "watch": {"paths": [str(vault_dir)], "glob": "**/*.md", "exclude": []},
            "stability": {"settle_ms": 0, "max_file_size_bytes": 524288},
        },
        "dispatch_config": {},
        "model_config": {},
        "registry": {},
    }


def _place_md(vault_dir: Path, name: str, title: str, section: str = "engineering") -> Path:
    """テスト用 .md ファイルを vault に配置する。"""
    md = vault_dir / name
    md.write_text(textwrap.dedent(f"""\
        ---
        title: {title}
        tags: [engineering, test]
        priority: 3
        section: {section}
        features:
          required_accuracy: medium
          latency_sensitivity: batch
        ---

        # {title}

        フルパイプライン E2E テスト用のサンプルタスクです。
    """), encoding="utf-8")
    return md


# ── シナリオ 1: needs_human 遷移 ─────────────────────────────────

class TestNeedsHumanEscalation:

    def test_failed_task_transitions_to_needs_human(self, pipeline_env):
        """runner がすべてのリトライで schema_valid=False を返すと needs_human に遷移する。"""
        env = pipeline_env
        _place_md(env["vault_dir"], "fail-task.md", "失敗テストタスク")

        store: TaskStore = env["store"]
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])

        assert len(store.list_by_status("in_progress")) == 1

        # _call_ollama が (content, parsed, schema_valid) タプルを返すことに合わせる
        with (
            patch("airflow_mcp.runner.main._call_ollama",
                  return_value=("invalid json output", None, False)),
            patch("airflow_mcp.runner.main.time.sleep"),  # リトライ待機をスキップ
        ):
            runner_run_once(
                store, env["model_config"], env["registry"],
                "agent_docs", "http://localhost",
            )

        assert store.list_by_status("needs_human"), "needs_human への遷移が起きていない"
        assert store.list_by_status("done") == []
        assert store.list_by_status("in_progress") == []

    def test_needs_human_task_has_reason_in_handoff(self, pipeline_env):
        """needs_human に遷移したタスクの handoff_notes にエスカレーション理由がある。"""
        env = pipeline_env
        _place_md(env["vault_dir"], "fail2.md", "エスカレーションタスク")

        store: TaskStore = env["store"]
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])

        with (
            patch("airflow_mcp.runner.main._call_ollama",
                  return_value=("", None, False)),
            patch("airflow_mcp.runner.main.time.sleep"),
        ):
            runner_run_once(
                store, env["model_config"], env["registry"],
                "agent_docs", "http://localhost",
            )

        nh_tasks = store.list_by_status("needs_human")
        assert nh_tasks, "needs_human タスクが存在しない"
        task = nh_tasks[0]
        assert task.get("handoff_notes"), "handoff_notes が空"

    def test_needs_human_count_reflected_in_monitor(self, monitor_pipeline_env):
        """needs_human タスクが get_status_dict() の task_counts に反映される。"""
        env = monitor_pipeline_env
        _place_md(env["vault_dir"], "fail3.md", "モニター確認タスク")

        store: TaskStore = env["store"]
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])

        with (
            patch("airflow_mcp.runner.main._call_ollama",
                  return_value=("", None, False)),
            patch("airflow_mcp.runner.main.time.sleep"),
        ):
            runner_run_once(
                store, env["model_config"], env["registry"],
                "agent_docs", "http://localhost",
            )

        status = get_status_dict()
        assert status["tasks"].get("needs_human", 0) >= 1


# ── シナリオ 2: バッチスケジューラ → パイプライン ──────────────

class TestBatchSchedulerPipeline:

    def test_batch_dispatch_tasks_flow_through_pipeline(self, monkeypatch):
        """batch_scheduler が生成したタスクを dispatcher → runner が done にすること。

        batch_scheduler は TaskStore()（デフォルト）を使うため、
        dispatcher / runner も同じデフォルト TaskStore を通じてタスクを処理する。
        """
        default_store = TaskStore()

        schedule = {
            "name": "e2e_batch_pipeline",
            "actions": [{
                "dispatch": {
                    "tasks": ["review"],
                    "targets": ["agent_docs"],
                    "tag": "e2e",
                }
            }],
        }

        # バッチ実行前のカウント（累積に対応）
        before_inbox = len([t for t in default_store.list_all()
                            if "e2e_batch_pipeline" in t.get("title", "")])

        run_schedule(schedule)

        # batch_scheduler がデフォルト TaskStore にタスクを作成したことを確認
        after_inbox = len([t for t in default_store.list_all()
                           if "e2e_batch_pipeline" in t.get("title", "")])
        assert after_inbox - before_inbox == 1, "batch_scheduler がタスクを生成していない"

        # デフォルト TaskStore を使って dispatcher / runner を実行
        before_done = len(default_store.list_by_status("done"))
        n_dispatched = dispatch_once(default_store, {})
        assert n_dispatched >= 1

        n_processed = runner_run_once(
            default_store, {}, {}, "agent_docs", "http://localhost",
        )
        assert n_processed >= 1

        after_done = len(default_store.list_by_status("done"))
        assert after_done > before_done, "batch_scheduler 生成タスクが done になっていない"

    def test_batch_schedule_result_has_required_fields(self):
        """run_schedule() の戻り値が必須フィールドを含み、duration が非負である。"""
        schedule = {
            "name": "fields_check_e2e",
            "actions": [{"cleaner": {}}],
        }
        result = run_schedule(schedule)

        for field in ("name", "success", "total", "duration_s"):
            assert field in result, f"フィールド '{field}' が存在しない"
        assert result["duration_s"] >= 0.0
        assert result["name"] == "fields_check_e2e"


# ── シナリオ 3: モニター整合確認 ─────────────────────────────────

class TestMonitorConsistency:

    def test_inbox_count_matches_ingested_tasks(self, monitor_pipeline_env):
        """ingester 実行後、get_status_dict().tasks.inbox が実際の inbox 数と一致する。"""
        env = monitor_pipeline_env
        store: TaskStore = env["store"]

        _place_md(env["vault_dir"], "mon1.md", "モニターテスト1")
        _place_md(env["vault_dir"], "mon2.md", "モニターテスト2")

        ingester_run_once(store, env["ingester_config"])

        inbox_actual = len(store.list_by_status("inbox"))
        status = get_status_dict()
        assert status["tasks"]["inbox"] == inbox_actual

    def test_done_count_increases_after_pipeline(self, monitor_pipeline_env):
        """パイプライン完了後、get_status_dict().tasks.done が増加する。"""
        env = monitor_pipeline_env
        store: TaskStore = env["store"]

        done_before = get_status_dict()["tasks"].get("done", 0)

        _place_md(env["vault_dir"], "mon3.md", "done カウントテスト")
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])
        runner_run_once(store, env["model_config"], env["registry"],
                        "agent_docs", "http://localhost")

        done_after = get_status_dict()["tasks"].get("done", 0)
        assert done_after > done_before, "done カウントが増加していない"

    def test_sections_reflect_in_progress_tasks(self, monitor_pipeline_env):
        """dispatch 後、get_status_dict().sections.engineering.active が増加する。"""
        env = monitor_pipeline_env
        store: TaskStore = env["store"]

        active_before = get_status_dict().get("sections", {}).get("engineering", {}).get("active", 0)

        _place_md(env["vault_dir"], "sec1.md", "セクション確認タスク", section="engineering")
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])

        active_after = get_status_dict().get("sections", {}).get("engineering", {}).get("active", 0)
        assert active_after > active_before, "sections.engineering.active が増加していない"

    def test_monitor_timestamp_advances(self):
        """連続して get_status_dict() を呼ぶと timestamp が単調増加する。"""
        import time
        s1 = get_status_dict()
        time.sleep(0.01)
        s2 = get_status_dict()
        assert s2["timestamp"] >= s1["timestamp"]

    def test_full_pipeline_leaves_no_inbox_or_in_progress(self, pipeline_env):
        """完全なパイプライン後、inbox と in_progress が空になること。"""
        env = pipeline_env
        store: TaskStore = env["store"]

        _place_md(env["vault_dir"], "full1.md", "完全パイプラインテスト")
        ingester_run_once(store, env["ingester_config"])
        dispatch_once(store, env["dispatch_config"])
        runner_run_once(store, env["model_config"], env["registry"],
                        "agent_docs", "http://localhost")

        assert store.list_by_status("inbox") == []
        assert store.list_by_status("in_progress") == []

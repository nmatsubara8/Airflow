"""
MVP エンドツーエンド確認スクリプト（OLLAMA_STUB=true）

フロー: Inbox .md → ingester.run_once → dispatcher.dispatch_once → runner.run_once → done

使い方:
    python scripts/e2e_ingester_check.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import textwrap
from pathlib import Path

# ── 環境変数セットアップ ──────────────────────────────────────
os.environ["OLLAMA_STUB"] = "true"
os.environ["AIRFLOW_AUTH_LOCALHOST_BYPASS"] = "true"

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import logging  # noqa: E402
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("e2e")

SEP = "─" * 60

# ── ヘルパー ──────────────────────────────────────────────────

def print_step(n: int, title: str) -> None:
    print(f"\n{SEP}")
    print(f"  STEP {n}: {title}")
    print(SEP)


def print_task(task: dict) -> None:
    print(f"  id      : {task['id']}")
    print(f"  title   : {task['title']}")
    print(f"  status  : {task['status']}")
    print(f"  section : {task.get('section', '-')}")
    print(f"  owner   : {task.get('owner', '-')}")
    if task.get("handoff_notes"):
        note_body = task["handoff_notes"][-1]["body"]
        print(f"  handoff : {note_body[:120]}{'...' if len(note_body) > 120 else ''}")


# ── メイン ────────────────────────────────────────────────────

def main() -> None:
    with tempfile.TemporaryDirectory(prefix="airflow_e2e_") as tmpdir:
        tmp = Path(tmpdir)

        # ディレクトリ構造を作成
        inbox_dir = tmp / "vault" / "Inbox"
        inbox_dir.mkdir(parents=True)
        tasks_dir = tmp / "tasks"
        tasks_dir.mkdir()
        notes_dir = tmp / ".airflow" / "notes"
        notes_dir.mkdir(parents=True)

        os.environ["VAULT_ROOT"] = str(tmp / "vault")
        os.environ["AIRFLOW_TASKBOARD_DIR"] = str(tasks_dir)

        # ── Step 0: サンプル .md を Inbox に配置 ─────────────────
        print_step(0, "Inbox に .md ファイルを配置")
        sample_md = inbox_dir / "api-rate-limit.md"
        sample_md.write_text(textwrap.dedent("""\
            ---
            title: APIレート制限の実装
            tags: [engineering, backend, api]
            priority: 3
            section: engineering
            features:
              required_accuracy: medium
              latency_sensitivity: batch
            ---

            # APIレート制限の実装

            現在のAPIエンドポイントにはレート制限が実装されておらず、
            過負荷攻撃に対して脆弱な状態です。

            ## 要件
            - 1分間あたり最大100リクエスト
            - IPアドレスおよびAPIキー単位での制限
            - 超過時は429 Too Many Requests
        """), encoding="utf-8")
        print(f"  作成: {sample_md}")

        # ── Step 1: Ingester ─────────────────────────────────────
        print_step(1, "ingester.run_once() — .md → TaskStore(inbox)")
        from airflow_mcp.ingester.main import run_once as ingest_once
        import airflow_mcp.ingester.main as ingester_mod
        ingester_mod._notes_index_path = notes_dir / "index.json"
        ingester_mod._seen_hashes = set()   # reset dedup for this run

        from airflow_mcp.taskboard.store import TaskStore
        store = TaskStore(base_dir=tasks_dir)

        ingester_config = {
            "watch_paths": [{"path": str(inbox_dir), "auto_dispatch": True}],
            "stability": {"settle_ms": 0, "poll_interval_ms": 5000, "max_file_size_bytes": 524288},
            "watch": {"glob": "**/*.md", "exclude": []},
        }
        n_ingested = ingest_once(store, ingester_config)
        print(f"  取り込み数: {n_ingested}")
        if n_ingested == 0:
            print("  ✗ ファイルが取り込まれませんでした")
            sys.exit(1)

        inbox_tasks = store.list_by_status("inbox")
        print(f"  inbox タスク数: {len(inbox_tasks)}")
        for t in inbox_tasks:
            print_task(t)

        # ── Step 2: Dispatcher ───────────────────────────────────
        print_step(2, "dispatcher.dispatch_once() — inbox → in_progress")
        from airflow_mcp.dispatcher.main import dispatch_once
        dispatcher_config = {}
        n_dispatched = dispatch_once(store, dispatcher_config)
        print(f"  ディスパッチ数: {n_dispatched}")

        in_progress = store.list_by_status("in_progress")
        print(f"  in_progress タスク数: {len(in_progress)}")
        for t in in_progress:
            print_task(t)

        if not in_progress:
            print("  ✗ in_progress タスクがありません")
            sys.exit(1)

        # ── Step 3: Runner ───────────────────────────────────────
        print_step(3, "runner.run_once() — in_progress → done  [OLLAMA_STUB=true]")
        from airflow_mcp.runner.main import run_once as runner_once
        runner_config = {}
        registry: dict = {}
        agent_id = in_progress[0].get("owner", "agent_docs")
        n_processed = runner_once(store, runner_config, registry, agent_id, "http://localhost:11434")
        print(f"  処理数: {n_processed}")

        # ── Step 4: 結果確認 ─────────────────────────────────────
        print_step(4, "最終タスク状態の確認")
        task_id = inbox_tasks[0]["id"]
        final = store.get(task_id)
        print_task(final)

        print(f"\n{SEP}")
        if final["status"] == "done":
            print("  ✓ MVP end-to-end PASS: inbox → in_progress → done")
        else:
            print(f"  ✗ FAIL: 最終ステータスが '{final['status']}' (expected: done)")
            sys.exit(1)
        print(SEP)

        # ── Step 5: 重複取り込みの確認（dedup） ──────────────────
        print_step(5, "dedup 確認 — 同じ .md を再実行しても新タスクは生成されないこと")
        n_dup = ingest_once(store, ingester_config)
        print(f"  2回目の取り込み数: {n_dup}  (期待値: 0)")
        if n_dup == 0:
            print("  ✓ dedup PASS")
        else:
            print(f"  ✗ dedup FAIL: {n_dup} 件重複取り込みされました")
            sys.exit(1)

        print(f"\n{SEP}")
        print("  ALL CHECKS PASSED")
        print(SEP)


if __name__ == "__main__":
    main()

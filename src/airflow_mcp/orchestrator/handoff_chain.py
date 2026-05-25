"""マルチエージェント handoff チェーン（DDD-06 §3 / DDD-08 §3.1）。

タスク完了時に chains.yaml で定義された次ステップのタスクを自動生成し、
親タスクの ``chain`` フィールドに実行履歴を記録する。
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from ..taskboard.store import TaskStore

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(os.environ.get("AIRFLOW_CHAINS_CONFIG", ".airflow/config/chains.yaml"))


# ── 設定ロード ────────────────────────────────────────────


def load_chains(config_path: Path = _DEFAULT_CONFIG) -> list[dict[str, Any]]:
    """chains.yaml からチェーン定義リストを返す。ファイルが無ければ空リスト。"""
    if not config_path.exists():
        return []
    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if not isinstance(cfg, dict):
            logger.warning("handoff_chain: chains.yaml is not a mapping, ignoring")
            return []
        chains = cfg.get("chains", [])
        if not isinstance(chains, list):
            logger.warning("handoff_chain: 'chains' must be a list, ignoring")
            return []
        return chains
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("handoff_chain: failed to load chains config: %s", exc)
        return []


# ── マッチング ────────────────────────────────────────────


def _task_matches_trigger(task: dict[str, Any], trigger: dict[str, Any]) -> bool:
    """タスクが trigger 条件に一致するか確認する。

    trigger キーは section / task_type / tags の組み合わせで指定。
    すべての条件を AND 評価する。
    """
    if not trigger:
        return False
    for key, expected in trigger.items():
        if key == "section":
            if task.get("section", "") != expected:
                return False
        elif key == "task_type":
            actual = task.get("features", {}).get("task_type") or task.get("task_type", "")
            if actual != expected:
                return False
        elif key == "tags":
            req = set(expected) if isinstance(expected, list) else {expected}
            if not req.intersection(task.get("tags", [])):
                return False
        # 未知のキーは無視（拡張性確保）
    return True


def find_next_steps(
    task: dict[str, Any],
    chains: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """タスクに対応する次ステップのリストを返す。

    Returns:
        各要素は step dict に ``chain_name`` を追加したもの。
    """
    steps: list[dict[str, Any]] = []
    for chain in chains:
        if not isinstance(chain, dict):
            continue
        trigger = chain.get("trigger", {})
        if _task_matches_trigger(task, trigger):
            for step in chain.get("steps", []):
                if isinstance(step, dict):
                    steps.append({"chain_name": chain.get("name", ""), **step})
    return steps


# ── タスク生成 ────────────────────────────────────────────


def spawn_handoff_tasks(
    source_task: dict[str, Any],
    store: TaskStore,
    chains: list[dict[str, Any]],
) -> list[str]:
    """完了タスクを起点に次ステップのタスクを生成し、生成した task_id リストを返す。

    - 親タスクの handoff_notes 最新エントリをコンテキストとして引き継ぐ。
    - 生成した各タスクに parent_task_id を記録する。
    """
    steps = find_next_steps(source_task, chains)
    if not steps:
        return []

    notes = source_task.get("handoff_notes", [])
    context = notes[-1].get("content", "") if notes else ""

    created_ids: list[str] = []
    for step in steps:
        prefix = step.get("title_prefix", "[Handoff] ")
        title = f"{prefix}{source_task.get('title', '')}"
        task_data: dict[str, Any] = {
            "title": title,
            "description": (
                f"前タスク: {source_task.get('id', '')}\n\n"
                + (context[:500] if context else "")
            ).strip(),
            "section": step.get("section", source_task.get("section", "engineering")),
            "tags": list(source_task.get("tags", [])) + ["handoff"],
            "priority": step.get("priority", source_task.get("priority", 5)),
            "parent_task_id": source_task["id"],
            "chain_name": step.get("chain_name", ""),
        }
        if owner := step.get("owner"):
            task_data["owner"] = owner

        try:
            new_task = store.create(task_data)
            created_ids.append(new_task["id"])
            logger.info(
                "handoff.spawned parent=%s child=%s chain=%s owner=%s",
                source_task["id"], new_task["id"],
                step.get("chain_name", ""), step.get("owner", ""),
            )
        except Exception as exc:
            logger.error(
                "handoff.spawn_failed parent=%s step=%s: %s",
                source_task["id"], step, exc,
            )

    return created_ids


# ── 連鎖履歴の記録 ───────────────────────────────────────


def record_chain_step(
    task_id: str,
    agent_id: str,
    note_id: str | None,
    store: TaskStore,
) -> None:
    """タスクの ``chain`` フィールドにエージェント完了ステップを追記する。

    chain フィールドはスキーマ上の配列（DDD-08 §3.1）。
    """
    try:
        task = store.get(task_id)
        chain_history: list[dict[str, Any]] = list(task.get("chain", []))
        chain_history.append({
            "agent_id": agent_id,
            "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "result_ref": note_id,
        })
        store.update(task_id, task["lock"]["version"], {"chain": chain_history})
    except Exception as exc:
        logger.warning("handoff.record_chain_step failed task=%s: %s", task_id, exc)

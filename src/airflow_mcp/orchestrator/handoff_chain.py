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

if TYPE_CHECKING:
    from ..taskboard.store import TaskStore

from ..common.io import safe_yaml_load

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = Path(os.environ.get("AIRFLOW_CHAINS_CONFIG", ".airflow/config/chains.yaml"))


# ── 設定ロード ────────────────────────────────────────────


def load_chains(config_path: Path | None = None) -> list[dict[str, Any]]:
    """chains.yaml からチェーン定義リストを返す。ファイルが無ければ空リスト。"""
    cfg = safe_yaml_load(config_path if config_path is not None else _DEFAULT_CONFIG)
    if not cfg:
        return []
    chains = cfg.get("chains", [])
    if not isinstance(chains, list):
        logger.warning("handoff_chain: 'chains' must be a list, ignoring")
        return []
    return chains


# ── マッチング ────────────────────────────────────────────


def _task_matches_trigger(task: dict[str, Any], trigger: dict[str, Any]) -> bool:
    """タスクが trigger 条件に一致するか確認する。

    trigger キーは section / task_type / tags / chain_depth の組み合わせで AND 評価。
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
        elif key == "chain_depth":
            if len(task.get("chain", [])) != int(expected):
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
    context = notes[-1].get("body", notes[-1].get("content", "")) if notes else ""

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


# ── ファンアウト + fan-in 登録 ────────────────────────────


def spawn_with_join_tracking(
    source_task: dict[str, Any],
    store: "TaskStore",
    chains: list[dict[str, Any]],
) -> list[str]:
    """spawn_handoff_tasks を呼び、複数タスク生成時は親に join 情報を登録する。

    fan-out で 2 件以上の子タスクが生成された場合、親タスクの ``join`` フィールドに
    ``child_ids`` を記録して tasks.join ツールで収束を待てるようにする（DDD-08 §3.2）。
    """
    child_ids = spawn_handoff_tasks(source_task, store, chains)
    if len(child_ids) > 1:
        join_data = {
            "parent_id": source_task["id"],
            "child_ids": child_ids,
            "policy": "all",
        }
        try:
            store.update(
                source_task["id"],
                source_task["lock"]["version"],
                {"join": join_data},
            )
        except Exception as exc:
            logger.warning(
                "handoff.join_register_failed parent=%s: %s", source_task["id"], exc
            )
    return child_ids


# ── 順次チェーン（intra-task sequential routing）────────────────────────────


def detect_cycle(task: dict[str, Any], next_agent: str, max_per_agent: int = 1) -> bool:
    """次のエージェントが既に上限回数処理済みであれば True（無限ループ防止）。"""
    chain = task.get("chain", [])
    count = sum(1 for step in chain if step.get("agent_id") == next_agent)
    return count >= max_per_agent


def route_to_next_agent(
    task: dict[str, Any],
    current_agent_id: str,
    store: "TaskStore",
    chains: list[dict[str, Any]],
) -> str | None:
    """順次チェーン（mode: sequential）で次のエージェントにタスクをルーティングする。

    ``task["chain"]`` の深さを現在のステップインデックスとして使い、
    chains.yaml の ``agents`` リストから次を選択する。
    一致するチェーンが無い・チェーンが末端・サイクル検出の場合は None を返す。

    Args:
        task: record_chain_step 呼び出し後に re-read した最新タスク。
        current_agent_id: 今回ステップを完了したエージェント。
        store: TaskStore インスタンス。
        chains: load_chains() で得たチェーン定義リスト。

    Returns:
        ルーティング先 agent_id（遷移成功）または None（完結 / 該当なし）。
    """
    chain_depth = len(task.get("chain", []))
    task_id = task["id"]

    for chain in chains:
        if not isinstance(chain, dict):
            continue
        if chain.get("mode") != "sequential":
            continue
        trigger = chain.get("trigger", {})
        if not _task_matches_trigger(task, trigger):
            continue

        agents = chain.get("agents", [])
        if not isinstance(agents, list):
            continue
        if chain_depth >= len(agents):
            continue  # このチェーンは末端

        next_step = agents[chain_depth]
        if not isinstance(next_step, dict):
            continue

        next_agent = next_step.get("id")
        if not next_agent:
            continue
        if next_agent == current_agent_id:
            logger.debug(
                "handoff.route_skip task=%s: next_agent == current_agent", task_id
            )
            continue

        if detect_cycle(task, next_agent):
            logger.warning(
                "handoff.cycle_detected task=%s agent=%s depth=%d — skip",
                task_id, next_agent, chain_depth,
            )
            continue

        try:
            store.reassign_in_progress(
                task_id, next_agent,
                reason=f"chain:{chain.get('name', '')}:step{chain_depth}",
            )
            logger.info(
                "handoff.routed task=%s chain=%s step=%d agent=%s→%s",
                task_id, chain.get("name", ""), chain_depth,
                current_agent_id, next_agent,
            )
            return next_agent
        except Exception as exc:
            logger.error("handoff.route_failed task=%s: %s", task_id, exc)

    return None


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

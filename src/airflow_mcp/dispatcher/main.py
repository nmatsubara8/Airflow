"""タスクディスパッチャー（DDD-04 §6.4）。

inbox タスクをポーリングし、dispatch.yaml のルールに従って
エージェントへ割り当て（tasks.claim）する。
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import yaml

from ..common.audit import log_event, new_audit_id
from ..common.errors import TaskAlreadyClaimed, TaskLockConflict
from ..taskboard.store import TaskStore

logger = logging.getLogger(__name__)

# section → デフォルトエージェントのマッピング（Phase 2 MVP 固定）
_MVP_AGENT_MAP: dict[str, str] = {
    "engineering":   "agent_docs",
    "content":       "agent_docs",
    "business":      "agent_docs",
    "orchestration": "agent_docs",
}


def load_config(config_path: str = ".airflow/config/dispatch.yaml") -> dict[str, Any]:
    p = Path(config_path)
    if p.exists():
        cfg = yaml.safe_load(p.read_text()) or {}
        if not isinstance(cfg, dict):
            logger.warning("dispatch config is not a dict, using defaults")
            return {}
        return cfg
    return {}


def infer_section(task: dict[str, Any], config: dict[str, Any]) -> str:
    """
    タスクのタグ・タイトルからセクションを推定（DDD-04 §6.4）。
    frontmatter に section が指定されていればそれを優先。
    """
    if section := task.get("section"):
        return section

    tags = set(task.get("tags", []))
    title = (task.get("title") or "").lower()
    rules: list[dict] = config.get("routing", {}).get("rules", [])

    for rule in rules:
        category = rule.get("category", "")
        match_any = rule.get("match_any", {})
        rule_tags = set(match_any.get("tags", []))
        keywords = [kw.lower() for kw in match_any.get("title_keywords", [])]

        if tags & rule_tags:
            return category
        if any(kw in title for kw in keywords):
            return category

    return config.get("routing", {}).get("default_category", "engineering")


def select_agent(section: str, task: dict[str, Any], agents_dir: str = "agents") -> str:
    """
    セクションから担当エージェントを選択。
    Phase 2 MVP: section ごとに固定エージェント（_MVP_AGENT_MAP）。
    Phase 3 以降でディレクトリスキャンによる動的選択に切り替える。
    """
    return _MVP_AGENT_MAP.get(section, "agent_docs")


def dispatch_once(store: TaskStore, config: dict[str, Any]) -> int:
    """inbox タスクをスキャンして agent に割り当てる。処理数を返す。"""
    inbox_tasks = store.list_by_status("inbox")
    dispatched = 0

    for task in inbox_tasks:
        task_id = task["id"]
        try:
            # セクション推定
            section = infer_section(task, config)
            # エージェント選択
            agent_id = select_agent(section, task)
            # タスクにセクションを書き込み（前回未設定の場合）
            if not task.get("section"):
                task = store.update(task_id, task["lock"]["version"],
                                    {"section": section})
            # クレーム
            task = store.claim(task_id, agent_id)
            aid = new_audit_id()
            log_event("info", "task.dispatched", "dispatcher", aid,
                      task_id=task_id, agent_id=agent_id,
                      data={"section": section})
            logger.info("dispatched %s → %s (%s)", task_id, agent_id, section)
            dispatched += 1
        except TaskAlreadyClaimed:
            # 別ディスパッチャーが先にクレーム済み — 次ポーリングで自然に消える
            pass
        except TaskLockConflict as exc:
            logger.warning("lock conflict for %s, will retry next poll: %s", task_id, exc)
        except Exception:
            logger.exception("unexpected dispatch error for %s", task_id)

    return dispatched


def run_loop(store: TaskStore, config: dict[str, Any], poll_interval_s: float = 3.0) -> None:
    logger.info("dispatcher started, poll_interval=%.1fs", poll_interval_s)
    while True:
        try:
            n = dispatch_once(store, config)
            if n > 0:
                logger.info("dispatched %d tasks", n)
        except Exception as exc:
            logger.error("dispatcher error: %s", exc)
        time.sleep(poll_interval_s)


def _cli() -> None:
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    from ..taskboard.store import TaskStore
    store = TaskStore()
    config = load_config()
    run_loop(store, config)

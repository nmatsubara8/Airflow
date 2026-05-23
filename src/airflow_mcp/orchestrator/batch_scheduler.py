"""夜間バッチスケジューラ（DDD-08 §7）。

schedules.yaml に記述された cron スケジュールを処理し、
タスクをディスパッチ・監査ハッシュを計算する。
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from ..common.audit import log_event, new_audit_id
from ..ops.audit_integrity import run_chain_hash
from ..ops.notifier import notify_batch_complete

logger = logging.getLogger(__name__)

_SCHEDULES_PATH = Path(os.environ.get("AIRFLOW_SCHEDULES", ".airflow/config/schedules.yaml"))


def _load_schedules() -> list[dict[str, Any]]:
    if not _SCHEDULES_PATH.exists():
        return []
    cfg = yaml.safe_load(_SCHEDULES_PATH.read_text())
    raw = cfg.get("schedules", {})
    # dict 形式（name がキー）をリストに正規化
    if isinstance(raw, dict):
        return [{"name": name, **sched} for name, sched in raw.items()]
    return raw  # list 形式はそのまま


def _parse_cron(cron: str, now: datetime) -> bool:
    """cron 式が現在時刻にマッチするか判定する（分・時・曜日のみ対応）。"""
    parts = cron.strip().split()
    if len(parts) != 5:
        return False
    minute, hour, _dom, _month, dow = parts

    def matches(field: str, value: int) -> bool:
        if field == "*":
            return True
        if "," in field:
            return value in [int(x) for x in field.split(",")]
        try:
            return int(field) == value
        except ValueError:
            return False

    return (
        matches(minute, now.minute)
        and matches(hour, now.hour)
        and matches(dow, now.weekday())
    )


def _run_dispatch_action(action: dict[str, Any], schedule_name: str) -> int:
    """dispatch アクションを実行し、作成したタスク数を返す。"""
    try:
        from ..taskboard.store import TaskStore
        store = TaskStore()
        dispatch = action.get("dispatch", {})
        task_types = dispatch.get("tasks", [])
        targets = dispatch.get("targets", [])
        tag = dispatch.get("tag", schedule_name)

        created = 0
        for task_spec in task_types:
            task_type = task_spec.get("type", "batch_task") if isinstance(task_spec, dict) else task_spec
            for target in targets:
                task_data = {
                    "title": f"[batch:{schedule_name}] {task_type}",
                    "description": f"夜間バッチ {schedule_name} から自動生成",
                    "section": "engineering",
                    "tags": [tag, "batch", task_type],
                    "owner": target,
                    "priority": 5,
                }
                store.create(task_data)
                created += 1

        return created
    except Exception as exc:
        logger.error("dispatch_action failed: %s", exc)
        return 0


def _run_audit_action() -> None:
    """監査ハッシュチェーンを計算する。"""
    try:
        result = run_chain_hash()
        logger.info("audit.chain_hash completed: %s", result["hash"][:16])
    except Exception as exc:
        logger.error("audit_action failed: %s", exc)


def _run_schedule_agents(schedule: dict[str, Any]) -> int:
    """既存 schedules.yaml 形式（agents + task_template）を処理する。"""
    try:
        from ..taskboard.store import TaskStore
        store = TaskStore()
        agents = schedule.get("agents", [])
        task_template = schedule.get("task_template", schedule.get("name", "batch_task"))
        category = schedule.get("category", "engineering")
        name = schedule.get("name", "batch")

        created = 0
        for agent_id in agents:
            task_data = {
                "title": f"[batch:{name}] {task_template}",
                "description": f"夜間バッチ {name} から自動生成",
                "section": category,
                "tags": [name, "batch", task_template],
                "owner": agent_id,
                "priority": schedule.get("priority", 5),
            }
            store.create(task_data)
            created += 1
        return created
    except Exception as exc:
        logger.error("schedule_agents failed: %s", exc)
        return 0


def run_schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    """1 スケジュールを実行して結果を返す。"""
    name = schedule.get("name", "unknown")
    aid = new_audit_id()
    start = time.monotonic()
    success = 0
    total = 0

    log_event("info", "batch.started", "batch-scheduler", aid, data={"schedule": name})

    actions = schedule.get("actions", [])
    if not actions and schedule.get("agents"):
        # 既存 schedules.yaml 形式: agents フィールドをそのまま処理
        actions = [{"dispatch": {}}]

    for action in actions:
        total += 1
        try:
            if "dispatch" in action:
                if schedule.get("agents"):
                    created = _run_schedule_agents(schedule)
                else:
                    created = _run_dispatch_action(action, name)
                logger.info("batch.dispatch schedule=%s tasks_created=%d", name, created)
                success += 1
            elif "audit" in action:
                _run_audit_action()
                success += 1
            elif "cleaner" in action:
                logger.info("batch.cleaner schedule=%s (stub)", name)
                success += 1
        except Exception as exc:
            logger.error("batch.action_failed schedule=%s action=%s: %s", name, action, exc)

    duration_s = time.monotonic() - start
    log_event("info", "batch.completed", "batch-scheduler", aid,
              duration_ms=int(duration_s * 1000),
              data={"schedule": name, "success": success, "total": total})
    notify_batch_complete(name, success, total, duration_s)

    return {"name": name, "success": success, "total": total, "duration_s": round(duration_s, 2)}


def run_due_schedules(now: datetime | None = None) -> list[dict[str, Any]]:
    """現在時刻に該当するスケジュールをすべて実行する。"""
    if now is None:
        now = datetime.now(timezone.utc)

    schedules = _load_schedules()
    results = []

    for sched in schedules:
        cron = sched.get("cron", "")
        if _parse_cron(cron, now):
            logger.info("batch.triggered schedule=%s cron=%s", sched.get("name"), cron)
            result = run_schedule(sched)
            results.append(result)

    return results


def run_loop(poll_interval_s: float = 60.0) -> None:
    """毎分ポーリングして due なスケジュールを実行するメインループ。"""
    logger.info("batch_scheduler started poll_interval=%.0fs", poll_interval_s)
    while True:
        try:
            results = run_due_schedules()
            if results:
                logger.info("batch_scheduler ran %d schedules", len(results))
        except Exception as exc:
            logger.error("batch_scheduler loop error: %s", exc)
        time.sleep(poll_interval_s)

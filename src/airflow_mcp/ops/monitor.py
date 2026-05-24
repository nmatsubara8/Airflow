"""CLI 監視ダッシュボード（DDD-08 §5.1）。

airflow-mcp status コマンドの実装。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_GPU_SNAPSHOT = Path(os.environ.get("AIRFLOW_GPU_SNAPSHOT", ".airflow/runtime/gpu_state.json"))
_AUDIT_LOG_DIR = Path(".airflow/logs")
_TASKBOARD_DB = Path(os.environ.get("AIRFLOW_TASKBOARD_DB", ".airflow/taskboard.db"))
_EXECUTION_LOG_DIR = Path(os.environ.get("AIRFLOW_EXECUTION_LOG_DIR", ".airflow/logs/execution"))
_PARALLEL_CONFIG = Path(os.environ.get("AIRFLOW_PARALLEL_CONFIG", ".airflow/config/parallel.yaml"))


def _load_gpu_snapshot() -> dict[str, Any]:
    if _GPU_SNAPSHOT.exists():
        return json.loads(_GPU_SNAPSHOT.read_text())
    return {}


def _load_task_counts() -> dict[str, int]:
    """TaskStore を直接 import して各ステータスの件数を返す。"""
    try:
        from ..taskboard.store import TaskStore
        store = TaskStore()
        counts: dict[str, int] = {}
        for status in ("inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"):
            counts[status] = len(store.list_by_status(status))
        return counts
    except Exception as exc:
        logger.warning("monitor: failed to load task counts: %s", exc)
        return {}


def _load_section_counts(counts_by_status: dict[str, int]) -> dict[str, dict[str, int]]:
    """セクション別の in_progress / 合計を返す。"""
    try:
        from ..taskboard.store import TaskStore
        store = TaskStore()
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
    except Exception as exc:
        logger.warning("monitor: failed to load section counts: %s", exc)
        return {}


def _recent_events(n: int = 5) -> list[dict[str, Any]]:
    """直近 n 件の監査ログイベントを返す（ファイル全体を読まずに末尾 n 行だけ処理）。"""
    from collections import deque
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = _AUDIT_LOG_DIR / f"{today}.jsonl"
    if not log_file.exists():
        return []
    tail: deque[str] = deque(maxlen=n)
    try:
        with log_file.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    tail.append(line)
    except OSError:
        return []
    events = []
    for line in tail:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return list(reversed(events))


def _load_active_profile() -> str:
    if _PARALLEL_CONFIG.exists():
        cfg = yaml.safe_load(_PARALLEL_CONFIG.read_text())
        return cfg.get("mvp_profile", "unknown")
    return "unknown"


def _format_dashboard(
    gpu: dict[str, Any],
    task_counts: dict[str, int],
    section_counts: dict[str, dict[str, int]],
    recent: list[dict[str, Any]],
    degraded: bool,
    profile: str,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    mode = "DEGRADED" if degraded else "NORMAL"

    free_vram = gpu.get("free_vram_gb", "?")
    total_vram = gpu.get("total_vram_gb", 16.0)
    loaded = ", ".join(
        m.get("model_id", "?")
        for m in gpu.get("loaded_models", [])
    ) or "none"

    inbox = task_counts.get("inbox", 0)
    in_prog = task_counts.get("in_progress", 0)
    done = task_counts.get("done", 0)
    needs_human = task_counts.get("needs_human", 0)
    blocked = task_counts.get("blocked", 0)

    w = 66
    sep = "═" * w

    lines = [
        f"╔{sep}╗",
        f"║  Airflow Status  -  {now:<43}║",
        f"╠{sep}╣",
        f"║  Mode: {mode:<57}║",
        f"║  GPU: {free_vram}/{total_vram} GB | Profile: {profile} | Loaded: {loaded:<20}║",
        f"╠{sep}╣",
        f"║  Tasks{' ' * 59}║",
        f"║    inbox:       {inbox:<5}  in_progress: {in_prog:<5}  done(total): {done:<8}║",
        f"║    needs_human: {needs_human:<5}  blocked:     {blocked:<5}{' ' * 26}║",
        f"╠{sep}╣",
        f"║  Sections{' ' * 56}║",
    ]

    for sec, cnts in sorted(section_counts.items()):
        active = cnts.get("active", 0)
        total = cnts.get("total", 0)
        line = f"║    §{sec}: {active} active / {total} total"
        lines.append(f"{line:<{w + 2}}║")

    lines.append(f"╠{sep}╣")
    lines.append(f"║  Recent Events{' ' * 51}║")

    for ev in recent[:5]:
        ts = ev.get("timestamp", "")[-8:-1] if ev.get("timestamp") else "?"
        etype = ev.get("event_type", ev.get("event", "?"))[:30]
        line = f"║    {ts}  {etype}"
        lines.append(f"{line:<{w + 2}}║")

    if needs_human > 0:
        lines.append(f"╠{sep}╣")
        lines.append(f"║  ⚠ Alerts: {needs_human} needs_human pending{' ' * 41}║")

    lines.append(f"╚{sep}╝")
    return "\n".join(lines)


def print_status(degraded: bool = False) -> None:
    """ターミナルにダッシュボードを出力する。"""
    gpu = _load_gpu_snapshot()
    task_counts = _load_task_counts()
    section_counts = _load_section_counts(task_counts)
    recent = _recent_events()
    profile = _load_active_profile()
    print(_format_dashboard(gpu, task_counts, section_counts, recent, degraded, profile))


def get_status_dict() -> dict[str, Any]:
    """ダッシュボード情報を辞書で返す（Web API 向け）。"""
    gpu = _load_gpu_snapshot()
    task_counts = _load_task_counts()
    section_counts = _load_section_counts(task_counts)
    profile = _load_active_profile()
    return {
        "gpu": gpu,
        "tasks": task_counts,
        "sections": section_counts,
        "profile": profile,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

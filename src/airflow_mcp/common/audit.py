"""DDD-03 §5 に準拠した監査 ID 採番と構造化ログ出力。"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

import ulid


def new_audit_id() -> str:
    """ULID 形式の監査 ID を生成（DDD-03 §5.1）。"""
    return str(ulid.new())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_event(
    level: str,
    event: str,
    component: str,
    audit_id: str,
    *,
    parent_audit_id: str | None = None,
    task_id: str | None = None,
    agent_id: str | None = None,
    model_id: str | None = None,
    duration_ms: int | None = None,
    error: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    """DDD-03 §6.1 LogEvent スキーマ準拠の構造化ログを stderr に出力。"""
    entry: dict[str, Any] = {
        "schema_version": "1.0.0",
        "timestamp": now_iso(),
        "level": level,
        "event": event,
        "audit_id": audit_id,
        "component": component,
    }
    if parent_audit_id:
        entry["parent_audit_id"] = parent_audit_id
    if task_id:
        entry["task_id"] = task_id
    if agent_id:
        entry["agent_id"] = agent_id
    if model_id:
        entry["model_id"] = model_id
    if duration_ms is not None:
        entry["duration_ms"] = duration_ms
    if error:
        entry["error"] = error
    if data:
        entry["data"] = data

    print(json.dumps(entry, ensure_ascii=False), file=sys.stderr)


def record_audit(
    event: str,
    *,
    sink: list[dict[str, Any]],
    component: str = "taskboard-mcp",
    task_id: str | None = None,
    agent_id: str | None = None,
    parent_audit_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> str:
    """監査イベントを記録し audit_id を返す。

    sink（プロセス内ログリスト）への追記と stderr への構造化ログ出力を同時に行う。
    """
    aid = new_audit_id()
    entry: dict[str, Any] = {
        "audit_id": aid,
        "event": event,
        "component": component,
        "timestamp": now_iso(),
    }
    if task_id:
        entry["task_id"] = task_id
    if agent_id:
        entry["agent_id"] = agent_id
    if parent_audit_id:
        entry["parent_audit_id"] = parent_audit_id
    if data:
        entry["data"] = data
    sink.append(entry)
    log_event(
        "info", event, component, aid,
        parent_audit_id=parent_audit_id,
        task_id=task_id,
        agent_id=agent_id,
        data=data,
    )
    return aid

"""ExecutionLog 永続化（DDD-06 §5）。

.airflow/execution_logs/YYYY-MM.jsonl に JSONL 形式で追記。
KNN 推奨の学習データとして使用。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.audit import new_audit_id, now_iso

_DEFAULT_LOG_DIR = Path(".airflow/execution_logs")


def compute_auto_score(
    schema_valid: bool,
    retry_count: int,
    human_feedback: str | None = None,
) -> float:
    """DDD-06 §5.2 の auto_score 算出。

    base: 0.7（二重実行なしで成功）、0.0（失敗）
    補正: -0.3 if not schema_valid, -0.1 * retry_count
    人間フィードバック: +0.1 accepted / -0.2 modified / -0.5 rejected
    """
    if not schema_valid:
        base = 0.0
    else:
        base = 0.7

    score = base - (0.1 * retry_count)

    if human_feedback == "accepted":
        score += 0.1
    elif human_feedback == "modified":
        score -= 0.2
    elif human_feedback == "rejected":
        score -= 0.5

    return max(0.0, min(1.0, score))


def append_execution_log(
    task_id: str,
    agent_id: str,
    model_id: str,
    features_snapshot: dict[str, Any],
    schema_valid: bool,
    retry_count: int,
    latency_ms: int,
    exploration_type: str | None = None,
    log_dir: str | Path = _DEFAULT_LOG_DIR,
) -> dict[str, Any]:
    """ExecutionLog を JSONL に追記してレコードを返す。"""
    auto_score = compute_auto_score(schema_valid, retry_count)
    ts = now_iso()
    record: dict[str, Any] = {
        "schema_version": "1.0.0",
        "id": "execlog_" + new_audit_id()[:16].lower(),
        "task_id": task_id,
        "agent_id": agent_id,
        "model_id": model_id,
        "features_snapshot": features_snapshot,
        "schema_valid": schema_valid,
        "retry_count": retry_count,
        "latency_ms": latency_ms,
        "auto_score": auto_score,
        "success": schema_valid,
        "human_feedback": None,
        "exploration_type": exploration_type,
        "created_at": ts,
    }

    base = Path(log_dir)
    base.mkdir(parents=True, exist_ok=True)
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    log_path = base / f"{month}.jsonl"

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return record


def load_history(
    log_dir: str | Path = _DEFAULT_LOG_DIR,
    window: int = 200,
) -> list[dict[str, Any]]:
    """直近 window 件の ExecutionLog を返す（JSONL を新しい月から読む）。"""
    base = Path(log_dir)
    if not base.exists():
        return []

    records: list[dict[str, Any]] = []
    for jsonl_path in sorted(base.glob("*.jsonl"), reverse=True):
        if len(records) >= window:
            break
        try:
            lines = jsonl_path.read_text(encoding="utf-8").splitlines()
            for line in reversed(lines):
                if not line.strip():
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if len(records) >= window:
                    break
        except OSError:
            continue

    return records

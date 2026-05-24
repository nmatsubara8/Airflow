"""ExecutionLog 永続化（DDD-06 §5）。

.airflow/execution_logs/YYYY-MM.jsonl に JSONL 形式で追記。
KNN 推奨の学習データとして使用。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.audit import new_audit_id, now_iso
from ..common.thresholds import (
    AUTO_SCORE_BASE_SUCCESS,
    AUTO_SCORE_FEEDBACK_ACCEPTED,
    AUTO_SCORE_FEEDBACK_MODIFIED,
    AUTO_SCORE_FEEDBACK_REJECTED,
    AUTO_SCORE_RETRY_PENALTY,
)

logger = logging.getLogger(__name__)

_DEFAULT_LOG_DIR = Path(".airflow/execution_logs")


def _log_id() -> str:
    """スキーマ準拠の ID を生成: ^log_[a-f0-9]{8,}$（DDD-01 execution_log.schema.json）。"""
    raw = new_audit_id().encode()
    return "log_" + hashlib.sha256(raw).hexdigest()[:16]


def compute_auto_score(
    schema_valid: bool,
    retry_count: int,
    human_feedback: str | None = None,
) -> float:
    """DDD-06 §5.2 の auto_score 算出。

    base: 0.7（二重実行なしで成功）、0.0（失敗）
    補正: -0.1 * retry_count
    人間フィードバック: +0.1 accepted / -0.2 modified / -0.5 rejected
    """
    base = 0.0 if not schema_valid else AUTO_SCORE_BASE_SUCCESS
    score = base - (AUTO_SCORE_RETRY_PENALTY * retry_count)

    if human_feedback == "accepted":
        score += AUTO_SCORE_FEEDBACK_ACCEPTED
    elif human_feedback == "modified":
        score -= AUTO_SCORE_FEEDBACK_MODIFIED
    elif human_feedback == "rejected":
        score -= AUTO_SCORE_FEEDBACK_REJECTED

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
        "id": _log_id(),          # ^log_[a-f0-9]{8,}$ に準拠
        "timestamp": ts,          # execution_log.schema.json 必須フィールド
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
    }

    base = Path(log_dir)
    base.mkdir(parents=True, exist_ok=True)
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    log_path = base / f"{month}.jsonl"

    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as exc:
        logger.error("execution_log: failed to append log for task=%s: %s", task_id, exc)
        raise

    return record


def update_feedback(
    task_id: str,
    human_feedback: str,
    log_dir: str | Path = _DEFAULT_LOG_DIR,
) -> dict[str, Any] | None:
    """task_id に対応する最新 ExecutionLog に human_feedback を反映（DDD-06 §5.3）。

    JSONL ファイルをインプレース書き換えし、更新済みレコードを返す。
    対応レコードが見つからない場合は None を返す。
    """
    base = Path(log_dir)
    if not base.exists():
        return None

    _MAX_FEEDBACK_FILE_BYTES = 50 * 1024 * 1024  # 50 MB per JSONL file

    for jsonl_path in sorted(base.glob("*.jsonl"), reverse=True):
        try:
            if jsonl_path.stat().st_size > _MAX_FEEDBACK_FILE_BYTES:
                logger.warning("execution_log: skipping oversized file %s", jsonl_path.name)
                continue
            lines = jsonl_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue

        updated_lines: list[str] = []
        found: dict[str, Any] | None = None

        # 最新レコード（末尾優先）を 1 件だけ更新
        for line in reversed(lines):
            if not line.strip():
                updated_lines.insert(0, line)
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                updated_lines.insert(0, line)
                continue

            if found is None and record.get("task_id") == task_id:
                record["human_feedback"] = human_feedback
                record["auto_score"] = compute_auto_score(
                    record.get("schema_valid", record.get("success", False)),
                    record.get("retry_count", 0),
                    human_feedback,
                )
                found = record
                updated_lines.insert(0, json.dumps(record, ensure_ascii=False))
            else:
                updated_lines.insert(0, line)

        if found is not None:
            tmp = jsonl_path.with_suffix(".jsonl.new")
            try:
                tmp.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
                os.replace(tmp, jsonl_path)
            except OSError as exc:
                logger.error("execution_log: atomic write failed for %s: %s", jsonl_path.name, exc)
                tmp.unlink(missing_ok=True)
                raise
            return found

    return None


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

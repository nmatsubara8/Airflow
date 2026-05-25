"""監査ログの改ざん耐性（DDD-08 §4.5）。

日次ログファイルの SHA256 ハッシュチェーンを構築・検証する。
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_LOGS_DIR = Path(".airflow/logs")
_INTEGRITY_DIR = _LOGS_DIR / "integrity"

# YYYY-MM-DD のみ許可（パストラバーサル防止）
_DATE_STR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_date_str(date_str: str) -> None:
    """date_str が YYYY-MM-DD 形式であることを検証する。"""
    if not _DATE_STR_RE.match(date_str):
        raise ValueError(f"Invalid date_str: {date_str!r} (must be YYYY-MM-DD)")


def _date_log_path(date_str: str) -> Path:
    _validate_date_str(date_str)
    return _LOGS_DIR / f"{date_str}.jsonl"


def _hash_path(date_str: str) -> Path:
    _validate_date_str(date_str)
    return _INTEGRITY_DIR / f"{date_str}.hash"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    if path.exists():
        h.update(path.read_bytes())
    return h.hexdigest()


def compute_chain_hash(date_str: str, prev_hash: str = "") -> str:
    """date の jsonl と前日ハッシュを組み合わせてチェーンハッシュを計算する。"""
    log_hash = _sha256_file(_date_log_path(date_str))
    combined = log_hash + prev_hash
    return hashlib.sha256(combined.encode()).hexdigest()


def run_chain_hash(date_str: str | None = None) -> dict[str, str]:
    """指定日（デフォルト: 今日）のハッシュを計算してファイルに記録する。

    Returns:
        {"date": ..., "hash": ..., "prev_hash": ...}
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    _INTEGRITY_DIR.mkdir(parents=True, exist_ok=True)

    # 前日ハッシュを読み込む
    prev_hash = ""
    hash_files = sorted(_INTEGRITY_DIR.glob("*.hash"))
    if hash_files:
        try:
            last = json.loads(hash_files[-1].read_text())
            prev_hash = last.get("hash", "")
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("audit: corrupted hash file %s, using empty prev_hash: %s",
                           hash_files[-1].name, exc)

    chain_hash = compute_chain_hash(date_str, prev_hash)

    record = {
        "date": date_str,
        "hash": chain_hash,
        "prev_hash": prev_hash,
        "computed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    _hash_path(date_str).write_text(json.dumps(record, ensure_ascii=False, indent=2))
    logger.info("audit.chain_hash date=%s hash=%s...", date_str, chain_hash[:16])
    return record


def verify_chain(since: str | None = None) -> list[dict[str, str]]:
    """since 以降のハッシュチェーンを検証し、破断箇所を返す。

    Returns:
        破断レコードのリスト。空なら整合。
    """
    if since is not None:
        _validate_date_str(since)

    _INTEGRITY_DIR.mkdir(parents=True, exist_ok=True)
    hash_files = sorted(_INTEGRITY_DIR.glob("*.hash"))

    if since:
        hash_files = [f for f in hash_files if f.stem >= since]

    failures: list[dict[str, str]] = []
    # since が指定された場合、最初のレコードに保存された prev_hash を起点にする
    prev_hash = ""
    if hash_files:
        try:
            first_record = json.loads(hash_files[0].read_text())
            prev_hash = first_record.get("prev_hash", "")
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("audit: corrupted first hash file %s: %s", hash_files[0].name, exc)

    for hf in hash_files:
        try:
            record = json.loads(hf.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("audit: unreadable hash file %s: %s", hf.name, exc)
            failures.append({"date": hf.stem, "expected_hash": "N/A",
                             "actual_hash": f"CORRUPT: {exc}"})
            continue
        date_str = record.get("date") or hf.stem
        expected = compute_chain_hash(date_str, prev_hash)

        actual_hash = record.get("hash") or ""
        if actual_hash != expected:
            failures.append({
                "date": date_str,
                "expected_hash": expected,
                "actual_hash": actual_hash,
            })
            logger.error(
                "audit.chain_broken date=%s expected=%s... got=%s...",
                date_str, expected[:16], actual_hash[:16],
            )

        prev_hash = record.get("hash", "")

    if not failures:
        logger.info("audit.chain_ok files_checked=%d", len(hash_files))

    return failures

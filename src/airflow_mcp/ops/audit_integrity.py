"""監査ログの改ざん耐性（DDD-08 §4.5）。

日次ログファイルの SHA256 ハッシュチェーンを構築・検証する。
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_LOGS_DIR = Path(".airflow/logs")
_INTEGRITY_DIR = _LOGS_DIR / "integrity"


def _date_log_path(date_str: str) -> Path:
    return _LOGS_DIR / f"{date_str}.jsonl"


def _hash_path(date_str: str) -> Path:
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
        last = json.loads(hash_files[-1].read_text())
        prev_hash = last.get("hash", "")

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
    _INTEGRITY_DIR.mkdir(parents=True, exist_ok=True)
    hash_files = sorted(_INTEGRITY_DIR.glob("*.hash"))

    if since:
        hash_files = [f for f in hash_files if f.stem >= since]

    failures: list[dict[str, str]] = []
    # since が指定された場合、最初のレコードに保存された prev_hash を起点にする
    prev_hash = ""
    if hash_files:
        first_record = json.loads(hash_files[0].read_text())
        prev_hash = first_record.get("prev_hash", "")

    for hf in hash_files:
        record = json.loads(hf.read_text())
        date_str = record["date"]
        expected = compute_chain_hash(date_str, prev_hash)

        if record.get("hash") != expected:
            failures.append({
                "date": date_str,
                "expected_hash": expected,
                "actual_hash": record.get("hash", ""),
            })
            logger.error(
                "audit.chain_broken date=%s expected=%s... got=%s...",
                date_str, expected[:16], record.get("hash", "")[:16],
            )

        prev_hash = record.get("hash", "")

    if not failures:
        logger.info("audit.chain_ok files_checked=%d", len(hash_files))

    return failures

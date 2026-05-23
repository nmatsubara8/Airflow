"""Vault ファイル監視 → タスク自動生成（DDD-04 §5）。

ポーリング方式（inotify は Phase 4 で追加予定）。
新規 .md を検出 → frontmatter 解析 → content_hash dedup → TaskStore.create
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import yaml

from ..common.audit import log_event, new_audit_id
from ..taskboard.store import TaskStore
from .parser import frontmatter_to_task_fields, parse_markdown

logger = logging.getLogger(__name__)

# 既処理ファイルの content_hash セット（dedup, DDD-04 §5.3）
_seen_hashes: set[str] = set()


def load_config(config_path: str = ".airflow/config/ingester.yaml") -> dict[str, Any]:
    p = Path(config_path)
    if p.exists():
        return yaml.safe_load(p.read_text()) or {}
    return {}


def run_once(store: TaskStore, config: dict[str, Any]) -> int:
    """監視パスをスキャンして未処理 .md をタスク化する。作成数を返す。"""
    watch_paths: list[str] = [
        p.replace("${VAULT_ROOT}", os.environ.get("VAULT_ROOT", ""))
        for p in (config.get("watch", {}).get("paths") or [])
    ]
    if not watch_paths:
        vault_root = os.environ.get("VAULT_ROOT", "")
        if vault_root:
            watch_paths = [vault_root]

    settle_ms: int = config.get("stability", {}).get("settle_ms", 500)
    max_bytes: int = config.get("stability", {}).get("max_file_size_bytes", 524288)
    glob_pattern: str = config.get("watch", {}).get("glob", "**/*.md")
    exclude: list[str] = config.get("watch", {}).get("exclude", [])

    created = 0
    for watch_path in watch_paths:
        base = Path(watch_path)
        if not base.exists():
            continue
        for md_path in base.glob(glob_pattern):
            if _is_excluded(md_path, base, exclude):
                continue
            if md_path.stat().st_size > max_bytes:
                logger.warning("skipping oversized file: %s", md_path)
                continue
            if not _is_stable(md_path, settle_ms):
                continue

            try:
                parsed = parse_markdown(md_path)
            except Exception as exc:
                logger.error("parse error %s: %s", md_path, exc)
                continue

            content_hash = parsed["content_hash"]
            if content_hash in _seen_hashes:
                continue

            # dedup: アクティブなタスクに同一 content_hash がないか確認（DDD-04 §5.3）
            # done は _seen_hashes がプロセス内重複を防ぐ。needs_human/blocked も検索対象に含める。
            existing = (
                store.list_by_status("inbox")
                + store.list_by_status("in_progress")
                + store.list_by_status("needs_human")
                + store.list_by_status("blocked")
            )
            if any(t.get("content_hash") == content_hash for t in existing):
                _seen_hashes.add(content_hash)
                continue

            fields = frontmatter_to_task_fields(parsed)
            task = store.create({
                **fields,
                "owner": None,
            })
            _seen_hashes.add(content_hash)
            aid = new_audit_id()
            log_event("info", "task.ingested", "ingester", aid,
                      task_id=task["id"],
                      data={"source_path": str(md_path), "title": task["title"]})
            logger.info("ingested: %s → %s", md_path.name, task["id"])
            created += 1

    return created


def _is_stable(path: Path, settle_ms: int) -> bool:
    """ファイルサイズが settle_ms の間変わっていないか確認（DDD-04 §5.1）。"""
    try:
        mtime = path.stat().st_mtime
        now = time.time()
        return (now - mtime) * 1000 >= settle_ms
    except OSError:
        return False


def _is_excluded(path: Path, base: Path, exclude_patterns: list[str]) -> bool:
    rel = str(path.relative_to(base))
    for pattern in exclude_patterns:
        # 簡易グロブマッチ
        from fnmatch import fnmatch
        if fnmatch(rel, pattern.lstrip("/")):
            return True
    return False


def run_loop(store: TaskStore, config: dict[str, Any], poll_interval_ms: int | None = None) -> None:
    """メインループ: poll_interval_ms ごとに run_once を呼ぶ。"""
    interval = (poll_interval_ms or config.get("stability", {}).get("poll_interval_ms", 5000)) / 1000
    logger.info("ingester started, poll_interval=%.1fs", interval)
    while True:
        try:
            n = run_once(store, config)
            if n > 0:
                logger.info("ingested %d tasks", n)
        except Exception as exc:
            logger.error("ingester error: %s", exc)
        time.sleep(interval)


def _cli() -> None:
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    store = TaskStore()
    config = load_config()
    run_loop(store, config)

"""タスクボード清掃エージェント（DDD-08 §7 / cleaner.yaml）。

実行内容:
  1. done/cancelled タスクを月別 _archive/ へ移動（archive_done_after_days 経過後）
  2. _archive/ 内の古いタスクを削除（purge_archive_after_days 経過後）
  3. execution.jsonl を gzip 圧縮・古い圧縮済みログを削除
  4. dry_run=true のときは実際の削除・移動を行わず結果を返す

使用方法:
    from airflow_mcp.cleaner.main import run_once, load_config
    result = run_once()
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.io import safe_yaml_load

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(os.environ.get("AIRFLOW_CLEANER_CONFIG", ".airflow/config/cleaner.yaml"))
_TASKS_DIR = Path(os.environ.get("AIRFLOW_TASKBOARD_DIR", ".airflow/tasks"))
_EXEC_LOG = Path(os.environ.get("AIRFLOW_EXECUTION_LOG", ".airflow/state/execution.jsonl"))
_ARCHIVE_SUBDIR = "_archive"


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or _CONFIG_PATH
    raw = safe_yaml_load(path)
    return raw.get("cleaner", {}) if isinstance(raw, dict) else {}


def _days_old(path: Path) -> float:
    """ファイルの最終更新からの経過日数を返す。"""
    mtime = path.stat().st_mtime
    return (time.time() - mtime) / 86400


def _archive_old_tasks(config: dict[str, Any], tasks_dir: Path, dry_run: bool) -> int:
    """done/cancelled タスクを _archive/<YYYY-MM>/ へ移動する。移動件数を返す。"""
    done_days: int = int(config.get("tasks", {}).get("archive_done_after_days", 30))
    failed_days: int = int(config.get("tasks", {}).get("archive_failed_after_days", 7))
    archive_root = tasks_dir / _ARCHIVE_SUBDIR
    moved = 0

    for status, threshold_days in (("done", done_days), ("cancelled", failed_days)):
        status_dir = tasks_dir / status
        if not status_dir.exists():
            continue
        # done は YYYY-MM サブディレクトリ構造を持つ
        task_files = list(status_dir.rglob("task_*.json"))
        for task_file in task_files:
            # _archive 配下はスキップ
            if _ARCHIVE_SUBDIR in task_file.parts:
                continue
            try:
                if _days_old(task_file) < threshold_days:
                    continue
                month = datetime.now(timezone.utc).strftime("%Y-%m")
                dest_dir = archive_root / status / month
                if not dry_run:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(task_file), str(dest_dir / task_file.name))
                logger.info("cleaner.archive %s → %s/%s", task_file.name, status, month)
                moved += 1
            except OSError as exc:
                logger.warning("cleaner.archive_failed %s: %s", task_file, exc)

    return moved


def _purge_archived_tasks(config: dict[str, Any], tasks_dir: Path, dry_run: bool) -> int:
    """_archive/ 内の古いタスクを削除する。削除件数を返す。"""
    purge_days: int = int(config.get("tasks", {}).get("purge_archive_after_days", 365))
    archive_root = tasks_dir / _ARCHIVE_SUBDIR
    purged = 0

    if not archive_root.exists():
        return 0

    for task_file in archive_root.rglob("task_*.json"):
        try:
            if _days_old(task_file) < purge_days:
                continue
            if not dry_run:
                task_file.unlink()
            logger.info("cleaner.purge %s", task_file.name)
            purged += 1
        except OSError as exc:
            logger.warning("cleaner.purge_failed %s: %s", task_file, exc)

    return purged


def _rotate_execution_log(config: dict[str, Any], exec_log: Path, dry_run: bool) -> int:
    """execution.jsonl の古い行を gzip アーカイブし、解放バイト数を返す。"""
    log_cfg = config.get("execution_logs", {})
    archive_days: int = int(log_cfg.get("archive_after_days", 14))
    purge_days: int = int(log_cfg.get("purge_after_days", 90))
    freed = 0

    # 既存の圧縮ファイルをパージ
    log_dir = exec_log.parent
    if log_dir.exists():
        for gz_file in log_dir.glob(f"{exec_log.stem}-*.jsonl.gz"):
            try:
                if _days_old(gz_file) >= purge_days:
                    size = gz_file.stat().st_size
                    if not dry_run:
                        gz_file.unlink()
                    freed += size
                    logger.info("cleaner.log_purge %s (%d bytes)", gz_file.name, size)
            except OSError as exc:
                logger.warning("cleaner.log_purge_failed %s: %s", gz_file, exc)

    # execution.jsonl を分割してアーカイブ
    if not exec_log.exists():
        return freed

    cutoff = time.time() - archive_days * 86400
    try:
        lines = exec_log.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        logger.warning("cleaner.log_read_failed: %s", exc)
        return freed

    old_lines, keep_lines = [], []
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            ts = entry.get("ts") or entry.get("timestamp") or ""
            # ISO 8601 タイムスタンプを epoch に変換
            try:
                from datetime import datetime as _dt
                epoch = _dt.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except (ValueError, AttributeError):
                epoch = cutoff + 1  # パース不能なら保持
            if epoch < cutoff:
                old_lines.append(line)
            else:
                keep_lines.append(line)
        except json.JSONDecodeError:
            keep_lines.append(line)  # 不正行は保持

    if not old_lines:
        return freed

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    gz_path = log_dir / f"{exec_log.stem}-{stamp}.jsonl.gz"
    old_bytes = sum(len(line.encode()) + 1 for line in old_lines)

    if not dry_run:
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            f.write("\n".join(old_lines) + "\n")
        exec_log.write_text("\n".join(keep_lines) + "\n", encoding="utf-8")

    freed += old_bytes
    logger.info(
        "cleaner.log_archive %s → %s (%d lines, %d bytes)",
        exec_log.name, gz_path.name, len(old_lines), old_bytes,
    )
    return freed


def run_once(
    config: dict[str, Any] | None = None,
    tasks_dir: Path | None = None,
    exec_log: Path | None = None,
) -> dict[str, Any]:
    """清掃を 1 回実行し、結果サマリーを返す。

    Args:
        config: cleaner セクションの設定 dict。None のとき cleaner.yaml から読み込む。
        tasks_dir: タスクボードのベースディレクトリ。None のとき環境変数から取得。
        exec_log: execution.jsonl のパス。None のとき環境変数から取得。

    Returns:
        {"archived": int, "purged": int, "log_bytes_freed": int, "dry_run": bool}
    """
    if config is None:
        config = load_config()
    if tasks_dir is None:
        tasks_dir = _TASKS_DIR
    if exec_log is None:
        exec_log = _EXEC_LOG

    dry_run: bool = bool(config.get("dry_run", False))

    archived = _archive_old_tasks(config, tasks_dir, dry_run)
    purged = _purge_archived_tasks(config, tasks_dir, dry_run)
    freed = _rotate_execution_log(config, exec_log, dry_run)

    result = {
        "archived": archived,
        "purged": purged,
        "log_bytes_freed": freed,
        "dry_run": dry_run,
    }
    logger.info("cleaner.done %s", result)
    return result

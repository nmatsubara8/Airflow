"""Git+JSON ファイルベースのタスクストア（DDD-04 §2〜3）。

ストレージ構造:
  .airflow/tasks/
    inbox/          task_<ulid>.json
    in_progress/    task_<ulid>.json
    done/<YYYY-MM>/ task_<ulid>.json
    needs_human/    task_<ulid>.json
    blocked/        task_<ulid>.json
    cancelled/      task_<ulid>.json
    _corrupt/       隔離

ロック戦略 (DDD-04 §3):
  L2: <task_id>.json.lock  — プロセス間排他
  L3: lock.version         — 楽観的バージョン検出
  原子書き込み: .new に書いてから rename(2)
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.errors import TaskAlreadyClaimed, TaskInvalidTransition, TaskLockConflict, TaskNotFound

logger = logging.getLogger(__name__)

# task_id は必ずこのパターンに一致する必要がある（パストラバーサル防止）
_TASK_ID_RE = re.compile(r"^task_[a-z0-9]{16,32}$")
_STATUS_ALLOWLIST = frozenset(
    ("inbox", "in_progress", "done", "needs_human", "blocked", "cancelled", "_corrupt")
)


def _validate_task_id(task_id: str) -> None:
    """task_id がパス安全なパターンに合致するか検証する。"""
    if not _TASK_ID_RE.match(task_id):
        raise TaskNotFound(task_id)  # 不正 ID は存在しないものとして扱う

_TRANSITIONS: dict[str, set[str]] = {
    "inbox":       {"in_progress", "cancelled"},
    "in_progress": {"done", "needs_human", "blocked", "cancelled"},
    "needs_human": {"in_progress", "cancelled"},
    "blocked":     {"in_progress", "cancelled"},
    "done":        set(),
    "cancelled":   set(),
}


def _canonical(task: dict[str, Any]) -> str:
    """etag 計算用正準化 JSON（lock フィールドを除外）。"""
    filtered = {k: v for k, v in task.items() if k != "lock"}
    return json.dumps(filtered, sort_keys=True, ensure_ascii=False)


def _etag(task: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_canonical(task).encode()).hexdigest()


_DEFAULT_TASKS_DIR = ".airflow/tasks"


class TaskStore:
    def __init__(self, base_dir: str | Path | None = None) -> None:
        if base_dir is None:
            base_dir = os.environ.get("AIRFLOW_TASKBOARD_DIR", _DEFAULT_TASKS_DIR)
        self.base = Path(base_dir)
        for sub in ("inbox", "in_progress", "needs_human", "blocked", "cancelled", "_corrupt"):
            (self.base / sub).mkdir(parents=True, exist_ok=True)

    # ── パス解決 ───────────────────────────────────────────
    def _task_path(self, task_id: str, status: str) -> Path:
        _validate_task_id(task_id)
        if status not in _STATUS_ALLOWLIST:
            raise ValueError(f"Invalid status: {status}")
        if status == "done":
            month = datetime.now(timezone.utc).strftime("%Y-%m")
            path = self.base / "done" / month
            path.mkdir(parents=True, exist_ok=True)
            return path / f"{task_id}.json"
        return self.base / status / f"{task_id}.json"

    def _find_task_path(self, task_id: str) -> Path | None:
        _validate_task_id(task_id)
        for status in ("inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"):
            direct = self.base / status / f"{task_id}.json"
            if direct.exists():
                return direct
            # done の月別サブディレクトリを検索
            if status == "done":
                for monthly in (self.base / "done").glob("*/"):
                    candidate = monthly / f"{task_id}.json"
                    if candidate.exists():
                        return candidate
        return None

    @contextmanager
    def _lock(self, task_id: str) -> Generator[None, None, None]:
        """L2: .lock ファイルによるプロセス間排他（DDD-04 §3.2）。"""
        _validate_task_id(task_id)
        lock_path = self.base / f"{task_id}.json.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "w") as fh:
            fh.write(f"{os.getpid()} {now_iso()}")
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _write_atomic(self, path: Path, task: dict[str, Any]) -> None:
        """.new ファイルに書いてから atomic rename（DDD-04 §3.2.f）。"""
        tmp = path.with_suffix(".json.new")
        tmp.write_text(json.dumps(task, ensure_ascii=False, indent=2))
        os.replace(tmp, path)

    # ── CRUD ──────────────────────────────────────────────
    def create(self, task: dict[str, Any]) -> dict[str, Any]:
        ts = now_iso()
        task_id = task.get("id") or ("task_" + new_audit_id()[:16].lower())
        task = {
            "schema_version": "1.0.0",
            "id": task_id,
            **task,
            "status": "inbox",
            "created_at": ts,
            "updated_at": ts,
            "handoff_notes": task.get("handoff_notes", []),
            "lock": {"version": 1, "etag": "sha256:" + "0" * 64},
        }
        task["lock"]["etag"] = _etag(task)
        path = self._task_path(task_id, "inbox")
        self._write_atomic(path, task)
        return task

    def get(self, task_id: str) -> dict[str, Any]:
        path = self._find_task_path(task_id)
        if path is None:
            raise TaskNotFound(task_id)
        return json.loads(path.read_text())

    def list_by_status(self, status: str, limit: int = 200) -> list[dict[str, Any]]:
        if status == "done":
            paths = list((self.base / "done").rglob("task_*.json"))
        else:
            paths = list((self.base / status).glob("task_*.json"))
        tasks = []
        for p in sorted(paths)[:limit]:
            try:
                tasks.append(json.loads(p.read_text()))
            except (json.JSONDecodeError, OSError):
                pass
        return tasks

    def list_all(self) -> list[dict[str, Any]]:
        """全ステータスのタスクを返す（dedup 確認等に使用）。"""
        result: list[dict[str, Any]] = []
        for status in _STATUS_ALLOWLIST - {"_corrupt"}:
            result.extend(self.list_by_status(status))
        return result

    def update(
        self, task_id: str, expected_version: int, patch: dict[str, Any]
    ) -> dict[str, Any]:
        with self._lock(task_id):
            path = self._find_task_path(task_id)
            if path is None:
                raise TaskNotFound(task_id)
            task = json.loads(path.read_text())
            if task["lock"]["version"] != expected_version:
                raise TaskLockConflict(task_id, task["lock"]["version"])
            for k, v in patch.items():
                if k not in ("id", "created_at", "lock", "schema_version"):
                    task[k] = v
            task["updated_at"] = now_iso()
            task["lock"]["version"] += 1
            task["lock"]["etag"] = _etag(task)
            self._write_atomic(path, task)
        return task

    def transition(self, task_id: str, to_status: str, reason: str | None = None) -> dict[str, Any]:
        with self._lock(task_id):
            path = self._find_task_path(task_id)
            if path is None:
                raise TaskNotFound(task_id)
            task = json.loads(path.read_text())
            from_status = task["status"]
            if to_status not in _TRANSITIONS.get(from_status, set()):
                raise TaskInvalidTransition(from_status, to_status)
            task["status"] = to_status
            task["updated_at"] = now_iso()
            task["lock"]["version"] += 1
            task["lock"]["etag"] = _etag(task)
            new_path = self._task_path(task_id, to_status)
            self._write_atomic(new_path, task)
            # 元ファイルを削除（別ディレクトリへ移動の場合）
            if path != new_path:
                path.unlink(missing_ok=True)
        log_event(
            "info", "task.transitioned", "taskboard", new_audit_id(),
            task_id=task_id,
            data={"from": from_status, "to": to_status, "reason": reason},
        )
        logger.info("store.transition task=%s %s→%s reason=%s", task_id, from_status, to_status, reason)
        return task

    def claim(self, task_id: str, agent_id: str) -> dict[str, Any]:
        with self._lock(task_id):
            path = self._find_task_path(task_id)
            if path is None:
                raise TaskNotFound(task_id)
            task = json.loads(path.read_text())
            if task["owner"] not in (None, agent_id):
                raise TaskAlreadyClaimed(task_id, task["owner"])
            if task["status"] != "inbox":
                raise TaskInvalidTransition(task["status"], "in_progress")
            task["owner"] = agent_id
            task["status"] = "in_progress"
            task["updated_at"] = now_iso()
            task["lock"]["version"] += 1
            task["lock"]["etag"] = _etag(task)
            new_path = self._task_path(task_id, "in_progress")
            self._write_atomic(new_path, task)
            if path != new_path:
                path.unlink(missing_ok=True)
        return task

    def assign(self, task_id: str, new_owner_id: str) -> dict[str, Any]:
        """オーナーをアトミックに変更する（TOCTOU 対策: M-6）。

        ロック内でステータスを再確認し、in_progress なら TaskInvalidTransition を raise する。
        """
        with self._lock(task_id):
            path = self._find_task_path(task_id)
            if path is None:
                raise TaskNotFound(task_id)
            task = json.loads(path.read_text())
            if task["status"] == "in_progress":
                raise TaskInvalidTransition("in_progress", "reassign")
            task["owner"] = new_owner_id
            task["updated_at"] = now_iso()
            task["lock"]["version"] += 1
            task["lock"]["etag"] = _etag(task)
            self._write_atomic(path, task)
        return task

    def add_handoff_note(self, task_id: str, content: str, author: str) -> dict[str, Any]:
        with self._lock(task_id):
            path = self._find_task_path(task_id)
            if path is None:
                raise TaskNotFound(task_id)
            task = json.loads(path.read_text())
            note = {
                "id": "hn_" + new_audit_id()[:12].lower(),
                "body": content,
                "author": {"type": "agent", "id": author},
                "timestamp": now_iso(),
            }
            task.setdefault("handoff_notes", []).append(note)
            task["updated_at"] = now_iso()
            task["lock"]["version"] += 1
            task["lock"]["etag"] = _etag(task)
            self._write_atomic(path, task)
        return task

    def quarantine(self, task_id: str, reason: str) -> None:
        """破損タスクを _corrupt/ に隔離（DDD-04 §10）。"""
        path = self._find_task_path(task_id)
        if path:
            dest = self.base / "_corrupt" / path.name
            path.rename(dest)

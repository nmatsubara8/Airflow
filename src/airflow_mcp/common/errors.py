"""DDD-03 §2 に準拠したエラークラスと MCP レスポンス変換。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import mcp.types as types


@dataclass
class AirflowError(Exception):
    code: str
    message: str
    error_class: str = "recoverable"  # recoverable | escalate | fatal
    retry_after_ms: int | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def to_mcp_result(self) -> list[types.TextContent]:
        payload: dict[str, Any] = {
            "error": {
                "class": self.error_class,
                "code": self.code,
                "message": self.message,
            }
        }
        if self.retry_after_ms is not None:
            payload["error"]["retry_after_ms"] = self.retry_after_ms
        if self.data:
            payload["data"] = self.data
        return [types.TextContent(type="text", text=json.dumps(payload, ensure_ascii=False))]


# ── 認証エラー ──────────────────────────────────────
class AuthTokenMissing(AirflowError):
    def __init__(self) -> None:
        super().__init__(
            code="AUTH_TOKEN_MISSING",
            message="AIRFLOW_MCP_TOKEN が設定されていません",
            error_class="fatal",
        )


class AuthTokenInvalid(AirflowError):
    def __init__(self) -> None:
        super().__init__(
            code="AUTH_TOKEN_INVALID",
            message="トークンが無効です",
            error_class="fatal",
        )


class AuthScopeInsufficient(AirflowError):
    def __init__(self, required: str) -> None:
        super().__init__(
            code="AUTH_SCOPE_INSUFFICIENT",
            message=f"スコープ '{required}' が必要です",
            error_class="escalate",
        )


class AuthRateLimited(AirflowError):
    def __init__(self, retry_after_ms: int = 1000) -> None:
        super().__init__(
            code="AUTH_RATE_LIMITED",
            message="レート制限超過",
            error_class="recoverable",
            retry_after_ms=retry_after_ms,
        )


# ── タスクエラー ──────────────────────────────────────
class TaskNotFound(AirflowError):
    def __init__(self, task_id: str) -> None:
        super().__init__(
            code="TASK_NOT_FOUND",
            message=f"タスク '{task_id}' が見つかりません",
            error_class="escalate",
        )


class TaskLockConflict(AirflowError):
    def __init__(self, task_id: str, current_version: int) -> None:
        super().__init__(
            code="TASK_LOCK_CONFLICT",
            message=f"タスク '{task_id}' は別プロセスが更新しました（現行 version={current_version}）",
            error_class="recoverable",
            retry_after_ms=200,
            data={"current_version": current_version},
        )


class TaskInvalidTransition(AirflowError):
    def __init__(self, from_status: str, to_status: str) -> None:
        super().__init__(
            code="TASK_INVALID_TRANSITION",
            message=f"ステータス遷移 '{from_status}' → '{to_status}' は許可されていません",
            error_class="escalate",
        )


class TaskAlreadyClaimed(AirflowError):
    def __init__(self, task_id: str, owner: str) -> None:
        super().__init__(
            code="TASK_ALREADY_CLAIMED",
            message=f"タスク '{task_id}' は '{owner}' が保有しています",
            error_class="escalate",
        )


# ── モデルエラー ──────────────────────────────────────
class ModelNotFound(AirflowError):
    def __init__(self, model_id: str) -> None:
        super().__init__(
            code="MODEL_NOT_FOUND",
            message=f"モデル '{model_id}' が registry に存在しません",
            error_class="escalate",
        )


class ModelStructuredOutputInvalid(AirflowError):
    def __init__(self, schema_errors: list[str]) -> None:
        super().__init__(
            code="MODEL_STRUCTURED_OUTPUT_INVALID",
            message="モデル出力が JSON Schema に不適合",
            error_class="escalate",
            data={"schema_errors": schema_errors},
        )


# ── ノートエラー ──────────────────────────────────────
class NoteNotFound(AirflowError):
    def __init__(self, note_id: str) -> None:
        super().__init__(
            code="NOTE_NOT_FOUND",
            message=f"ノート '{note_id}' が見つかりません",
            error_class="escalate",
        )


# ── GPU エラー ──────────────────────────────────────
class GpuBusy(AirflowError):
    def __init__(self, retry_after_ms: int = 5000) -> None:
        super().__init__(
            code="GPU_BUSY",
            message="VRAM 不足のためモデルをロードできません",
            error_class="recoverable",
            retry_after_ms=retry_after_ms,
        )

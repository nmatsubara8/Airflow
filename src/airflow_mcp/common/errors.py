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


# ── タスクエラー（追加）──────────────────────────────
class TaskScopeViolation(AirflowError):
    def __init__(self, task_id: str, detail: str = "") -> None:
        super().__init__(
            code="TASK_SCOPE_VIOLATION",
            message=f"タスク '{task_id}' でスコープ外の判断が要求されました。{detail}".rstrip(),
            error_class="escalate",
        )


class TaskCorrupt(AirflowError):
    def __init__(self, task_id: str) -> None:
        super().__init__(
            code="TASK_CORRUPT",
            message=f"タスク '{task_id}' のファイルが破損しています（_corrupt/ へ隔離済み）",
            error_class="fatal",
        )


# ── モデルエラー（追加）──────────────────────────────
class ModelExecutionFailed(AirflowError):
    def __init__(self, model_id: str, reason: str = "") -> None:
        super().__init__(
            code="MODEL_EXECUTION_FAILED",
            message=f"モデル '{model_id}' の推論実行に失敗しました。{reason}".rstrip(),
            error_class="recoverable",
            retry_after_ms=5000,
        )


class ModelLowConfidence(AirflowError):
    def __init__(self, model_id: str, score: float) -> None:
        super().__init__(
            code="MODEL_LOW_CONFIDENCE",
            message=f"モデル '{model_id}' の推奨信頼度 {score:.2f} が閾値未満です",
            error_class="escalate",
            data={"score": score},
        )


class ModelRerouteExhausted(AirflowError):
    def __init__(self, model_id: str) -> None:
        super().__init__(
            code="MODEL_REROUTE_EXHAUSTED",
            message=f"fallback_chain を使い切っても auto_score が閾値を下回りました（起点: {model_id}）",
            error_class="escalate",
        )


# ── ノートエラー（追加）──────────────────────────────
class NoteParseError(AirflowError):
    def __init__(self, path: str, detail: str = "") -> None:
        super().__init__(
            code="NOTE_PARSE_ERROR",
            message=f"ノート '{path}' の YAML フロントマター解析に失敗しました。{detail}".rstrip(),
            error_class="recoverable",
        )


class NoteConfidentialAccessDenied(AirflowError):
    def __init__(self, note_id: str) -> None:
        super().__init__(
            code="NOTE_CONFIDENTIAL_ACCESS_DENIED",
            message=f"ノート '{note_id}' は confidential です。アクセス権がありません",
            error_class="fatal",
        )


# ── GPU エラー ──────────────────────────────────────
class SchedulerProfileNotFound(AirflowError):
    def __init__(self, profile_name: str, known: list[str]) -> None:
        super().__init__(
            code="UNKNOWN_PROFILE",
            message=f"'{profile_name}' は未定義。既知: {known}",
            error_class="escalate",
        )


class GpuBusy(AirflowError):
    def __init__(self, retry_after_ms: int = 5000) -> None:
        super().__init__(
            code="GPU_BUSY",
            message="VRAM 不足のためモデルをロードできません",
            error_class="recoverable",
            retry_after_ms=retry_after_ms,
        )


class GpuInsufficientVram(AirflowError):
    def __init__(self, required_gb: float, free_gb: float) -> None:
        super().__init__(
            code="GPU_INSUFFICIENT_VRAM",
            message=f"VRAM 不足: 必要 {required_gb:.1f} GB、空き {free_gb:.1f} GB（LRU 退避で解消可能性あり）",
            error_class="recoverable",
            retry_after_ms=5000,
            data={"required_gb": required_gb, "free_gb": free_gb},
        )


class GpuReserveTimeout(AirflowError):
    def __init__(self, model_id: str, timeout_ms: int) -> None:
        super().__init__(
            code="GPU_RESERVE_TIMEOUT",
            message=f"モデル '{model_id}' のリザーブ待機が {timeout_ms} ms でタイムアウトしました",
            error_class="recoverable",
            retry_after_ms=10000,
        )


class GpuOom(AirflowError):
    def __init__(self, model_id: str) -> None:
        super().__init__(
            code="GPU_OOM",
            message=f"モデル '{model_id}' の実行中に GPU OOM が発生しました",
            error_class="fatal",
        )


# ── 取り込みエラー ──────────────────────────────────
class IngestPathMissing(AirflowError):
    def __init__(self, path: str) -> None:
        super().__init__(
            code="INGEST_PATH_MISSING",
            message=f"監視対象 Vault パス '{path}' が消失しました（ingester 停止）",
            error_class="fatal",
        )


# ── スキーマエラー ──────────────────────────────────
class SchemaValidationFailed(AirflowError):
    def __init__(self, detail: str) -> None:
        super().__init__(
            code="SCHEMA_VALIDATION_FAILED",
            message=f"入出力スキーマ違反: {detail}",
            error_class="fatal",
        )


class SchemaVersionUnsupported(AirflowError):
    def __init__(self, version: str) -> None:
        super().__init__(
            code="SCHEMA_VERSION_UNSUPPORTED",
            message=f"schema_version '{version}' は未対応です",
            error_class="fatal",
        )


# ── 内部エラー ──────────────────────────────────────
class AgentNotFound(AirflowError):
    def __init__(self, agent_id: str) -> None:
        super().__init__(
            code="AGENT_NOT_FOUND",
            message=f"エージェント '{agent_id}' が見つかりません",
            error_class="escalate",
        )


class InternalTimeout(AirflowError):
    def __init__(self, operation: str, timeout_ms: int) -> None:
        super().__init__(
            code="INTERNAL_TIMEOUT",
            message=f"内部処理 '{operation}' が {timeout_ms} ms でタイムアウトしました",
            error_class="recoverable",
            retry_after_ms=timeout_ms,
        )


class InternalUnexpected(AirflowError):
    def __init__(self, detail: str = "") -> None:
        super().__init__(
            code="INTERNAL_UNEXPECTED",
            message=f"想定外のサーバーエラーが発生しました。{detail}".rstrip(),
            error_class="fatal",
        )

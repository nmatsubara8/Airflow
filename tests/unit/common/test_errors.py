"""common/errors.py の単体テスト（DDD-03 §2）。"""
from __future__ import annotations

import json

from airflow_mcp.common.errors import (
    AirflowError,
    AuthRateLimited,
    AuthScopeInsufficient,
    AuthTokenInvalid,
    AuthTokenMissing,
    GpuBusy,
    ModelNotFound,
    ModelStructuredOutputInvalid,
    NoteNotFound,
    TaskAlreadyClaimed,
    TaskInvalidTransition,
    TaskLockConflict,
    TaskNotFound,
)


# ── AirflowError 基底クラス ────────────────────────────

def test_airflow_error_to_mcp_result_has_error_key():
    err = AirflowError(code="TEST_ERR", message="テスト", error_class="recoverable")
    result = err.to_mcp_result()
    assert len(result) == 1
    payload = json.loads(result[0].text)
    assert "error" in payload


def test_airflow_error_to_mcp_result_includes_code_and_message():
    err = AirflowError(code="MY_CODE", message="詳細メッセージ", error_class="escalate")
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["error"]["code"] == "MY_CODE"
    assert payload["error"]["message"] == "詳細メッセージ"
    assert payload["error"]["class"] == "escalate"


def test_airflow_error_retry_after_ms_included_when_set():
    err = AirflowError(code="X", message="m", error_class="recoverable", retry_after_ms=500)
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["error"]["retry_after_ms"] == 500


def test_airflow_error_retry_after_ms_absent_when_none():
    err = AirflowError(code="X", message="m", error_class="escalate")
    payload = json.loads(err.to_mcp_result()[0].text)
    assert "retry_after_ms" not in payload["error"]


def test_airflow_error_data_included_when_set():
    err = AirflowError(code="X", message="m", error_class="escalate",
                       data={"key": "value"})
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["data"] == {"key": "value"}


def test_airflow_error_data_absent_when_empty():
    err = AirflowError(code="X", message="m", error_class="recoverable")
    payload = json.loads(err.to_mcp_result()[0].text)
    assert "data" not in payload


# ── 認証エラー ────────────────────────────────────────

def test_auth_token_missing_code():
    err = AuthTokenMissing()
    assert err.code == "AUTH_TOKEN_MISSING"
    assert err.error_class == "fatal"


def test_auth_token_invalid_code():
    err = AuthTokenInvalid()
    assert err.code == "AUTH_TOKEN_INVALID"
    assert err.error_class == "fatal"


def test_auth_scope_insufficient_includes_scope_in_message():
    err = AuthScopeInsufficient("tasks:write")
    assert "tasks:write" in err.message
    assert err.error_class == "escalate"


def test_auth_rate_limited_has_retry_after():
    err = AuthRateLimited(retry_after_ms=2000)
    assert err.retry_after_ms == 2000
    assert err.error_class == "recoverable"
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["error"]["retry_after_ms"] == 2000


# ── タスクエラー ──────────────────────────────────────

def test_task_not_found_includes_task_id():
    err = TaskNotFound("task_abc123")
    assert "task_abc123" in err.message
    assert err.error_class == "escalate"


def test_task_lock_conflict_has_retry_and_version():
    err = TaskLockConflict("task_xyz", 5)
    assert err.retry_after_ms == 200
    assert err.error_class == "recoverable"
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["data"]["current_version"] == 5


def test_task_invalid_transition_includes_statuses():
    err = TaskInvalidTransition("inbox", "done")
    assert "inbox" in err.message
    assert "done" in err.message


def test_task_already_claimed_includes_owner():
    err = TaskAlreadyClaimed("task_001", "agent_docs")
    assert "agent_docs" in err.message


# ── モデルエラー ──────────────────────────────────────

def test_model_not_found_includes_model_id():
    err = ModelNotFound("model_qwen25_32b_q3")
    assert "model_qwen25_32b_q3" in err.message
    assert err.error_class == "escalate"


def test_model_structured_output_invalid_includes_errors():
    errs = ["field 'summary' required", "type mismatch"]
    err = ModelStructuredOutputInvalid(errs)
    payload = json.loads(err.to_mcp_result()[0].text)
    assert payload["data"]["schema_errors"] == errs


# ── その他のエラー ────────────────────────────────────

def test_note_not_found_includes_note_id():
    err = NoteNotFound("note_abc")
    assert "note_abc" in err.message


def test_gpu_busy_has_retry_and_recoverable():
    err = GpuBusy(retry_after_ms=10000)
    assert err.retry_after_ms == 10000
    assert err.error_class == "recoverable"


# ── 追加エラークラス（DDD-03 定義分）────────────────────────

def test_task_scope_violation_mcp_result():
    from airflow_mcp.common.errors import TaskScopeViolation
    e = TaskScopeViolation("task_abc1234567890123")
    r = e.to_mcp_result()
    assert r[0].type == "text"
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "TASK_SCOPE_VIOLATION"
    assert d["error"]["class"] == "escalate"

def test_task_corrupt_mcp_result():
    from airflow_mcp.common.errors import TaskCorrupt
    e = TaskCorrupt("task_abc1234567890123")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "TASK_CORRUPT"
    assert d["error"]["class"] == "fatal"

def test_model_execution_failed_mcp_result():
    from airflow_mcp.common.errors import ModelExecutionFailed
    e = ModelExecutionFailed("model_qwen25_32b", "timeout")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "MODEL_EXECUTION_FAILED"
    assert d["error"]["class"] == "recoverable"
    assert "retry_after_ms" in d["error"]

def test_model_low_confidence_mcp_result():
    from airflow_mcp.common.errors import ModelLowConfidence
    import pytest
    e = ModelLowConfidence("model_qwen25_14b", 0.62)
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "MODEL_LOW_CONFIDENCE"
    assert d["data"]["score"] == pytest.approx(0.62)

def test_model_reroute_exhausted_mcp_result():
    from airflow_mcp.common.errors import ModelRerouteExhausted
    e = ModelRerouteExhausted("model_qwen25_32b")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "MODEL_REROUTE_EXHAUSTED"

def test_note_parse_error_mcp_result():
    from airflow_mcp.common.errors import NoteParseError
    e = NoteParseError("Inbox/note.md", "invalid YAML")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "NOTE_PARSE_ERROR"
    assert d["error"]["class"] == "recoverable"

def test_note_confidential_access_denied_mcp_result():
    from airflow_mcp.common.errors import NoteConfidentialAccessDenied
    e = NoteConfidentialAccessDenied("note_xyz")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "NOTE_CONFIDENTIAL_ACCESS_DENIED"
    assert d["error"]["class"] == "fatal"

def test_gpu_insufficient_vram_mcp_result():
    from airflow_mcp.common.errors import GpuInsufficientVram
    import pytest
    e = GpuInsufficientVram(24.0, 8.5)
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "GPU_INSUFFICIENT_VRAM"
    assert d["data"]["required_gb"] == pytest.approx(24.0)
    assert d["data"]["free_gb"] == pytest.approx(8.5)

def test_gpu_reserve_timeout_mcp_result():
    from airflow_mcp.common.errors import GpuReserveTimeout
    e = GpuReserveTimeout("model_qwen25_32b", 60000)
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "GPU_RESERVE_TIMEOUT"

def test_gpu_oom_mcp_result():
    from airflow_mcp.common.errors import GpuOom
    e = GpuOom("model_qwen25_32b")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "GPU_OOM"
    assert d["error"]["class"] == "fatal"

def test_ingest_path_missing_mcp_result():
    from airflow_mcp.common.errors import IngestPathMissing
    e = IngestPathMissing("/mnt/vault")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "INGEST_PATH_MISSING"
    assert d["error"]["class"] == "fatal"

def test_schema_validation_failed_mcp_result():
    from airflow_mcp.common.errors import SchemaValidationFailed
    e = SchemaValidationFailed("missing required field: title")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "SCHEMA_VALIDATION_FAILED"

def test_schema_version_unsupported_mcp_result():
    from airflow_mcp.common.errors import SchemaVersionUnsupported
    e = SchemaVersionUnsupported("2.0.0")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "SCHEMA_VERSION_UNSUPPORTED"

def test_internal_timeout_mcp_result():
    from airflow_mcp.common.errors import InternalTimeout
    e = InternalTimeout("execution_log.write", 5000)
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "INTERNAL_TIMEOUT"
    assert d["error"]["class"] == "recoverable"

def test_internal_unexpected_mcp_result():
    from airflow_mcp.common.errors import InternalUnexpected
    e = InternalUnexpected("something went wrong")
    r = e.to_mcp_result()
    import json; d = json.loads(r[0].text)
    assert d["error"]["code"] == "INTERNAL_UNEXPECTED"
    assert d["error"]["class"] == "fatal"

"""execution_log.py の単体テスト（DDD-06 §5）。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from airflow_mcp.orchestrator.execution_log import (
    append_execution_log,
    compute_auto_score,
    load_history,
    update_feedback,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
EXEC_LOG_SCHEMA = json.loads(
    (PROJECT_ROOT / "schemas" / "execution_log.schema.json").read_text()
)


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "logs"


def _append(log_dir: Path, task_id: str = "task_001", **kw) -> dict:
    defaults: dict = {
        "schema_valid": True,
        "retry_count": 0,
        "latency_ms": 500,
    }
    defaults.update(kw)
    return append_execution_log(
        task_id=task_id,
        agent_id="agent_docs",
        model_id="model_qwen25_14b_q4",
        features_snapshot={"task_type": "summarize", "language": "ja",
                            "required_accuracy": "medium", "latency_sensitivity": "batch"},
        log_dir=log_dir,
        **defaults,
    )


# ── append_execution_log ──────────────────────────────────

def test_append_creates_jsonl_file(log_dir: Path):
    _append(log_dir)
    files = list(log_dir.glob("*.jsonl"))
    assert len(files) == 1


def test_append_record_passes_schema_validation(log_dir: Path):
    record = _append(log_dir)
    validator = Draft202012Validator(EXEC_LOG_SCHEMA)
    errors = list(validator.iter_errors(record))
    assert errors == [], f"スキーマ不適合:\n" + "\n".join(str(e) for e in errors)


def test_append_id_matches_schema_pattern(log_dir: Path):
    import re
    record = _append(log_dir)
    assert re.match(r"^log_[a-f0-9]{8,}$", record["id"]), \
        f"ID パターン不一致: {record['id']}"


def test_append_has_timestamp_not_created_at(log_dir: Path):
    record = _append(log_dir)
    assert "timestamp" in record
    assert "created_at" not in record


def test_append_multiple_records_all_in_same_file(log_dir: Path):
    _append(log_dir, task_id="t1")
    _append(log_dir, task_id="t2")
    files = list(log_dir.glob("*.jsonl"))
    assert len(files) == 1
    lines = [l for l in files[0].read_text().splitlines() if l.strip()]
    assert len(lines) == 2


def test_append_auto_score_computed_from_schema_valid_and_retry(log_dir: Path):
    rec_ok = _append(log_dir, schema_valid=True, retry_count=0)
    rec_retry = _append(log_dir, schema_valid=True, retry_count=2)
    assert rec_ok["auto_score"] == pytest.approx(0.7)
    assert rec_retry["auto_score"] == pytest.approx(0.5)


def test_append_schema_invalid_gives_zero_auto_score(log_dir: Path):
    record = append_execution_log(
        task_id="t1", agent_id="a", model_id="m",
        features_snapshot={}, schema_valid=False, retry_count=0, latency_ms=100,
        log_dir=log_dir,
    )
    assert record["auto_score"] == pytest.approx(0.0)


# ── load_history ──────────────────────────────────────────

def test_load_history_returns_records(log_dir: Path):
    _append(log_dir, task_id="t1")
    _append(log_dir, task_id="t2")
    history = load_history(log_dir=log_dir)
    assert len(history) == 2


def test_load_history_empty_dir_returns_empty_list(tmp_path: Path):
    history = load_history(log_dir=tmp_path / "nonexistent")
    assert history == []


def test_load_history_respects_window(log_dir: Path):
    for i in range(10):
        _append(log_dir, task_id=f"t{i}")
    history = load_history(log_dir=log_dir, window=3)
    assert len(history) == 3


# ── update_feedback ───────────────────────────────────────

def test_update_feedback_updates_human_feedback_field(log_dir: Path):
    _append(log_dir, task_id="task_fb")
    updated = update_feedback("task_fb", "accepted", log_dir=log_dir)
    assert updated is not None
    assert updated["human_feedback"] == "accepted"


def test_update_feedback_recomputes_auto_score(log_dir: Path):
    _append(log_dir, task_id="task_fb", schema_valid=True, retry_count=0)
    # base=0.7, accepted +0.1 → 0.8
    updated = update_feedback("task_fb", "accepted", log_dir=log_dir)
    assert updated["auto_score"] == pytest.approx(0.8)


def test_update_feedback_rejected_reduces_auto_score(log_dir: Path):
    _append(log_dir, task_id="task_fb", schema_valid=True, retry_count=0)
    # base=0.7, rejected -0.5 → 0.2
    updated = update_feedback("task_fb", "rejected", log_dir=log_dir)
    assert updated["auto_score"] == pytest.approx(0.2)


def test_update_feedback_persists_to_file(log_dir: Path):
    _append(log_dir, task_id="task_persist")
    update_feedback("task_persist", "modified", log_dir=log_dir)
    # ファイルを再読み込みして確認
    history = load_history(log_dir=log_dir)
    record = next(r for r in history if r["task_id"] == "task_persist")
    assert record["human_feedback"] == "modified"


def test_update_feedback_returns_none_for_unknown_task(log_dir: Path):
    result = update_feedback("nonexistent_task", "accepted", log_dir=log_dir)
    assert result is None


def test_update_feedback_reflected_in_next_load_history(log_dir: Path):
    """フィードバック後の load_history が更新済みスコアを返すこと（KNN リアルタイム反映）。"""
    _append(log_dir, task_id="task_knn", schema_valid=True, retry_count=0)
    update_feedback("task_knn", "accepted", log_dir=log_dir)
    history = load_history(log_dir=log_dir)
    record = next(r for r in history if r["task_id"] == "task_knn")
    assert record["auto_score"] == pytest.approx(0.8)


# ── エッジケース追加 ──────────────────────────────────

def test_load_history_skips_unreadable_file(log_dir: Path):
    """OSError が発生する .jsonl は無視して残りを返すこと（lines 173-174）。"""
    # 正常ファイルを先に書く
    _append(log_dir, task_id="task_ok", schema_valid=True, retry_count=0)
    # 読み取り不可ファイルを追加
    bad = log_dir / "bad.jsonl"
    bad.write_bytes(b"")
    bad.chmod(0o000)
    try:
        from airflow_mcp.orchestrator.execution_log import load_history
        history = load_history(log_dir=log_dir)
        assert any(r["task_id"] == "task_ok" for r in history)
    finally:
        bad.chmod(0o644)


def test_load_history_skips_empty_lines(log_dir: Path):
    """空行を含む .jsonl でも正しくレコードを返すこと（lines 165-166）。"""
    import json as _json
    from airflow_mcp.orchestrator.execution_log import load_history
    import datetime, uuid
    log_dir.mkdir(parents=True, exist_ok=True)
    jsonl = log_dir / f"{datetime.date.today()}.jsonl"
    record = {"id": "log_abc", "task_id": "task_empty_line", "timestamp": "2026-05-23T00:00:00Z",
              "schema_valid": True, "retry_count": 0, "auto_score": 0.5}
    # 空行を挟んで書き込む
    jsonl.write_text("\n" + _json.dumps(record) + "\n\n", encoding="utf-8")
    history = load_history(log_dir=log_dir)
    assert any(r["task_id"] == "task_empty_line" for r in history)


def test_update_feedback_skips_unreadable_file(log_dir: Path):
    """OSError の .jsonl は無視して None を返すこと（update_feedback lines 113-114）。"""
    log_dir.mkdir(parents=True, exist_ok=True)
    bad = log_dir / "bad.jsonl"
    bad.write_bytes(b"")
    bad.chmod(0o000)
    try:
        from airflow_mcp.orchestrator.execution_log import update_feedback
        result = update_feedback("any_task", "accepted", log_dir=log_dir)
        assert result is None
    finally:
        bad.chmod(0o644)


def test_update_feedback_skips_corrupt_json_line(log_dir: Path):
    """破損 JSON 行は無視して正常レコードを見つけること（lines 126-128）。"""
    import json as _json
    from airflow_mcp.orchestrator.execution_log import update_feedback
    import datetime
    log_dir.mkdir(parents=True, exist_ok=True)
    jsonl = log_dir / f"{datetime.date.today()}.jsonl"
    record = {"id": "log_x", "task_id": "task_corrupt_line",
              "timestamp": "2026-05-23T00:00:00Z", "schema_valid": True,
              "retry_count": 0, "auto_score": 0.5}
    jsonl.write_text("{ bad json }\n" + _json.dumps(record) + "\n", encoding="utf-8")
    result = update_feedback("task_corrupt_line", "accepted", log_dir=log_dir)
    assert result is not None
    assert result["human_feedback"] == "accepted"


def test_update_feedback_oserror_on_read_is_skipped(log_dir: Path):
    """read_text が OSError を raise したファイルはスキップすること（lines 113-114）。
    chmod は root で効かないため read_text をモックする。"""
    from unittest.mock import patch
    import datetime
    log_dir.mkdir(parents=True, exist_ok=True)
    bad = log_dir / "2020-01.jsonl"
    bad.write_text("")   # ファイルは存在する

    with patch.object(Path, "read_text", side_effect=OSError("permission denied")):
        result = update_feedback("any_task", "accepted", log_dir=log_dir)
    assert result is None


def test_update_feedback_preserves_empty_lines(log_dir: Path):
    """JSONL に空行が含まれていても更新が正常動作すること（lines 122-123）。"""
    import json as _json
    import datetime
    log_dir.mkdir(parents=True, exist_ok=True)
    jsonl = log_dir / f"{datetime.date.today().strftime('%Y-%m')}.jsonl"
    record = {"id": "log_y", "task_id": "task_empty",
              "timestamp": "2026-05-23T00:00:00Z", "schema_valid": True,
              "retry_count": 0, "auto_score": 0.7, "human_feedback": None}
    # 空行を前後に挟む
    jsonl.write_text("\n" + _json.dumps(record) + "\n\n", encoding="utf-8")
    result = update_feedback("task_empty", "accepted", log_dir=log_dir)
    assert result is not None
    assert result["human_feedback"] == "accepted"


def test_load_history_breaks_between_files_when_window_full(tmp_path: Path):
    """window が最初のファイルで満たされたとき次ファイルを読まないこと（line 161）。"""
    import json as _json
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    # 新しいファイル（ソート順で先に来る）に window 分のレコードを書く
    newer = log_dir / "2026-05.jsonl"
    older = log_dir / "2026-04.jsonl"
    for i in range(5):
        newer.open("a").write(
            _json.dumps({"id": f"log_{i:04d}", "task_id": f"t{i}"}) + "\n"
        )
    older.write_text(_json.dumps({"id": "log_old", "task_id": "t_old"}) + "\n")

    history = load_history(log_dir=log_dir, window=5)
    assert len(history) == 5
    assert all(r["task_id"] != "t_old" for r in history)


def test_load_history_skips_corrupt_json_in_file(tmp_path: Path):
    """JSONL 内の破損行をスキップして他レコードを返すこと（lines 169-170）。"""
    import json as _json
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    jsonl = log_dir / "2026-05.jsonl"
    good = {"id": "log_good", "task_id": "task_good"}
    jsonl.write_text("{ invalid json\n" + _json.dumps(good) + "\n")
    history = load_history(log_dir=log_dir)
    assert any(r["task_id"] == "task_good" for r in history)


def test_load_history_oserror_on_read_is_skipped(tmp_path: Path):
    """read_text が OSError の .jsonl はスキップすること（lines 173-174）。"""
    from unittest.mock import patch
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    bad = log_dir / "2026-05.jsonl"
    bad.write_text("")

    with patch.object(Path, "read_text", side_effect=OSError("unreadable")):
        history = load_history(log_dir=log_dir)
    assert history == []


def test_update_feedback_preserves_other_task_records(log_dir: Path):
    """更新対象でないレコードも else ブランチで保持されること（line 140）。"""
    _append(log_dir, task_id="task_a")
    _append(log_dir, task_id="task_b")
    result = update_feedback("task_a", "accepted", log_dir=log_dir)
    assert result is not None
    # task_b のレコードも残っている
    history = load_history(log_dir=log_dir)
    assert any(r["task_id"] == "task_b" for r in history)

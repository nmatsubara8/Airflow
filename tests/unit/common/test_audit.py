"""common/audit.py の単体テスト（DDD-03 §5）。"""
from __future__ import annotations

import json
import re

from airflow_mcp.common.audit import log_event, new_audit_id, now_iso, record_audit


# ── new_audit_id ──────────────────────────────────────────

def test_new_audit_id_returns_string():
    assert isinstance(new_audit_id(), str)


def test_new_audit_id_is_non_empty():
    assert len(new_audit_id()) > 0


def test_new_audit_id_unique():
    ids = {new_audit_id() for _ in range(100)}
    assert len(ids) == 100


def test_new_audit_id_ulid_format():
    """ULID は 26 文字の Crockford Base32 文字列。"""
    aid = new_audit_id()
    assert re.match(r'^[0-9A-Z]{26}$', aid), f"不正なULID形式: {aid}"


# ── now_iso ───────────────────────────────────────────────

def test_now_iso_ends_with_z():
    ts = now_iso()
    assert ts.endswith("Z"), f"UTC ISO8601 は 'Z' で終わること: {ts}"


def test_now_iso_parseable():
    from datetime import datetime
    ts = now_iso()
    # 末尾 Z を +00:00 に変換してパース
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert dt.tzinfo is not None


def test_now_iso_format():
    ts = now_iso()
    assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', ts), f"不正な形式: {ts}"


# ── log_event ─────────────────────────────────────────────

def test_log_event_outputs_json_to_stderr(capsys):
    aid = new_audit_id()
    log_event("info", "test.event", "test-component", aid)
    captured = capsys.readouterr()
    entry = json.loads(captured.err.strip())
    assert entry["event"] == "test.event"
    assert entry["audit_id"] == aid
    assert entry["component"] == "test-component"
    assert entry["level"] == "info"


def test_log_event_has_required_schema_fields(capsys):
    log_event("info", "test.schema", "comp", new_audit_id())
    entry = json.loads(capsys.readouterr().err.strip())
    for field in ("schema_version", "timestamp", "level", "event", "audit_id", "component"):
        assert field in entry, f"必須フィールド '{field}' がない"


def test_log_event_optional_fields_included_when_set(capsys):
    aid = new_audit_id()
    log_event("info", "ev", "comp", aid,
              task_id="task_abc", agent_id="agent_x", data={"k": "v"})
    entry = json.loads(capsys.readouterr().err.strip())
    assert entry["task_id"] == "task_abc"
    assert entry["agent_id"] == "agent_x"
    assert entry["data"] == {"k": "v"}


def test_log_event_optional_fields_absent_when_none(capsys):
    log_event("info", "ev", "comp", new_audit_id())
    entry = json.loads(capsys.readouterr().err.strip())
    assert "task_id" not in entry
    assert "agent_id" not in entry
    assert "data" not in entry


def test_log_event_parent_audit_id(capsys):
    parent = new_audit_id()
    child = new_audit_id()
    log_event("info", "ev", "comp", child, parent_audit_id=parent)
    entry = json.loads(capsys.readouterr().err.strip())
    assert entry["parent_audit_id"] == parent


# ── record_audit ──────────────────────────────────────────

def test_record_audit_appends_to_sink(capsys):
    sink: list = []
    aid = record_audit("task.created", sink=sink, task_id="task_001")
    assert len(sink) == 1
    assert sink[0]["event"] == "task.created"
    assert sink[0]["task_id"] == "task_001"
    assert sink[0]["audit_id"] == aid


def test_record_audit_returns_audit_id(capsys):
    sink: list = []
    aid = record_audit("ev", sink=sink)
    assert isinstance(aid, str)
    assert len(aid) > 0


def test_record_audit_also_writes_stderr(capsys):
    sink: list = []
    record_audit("ev.check", sink=sink, component="mycomp")
    entry = json.loads(capsys.readouterr().err.strip())
    assert entry["event"] == "ev.check"
    assert entry["component"] == "mycomp"


def test_record_audit_parent_propagated(capsys):
    sink: list = []
    parent = new_audit_id()
    record_audit("child.ev", sink=sink, parent_audit_id=parent)
    assert sink[0]["parent_audit_id"] == parent

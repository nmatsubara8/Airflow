"""ops/monitor.py の単体テスト（DDD-08 §5.1）。"""
from __future__ import annotations

import json


from airflow_mcp.ops.monitor import _format_dashboard, get_status_dict


_SAMPLE_GPU = {
    "total_vram_gb": 16.0,
    "free_vram_gb": 7.0,
    "loaded_models": [
        {"model_id": "model_14b"},
        {"model_id": "model_bge"},
    ],
}

_SAMPLE_TASKS = {
    "inbox": 5,
    "in_progress": 2,
    "done": 30,
    "needs_human": 1,
    "blocked": 0,
}

_SAMPLE_SECTIONS = {
    "engineering": {"active": 2, "total": 35},
    "content": {"active": 0, "total": 4},
}


def test_format_dashboard_contains_mode():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "NORMAL" in output


def test_format_dashboard_degraded_mode():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], True, "single-14b"
    )
    assert "DEGRADED" in output


def test_format_dashboard_shows_vram():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "7.0" in output
    assert "16.0" in output


def test_format_dashboard_shows_task_counts():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "5" in output   # inbox
    assert "30" in output  # done


def test_format_dashboard_shows_profile():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "single-14b" in output


def test_format_dashboard_shows_loaded_models():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "model_14b" in output


def test_format_dashboard_shows_needs_human_alert():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "needs_human" in output.lower() or "Alert" in output


def test_format_dashboard_shows_section():
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, [], False, "single-14b"
    )
    assert "engineering" in output


def test_format_dashboard_recent_events():
    events = [
        {"event_type": "task.done", "timestamp": "2026-05-23T14:55:00Z"},
    ]
    output = _format_dashboard(
        _SAMPLE_GPU, _SAMPLE_TASKS, _SAMPLE_SECTIONS, events, False, "single-14b"
    )
    assert "task.done" in output


def test_get_status_dict_returns_required_keys(monkeypatch):
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_gpu_snapshot", lambda: _SAMPLE_GPU)
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_task_counts", lambda: _SAMPLE_TASKS)
    monkeypatch.setattr(
        "airflow_mcp.ops.monitor._load_section_counts", lambda _: _SAMPLE_SECTIONS
    )
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_active_profile", lambda: "single-14b")

    status = get_status_dict()
    assert "gpu" in status
    assert "tasks" in status
    assert "sections" in status
    assert "profile" in status
    assert "timestamp" in status


# ── _load_gpu_snapshot ────────────────────────────────

def test_load_gpu_snapshot_returns_empty_when_no_file(monkeypatch):
    from pathlib import Path
    import airflow_mcp.ops.monitor as mon
    monkeypatch.setattr(mon, "_GPU_SNAPSHOT", Path("/nonexistent/gpu_state.json"))
    from airflow_mcp.ops.monitor import _load_gpu_snapshot
    assert _load_gpu_snapshot() == {}


def test_load_gpu_snapshot_reads_file(tmp_path, monkeypatch):
    import airflow_mcp.ops.monitor as mon
    snap = tmp_path / "gpu_state.json"
    snap.write_text(json.dumps({"free_vram_gb": 8.0, "total_vram_gb": 16.0}))
    monkeypatch.setattr(mon, "_GPU_SNAPSHOT", snap)
    from airflow_mcp.ops.monitor import _load_gpu_snapshot
    result = _load_gpu_snapshot()
    assert result["free_vram_gb"] == 8.0


# ── _load_task_counts ─────────────────────────────────

def test_load_task_counts_returns_counts(tmp_path):
    from unittest.mock import patch
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    store.create({"title": "t1", "description": "", "tags": [], "priority": 5, "owner": None})
    with patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.ops.monitor import _load_task_counts
        counts = _load_task_counts()
    assert counts["inbox"] == 1
    assert counts["done"] == 0


def test_load_task_counts_returns_empty_on_exception():
    from unittest.mock import patch
    with patch("airflow_mcp.taskboard.store.TaskStore", side_effect=RuntimeError("db error")):
        from airflow_mcp.ops.monitor import _load_task_counts
        assert _load_task_counts() == {}


# ── _load_section_counts ──────────────────────────────

def test_load_section_counts_returns_section_data(tmp_path):
    from unittest.mock import patch
    from airflow_mcp.taskboard.store import TaskStore
    store = TaskStore(base_dir=tmp_path)
    t = store.create({
        "title": "eng task", "description": "", "tags": [],
        "priority": 5, "owner": None, "section": "engineering",
    })
    store.claim(t["id"], "agent_docs")  # → in_progress
    with patch("airflow_mcp.taskboard.store.TaskStore", return_value=store):
        from airflow_mcp.ops.monitor import _load_section_counts
        sections = _load_section_counts({})
    assert "engineering" in sections
    assert sections["engineering"]["active"] == 1


def test_load_section_counts_returns_empty_on_exception():
    from unittest.mock import patch
    with patch("airflow_mcp.taskboard.store.TaskStore", side_effect=RuntimeError("err")):
        from airflow_mcp.ops.monitor import _load_section_counts
        assert _load_section_counts({}) == {}


# ── _recent_events ────────────────────────────────────

def test_recent_events_returns_empty_when_no_log_file(monkeypatch):
    from pathlib import Path
    import airflow_mcp.ops.monitor as mon
    monkeypatch.setattr(mon, "_AUDIT_LOG_DIR", Path("/nonexistent/logs"))
    from airflow_mcp.ops.monitor import _recent_events
    assert _recent_events() == []


def test_recent_events_reads_jsonl_file(tmp_path, monkeypatch):
    import airflow_mcp.ops.monitor as mon
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = tmp_path / f"{today}.jsonl"
    log_file.write_text(
        '{"event_type": "task.done", "timestamp": "2026-05-23T14:55:00Z"}\n'
        '{"event_type": "task.ingested", "timestamp": "2026-05-23T14:56:00Z"}\n'
    )
    monkeypatch.setattr(mon, "_AUDIT_LOG_DIR", tmp_path)
    from airflow_mcp.ops.monitor import _recent_events
    events = _recent_events(n=5)
    assert len(events) == 2
    assert events[0]["event_type"] == "task.ingested"  # reversed


def test_recent_events_skips_invalid_json_lines(tmp_path, monkeypatch):
    import airflow_mcp.ops.monitor as mon
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = tmp_path / f"{today}.jsonl"
    log_file.write_text(
        '{"event_type": "task.done"}\n'
        'NOT VALID JSON\n'
        '{"event_type": "task.ingested"}\n'
    )
    monkeypatch.setattr(mon, "_AUDIT_LOG_DIR", tmp_path)
    from airflow_mcp.ops.monitor import _recent_events
    events = _recent_events(n=5)
    assert len(events) == 2  # bad line skipped


# ── _load_active_profile ──────────────────────────────

def test_load_active_profile_returns_unknown_when_no_file(monkeypatch):
    from pathlib import Path
    import airflow_mcp.ops.monitor as mon
    monkeypatch.setattr(mon, "_PARALLEL_CONFIG", Path("/nonexistent/parallel.yaml"))
    from airflow_mcp.ops.monitor import _load_active_profile
    assert _load_active_profile() == "unknown"


def test_load_active_profile_reads_yaml(tmp_path, monkeypatch):
    import airflow_mcp.ops.monitor as mon
    cfg = tmp_path / "parallel.yaml"
    cfg.write_text("mvp_profile: dual-14b\n")
    monkeypatch.setattr(mon, "_PARALLEL_CONFIG", cfg)
    from airflow_mcp.ops.monitor import _load_active_profile
    assert _load_active_profile() == "dual-14b"


# ── print_status ──────────────────────────────────────

def test_print_status_outputs_dashboard(monkeypatch, capsys):
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_gpu_snapshot", lambda: _SAMPLE_GPU)
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_task_counts", lambda: _SAMPLE_TASKS)
    monkeypatch.setattr(
        "airflow_mcp.ops.monitor._load_section_counts", lambda _: _SAMPLE_SECTIONS
    )
    monkeypatch.setattr("airflow_mcp.ops.monitor._recent_events", lambda: [])
    monkeypatch.setattr("airflow_mcp.ops.monitor._load_active_profile", lambda: "single-14b")

    from airflow_mcp.ops.monitor import print_status
    print_status(degraded=False)
    captured = capsys.readouterr()
    assert "NORMAL" in captured.out
    assert "single-14b" in captured.out


# ── 追加テスト: _load_active_profile YAML ガード ──────

def test_load_active_profile_non_dict_yaml_returns_unknown(tmp_path, monkeypatch):
    """parallel.yaml が list などの非dict の場合 'unknown' を返すこと。"""
    from pathlib import Path
    import airflow_mcp.ops.monitor as mon
    cfg = tmp_path / "parallel.yaml"
    cfg.write_text("- item1\n- item2\n")
    monkeypatch.setattr(mon, "_PARALLEL_CONFIG", cfg)
    from airflow_mcp.ops.monitor import _load_active_profile
    assert _load_active_profile() == "unknown"


def test_load_active_profile_empty_file_returns_unknown(tmp_path, monkeypatch):
    """parallel.yaml が空ファイルの場合 'unknown' を返すこと。"""
    from pathlib import Path
    import airflow_mcp.ops.monitor as mon
    cfg = tmp_path / "parallel.yaml"
    cfg.write_text("")
    monkeypatch.setattr(mon, "_PARALLEL_CONFIG", cfg)
    from airflow_mcp.ops.monitor import _load_active_profile
    assert _load_active_profile() == "unknown"

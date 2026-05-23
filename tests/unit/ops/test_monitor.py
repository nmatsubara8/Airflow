"""ops/monitor.py の単体テスト（DDD-08 §5.1）。"""
from __future__ import annotations

import json

import pytest

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

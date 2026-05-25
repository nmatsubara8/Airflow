"""ops/metrics.py の単体テスト（DDD-08 §5.3 / DDD-07 §11.1）。"""
from __future__ import annotations

import json
import socket
import urllib.request
import pytest

from airflow_mcp.ops.metrics import (
    collect_execution_metrics,
    collect_gpu_metrics,
    collect_task_metrics,
    get_metrics_text,
    render_prometheus_text,
    start_metrics_server,
)


@pytest.fixture
def task_store(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    return TaskStore(base_dir=str(tmp_path / "taskboard"))


# ── collect_task_metrics ──────────────────────────────

def test_collect_task_metrics_empty_store(task_store):
    result = collect_task_metrics(task_store)
    assert result == {"task_counts": {}}


def test_collect_task_metrics_counts_by_status_and_section(task_store):
    task_store.create({"title": "A", "section": "engineering", "tags": [], "priority": 5, "owner": None})
    task_store.create({"title": "B", "section": "engineering", "tags": [], "priority": 5, "owner": None})
    task_store.create({"title": "C", "section": "content", "tags": [], "priority": 3, "owner": None})

    result = collect_task_metrics(task_store)
    counts = result["task_counts"]
    assert counts.get(("inbox", "engineering"), 0) == 2
    assert counts.get(("inbox", "content"), 0) == 1


def test_collect_task_metrics_done_tasks(task_store):
    t = task_store.create({"title": "D", "section": "business", "tags": [], "priority": 4, "owner": None})
    task_store.claim(t["id"], "agent_docs")
    task_store.transition(t["id"], "done")

    result = collect_task_metrics(task_store)
    counts = result["task_counts"]
    assert counts.get(("done", "business"), 0) == 1


# ── collect_gpu_metrics ───────────────────────────────

def test_collect_gpu_metrics_returns_dict(monkeypatch):
    import types
    fake_gpu = types.SimpleNamespace(
        snapshot=lambda: {"vram_used_gb": 12.5, "vram_total_gb": 16.0},
        free_vram_gb=3.5,
    )
    fake_queue = types.SimpleNamespace(model_depths=lambda: {"qwen25_14b_q4": 3})

    import airflow_mcp.gpu.server as gpu_mod
    monkeypatch.setattr(gpu_mod, "_gpu", fake_gpu)
    monkeypatch.setattr(gpu_mod, "_scheduler_queue", fake_queue)
    monkeypatch.setattr(gpu_mod, "_degraded", True)

    result = collect_gpu_metrics()
    assert result["gpu_vram_used_gb"] == 12.5
    assert result["scheduler_degraded"] == 1
    assert result["scheduler_queue_depths"] == {"qwen25_14b_q4": 3}


def test_collect_gpu_metrics_returns_empty_on_import_error(monkeypatch):
    import sys
    original = sys.modules.pop("airflow_mcp.gpu.server", None)
    monkeypatch.setitem(sys.modules, "airflow_mcp.gpu.server", None)
    result = collect_gpu_metrics()
    assert result == {}
    if original is not None:
        sys.modules["airflow_mcp.gpu.server"] = original
    else:
        sys.modules.pop("airflow_mcp.gpu.server", None)


# ── collect_execution_metrics ────────────────────────

def test_collect_execution_metrics_missing_file(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW_EXECUTION_LOG", str(tmp_path / "nonexistent.jsonl"))
    result = collect_execution_metrics()
    assert result == {}


def test_collect_execution_metrics_parses_log(tmp_path, monkeypatch):
    log = tmp_path / "exec.jsonl"
    log.write_text(
        json.dumps({"model_id": "qwen14b", "duration_s": 2.5}) + "\n"
        + json.dumps({"model_id": "qwen14b", "duration_s": 4.0}) + "\n"
        + json.dumps({"model_id": "bge", "duration_s": 0.5}) + "\n"
        + "\n"
        + "bad-line\n"
        + json.dumps({"model_id": "missing_dur"}) + "\n"
    )
    monkeypatch.setenv("AIRFLOW_EXECUTION_LOG", str(log))
    result = collect_execution_metrics()
    assert "qwen14b" in result["execution_durations"]
    assert result["execution_durations"]["qwen14b"]["count"] == 2
    assert result["execution_durations"]["bge"]["count"] == 1
    assert "missing_dur" not in result["execution_durations"]


# ── render_prometheus_text ────────────────────────────

def test_render_includes_task_total():
    task_m = {"task_counts": {("done", "engineering"): 5, ("inbox", "content"): 2}}
    text = render_prometheus_text(task_m, {}, {})
    assert 'airflow_tasks_total{status="done",section="engineering"} 5' in text
    assert 'airflow_tasks_total{status="inbox",section="content"} 2' in text


def test_render_includes_gpu_metrics():
    gpu_m = {
        "gpu_vram_used_gb": 12.0,
        "gpu_vram_total_gb": 16.0,
        "scheduler_degraded": 1,
        "scheduler_queue_depths": {"qwen14b": 3},
    }
    text = render_prometheus_text({}, gpu_m, {})
    assert "airflow_gpu_vram_used_gb 12.0" in text
    assert "airflow_scheduler_degraded 1" in text
    assert 'airflow_scheduler_queue_depth{model_id="qwen14b"} 3' in text


def test_render_includes_execution_duration():
    exec_m = {
        "execution_durations": {
            "qwen14b": {"count": 10, "sum": 30.0, "p50": 2.5, "p95": 4.8, "p99": 5.2}
        }
    }
    text = render_prometheus_text({}, {}, exec_m)
    assert 'airflow_model_execution_duration_seconds_count{model="qwen14b"} 10' in text
    assert 'airflow_model_execution_duration_seconds_sum{model="qwen14b"}' in text
    assert 'quantile="0.5"' in text
    assert 'quantile="0.95"' in text
    assert 'quantile="0.99"' in text


def test_render_empty_inputs():
    text = render_prometheus_text({}, {}, {})
    assert "# HELP airflow_tasks_total" in text
    assert text.endswith("\n")


# ── get_metrics_text ──────────────────────────────────

def test_get_metrics_text_without_store():
    text = get_metrics_text(store=None)
    assert isinstance(text, str)
    assert "# HELP" in text


# ── start_metrics_server ──────────────────────────────

def test_start_metrics_server_responds_to_get(tmp_path):
    """HTTP サーバーが /metrics に 200 を返すこと。"""
    store = None
    port = _find_free_port()
    httpd = start_metrics_server(store=store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            assert resp.status == 200
            body = resp.read().decode("utf-8")
            assert "# HELP" in body
    finally:
        httpd.shutdown()


def test_start_metrics_server_returns_404_for_other_paths(tmp_path):
    port = _find_free_port()
    httpd = start_metrics_server(store=None, port=port)
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/other", timeout=5)
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        httpd.shutdown()


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_collect_task_metrics_exception_branch(monkeypatch):
    """list_by_status が例外を投げても空リストを返してクラッシュしないこと（line 31-32）。"""
    from unittest.mock import MagicMock
    store = MagicMock()
    store.list_by_status.side_effect = RuntimeError("db down")
    result = collect_task_metrics(store)
    assert result == {"task_counts": {}}


def test_collect_execution_metrics_oserror(tmp_path, monkeypatch):
    """OSError が発生した場合に空 dict を返すこと（line 69）。"""
    from unittest.mock import patch
    log = tmp_path / "exec.jsonl"
    log.write_text('{"model_id": "x", "duration_s": 1.0}\n')
    monkeypatch.setenv("AIRFLOW_EXECUTION_LOG", str(log))
    with patch("pathlib.Path.read_text", side_effect=OSError("unreadable")):
        result = collect_execution_metrics()
    assert result == {}


def test_collect_execution_metrics_result_stats(tmp_path, monkeypatch):
    """duration 統計の count/sum/p50/p95 が正しく計算されること（line 78-80）。"""
    import json
    log = tmp_path / "exec.jsonl"
    lines = [json.dumps({"model_id": "m", "duration_s": float(i)}) for i in range(1, 11)]
    log.write_text("\n".join(lines) + "\n")
    monkeypatch.setenv("AIRFLOW_EXECUTION_LOG", str(log))
    result = collect_execution_metrics()
    stats = result["execution_durations"]["m"]
    assert stats["count"] == 10
    assert stats["sum"] == 55.0
    assert stats["p50"] == 6.0   # vals_sorted[10//2] = vals_sorted[5] = 6.0
    assert stats["p95"] == 10.0  # vals_sorted[9] = 10.0

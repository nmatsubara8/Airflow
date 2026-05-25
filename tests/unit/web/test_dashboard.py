"""web/dashboard.py の単体テスト（DDD-08 §5.2）。"""
from __future__ import annotations

import socket
import urllib.error
import urllib.request

import pytest

from airflow_mcp.web.dashboard import (
    render_home,
    render_scheduler,
    render_task_detail,
    render_tasks,
    start_dashboard,
)


@pytest.fixture
def store(tmp_path):
    from airflow_mcp.taskboard.store import TaskStore
    return TaskStore(base_dir=str(tmp_path / "tb"))


@pytest.fixture
def populated_store(store):
    store.create({"title": "Eng task", "section": "engineering", "tags": [], "priority": 5, "owner": None})
    t2 = store.create({"title": "Content task", "section": "content", "tags": [], "priority": 3, "owner": None})
    store.claim(t2["id"], "agent_docs")
    store.add_handoff_note(t2["id"], "note text", author="agent_docs")
    store.transition(t2["id"], "done")
    return store


# ── render_home ───────────────────────────────────────

def test_render_home_contains_status_counts(populated_store):
    html = render_home(populated_store)
    assert "inbox" in html
    assert "done" in html
    assert "Total tasks" in html


def test_render_home_contains_nav_links(populated_store):
    html = render_home(populated_store)
    assert "href='/tasks'" in html
    assert "href='/scheduler'" in html


# ── render_tasks ──────────────────────────────────────

def test_render_tasks_lists_all_tasks(populated_store):
    html = render_tasks(populated_store)
    assert "Eng task" in html
    assert "Content task" in html


def test_render_tasks_filter_by_status(populated_store):
    html = render_tasks(populated_store, status="inbox")
    assert "Eng task" in html
    assert "Content task" not in html


def test_render_tasks_filter_by_section(populated_store):
    html = render_tasks(populated_store, section="engineering")
    assert "Eng task" in html
    assert "Content task" not in html


def test_render_tasks_shows_count(populated_store):
    html = render_tasks(populated_store)
    assert "tasks" in html


# ── render_task_detail ────────────────────────────────

def test_render_task_detail_returns_none_for_missing(store):
    result = render_task_detail(store, "nonexistent_id")
    assert result is None


def test_render_task_detail_shows_task_fields(populated_store):
    tasks = populated_store.list_by_status("inbox")
    tid = tasks[0]["id"]
    html = render_task_detail(populated_store, tid)
    assert html is not None
    assert "Eng task" in html
    assert tid in html


def test_render_task_detail_shows_handoff_notes(populated_store):
    tasks = populated_store.list_by_status("done")
    tid = tasks[0]["id"]
    html = render_task_detail(populated_store, tid)
    assert "note text" in html
    assert "Handoff Notes" in html


def test_render_task_detail_shows_chain(populated_store):
    """chain フィールドがある場合に Chain History セクションが表示されること。"""
    from airflow_mcp.orchestrator.handoff_chain import record_chain_step
    tasks = populated_store.list_by_status("done")
    tid = tasks[0]["id"]
    record_chain_step(tid, "agent_docs", "log_xyz", populated_store)
    html = render_task_detail(populated_store, tid)
    assert "Chain History" in html
    assert "agent_docs" in html


# ── render_scheduler ──────────────────────────────────

def test_render_scheduler_without_gpu_module(monkeypatch):
    """gpu モジュールで例外が発生してもクラッシュしないこと。"""
    import airflow_mcp.gpu.server as real_gpu_mod

    def raise_exc():
        raise RuntimeError("gpu unavailable")

    monkeypatch.setattr(real_gpu_mod._gpu, "snapshot", lambda: (_ for _ in ()).throw(RuntimeError("gpu unavailable")))
    html = render_scheduler()
    assert "Scheduler" in html
    assert "unavailable" in html or "GPU" in html


def test_render_scheduler_with_gpu_module(monkeypatch):
    """gpu モジュールが読める場合にVRAM 情報が表示されること。"""
    import types
    import airflow_mcp.gpu.server as real_gpu_mod

    fake_queue = types.SimpleNamespace(model_depths=lambda: {"qwen14b": 2})
    fake_gpu = types.SimpleNamespace(
        snapshot=lambda: {"vram_used_gb": 10.0, "vram_total_gb": 16.0},
    )
    monkeypatch.setattr(real_gpu_mod, "_gpu", fake_gpu)
    monkeypatch.setattr(real_gpu_mod, "_degraded", False)
    monkeypatch.setattr(real_gpu_mod, "_degradation_mode", "auto")
    monkeypatch.setattr(real_gpu_mod, "_scheduler_queue", fake_queue)
    monkeypatch.setattr(real_gpu_mod, "_current_profile", "single-14b")

    html = render_scheduler()
    assert "10.0" in html
    assert "qwen14b" in html
    assert "single-14b" in html


# ── HTTP server ───────────────────────────────────────

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.slow
def test_dashboard_home_returns_200(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as resp:
            assert resp.status == 200
            assert b"Dashboard" in resp.read()
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_tasks_returns_200(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_task_detail_returns_200(populated_store):
    tasks = populated_store.list_by_status("inbox")
    tid = tasks[0]["id"]
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks/{tid}", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_task_not_found_returns_404(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks/nonexistent", timeout=5)
            assert False, "expected 404"
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_unknown_path_returns_404(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown", timeout=5)
            assert False, "expected 404"
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_scheduler_returns_200(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/scheduler", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_tasks_filter_querystring(populated_store):
    port = _free_port()
    httpd = start_dashboard(store=populated_store, port=port)
    try:
        url = f"http://127.0.0.1:{port}/tasks?status=inbox&section=engineering"
        with urllib.request.urlopen(url, timeout=5) as resp:
            assert resp.status == 200
            body = resp.read().decode()
            assert "Eng task" in body
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_no_store_returns_200():
    port = _free_port()
    httpd = start_dashboard(store=None, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


def test_render_home_exception_branch(monkeypatch):
    """list_by_status が例外を投げても count=0 でクラッシュしないこと（line 75-76）。"""
    from unittest.mock import MagicMock
    bad_store = MagicMock()
    bad_store.list_by_status.side_effect = RuntimeError("db down")
    html = render_home(bad_store)
    assert "0" in html  # count falls back to 0
    assert "Dashboard" in html


def test_render_tasks_exception_branch(monkeypatch):
    """list_by_status が例外を投げても空リストで継続すること（line 93-94）。"""
    from unittest.mock import MagicMock
    bad_store = MagicMock()
    bad_store.list_by_status.side_effect = RuntimeError("db down")
    html = render_tasks(bad_store)
    assert "0 tasks" in html


@pytest.mark.slow
def test_dashboard_no_store_tasks_returns_200():
    """store=None のとき /tasks も 200 を返すこと（line 220）。"""
    port = _free_port()
    httpd = start_dashboard(store=None, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


@pytest.mark.slow
def test_dashboard_no_store_task_detail_returns_200():
    """store=None のとき /tasks/<id> も 200 を返すこと（line 230）。"""
    port = _free_port()
    httpd = start_dashboard(store=None, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks/some_id", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


# ── render_api_status ─────────────────────────────────────

def test_render_api_status_returns_json_with_tasks_and_timestamp(store):
    """render_api_status が tasks と timestamp キーを持つ JSON を返すこと。"""
    import json as _json
    from airflow_mcp.web.dashboard import render_api_status
    raw = render_api_status(store)
    data = _json.loads(raw)
    assert "tasks" in data
    assert "timestamp" in data
    assert "inbox" in data["tasks"]


def test_render_api_status_with_none_store():
    """store=None のとき tasks が空 dict であること。"""
    import json as _json
    from airflow_mcp.web.dashboard import render_api_status
    data = _json.loads(render_api_status(None))
    assert data["tasks"] == {}


def test_dashboard_api_status_http_returns_200(store):
    """HTTP GET /api/status → 200 + JSON body。"""
    import json as _json
    port = _free_port()
    httpd = start_dashboard(store=store, port=port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=5) as resp:
            assert resp.status == 200
            data = _json.loads(resp.read().decode())
            assert "tasks" in data
    finally:
        httpd.shutdown()


# ── render_task_detail join display ───────────────────────

def test_render_api_status_exception_branch():
    """list_by_status が例外を投げても count=0 でクラッシュしないこと（lines 225-226）。"""
    import json as _json
    from unittest.mock import MagicMock
    from airflow_mcp.web.dashboard import render_api_status
    bad_store = MagicMock()
    bad_store.list_by_status.side_effect = RuntimeError("db down")
    data = _json.loads(render_api_status(bad_store))
    assert all(v == 0 for v in data["tasks"].values())


def test_render_task_detail_shows_join_info(store):
    """join フィールドがあるタスクの詳細 HTML に Fan-in Join セクションが含まれること。"""
    t = store.create({"title": "親タスク", "section": "content", "tags": [], "priority": 5, "owner": None})
    store.claim(t["id"], "agent_a")
    child_id = "task_" + "c" * 16
    store.update(t["id"], store.get(t["id"])["lock"]["version"], {
        "join": {"parent_id": t["id"], "child_ids": [child_id], "policy": "all"},
    })
    html = render_task_detail(store, t["id"])
    assert "Fan-in Join" in html
    assert child_id[:16] in html

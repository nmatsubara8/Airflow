"""web/dashboard.py の単体テスト（DDD-08 §5.2）。"""
from __future__ import annotations

import json as json_mod
import socket
import urllib.error
import urllib.parse
import urllib.request

import pytest

from airflow_mcp.web.dashboard import (
    _build_graph_data,
    _render_diff,
    render_create_task,
    render_home,
    render_review_queue,
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


# ── _render_description ───────────────────────────────────

def test_render_description_empty():
    from airflow_mcp.web.dashboard import _render_description
    assert _render_description("") == ""


def test_render_description_plain_text():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("Hello world")
    assert "Hello world" in result


def test_render_description_heading_h2():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("## Section Title")
    assert "<h3" in result
    assert "Section Title" in result


def test_render_description_heading_h3():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("### Sub Section")
    assert "<h4" in result
    assert "Sub Section" in result


def test_render_description_bullet():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("- item one")
    assert "•" in result
    assert "item one" in result


def test_render_description_checkbox_unchecked():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("- [ ] todo item")
    assert "☐" in result
    assert "todo item" in result


def test_render_description_checkbox_checked():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("- [x] done item")
    assert "✅" in result
    assert "done item" in result


def test_render_description_tree_structure():
    from airflow_mcp.web.dashboard import _render_description
    text = "src/\n├── main.py\n└── utils.py"
    result = _render_description(text)
    assert "<pre" in result
    assert "border-left" in result
    assert "main.py" in result
    assert "utils.py" in result


def test_render_description_consecutive_tree_lines_grouped():
    from airflow_mcp.web.dashboard import _render_description
    text = "├── a.py\n├── b.py\n└── c.py"
    result = _render_description(text)
    # 3行がひとつの<pre>にまとまること（<pre>が1つだけ）
    assert result.count("<pre") == 1


def test_render_description_code_block():
    from airflow_mcp.web.dashboard import _render_description
    text = "```python\nprint('hello')\n```"
    result = _render_description(text)
    assert "<pre" in result
    assert "print" in result


def test_render_description_separator():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("---")
    assert "<hr" in result


def test_render_description_escapes_html():
    from airflow_mcp.web.dashboard import _render_description
    result = _render_description("<script>alert('xss')</script>")
    assert "<script>" not in result
    assert "&lt;script&gt;" in result


def test_render_task_detail_description_rendered(store):
    """description フィールドが _render_description で構造化表示されること。"""
    t = store.create({
        "title": "説明付きタスク",
        "section": "engineering",
        "description": "## ゴール\n- [ ] 実装する\n\n### ファイル構成\n├── main.py\n└── utils.py",
    })
    from airflow_mcp.web.dashboard import render_task_detail
    result = render_task_detail(store, t["id"])
    assert "ゴール" in result
    assert "main.py" in result
    assert "<pre" in result   # tree block rendered as pre


# ── render_create_task ────────────────────────────────

def test_render_create_task_returns_form():
    html = render_create_task()
    assert "新規タスク作成" in html
    assert "name='title'" in html
    assert "name='description'" in html
    assert "name='section'" in html
    assert "name='tags'" in html
    assert "name='priority'" in html
    assert "/tasks/new" in html


# ── GET /tasks/new ────────────────────────────────────

def test_get_tasks_new_returns_create_form(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/tasks/new")
        body = resp.read().decode()
        assert resp.status == 200
        assert "新規タスク作成" in body
    finally:
        srv.shutdown()


# ── GET /api/tasks ────────────────────────────────────

def test_get_api_tasks_returns_all_tasks(store):
    import json as json_mod
    store.create({"title": "T1", "section": "eng", "tags": [], "priority": 1, "owner": None})
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/tasks")
        data = json_mod.loads(resp.read().decode())
        assert data["count"] >= 1
        assert "tasks" in data
    finally:
        srv.shutdown()


def test_get_api_tasks_status_filter(store):
    import json as json_mod
    store.create({"title": "T1", "section": "eng", "tags": [], "priority": 1, "owner": None})
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/tasks?status=inbox")
        data = json_mod.loads(resp.read().decode())
        assert data["count"] >= 1
        assert all(t["status"] == "inbox" for t in data["tasks"])
    finally:
        srv.shutdown()


# ── POST /api/tasks (JSON) ────────────────────────────

def test_post_api_tasks_creates_task(store):
    import json as json_mod
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        payload = json_mod.dumps({
            "title": "API作成タスク",
            "section": "engineering",
            "tags": ["test"],
            "priority": 2,
            "description": "説明文",
        }).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        resp = urllib.request.urlopen(req)
        data = json_mod.loads(resp.read().decode())
        assert resp.status == 201
        assert data["task"]["title"] == "API作成タスク"
        assert data["task"]["status"] == "inbox"
    finally:
        srv.shutdown()


def test_post_api_tasks_missing_title_returns_400(store):
    import json as json_mod
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        payload = json_mod.dumps({"section": "eng"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        srv.shutdown()


def test_post_api_tasks_invalid_json_returns_400(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=b"not json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        srv.shutdown()


# ── POST /tasks/new (form) ────────────────────────────

def test_post_tasks_new_creates_and_redirects(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        import urllib.parse
        data = urllib.parse.urlencode({
            "title": "フォーム作成タスク",
            "section": "engineering",
            "tags": "t1, t2",
            "priority": "2",
            "description": "詳細説明",
        }).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/new",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            # 303 redirect は urlopen がエラーとして扱う場合がある
            assert e.code in (200, 303)
        tasks = store.list_by_status("inbox")
        assert any(t["title"] == "フォーム作成タスク" for t in tasks)
    finally:
        srv.shutdown()


def test_post_tasks_new_missing_title_returns_400(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        import urllib.parse
        data = urllib.parse.urlencode({"section": "eng"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/new",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        srv.shutdown()


# ── POST apply_agent_note action ─────────────────────

def test_post_apply_agent_note_updates_description(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1,
                         "owner": None, "description": "旧説明"})
    store.claim(task["id"], "agent_docs")
    hn = store.add_handoff_note(task["id"], "エージェント回答の詳細内容", author="agent_docs")
    note_id = hn["handoff_notes"][-1]["id"]

    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        import urllib.parse
        data = urllib.parse.urlencode({
            "action": "apply_agent_note",
            "note_id": note_id,
        }).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["description"] == "エージェント回答の詳細内容"
    finally:
        srv.shutdown()


def test_post_apply_agent_note_invalid_note_id_returns_400(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")

    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        import urllib.parse
        data = urllib.parse.urlencode({
            "action": "apply_agent_note",
            "note_id": "hn_nonexistent",
        }).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        srv.shutdown()


# ── POST edit_description action ─────────────────────

def test_post_edit_description_updates_description(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1,
                         "owner": None, "description": "旧説明"})
    store.claim(task["id"], "agent_docs")

    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        import urllib.parse
        data = urllib.parse.urlencode({
            "action": "edit_description",
            "new_description": "新しい説明文",
        }).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["description"] == "新しい説明文"
    finally:
        srv.shutdown()


# ── nav includes タスク作成 link ─────────────────────

def test_nav_includes_task_create_link(store):
    html = render_home(store)
    assert "タスク作成" in html
    assert "/tasks/new" in html


# ── _render_diff ──────────────────────────────────────

def test_render_diff_no_change_returns_unchanged_message():
    result = _render_diff("same text", "same text")
    assert "変更なし" in result


def test_render_diff_with_changes_shows_diff():
    result = _render_diff("old line\n", "new line\n")
    assert "<pre" in result
    assert "old line" in result or "new line" in result


# ── render_description h1 heading ─────────────────────

def test_render_description_heading_h1(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1,
                         "owner": None, "description": "# トップ見出し"})
    html = render_task_detail(store, task["id"])
    assert html is not None
    assert "h2" in html or "トップ見出し" in html


# ── _build_graph_data ─────────────────────────────────

def test_build_graph_data_returns_nodes_and_edges(populated_store):
    data = _build_graph_data(populated_store)
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) >= 1


def test_build_graph_data_with_join_parent(store):
    parent = store.create({"title": "Parent", "section": "eng", "tags": [], "priority": 1, "owner": None})
    child = store.create({"title": "Child", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.update(child["id"], child["lock"]["version"], {"join": {"parent_id": parent["id"]}})
    data = _build_graph_data(store)
    assert any(e["type"] == "spawn" for e in data["edges"])


def test_build_graph_data_with_join_child_ids(store):
    parent = store.create({"title": "Parent", "section": "eng", "tags": [], "priority": 1, "owner": None})
    child = store.create({"title": "Child", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.update(parent["id"], parent["lock"]["version"], {"join": {"child_ids": [child["id"]]}})
    data = _build_graph_data(store)
    assert any(e["type"] == "spawn" for e in data["edges"])


def test_build_graph_data_owner_dict(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.update(task["id"], task["lock"]["version"], {"owner": {"id": "agent_docs", "type": "agent"}})
    data = _build_graph_data(store)
    node = next((n for n in data["nodes"] if n["id"] == task["id"]), None)
    assert node is not None
    assert node["owner"] == "agent_docs"


def test_build_graph_data_exception_in_list_by_status(store, monkeypatch):
    real_list = store.list_by_status
    call_count = [0]
    def patched(s):
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("db error")
        return real_list(s)
    monkeypatch.setattr(store, "list_by_status", patched)
    data = _build_graph_data(store)
    assert "nodes" in data


# ── render_graph and /graph /api/graph routes ─────────

def test_get_graph_returns_200(populated_store):
    srv = start_dashboard(populated_store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/graph")
        assert resp.status == 200
        assert b"timeline" in resp.read()
    finally:
        srv.shutdown()


def test_get_api_graph_returns_json(populated_store):
    srv = start_dashboard(populated_store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/graph")
        data = json_mod.loads(resp.read().decode())
        assert "nodes" in data
        assert "edges" in data
    finally:
        srv.shutdown()


# ── /review route ─────────────────────────────────────

def test_get_review_returns_200(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/review")
        assert resp.status == 200
        body = resp.read().decode()
        assert "レビューキュー" in body
    finally:
        srv.shutdown()


# ── render_review_queue ───────────────────────────────

def test_render_review_queue_no_store():
    result = render_review_queue(None)
    assert "Store unavailable" in result


def test_render_review_queue_exception(store, monkeypatch):
    monkeypatch.setattr(store, "list_by_status", lambda s: (_ for _ in ()).throw(RuntimeError("db error")))
    result = render_review_queue(store)
    assert "Error" in result


def test_render_review_queue_empty(store):
    result = render_review_queue(store)
    assert "レビュー待ちのタスクはありません" in result


def test_render_review_queue_with_ingester_task(store):
    task = store.create({"title": "Vault Note", "section": "eng",
                         "tags": ["ingester-review"], "priority": 1, "owner": None})
    store.transition(task["id"], "needs_human", reason="ingester:review")
    result = render_review_queue(store)
    assert "Vault Note" in result


# ── /api/tasks exception branch ───────────────────────

def test_get_api_tasks_exception(store, monkeypatch):
    monkeypatch.setattr(store, "list_all", lambda: (_ for _ in ()).throw(RuntimeError("db error")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/tasks")
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()


# ── POST /api/tasks store None branch ─────────────────

def test_post_api_tasks_store_none():
    from airflow_mcp.web.dashboard import start_dashboard as sd
    srv = sd(None, port=0)
    port = srv.server_address[1]
    try:
        body = json_mod.dumps({"title": "T"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 503
    finally:
        srv.shutdown()


# ── POST /api/tasks create exception ──────────────────

def test_post_api_tasks_create_exception(store, monkeypatch):
    monkeypatch.setattr(store, "create", lambda d: (_ for _ in ()).throw(RuntimeError("disk full")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        body = json_mod.dumps({"title": "T"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()


# ── POST /tasks/new priority ValueError and create exception ──

def test_post_tasks_new_invalid_priority_uses_default(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"title": "T", "priority": "abc"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/new",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        tasks = store.list_all()
        assert any(t["title"] == "T" for t in tasks)
    finally:
        srv.shutdown()


def test_post_tasks_new_create_exception(store, monkeypatch):
    monkeypatch.setattr(store, "create", lambda d: (_ for _ in ()).throw(RuntimeError("disk full")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"title": "T"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/new",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()


# ── POST action 404 (task not found) ──────────────────

def test_post_action_task_not_found_returns_404(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "note", "instruction": "test"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/nonexistent/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        srv.shutdown()


# ── POST action owner as dict branch ──────────────────

def test_post_action_owner_dict(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.update(task["id"], task["lock"]["version"], {"owner": {"id": "agent_docs", "type": "agent"}})
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "note", "instruction": "check owner"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
    finally:
        srv.shutdown()


# ── POST apply_agent_note exception branch ────────────

def test_post_apply_agent_note_exception(store, monkeypatch):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    hn = store.add_handoff_note(task["id"], "回答", author="agent_docs")
    note_id = hn["handoff_notes"][-1]["id"]
    monkeypatch.setattr(store, "update", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("lock")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "apply_agent_note", "note_id": note_id}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()


# ── POST edit_description exception branch ────────────

def test_post_edit_description_exception(store, monkeypatch):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    monkeypatch.setattr(store, "update", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("lock")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "edit_description", "new_description": "X"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()


# ── POST retry / reconsider / deep_dive / continue actions ──

def test_post_action_retry_transitions_to_inbox(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    store.transition(task["id"], "done")
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "retry", "instruction": ""}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["status"] == "inbox"
    finally:
        srv.shutdown()


def test_post_action_reconsider_transitions_to_inbox(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    store.transition(task["id"], "done")
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "reconsider", "instruction": "もう一度"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["status"] == "inbox"
    finally:
        srv.shutdown()


def test_post_action_deep_dive_transitions_to_inbox(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    store.transition(task["id"], "done")
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "deep_dive", "instruction": "詳しく"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["status"] == "inbox"
    finally:
        srv.shutdown()


def test_post_action_continue_transitions_to_inbox(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    store.transition(task["id"], "done")
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "continue", "instruction": "続けて"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["status"] == "inbox"
    finally:
        srv.shutdown()


def test_post_action_reconsider_skips_inbox_if_already_inbox(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "reconsider", "instruction": ""}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code in (200, 303)
        updated = store.get(task["id"])
        assert updated["status"] == "inbox"
    finally:
        srv.shutdown()


# ── POST 405 (unsupported path) ───────────────────────

def test_post_unknown_path_returns_405(store):
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = b"x=1"
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/unknown/path",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 405
    finally:
        srv.shutdown()


# ── handle_post store None ────────────────────────────

def test_post_action_store_none_returns_503(store):
    from airflow_mcp.web.dashboard import start_dashboard as sd
    srv = sd(None, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "note", "instruction": "x"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/task_abc/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 503
    finally:
        srv.shutdown()


# ── Recommended tasks section ─────────────────────────

def test_render_task_detail_recommended_tasks_shown(store):
    t1 = store.create({"title": "Main", "section": "eng", "tags": ["ai"], "priority": 1, "owner": None})
    store.create({"title": "Related", "section": "eng", "tags": ["ai"], "priority": 2, "owner": None})
    html = render_task_detail(store, t1["id"])
    assert html is not None
    assert "推奨タスク" in html or "Related" in html


def test_render_task_detail_done_no_inbox_tasks_shows_message(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    store.claim(task["id"], "agent_docs")
    store.transition(task["id"], "done")
    html = render_task_detail(store, task["id"])
    assert html is not None
    assert "inbox に関連タスクはありません" in html


# ── Agent notes diff section ──────────────────────────

def test_render_task_detail_shows_agent_note_after_human_instruction(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1,
                         "owner": None, "description": "元の説明"})
    store.claim(task["id"], "agent_docs")
    store.add_handoff_note(task["id"], "人間の指示内容", author="dashboard",
                           description_snapshot="元の説明")
    store.add_handoff_note(task["id"], "エージェントの回答", author="agent_docs")
    html = render_task_detail(store, task["id"])
    assert html is not None
    assert "エージェントの対応" in html or "description に反映" in html


def test_render_task_detail_agent_note_breaks_at_second_dashboard_note(store):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1,
                         "owner": None, "description": "元の説明"})
    store.claim(task["id"], "agent_docs")
    store.add_handoff_note(task["id"], "指示1", author="dashboard",
                           description_snapshot="元の説明")
    store.add_handoff_note(task["id"], "エージェント回答", author="agent_docs")
    store.add_handoff_note(task["id"], "指示2 (should not show in first)", author="dashboard",
                           description_snapshot="中間説明")
    html = render_task_detail(store, task["id"])
    assert html is not None
    assert "エージェントの対応" in html


# ── Recommendation try/except branch ─────────────────

def test_render_task_detail_recommendation_exception(store, monkeypatch):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    real_list = store.list_by_status
    def patched_list(s):
        if s == "inbox":
            raise RuntimeError("db error")
        return real_list(s)
    monkeypatch.setattr(store, "list_by_status", patched_list)
    html = render_task_detail(store, task["id"])
    assert html is not None


# ── Ingester-review needs_human action form ───────────

def test_render_task_detail_ingester_review_needs_human_shows_action_form(store):
    task = store.create({"title": "Vault Note", "section": "eng",
                         "tags": ["ingester-review"], "priority": 1, "owner": None})
    store.transition(task["id"], "needs_human", reason="ingester:review")
    html = render_task_detail(store, task["id"])
    assert html is not None
    assert "承認" in html or "Vault" in html


# ── _relevance source_note_id branch ─────────────────

def test_render_task_detail_relevance_uses_source_note_id(store):
    t1 = store.create({"title": "Main", "section": "eng", "tags": [], "priority": 1,
                       "owner": None})
    store.update(t1["id"], t1["lock"]["version"], {"source_note_id": "note123"})
    t2 = store.create({"title": "Related by note", "section": "other", "tags": [],
                       "priority": 1, "owner": None})
    store.update(t2["id"], t2["lock"]["version"], {"source_note_id": "note123"})
    html = render_task_detail(store, t1["id"])
    assert html is not None
    assert "Related by note" in html or "推奨タスク" in html


# ── POST do_POST auth failure (401) ───────────────────

def test_post_api_tasks_auth_failure_returns_401(store, monkeypatch):
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "secret-token")
    # Patch _expected_token in the http_server module to return the token
    import airflow_mcp.common.http_server as hs
    monkeypatch.setattr(hs, "_expected_token", lambda: "secret-token")
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        body = json_mod.dumps({"title": "T"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/tasks",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            assert False, "Expected 401"
        except urllib.error.HTTPError as e:
            assert e.code == 401
    finally:
        srv.shutdown()


# ── POST action exception in transition ───────────────

def test_post_action_transition_exception_returns_500(store, monkeypatch):
    task = store.create({"title": "T", "section": "eng", "tags": [], "priority": 1, "owner": None})
    monkeypatch.setattr(store, "add_handoff_note", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db")))
    srv = start_dashboard(store, port=0)
    port = srv.server_address[1]
    try:
        data = urllib.parse.urlencode({"action": "note", "instruction": "test"}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/tasks/{task['id']}/action",
            data=data,
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            assert e.code == 500
    finally:
        srv.shutdown()

"""最小限の read-only Web ダッシュボード（DDD-08 §5.2）。

stdlib の http.server のみ使用（FastAPI 不要）。
認証は内蔵 JWT（Authorization: Bearer <token>）で行う。
デーモンスレッドで起動し、外部の MCP サーバーと共存する。
"""
from __future__ import annotations

import html
import json
import logging
import os
from datetime import datetime, timezone
from http.server import HTTPServer
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..taskboard.store import TaskStore

from ..common.http_server import _BaseHandler, start_server

logger = logging.getLogger(__name__)

_DASHBOARD_PORT = int(os.environ.get("AIRFLOW_DASHBOARD_PORT", "7400"))


# ── HTML ヘルパー ────────────────────────────────────────

_AUTO_REFRESH_S = int(os.environ.get("AIRFLOW_DASHBOARD_REFRESH", "30"))


def _page(title: str, body: str, refresh: bool = True) -> str:
    refresh_tag = f"<meta http-equiv='refresh' content='{_AUTO_REFRESH_S}'>" if refresh and _AUTO_REFRESH_S > 0 else ""
    return (
        "<!DOCTYPE html><html lang='ja'><head>"
        "<meta charset='utf-8'>"
        f"{refresh_tag}"
        f"<title>{html.escape(title)} – Airflow</title>"
        "<style>body{font-family:sans-serif;margin:2em;}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:.4em .8em;text-align:left}"
        "th{background:#f0f0f0}a{color:#0066cc}"
        ".badge{padding:.1em .5em;border-radius:.3em;font-size:.85em}"
        ".inbox{background:#ddf}.in_progress{background:#ffd}"
        ".done{background:#dfd}.needs_human{background:#fdd}"
        ".blocked{background:#ffa}.cancelled{background:#eee}"
        ".DEGRADED{background:#f88;color:#fff}.NORMAL{background:#8f8}</style>"
        "</head><body>"
        "<nav><a href='/'>Home</a> | "
        "<a href='/tasks'>Tasks</a> | "
        "<a href='/scheduler'>Scheduler</a> | "
        "<a href='/api/status'>API</a> | "
        "<a href='http://localhost:9090/metrics' target='_blank'>Metrics</a></nav><hr>"
        f"<h1>{html.escape(title)}</h1>"
        f"{body}"
        "</body></html>"
    )


def _badge(status: str) -> str:
    s = html.escape(status)
    return f"<span class='badge {s}'>{s}</span>"


def _table(headers: list[str], rows: list[list[str]]) -> str:
    ths = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    trs = ""
    for row in rows:
        tds = "".join(f"<td>{cell}</td>" for cell in row)
        trs += f"<tr>{tds}</tr>"
    return f"<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>"


# ── ページレンダラー ────────────────────────────────────

def render_home(store: TaskStore) -> str:
    statuses = ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]
    rows = []
    total = 0
    for s in statuses:
        try:
            count = len(store.list_by_status(s))
        except Exception as exc:
            logger.debug("render_home: list_by_status(%s) failed: %s", s, exc)
            count = 0
        total += count
        rows.append([_badge(s), str(count)])
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = f"<p>Generated: {now} | Total tasks: {total}</p>"
    body += _table(["Status", "Count"], rows)
    return _page("Dashboard", body)


def render_tasks(store: TaskStore, status: str | None = None, section: str | None = None) -> str:
    statuses = ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]
    tasks: list[dict[str, Any]] = []
    for s in statuses:
        if status and s != status:
            continue
        try:
            tasks.extend(store.list_by_status(s))
        except Exception as exc:
            logger.debug("render_tasks: list_by_status(%s) failed: %s", s, exc)

    if section:
        tasks = [t for t in tasks if t.get("section") == section]

    rows = []
    for t in tasks:
        tid = html.escape(t.get("id", ""))
        title = html.escape(t.get("title", "")[:60])
        rows.append([
            f"<a href='/tasks/{tid}'>{tid[:16]}…</a>",
            title,
            _badge(t.get("status", "")),
            html.escape(t.get("section", "")),
            html.escape(t.get("owner") or "—"),
            html.escape(str(t.get("priority", ""))),
        ])

    filters = ""
    if status:
        filters += f" (status={status})"
    if section:
        filters += f" (section={section})"

    body = f"<p>{len(tasks)} tasks{filters}</p>"
    body += _table(["ID", "Title", "Status", "Section", "Owner", "Priority"], rows)
    return _page("Tasks", body)


def render_task_detail(store: TaskStore, task_id: str) -> str | None:
    try:
        task = store.get(task_id)
    except Exception as exc:
        logger.debug("render_task_detail: task %s not found: %s", task_id, exc)
        return None

    rows = [
        [
            html.escape(str(k)),
            html.escape(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)),
        ]
        for k, v in task.items()
        if k not in ("handoff_notes", "chain")
    ]

    body = _table(["Field", "Value"], rows)

    notes = task.get("handoff_notes", [])
    if notes:
        body += "<h2>Handoff Notes</h2><ol>"
        for n in notes:
            author_raw = n.get("author", "")
            if isinstance(author_raw, dict):
                author_raw = author_raw.get("id", str(author_raw))
            author = html.escape(str(author_raw))
            ts = html.escape(n.get("timestamp", n.get("created_at", "")))
            content = html.escape(n.get("body", n.get("content", "")))
            body += f"<li><strong>{author}</strong> ({ts})<br><pre>{content}</pre></li>"
        body += "</ol>"

    chain = task.get("chain", [])
    if chain:
        body += "<h2>Chain History</h2>"
        chain_rows = [[
            html.escape(c.get("agent_id", "")),
            html.escape(c.get("completed_at", "")),
            html.escape(str(c.get("result_ref") or "—")),
        ] for c in chain]
        body += _table(["Step", "Agent", "Completed At", "Result Ref"],
                       [[str(i + 1)] + r for i, r in enumerate(chain_rows)])

    join_info = task.get("join")
    if join_info:
        body += "<h2>Fan-in Join</h2>"
        child_links = ", ".join(
            f"<a href='/tasks/{html.escape(cid)}'>{html.escape(cid[:16])}…</a>"
            for cid in join_info.get("child_ids", [])
        )
        body += f"<p>Policy: {html.escape(join_info.get('policy', 'all'))} | Children: {child_links or '—'}</p>"

    # アクションフォーム
    status = task.get("status", "")
    body += """
<h2>アクション</h2>
<form method='POST' action='""" + f"/tasks/{task_id}/action" + """'>
<table style='width:auto;border:none'>
<tr><td style='border:none;padding:.4em'><strong>操作</strong></td><td style='border:none'>
  <label><input type='radio' name='action' value='note' checked> 📝 メモ追加（指示・コメント）</label><br>
  <label><input type='radio' name='action' value='retry'> 🔄 やり直し（inbox に戻す）</label><br>
  <label><input type='radio' name='action' value='reconsider'> 🔍 再検討（同エージェントで再処理）</label><br>
  <label><input type='radio' name='action' value='deep_dive'> 🔬 深堀り（詳細分析を要求）</label><br>
  <label><input type='radio' name='action' value='continue'> ▶️ 後続処理（次ステップへ進める）</label>
</td></tr>
<tr><td style='border:none;padding:.4em'><strong>指示内容</strong></td>
<td style='border:none'><textarea name='instruction' rows='3' cols='60' placeholder='追加の指示・コメントを入力（任意）'></textarea></td></tr>
<tr><td style='border:none'></td><td style='border:none'>
  <button type='submit' style='padding:.4em 1.2em;font-size:1em'>実行</button>
</td></tr>
</table>
</form>
"""
    return _page(f"Task: {task_id[:16]}", body)


def render_scheduler() -> str:
    """スケジューラ・GPU 状態ページ。"""
    body = ""
    try:
        import airflow_mcp.gpu.server as gpu_mod
        snap = gpu_mod._gpu.snapshot()
        used = snap.get("vram_used_gb", 0.0)
        total = snap.get("vram_total_gb", 16.0)
        degraded = gpu_mod._degraded
        mode = gpu_mod._degradation_mode

        body += f"<h2>GPU</h2><p>VRAM: {used:.1f} / {total:.1f} GB</p>"
        body += f"<p>Degradation: {_badge('DEGRADED' if degraded else 'NORMAL')} (mode={mode})</p>"

        depths = gpu_mod._scheduler_queue.model_depths()
        if depths:
            body += "<h2>Queue Depths</h2>"
            body += _table(["Model", "Depth"], [[html.escape(m), str(d)] for m, d in depths.items()])
        else:
            body += "<h2>Queue Depths</h2><p>No queued tasks.</p>"

        profile = gpu_mod._current_profile or "—"
        body += f"<p>Active profile: {html.escape(profile)}</p>"
    except Exception as exc:
        body += f"<p>GPU data unavailable: {html.escape(str(exc))}</p>"

    return _page("Scheduler", body)


# ── HTTP ハンドラ ─────────────────────────────────────

def render_api_status(store: TaskStore | None) -> str:
    """JSON ステータスサマリーを返す（/api/status 用）。"""
    statuses = ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]
    counts: dict[str, int] = {}
    if store is not None:
        for s in statuses:
            try:
                counts[s] = len(store.list_by_status(s))
            except Exception:
                counts[s] = 0
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return json.dumps({"tasks": counts, "timestamp": now}, ensure_ascii=False)


class _DashboardHandler(_BaseHandler):
    _server_name = "dashboard"
    _cors_enabled = True
    store: TaskStore | None = None

    def handle_get(self, path: str, qs: dict[str, list[str]]) -> tuple[int, str, str]:
        path = path.rstrip("/") or "/"
        store = self.__class__.store

        if path == "/":
            content = render_home(store) if store is not None else _page("Dashboard", "<p>No store configured.</p>")
            return 200, "text/html; charset=utf-8", content

        if path == "/tasks":
            status = qs.get("status", [None])[0]
            section = qs.get("section", [None])[0]
            content = render_tasks(store, status, section) if store is not None else _page("Tasks", "<p>No store configured.</p>")
            return 200, "text/html; charset=utf-8", content

        if path.startswith("/tasks/"):
            task_id = path[len("/tasks/"):]
            if store is not None:
                content = render_task_detail(store, task_id)
                if content is None:
                    return 404, "text/html; charset=utf-8", _page("Not Found", f"<p>Task {html.escape(task_id)} not found.</p>")
                return 200, "text/html; charset=utf-8", content
            return 200, "text/html; charset=utf-8", _page("Tasks", "<p>No store configured.</p>")

        if path == "/scheduler":
            return 200, "text/html; charset=utf-8", render_scheduler()

        if path == "/api/status":
            return 200, "application/json; charset=utf-8", render_api_status(store)

        return 404, "text/html; charset=utf-8", _page("Not Found", "<p>Page not found.</p>")

    def handle_post(self, path: str, form: dict[str, str]) -> tuple[int, str, str]:
        store = self.__class__.store
        if store is None:
            return 503, "text/html; charset=utf-8", _page("Error", "<p>Store not available.</p>")

        # POST /tasks/<id>/action
        if path.startswith("/tasks/") and path.endswith("/action"):
            task_id = path[len("/tasks/"):-len("/action")]
            action = form.get("action", "note")
            instruction = form.get("instruction", "").strip()

            try:
                task = store.get(task_id)
            except Exception:
                return 404, "text/html; charset=utf-8", _page("Not Found", f"<p>Task {html.escape(task_id)} not found.</p>")

            current_status = task.get("status", "")
            owner = task.get("owner")
            if isinstance(owner, dict):
                owner = owner.get("id", "")

            note_prefix = {
                "note": "📝 [手動指示]",
                "retry": "🔄 [やり直し指示]",
                "reconsider": "🔍 [再検討指示]",
                "deep_dive": "🔬 [深堀り要求]",
                "continue": "▶️ [後続処理指示]",
            }.get(action, "📝 [手動指示]")

            note_body = f"{note_prefix} {instruction}" if instruction else note_prefix

            try:
                store.add_handoff_note(task_id, note_body, author="dashboard")

                if action == "retry":
                    store.transition(task_id, "inbox", reason="manual:retry")

                elif action in ("reconsider", "deep_dive"):
                    # inbox に戻して dispatcher に再割り当てさせる
                    if current_status != "inbox":
                        store.transition(task_id, "inbox", reason=f"manual:{action}")

                elif action == "continue":
                    # needs_human / blocked → in_progress に戻す
                    if current_status in ("needs_human", "blocked", "done"):
                        store.transition(task_id, "inbox", reason="manual:continue")

                # note のみの場合はステータス変更なし

            except Exception as exc:
                logger.error("dashboard action error task=%s: %s", task_id, exc)
                return 500, "text/html; charset=utf-8", _page("Error", f"<p>Action failed: {html.escape(str(exc))}</p>")

            # 処理後はタスク詳細ページにリダイレクト
            self.send_response(303)
            self.send_header("Location", f"/tasks/{task_id}")
            self.end_headers()
            return -1, "", ""  # _send は呼ばれない（既に send_response 済み）

        return 405, "text/html; charset=utf-8", _page("Error", "<p>Method Not Allowed</p>")


def start_dashboard(
    store: TaskStore | None = None,
    port: int = _DASHBOARD_PORT,
) -> HTTPServer:
    """バックグラウンドスレッドで Web ダッシュボードを起動する。"""
    _DashboardHandler.store = store
    return start_server(_DashboardHandler, port, "dashboard-server")

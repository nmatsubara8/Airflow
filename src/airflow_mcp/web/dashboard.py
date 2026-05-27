"""最小限の Web ダッシュボード（DDD-08 §5.2）。

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
        "<a href='/graph'>Graph</a> | "
        "<a href='/scheduler'>Scheduler</a> | "
        "<a href='/api/status'>API</a> | "
        "<a href='http://localhost:9090/metrics' target='_blank'>Metrics</a></nav><hr>"
        f"<h1>{html.escape(title)}</h1>"
        f"{body}"
        "</body></html>"
    )


def _build_graph_data(store: "TaskStore") -> dict:
    """グラフ表示用ノード・エッジデータを構築する。"""
    statuses = ["inbox", "in_progress", "done", "needs_human", "blocked", "cancelled"]
    all_tasks: list[dict] = []
    for s in statuses:
        try:
            all_tasks.extend(store.list_by_status(s))
        except Exception:
            pass

    nodes = []
    edges = []
    edge_set: set[tuple[str, str, str]] = set()

    for task in all_tasks:
        tid = task["id"]
        chain = task.get("chain") or []
        agent_flow = " → ".join(c.get("agent_id", "") for c in chain) if chain else ""
        owner_raw = task.get("owner") or ""
        owner = owner_raw.get("id", "") if isinstance(owner_raw, dict) else str(owner_raw)
        nodes.append({
            "id": tid,
            "title": task.get("title", "")[:60],
            "status": task.get("status", ""),
            "section": task.get("section", ""),
            "owner": owner,
            "agent_flow": agent_flow,
            "created_at": task.get("created_at", ""),
            "updated_at": task.get("updated_at", ""),
            "priority": task.get("priority", 3),
        })

        join_info = task.get("join") or {}
        parent_id = join_info.get("parent_id")
        if parent_id:
            key = (parent_id, tid, "spawn")
            if key not in edge_set:
                edges.append({"source": parent_id, "target": tid, "type": "spawn"})
                edge_set.add(key)
        for child_id in join_info.get("child_ids") or []:
            key = (tid, child_id, "spawn")
            if key not in edge_set:
                edges.append({"source": tid, "target": child_id, "type": "spawn"})
                edge_set.add(key)

    return {"nodes": nodes, "edges": edges}


def render_graph(store: "TaskStore") -> str:
    """タスク関係グラフページ（D3.js）。"""
    data = _build_graph_data(store)
    data_json = json.dumps(data, ensure_ascii=False)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    body = f"""
<p style="color:#666;font-size:.9em">Generated: {now} | {len(data['nodes'])} tasks | {len(data['edges'])} edges</p>
<div id="tabs" style="margin-bottom:.5em">
  <button onclick="showTab('graph')" id="tab-graph" class="tab-btn active">依存グラフ</button>
  <button onclick="showTab('timeline')" id="tab-timeline" class="tab-btn">タイムライン</button>
  <span style="margin-left:1em;font-size:.85em;color:#888">ノードをクリック→詳細 | ドラッグで移動</span>
</div>
<div id="view-graph">
  <svg id="graph-svg" style="width:100%;height:620px;border:1px solid #ddd;background:#fafafa;border-radius:4px"></svg>
</div>
<div id="view-timeline" style="display:none">
  <div id="timeline-container" style="overflow-x:auto">
    <svg id="timeline-svg" style="min-width:800px;border:1px solid #ddd;background:#fafafa;border-radius:4px"></svg>
  </div>
</div>
<div id="tt" style="position:fixed;display:none;background:rgba(0,0,0,.82);color:#fff;padding:.4em .7em;border-radius:4px;font-size:.85em;pointer-events:none;max-width:280px;z-index:999"></div>

<style>
.tab-btn{{padding:.3em .9em;cursor:pointer;border:1px solid #ccc;background:#f0f0f0;border-radius:3px;margin-right:.3em}}
.tab-btn.active{{background:#0066cc;color:#fff;border-color:#0066cc}}
</style>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const DATA = {data_json};
const STATUS_COLOR = {{
  inbox:'#aaccff', in_progress:'#ffe88a', done:'#88dd88',
  needs_human:'#ff9999', blocked:'#ffcc77', cancelled:'#cccccc'
}};
const SECTION_X = {{}};

function showTab(t){{
  document.getElementById('view-graph').style.display = t==='graph'?'':'none';
  document.getElementById('view-timeline').style.display = t==='timeline'?'':'none';
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+t).classList.add('active');
  if(t==='timeline') drawTimeline();
}}

// ── 依存グラフ ──────────────────────────────────
(function(){{
  const svg = d3.select('#graph-svg');
  const el = svg.node().getBoundingClientRect();
  const W = el.width || 900, H = 620;
  svg.attr('viewBox',`0 0 ${{W}} ${{H}}`);
  const tt = document.getElementById('tt');

  svg.append('defs').append('marker')
    .attr('id','arr').attr('viewBox','0 -4 8 8').attr('refX',22)
    .attr('markerWidth',5).attr('markerHeight',5).attr('orient','auto')
    .append('path').attr('d','M0,-4L8,0L0,4').attr('fill','#888');

  if(DATA.nodes.length===0){{
    svg.append('text').attr('x',W/2).attr('y',H/2).attr('text-anchor','middle')
      .style('fill','#999').text('タスクがありません');
    return;
  }}

  const tMin = Math.min(...DATA.nodes.map(n=>new Date(n.created_at||0).getTime()));
  const tMax = Math.max(...DATA.nodes.map(n=>new Date(n.created_at||0).getTime()));
  const tRange = tMax - tMin || 1;

  const sim = d3.forceSimulation(DATA.nodes)
    .force('link', d3.forceLink(DATA.edges).id(d=>d.id).distance(110).strength(0.8))
    .force('charge', d3.forceManyBody().strength(-260))
    .force('center', d3.forceCenter(W/2, H/2))
    .force('y', d3.forceY(d=>{{
      const t = new Date(d.created_at||0).getTime();
      return 50 + (t-tMin)/tRange*(H-100);
    }}).strength(0.25))
    .force('collision', d3.forceCollide(24));

  const link = svg.append('g').selectAll('line').data(DATA.edges).join('line')
    .attr('stroke', d=>d.type==='spawn'?'#e06000':'#8888cc')
    .attr('stroke-width',2).attr('stroke-dasharray', d=>d.type==='spawn'?'':'4 2')
    .attr('marker-end','url(#arr)').attr('opacity',0.7);

  const node = svg.append('g').selectAll('g').data(DATA.nodes).join('g')
    .attr('cursor','pointer')
    .call(d3.drag()
      .on('start',(e,d)=>{{if(!e.active)sim.alphaTarget(0.3).restart();d.fx=d.x;d.fy=d.y}})
      .on('drag',(e,d)=>{{d.fx=e.x;d.fy=e.y}})
      .on('end',(e,d)=>{{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null}}))
    .on('click',(e,d)=>window.location.href='/tasks/'+d.id)
    .on('mouseover',(e,d)=>{{
      tt.style.display='block';
      tt.innerHTML=`<b>${{d.title}}</b><br>状態: ${{d.status}}<br>担当: ${{d.owner||'—'}}<br>セクション: ${{d.section}}<br>${{d.agent_flow?'chain: '+d.agent_flow:''}}`;
    }})
    .on('mousemove',e=>{{tt.style.left=(e.pageX+14)+'px';tt.style.top=(e.pageY-30)+'px'}})
    .on('mouseout',()=>tt.style.display='none');

  const r = d => Math.max(14, 22 - (d.priority||3)*1.5);
  node.append('circle').attr('r',r)
    .attr('fill',d=>STATUS_COLOR[d.status]||'#ddd')
    .attr('stroke',d=>d.status==='needs_human'?'#c00':'#555').attr('stroke-width',1.5);
  node.append('text').attr('text-anchor','middle').attr('dy','0.35em')
    .style('font-size','8px').style('pointer-events','none')
    .text(d=>d.title.substring(0,10)+(d.title.length>10?'…':''));

  // Time axis
  const timeScale = d3.scaleTime().domain([new Date(tMin),new Date(tMax)]).range([50,H-50]);
  const axis = d3.axisLeft(timeScale).ticks(5).tickFormat(d3.timeFormat('%m/%d %H:%M'));
  svg.append('g').attr('transform','translate(55,0)').call(axis)
    .style('font-size','9px').select('.domain').attr('stroke','#ccc');

  // Legend
  const lg = svg.append('g').attr('transform',`translate(${{W-120}},10)`);
  Object.entries(STATUS_COLOR).forEach(([s,c],i)=>{{
    lg.append('rect').attr('x',0).attr('y',i*16).attr('width',11).attr('height',11).attr('fill',c).attr('stroke','#555');
    lg.append('text').attr('x',14).attr('y',i*16+9).style('font-size','10px').text(s);
  }});
  // Edge legend
  const el2 = svg.append('g').attr('transform',`translate(${{W-120}},${{Object.keys(STATUS_COLOR).length*16+14}})`);
  el2.append('line').attr('x2',18).attr('stroke','#e06000').attr('stroke-width',2).attr('y1',5).attr('y2',5);
  el2.append('text').attr('x',20).attr('y',9).style('font-size','9px').text('spawn/join');
  el2.append('line').attr('x2',18).attr('stroke','#8888cc').attr('stroke-width',2).attr('y1',20).attr('y2',20).attr('stroke-dasharray','4 2');
  el2.append('text').attr('x',20).attr('y',24).style('font-size','9px').text('chain');

  sim.on('tick',()=>{{
    link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
        .attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
    node.attr('transform',d=>`translate(${{Math.max(65,Math.min(W-20,d.x))}},${{Math.max(20,Math.min(H-20,d.y))}})`);
  }});
}})();

// ── タイムライン ────────────────────────────────
function drawTimeline(){{
  const container = document.getElementById('timeline-svg');
  if(container.dataset.drawn) return;
  container.dataset.drawn = '1';

  const sections = [...new Set(DATA.nodes.map(n=>n.section||'other'))].sort();
  const W = Math.max(800, sections.length*180+120);
  const MARGIN = {{top:40,left:80,right:20,bottom:20}};
  const colW = (W-MARGIN.left-MARGIN.right)/Math.max(sections.length,1);

  const sorted = [...DATA.nodes].sort((a,b)=>new Date(a.created_at||0)-new Date(b.created_at||0));
  const tMin = new Date(sorted[0]?.created_at||Date.now());
  const tMax = new Date(sorted[sorted.length-1]?.created_at||Date.now());
  const H = Math.max(400, sorted.length*55+MARGIN.top+MARGIN.bottom);

  const svg = d3.select('#timeline-svg').attr('viewBox',`0 0 ${{W}} ${{H}}`).attr('height',H);
  const tt = document.getElementById('tt');

  const tScale = d3.scaleTime().domain([tMin,tMax]).range([MARGIN.top, H-MARGIN.bottom]);
  const timeAxis = d3.axisLeft(tScale).ticks(8).tickFormat(d3.timeFormat('%m/%d %H:%M'));
  svg.append('g').attr('transform',`translate(${{MARGIN.left}},0)`).call(timeAxis).style('font-size','9px');

  // Column headers
  sections.forEach((sec,i)=>{{
    const cx = MARGIN.left + i*colW + colW/2;
    svg.append('line').attr('x1',cx).attr('x2',cx).attr('y1',MARGIN.top).attr('y2',H-MARGIN.bottom)
      .attr('stroke','#e0e0e0').attr('stroke-width',1);
    svg.append('text').attr('x',cx).attr('y',MARGIN.top-10).attr('text-anchor','middle')
      .style('font-size','11px').style('font-weight','bold').style('fill','#555').text(sec);
  }});

  // Build section index for node position
  const secIdx = {{}};
  sections.forEach((s,i)=>secIdx[s]=i);

  // Draw edges first (behind nodes)
  const nodePos = {{}};
  sorted.forEach(n=>{{
    const si = secIdx[n.section||'other']??0;
    nodePos[n.id] = {{x: MARGIN.left+si*colW+colW/2, y: tScale(new Date(n.created_at||tMin))}};
  }});

  DATA.edges.forEach(e=>{{
    const s = nodePos[e.source?.id||e.source];
    const t = nodePos[e.target?.id||e.target];
    if(!s||!t) return;
    svg.append('path')
      .attr('d',`M${{s.x}},${{s.y}} C${{s.x}},${{(s.y+t.y)/2}} ${{t.x}},${{(s.y+t.y)/2}} ${{t.x}},${{t.y}}`)
      .attr('fill','none').attr('stroke',e.type==='spawn'?'#e06000':'#8888cc')
      .attr('stroke-width',1.5).attr('stroke-dasharray',e.type==='spawn'?'':'4 2').attr('opacity',0.6);
  }});

  // Draw nodes
  sorted.forEach(n=>{{
    const pos = nodePos[n.id];
    if(!pos) return;
    const g = svg.append('g').attr('transform',`translate(${{pos.x}},${{pos.y}})`)
      .attr('cursor','pointer')
      .on('click',()=>window.location.href='/tasks/'+n.id)
      .on('mouseover',(e)=>{{
        tt.style.display='block';
        tt.innerHTML=`<b>${{n.title}}</b><br>状態: ${{n.status}}<br>担当: ${{n.owner||'—'}}<br>${{n.agent_flow?'chain: '+n.agent_flow:''}}`;
      }})
      .on('mousemove',e=>{{tt.style.left=(e.pageX+14)+'px';tt.style.top=(e.pageY-30)+'px'}})
      .on('mouseout',()=>tt.style.display='none');

    g.append('circle').attr('r',10).attr('fill',STATUS_COLOR[n.status]||'#ddd')
      .attr('stroke','#555').attr('stroke-width',1.2);
    g.append('text').attr('x',14).attr('dy','0.35em').style('font-size','9px')
      .text(n.title.substring(0,20)+(n.title.length>20?'…':''));
    if(n.agent_flow){{
      g.append('text').attr('x',14).attr('y',12).style('font-size','8px').style('fill','#888')
        .text(n.agent_flow.substring(0,30));
    }}
  }});
}}
</script>
"""
    return _page("Task Graph", body, refresh=False)


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

        if path == "/graph":
            content = render_graph(store) if store is not None else _page("Graph", "<p>No store configured.</p>")
            return 200, "text/html; charset=utf-8", content

        if path == "/api/graph":
            data = _build_graph_data(store) if store is not None else {"nodes": [], "edges": []}
            return 200, "application/json; charset=utf-8", json.dumps(data, ensure_ascii=False)

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

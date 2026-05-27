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
    refresh_js = ""
    if refresh and _AUTO_REFRESH_S > 0:
        ms = _AUTO_REFRESH_S * 1000
        refresh_js = (
            f"<script>"
            f"(function(){{"
            f"var ms={ms},tid=null,paused=false;"
            f"function schedule(){{tid=setTimeout(function(){{if(!paused)location.reload();}},ms);}}"
            f"function pause(){{paused=true;clearTimeout(tid);}}"
            f"function resume(){{paused=false;schedule();}}"
            f"document.addEventListener('focusin',function(e){{"
            f"if(e.target.tagName==='TEXTAREA'||e.target.tagName==='INPUT')pause();}});"
            f"document.addEventListener('focusout',function(e){{"
            f"if(e.target.tagName==='TEXTAREA'||e.target.tagName==='INPUT')resume();}});"
            f"schedule();"
            f"}})();"
            f"</script>"
        )
    return (
        "<!DOCTYPE html><html lang='ja'><head>"
        "<meta charset='utf-8'>"
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
        "<a href='/review'>📋 レビューキュー</a> | "
        "<a href='/graph'>Graph</a> | "
        "<a href='/scheduler'>Scheduler</a> | "
        "<a href='/api/status'>API</a> | "
        "<a href='http://localhost:9090/metrics' target='_blank'>Metrics</a></nav><hr>"
        f"<h1>{html.escape(title)}</h1>"
        f"{body}"
        f"{refresh_js}"
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
<p style="color:#666;font-size:.9em">Generated: {now} | {len(data['nodes'])} tasks | {len(data['edges'])} edges
&nbsp;|&nbsp;<a href="/api/graph">JSON</a>
&nbsp;<button onclick="location.reload()" style="font-size:.85em;padding:.1em .6em">↺ 更新</button>
</p>
<div id="tabs" style="margin-bottom:.8em">
  <button onclick="showTab('timeline')" id="tab-timeline" class="tab-btn active">📅 タイムライン</button>
  <button onclick="showTab('graph')" id="tab-graph" class="tab-btn">🔗 依存グラフ</button>
</div>
<div id="view-timeline">
  <div id="timeline-wrap" style="overflow-x:auto;border:1px solid #ddd;border-radius:4px;background:#fafafa">
    <svg id="timeline-svg"></svg>
  </div>
</div>
<div id="view-graph" style="display:none">
  <p style="color:#888;font-size:.85em">※ tasks.join で親子関係が作られるとエッジが表示されます。ドラッグ可能。</p>
  <svg id="graph-svg" style="width:100%;height:640px;border:1px solid #ddd;background:#fafafa;border-radius:4px"></svg>
</div>
<div id="tt" style="position:fixed;display:none;background:rgba(0,0,0,.85);color:#fff;padding:.5em .8em;border-radius:5px;font-size:.82em;pointer-events:none;max-width:320px;z-index:999;line-height:1.5"></div>

<style>
.tab-btn{{padding:.35em 1em;cursor:pointer;border:1px solid #ccc;background:#f0f0f0;border-radius:3px;margin-right:.3em;font-size:.95em}}
.tab-btn.active{{background:#0066cc;color:#fff;border-color:#0066cc}}
</style>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const DATA = {data_json};
const SC = {{
  inbox:'#aaccff', in_progress:'#ffe470', done:'#77cc77',
  needs_human:'#ff8888', blocked:'#ffbb55', cancelled:'#cccccc'
}};

function showTab(t){{
  ['graph','timeline'].forEach(v=>document.getElementById('view-'+v).style.display=v===t?'':'none');
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+t).classList.add('active');
}}

const tt = document.getElementById('tt');
function tip(html){{ tt.style.display='block'; tt.innerHTML=html; }}
function tipMove(e){{ tt.style.left=(e.pageX+14)+'px'; tt.style.top=(e.pageY-34)+'px'; }}
function tipHide(){{ tt.style.display='none'; }}

// ════════════════════════════════════════
//  タイムライン（デフォルト表示）
// ════════════════════════════════════════
(function drawTimeline(){{
  if(!DATA.nodes.length){{ document.getElementById('timeline-wrap').innerHTML='<p style="padding:1em;color:#999">タスクがありません</p>'; return; }}

  const sections = [...new Set(DATA.nodes.map(n=>n.section||'other'))].sort();
  const ROW_H = 64, ML = 90, MT = 44, MR = 20, MB = 20;
  const COL_W = Math.max(220, Math.floor((Math.max(900,sections.length*230)-ML-MR)/sections.length));
  const W = ML + sections.length*COL_W + MR;

  const sorted = [...DATA.nodes].sort((a,b)=>new Date(a.created_at||0)-new Date(b.created_at||0));
  const H = MT + sorted.length*ROW_H + MB;

  const svg = d3.select('#timeline-svg').attr('viewBox',`0 0 ${{W}} ${{H}}`).attr('width',W).attr('height',H);

  // background stripes
  sorted.forEach((_,i)=>{{
    svg.append('rect').attr('x',0).attr('y',MT+i*ROW_H).attr('width',W).attr('height',ROW_H)
      .attr('fill',i%2?'#f7f7f7':'#fff');
  }});

  // time axis
  const tMin = new Date(sorted[0].created_at||Date.now());
  const tMax = new Date(sorted[sorted.length-1].created_at||Date.now());
  const tEq = tMin.getTime()===tMax.getTime();
  const tScale = d3.scaleTime()
    .domain(tEq?[new Date(tMin.getTime()-60000),new Date(tMin.getTime()+60000)]:[tMin,tMax])
    .range([MT+ROW_H/2, H-MB-ROW_H/2]);
  svg.append('g').attr('transform',`translate(${{ML}},0)`)
    .call(d3.axisLeft(tScale).ticks(Math.min(8,sorted.length)).tickFormat(d3.timeFormat('%m/%d %H:%M')))
    .style('font-size','10px');

  // column headers
  sections.forEach((sec,i)=>{{
    const cx = ML + i*COL_W + COL_W/2;
    svg.append('rect').attr('x',ML+i*COL_W).attr('y',0).attr('width',COL_W).attr('height',MT)
      .attr('fill',i%2?'#e8eef8':'#dde4f0');
    svg.append('text').attr('x',cx).attr('y',MT-8).attr('text-anchor','middle')
      .style('font-size','12px').style('font-weight','bold').style('fill','#334').text(sec);
    svg.append('line').attr('x1',cx).attr('x2',cx).attr('y1',MT).attr('y2',H)
      .attr('stroke','#ddd').attr('stroke-width',1);
  }});

  // node positions
  const secIdx = Object.fromEntries(sections.map((s,i)=>[s,i]));
  const pos = {{}};
  sorted.forEach(n=>{{
    const si = secIdx[n.section||'other']??0;
    pos[n.id] = {{ x: ML+si*COL_W+COL_W/2, y: tScale(new Date(n.created_at||tMin)) }};
  }});

  // edges
  DATA.edges.forEach(e=>{{
    const s=pos[typeof e.source==='object'?e.source.id:e.source];
    const t=pos[typeof e.target==='object'?e.target.id:e.target];
    if(!s||!t) return;
    const mid=(s.y+t.y)/2;
    svg.append('path')
      .attr('d',`M${{s.x}},${{s.y}} C${{s.x}},${{mid}} ${{t.x}},${{mid}} ${{t.x}},${{t.y}}`)
      .attr('fill','none').attr('stroke',e.type==='spawn'?'#d05000':'#7777bb')
      .attr('stroke-width',2).attr('opacity',.7)
      .attr('marker-end','url(#arr-tl)');
  }});
  if(DATA.edges.length){{
    svg.append('defs').append('marker').attr('id','arr-tl')
      .attr('viewBox','0 -4 8 8').attr('refX',14).attr('markerWidth',5).attr('markerHeight',5).attr('orient','auto')
      .append('path').attr('d','M0,-4L8,0L0,4').attr('fill','#888');
  }}

  // nodes
  sorted.forEach(n=>{{
    const p=pos[n.id]; if(!p) return;
    const g=svg.append('g').attr('transform',`translate(${{p.x}},${{p.y}})`)
      .attr('cursor','pointer')
      .on('click',()=>window.location.href='/tasks/'+n.id)
      .on('mouseover',e=>{{
        tip(`<b>${{n.title}}</b><br>🔵 状態: ${{n.status}}<br>👤 担当: ${{n.owner||'—'}}<br>📁 section: ${{n.section}}<br>${{n.agent_flow?'🔗 '+n.agent_flow:''}}${{n.created_at?'<br>🕐 '+n.created_at.substring(0,19).replace('T',' '):''}}`);
        tipMove(e);
      }})
      .on('mousemove',tipMove).on('mouseout',tipHide);

    // card background
    const CW=COL_W-20, CH=52;
    g.append('rect').attr('x',-CW/2).attr('y',-CH/2).attr('width',CW).attr('height',CH)
      .attr('rx',6).attr('fill',SC[n.status]||'#eee')
      .attr('stroke',n.status==='needs_human'?'#c00':'#aaa').attr('stroke-width',1.2);

    // status dot
    g.append('circle').attr('cx',-CW/2+12).attr('cy',0).attr('r',6)
      .attr('fill',SC[n.status]||'#ccc').attr('stroke','#666').attr('stroke-width',1);

    // title
    const maxT=Math.floor((CW-30)/7);
    g.append('text').attr('x',-CW/2+24).attr('dy','-0.3em')
      .style('font-size','11px').style('font-weight','bold').style('fill','#222')
      .text(n.title.length>maxT?n.title.substring(0,maxT)+'…':n.title);

    // agent chain
    if(n.agent_flow){{
      g.append('text').attr('x',-CW/2+24).attr('dy','1em')
        .style('font-size','9px').style('fill','#556')
        .text(n.agent_flow.length>38?n.agent_flow.substring(0,38)+'…':n.agent_flow);
    }} else if(n.owner){{
      g.append('text').attr('x',-CW/2+24).attr('dy','1em')
        .style('font-size','9px').style('fill','#667').text('👤 '+n.owner);
    }}
  }});

  // legend
  const lg=svg.append('g').attr('transform',`translate(2,${{H-Object.keys(SC).length*16-4}})`);
  Object.entries(SC).forEach(([s,c],i)=>{{
    lg.append('rect').attr('x',0).attr('y',i*16).attr('width',11).attr('height',11).attr('fill',c).attr('stroke','#888').attr('rx',2);
    lg.append('text').attr('x',14).attr('y',i*16+9).style('font-size','10px').style('fill','#444').text(s);
  }});
}})();

// ════════════════════════════════════════
//  依存グラフ（補助ビュー）
// ════════════════════════════════════════
(function drawGraph(){{
  const svg=d3.select('#graph-svg');
  const W=svg.node().getBoundingClientRect().width||900, H=640;
  svg.attr('viewBox',`0 0 ${{W}} ${{H}}`);
  if(!DATA.nodes.length){{ svg.append('text').attr('x',W/2).attr('y',H/2).attr('text-anchor','middle').style('fill','#bbb').text('タスクなし'); return; }}

  svg.append('defs').append('marker').attr('id','arr-g')
    .attr('viewBox','0 -4 8 8').attr('refX',22).attr('markerWidth',5).attr('markerHeight',5).attr('orient','auto')
    .append('path').attr('d','M0,-4L8,0L0,4').attr('fill','#888');

  const tMin=Math.min(...DATA.nodes.map(n=>+new Date(n.created_at||0)));
  const tMax=Math.max(...DATA.nodes.map(n=>+new Date(n.created_at||0)));
  const tR=tMax-tMin||1;

  const sim=d3.forceSimulation(DATA.nodes)
    .force('link',d3.forceLink(DATA.edges).id(d=>d.id).distance(150).strength(0.9))
    .force('charge',d3.forceManyBody().strength(-320))
    .force('center',d3.forceCenter(W/2,H/2))
    .force('y',d3.forceY(d=>60+(+new Date(d.created_at||0)-tMin)/tR*(H-120)).strength(0.2))
    .force('collision',d3.forceCollide(38));

  const link=svg.append('g').selectAll('line').data(DATA.edges).join('line')
    .attr('stroke',d=>d.type==='spawn'?'#d05000':'#8888cc').attr('stroke-width',2)
    .attr('marker-end','url(#arr-g)').attr('opacity',.75);

  // time axis
  const tScale2=d3.scaleTime().domain([new Date(tMin),new Date(tMax)]).range([60,H-60]);
  svg.append('g').attr('transform','translate(58,0)')
    .call(d3.axisLeft(tScale2).ticks(5).tickFormat(d3.timeFormat('%m/%d %H:%M'))).style('font-size','9px');

  const node=svg.append('g').selectAll('g').data(DATA.nodes).join('g').attr('cursor','pointer')
    .call(d3.drag()
      .on('start',(e,d)=>{{if(!e.active)sim.alphaTarget(.3).restart();d.fx=d.x;d.fy=d.y}})
      .on('drag',(e,d)=>{{d.fx=e.x;d.fy=e.y}})
      .on('end',(e,d)=>{{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null}}))
    .on('click',(e,d)=>window.location.href='/tasks/'+d.id)
    .on('mouseover',(e,d)=>{{
      tip(`<b>${{d.title}}</b><br>🔵 ${{d.status}}<br>👤 ${{d.owner||'—'}}<br>📁 ${{d.section}}<br>${{d.agent_flow?'🔗 '+d.agent_flow:''}}`);
      tipMove(e);
    }}).on('mousemove',tipMove).on('mouseout',tipHide);

  const CW=160, CH=44;
  node.append('rect').attr('x',-CW/2).attr('y',-CH/2).attr('width',CW).attr('height',CH)
    .attr('rx',6).attr('fill',d=>SC[d.status]||'#eee')
    .attr('stroke',d=>d.status==='needs_human'?'#c00':'#999').attr('stroke-width',1.3);
  node.append('text').attr('text-anchor','middle').attr('dy','-0.2em')
    .style('font-size','10px').style('font-weight','bold').style('pointer-events','none')
    .text(d=>d.title.length>18?d.title.substring(0,18)+'…':d.title);
  node.append('text').attr('text-anchor','middle').attr('dy','1em')
    .style('font-size','8.5px').style('fill','#556').style('pointer-events','none')
    .text(d=>d.status+(d.owner?' · '+d.owner:''));

  sim.on('tick',()=>{{
    link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y).attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
    node.attr('transform',d=>`translate(${{Math.max(90,Math.min(W-90,d.x))}},${{Math.max(30,Math.min(H-30,d.y))}})`);
  }});

  // legend
  const lg=svg.append('g').attr('transform',`translate(${{W-130}},10)`);
  Object.entries(SC).forEach(([s,c],i)=>{{
    lg.append('rect').attr('x',0).attr('y',i*17).attr('width',12).attr('height',12).attr('fill',c).attr('stroke','#888').attr('rx',2);
    lg.append('text').attr('x',16).attr('y',i*17+10).style('font-size','10px').text(s);
  }});
}})();
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


_TREE_CHARS = frozenset("├└│─")


def _render_description(text: str) -> str:
    """Markdown 風 description を構造化 HTML に変換する。

    ## 見出し・箇条書き・ツリー行・コードブロックを視覚的に区別。
    """
    if not text:
        return ""

    lines = text.splitlines()
    parts: list[str] = []
    tree_buf: list[str] = []
    code_buf: list[str] = []
    in_code = False

    def flush_tree() -> None:
        if not tree_buf:
            return
        block = html.escape("\n".join(tree_buf))
        parts.append(
            f"<pre style='margin:.4em 0;padding:.5em .8em;background:#f0f4f8;"
            f"border-left:3px solid #7090b0;border-radius:.3em;"
            f"font-family:monospace;font-size:.88em;overflow-x:auto'>{block}</pre>"
        )
        tree_buf.clear()

    def flush_code(lang: str = "") -> None:
        if not code_buf:
            return
        block = html.escape("\n".join(code_buf))
        label = html.escape(lang) if lang else "code"
        parts.append(
            f"<pre style='margin:.4em 0;padding:.5em .8em;background:#1e1e2e;color:#cdd6f4;"
            f"border-radius:.3em;font-family:monospace;font-size:.85em;overflow-x:auto'>"
            f"<span style='color:#6c7086;font-size:.8em'>{label}</span>\n{block}</pre>"
        )
        code_buf.clear()

    code_lang = ""
    for line in lines:
        # ── コードブロック ──
        if line.startswith("```"):
            if in_code:
                flush_code(code_lang)
                in_code = False
                code_lang = ""
            else:
                flush_tree()
                in_code = True
                code_lang = line[3:].strip()
            continue
        if in_code:
            code_buf.append(line)
            continue

        # ── ツリー行（├ └ │ ─ を含む行）：連続行をひとつの <pre> にまとめる ──
        if any(c in line for c in _TREE_CHARS):
            tree_buf.append(line)
            continue
        flush_tree()  # 非ツリー行に切り替わったときだけフラッシュ

        # ── 見出し ──
        if line.startswith("### "):
            h = html.escape(line[4:])
            parts.append(f"<h4 style='margin:.7em 0 .2em;color:#334;font-size:1em'>{h}</h4>")
        elif line.startswith("## "):
            h = html.escape(line[3:])
            parts.append(
                f"<h3 style='margin:.9em 0 .3em;color:#223;font-size:1.05em;"
                f"border-bottom:1px solid #ccd;padding-bottom:.2em'>{h}</h3>"
            )
        elif line.startswith("# "):
            h = html.escape(line[2:])
            parts.append(f"<h2 style='margin:1em 0 .3em;color:#112;font-size:1.1em'>{h}</h2>")

        # ── チェックリスト ──
        elif line.lstrip().startswith("- [ ] ") or line.lstrip().startswith("- [x] "):
            stripped = line.lstrip()
            checked = stripped[3] == 'x'
            content = html.escape(stripped[6:])
            box = "✅" if checked else "☐"
            indent = (len(line) - len(stripped)) // 2
            parts.append(
                f"<div style='margin:.15em 0;padding-left:{1.0 + indent}em'>{box} {content}</div>"
            )

        # ── 箇条書き ──
        elif line.lstrip().startswith("- "):
            stripped = line.lstrip()
            content = html.escape(stripped[2:])
            indent = (len(line) - len(stripped)) // 2
            parts.append(
                f"<div style='margin:.15em 0;padding-left:{1.0 + indent}em'>• {content}</div>"
            )

        # ── 区切り線 ──
        elif line.strip() in ("---", "===", "***"):
            parts.append("<hr style='border:none;border-top:1px solid #dde;margin:.6em 0'>")

        # ── 空行 ──
        elif line.strip() == "":
            parts.append("<div style='height:.4em'></div>")

        # ── 通常テキスト ──
        else:
            parts.append(f"<div style='margin:.1em 0'>{html.escape(line)}</div>")

    flush_tree()
    flush_code(code_lang)
    return "\n".join(parts)


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

    rows = []
    for k, v in task.items():
        if k in ("handoff_notes", "chain"):
            continue
        key_cell = html.escape(str(k))
        if k == "description" and isinstance(v, str) and v:
            rendered = _render_description(v)
            val_cell = (
                f"<div style='max-height:420px;overflow-y:auto;padding:.4em;"
                f"background:#fafafa;border-radius:.2em;font-size:.93em'>{rendered}</div>"
            )
        elif isinstance(v, (dict, list)):
            val_cell = html.escape(json.dumps(v, ensure_ascii=False))
        else:
            val_cell = html.escape(str(v))
        rows.append([key_cell, val_cell])

    body = _table(["Field", "Value"], rows)

    notes = task.get("handoff_notes", [])
    if notes:
        body += "<h2>Handoff Notes</h2><ol>"
        for n in notes:
            author_raw = n.get("author", "")
            if isinstance(author_raw, dict):
                author_raw = author_raw.get("id", str(author_raw))
            author_str = str(author_raw)
            author = html.escape(author_str)
            ts = html.escape(n.get("timestamp", n.get("created_at", "")))
            content = html.escape(n.get("body", n.get("content", "")))
            if author_str == "dashboard":
                body += (
                    f"<li style='background:#fff8dc;border-left:4px solid #f0c040;"
                    f"padding:.5em .8em;margin:.4em 0;list-style:none'>"
                    f"<strong>👤 人間操作</strong> <span style='color:#888;font-size:.85em'>({ts})</span><br>"
                    f"<pre style='margin:.4em 0;white-space:pre-wrap;word-break:break-word'>{content}</pre>"
                    f"</li>"
                )
            else:
                body += (
                    f"<li style='margin:.4em 0'>"
                    f"<strong>{author}</strong> <span style='color:#888;font-size:.85em'>({ts})</span><br>"
                    f"<pre style='white-space:pre-wrap;word-break:break-word'>{content}</pre>"
                    f"</li>"
                )
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
    is_ingester_review = "ingester-review" in task.get("tags", [])
    if is_ingester_review and task.get("status") == "needs_human":
        body += f"""
<h2>アクション（Vault ノート レビュー）</h2>
<p>このタスクは Vault から取り込まれたノートです。承認するとエージェントへ割り振られます。</p>
<form method='POST' action='/tasks/{html.escape(task_id)}/action' style='display:inline'>
  <input type='hidden' name='action' value='approve'>
  <button type='submit' style='background:#4a8;color:#fff;border:none;padding:.5em 1.5em;
    border-radius:.3em;cursor:pointer;font-size:1.05em'>✅ 承認（inbox へ）</button>
</form>
&nbsp;
<form method='POST' action='/tasks/{html.escape(task_id)}/action' style='display:inline'>
  <input type='hidden' name='action' value='reject'>
  <button type='submit' style='background:#c44;color:#fff;border:none;padding:.5em 1.5em;
    border-radius:.3em;cursor:pointer;font-size:1.05em'>❌ 却下（cancelled）</button>
</form>
<hr style='margin:1.5em 0'>
"""
    body += """
<h2>その他のアクション</h2>
<form method='POST' action='""" + f"/tasks/{task_id}/action" + """'>
<table style='width:auto;border:none'>
<tr><td style='border:none;padding:.4em'><strong>操作</strong></td><td style='border:none'>
  <label><input type='radio' name='action' value='note' checked> 📝 メモ追加（指示・コメント）</label><br>
  <label><input type='radio' name='action' value='retry'> 🔄 やり直し（inbox に戻す）</label><br>
  <label><input type='radio' name='action' value='reconsider'> 🔍 再検討（同エージェントで再処理）</label><br>
  <label><input type='radio' name='action' value='deep_dive'> 🔬 深堀り（詳細分析を要求）</label><br>
  <label><input type='radio' name='action' value='continue'> ▶️ 後続処理（次ステップへ進める）</label>
</td></tr>
<tr><td style='border:none;padding:.4em;vertical-align:top'><strong>指示内容</strong></td>
<td style='border:none'><textarea name='instruction' rows='6'
  style='width:640px;max-width:100%;resize:vertical;font-family:sans-serif;font-size:.95em;padding:.4em'
  placeholder='追加の指示・コメントを入力（任意）'></textarea></td></tr>
<tr><td style='border:none'></td><td style='border:none'>
  <button type='submit' style='padding:.4em 1.2em;font-size:1em'>実行</button>
</td></tr>
</table>
</form>
"""
    return _page(f"Task: {task_id[:16]}", body)


def render_review_queue(store: "TaskStore | None") -> str:
    """ingester-review タグの needs_human タスクを一覧表示するレビューキューページ。"""
    if store is None:
        return _page("レビューキュー", "<p>Store unavailable.</p>")

    tasks = []
    try:
        for t in store.list_by_status("needs_human"):
            if "ingester-review" in t.get("tags", []):
                tasks.append(t)
    except Exception as exc:
        return _page("レビューキュー", f"<p>Error: {html.escape(str(exc))}</p>")

    tasks.sort(key=lambda t: t.get("priority", 5))

    if not tasks:
        body = "<h2>📋 Vault ノート レビューキュー</h2><p>レビュー待ちのタスクはありません。</p>"
        return _page("レビューキュー", body)

    body = f"<h2>📋 Vault ノート レビューキュー（{len(tasks)} 件）</h2>"
    body += "<p>Vault から取り込まれたノートを確認して承認または却下してください。</p>"

    for t in tasks:
        tid = t["id"]
        title = html.escape(t.get("title", "(無題)"))
        section = html.escape(t.get("section", "-"))
        tags = ", ".join(html.escape(tag) for tag in t.get("tags", []) if tag != "ingester-review")
        desc = html.escape((t.get("description") or "")[:400])
        source = html.escape(t.get("source_note_id", "") or "")
        created_at = html.escape(t.get("created_at", "")[:19])

        body += f"""
<div style='border:1px solid #ccc;border-radius:.4em;padding:1em;margin:1em 0;background:#fff9f9'>
  <h3 style='margin:.2em 0'><a href='/tasks/{html.escape(tid)}'>{title}</a></h3>
  <p style='margin:.3em 0;color:#666'>
    section: <b>{section}</b> | tags: {tags or "—"} | 作成: {created_at}
    {f"| source: <code>{source[:40]}</code>" if source else ""}
  </p>
  <pre style='background:#f8f8f8;padding:.6em;border-radius:.3em;max-height:200px;overflow:auto;white-space:pre-wrap'>{desc}</pre>
  <form method='POST' action='/tasks/{html.escape(tid)}/action' style='display:inline'>
    <input type='hidden' name='action' value='approve'>
    <button type='submit' style='background:#4a8;color:#fff;border:none;padding:.4em 1.2em;
      border-radius:.3em;cursor:pointer;font-size:1em'>✅ 承認（inbox へ）</button>
  </form>
  &nbsp;
  <form method='POST' action='/tasks/{html.escape(tid)}/action' style='display:inline'>
    <input type='hidden' name='action' value='reject'>
    <button type='submit' style='background:#c44;color:#fff;border:none;padding:.4em 1.2em;
      border-radius:.3em;cursor:pointer;font-size:1em'>❌ 却下</button>
  </form>
  &nbsp;
  <a href='/tasks/{html.escape(tid)}' style='font-size:.9em'>詳細を見る →</a>
</div>"""

    return _page("レビューキュー", body)


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

        if path == "/review":
            content = render_review_queue(store)
            return 200, "text/html; charset=utf-8", content

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
                "approve": "✅ [承認]",
                "reject": "❌ [却下]",
            }.get(action, "📝 [手動指示]")

            note_body = f"{note_prefix} {instruction}" if instruction else note_prefix

            try:
                store.add_handoff_note(task_id, note_body, author="dashboard")

                if action == "approve":
                    # ingester-review タスクを inbox へ（F-08 人間承認フロー）
                    store.transition(task_id, "inbox", reason="human:approved")

                elif action == "reject":
                    store.transition(task_id, "cancelled", reason="human:rejected")

                elif action == "retry":
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

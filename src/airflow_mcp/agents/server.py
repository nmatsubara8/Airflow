"""agents-mcp: エージェント管理 MCP サーバー（DDD-02 §7）。

ツール: agents.list / agents.get / agents.find_for_task /
        agents.extract_features / agents.register
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server
import yaml

from ..common.auth import require_scope
from ..common.errors import AgentNotFound
from ..common.mcp import dispatch, mcp_result, run_stdio_server
from ..common.thresholds import AGENT_SECTION_MATCH_SCORE, AGENT_TAG_MATCH_SCORE

server = Server("agents-mcp")

_AGENTS_DIR = Path(os.environ.get("AIRFLOW_AGENTS_DIR", "agents"))
_agents: dict[str, dict[str, Any]] = {}


def _load_agents() -> None:
    """agents/ ディレクトリから skills.md を読み込む。"""
    if not _AGENTS_DIR.exists():
        return
    for skills_path in _AGENTS_DIR.rglob("skills.md"):
        try:
            content = skills_path.read_text(encoding="utf-8")
            # YAML フロントマターを解析
            if content.startswith("---"):
                parts = content.split("---", 2)
                if len(parts) >= 3:
                    meta = yaml.safe_load(parts[1]) or {}
                    meta["skills_md_path"] = str(skills_path)
                    meta["body"] = parts[2].strip()
                    agent_id = meta.get("agent_id")
                    if agent_id:
                        _agents[agent_id] = meta
        except Exception:
            pass


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="agents.list",
            description="登録済みエージェント一覧を返す",
            inputSchema={
                "type": "object",
                "properties": {
                    "section": {"type": "string"},
                    "platform": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="agents.get",
            description="ID 指定でエージェント情報と skills.md を取得",
            inputSchema={
                "type": "object",
                "required": ["agent_id"],
                "properties": {"agent_id": {"type": "string"}},
            },
        ),
        types.Tool(
            name="agents.find_for_task",
            description="タスク特徴から担当エージェント候補を返す",
            inputSchema={
                "type": "object",
                "required": ["task"],
                "properties": {"task": {"type": "object"}},
            },
        ),
        types.Tool(
            name="agents.extract_features",
            description="タスクから features を推定して返す",
            inputSchema={
                "type": "object",
                "required": ["task"],
                "properties": {"task": {"type": "object"}},
            },
        ),
        types.Tool(
            name="agents.register",
            description="エージェントを新規登録",
            inputSchema={
                "type": "object",
                "required": ["agent"],
                "properties": {"agent": {"type": "object"}},
            },
        ),
    ]


async def _agents_list(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "agents:read")
    results = list(_agents.values())
    if section := args.get("section"):
        results = [a for a in results if a.get("section") == section]
    if platform := args.get("platform"):
        results = [a for a in results if a.get("platform") == platform]
    # body は省略して概要のみ
    summary = [{k: v for k, v in a.items() if k != "body"} for a in results]
    return mcp_result({"agents": summary, "total": len(summary)})


async def _agents_get(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "agents:read")
    agent_id = args["agent_id"]
    if agent_id not in _agents:
        raise AgentNotFound(agent_id)
    return mcp_result(_agents[agent_id])


async def _agents_find_for_task(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "agents:read")
    task = args["task"]
    task_tags = set(task.get("tags", []))
    task_section = task.get("section", "")

    candidates = []
    for agent in _agents.values():
        score = 0.0
        agent_id = agent.get("agent_id", "")

        # セクション一致
        if agent.get("section") == task_section:
            score += AGENT_SECTION_MATCH_SCORE

        # skills ベースのタグマッチ（responsibilities キーワード）
        responsibilities = " ".join(agent.get("responsibilities", []) or []).lower()
        for tag in task_tags:
            if tag.lower() in responsibilities:
                score += AGENT_TAG_MATCH_SCORE

        if score > 0:
            candidates.append({
                "agent_id": agent_id,
                "score": round(score, 2),
                "reason": f"section={agent.get('section')}, tag_match={score > 0.1}",
            })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return mcp_result({"candidates": candidates[:5]})


async def _agents_extract_features(
    args: dict[str, Any], token: str | None
) -> list[types.TextContent]:
    require_scope(token, "agents:read")
    task = args["task"]

    # 既存 features があれば優先
    existing = task.get("features", {})
    inferred: list[str] = []

    features = dict(existing)
    if "task_type" not in features:
        tags = task.get("tags", [])
        if any(t in tags for t in ["review", "code-review"]):
            features["task_type"] = "review"
        elif any(t in tags for t in ["summarize", "summary"]):
            features["task_type"] = "summarize"
        elif any(t in tags for t in ["translate"]):
            features["task_type"] = "translate"
        else:
            features["task_type"] = "draft"
        inferred.append("task_type")

    if "required_accuracy" not in features:
        features["required_accuracy"] = "medium"
        inferred.append("required_accuracy")

    if "latency_sensitivity" not in features:
        features["latency_sensitivity"] = "batch"
        inferred.append("latency_sensitivity")

    if "language" not in features:
        title = task.get("title", "")
        # 簡易日本語検出
        features["language"] = "ja" if any(ord(c) > 0x3000 for c in title) else "en"
        inferred.append("language")

    return mcp_result({"features": features, "inferred_fields": inferred})


async def _agents_register(args: dict[str, Any], token: str | None) -> list[types.TextContent]:
    require_scope(token, "agents:write")
    agent = args["agent"]
    agent_id = agent.get("agent_id") or agent.get("id")
    if agent_id:
        _agents[agent_id] = agent
    return mcp_result({"registered": agent_id})


_HANDLERS = {
    "agents.list": _agents_list,
    "agents.get": _agents_get,
    "agents.find_for_task": _agents_find_for_task,
    "agents.extract_features": _agents_extract_features,
    "agents.register": _agents_register,
}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    return await dispatch(name, arguments, _HANDLERS, rate_limit=False)


async def run() -> None:
    await run_stdio_server(server, init_fn=_load_agents)

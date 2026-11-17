"""エンジニアリングディレクター週次ブリーフィング（DDD-08 §7.1）。

毎週月曜 09:00 にバッチスケジューラから呼ばれ、タスクボード全体をスキャンして
スプリント計画を生成し、 needs_human タスクとして人間に提示する。
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.audit import log_event, new_audit_id
from ..common.ollama import call_chat, is_stub_mode
from ..ops.notifier import notify_needs_human

logger = logging.getLogger(__name__)

_AGENT_ID = "agent_eng_director"
_SYSTEM_PROMPT_PATH = Path("prompts/system/eng-director.md")
_OUTPUT_SCHEMA_PATH = Path("prompts/output-schemas/sprint-plan.json")
_OLLAMA_TIMEOUT_S = float(os.environ.get("AIRFLOW_OLLAMA_TIMEOUT_S", "180"))


def _collect_stats(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    """タスクリストから統計情報を集計する。"""
    by_status: dict[str, int] = {}
    by_section: dict[str, int] = {}
    blocked: list[dict[str, Any]] = []
    needs_human: list[dict[str, Any]] = []
    in_progress_long: list[dict[str, Any]] = []
    now_ts = time.time()
    stale_threshold_s = 72 * 3600  # 72 時間

    for t in tasks:
        status = t.get("status", "unknown")
        section = t.get("section", "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        by_section[section] = by_section.get(section, 0) + 1

        if status == "blocked":
            blocked.append({"id": t["id"], "title": t.get("title", "")})
        if status == "needs_human":
            needs_human.append({"id": t["id"], "title": t.get("title", "")})
        if status == "in_progress":
            updated_at = t.get("updated_at", "")
            if updated_at:
                try:
                    from datetime import datetime as _dt
                    ts = _dt.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
                    if now_ts - ts > stale_threshold_s:
                        in_progress_long.append({"id": t["id"], "title": t.get("title", "")})
                except (ValueError, AttributeError):
                    pass

    return {
        "total": len(tasks),
        "by_status": by_status,
        "by_section": by_section,
        "blocked": blocked,
        "needs_human": needs_human,
        "in_progress_long": in_progress_long,
    }


def _build_context(tasks: list[dict[str, Any]], stats: dict[str, Any]) -> str:
    """LLM に渡すタスクコンテキスト文字列を組み立てる。"""
    lines = [
        "## タスクボード統計",
        f"- 総タスク数: {stats['total']}",
        f"- ステータス別: {json.dumps(stats['by_status'], ensure_ascii=False)}",
        f"- セクション別: {json.dumps(stats['by_section'], ensure_ascii=False)}",
        "",
        "## ブロッカー",
    ]
    for b in stats["blocked"]:
        lines.append(f"- [{b['id']}] {b['title']}")
    if not stats["blocked"]:
        lines.append("- なし")

    lines += ["", "## needs_human（人間承認待ち）"]
    for n in stats["needs_human"]:
        lines.append(f"- [{n['id']}] {n['title']}")
    if not stats["needs_human"]:
        lines.append("- なし")

    lines += ["", "## 長時間停滞中（72h以上）"]
    for s in stats["in_progress_long"]:
        lines.append(f"- [{s['id']}] {s['title']}")
    if not stats["in_progress_long"]:
        lines.append("- なし")

    # inbox / in_progress タスクの上位20件をリスト
    active_statuses = {"inbox", "in_progress", "blocked", "needs_human"}
    active = [t for t in tasks if t.get("status") in active_statuses]
    active.sort(key=lambda t: t.get("priority", 5))
    lines += ["", "## アクティブタスク（優先度順、上位20件）"]
    for t in active[:20]:
        lines.append(
            f"- [{t['id']}] {t.get('title', '(無題)')} "
            f"| status={t.get('status')} priority={t.get('priority', 5)} "
            f"owner={t.get('owner', '未割当')} section={t.get('section', '-')}"
        )

    return "\n".join(lines)


def _stub_plan(stats: dict[str, Any]) -> dict[str, Any]:
    """OLLAMA_STUB=true 時のスタブ応答。"""
    return {
        "week_goal": "[STUB] 今週はアクティブタスクの消化と品質向上に注力する。",
        "summary": (
            f"[STUB] 総タスク数 {stats['total']}。"
            f"ブロッカー {len(stats['blocked'])} 件。needs_human {len(stats['needs_human'])} 件。"
        ),
        "top_priorities": [
            {"task_id": "stub-001", "title": "スタブタスク1", "reason": "優先度高"},
        ],
        "agent_actions": {"agent_docs": "[STUB] ドキュメント更新を継続する。"},
        "blockers": [],
        "done_count_last_week": stats["by_status"].get("done", 0),
        "risks": ["[STUB] スタブモードのため実際の分析は実施されていない"],
    }


def _call_llm(
    ollama_url: str,
    tasks: list[dict[str, Any]],
    stats: dict[str, Any],
) -> dict[str, Any]:
    """LLM を呼び出してスプリント計画を生成する。"""
    if is_stub_mode():
        return _stub_plan(stats)

    try:
        output_schema = json.loads(_OUTPUT_SCHEMA_PATH.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        output_schema = {
            "type": "object",
            "required": ["week_goal", "summary", "top_priorities",
                         "agent_actions", "blockers", "risks"],
            "properties": {
                "week_goal": {"type": "string"},
                "summary":   {"type": "string"},
                "top_priorities": {"type": "array", "items": {"type": "object"}},
                "agent_actions": {"type": "object"},
                "blockers": {"type": "array", "items": {"type": "object"}},
                "done_count_last_week": {"type": "integer"},
                "risks": {"type": "array", "items": {"type": "string"}},
            },
        }

    system_prompt = (
        _SYSTEM_PROMPT_PATH.read_text()
        if _SYSTEM_PROMPT_PATH.exists()
        else "タスクボードを分析してスプリント計画をJSONで出力してください。"
    )
    context = _build_context(tasks, stats)
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"{context}\n\n---\n"
                "上記のタスクボード情報をもとに、今週のスプリント計画を JSON で出力してください。"
            ),
        },
    ]

    from ..common.io import load_model_registry
    model_tag = "qwen2.5:14b-instruct-q4_K_M"
    try:
        registry = load_model_registry(Path(".airflow/models/registry.json"))
        model_id = "model_qwen25_14b_q4"
        if model_id in registry:
            model_tag = registry[model_id].get("ollama_tag", model_tag)
    except Exception:
        pass

    payload: dict[str, Any] = {
        "model": model_tag,
        "messages": messages,
        "stream": False,
        "format": output_schema,
        "options": {
            "temperature": 0.3,
            "num_predict": 1024,
            "think": False,
        },
    }

    content = call_chat(ollama_url, payload, _OLLAMA_TIMEOUT_S)
    try:
        plan = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("eng_director: LLM output was not valid JSON, using fallback")
        plan = _stub_plan(stats)
        plan["summary"] = f"[LLM出力パースエラー] 生の出力: {content[:200]}"

    return plan


def _format_plan_description(plan: dict[str, Any], stats: dict[str, Any]) -> str:
    """スプリント計画を人間が読みやすい Markdown に変換する。"""
    top = plan.get("top_priorities", [])
    blockers = plan.get("blockers", [])
    risks = plan.get("risks", [])
    agent_actions = plan.get("agent_actions", {})

    lines = [
        f"## 今週のゴール\n{plan.get('week_goal', '')}",
        "",
        f"## 現状サマリー\n{plan.get('summary', '')}",
        "",
        "## 優先タスク（上位）",
    ]
    for i, p in enumerate(top, 1):
        lines.append(f"{i}. [{p.get('task_id', '?')}] {p.get('title', '')} — {p.get('reason', '')}")

    lines += ["", "## エージェント別アクション"]
    for agent, action in agent_actions.items():
        lines.append(f"- **{agent}**: {action}")

    if blockers:
        lines += ["", "## ブロッカー"]
        for b in blockers:
            lines.append(f"- {b.get('description', '')} → 対応: {b.get('action', '')}")

    if risks:
        lines += ["", "## リスク"]
        for r in risks:
            lines.append(f"- {r}")

    lines += [
        "",
        "---",
        f"*総タスク数: {stats['total']} | 先週完了: {plan.get('done_count_last_week', 0)} 件*",
        "",
        "**承認する場合は「承認」ボタンを押してください。修正が必要な場合はメモに追記してください。**",
    ]
    return "\n".join(lines)


def run_briefing(
    store: Any,
    ollama_url: str = "http://localhost:11434",
) -> dict[str, Any]:
    """週次ブリーフィングを実行し、needs_human タスクを作成して結果を返す。

    Returns:
        {"task_id": str, "stats": dict, "plan": dict}
    """
    aid = new_audit_id()
    log_event("info", "eng_director.started", _AGENT_ID, aid)

    all_tasks = list(store.list_all()) if hasattr(store, "list_all") else []
    if not all_tasks:
        # list_all がなければ全ステータスを順にフェッチ
        statuses = ["inbox", "in_progress", "done", "cancelled", "blocked", "needs_human"]
        for s in statuses:
            try:
                all_tasks.extend(store.list_by_status(s))
            except Exception:
                pass

    stats = _collect_stats(all_tasks)
    logger.info(
        "eng_director: scanned %d tasks blocked=%d needs_human=%d",
        stats["total"], len(stats["blocked"]), len(stats["needs_human"]),
    )

    plan = _call_llm(ollama_url, all_tasks, stats)

    description = _format_plan_description(plan, stats)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    task_data = {
        "title": f"[週次ブリーフィング {now_str}] スプリント計画 — 人間承認待ち",
        "description": description,
        "section": "engineering",
        "tags": ["sprint-plan", "weekly", "eng-director", "needs_human"],
        "owner": _AGENT_ID,
        "priority": 2,
        "status": "needs_human",
        "metadata": {
            "plan": plan,
            "stats": stats,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    task = store.create(task_data)
    task_id = task["id"]

    notify_needs_human(
        task_id,
        task_data["title"],
        "engineering",
        _AGENT_ID,
        "週次スプリント計画の確認・承認をお願いします",
    )

    log_event(
        "info", "eng_director.completed", _AGENT_ID, aid,
        task_id=task_id,
        data={"total_tasks": stats["total"], "blockers": len(stats["blocked"])},
    )
    logger.info("eng_director: created needs_human task %s", task_id)

    return {"task_id": task_id, "stats": stats, "plan": plan}

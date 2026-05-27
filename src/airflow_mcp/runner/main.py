"""エージェント実行ループ（DDD-05 §9）。

in_progress タスクをポーリングし、Ollama 経由で処理して done に遷移。

Flow per DDD-05 §9 / DDD-06 §4:
  1. in_progress タスクを取得（自分が owner のもの）
  2. KNN 推奨 or コールドスタートフォールバックでモデル選択（DDD-06）
  3. system prompt + タスク内容でメッセージ構築
  4. models-mcp model.execute（構造化出力強制）
  5. schema_valid チェック、失敗時リトライ最大 3 回
  6. auto_score 算出 → ExecutionLog 記録
  7. handoff_note に parsed.summary を格納
  8. tasks.transition(done)  or  tasks.transition(needs_human) on final failure
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft202012Validator

from ..common.audit import log_event, new_audit_id
from ..ops.notifier import notify_needs_human
from ..common.io import load_model_registry, safe_yaml_load
from ..common.ollama import call_chat, is_stub_mode
from ..common.thresholds import REROUTE_SCORE_THRESHOLD
from ..common.url_validator import validate_ollama_url
from ..orchestrator.execution_log import append_execution_log, compute_auto_score, load_history
from ..orchestrator.handoff_chain import (
    load_chains,
    record_chain_step,
    route_to_next_agent,
    spawn_with_join_tracking,
)
from ..orchestrator.recommender import select_model, simple_rule_select
from ..taskboard.store import TaskStore
from .context_fetcher import fetch_relevant_context

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_PATH = Path("prompts/system/document-summary.md")
_OUTPUT_SCHEMA_PATH = Path("prompts/output-schemas/summary.json")
_NOTES_INDEX_PATH = Path(os.environ.get("AIRFLOW_NOTES_INDEX", ".airflow/notes/index.json"))
_MAX_RETRY = 3
_OLLAMA_TIMEOUT_S = float(os.environ.get("AIRFLOW_OLLAMA_TIMEOUT_S", "180"))


def load_model_config(path: str = ".airflow/config/model-config.yaml") -> dict[str, Any]:
    return safe_yaml_load(Path(path))


def _load_registry(path: str = ".airflow/models/registry.json") -> dict[str, dict]:
    return load_model_registry(Path(path))


def _select_model(
    task: dict[str, Any],
    model_config: dict[str, Any],
    registry: dict[str, dict] | None = None,
    history: list[dict[str, Any]] | None = None,
    *,
    loaded_model_ids: list[str] | None = None,
    free_vram_gb: float = 0.0,
    total_vram_gb: float | None = None,
) -> tuple[str, str | None]:
    """KNN 推奨でモデルを選択（DDD-06 §4）。履歴がなければ固定ルールにフォールバック。

    Returns:
        (model_id, exploration_type)
    """
    if registry and model_config:
        hist = history if history is not None else load_history()
        return select_model(task, model_config, registry, hist,
                            loaded_model_ids=loaded_model_ids or [],
                            free_vram_gb=free_vram_gb,
                            total_vram_gb=total_vram_gb)
    # registry / config なし → Phase 2 固定ルール
    return simple_rule_select(task), None


def _model_id_to_ollama_tag(model_id: str, registry: dict[str, dict]) -> str:
    if model_id in registry:
        return registry[model_id].get("ollama_tag", model_id)
    # フォールバック
    return model_id.replace("model_", "").replace("_", ":")


def _extract_human_instructions(task: dict[str, Any]) -> list[str]:
    """handoff_notes から未処理の人間指示（dashboard author）を抽出する。"""
    instructions = []
    for hn in task.get("handoff_notes", []):
        author = hn.get("author", "")
        if isinstance(author, dict):
            author = author.get("id", "")
        if str(author) == "dashboard":
            body = hn.get("body", "")
            # description 更新系・システム操作系ノートは除外
            if not any(body.startswith(p) for p in ("✏️", "📥", "📝 [手動指示]")):
                instructions.append(body)
            elif body.startswith("📝 [手動指示]"):
                instructions.append(body)
    return instructions


def _build_messages(
    task: dict[str, Any],
    system_prompt: str,
    context: str = "",
    no_think_prefix: bool = False,
) -> list[dict[str, Any]]:
    """DDD-05 §9 の prompt 構築。context が与えられた場合はユーザーメッセージに付加する。"""
    context_block = f"\n\n{context}" if context else ""
    tags_str = ", ".join(task.get("tags", [])) or "（なし）"
    prefix = "/no-think\n\n" if no_think_prefix else ""

    # 人間からの追加指示をプロンプトに含める
    human_instructions = _extract_human_instructions(task)
    instructions_block = ""
    if human_instructions:
        instructions_text = "\n".join(f"- {i}" for i in human_instructions)
        instructions_block = (
            f"\n\n## 人間からの追加指示\n{instructions_text}\n\n"
            f"上記の指示を反映した updated_description を必ず記述してください。"
        )

    user_content = (
        f"{prefix}"
        f"## 分析対象タスク\n\n"
        f"**タイトル**: {task['title']}\n\n"
        f"**説明**: {task.get('description', '（説明なし）')}\n\n"
        f"**タグ**: {tags_str}\n"
        f"**セクション**: {task.get('section', 'engineering')}"
        f"{instructions_block}"
        f"{context_block}\n\n"
        f"---\n"
        f"上記のタスクを分析し、具体的な洞察・実装方針・注意点を含む JSON を出力してください。"
        f" key_points には必ず3点以上の具体的な指摘を記載してください。"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]


def _call_ollama(
    ollama_url: str,
    model_tag: str,
    messages: list[dict],
    output_schema: dict,
    temperature: float,
    max_tokens: int,
    retry_attempt: int,
) -> tuple[str, dict | None, bool]:
    """
    Ollama API 呼び出し + structured output 検証（DDD-05 §6）。
    OLLAMA_STUB=true の場合はスタブ応答。
    """
    if is_stub_mode():
        user_preview = messages[1]["content"][:30] if len(messages) > 1 else ""
        stub = {
            "summary": f"[STUB] タスク '{user_preview}...' の要約",
            "key_points": ["スタブ応答 1", "スタブ応答 2", "スタブ応答 3"],
            "confidence": 0.99,
            "suggested_tags": ["stub"],
        }
        return json.dumps(stub, ensure_ascii=False), stub, True

    payload: dict[str, Any] = {
        "model": model_tag,
        "messages": messages,
        "stream": False,
        "format": output_schema,
        "options": {
            "temperature": temperature + retry_attempt * 0.1,  # retry ごとに温度を上げる
            "num_predict": max_tokens,
            "think": False,  # Qwen3 thinking mode conflicts with structured output (format:)
        },
    }
    content = call_chat(ollama_url, payload, _OLLAMA_TIMEOUT_S)

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return content, None, False

    errors = list(Draft202012Validator(output_schema).iter_errors(parsed))
    if errors:
        return content, parsed, False

    return content, parsed, True


def _execute_with_retry(
    ollama_url: str,
    model_tag: str,
    messages: list[dict],
    output_schema: dict,
    temperature: float = 0.3,
    max_tokens: int = 512,
) -> tuple[str, dict | None, bool, int]:
    """モデルを最大 _MAX_RETRY 回試行する。(content, parsed, schema_valid, retry_count)。"""
    # attempt をまたいで前回の失敗値が残らないよう最終結果用変数をここで宣言
    content = ""
    parsed: dict | None = None
    schema_valid = False
    retry_count = 0
    for attempt in range(_MAX_RETRY):
        retry_count = attempt
        # attempt ごとに状態をリセットして前回の部分的な結果を引き継がない
        content = ""
        parsed = None
        schema_valid = False
        try:
            content, parsed, schema_valid = _call_ollama(
                ollama_url, model_tag, messages, output_schema, temperature, max_tokens, attempt
            )
            if schema_valid:
                break
        except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
            logger.error("ollama call failed attempt %d: %s", attempt + 1, exc)
            if attempt == _MAX_RETRY - 1:
                break
        if not schema_valid:
            logger.warning("schema invalid attempt %d/%d", attempt + 1, _MAX_RETRY)
        time.sleep(0.5)
    return content, parsed, schema_valid, retry_count


def _get_fallback_model(model_id: str, model_config: dict[str, Any]) -> str | None:
    """reroute.fallback_chain から次モデルを返す（DDD-06 §7.3）。"""
    chain = model_config.get("reroute", {}).get("fallback_chain", {})
    fallbacks = chain.get(model_id, [])
    return fallbacks[0] if fallbacks else None


@dataclass
class ExecutionContext:
    """runner の実行コンテキスト（store + 設定一式）。"""
    store: "TaskStore"
    model_config: dict[str, Any]
    registry: dict[str, dict]
    agent_id: str
    ollama_url: str


@dataclass
class _ExecResult:
    model_id: str
    content: str
    parsed: dict | None
    schema_valid: bool
    retry_count: int
    latency_ms: int
    log_id: str | None
    rerouted_from: str | None


def _execute_with_reroute(
    task: dict[str, Any],
    model_id: str,
    exploration_type: str | None,
    ctx: ExecutionContext,
    messages: list[dict],
    output_schema: dict,
    aid: str,
) -> _ExecResult:
    """プライマリ実行 + reroute を行い結果を返す（DDD-06 §7.3）。"""
    task_id = task["id"]
    model_config = ctx.model_config
    registry = ctx.registry
    agent_id = ctx.agent_id
    ollama_url = ctx.ollama_url
    max_reroute = model_config.get("reroute", {}).get("max_reroute_attempts", 1)

    model_tag = _model_id_to_ollama_tag(model_id, registry)
    start_ms = int(time.time() * 1000)
    content, parsed, schema_valid, retry_count = _execute_with_retry(
        ollama_url, model_tag, messages, output_schema
    )
    latency_ms = int(time.time() * 1000) - start_ms

    log_id: str | None = None
    try:
        exec_record = append_execution_log(
            task_id=task_id, agent_id=agent_id, model_id=model_id,
            features_snapshot=task.get("features", {}),
            schema_valid=schema_valid, retry_count=retry_count,
            latency_ms=latency_ms, exploration_type=exploration_type,
        )
        log_id = exec_record["id"]
    except Exception as exc:
        logger.warning("execution_log write failed: %s", exc)

    auto_score_primary = compute_auto_score(schema_valid, retry_count)
    features = task.get("features", {})
    skip_for: list[str] = model_config.get("reroute", {}).get("skip_for", [])
    should_reroute = (
        (not schema_valid or auto_score_primary < REROUTE_SCORE_THRESHOLD)
        and max_reroute > 0
    )

    if should_reroute and features.get("latency_sensitivity") in skip_for:
        should_reroute = False

    rerouted_from: str | None = None
    if should_reroute:
        fallback_id = _get_fallback_model(model_id, model_config)
        if fallback_id and fallback_id in registry:
            rerouted_from = model_id
            model_id = fallback_id
            model_tag = _model_id_to_ollama_tag(fallback_id, registry)
            reroute_start = int(time.time() * 1000)

            log_event("info", "model.rerouted", "runner", new_audit_id(),
                      task_id=task_id, agent_id=agent_id,
                      data={"from": rerouted_from, "to": fallback_id})
            logger.info("reroute %s → %s for %s", rerouted_from, fallback_id, task_id)

            content, parsed, schema_valid, retry_count = _execute_with_retry(
                ollama_url, model_tag, messages, output_schema
            )
            reroute_latency = int(time.time() * 1000) - reroute_start
            latency_ms += reroute_latency

            try:
                exec_record = append_execution_log(
                    task_id=task_id, agent_id=agent_id, model_id=fallback_id,
                    features_snapshot=task.get("features", {}),
                    schema_valid=schema_valid, retry_count=retry_count,
                    latency_ms=reroute_latency, exploration_type="reroute",
                )
                log_id = exec_record["id"]
            except Exception as exc:
                logger.warning("execution_log write failed (reroute): %s", exc)

    return _ExecResult(
        model_id=model_id,
        content=content,
        parsed=parsed,
        schema_valid=schema_valid,
        retry_count=retry_count,
        latency_ms=latency_ms,
        log_id=log_id,
        rerouted_from=rerouted_from,
    )


def _build_handoff_note(
    parsed: dict | None,
    content: str,
    model_id: str,
    latency_ms: int,
    retry_count: int,
    rerouted_from: str | None,
    log_id: str | None,
) -> str:
    """成功時の handoff_note 本文を組み立てる。"""
    summary = parsed.get("summary", "") if parsed else content[:200]
    key_points = parsed.get("key_points", []) if parsed else []
    confidence = parsed.get("confidence", 0.0) if parsed else 0.0
    reroute_note = f" | **reroute**: {rerouted_from}→{model_id}" if rerouted_from else ""
    log_note = f" | **実行ログ**: {log_id}" if log_id else ""
    return (
        f"**要約**: {summary}\n\n"
        f"**重要ポイント**:\n" + "\n".join(f"- {p}" for p in key_points) + "\n\n"
        f"**確信度**: {confidence:.2f} | **モデル**: {model_id} | "
        f"**レイテンシ**: {latency_ms}ms | **リトライ**: {retry_count}回"
        f"{reroute_note}{log_note}"
    )


def process_task(task: dict[str, Any], ctx: ExecutionContext) -> bool:
    """
    タスク 1 件を処理する。成功時 True、最終失敗時 False。
    DDD-05 §9 + DDD-06 §7.3 の end-to-end フロー（reroute 含む）。
    """
    task_id = task["id"]
    aid = new_audit_id()

    system_prompt = (
        _SYSTEM_PROMPT_PATH.read_text() if _SYSTEM_PROMPT_PATH.exists()
        else "与えられたタスクを JSON 形式で要約してください。"
    )
    try:
        output_schema = json.loads(_OUTPUT_SCHEMA_PATH.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        output_schema = {"type": "object", "required": ["summary"],
                         "properties": {"summary": {"type": "string"}}}

    try:
        model_id, exploration_type = _select_model(task, ctx.model_config, ctx.registry)
    except Exception as exc:
        logger.error("model selection failed for task %s: %s", task_id, exc)
        ctx.store.add_handoff_note(task_id, f"[FAILED] モデル選択エラー: {exc}", author=ctx.agent_id)
        ctx.store.transition(task_id, "needs_human", reason="MODEL_SELECTION_ERROR")
        notify_needs_human(task_id, task.get("title", ""), task.get("section", ""),
                           ctx.agent_id, f"MODEL_SELECTION_ERROR: {exc}")
        return False
    logger.info(
        "runner.model_selected task=%s model=%s exploration=%s",
        task_id, model_id, exploration_type,
    )
    context = fetch_relevant_context(task, _NOTES_INDEX_PATH)
    no_think = ctx.registry.get(model_id, {}).get("no_think_prefix", False)
    messages = _build_messages(task, system_prompt, context, no_think_prefix=no_think)

    r = _execute_with_reroute(task, model_id, exploration_type, ctx, messages, output_schema, aid)

    log_event(
        "info" if r.schema_valid else "error",
        "model.executed" if r.schema_valid else "escalation.triggered",
        "models-mcp", aid,
        model_id=r.model_id, task_id=task_id, agent_id=ctx.agent_id,
        duration_ms=r.latency_ms,
        error=None if r.schema_valid else {
            "class": "escalate",
            "code": "MODEL_STRUCTURED_OUTPUT_INVALID",
            "message": f"{r.retry_count}回リトライ後も schema 不適合",
        },
        data={"retry_count": r.retry_count, "schema_valid": r.schema_valid,
              "exploration_type": exploration_type, "rerouted_from": r.rerouted_from},
    )

    if not r.schema_valid:
        note = (
            f"[FAILED] {r.retry_count}回リトライ後も構造化出力が schema に不適合。"
            + (f" reroute({r.rerouted_from}→{r.model_id})も失敗。" if r.rerouted_from else "")
            + f" 生の出力: {r.content[:200]}"
        )
        ctx.store.add_handoff_note(task_id, note, author=ctx.agent_id)
        ctx.store.transition(task_id, "needs_human",
                             reason="MODEL_STRUCTURED_OUTPUT_INVALID after retries")
        logger.error("task %s → needs_human (schema invalid)", task_id)
        notify_needs_human(task_id, task.get("title", ""), task.get("section", ""),
                           ctx.agent_id, f"MODEL_STRUCTURED_OUTPUT_INVALID (model={r.model_id})")
        return False

    handoff_content = _build_handoff_note(
        r.parsed, r.content, r.model_id, r.latency_ms, r.retry_count, r.rerouted_from, r.log_id,
    )
    ctx.store.add_handoff_note(task_id, handoff_content, author=ctx.agent_id)

    # 人間指示を反映した updated_description があれば description を自動更新する
    updated_desc = (r.parsed or {}).get("updated_description")
    if updated_desc and isinstance(updated_desc, str) and updated_desc.strip():
        try:
            fresh = ctx.store.get(task_id)
            ctx.store.update(task_id, fresh["lock"]["version"],
                             {"description": updated_desc.strip()})
            logger.info("task %s: description auto-updated from agent output", task_id)
        except Exception as exc:
            logger.warning("description auto-update failed for %s: %s", task_id, exc)

    # handoff チェーン処理（DDD-06 §3 / DDD-08 §3.1）
    # record_chain_step を先に実行してから chain 深さを元にルーティングを決定する。
    chains = load_chains()
    record_chain_step(task_id, ctx.agent_id, r.log_id, ctx.store)
    updated_task = ctx.store.get(task_id)

    next_agent = route_to_next_agent(updated_task, ctx.agent_id, ctx.store, chains)
    if next_agent:
        # 順次チェーン継続: done に遷移せずオーナーを次エージェントに渡す
        logger.info(
            "handoff.sequential task=%s → %s (chain_depth=%d)",
            task_id, next_agent, len(updated_task.get("chain", [])),
        )
        return True

    # チェーン末端 or 該当なし: done に遷移してファンアウトチェックを行う
    ctx.store.transition(task_id, "done", reason="agent completed successfully")
    logger.info("task %s → done (%.1fs, model=%s%s)", task_id, r.latency_ms / 1000, r.model_id,
                f" via reroute from {r.rerouted_from}" if r.rerouted_from else "")

    done_task = ctx.store.get(task_id)
    spawned = spawn_with_join_tracking(done_task, ctx.store, chains)
    if spawned:
        logger.info("handoff.spawned count=%d parent=%s", len(spawned), task_id)

    return True


def run_once(ctx: ExecutionContext) -> int:
    """in_progress で自分が owner のタスクを処理する。処理数を返す。"""
    tasks = [t for t in ctx.store.list_by_status("in_progress")
             if t.get("owner") == ctx.agent_id]
    processed = 0
    for task in tasks:
        try:
            process_task(task, ctx)
            processed += 1
        except Exception as exc:
            logger.error("runner error for %s: %s", task["id"], exc)
    return processed


def run_loop(ctx: ExecutionContext, poll_interval_s: float = 5.0) -> None:
    logger.info("runner started: agent=%s ollama=%s poll=%.1fs",
                ctx.agent_id, ctx.ollama_url, poll_interval_s)
    while True:
        try:
            n = run_once(ctx)
            if n > 0:
                logger.info("runner processed %d tasks", n)
        except Exception as exc:
            logger.error("runner loop error: %s", exc)
        time.sleep(poll_interval_s)


def _cli() -> None:
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    from ..taskboard.store import TaskStore
    ollama_url = validate_ollama_url(
        os.environ.get("OLLAMA_URL", "http://localhost:11434")
    )
    agent_id = os.environ.get("AIRFLOW_AGENT_ID", "agent_docs")
    ctx = ExecutionContext(
        store=TaskStore(),
        model_config=load_model_config(),
        registry=_load_registry(),
        agent_id=agent_id,
        ollama_url=ollama_url,
    )
    run_loop(ctx)


if __name__ == "__main__":  # pragma: no cover
    _cli()

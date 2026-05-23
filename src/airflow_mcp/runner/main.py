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
from pathlib import Path
from typing import Any

import httpx
import yaml
from jsonschema import Draft202012Validator

from ..common.audit import log_event, new_audit_id
from ..common.url_validator import validate_ollama_url
from ..orchestrator.execution_log import append_execution_log, load_history
from ..orchestrator.recommender import select_model, simple_rule_select
from ..taskboard.store import TaskStore

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_PATH = Path("prompts/system/document-summary.md")
_OUTPUT_SCHEMA_PATH = Path("prompts/output-schemas/summary.json")
_MAX_RETRY = 3


def load_model_config(path: str = ".airflow/config/model-config.yaml") -> dict[str, Any]:
    p = Path(path)
    if p.exists():
        return yaml.safe_load(p.read_text()) or {}
    return {}


def _load_registry(path: str = ".airflow/models/registry.json") -> dict[str, dict]:
    p = Path(path)
    if not p.exists():
        return {}
    data = json.loads(p.read_text())
    return {m["id"]: m for m in data.get("models", [])}


def _select_model(
    task: dict[str, Any],
    model_config: dict[str, Any],
    registry: dict[str, dict] | None = None,
    history: list[dict[str, Any]] | None = None,
) -> tuple[str, str | None]:
    """KNN 推奨でモデルを選択（DDD-06 §4）。履歴がなければ固定ルールにフォールバック。

    Returns:
        (model_id, exploration_type)
    """
    if registry and model_config:
        hist = history if history is not None else load_history()
        return select_model(task, model_config, registry, hist)
    # registry / config なし → Phase 2 固定ルール
    return simple_rule_select(task), None


def _model_id_to_ollama_tag(model_id: str, registry: dict[str, dict]) -> str:
    if model_id in registry:
        return registry[model_id].get("ollama_tag", model_id)
    # フォールバック
    return model_id.replace("model_", "").replace("_", ":")


def _build_messages(task: dict[str, Any], system_prompt: str) -> list[dict[str, Any]]:
    """DDD-05 §9 の prompt 構築。"""
    user_content = f"""## タスク: {task['title']}

{task.get('description', '')}

---
タグ: {', '.join(task.get('tags', []))}
セクション: {task.get('section', 'engineering')}
"""
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
    if os.environ.get("OLLAMA_STUB", "false").lower() == "true":
        stub = {
            "summary": f"[STUB] タスク '{messages[1]['content'][:30]}...' の要約",
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
        },
    }
    resp = httpx.post(f"{ollama_url}/api/chat", json=payload, timeout=180.0)
    resp.raise_for_status()
    content = resp.json()["message"]["content"]

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
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
    content = ""
    parsed: dict | None = None
    schema_valid = False
    retry_count = 0
    for attempt in range(_MAX_RETRY):
        retry_count = attempt
        try:
            content, parsed, schema_valid = _call_ollama(
                ollama_url, model_tag, messages, output_schema, temperature, max_tokens, attempt
            )
            if schema_valid:
                break
        except httpx.HTTPError as exc:
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


def process_task(
    task: dict[str, Any],
    store: TaskStore,
    model_config: dict[str, Any],
    registry: dict[str, dict],
    agent_id: str,
    ollama_url: str,
) -> bool:
    """
    タスク 1 件を処理する。成功時 True、最終失敗時 False。
    DDD-05 §9 + DDD-06 §7.3 の end-to-end フロー（reroute 含む）。
    """
    task_id = task["id"]
    aid = new_audit_id()
    max_reroute = model_config.get("reroute", {}).get("max_reroute_attempts", 1)

    # system prompt & output schema 読み込み
    system_prompt = (
        _SYSTEM_PROMPT_PATH.read_text() if _SYSTEM_PROMPT_PATH.exists()
        else "与えられたタスクを JSON 形式で要約してください。"
    )
    output_schema = (
        json.loads(_OUTPUT_SCHEMA_PATH.read_text()) if _OUTPUT_SCHEMA_PATH.exists()
        else {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    )

    # DDD-06 §4: KNN 推奨でモデルを選択
    model_id, exploration_type = _select_model(task, model_config, registry)
    messages = _build_messages(task, system_prompt)
    start_ms = int(time.time() * 1000)

    # ── プライマリ実行 ────────────────────────────────────
    model_tag = _model_id_to_ollama_tag(model_id, registry)
    content, parsed, schema_valid, retry_count = _execute_with_retry(
        ollama_url, model_tag, messages, output_schema
    )
    latency_ms = int(time.time() * 1000) - start_ms

    # DDD-06 §5: ExecutionLog 記録（プライマリ）
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
        log_id = "log_" + aid[:16].lower()

    # ── Reroute（DDD-06 §7.3）: プライマリ失敗時にフォールバックモデルで再試行 ──
    rerouted_from: str | None = None
    if not schema_valid and max_reroute > 0:
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

    log_event(
        "info" if schema_valid else "error",
        "model.executed" if schema_valid else "escalation.triggered",
        "models-mcp", aid,
        model_id=model_id, task_id=task_id, agent_id=agent_id,
        duration_ms=latency_ms,
        error=None if schema_valid else {
            "class": "escalate",
            "code": "MODEL_STRUCTURED_OUTPUT_INVALID",
            "message": f"{retry_count + 1}回リトライ後も schema 不適合",
        },
        data={"retry_count": retry_count, "schema_valid": schema_valid,
              "exploration_type": exploration_type,
              "rerouted_from": rerouted_from},
    )

    if not schema_valid:
        # 最終失敗 → needs_human に遷移（DDD-03 §3.4）
        note = (
            f"[FAILED] {retry_count + 1}回試行後も構造化出力が schema に不適合。"
            + (f" reroute({rerouted_from}→{model_id})も失敗。" if rerouted_from else "")
            + f" 生の出力: {content[:200]}"
        )
        store.add_handoff_note(task_id, note, author=agent_id)
        store.transition(task_id, "needs_human",
                         reason="MODEL_STRUCTURED_OUTPUT_INVALID after retries")
        logger.error("task %s → needs_human (schema invalid)", task_id)
        return False

    # 成功: handoff_note に summary を格納
    summary = parsed.get("summary", "") if parsed else content[:200]
    key_points = parsed.get("key_points", []) if parsed else []
    confidence = parsed.get("confidence", 0.0) if parsed else 0.0
    reroute_note = f" | **reroute**: {rerouted_from}→{model_id}" if rerouted_from else ""

    handoff_content = (
        f"**要約**: {summary}\n\n"
        f"**重要ポイント**:\n" + "\n".join(f"- {p}" for p in key_points) + "\n\n"
        f"**確信度**: {confidence:.2f} | **モデル**: {model_id} | "
        f"**レイテンシ**: {latency_ms}ms | **リトライ**: {retry_count}回"
        f"{reroute_note} | **実行ログ**: {log_id}"
    )
    store.add_handoff_note(task_id, handoff_content, author=agent_id)
    store.transition(task_id, "done", reason="agent completed successfully")

    logger.info("task %s → done (%.1fs, model=%s%s)", task_id, latency_ms / 1000, model_id,
                f" via reroute from {rerouted_from}" if rerouted_from else "")
    return True


def run_once(
    store: TaskStore,
    model_config: dict[str, Any],
    registry: dict[str, dict],
    agent_id: str,
    ollama_url: str,
) -> int:
    """in_progress で自分が owner のタスクを処理する。処理数を返す。"""
    tasks = [t for t in store.list_by_status("in_progress")
             if t.get("owner") == agent_id]
    processed = 0
    for task in tasks:
        try:
            process_task(task, store, model_config, registry, agent_id, ollama_url)
            processed += 1
        except Exception as exc:
            logger.error("runner error for %s: %s", task["id"], exc)
    return processed


def run_loop(
    store: TaskStore,
    model_config: dict[str, Any],
    registry: dict[str, dict],
    agent_id: str = "agent_docs",
    ollama_url: str | None = None,
    poll_interval_s: float = 5.0,
) -> None:
    url = validate_ollama_url(
        ollama_url or os.environ.get("OLLAMA_URL", "http://localhost:11434")
    )
    logger.info("runner started: agent=%s ollama=%s poll=%.1fs", agent_id, url, poll_interval_s)
    while True:
        try:
            n = run_once(store, model_config, registry, agent_id, url)
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
    store = TaskStore()
    model_config = load_model_config()
    registry = _load_registry()
    run_loop(store, model_config, registry)

"""タスク処理時に Vault / GitHub ノートから関連コンテキストを取得する。

Qdrant 未接続（Phase 2）のため、タイトル+本文へのキーワードスコアリングで代替。
上位 top_k 件を取得し、プロンプトに埋め込める文字列として返す。
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from ..ingester.notes_index import load_index

logger = logging.getLogger(__name__)

_DEFAULT_INDEX = Path(".airflow/notes/index.json")
_DEFAULT_TOP_K = 5
_DEFAULT_MAX_CHARS_PER_NOTE = 600
_DEFAULT_MAX_TOTAL_CHARS = 3000

# 要件定義タスクとみなす条件
_REQUIREMENTS_TAGS = {"requirements"}
_REQUIREMENTS_SECTIONS = {"backlog"}


def _should_fetch_context(task: dict[str, Any]) -> bool:
    """要件定義タスクかどうか判定する。"""
    tags = set(task.get("tags", []))
    section = task.get("section", "")
    return bool(tags & _REQUIREMENTS_TAGS) or section in _REQUIREMENTS_SECTIONS


def _score_note(note: dict[str, Any], words: list[str]) -> float:
    """タイトル + 本文への単語ヒット数でスコアを計算する。タイトルヒットは 3 倍重み。"""
    title = note.get("title", "").lower()
    body = note.get("body", "").lower()
    score = 0.0
    for w in words:
        if w in title:
            score += 3.0
        if w in body:
            score += 1.0
    return score


def _build_query_words(task: dict[str, Any]) -> list[str]:
    """タスクのタイトル・説明から検索ワードを抽出する。"""
    text = f"{task.get('title', '')} {task.get('description', '')}"
    words = re.findall(r"[a-zA-Zぁ-ん一-龯ァ-ン]{2,}", text.lower())
    # ストップワード除外（超短い / 極めて一般的）
    stop = {"the", "and", "for", "that", "this", "with", "from", "have",
            "タスク", "する", "こと", "ため", "など", "以下", "について"}
    return [w for w in words if w not in stop]


def search_notes(
    query_words: list[str],
    notes_index_path: Path = _DEFAULT_INDEX,
    top_k: int = _DEFAULT_TOP_K,
) -> list[dict[str, Any]]:
    """notes index からキーワードスコアで上位 top_k 件を返す。"""
    if not query_words:
        return []

    index = load_index(notes_index_path)
    scored: list[tuple[float, dict[str, Any]]] = []

    for note in index.get("notes", {}).values():
        if note.get("confidential"):
            continue
        score = _score_note(note, query_words)
        if score > 0:
            scored.append((score, note))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [note for _, note in scored[:top_k]]


def _source_label(note: dict[str, Any]) -> str:
    """ノートのソース種別を読みやすい形式で返す。"""
    fm = note.get("frontmatter", {})
    note_type = fm.get("type", "")
    if note_type == "github_reference":
        repo = fm.get("repo", "")
        branch = fm.get("branch", "main")
        path = fm.get("path", "")
        return f"GitHub: {repo}@{branch} / {path}"
    source = note.get("source_path", "")
    return f"Vault: {source}" if source else "Vault"


def format_context(notes: list[dict[str, Any]], max_chars_per_note: int = _DEFAULT_MAX_CHARS_PER_NOTE) -> str:
    """検索結果をプロンプト埋め込み用のテキストに整形する。"""
    if not notes:
        return ""

    lines = ["## 参考情報（Vault / GitHub）\n"]
    total = len(lines[0])
    for i, note in enumerate(notes, 1):
        title = note.get("title", "no title")
        label = _source_label(note)
        body_raw = note.get("body", "").strip()
        body_snip = body_raw[:max_chars_per_note]
        if len(body_raw) > max_chars_per_note:
            body_snip += "…"

        block = f"### [{i}] {title}\n出典: {label}\n\n{body_snip}\n\n"
        if total + len(block) > _DEFAULT_MAX_TOTAL_CHARS:
            break
        lines.append(block)
        total += len(block)

    return "".join(lines).rstrip()


def fetch_relevant_context(
    task: dict[str, Any],
    notes_index_path: Path = _DEFAULT_INDEX,
    top_k: int = _DEFAULT_TOP_K,
) -> str:
    """要件定義タスクの場合に関連ノートを検索して整形済みコンテキストを返す。

    要件定義タスクでない場合は空文字を返す（呼び出し側で条件分岐不要）。
    """
    if not _should_fetch_context(task):
        return ""

    words = _build_query_words(task)
    if not words:
        return ""

    try:
        notes = search_notes(words, notes_index_path, top_k=top_k)
    except Exception as exc:
        logger.warning("context_fetcher: notes search failed: %s", exc)
        return ""

    if not notes:
        logger.debug("context_fetcher: no relevant notes found for task %s", task.get("id"))
        return ""

    context = format_context(notes)
    logger.info(
        "context_fetcher: injecting %d notes (%d chars) for task %s",
        len(notes), len(context), task.get("id"),
    )
    return context

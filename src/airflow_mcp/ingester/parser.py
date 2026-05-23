"""Markdown フロントマター解析（DDD-04 §5.3）。"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import yaml


def parse_markdown(path: Path) -> dict[str, Any]:
    """
    .md ファイルを読み込み frontmatter + body を返す。

    Returns:
        {
          "title": str,
          "body": str,
          "frontmatter": dict,
          "content_hash": "sha256:...",
          "source_path": str,
        }
    """
    raw = path.read_text(encoding="utf-8")
    frontmatter: dict[str, Any] = {}
    body = raw

    # YAML フロントマター抽出
    if raw.startswith("---"):
        parts = raw.split("---", 2)
        if len(parts) >= 3:
            try:
                frontmatter = yaml.safe_load(parts[1]) or {}
            except yaml.YAMLError:
                frontmatter = {}
            body = parts[2].strip()

    # タイトル決定: frontmatter.title → 最初の H1 → ファイル名
    title = frontmatter.get("title") or _extract_h1(body) or path.stem

    # 本文正規化（改行・空白の標準化）
    normalized_body = re.sub(r"\n{3,}", "\n\n", body.strip())

    content_hash = "sha256:" + hashlib.sha256(normalized_body.encode()).hexdigest()

    return {
        "title": title,
        "body": normalized_body,
        "frontmatter": frontmatter,
        "content_hash": content_hash,
        "source_path": str(path),
    }


def _extract_h1(body: str) -> str | None:
    for line in body.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return None


def frontmatter_to_task_fields(parsed: dict[str, Any]) -> dict[str, Any]:
    """
    parse_markdown の結果をタスクフィールドに変換（DDD-04 §5.3）。

    frontmatter の既知フィールド:
      title, tags, priority, section, category, features,
      required_accuracy, latency_sensitivity, confidential
    """
    fm = parsed["frontmatter"]
    tags: list[str] = fm.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]

    # features の組み立て
    features: dict[str, Any] = dict(fm.get("features") or {})
    if "required_accuracy" in fm:
        features["required_accuracy"] = fm["required_accuracy"]
    if "latency_sensitivity" in fm:
        features["latency_sensitivity"] = fm["latency_sensitivity"]
    if "task_type" in fm:
        features["task_type"] = fm["task_type"]

    return {
        "title": parsed["title"],
        "description": parsed["body"][:500],  # 先頭 500 文字を description に
        "tags": tags,
        "priority": fm.get("priority", 5),
        "section": fm.get("section"),          # None の場合 dispatcher が決定
        "features": features,
        "source_note_id": None,                # ingester が後で設定
        "content_hash": parsed["content_hash"],
        "confidential": fm.get("confidential", False),
        "_raw_body": parsed["body"],
        "_source_path": parsed["source_path"],
    }

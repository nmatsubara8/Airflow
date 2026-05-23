"""タスク特徴抽出・推論ロジック（DDD-06 §3.2）。

agents.extract_features MCP ツールと orchestrator.infer_features が共通利用。
KNN エンコード定数（TASK_TYPES 等）も recommender に公開する。
"""
from __future__ import annotations

from typing import Any

# ── KNN エンコード定数（recommender.encode_features が参照） ──────
TASK_TYPES: list[str] = [
    "code_gen", "review", "summarize", "classify",
    "extract", "chat", "test", "document",
]
LANGUAGES: list[str] = ["ja", "en", "code", "mixed"]
ACCURACY_MAP: dict[str, float] = {"low": 0.0, "medium": 0.5, "high": 1.0}
LATENCY_MAP: dict[str, float] = {"none": 0.0, "batch": 0.5, "interactive": 1.0}

# ── 推論ルール ────────────────────────────────────────────────────
# タグ → task_type の別名（TASK_TYPES に存在しない表記を正規化）
_TAG_ALIASES: dict[str, str] = {
    "code-review": "review",
    "summary": "summarize",
}
_HIGH_ACCURACY_SECTIONS: frozenset[str] = frozenset({"business", "legal", "finance"})


def _infer_task_type(tags: list[str]) -> str:
    """タグリストから task_type を推論する。"""
    for tag in tags:
        resolved = _TAG_ALIASES.get(tag, tag)
        if resolved in TASK_TYPES:
            return resolved
    if "code" in tags:
        return "code_gen"
    if "translate" in tags:
        return "translate"
    return "draft"


def extract_features(task: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """タスクフィールドから不足特徴を推論し (features, inferred_fields) を返す。

    既存 features は上書きしない。推論元:
      - task_type          : tags（エイリアス含む）→ TASK_TYPES 照合 / "code" → "code_gen"
      - required_accuracy  : section が business/legal/finance → "high"、それ以外 "medium"
      - latency_sensitivity: tags に "urgent"/"interactive" → "interactive"、それ以外 "batch"
      - language           : title の Unicode コードポイントで日本語判定
    """
    features: dict[str, Any] = dict(task.get("features") or {})
    tags = [t.lower() for t in (task.get("tags") or [])]
    section = (task.get("section") or "").lower()
    title = task.get("title", "")
    inferred: list[str] = []

    if "task_type" not in features:
        features["task_type"] = _infer_task_type(tags)
        inferred.append("task_type")

    if "required_accuracy" not in features:
        features["required_accuracy"] = (
            "high" if section in _HIGH_ACCURACY_SECTIONS else "medium"
        )
        inferred.append("required_accuracy")

    if "latency_sensitivity" not in features:
        features["latency_sensitivity"] = (
            "interactive" if ("urgent" in tags or "interactive" in tags) else "batch"
        )
        inferred.append("latency_sensitivity")

    if "language" not in features:
        features["language"] = "ja" if any(ord(c) > 0x3000 for c in title) else "en"
        inferred.append("language")

    return features, inferred

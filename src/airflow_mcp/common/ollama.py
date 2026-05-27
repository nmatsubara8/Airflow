"""Ollama HTTP 呼び出しプリミティブ（runner / models 共用）。"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def is_stub_mode() -> bool:
    """OLLAMA_STUB=true のときは True を返す。"""
    return os.environ.get("OLLAMA_STUB", "false").lower() == "true"


def _extract_json(text: str) -> str:
    """テキストから最初の JSON オブジェクトを抽出する。"""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else text


def call_chat(base_url: str, payload: dict[str, Any], timeout_s: float) -> str:
    """POST {base_url}/api/chat を実行してレスポンス content 文字列を返す。

    content が空（thinking モデルが format: と競合）の場合、format: なしで再試行して
    JSON をテキストから抽出する。
    HTTP エラーは httpx.HTTPError / httpx.HTTPStatusError として伝播する。
    """
    resp = httpx.post(f"{base_url}/api/chat", json=payload, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    content = data.get("message", {}).get("content", "")

    if content:
        return content

    # content が空 → thinking モデルが応答を message.thinking に書いている
    thinking = data.get("message", {}).get("thinking", "")
    if thinking:
        extracted = _extract_json(thinking)
        if extracted != thinking:  # JSON が見つかった
            logger.info(
                "Extracted JSON from message.thinking (%d chars) — thinking model without content",
                len(thinking),
            )
            return extracted

    # format: を除いて再試行し、レスポンスから JSON を抽出する
    logger.warning(
        "Ollama returned empty content (thinking_len=%d) — retrying without format: constraint",
        len(thinking),
    )
    payload_no_fmt = {k: v for k, v in payload.items() if k != "format"}
    resp2 = httpx.post(f"{base_url}/api/chat", json=payload_no_fmt, timeout=timeout_s)
    resp2.raise_for_status()
    data2 = resp2.json()
    content2 = data2.get("message", {}).get("content", "")
    if content2:
        return _extract_json(content2)

    # format: なしでも content が空 → thinking フィールドを確認
    thinking2 = data2.get("message", {}).get("thinking", "")
    if thinking2:
        extracted2 = _extract_json(thinking2)
        if extracted2 != thinking2:
            logger.info("Extracted JSON from thinking (no-format retry), thinking_len=%d", len(thinking2))
            return extracted2

    raise ValueError(
        f"Ollama returned empty content even without format: constraint "
        f"(thinking_len={len(thinking2)}, response keys: {list(data2.keys())})"
    )

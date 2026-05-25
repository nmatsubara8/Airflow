"""Ollama HTTP 呼び出しプリミティブ（runner / models 共用）。"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def is_stub_mode() -> bool:
    """OLLAMA_STUB=true のときは True を返す。"""
    return os.environ.get("OLLAMA_STUB", "false").lower() == "true"


def call_chat(base_url: str, payload: dict[str, Any], timeout_s: float) -> str:
    """POST {base_url}/api/chat を実行してレスポンス content 文字列を返す。

    HTTP エラーは httpx.HTTPError / httpx.HTTPStatusError として伝播する。
    """
    resp = httpx.post(f"{base_url}/api/chat", json=payload, timeout=timeout_s)
    resp.raise_for_status()
    content = resp.json().get("message", {}).get("content", "")
    if not content:
        raise ValueError(
            f"Ollama returned empty content (response keys: {list(resp.json().keys())})"
        )
    return content

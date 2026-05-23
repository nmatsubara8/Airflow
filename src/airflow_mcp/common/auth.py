"""DDD-02 §2.2 / DDD-08 §4 に準拠した認証・認可。

Phase 2 MVP: AIRFLOW_MCP_TOKEN 環境変数による API Key 認証。
localhost_bypass が有効なためローカル stdio 運用では実質スキップ可能。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from .errors import AuthScopeInsufficient, AuthTokenInvalid, AuthTokenMissing

# Phase 2: ローカル stdio のため localhost_bypass=true が実質デフォルト
_LOCALHOST_BYPASS = os.environ.get("AIRFLOW_AUTH_LOCALHOST_BYPASS", "true").lower() == "true"

# tokens.json のパス（存在しない場合は bypass または環境変数のみ）
_TOKENS_FILE = Path(os.environ.get("AIRFLOW_TOKENS_FILE", ".airflow/auth/tokens.json"))


def _load_tokens() -> dict[str, dict]:
    """tokens.json をロード。ファイルが存在しない場合は空辞書。"""
    if _TOKENS_FILE.exists():
        return {t["token"]: t for t in json.loads(_TOKENS_FILE.read_text())["tokens"]}
    return {}


def require_scope(token_env: str | None, scope: str) -> None:
    """トークンを検証し、指定スコープを持つか確認する。

    Phase 2 MVP: localhost_bypass=true の場合はスキップ。
    """
    if _LOCALHOST_BYPASS:
        return

    if token_env is None:
        raise AuthTokenMissing()

    tokens = _load_tokens()
    if token_env not in tokens:
        # 環境変数から直接トークンを取得する場合
        env_token = os.environ.get("AIRFLOW_MCP_TOKEN")
        if env_token != token_env:
            raise AuthTokenInvalid()
        # 環境変数トークンはすべてのスコープを持つ（Phase 2 MVP 簡略化）
        return

    token_info = tokens[token_env]
    allowed: list[str] = token_info.get("scopes", [])
    if "*" in allowed or scope in allowed or f"{scope.split(':')[0]}:*" in allowed:
        return
    raise AuthScopeInsufficient(scope)


def get_token_from_env() -> str | None:
    """AIRFLOW_MCP_TOKEN 環境変数からトークンを取得。"""
    return os.environ.get("AIRFLOW_MCP_TOKEN")

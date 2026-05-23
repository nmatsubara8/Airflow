"""DDD-02 §2.2 / DDD-08 §4 に準拠した認証・認可。

Phase 2 MVP: AIRFLOW_MCP_TOKEN 環境変数による API Key 認証。
本番環境では AIRFLOW_AUTH_LOCALHOST_BYPASS=false を必ず設定すること。
"""
from __future__ import annotations

import hmac
import json
import logging
import os
import threading
from pathlib import Path

from .errors import AuthScopeInsufficient, AuthTokenInvalid, AuthTokenMissing

logger = logging.getLogger(__name__)

# 本番では必ず false に設定する（デフォルト false に変更: DDD-08 §4.1）
_LOCALHOST_BYPASS = os.environ.get("AIRFLOW_AUTH_LOCALHOST_BYPASS", "false").lower() == "true"

if _LOCALHOST_BYPASS:
    logger.warning(
        "AIRFLOW_AUTH_LOCALHOST_BYPASS=true: 認証が無効化されています。本番環境では使用しないこと。"
    )

# tokens.json のパス（存在しない場合は bypass または環境変数のみ）
_TOKENS_FILE = Path(os.environ.get("AIRFLOW_TOKENS_FILE", ".airflow/auth/tokens.json"))

# キャッシュ（None = 未ロード）と保護ロック
_token_cache: dict[str, dict] | None = None
_cache_lock = threading.RLock()


def _load_tokens_from_disk() -> dict[str, dict]:
    """tokens.json をディスクから読み込む。ファイル不在・破損時は空辞書。"""
    if not _TOKENS_FILE.exists():
        return {}
    try:
        return {t["token"]: t for t in json.loads(_TOKENS_FILE.read_text())["tokens"]}
    except Exception as exc:
        logger.error("tokens.json 読み込み失敗（fail-closed）: %s", exc)
        return {}


def _get_tokens() -> dict[str, dict]:
    """キャッシュからトークンを返す。未ロードなら初回ロードする（遅延初期化）。"""
    global _token_cache
    with _cache_lock:
        if _token_cache is None:
            _token_cache = _load_tokens_from_disk()
            logger.debug("tokens.json キャッシュロード: %d 件", len(_token_cache))
        return _token_cache


def reload_tokens() -> int:
    """tokens.json を再読み込みしてキャッシュを更新する。

    SIGHUP ハンドラやトークンローテーション後の管理操作から呼び出す。
    Returns: ロードしたトークン件数
    """
    global _token_cache
    with _cache_lock:
        _token_cache = _load_tokens_from_disk()
        count = len(_token_cache)
    logger.info("tokens.json 再ロード完了: %d 件", count)
    return count


def require_scope(token_env: str | None, scope: str) -> None:
    """トークンを検証し、指定スコープを持つか確認する。

    Phase 2 MVP: localhost_bypass=true の場合はスキップ。
    """
    if _LOCALHOST_BYPASS:
        return

    if token_env is None:
        raise AuthTokenMissing()

    tokens = _get_tokens()
    if token_env not in tokens:
        # 環境変数から直接トークンを取得する場合
        env_token = os.environ.get("AIRFLOW_MCP_TOKEN")
        if env_token is None or not hmac.compare_digest(env_token, token_env):
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

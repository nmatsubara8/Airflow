"""DDD-02 §2.2 / DDD-08 §4 に準拠した認証・認可。

Phase 2 MVP: AIRFLOW_MCP_TOKEN 環境変数による API Key 認証。
DDD-08 §4.1: JWT EdDSA (Ed25519) 認証をサポート。
本番環境では AIRFLOW_AUTH_LOCALHOST_BYPASS=false を必ず設定すること。
"""
from __future__ import annotations

import hmac
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

# JWT キーパス・定数（DDD-08 §4.1）
_JWT_PUBLIC_KEY_PATH = Path(
    os.environ.get("AIRFLOW_JWT_PUBLIC_KEY", ".airflow/auth/ed25519.pub")
)
_JWT_PRIVATE_KEY_PATH = Path(
    os.environ.get("AIRFLOW_JWT_PRIVATE_KEY", ".airflow/auth/ed25519.pem")
)
_JWT_ISSUER = "airflow-local"
_JWT_ALGORITHM = "EdDSA"

# キャッシュ（None = 未ロード）と保護ロック
_token_cache: dict[str, dict] | None = None
_token_cache_mtime: float = 0.0  # tokens.json の最終更新時刻
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
    """キャッシュからトークンを返す。tokens.json が更新されていれば自動再ロード。"""
    global _token_cache, _token_cache_mtime
    with _cache_lock:
        try:
            current_mtime = _TOKENS_FILE.stat().st_mtime if _TOKENS_FILE.exists() else 0.0
        except OSError:
            current_mtime = 0.0
        if _token_cache is None or current_mtime != _token_cache_mtime:
            _token_cache = _load_tokens_from_disk()
            _token_cache_mtime = current_mtime
            logger.debug("tokens.json キャッシュロード: %d 件", len(_token_cache))
        return _token_cache


def reload_tokens() -> int:
    """tokens.json を再読み込みしてキャッシュを更新する。

    SIGHUP ハンドラやトークンローテーション後の管理操作から呼び出す。
    Returns: ロードしたトークン件数
    """
    global _token_cache, _token_cache_mtime
    with _cache_lock:
        _token_cache = _load_tokens_from_disk()
        _token_cache_mtime = _TOKENS_FILE.stat().st_mtime if _TOKENS_FILE.exists() else 0.0
        count = len(_token_cache)
    logger.info("tokens.json 再ロード完了: %d 件", count)
    return count


# ── JWT 関連（DDD-08 §4.1）──────────────────────────────────────────────────


def _is_jwt(token: str) -> bool:
    """JWT 形式（header.payload.signature）かどうかを判定する。"""
    return token.count(".") == 2


def verify_jwt(token: str, public_key_path: Path | None = None) -> dict[str, Any]:
    """JWT (EdDSA/Ed25519) を検証して claims を返す。

    Args:
        token: JWT 文字列
        public_key_path: 公開鍵 PEM ファイルパス（省略時は _JWT_PUBLIC_KEY_PATH）

    Returns:
        検証済み JWT claims dict

    Raises:
        AuthTokenInvalid: 署名不正・有効期限切れ・issuer 不一致
    """
    try:
        import jwt as pyjwt
    except ImportError as exc:
        raise AuthTokenInvalid() from exc

    key_path = public_key_path or _JWT_PUBLIC_KEY_PATH
    if not key_path.exists():
        raise AuthTokenInvalid()

    try:
        public_key_pem = key_path.read_bytes()
        claims = pyjwt.decode(
            token,
            public_key_pem,
            algorithms=[_JWT_ALGORITHM],
            options={"require": ["exp", "iat", "sub", "iss"]},
        )
    except Exception as exc:
        logger.debug("JWT 検証失敗: %s", exc)
        raise AuthTokenInvalid() from exc

    if claims.get("iss") != _JWT_ISSUER:
        raise AuthTokenInvalid()

    return claims


def issue_jwt(
    sub: str,
    scopes: list[str],
    ttl_sec: int = 3600,
    private_key_path: Path | None = None,
) -> str:
    """JWT (EdDSA/Ed25519) を発行する。

    Args:
        sub: subject（エージェント ID 等）
        scopes: 付与するスコープリスト
        ttl_sec: 有効期間（秒）
        private_key_path: 秘密鍵 PEM ファイルパス（省略時は _JWT_PRIVATE_KEY_PATH）

    Returns:
        JWT 文字列

    Raises:
        AuthTokenInvalid: 秘密鍵が存在しない、またはエンコード失敗
    """
    try:
        import jwt as pyjwt
    except ImportError as exc:
        raise AuthTokenInvalid() from exc

    key_path = private_key_path or _JWT_PRIVATE_KEY_PATH
    if not key_path.exists():
        raise AuthTokenInvalid()

    try:
        private_key_pem = key_path.read_bytes()
        now = int(datetime.now(timezone.utc).timestamp())
        payload: dict[str, Any] = {
            "iss": _JWT_ISSUER,
            "sub": sub,
            "scopes": scopes,
            "iat": now,
            "exp": now + ttl_sec,
        }
        return pyjwt.encode(payload, private_key_pem, algorithm=_JWT_ALGORITHM)
    except Exception as exc:
        logger.debug("JWT 発行失敗: %s", exc)
        raise AuthTokenInvalid() from exc


def generate_keypair(key_dir: Path | None = None) -> tuple[Path, Path]:
    """Ed25519 キーペアを生成して PEM ファイルに保存する。

    Args:
        key_dir: 保存先ディレクトリ（省略時は .airflow/auth/）

    Returns:
        (private_key_path, public_key_path)
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    save_dir = key_dir or _JWT_PRIVATE_KEY_PATH.parent
    save_dir.mkdir(parents=True, exist_ok=True)

    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    priv_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    priv_path = save_dir / "ed25519.pem"
    pub_path = save_dir / "ed25519.pub"
    priv_path.write_bytes(priv_pem)
    pub_path.write_bytes(pub_pem)

    logger.info("Ed25519 キーペア生成完了: %s / %s", priv_path, pub_path)
    return priv_path, pub_path


# ── 認可 ────────────────────────────────────────────────────────────────────


def require_scope(token_env: str | None, scope: str) -> None:
    """トークンを検証し、指定スコープを持つか確認する。

    JWT（header.payload.signature 形式）であれば EdDSA 検証を行う。
    それ以外は API Key として tokens.json / 環境変数で検証する。
    """
    if _LOCALHOST_BYPASS:
        return

    if token_env is None:
        raise AuthTokenMissing()

    # JWT パス
    if _is_jwt(token_env):
        claims = verify_jwt(token_env)  # raises AuthTokenInvalid on failure
        allowed: list[str] = claims.get("scopes", [])
        if "*" in allowed or scope in allowed or f"{scope.split(':')[0]}:*" in allowed:
            return
        raise AuthScopeInsufficient(scope)

    # API Key パス
    tokens = _get_tokens()
    if token_env not in tokens:
        # 環境変数から直接トークンを取得する場合
        env_token = os.environ.get("AIRFLOW_MCP_TOKEN")
        if env_token is None or not hmac.compare_digest(env_token, token_env):
            raise AuthTokenInvalid()
        # 環境変数トークンはすべてのスコープを持つ（Phase 2 MVP 簡略化）
        return

    token_info = tokens[token_env]
    allowed = token_info.get("scopes", [])
    if "*" in allowed or scope in allowed or f"{scope.split(':')[0]}:*" in allowed:
        return
    raise AuthScopeInsufficient(scope)


def get_token_from_env() -> str | None:
    """AIRFLOW_MCP_TOKEN 環境変数からトークンを取得。"""
    return os.environ.get("AIRFLOW_MCP_TOKEN")


# ── トークン管理 CLI 向け操作（TF-2）────────────────────────────────────────


def _read_tokens_file() -> dict:
    """tokens.json を読んで {"tokens": [...]} dict を返す。ファイル不在時は空。"""
    if not _TOKENS_FILE.exists():
        return {"tokens": []}
    try:
        return json.loads(_TOKENS_FILE.read_text())
    except Exception as exc:
        logger.error("tokens.json 読み込み失敗: %s", exc)
        return {"tokens": []}


def _write_tokens_file(data: dict) -> None:
    """tokens.json にアトミック書き込みしてキャッシュを無効化する。"""
    _TOKENS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _TOKENS_FILE.with_suffix(".json.new")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    tmp.replace(_TOKENS_FILE)
    # キャッシュを強制無効化
    global _token_cache, _token_cache_mtime
    with _cache_lock:
        _token_cache = None
        _token_cache_mtime = 0.0


def list_tokens() -> list[dict]:
    """登録済みトークンの一覧を返す（トークン値は伏せる）。

    Returns:
        [{"agent_id": str, "scopes": list, "created_at": str}, ...]
    """
    data = _read_tokens_file()
    result = []
    for entry in data.get("tokens", []):
        result.append({
            "agent_id": entry.get("agent_id", "<unknown>"),
            "scopes": entry.get("scopes", []),
            "created_at": entry.get("created_at", ""),
        })
    return result


def revoke_token(agent_id: str) -> bool:
    """指定 agent_id のトークンを失効させる。

    Returns:
        True = 削除成功、False = 対象トークンが存在しなかった
    """
    data = _read_tokens_file()
    original = data.get("tokens", [])
    updated = [t for t in original if t.get("agent_id") != agent_id]
    if len(updated) == len(original):
        return False
    data["tokens"] = updated
    _write_tokens_file(data)
    logger.info("auth.revoke agent_id=%s (removed %d token(s))", agent_id,
                len(original) - len(updated))
    return True


def rotate_token(agent_id: str, scopes: list[str], ttl_sec: int = 3600) -> str:
    """旧トークンを失効させて新しい JWT トークンを発行する。

    Args:
        agent_id: 対象エージェント ID
        scopes: 新トークンに付与するスコープリスト
        ttl_sec: 有効期間（秒）

    Returns:
        新しく発行した JWT 文字列
    """
    revoke_token(agent_id)  # 旧トークンを削除（存在しなくても OK）
    new_token = issue_jwt(sub=agent_id, scopes=scopes, ttl_sec=ttl_sec)
    logger.info("auth.rotate agent_id=%s scopes=%s", agent_id, scopes)
    return new_token

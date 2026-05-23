"""common/auth.py の単体テスト（DDD-02 §2.2 / DDD-08 §4）。"""
from __future__ import annotations

import json

import pytest

from airflow_mcp.common.errors import (
    AuthScopeInsufficient,
    AuthTokenInvalid,
    AuthTokenMissing,
)


def _require(token, scope):
    """AIRFLOW_AUTH_LOCALHOST_BYPASS=false の状態で require_scope を呼ぶヘルパ。"""
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_cache = auth_mod._token_cache
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._token_cache = None  # キャッシュをクリアして毎回ディスクから読む
    try:
        auth_mod.require_scope(token, scope)
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._token_cache = original_cache


# ── localhost_bypass ──────────────────────────────────

def test_require_scope_passes_when_localhost_bypass_true():
    import airflow_mcp.common.auth as auth_mod
    original = auth_mod._LOCALHOST_BYPASS
    auth_mod._LOCALHOST_BYPASS = True
    try:
        auth_mod.require_scope(None, "tasks:write")  # None token OK when bypass
    finally:
        auth_mod._LOCALHOST_BYPASS = original


# ── token なし ────────────────────────────────────────

def test_require_scope_raises_missing_when_token_none():
    with pytest.raises(AuthTokenMissing):
        _require(None, "tasks:read")


# ── 環境変数トークン（tokens.json なし）─────────────────

def test_require_scope_env_token_any_scope_allowed(monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "my_secret_token")
    _require("my_secret_token", "tasks:write")  # raises nothing


def test_require_scope_env_token_mismatch_raises_invalid(monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "correct_token")
    with pytest.raises(AuthTokenInvalid):
        _require("wrong_token", "tasks:read")


# ── tokens.json ベースの認可 ────────────────────────────

def test_require_scope_exact_scope_allowed(tmp_path, monkeypatch):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok123", "scopes": ["tasks:read"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        auth_mod.require_scope("tok123", "tasks:read")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_require_scope_wildcard_allows_any_scope(tmp_path):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "admin_tok", "scopes": ["*"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        auth_mod.require_scope("admin_tok", "config:write")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_require_scope_namespace_wildcard_allows_subscope(tmp_path):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok", "scopes": ["tasks:*"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        auth_mod.require_scope("tok", "tasks:write")  # tasks:* covers tasks:write
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_require_scope_raises_insufficient_when_scope_missing(tmp_path):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok", "scopes": ["tasks:read"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        with pytest.raises(AuthScopeInsufficient) as exc_info:
            auth_mod.require_scope("tok", "tasks:write")
        assert "tasks:write" in str(exc_info.value.message)
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


# ── キャッシュ挙動 ────────────────────────────────────


def test_tokens_cached_after_first_load(tmp_path):
    """2 回目以降は _load_tokens_from_disk を呼ばずキャッシュを返すこと。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "cached_tok", "scopes": ["tasks:read"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    auth_mod._token_cache = None
    auth_mod._TOKENS_FILE = tokens_file
    try:
        # 1 回目: ディスクから読む
        result1 = auth_mod._get_tokens()
        # ファイルを削除してもキャッシュから返る
        tokens_file.unlink()
        result2 = auth_mod._get_tokens()
        assert result1 is result2  # 同一オブジェクト
        assert "cached_tok" in result2
    finally:
        auth_mod._token_cache = None
        auth_mod._TOKENS_FILE = auth_mod._TOKENS_FILE  # restore (already reset)


def test_reload_tokens_picks_up_changes(tmp_path):
    """reload_tokens() がファイル更新後の新しい内容を返すこと。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "old_tok", "scopes": ["tasks:read"]}]
    }))
    import airflow_mcp.common.auth as auth_mod
    original_file = auth_mod._TOKENS_FILE
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        auth_mod._get_tokens()  # キャッシュに載せる
        assert "old_tok" in auth_mod._token_cache

        # ファイルを更新
        tokens_file.write_text(json.dumps({
            "tokens": [{"token": "new_tok", "scopes": ["tasks:write"]}]
        }))
        # reload_tokens() で再読み込み
        count = auth_mod.reload_tokens()
        assert count == 1
        assert "new_tok" in auth_mod._token_cache
        assert "old_tok" not in auth_mod._token_cache
    finally:
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_get_tokens_returns_empty_on_missing_file():
    """tokens.json が存在しない場合、空辞書を返してクラッシュしないこと。"""
    import airflow_mcp.common.auth as auth_mod
    original_file = auth_mod._TOKENS_FILE
    auth_mod._TOKENS_FILE = auth_mod._TOKENS_FILE.parent / "nonexistent.json"
    auth_mod._token_cache = None
    try:
        result = auth_mod._get_tokens()
        assert result == {}
    finally:
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_get_tokens_returns_empty_on_corrupt_file(tmp_path):
    """破損した tokens.json でも fail-closed（空辞書）で返ること。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text("{ this is not valid json }")
    import airflow_mcp.common.auth as auth_mod
    original_file = auth_mod._TOKENS_FILE
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    try:
        result = auth_mod._get_tokens()
        assert result == {}
    finally:
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_require_scope_fails_closed_on_corrupt_tokens(tmp_path, monkeypatch):
    """破損 tokens.json かつ env_token 不一致 → AuthTokenInvalid（クラッシュしない）。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text("{ bad json }")
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = tokens_file
    auth_mod._token_cache = None
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "legit_token")
    try:
        from airflow_mcp.common.errors import AuthTokenInvalid
        with pytest.raises(AuthTokenInvalid):
            auth_mod.require_scope("some_unknown_token", "tasks:read")
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


def test_require_scope_env_token_none_raises_invalid(monkeypatch):
    """AIRFLOW_MCP_TOKEN 未設定かつ tokens.json なし → AuthTokenInvalid（TypeError なし）。"""
    monkeypatch.delenv("AIRFLOW_MCP_TOKEN", raising=False)
    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_file = auth_mod._TOKENS_FILE
    from pathlib import Path
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._TOKENS_FILE = Path("/nonexistent/tokens.json")
    auth_mod._token_cache = None
    try:
        from airflow_mcp.common.errors import AuthTokenInvalid
        with pytest.raises(AuthTokenInvalid):
            auth_mod.require_scope("any_token", "tasks:read")
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file
        auth_mod._token_cache = None


# ── get_token_from_env ────────────────────────────────

def test_get_token_from_env_returns_env_value(monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "test_token_xyz")
    from airflow_mcp.common.auth import get_token_from_env
    assert get_token_from_env() == "test_token_xyz"


def test_get_token_from_env_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv("AIRFLOW_MCP_TOKEN", raising=False)
    from airflow_mcp.common.auth import get_token_from_env
    assert get_token_from_env() is None


# ── JWT: _is_jwt ──────────────────────────────────────

def test_is_jwt_returns_true_for_three_dot_format():
    from airflow_mcp.common.auth import _is_jwt
    assert _is_jwt("header.payload.signature") is True


def test_is_jwt_returns_false_for_api_key():
    from airflow_mcp.common.auth import _is_jwt
    assert _is_jwt("plain_api_key_value") is False


def test_is_jwt_returns_false_for_two_dots():
    from airflow_mcp.common.auth import _is_jwt
    assert _is_jwt("a.b") is False


# ── JWT: generate_keypair ─────────────────────────────

def test_generate_keypair_creates_pem_files(tmp_path):
    from airflow_mcp.common.auth import generate_keypair
    priv, pub = generate_keypair(tmp_path)
    assert priv.exists()
    assert pub.exists()
    assert priv.name == "ed25519.pem"
    assert pub.name == "ed25519.pub"


def test_generate_keypair_pem_content(tmp_path):
    from airflow_mcp.common.auth import generate_keypair
    priv, pub = generate_keypair(tmp_path)
    assert b"PRIVATE KEY" in priv.read_bytes()
    assert b"PUBLIC KEY" in pub.read_bytes()


# ── JWT: issue_jwt + verify_jwt ───────────────────────

def test_issue_and_verify_jwt_roundtrip(tmp_path):
    from airflow_mcp.common.auth import generate_keypair, issue_jwt, verify_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_test", ["tasks:read", "models:read"],
                      ttl_sec=3600, private_key_path=priv)
    claims = verify_jwt(token, public_key_path=pub)
    assert claims["sub"] == "agent_test"
    assert "tasks:read" in claims["scopes"]
    assert claims["iss"] == "airflow-local"


def test_verify_jwt_raises_invalid_on_wrong_key(tmp_path):
    from airflow_mcp.common.auth import generate_keypair, issue_jwt, verify_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid
    priv1, pub1 = generate_keypair(tmp_path / "key1")
    _priv2, pub2 = generate_keypair(tmp_path / "key2")
    token = issue_jwt("agent_x", ["tasks:read"], private_key_path=priv1)
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub2)


def test_verify_jwt_raises_invalid_on_missing_public_key(tmp_path):
    from airflow_mcp.common.auth import verify_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid
    with pytest.raises(AuthTokenInvalid):
        verify_jwt("header.payload.sig", public_key_path=tmp_path / "nonexistent.pub")


def test_verify_jwt_raises_invalid_on_expired_token(tmp_path):
    from airflow_mcp.common.auth import generate_keypair, issue_jwt, verify_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_x", ["tasks:read"], ttl_sec=-1, private_key_path=priv)
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub)


def test_verify_jwt_raises_invalid_on_wrong_issuer(tmp_path):
    """issuer が 'airflow-local' と異なる場合は AuthTokenInvalid。"""
    import jwt as pyjwt
    from datetime import datetime, timezone
    from airflow_mcp.common.auth import generate_keypair, verify_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid

    priv, pub = generate_keypair(tmp_path)
    private_key_pem = priv.read_bytes()
    now = int(datetime.now(timezone.utc).timestamp())
    payload = {
        "iss": "other-issuer",
        "sub": "agent_x",
        "scopes": ["tasks:read"],
        "iat": now,
        "exp": now + 3600,
    }
    token = pyjwt.encode(payload, private_key_pem, algorithm="EdDSA")
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub)


def test_issue_jwt_raises_invalid_on_missing_private_key(tmp_path):
    from airflow_mcp.common.auth import issue_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid
    with pytest.raises(AuthTokenInvalid):
        issue_jwt("agent_x", ["tasks:read"], private_key_path=tmp_path / "nonexistent.pem")


# ── JWT: require_scope with JWT token ─────────────────

def test_require_scope_accepts_valid_jwt(tmp_path):
    """有効な JWT を require_scope が受け入れること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    import airflow_mcp.common.auth as auth_mod

    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_test", ["tasks:read"], private_key_path=priv)

    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_pub = auth_mod._JWT_PUBLIC_KEY_PATH
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._JWT_PUBLIC_KEY_PATH = pub
    try:
        auth_mod.require_scope(token, "tasks:read")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._JWT_PUBLIC_KEY_PATH = original_pub


def test_require_scope_jwt_raises_insufficient_for_missing_scope(tmp_path):
    """JWT が要求スコープを持たない場合に AuthScopeInsufficient。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    import airflow_mcp.common.auth as auth_mod

    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_test", ["tasks:read"], private_key_path=priv)

    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_pub = auth_mod._JWT_PUBLIC_KEY_PATH
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._JWT_PUBLIC_KEY_PATH = pub
    try:
        with pytest.raises(AuthScopeInsufficient):
            auth_mod.require_scope(token, "tasks:write")
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._JWT_PUBLIC_KEY_PATH = original_pub


def test_require_scope_jwt_wildcard_scope_passes(tmp_path):
    """JWT scopes に '*' が含まれる場合は何でも通ること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    import airflow_mcp.common.auth as auth_mod

    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("admin_agent", ["*"], private_key_path=priv)

    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_pub = auth_mod._JWT_PUBLIC_KEY_PATH
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._JWT_PUBLIC_KEY_PATH = pub
    try:
        auth_mod.require_scope(token, "config:write")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._JWT_PUBLIC_KEY_PATH = original_pub


def test_require_scope_jwt_namespace_wildcard_passes(tmp_path):
    """JWT の 'tasks:*' が 'tasks:write' を許可すること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    import airflow_mcp.common.auth as auth_mod

    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_rw", ["tasks:*"], private_key_path=priv)

    original_bypass = auth_mod._LOCALHOST_BYPASS
    original_pub = auth_mod._JWT_PUBLIC_KEY_PATH
    auth_mod._LOCALHOST_BYPASS = False
    auth_mod._JWT_PUBLIC_KEY_PATH = pub
    try:
        auth_mod.require_scope(token, "tasks:write")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._JWT_PUBLIC_KEY_PATH = original_pub


def test_verify_jwt_raises_invalid_when_pyjwt_not_installed(tmp_path, monkeypatch):
    """PyJWT がインポートできない場合に AuthTokenInvalid を返すこと（line 106-107）。"""
    import builtins
    from airflow_mcp.common.errors import AuthTokenInvalid

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "jwt":
            raise ImportError("no module named jwt")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    # Need to reimport verify_jwt so the import inside the function fires
    import airflow_mcp.common.auth as auth_mod
    with pytest.raises(AuthTokenInvalid):
        auth_mod.verify_jwt("header.payload.sig", public_key_path=tmp_path / "pub.pem")


def test_issue_jwt_raises_invalid_when_pyjwt_not_installed(tmp_path, monkeypatch):
    """PyJWT がインポートできない場合に AuthTokenInvalid を返すこと（line 153-154）。"""
    import builtins
    from airflow_mcp.common.errors import AuthTokenInvalid

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "jwt":
            raise ImportError("no module named jwt")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    import airflow_mcp.common.auth as auth_mod
    with pytest.raises(AuthTokenInvalid):
        auth_mod.issue_jwt("agent_x", ["tasks:read"], private_key_path=tmp_path / "priv.pem")


def test_issue_jwt_raises_invalid_on_corrupt_private_key(tmp_path):
    """秘密鍵が不正な内容の場合に AuthTokenInvalid を返すこと（line 171-173）。"""
    from airflow_mcp.common.auth import issue_jwt
    from airflow_mcp.common.errors import AuthTokenInvalid

    corrupt_key = tmp_path / "corrupt.pem"
    corrupt_key.write_bytes(b"this is not a valid pem key")
    with pytest.raises(AuthTokenInvalid):
        issue_jwt("agent_x", ["tasks:read"], private_key_path=corrupt_key)

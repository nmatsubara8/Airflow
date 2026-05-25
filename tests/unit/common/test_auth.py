"""common/auth.py の単体テスト（DDD-02 §2.2 / DDD-08 §4）。"""
from __future__ import annotations

import json

import pytest

from airflow_mcp.common.errors import (
    AuthScopeInsufficient,
    AuthTokenInvalid,
    AuthTokenMissing,
)


# ── localhost_bypass ──────────────────────────────────

def test_require_scope_passes_when_localhost_bypass_true(monkeypatch):
    import airflow_mcp.common.auth as auth_mod
    monkeypatch.setattr(auth_mod, "_LOCALHOST_BYPASS", True)
    auth_mod.require_scope(None, "tasks:write")  # None token OK when bypass


# ── token なし ────────────────────────────────────────

def test_require_scope_raises_missing_when_token_none(auth_isolated):
    with pytest.raises(AuthTokenMissing):
        auth_isolated.require_scope(None, "tasks:read")


# ── 環境変数トークン（tokens.json なし）─────────────────

def test_require_scope_env_token_any_scope_allowed(auth_isolated, monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "my_secret_token")
    auth_isolated.require_scope("my_secret_token", "tasks:write")  # raises nothing


def test_require_scope_env_token_mismatch_raises_invalid(auth_isolated, monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "correct_token")
    with pytest.raises(AuthTokenInvalid):
        auth_isolated.require_scope("wrong_token", "tasks:read")


# ── tokens.json ベースの認可 ────────────────────────────

def test_require_scope_exact_scope_allowed(tmp_path, auth_isolated, monkeypatch):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok123", "scopes": ["tasks:read"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    auth_isolated.require_scope("tok123", "tasks:read")  # passes


def test_require_scope_wildcard_allows_any_scope(tmp_path, auth_isolated, monkeypatch):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "admin_tok", "scopes": ["*"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    auth_isolated.require_scope("admin_tok", "config:write")  # passes


def test_require_scope_namespace_wildcard_allows_subscope(tmp_path, auth_isolated, monkeypatch):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok", "scopes": ["tasks:*"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    auth_isolated.require_scope("tok", "tasks:write")  # tasks:* covers tasks:write


def test_require_scope_raises_insufficient_when_scope_missing(tmp_path, auth_isolated, monkeypatch):
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "tok", "scopes": ["tasks:read"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    with pytest.raises(AuthScopeInsufficient) as exc_info:
        auth_isolated.require_scope("tok", "tasks:write")
    assert "tasks:write" in str(exc_info.value.message)


# ── キャッシュ挙動 ────────────────────────────────────

def test_tokens_cached_when_file_unchanged(tmp_path, auth_isolated, monkeypatch):
    """tokens.json が変更されていない間はキャッシュを返すこと（同一オブジェクト）。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "cached_tok", "scopes": ["tasks:read"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    result1 = auth_isolated._get_tokens()
    result2 = auth_isolated._get_tokens()   # mtime 不変 → 同一オブジェクト
    assert result1 is result2
    assert "cached_tok" in result2


def test_tokens_reloaded_when_file_deleted(tmp_path, auth_isolated, monkeypatch):
    """tokens.json が削除されたとき _get_tokens() が空辞書を返すこと。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "cached_tok", "scopes": ["tasks:read"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    auth_isolated._get_tokens()  # 初回ロード
    tokens_file.unlink()         # ファイル削除 → mtime 変化 → キャッシュ無効化
    result = auth_isolated._get_tokens()
    assert result == {}


def test_reload_tokens_picks_up_changes(tmp_path, auth_isolated, monkeypatch):
    """reload_tokens() がファイル更新後の新しい内容を返すこと。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "old_tok", "scopes": ["tasks:read"]}]
    }))
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    auth_isolated._get_tokens()  # キャッシュに載せる
    assert "old_tok" in auth_isolated._token_cache

    tokens_file.write_text(json.dumps({
        "tokens": [{"token": "new_tok", "scopes": ["tasks:write"]}]
    }))
    count = auth_isolated.reload_tokens()
    assert count == 1
    assert "new_tok" in auth_isolated._token_cache
    assert "old_tok" not in auth_isolated._token_cache


def test_get_tokens_returns_empty_on_missing_file(auth_isolated):
    """tokens.json が存在しない場合、空辞書を返してクラッシュしないこと。"""
    # auth_isolated の _TOKENS_FILE はデフォルトで存在しないパスを指している
    result = auth_isolated._get_tokens()
    assert result == {}


def test_get_tokens_returns_empty_on_corrupt_file(tmp_path, auth_isolated, monkeypatch):
    """破損した tokens.json でも fail-closed（空辞書）で返ること。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text("{ this is not valid json }")
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    result = auth_isolated._get_tokens()
    assert result == {}


def test_require_scope_fails_closed_on_corrupt_tokens(tmp_path, auth_isolated, monkeypatch):
    """破損 tokens.json かつ env_token 不一致 → AuthTokenInvalid（クラッシュしない）。"""
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text("{ bad json }")
    monkeypatch.setattr(auth_isolated, "_TOKENS_FILE", tokens_file)
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "legit_token")
    with pytest.raises(AuthTokenInvalid):
        auth_isolated.require_scope("some_unknown_token", "tasks:read")


def test_require_scope_env_token_none_raises_invalid(auth_isolated, monkeypatch):
    """AIRFLOW_MCP_TOKEN 未設定かつ tokens.json なし → AuthTokenInvalid（TypeError なし）。"""
    monkeypatch.delenv("AIRFLOW_MCP_TOKEN", raising=False)
    with pytest.raises(AuthTokenInvalid):
        auth_isolated.require_scope("any_token", "tasks:read")


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
    priv1, _pub1 = generate_keypair(tmp_path / "key1")
    _priv2, pub2 = generate_keypair(tmp_path / "key2")
    token = issue_jwt("agent_x", ["tasks:read"], private_key_path=priv1)
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub2)


def test_verify_jwt_raises_invalid_on_missing_public_key(tmp_path):
    from airflow_mcp.common.auth import verify_jwt
    with pytest.raises(AuthTokenInvalid):
        verify_jwt("header.payload.sig", public_key_path=tmp_path / "nonexistent.pub")


def test_verify_jwt_raises_invalid_on_expired_token(tmp_path):
    from airflow_mcp.common.auth import generate_keypair, issue_jwt, verify_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_x", ["tasks:read"], ttl_sec=-1, private_key_path=priv)
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub)


def test_verify_jwt_raises_invalid_on_wrong_issuer(tmp_path):
    """issuer が 'airflow-local' と異なる場合は AuthTokenInvalid。"""
    import jwt as pyjwt
    from datetime import datetime, timezone
    from airflow_mcp.common.auth import generate_keypair, verify_jwt

    priv, pub = generate_keypair(tmp_path)
    now = int(datetime.now(timezone.utc).timestamp())
    payload = {
        "iss": "other-issuer",
        "sub": "agent_x",
        "scopes": ["tasks:read"],
        "iat": now,
        "exp": now + 3600,
    }
    token = pyjwt.encode(payload, priv.read_bytes(), algorithm="EdDSA")
    with pytest.raises(AuthTokenInvalid):
        verify_jwt(token, public_key_path=pub)


def test_issue_jwt_raises_invalid_on_missing_private_key(tmp_path):
    from airflow_mcp.common.auth import issue_jwt
    with pytest.raises(AuthTokenInvalid):
        issue_jwt("agent_x", ["tasks:read"], private_key_path=tmp_path / "nonexistent.pem")


# ── JWT: require_scope with JWT token ─────────────────

def test_require_scope_accepts_valid_jwt(tmp_path, auth_isolated, monkeypatch):
    """有効な JWT を require_scope が受け入れること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_test", ["tasks:read"], private_key_path=priv)
    monkeypatch.setattr(auth_isolated, "_JWT_PUBLIC_KEY_PATH", pub)
    auth_isolated.require_scope(token, "tasks:read")  # passes


def test_require_scope_jwt_raises_insufficient_for_missing_scope(tmp_path, auth_isolated, monkeypatch):
    """JWT が要求スコープを持たない場合に AuthScopeInsufficient。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_test", ["tasks:read"], private_key_path=priv)
    monkeypatch.setattr(auth_isolated, "_JWT_PUBLIC_KEY_PATH", pub)
    with pytest.raises(AuthScopeInsufficient):
        auth_isolated.require_scope(token, "tasks:write")


def test_require_scope_jwt_wildcard_scope_passes(tmp_path, auth_isolated, monkeypatch):
    """JWT scopes に '*' が含まれる場合は何でも通ること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("admin_agent", ["*"], private_key_path=priv)
    monkeypatch.setattr(auth_isolated, "_JWT_PUBLIC_KEY_PATH", pub)
    auth_isolated.require_scope(token, "config:write")  # passes


def test_require_scope_jwt_namespace_wildcard_passes(tmp_path, auth_isolated, monkeypatch):
    """JWT の 'tasks:*' が 'tasks:write' を許可すること。"""
    from airflow_mcp.common.auth import generate_keypair, issue_jwt
    priv, pub = generate_keypair(tmp_path)
    token = issue_jwt("agent_rw", ["tasks:*"], private_key_path=priv)
    monkeypatch.setattr(auth_isolated, "_JWT_PUBLIC_KEY_PATH", pub)
    auth_isolated.require_scope(token, "tasks:write")  # passes


def test_verify_jwt_raises_invalid_when_pyjwt_not_installed(tmp_path, monkeypatch):
    """PyJWT がインポートできない場合に AuthTokenInvalid を返すこと（line 106-107）。"""
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "jwt":
            raise ImportError("no module named jwt")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    import airflow_mcp.common.auth as auth_mod
    with pytest.raises(AuthTokenInvalid):
        auth_mod.verify_jwt("header.payload.sig", public_key_path=tmp_path / "pub.pem")


def test_issue_jwt_raises_invalid_when_pyjwt_not_installed(tmp_path, monkeypatch):
    """PyJWT がインポートできない場合に AuthTokenInvalid を返すこと（line 153-154）。"""
    import builtins
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

    corrupt_key = tmp_path / "corrupt.pem"
    corrupt_key.write_bytes(b"this is not a valid pem key")
    with pytest.raises(AuthTokenInvalid):
        issue_jwt("agent_x", ["tasks:read"], private_key_path=corrupt_key)


def test_get_tokens_handles_stat_oserror(monkeypatch):
    """_TOKENS_FILE.stat() が OSError のとき current_mtime=0 でロードが進むこと（lines 66-67）。"""
    from unittest.mock import patch
    import airflow_mcp.common.auth as auth_mod

    # _TOKENS_FILE が存在するように見せ、stat() が OSError を投げるようにする
    with patch.object(auth_mod._TOKENS_FILE.__class__, "exists", return_value=True), \
         patch.object(auth_mod._TOKENS_FILE.__class__, "stat", side_effect=OSError("no stat")):
        auth_mod._token_cache = None
        auth_mod._token_cache_mtime = -1.0
        result = auth_mod._get_tokens()
    # クラッシュせずに dict を返すこと（ファイルがないので空 dict）
    assert isinstance(result, dict)


# ── list_tokens / revoke_token / rotate_token (TF-2) ──────────────────────────

def test_list_tokens_returns_empty_when_no_file(tmp_path, monkeypatch):
    """tokens.json が存在しないとき空リストを返すこと。"""
    import airflow_mcp.common.auth as auth_mod
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tmp_path / "none.json")
    from airflow_mcp.common.auth import list_tokens
    assert list_tokens() == []


def test_list_tokens_returns_agent_info(tmp_path, monkeypatch):
    """tokens.json から agent_id・scopes・created_at を返すこと。"""
    import json
    import airflow_mcp.common.auth as auth_mod
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tokens": [
        {"token": "tok1", "agent_id": "agent_docs",
         "scopes": ["tasks:read"], "created_at": "2026-01-01T00:00:00Z"},
    ]}))
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tokens_file)
    from airflow_mcp.common.auth import list_tokens
    result = list_tokens()
    assert len(result) == 1
    assert result[0]["agent_id"] == "agent_docs"
    assert "tasks:read" in result[0]["scopes"]
    assert "token" not in result[0]  # トークン値は返さない


def test_revoke_token_removes_matching_agent(tmp_path, monkeypatch):
    """revoke_token が指定 agent_id のエントリを削除すること。"""
    import json
    import airflow_mcp.common.auth as auth_mod
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tokens": [
        {"token": "t1", "agent_id": "agent_docs", "scopes": []},
        {"token": "t2", "agent_id": "agent_qa",   "scopes": []},
    ]}))
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tokens_file)
    auth_mod._token_cache = None
    from airflow_mcp.common.auth import revoke_token
    ok = revoke_token("agent_docs")
    assert ok is True
    remaining = json.loads(tokens_file.read_text())["tokens"]
    assert all(t["agent_id"] != "agent_docs" for t in remaining)
    assert len(remaining) == 1


def test_revoke_token_returns_false_when_not_found(tmp_path, monkeypatch):
    """対象 agent_id が存在しないとき False を返すこと。"""
    import json
    import airflow_mcp.common.auth as auth_mod
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tokens": []}))
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tokens_file)
    from airflow_mcp.common.auth import revoke_token
    assert revoke_token("nonexistent") is False


def test_rotate_token_issues_new_jwt(tmp_path, monkeypatch):
    """rotate_token が旧トークンを削除して JWT を返すこと。"""
    import json
    import airflow_mcp.common.auth as auth_mod
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"tokens": [
        {"token": "old_tok", "agent_id": "agent_docs", "scopes": ["tasks:read"]},
    ]}))
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tokens_file)
    auth_mod._token_cache = None
    # JWT 発行には秘密鍵が必要なので issue_jwt をモック
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.issue_jwt", return_value="jwt_new_token") as mock_issue:
        from airflow_mcp.common.auth import rotate_token
        token = rotate_token("agent_docs", scopes=["tasks:read", "tasks:write"])
    assert token == "jwt_new_token"
    mock_issue.assert_called_once()
    # 旧トークンが削除されていること
    remaining = json.loads(tokens_file.read_text())["tokens"]
    assert all(t["agent_id"] != "agent_docs" for t in remaining)


def test_read_tokens_file_returns_empty_on_parse_error(tmp_path, monkeypatch):
    """tokens.json が壊れているとき空の dict を返すこと (lines 274-276)。"""
    import airflow_mcp.common.auth as auth_mod
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text("not valid json {{{{")
    monkeypatch.setattr(auth_mod, "_TOKENS_FILE", tokens_file)
    from airflow_mcp.common.auth import _read_tokens_file
    result = _read_tokens_file()
    assert result == {"tokens": []}

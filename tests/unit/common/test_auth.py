"""common/auth.py の単体テスト（DDD-02 §2.2 / DDD-08 §4）。"""
from __future__ import annotations

import json
import os

import pytest

from airflow_mcp.common.errors import (
    AuthScopeInsufficient,
    AuthTokenInvalid,
    AuthTokenMissing,
)


def _require(token, scope):
    """AIRFLOW_AUTH_LOCALHOST_BYPASS=false の状態で require_scope を呼ぶヘルパ。"""
    import importlib
    import airflow_mcp.common.auth as auth_mod
    original = auth_mod._LOCALHOST_BYPASS
    auth_mod._LOCALHOST_BYPASS = False
    try:
        auth_mod.require_scope(token, scope)
    finally:
        auth_mod._LOCALHOST_BYPASS = original


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
    try:
        auth_mod.require_scope("tok123", "tasks:read")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file


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
    try:
        auth_mod.require_scope("admin_tok", "config:write")  # passes
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file


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
    try:
        auth_mod.require_scope("tok", "tasks:write")  # tasks:* covers tasks:write
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file


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
    try:
        with pytest.raises(AuthScopeInsufficient) as exc_info:
            auth_mod.require_scope("tok", "tasks:write")
        assert "tasks:write" in str(exc_info.value.message)
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass
        auth_mod._TOKENS_FILE = original_file


# ── get_token_from_env ────────────────────────────────

def test_get_token_from_env_returns_env_value(monkeypatch):
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "test_token_xyz")
    from airflow_mcp.common.auth import get_token_from_env
    assert get_token_from_env() == "test_token_xyz"


def test_get_token_from_env_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv("AIRFLOW_MCP_TOKEN", raising=False)
    from airflow_mcp.common.auth import get_token_from_env
    assert get_token_from_env() is None

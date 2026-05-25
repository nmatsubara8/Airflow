"""common/config_validator.py の単体テスト（TE-2）。"""
from __future__ import annotations

import pytest

from airflow_mcp.common.config_validator import validate_config


# ── 基本: 正常ケース ──────────────────────────────────────────

def test_validate_dispatch_valid():
    cfg = {
        "routing": {
            "default_category": "engineering",
            "rules": [],
        },
        "section_agents": {"engineering": "agent_docs"},
    }
    assert validate_config("dispatch", cfg) == []


def test_validate_auth_valid():
    cfg = {
        "auth": {
            "mode": "api_key",
            "key_env_prefix": "AIRFLOW_MCP_TOKEN_",
        },
        "mcp_auth": {"require_token": True, "localhost_bypass": True},
    }
    assert validate_config("auth", cfg) == []


def test_validate_model_config_valid():
    cfg = {
        "selection": {
            "default_mode": "recommend",
            "exploration_rate": 0.1,
        }
    }
    assert validate_config("model-config", cfg) == []


def test_validate_ingester_valid():
    cfg = {
        "watch": {
            "paths": ["${VAULT_ROOT}/Inbox"],
            "glob": "**/*.md",
        },
    }
    assert validate_config("ingester", cfg) == []


def test_validate_chains_valid():
    cfg = {"chains": [{"name": "c1", "mode": "sequential"}]}
    assert validate_config("chains", cfg) == []


def test_validate_unknown_schema_name_returns_empty():
    """未知のスキーマ名は検証をスキップして空リストを返すこと。"""
    assert validate_config("unknown_config", {"anything": "goes"}) == []


# ── エラー: 必須フィールド欠落 ──────────────────────────────

def test_validate_auth_missing_mode():
    """auth.mode が欠落のとき error を返すこと。"""
    cfg = {"auth": {"key_env_prefix": "PREFIX_"}}
    errors = validate_config("auth", cfg)
    assert any("auth.mode" in e for e in errors)


def test_validate_auth_missing_auth_root():
    """auth キー自体が欠落のとき error を返すこと。"""
    cfg = {"rate_limit": {}}
    errors = validate_config("auth", cfg)
    assert any("'auth'" in e and "missing" in e for e in errors)


def test_validate_ingester_missing_watch():
    """watch が欠落のとき error を返すこと。"""
    errors = validate_config("ingester", {})
    assert any("'watch'" in e for e in errors)


def test_validate_ingester_missing_watch_paths():
    """watch.paths が欠落のとき error を返すこと。"""
    errors = validate_config("ingester", {"watch": {"glob": "*.md"}})
    assert any("watch.paths" in e for e in errors)


def test_validate_chains_missing_chains_key():
    """chains キーが欠落のとき error を返すこと。"""
    errors = validate_config("chains", {})
    assert any("'chains'" in e for e in errors)


def test_validate_model_config_missing_selection():
    """selection キーが欠落のとき error を返すこと。"""
    errors = validate_config("model-config", {})
    assert any("'selection'" in e for e in errors)


# ── エラー: 型違い ───────────────────────────────────────────

def test_validate_dispatch_routing_wrong_type():
    """routing が dict でなければ型エラーを返すこと。"""
    cfg = {"routing": "not-a-dict"}
    errors = validate_config("dispatch", cfg)
    assert any("routing" in e and "dict" in e for e in errors)


def test_validate_dispatch_rules_wrong_type():
    """routing.rules が list でなければ型エラーを返すこと。"""
    cfg = {"routing": {"rules": {"bad": "type"}, "default_category": "eng"}}
    errors = validate_config("dispatch", cfg)
    assert any("routing.rules" in e for e in errors)


def test_validate_auth_mode_wrong_type():
    """auth.mode が str でなければ型エラーを返すこと。"""
    cfg = {"auth": {"mode": 123}}
    errors = validate_config("auth", cfg)
    assert any("auth.mode" in e and "str" in e for e in errors)


def test_validate_mcp_auth_require_token_wrong_type():
    """mcp_auth.require_token が bool でなければ型エラーを返すこと。"""
    cfg = {"auth": {"mode": "api_key"}, "mcp_auth": {"require_token": "yes"}}
    errors = validate_config("auth", cfg)
    assert any("require_token" in e for e in errors)


def test_validate_model_config_exploration_rate_accepts_float():
    """exploration_rate は float を受け入れること。"""
    cfg = {"selection": {"default_mode": "recommend", "exploration_rate": 0.1}}
    assert validate_config("model-config", cfg) == []


def test_validate_model_config_exploration_rate_wrong_type():
    """exploration_rate が str のとき型エラーを返すこと。"""
    cfg = {"selection": {"default_mode": "recommend", "exploration_rate": "high"}}
    errors = validate_config("model-config", cfg)
    assert any("exploration_rate" in e for e in errors)


# ── エラー: root が dict でない ──────────────────────────────

def test_validate_root_not_dict_returns_error():
    """config root が dict でないとき即 error を返すこと。"""
    errors = validate_config("auth", ["a", "list"])
    assert len(errors) == 1
    assert "must be a dict" in errors[0]


def test_validate_root_none_returns_error():
    errors = validate_config("dispatch", None)  # type: ignore[arg-type]
    assert errors


# ── 実設定ファイルとの整合 ───────────────────────────────────

def test_validate_actual_config_files(tmp_path):
    """実際の .airflow/config/*.yaml が全てバリデーションを通過すること。"""
    from pathlib import Path
    from airflow_mcp.common.io import safe_yaml_load

    config_dir = Path(".airflow/config")
    if not config_dir.exists():
        pytest.skip(".airflow/config not found")

    names = ["auth", "chains", "dispatch", "ingester", "model-config",
             "notifications", "parallel", "schedules"]
    for name in names:
        path = config_dir / f"{name}.yaml"
        if not path.exists():
            continue
        data = safe_yaml_load(path)
        errors = validate_config(name, data)
        assert errors == [], f"{name}.yaml failed: {errors}"

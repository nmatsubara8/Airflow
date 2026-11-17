"""common/io.py の単体テスト（safe_yaml_load / safe_json_load / load_model_registry）。"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch


from airflow_mcp.common.io import load_model_registry, safe_json_load, safe_yaml_load


# ── safe_yaml_load ────────────────────────────────────────

def test_safe_yaml_load_valid_dict(tmp_path):
    f = tmp_path / "cfg.yaml"
    f.write_text("key: value\n")
    assert safe_yaml_load(f) == {"key": "value"}


def test_safe_yaml_load_missing_file(tmp_path):
    assert safe_yaml_load(tmp_path / "nonexistent.yaml") == {}


def test_safe_yaml_load_empty_file(tmp_path):
    f = tmp_path / "empty.yaml"
    f.write_text("")
    assert safe_yaml_load(f) == {}


def test_safe_yaml_load_comment_only(tmp_path):
    f = tmp_path / "comment.yaml"
    f.write_text("# just a comment\n")
    assert safe_yaml_load(f) == {}


def test_safe_yaml_load_list_returns_empty(tmp_path):
    f = tmp_path / "list.yaml"
    f.write_text("- a\n- b\n")
    assert safe_yaml_load(f) == {}


def test_safe_yaml_load_oserror_returns_empty(tmp_path):
    f = tmp_path / "file.yaml"
    f.write_text("key: v\n")
    with patch.object(Path, "read_text", side_effect=OSError("unreadable")):
        assert safe_yaml_load(f) == {}


def test_safe_yaml_load_invalid_yaml_returns_empty(tmp_path):
    f = tmp_path / "bad.yaml"
    f.write_text("key: [unclosed\n")
    assert safe_yaml_load(f) == {}


# ── safe_json_load ────────────────────────────────────────

def test_safe_json_load_valid_dict(tmp_path):
    f = tmp_path / "data.json"
    f.write_text(json.dumps({"x": 1}))
    assert safe_json_load(f) == {"x": 1}


def test_safe_json_load_missing_file(tmp_path):
    assert safe_json_load(tmp_path / "nonexistent.json") == {}


def test_safe_json_load_list_returns_empty(tmp_path):
    f = tmp_path / "list.json"
    f.write_text("[1, 2, 3]")
    assert safe_json_load(f) == {}


def test_safe_json_load_invalid_json_returns_empty(tmp_path):
    f = tmp_path / "bad.json"
    f.write_text("{broken")
    assert safe_json_load(f) == {}


def test_safe_json_load_oserror_returns_empty(tmp_path):
    f = tmp_path / "file.json"
    f.write_text("{}")
    with patch.object(Path, "read_text", side_effect=OSError("unreadable")):
        assert safe_json_load(f) == {}


# ── load_model_registry ───────────────────────────────────

def test_load_model_registry_returns_models_by_id(tmp_path, monkeypatch):
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"models": [
        {"id": "qwen14b", "name": "Qwen 14B"},
        {"id": "bge", "name": "BGE"},
    ]}))
    monkeypatch.setenv("AIRFLOW_REGISTRY", str(reg))
    result = load_model_registry()
    assert "qwen14b" in result
    assert "bge" in result


def test_load_model_registry_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_REGISTRY", str(tmp_path / "nonexistent.json"))
    assert load_model_registry() == {}


def test_load_model_registry_skips_entries_without_id(tmp_path, monkeypatch):
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"models": [
        {"id": "good"},
        {"name": "no_id"},
    ]}))
    monkeypatch.setenv("AIRFLOW_REGISTRY", str(reg))
    result = load_model_registry()
    assert "good" in result
    assert len(result) == 1


def test_load_model_registry_explicit_path(tmp_path):
    reg = tmp_path / "custom.json"
    reg.write_text(json.dumps({"models": [{"id": "m1"}]}))
    result = load_model_registry(path=reg)
    assert "m1" in result

"""registry.json のスキーマ準拠テスト（DDD-01 §10、test-strategy §2.6）。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
REGISTRY_PATH = PROJECT_ROOT / ".airflow" / "models" / "registry.json"
MODEL_SCHEMA_PATH = PROJECT_ROOT / "schemas" / "model.schema.json"


@pytest.fixture(scope="module")
def registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text())


@pytest.fixture(scope="module")
def model_schema() -> dict:
    return json.loads(MODEL_SCHEMA_PATH.read_text())


def test_registry_file_exists():
    assert REGISTRY_PATH.exists(), f"{REGISTRY_PATH} が見つかりません"


def test_registry_has_models_list(registry: dict):
    assert "models" in registry
    assert isinstance(registry["models"], list)
    assert len(registry["models"]) > 0


def test_all_models_pass_schema_validation(registry: dict, model_schema: dict):
    validator = Draft202012Validator(model_schema)
    for model in registry["models"]:
        errors = list(validator.iter_errors(model))
        assert errors == [], (
            f"モデル '{model.get('id', '?')}' がスキーマ不適合:\n"
            + "\n".join(str(e) for e in errors)
        )


def test_all_models_have_unique_ids(registry: dict):
    ids = [m["id"] for m in registry["models"]]
    assert len(ids) == len(set(ids)), "重複した model id が存在します"


def test_all_models_have_ollama_tag(registry: dict):
    for model in registry["models"]:
        assert "ollama_tag" in model, f"モデル '{model['id']}' に ollama_tag がありません"
        assert model["ollama_tag"], "ollama_tag が空です"


def test_all_models_have_vram_gb(registry: dict):
    for model in registry["models"]:
        assert "vram_gb" in model, f"モデル '{model['id']}' に vram_gb がありません"
        assert model["vram_gb"] > 0

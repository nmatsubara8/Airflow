"""config 間の参照整合性テスト（test-strategy §2.6）。"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / ".airflow" / "config"
AGENTS_DIR = PROJECT_ROOT / "agents"
REGISTRY_PATH = PROJECT_ROOT / ".airflow" / "models" / "registry.json"


@pytest.fixture(scope="module")
def registry_ids() -> set[str]:
    data = json.loads(REGISTRY_PATH.read_text())
    return {m["id"] for m in data.get("models", [])}


@pytest.fixture(scope="module")
def parallel_config() -> dict:
    return yaml.safe_load((CONFIG_DIR / "parallel.yaml").read_text()) or {}


@pytest.fixture(scope="module")
def model_config() -> dict:
    return yaml.safe_load((CONFIG_DIR / "model-config.yaml").read_text()) or {}


@pytest.fixture(scope="module")
def schedules_config() -> dict:
    return yaml.safe_load((CONFIG_DIR / "schedules.yaml").read_text()) or {}


@pytest.fixture(scope="module")
def auth_config() -> dict:
    return yaml.safe_load((CONFIG_DIR / "auth.yaml").read_text()) or {}


@pytest.fixture(scope="module")
def ingester_config() -> dict:
    return yaml.safe_load((CONFIG_DIR / "ingester.yaml").read_text()) or {}


@pytest.fixture(scope="module")
def agent_ids_from_skills() -> set[str]:
    ids: set[str] = set()
    for skills_path in AGENTS_DIR.rglob("skills.md"):
        content = skills_path.read_text()
        m = re.search(r"agent_id:\s*(\S+)", content)
        if m:
            ids.add(m.group(1))
    return ids


# ── parallel.yaml ─────────────────────────────────────────

def test_parallel_vram_available_le_total_minus_reserve(parallel_config: dict):
    sched = parallel_config.get("scheduler", {})
    total = sched.get("vram_total_gb", 0)
    reserve = sched.get("vram_reserve_gb", 0)
    available = sched.get("vram_available_gb", 0)
    assert available <= total - reserve, (
        f"vram_available_gb({available}) > total({total}) - reserve({reserve})"
    )


def test_parallel_mvp_profile_exists(parallel_config: dict):
    mvp = parallel_config.get("mvp_profile")
    profiles = {p["name"] for p in parallel_config.get("profiles", [])}
    assert mvp in profiles, f"mvp_profile '{mvp}' が profiles に存在しません"


# ── model-config.yaml ─────────────────────────────────────

def test_model_config_knn_weights_all_positive(model_config: dict):
    weights = model_config.get("knn", {}).get("weights", {})
    assert weights, "knn.weights が空です"
    for key, val in weights.items():
        assert val > 0, f"knn.weights.{key} = {val} は正でなければなりません"


def test_model_config_fallback_chain_models_in_registry(
    model_config: dict, registry_ids: set[str]
):
    chain = model_config.get("reroute", {}).get("fallback_chain", {})
    for src_model, fallbacks in chain.items():
        assert src_model in registry_ids, f"fallback_chain のキー '{src_model}' が registry にありません"
        for fb in fallbacks:
            assert fb in registry_ids, f"fallback '{fb}' が registry にありません"


# ── schedules.yaml ────────────────────────────────────────

def test_schedules_agent_ids_have_skills_md(
    schedules_config: dict, agent_ids_from_skills: set[str]
):
    schedules = schedules_config.get("schedules", {})
    if isinstance(schedules, dict):
        items = schedules.values()
    else:
        items = schedules
    missing = []
    for sched in items:
        for agent_id in sched.get("agents", []):
            if agent_id not in agent_ids_from_skills:
                missing.append(agent_id)
    assert not missing, f"schedules.yaml のエージェントが skills.md に見つかりません: {missing}"


# ── auth.yaml ─────────────────────────────────────────────

def test_auth_rate_limit_roles_defined_in_roles(auth_config: dict):
    # roles は auth.roles 以下にネスト（DDD-02 §2.3 の構造）
    roles = set(auth_config.get("auth", {}).get("roles", {}).keys())
    per_role = auth_config.get("rate_limit", {}).get("per_role", {})
    for role in per_role:
        assert role in roles, f"rate_limit.per_role に未定義のロール '{role}'"


# ── ingester.yaml ─────────────────────────────────────────

def test_ingester_embedding_model_in_registry(
    ingester_config: dict, registry_ids: set[str]
):
    emb_model = ingester_config.get("embedding", {}).get("model_id")
    if emb_model:
        assert emb_model in registry_ids, (
            f"ingester.yaml の embedding.model_id '{emb_model}' が registry にありません"
        )

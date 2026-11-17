"""common/feature_extractor.py の単体テスト。"""
from __future__ import annotations

from airflow_mcp.common.feature_extractor import (
    ACCURACY_MAP,
    LANGUAGES,
    LATENCY_MAP,
    TASK_TYPES,
    extract_features,
)


# ── 定数 ─────────────────────────────────────────────────────

def test_task_types_contains_core_types():
    for t in ("review", "summarize", "code_gen", "document", "translate"):
        assert t in TASK_TYPES


def test_languages_contains_ja_and_en():
    assert "ja" in LANGUAGES
    assert "en" in LANGUAGES


def test_accuracy_map_covers_all_levels():
    assert set(ACCURACY_MAP) == {"low", "medium", "high"}


def test_latency_map_covers_all_levels():
    assert set(LATENCY_MAP) == {"none", "batch", "interactive"}


# ── task_type 推論 ────────────────────────────────────────────

def test_task_type_from_review_tag():
    features, inferred = extract_features({"tags": ["review"]})
    assert features["task_type"] == "review"
    assert "task_type" in inferred


def test_task_type_code_review_alias_maps_to_review():
    features, _ = extract_features({"tags": ["code-review"]})
    assert features["task_type"] == "review"


def test_task_type_summary_alias_maps_to_summarize():
    features, _ = extract_features({"tags": ["summary"]})
    assert features["task_type"] == "summarize"


def test_task_type_code_tag_maps_to_code_gen():
    features, _ = extract_features({"tags": ["code", "backend"]})
    assert features["task_type"] == "code_gen"


def test_task_type_translate_tag():
    features, _ = extract_features({"tags": ["translate"]})
    assert features["task_type"] == "translate"


def test_task_type_unknown_tag_defaults_to_document():
    features, _ = extract_features({"tags": ["unknown-tag"]})
    assert features["task_type"] == "document"


def test_task_type_no_tags_defaults_to_document():
    features, _ = extract_features({})
    assert features["task_type"] == "document"


def test_task_type_summarize_tag_direct():
    features, _ = extract_features({"tags": ["summarize"]})
    assert features["task_type"] == "summarize"


# ── required_accuracy 推論 ────────────────────────────────────

def test_required_accuracy_business_section_is_high():
    features, inferred = extract_features({"section": "business"})
    assert features["required_accuracy"] == "high"
    assert "required_accuracy" in inferred


def test_required_accuracy_legal_section_is_high():
    features, _ = extract_features({"section": "legal"})
    assert features["required_accuracy"] == "high"


def test_required_accuracy_finance_section_is_high():
    features, _ = extract_features({"section": "finance"})
    assert features["required_accuracy"] == "high"


def test_required_accuracy_other_section_defaults_to_medium():
    features, _ = extract_features({"section": "engineering"})
    assert features["required_accuracy"] == "medium"


def test_required_accuracy_no_section_defaults_to_medium():
    features, _ = extract_features({})
    assert features["required_accuracy"] == "medium"


# ── latency_sensitivity 推論 ──────────────────────────────────

def test_latency_urgent_tag_is_interactive():
    features, inferred = extract_features({"tags": ["urgent"]})
    assert features["latency_sensitivity"] == "interactive"
    assert "latency_sensitivity" in inferred


def test_latency_interactive_tag_is_interactive():
    features, _ = extract_features({"tags": ["interactive"]})
    assert features["latency_sensitivity"] == "interactive"


def test_latency_no_special_tag_defaults_to_batch():
    features, _ = extract_features({"tags": ["summarize"]})
    assert features["latency_sensitivity"] == "batch"


def test_latency_empty_task_defaults_to_batch():
    features, _ = extract_features({})
    assert features["latency_sensitivity"] == "batch"


# ── language 推論 ─────────────────────────────────────────────

def test_language_japanese_title_is_ja():
    features, inferred = extract_features({"title": "日本語タイトル"})
    assert features["language"] == "ja"
    assert "language" in inferred


def test_language_ascii_title_is_en():
    features, _ = extract_features({"title": "English title"})
    assert features["language"] == "en"


def test_language_empty_title_defaults_to_en():
    features, _ = extract_features({})
    assert features["language"] == "en"


# ── 既存 features の保護 ─────────────────────────────────────

def test_does_not_overwrite_existing_task_type():
    task = {"tags": ["review"], "features": {"task_type": "summarize"}}
    features, inferred = extract_features(task)
    assert features["task_type"] == "summarize"
    assert "task_type" not in inferred


def test_does_not_overwrite_existing_required_accuracy():
    task = {"section": "business", "features": {"required_accuracy": "low"}}
    features, _ = extract_features(task)
    assert features["required_accuracy"] == "low"


def test_does_not_overwrite_existing_latency_sensitivity():
    task = {"tags": ["urgent"], "features": {"latency_sensitivity": "batch"}}
    features, _ = extract_features(task)
    assert features["latency_sensitivity"] == "batch"


def test_does_not_overwrite_existing_language():
    task = {"title": "日本語", "features": {"language": "en"}}
    features, _ = extract_features(task)
    assert features["language"] == "en"


# ── inferred_fields ───────────────────────────────────────────

def test_inferred_fields_empty_when_all_preset():
    task = {"features": {
        "task_type": "review",
        "required_accuracy": "high",
        "latency_sensitivity": "interactive",
        "language": "ja",
    }}
    _, inferred = extract_features(task)
    assert inferred == []


def test_inferred_fields_lists_all_four_for_empty_task():
    _, inferred = extract_features({})
    assert set(inferred) == {"task_type", "required_accuracy", "latency_sensitivity", "language"}


# ── 戻り値の独立性（元の features を変更しない） ──────────────

def test_original_features_not_mutated():
    original = {"task_type": "review"}
    task = {"features": original}
    features, _ = extract_features(task)
    features["extra"] = "injected"
    assert "extra" not in original


# ── タグ大文字・混在ケース正規化 ─────────────────────────────────

def test_tag_uppercase_maps_correctly():
    """大文字タグ "REVIEW" は小文字化されて "review" にマッチする。"""
    features, _ = extract_features({"tags": ["REVIEW"]})
    assert features["task_type"] == "review"


def test_tag_mixed_case_maps_correctly():
    """混在ケース "Review" も正規化されて "review" にマッチする。"""
    features, _ = extract_features({"tags": ["Review"]})
    assert features["task_type"] == "review"


def test_tag_uppercase_translate():
    """大文字タグ "TRANSLATE" は "translate" として認識される。"""
    features, _ = extract_features({"tags": ["TRANSLATE"]})
    assert features["task_type"] == "translate"


def test_tag_uppercase_alias_code_review():
    """大文字エイリアス "CODE-REVIEW" はエイリアス解決後 "review" になる。"""
    features, _ = extract_features({"tags": ["CODE-REVIEW"]})
    assert features["task_type"] == "review"


def test_tag_uppercase_code_maps_to_code_gen():
    """大文字 "CODE" タグは "code_gen" にマッチする。"""
    features, _ = extract_features({"tags": ["CODE"]})
    assert features["task_type"] == "code_gen"


def test_tag_urgent_uppercase_infers_interactive_latency():
    """大文字 "URGENT" タグは latency_sensitivity=interactive を推論する。"""
    features, _ = extract_features({"tags": ["URGENT"]})
    assert features["latency_sensitivity"] == "interactive"


def test_section_mixed_case_infers_high_accuracy():
    """section="Business"（混在ケース）でも required_accuracy=high になる。"""
    features, _ = extract_features({"section": "Business"})
    assert features["required_accuracy"] == "high"


def test_section_uppercase_legal_infers_high_accuracy():
    """section="LEGAL" でも required_accuracy=high になる。"""
    features, _ = extract_features({"section": "LEGAL"})
    assert features["required_accuracy"] == "high"


# ── None / 不正型 入力の堅牢性 ──────────────────────────────────

def test_extract_features_title_none_does_not_crash():
    """title=None でも TypeError にならず language が推論されること。"""
    features, inferred = extract_features({"title": None})
    assert "language" in features
    assert features["language"] in ("ja", "en")


def test_extract_features_title_none_infers_english():
    """title=None は ASCII 相当として 'en' に推論されること。"""
    features, _ = extract_features({"title": None})
    assert features["language"] == "en"


def test_extract_features_features_list_treated_as_empty():
    """features フィールドがリスト（非 dict）のとき空 dict として扱われること。"""
    features, inferred = extract_features({"features": ["bad", "data"]})
    assert isinstance(features, dict)
    assert "task_type" in features  # デフォルト推論が走ること


def test_extract_features_features_invalid_type_all_inferred():
    """features が非 dict のとき全フィールドが inferred になること。"""
    _, inferred = extract_features({"features": 42})
    assert "task_type" in inferred
    assert "language" in inferred

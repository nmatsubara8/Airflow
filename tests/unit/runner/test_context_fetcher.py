"""context_fetcher のユニットテスト。"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from airflow_mcp.runner.context_fetcher import (
    _build_query_words,
    _is_github_note,
    _score_note,
    _should_fetch_context,
    _source_label,
    fetch_relevant_context,
    format_context,
    search_notes,
)


# ── _should_fetch_context ──────────────────────────────────────────

@pytest.mark.parametrize("task,expected", [
    ({"tags": ["requirements"], "section": "engineering"}, True),
    ({"tags": [],               "section": "backlog"},     True),
    ({"tags": [],               "section": "design"},      True),
    ({"tags": [],               "section": "engineering"}, True),
    ({"tags": [],               "section": "qa"},          True),
    ({"tags": ["requirements"], "section": "backlog"},     True),
    ({"tags": ["needs_review"], "section": "other"},       True),
    ({"tags": [],               "section": "done"},        False),
    ({"tags": [],               "section": "cancelled"},   False),
])
def test_should_fetch_context(task, expected):
    assert _should_fetch_context(task) == expected


# ── _is_github_note ────────────────────────────────────────────────

def test_is_github_note_true():
    assert _is_github_note({"frontmatter": {"type": "github_reference"}}) is True

def test_is_github_note_false():
    assert _is_github_note({"frontmatter": {}}) is False
    assert _is_github_note({}) is False


# ── _build_query_words ─────────────────────────────────────────────

def test_build_query_words_extracts_words():
    task = {"title": "Horse Racing prediction system", "description": "predict winners"}
    words = _build_query_words(task)
    assert "horse" in words
    assert "racing" in words
    assert "predict" in words


def test_build_query_words_removes_stopwords():
    task = {"title": "for the system", "description": ""}
    words = _build_query_words(task)
    assert "the" not in words
    assert "for" not in words


def test_build_query_words_removes_single_char():
    task = {"title": "a b hello", "description": ""}
    words = _build_query_words(task)
    assert "a" not in words
    assert "b" not in words
    assert "hello" in words


# ── _score_note ────────────────────────────────────────────────────

def test_score_note_title_hit_weighted():
    note = {"title": "horse racing guide", "body": "some content"}
    score = _score_note(note, ["horse"])
    assert score == 3.0  # title hit × 3


def test_score_note_body_hit():
    note = {"title": "other", "body": "horse racing tips"}
    score = _score_note(note, ["horse"])
    assert score == 1.0


def test_score_note_both_hit():
    note = {"title": "horse guide", "body": "horse tips"}
    score = _score_note(note, ["horse"])
    assert score == 4.0  # title 3 + body 1


def test_score_note_no_hit():
    note = {"title": "weather forecast", "body": "rain tomorrow"}
    score = _score_note(note, ["horse"])
    assert score == 0.0


# ── _source_label ──────────────────────────────────────────────────

def test_source_label_github():
    note = {"frontmatter": {"type": "github_reference", "repo": "owner/repo",
                             "branch": "main", "path": "README.md"},
            "source_path": "github:owner/repo/main/README.md"}
    assert _source_label(note) == "GitHub: owner/repo@main / README.md"


def test_source_label_vault():
    note = {"frontmatter": {}, "source_path": "/vault/Projects/design.md"}
    assert "Vault" in _source_label(note)


def test_source_label_no_source():
    note = {"frontmatter": {}}
    assert _source_label(note) == "Vault"


# ── search_notes ───────────────────────────────────────────────────

def _make_index(notes: list[dict]) -> dict:
    return {"schema_version": "1.0.0",
            "notes": {n["note_id"]: n for n in notes}}


def test_search_notes_returns_top_k(tmp_path):
    idx = _make_index([
        {"note_id": "n1", "title": "horse racing", "body": "predict winners", "confidential": False, "frontmatter": {}},
        {"note_id": "n2", "title": "weather",      "body": "rain today",       "confidential": False, "frontmatter": {}},
        {"note_id": "n3", "title": "horse tips",   "body": "speed horse data", "confidential": False, "frontmatter": {}},
    ])
    p = tmp_path / "index.json"
    p.write_text(json.dumps(idx))
    results = search_notes(["horse"], p, top_k=1)
    assert len(results) == 1
    # n3 は title + body 両方ヒット (score 4) > n1 (title hit 3)
    # n1 は title のみ (score 3)
    # どちらが 1 位でも "horse" を含む
    assert "horse" in results[0]["title"].lower()


def test_search_notes_excludes_confidential(tmp_path):
    idx = _make_index([
        {"note_id": "n1", "title": "secret horse", "body": "horse data", "confidential": True, "frontmatter": {}},
    ])
    p = tmp_path / "index.json"
    p.write_text(json.dumps(idx))
    results = search_notes(["horse"], p)
    assert results == []


def test_search_notes_empty_words(tmp_path):
    p = tmp_path / "index.json"
    p.write_text(json.dumps({"schema_version": "1.0.0", "notes": {}}))
    results = search_notes([], p)
    assert results == []


# ── format_context ─────────────────────────────────────────────────

def test_format_context_empty():
    assert format_context([]) == ""


def test_format_context_includes_title_and_label():
    notes = [{"note_id": "n1", "title": "Horse Guide", "body": "content here",
               "frontmatter": {"type": "github_reference", "repo": "o/r",
                                "branch": "main", "path": "guide.md"},
               "source_path": "github:o/r/main/guide.md"}]
    result = format_context(notes)
    assert "Horse Guide" in result
    assert "GitHub:" in result
    assert "content here" in result


def test_format_context_separates_vault_and_github():
    notes = [
        {"note_id": "v1", "title": "Vault Note", "body": "vault body",
         "frontmatter": {}, "source_path": "vault/note.md"},
        {"note_id": "g1", "title": "GitHub File", "body": "github body",
         "frontmatter": {"type": "github_reference", "repo": "o/r", "branch": "main", "path": "f.py"},
         "source_path": "github:o/r/main/f.py"},
    ]
    result = format_context(notes)
    assert "Vault ノート" in result
    assert "GitHub ファイル" in result
    assert result.index("Vault ノート") < result.index("GitHub ファイル")


def test_format_context_truncates_long_body():
    long_body = "x" * 2000
    notes = [{"note_id": "n1", "title": "T", "body": long_body,
               "frontmatter": {}, "source_path": "vault/t.md"}]
    result = format_context(notes, max_chars_per_note=100)
    assert "…" in result
    assert result.count("x") <= 100


def test_format_context_total_cap():
    # 10 件追加しても _DEFAULT_MAX_TOTAL_CHARS を超えない
    notes = [
        {"note_id": f"n{i}", "title": f"Note {i}", "body": "z" * 400,
         "frontmatter": {}, "source_path": f"vault/n{i}.md"}
        for i in range(10)
    ]
    result = format_context(notes)
    assert len(result) <= 3200  # 上限 3000 + マージン


# ── fetch_relevant_context ─────────────────────────────────────────

def test_fetch_skips_non_requirements_task(tmp_path):
    task = {"id": "t1", "title": "bug fix", "description": "", "tags": [], "section": "engineering"}
    result = fetch_relevant_context(task, tmp_path / "index.json")
    assert result == ""


def test_fetch_returns_empty_when_no_notes(tmp_path):
    p = tmp_path / "index.json"
    p.write_text(json.dumps({"schema_version": "1.0.0", "notes": {}}))
    task = {"id": "t1", "title": "Horse Racing Requirements", "description": "build prediction",
            "tags": ["requirements"], "section": "backlog"}
    result = fetch_relevant_context(task, p)
    assert result == ""


def test_fetch_returns_context_when_notes_match(tmp_path):
    idx = _make_index([
        {"note_id": "n1", "title": "Horse Racing Guide", "body": "racing prediction data",
         "confidential": False, "frontmatter": {}, "source_path": "vault/guide.md"},
    ])
    p = tmp_path / "index.json"
    p.write_text(json.dumps(idx))
    task = {"id": "t1", "title": "Horse Racing system requirements",
            "description": "racing prediction", "tags": ["requirements"], "section": "backlog"}
    result = fetch_relevant_context(task, p)
    assert "参考情報" in result
    assert "Horse Racing Guide" in result


def test_fetch_handles_exception_gracefully(tmp_path):
    task = {"id": "t1", "title": "requirements", "description": "",
            "tags": ["requirements"], "section": "backlog"}
    with patch("airflow_mcp.runner.context_fetcher.search_notes", side_effect=OSError("disk error")):
        result = fetch_relevant_context(task, tmp_path / "missing.json")
    assert result == ""


def test_fetch_returns_empty_for_unrecognized_section(tmp_path):
    """_should_fetch_context が False を返すタスクは早期 return する。"""
    task = {"id": "t1", "title": "sales report", "description": "quarterly", "tags": [], "section": "sales"}
    result = fetch_relevant_context(task, tmp_path / "index.json")
    assert result == ""


def test_fetch_returns_empty_when_no_query_words(tmp_path):
    """_build_query_words が空リストを返すとき early return する。"""
    p = tmp_path / "index.json"
    p.write_text('{"schema_version":"1.0.0","notes":{}}')
    # タイトルが単一文字のみ → words が空
    task = {"id": "t1", "title": "a b c", "description": "", "tags": ["requirements"], "section": "backlog"}
    result = fetch_relevant_context(task, p)
    assert result == ""

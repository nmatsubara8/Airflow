"""ingester/notes_index.py の単体テスト（DDD-04 §7）。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from airflow_mcp.ingester.notes_index import (
    delete_note,
    load_index,
    note_id_for_path,
    save_index,
    upsert_note,
)


# ── note_id_for_path ──────────────────────────────────

def test_note_id_for_path_starts_with_note_prefix():
    assert note_id_for_path("/some/path.md").startswith("note_")


def test_note_id_for_path_is_deterministic():
    assert note_id_for_path("/a/b.md") == note_id_for_path("/a/b.md")


def test_note_id_for_path_differs_for_different_paths():
    assert note_id_for_path("/a/b.md") != note_id_for_path("/a/c.md")


# ── load_index ────────────────────────────────────────

def test_load_index_returns_empty_when_no_file(tmp_path):
    idx = load_index(tmp_path / "nonexistent.json")
    assert idx["notes"] == {}
    assert "schema_version" in idx


def test_load_index_reads_existing_file(tmp_path):
    p = tmp_path / "index.json"
    p.write_text('{"schema_version": "1.0.0", "notes": {"note_abc": {}}}')
    idx = load_index(p)
    assert "note_abc" in idx["notes"]


def test_load_index_returns_empty_on_corrupt_json(tmp_path):
    p = tmp_path / "index.json"
    p.write_text("not json {{}")
    idx = load_index(p)
    assert idx["notes"] == {}


# ── save_index ────────────────────────────────────────

def test_save_index_creates_file(tmp_path):
    p = tmp_path / "sub" / "index.json"
    idx = {"schema_version": "1.0.0", "notes": {}}
    save_index(idx, p)
    assert p.exists()


def test_save_index_roundtrip(tmp_path):
    p = tmp_path / "index.json"
    idx = {"schema_version": "1.0.0", "notes": {"note_x": {"title": "Test"}}}
    save_index(idx, p)
    loaded = json.loads(p.read_text())
    assert loaded["notes"]["note_x"]["title"] == "Test"


def test_save_index_is_atomic(tmp_path):
    """保存中に tmp ファイルが残らないこと。"""
    p = tmp_path / "index.json"
    idx = {"schema_version": "1.0.0", "notes": {}}
    save_index(idx, p)
    tmp = p.with_suffix(".json.tmp")
    assert not tmp.exists()


# ── upsert_note ───────────────────────────────────────

def test_upsert_note_adds_entry():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    upsert_note(idx, note_id="note_abc", source_path="/a.md", title="A",
                content_hash="sha256:abc", frontmatter={})
    assert "note_abc" in idx["notes"]
    assert idx["notes"]["note_abc"]["title"] == "A"


def test_upsert_note_overwrites_existing():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    upsert_note(idx, note_id="note_abc", source_path="/a.md", title="Old",
                content_hash="sha256:old", frontmatter={})
    upsert_note(idx, note_id="note_abc", source_path="/a.md", title="New",
                content_hash="sha256:new", frontmatter={})
    assert idx["notes"]["note_abc"]["title"] == "New"


def test_upsert_note_stores_confidential_flag():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    upsert_note(idx, note_id="note_s", source_path="/s.md", title="S",
                content_hash="sha256:s", frontmatter={}, confidential=True)
    assert idx["notes"]["note_s"]["confidential"] is True


def test_upsert_note_stores_internal_links():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    upsert_note(idx, note_id="note_a", source_path="/a.md", title="A",
                content_hash="sha256:a", frontmatter={},
                internal_links=["note_b", "note_c"])
    assert idx["notes"]["note_a"]["internal_links"] == ["note_b", "note_c"]


def test_upsert_note_has_last_indexed():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    upsert_note(idx, note_id="note_t", source_path="/t.md", title="T",
                content_hash="sha256:t", frontmatter={})
    assert "last_indexed" in idx["notes"]["note_t"]


# ── delete_note ───────────────────────────────────────

def test_delete_note_removes_entry():
    idx: dict = {"schema_version": "1.0.0", "notes": {"note_x": {"title": "X"}}}
    result = delete_note(idx, "note_x")
    assert result is True
    assert "note_x" not in idx["notes"]


def test_delete_note_returns_false_for_nonexistent():
    idx: dict = {"schema_version": "1.0.0", "notes": {}}
    assert delete_note(idx, "note_missing") is False


# ── _DateTimeEncoder ──────────────────────────────────

def test_save_index_serializes_datetime_in_frontmatter(tmp_path):
    """frontmatter に datetime オブジェクトが含まれても save_index がクラッシュしないこと。"""
    import datetime
    p = tmp_path / "index.json"
    idx = {
        "schema_version": "1.0.0",
        "notes": {
            "note_dt": {
                "frontmatter": {"processed_at": datetime.datetime(2026, 5, 23, tzinfo=datetime.timezone.utc)},
            }
        },
    }
    save_index(idx, p)
    loaded = json.loads(p.read_text())
    assert "2026-05-23" in loaded["notes"]["note_dt"]["frontmatter"]["processed_at"]


def test_datetime_encoder_raises_for_unserializable_type(tmp_path):
    """_DateTimeEncoder が未知の型に対して super().default() を呼び TypeError を送出すること。"""
    from airflow_mcp.ingester.notes_index import _DateTimeEncoder
    encoder = _DateTimeEncoder()
    with pytest.raises(TypeError):
        encoder.default(object())


def test_save_index_oserror_reraises(tmp_path):
    """save_index が OSError を送出し tmp ファイルを削除して再送出すること（lines 48-51）。"""
    from unittest.mock import patch
    from airflow_mcp.ingester.notes_index import save_index
    idx = {"schema_version": "1.0.0", "notes": {}}
    p = tmp_path / "index.json"
    with patch.object(Path, "write_text", side_effect=OSError("no space")):
        with pytest.raises(OSError, match="no space"):
            save_index(idx, p)

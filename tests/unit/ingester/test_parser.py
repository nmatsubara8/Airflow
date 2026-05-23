"""ingester/parser.py の単体テスト（DDD-04 §5）。"""
from __future__ import annotations

from pathlib import Path


from airflow_mcp.ingester.parser import frontmatter_to_task_fields, parse_markdown


# ── parse_markdown ────────────────────────────────────────

def test_parse_markdown_extracts_title_from_frontmatter(tmp_path: Path):
    md = tmp_path / "test.md"
    md.write_text("---\ntitle: テストタイトル\ntags: [a, b]\n---\n# 本文")
    parsed = parse_markdown(md)
    assert parsed["title"] == "テストタイトル"


def test_parse_markdown_extracts_title_from_heading_when_no_frontmatter(tmp_path: Path):
    md = tmp_path / "heading.md"
    md.write_text("# 見出しタイトル\n\n本文テキスト")
    parsed = parse_markdown(md)
    assert parsed["title"] == "見出しタイトル"


def test_parse_markdown_uses_filename_as_fallback_title(tmp_path: Path):
    md = tmp_path / "my-document.md"
    md.write_text("frontmatterなし\n本文のみ")
    parsed = parse_markdown(md)
    assert parsed["title"] == "my-document"


def test_parse_markdown_computes_content_hash(tmp_path: Path):
    md = tmp_path / "hash.md"
    md.write_text("# タイトル\n\n本文")
    parsed = parse_markdown(md)
    h = parsed["content_hash"]
    assert h.startswith("sha256:"), f"content_hash は 'sha256:' で始まること: {h}"
    hex_part = h[len("sha256:"):]
    assert len(hex_part) == 64, f"sha256 hex は 64 文字であること: got {len(hex_part)}"
    assert all(c in "0123456789abcdef" for c in hex_part)


def test_parse_markdown_content_hash_deterministic(tmp_path: Path):
    content = "# 同じ内容\n\nbody text"
    md1 = tmp_path / "file1.md"
    md2 = tmp_path / "file2.md"
    md1.write_text(content)
    md2.write_text(content)
    assert parse_markdown(md1)["content_hash"] == parse_markdown(md2)["content_hash"]


def test_parse_markdown_content_hash_differs_for_different_content(tmp_path: Path):
    md1 = tmp_path / "a.md"
    md2 = tmp_path / "b.md"
    md1.write_text("# タイトルA\n本文A")
    md2.write_text("# タイトルB\n本文B")
    assert parse_markdown(md1)["content_hash"] != parse_markdown(md2)["content_hash"]


def test_parse_markdown_returns_source_path(tmp_path: Path):
    md = tmp_path / "doc.md"
    md.write_text("---\ntitle: パス確認\n---\n本文")
    parsed = parse_markdown(md)
    assert str(md) in parsed["source_path"]


def test_parse_markdown_handles_invalid_yaml_frontmatter_gracefully(tmp_path: Path):
    md = tmp_path / "bad.md"
    md.write_text("---\ntitle: [unclosed bracket\n---\n本文")
    parsed = parse_markdown(md)
    # 不正 YAML でも parse_markdown がクラッシュせず、本文からタイトルを取得すること
    assert parsed["title"] is not None


def test_parse_markdown_handles_empty_file(tmp_path: Path):
    md = tmp_path / "empty.md"
    md.write_text("")
    parsed = parse_markdown(md)
    assert parsed["title"] is not None  # ファイル名フォールバック


# ── frontmatter_to_task_fields ────────────────────────────

def test_frontmatter_to_task_fields_maps_section(tmp_path: Path):
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\nsection: business\ntags: [x]\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["section"] == "business"


def test_frontmatter_to_task_fields_default_section_is_none(tmp_path: Path):
    # section 未指定の場合は None を返し、dispatcher が section を決定する（DDD-04 §6.4）
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["section"] is None


def test_frontmatter_to_task_fields_maps_tags(tmp_path: Path):
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\ntags: [api, backend]\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert "api" in fields["tags"]
    assert "backend" in fields["tags"]


def test_frontmatter_to_task_fields_description_truncated_to_500(tmp_path: Path):
    long_body = "x" * 1000
    md = tmp_path / "t.md"
    md.write_text(f"---\ntitle: T\n---\n{long_body}")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert len(fields["description"]) <= 500


def test_frontmatter_to_task_fields_includes_content_hash(tmp_path: Path):
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert "content_hash" in fields
    assert fields["content_hash"].startswith("sha256:")


def test_frontmatter_to_task_fields_maps_priority(tmp_path: Path):
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\npriority: 2\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["priority"] == 2


def test_frontmatter_to_task_fields_maps_features(tmp_path: Path):
    md = tmp_path / "t.md"
    md.write_text(
        "---\ntitle: T\nfeatures:\n  required_accuracy: high\n  latency_sensitivity: interactive\n---\n本文"
    )
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["features"]["required_accuracy"] == "high"


def test_frontmatter_to_task_fields_tags_as_comma_string(tmp_path: Path):
    """tags が カンマ区切り文字列のとき list に変換されること（line 74）。"""
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\ntags: api, backend, docs\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["tags"] == ["api", "backend", "docs"]


def test_frontmatter_to_task_fields_top_level_required_accuracy(tmp_path: Path):
    """required_accuracy が features: 外のトップレベルに書かれても features に入ること（line 79）。"""
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\nrequired_accuracy: medium\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["features"]["required_accuracy"] == "medium"


def test_frontmatter_to_task_fields_top_level_latency_sensitivity(tmp_path: Path):
    """latency_sensitivity がトップレベルに書かれても features に入ること（line 81）。"""
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\nlatency_sensitivity: batch\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["features"]["latency_sensitivity"] == "batch"


def test_frontmatter_to_task_fields_top_level_task_type(tmp_path: Path):
    """task_type がトップレベルに書かれても features に入ること（line 83）。"""
    md = tmp_path / "t.md"
    md.write_text("---\ntitle: T\ntask_type: summarize\n---\n本文")
    parsed = parse_markdown(md)
    fields = frontmatter_to_task_fields(parsed)
    assert fields["features"]["task_type"] == "summarize"

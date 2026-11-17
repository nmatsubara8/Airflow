"""GitHub reference ingester のユニットテスト。"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from airflow_mcp.ingester.github import (
    _file_title,
    _should_include,
    fetch_file_content,
    fetch_repo_tree,
    run_github_reference_once,
)
from airflow_mcp.ingester.notes_index import load_index

import base64 as _base64


# ── _should_include ───────────────────────────────────────────────


@pytest.mark.parametrize("path,allowed_paths,exts,exclude,expected", [
    ("README.md",        [],          {".md"},       [],                   True),
    ("src/main.py",      [],          {".py"},       [],                   True),
    ("src/main.py",      [],          {".md"},       [],                   False),
    ("src/main.py",      ["src/"],    {".py"},       [],                   True),
    ("tests/test_x.py",  ["src/"],    {".py"},       [],                   False),
    ("README.md",        ["docs/"],   {".md"},       [],                   False),
    ("src/main.py",      [],          {".py"},       ["**/test*"],         True),
    ("tests/test_x.py",  [],          {".py"},       ["tests/**"],         False),
    ("a/__pycache__/x.py", [],        {".py"},       ["**/__pycache__/**"], False),
])
def test_should_include(path, allowed_paths, exts, exclude, expected):
    assert _should_include(path, allowed_paths, exts, exclude) == expected


# ── _file_title ───────────────────────────────────────────────────


def test_file_title_from_h1():
    assert _file_title("README.md", "# My Project\nsome body") == "My Project"


def test_file_title_falls_back_to_filename():
    assert _file_title("README.md", "no heading here") == "README.md"
    assert _file_title("src/main.py", "def foo(): pass") == "main.py"


# ── fetch_repo_tree ────────────────────────────────────────────────


def _mock_tree_response(blobs: list[str]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {
        "tree": [
            {"type": "blob", "path": p} for p in blobs
        ] + [{"type": "tree", "path": "dir"}]
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_fetch_repo_tree_returns_blobs_only():
    with patch("httpx.Client") as mock_cls:
        mock_client = mock_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_tree_response(["README.md", "src/main.py"])
        result = fetch_repo_tree("owner", "repo", "main", token=None)
    assert len(result) == 2
    assert all(item["type"] == "blob" for item in result)


# ── fetch_file_content ────────────────────────────────────────────


def _mock_content_response(content: str, size: int | None = None) -> MagicMock:
    encoded = _base64.b64encode(content.encode()).decode()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "content": encoded + "\n",
        "size": size if size is not None else len(content),
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_fetch_file_content_decodes_base64():
    with patch("httpx.Client") as mock_cls:
        mock_client = mock_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_content_response("hello world")
        result = fetch_file_content("owner", "repo", "README.md", "main", token=None)
    assert result == "hello world"


def test_fetch_file_content_returns_none_if_too_large():
    with patch("httpx.Client") as mock_cls:
        mock_client = mock_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_content_response("x", size=600_000)
        result = fetch_file_content("owner", "repo", "big.py", "main", token=None)
    assert result is None


def test_fetch_file_content_returns_none_on_404():
    resp = MagicMock()
    resp.status_code = 404
    resp.raise_for_status = MagicMock()
    with patch("httpx.Client") as mock_cls:
        mock_client = mock_cls.return_value.__enter__.return_value
        mock_client.get.return_value = resp
        result = fetch_file_content("owner", "repo", "missing.md", "main", token=None)
    assert result is None


# ── run_github_reference_once ─────────────────────────────────────


def _patch_github(tree_files: list[str], file_contents: dict[str, str]):
    """fetch_repo_tree と fetch_file_content を同時にモックするコンテキスト。"""
    def fake_tree(owner, repo_name, branch, token):
        return [{"type": "blob", "path": p} for p in tree_files]

    def fake_content(owner, repo_name, path, branch, token):
        return file_contents.get(path)

    return (
        patch("airflow_mcp.ingester.github.fetch_repo_tree", side_effect=fake_tree),
        patch("airflow_mcp.ingester.github.fetch_file_content", side_effect=fake_content),
    )


def test_run_reference_indexes_files(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    tree = ["README.md", "src/main.py"]
    contents = {"README.md": "# Hello\ncontent", "src/main.py": "def foo(): pass"}

    p1, p2 = _patch_github(tree, contents)
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo", "branch": "main"}],
            notes_index_path=str(index_path),
        )

    assert count == 2
    index = load_index(index_path)
    notes = index["notes"]
    assert len(notes) == 2
    titles = {n["title"] for n in notes.values()}
    assert "Hello" in titles   # H1 から抽出
    assert "main.py" in titles


def test_run_reference_dedup_skips_unchanged(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    tree = ["README.md"]
    contents = {"README.md": "same content"}

    p1, p2 = _patch_github(tree, contents)
    with p1, p2:
        first = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )
        second = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )

    assert first == 1
    assert second == 0  # 内容変化なし → スキップ


def test_run_reference_reindexes_on_content_change(tmp_path):
    index_path = tmp_path / "notes" / "index.json"

    p1, p2 = _patch_github(["README.md"], {"README.md": "version 1"})
    with p1, p2:
        run_github_reference_once([{"repo": "owner/repo"}], notes_index_path=str(index_path))

    p1, p2 = _patch_github(["README.md"], {"README.md": "version 2 updated"})
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )

    assert count == 1  # 変更があれば再索引化


def test_run_reference_respects_file_types(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    tree = ["README.md", "src/main.py", "data.json"]
    contents = {"README.md": "md content", "src/main.py": "py content", "data.json": "{}"}

    p1, p2 = _patch_github(tree, contents)
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo", "file_types": [".md"]}],
            notes_index_path=str(index_path),
        )

    assert count == 1  # .md のみ


def test_run_reference_respects_paths_filter(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    tree = ["README.md", "docs/guide.md", "src/main.py"]
    contents = {"README.md": "root", "docs/guide.md": "guide", "src/main.py": "code"}

    p1, p2 = _patch_github(tree, contents)
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo", "paths": ["docs/"], "file_types": [".md"]}],
            notes_index_path=str(index_path),
        )

    assert count == 1
    index = load_index(index_path)
    note = next(iter(index["notes"].values()))
    assert note["title"] == "guide.md"


def test_run_reference_stores_body_and_metadata(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    p1, p2 = _patch_github(["README.md"], {"README.md": "# Title\nsome body text"})
    with p1, p2:
        run_github_reference_once(
            [{"repo": "owner/repo", "branch": "develop"}],
            notes_index_path=str(index_path),
        )

    note = next(iter(load_index(index_path)["notes"].values()))
    assert "some body text" in note["body"]
    assert note["frontmatter"]["type"] == "github_reference"
    assert note["frontmatter"]["repo"] == "owner/repo"
    assert note["frontmatter"]["branch"] == "develop"
    assert note["frontmatter"]["language"] == "md"


def test_run_reference_respects_max_files(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    tree = [f"file{i}.md" for i in range(10)]
    contents = {f"file{i}.md": f"content {i}" for i in range(10)}

    p1, p2 = _patch_github(tree, contents)
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo", "max_files": 3}],
            notes_index_path=str(index_path),
        )

    assert count == 3


def test_run_reference_skips_invalid_repo(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    count = run_github_reference_once(
        [{"repo": "no-slash"}], notes_index_path=str(index_path)
    )
    assert count == 0


def test_run_reference_handles_tree_http_error(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    with patch(
        "airflow_mcp.ingester.github.fetch_repo_tree",
        side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock(status_code=404)),
    ):
        count = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )
    assert count == 0


def test_run_reference_handles_network_error(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    with patch(
        "airflow_mcp.ingester.github.fetch_repo_tree",
        side_effect=httpx.ConnectError("connection refused"),
    ):
        count = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )
    assert count == 0


def test_fetch_file_content_handles_decode_error():
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"content": "!!!invalid-base64!!!", "size": 10}
    resp.raise_for_status = MagicMock()
    with patch("httpx.Client") as mock_cls:
        mock_client = mock_cls.return_value.__enter__.return_value
        mock_client.get.return_value = resp
        result = fetch_file_content("owner", "repo", "bad.md", "main", token=None)
    assert result is None


def test_build_client_sets_auth_header():
    with patch("httpx.Client") as mock_cls:
        mock_cls.return_value.__enter__.return_value.get.return_value = _mock_tree_response([])
        fetch_repo_tree("owner", "repo", "main", token="ghp_secret")
    headers = mock_cls.call_args.kwargs["headers"]
    assert headers.get("Authorization") == "Bearer ghp_secret"


def test_run_reference_skips_none_content(tmp_path):
    index_path = tmp_path / "notes" / "index.json"
    p1, p2 = _patch_github(["big.md"], {"big.md": None})
    with p1, p2:
        count = run_github_reference_once(
            [{"repo": "owner/repo"}], notes_index_path=str(index_path)
        )
    assert count == 0

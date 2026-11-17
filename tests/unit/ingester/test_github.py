"""GitHub ingester のユニットテスト。"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from airflow_mcp.ingester.github import (
    _content_hash,
    _infer_section,
    _parse_link_next,
    fetch_github_items,
    github_item_to_task_fields,
    run_github_once,
)
from airflow_mcp.taskboard.store import TaskStore


# ── フィクスチャ ───────────────────────────────────────────────────


def _make_issue(number: int = 1, labels: list[str] | None = None, updated_at: str = "2026-01-01T00:00:00Z") -> dict[str, Any]:
    return {
        "number": number,
        "title": f"Test issue {number}",
        "body": f"Body of issue {number}",
        "html_url": f"https://github.com/owner/repo/issues/{number}",
        "updated_at": updated_at,
        "labels": [{"name": lb} for lb in (labels or [])],
        "milestone": None,
        "state": "open",
    }


def _make_pr(number: int = 10) -> dict[str, Any]:
    return {
        "number": number,
        "title": f"Test PR {number}",
        "body": "PR body",
        "html_url": f"https://github.com/owner/repo/pull/{number}",
        "updated_at": "2026-01-01T00:00:00Z",
        "labels": [],
        "milestone": None,
        "pull_request": {"url": "..."},
    }


# ── _content_hash ────────────────────────────────────────────────


def test_content_hash_stable_by_default():
    h1 = _content_hash("owner", "repo", "issues", 1, "2026-01-01", False)
    h2 = _content_hash("owner", "repo", "issues", 1, "2026-02-01", False)
    assert h1 == h2  # updated_at が変わっても同じ


def test_content_hash_track_updates():
    h1 = _content_hash("owner", "repo", "issues", 1, "2026-01-01", True)
    h2 = _content_hash("owner", "repo", "issues", 1, "2026-02-01", True)
    assert h1 != h2  # updated_at が変わると別ハッシュ


def test_content_hash_different_repos():
    h1 = _content_hash("owner", "repo_a", "issues", 1, "", False)
    h2 = _content_hash("owner", "repo_b", "issues", 1, "", False)
    assert h1 != h2


# ── _infer_section ────────────────────────────────────────────────


@pytest.mark.parametrize("labels,expected", [
    (["bug"], "engineering"),
    (["enhancement"], "engineering"),
    (["docs"], "content"),
    (["documentation"], "content"),
    (["business"], "business"),
    (["legal"], "business"),
    (["workflow"], "orchestration"),
    (["unknown-label"], "engineering"),
    ([], "engineering"),
])
def test_infer_section_from_labels(labels, expected):
    assert _infer_section(labels, None) == expected


def test_infer_section_override_takes_priority():
    assert _infer_section(["bug"], "content") == "content"


# ── _parse_link_next ──────────────────────────────────────────────


def test_parse_link_next_found():
    header = '<https://api.github.com/repos/o/r/issues?page=2>; rel="next", <https://api.github.com/repos/o/r/issues?page=5>; rel="last"'
    assert _parse_link_next(header) == "https://api.github.com/repos/o/r/issues?page=2"


def test_parse_link_next_not_found():
    assert _parse_link_next("") is None
    assert _parse_link_next('<https://...>; rel="last"') is None


# ── github_item_to_task_fields ────────────────────────────────────


def test_issue_to_task_fields_basic():
    issue = _make_issue(number=42, labels=["bug"])
    fields = github_item_to_task_fields(issue, "owner", "repo", "issues")

    assert fields["title"] == "Test issue 42"
    assert fields["section"] == "engineering"
    assert fields["tags"] == ["bug"]
    assert fields["priority"] == 5
    assert fields["source_note_id"] == "github:owner/repo/issues/42"
    assert fields["content_hash"].startswith("sha256:")
    assert fields["_github_number"] == 42
    assert "issues/42" in fields["_github_url"]


def test_pr_to_task_fields():
    pr = _make_pr(number=7)
    fields = github_item_to_task_fields(pr, "owner", "repo", "pull_requests")
    assert fields["source_note_id"] == "github:owner/repo/pull_requests/7"


def test_milestone_sets_priority_3():
    issue = _make_issue()
    issue["milestone"] = {"title": "v1.0"}
    fields = github_item_to_task_fields(issue, "owner", "repo", "issues")
    assert fields["priority"] == 3


def test_body_truncated_to_500():
    issue = _make_issue()
    issue["body"] = "x" * 1000
    fields = github_item_to_task_fields(issue, "owner", "repo", "issues")
    assert len(fields["description"]) == 500


def test_none_body_becomes_empty():
    issue = _make_issue()
    issue["body"] = None
    fields = github_item_to_task_fields(issue, "owner", "repo", "issues")
    assert fields["description"] == ""


# ── fetch_github_items (モック) ───────────────────────────────────


def _mock_response(items: list[dict], link: str = "") -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = items
    resp.headers = {"Link": link}
    resp.raise_for_status = MagicMock()
    return resp


def test_fetch_issues_filters_prs(monkeypatch):
    issues = [_make_issue(1), _make_pr(10)]  # PR は除外されるはず
    with patch("httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_response(issues)
        result = fetch_github_items("owner/repo", "issues", token=None)
    assert len(result) == 1
    assert result[0]["number"] == 1


def test_fetch_pull_requests(monkeypatch):
    prs = [_make_pr(1), _make_pr(2)]
    with patch("httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_response(prs)
        result = fetch_github_items("owner/repo", "pull_requests", token=None)
    assert len(result) == 2


def test_fetch_pagination(monkeypatch):
    page1 = [_make_issue(i) for i in range(1, 4)]
    page2 = [_make_issue(i) for i in range(4, 6)]
    link_header = '<https://api.github.com/repos/o/r/issues?page=2>; rel="next"'

    with patch("httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.side_effect = [
            _mock_response(page1, link_header),
            _mock_response(page2),
        ]
        result = fetch_github_items("owner/repo", "issues", token=None, max_items=10)
    assert len(result) == 5


def test_fetch_respects_max_items():
    items = [_make_issue(i) for i in range(1, 6)]
    with patch("httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_response(items)
        result = fetch_github_items("owner/repo", "issues", token=None, max_items=3)
    assert len(result) == 3


# ── run_github_once ───────────────────────────────────────────────


def test_run_github_once_creates_tasks(tmp_path):
    store = TaskStore(tmp_path)
    issues = [_make_issue(1, labels=["bug"]), _make_issue(2)]

    with patch("airflow_mcp.ingester.github.fetch_github_items", return_value=issues):
        created = run_github_once(store, [{"repo": "owner/repo", "types": ["issues"]}])

    assert created == 2
    tasks = store.list_by_status("inbox")
    assert len(tasks) == 2
    assert all(t["section"] == "engineering" for t in tasks)


def test_run_github_once_dedup(tmp_path):
    store = TaskStore(tmp_path)
    issue = _make_issue(1)

    with patch("airflow_mcp.ingester.github.fetch_github_items", return_value=[issue]):
        first = run_github_once(store, [{"repo": "owner/repo", "types": ["issues"]}])
        second = run_github_once(store, [{"repo": "owner/repo", "types": ["issues"]}])

    assert first == 1
    assert second == 0  # dedup により再作成なし


def test_run_github_once_skips_invalid_repo(tmp_path):
    store = TaskStore(tmp_path)
    created = run_github_once(store, [{"repo": "invalid-no-slash"}])
    assert created == 0


def test_run_github_once_handles_http_error(tmp_path):
    store = TaskStore(tmp_path)
    with patch(
        "airflow_mcp.ingester.github.fetch_github_items",
        side_effect=httpx.HTTPStatusError("403", request=MagicMock(), response=MagicMock(status_code=403)),
    ):
        created = run_github_once(store, [{"repo": "owner/repo", "types": ["issues"]}])
    assert created == 0  # エラーでも例外を外に出さない


def test_run_github_once_handles_network_error(tmp_path):
    store = TaskStore(tmp_path)
    with patch(
        "airflow_mcp.ingester.github.fetch_github_items",
        side_effect=httpx.ConnectError("connection refused"),
    ):
        created = run_github_once(store, [{"repo": "owner/repo", "types": ["issues"]}])
    assert created == 0


def test_run_github_once_empty_config(tmp_path):
    store = TaskStore(tmp_path)
    assert run_github_once(store, []) == 0


def test_run_github_once_section_override(tmp_path):
    store = TaskStore(tmp_path)
    issue = _make_issue(1, labels=["bug"])

    with patch("airflow_mcp.ingester.github.fetch_github_items", return_value=[issue]):
        run_github_once(store, [{"repo": "owner/repo", "types": ["issues"], "section": "content"}])

    task = store.list_by_status("inbox")[0]
    assert task["section"] == "content"


def test_fetch_with_token_and_labels():
    issues = [_make_issue(1)]
    with patch("httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value = _mock_response(issues)
        result = fetch_github_items(
            "owner/repo", "issues", token="ghp_test",
            labels=["bug", "enhancement"],
        )
    assert len(result) == 1
    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["params"]["labels"] == "bug,enhancement"


def test_run_github_once_uses_token_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_GH_TOKEN", "ghp_test123")
    store = TaskStore(tmp_path)

    captured: list[str | None] = []

    def fake_fetch(repo, item_type, token, **kwargs):
        captured.append(token)
        return []

    with patch("airflow_mcp.ingester.github.fetch_github_items", side_effect=fake_fetch):
        run_github_once(store, [{"repo": "owner/repo", "types": ["issues"], "token_env": "MY_GH_TOKEN"}])

    assert captured[0] == "ghp_test123"

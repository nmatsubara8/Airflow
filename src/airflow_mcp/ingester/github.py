"""GitHub Issues / PR → タスク自動生成。

GitHub REST API からイシューまたは PR を取得し、Vault ingester と同じ
TaskStore に投入する。dedup は content_hash（stable key / update-aware）で行う。

設定例（ingester.yaml）:
  github_sources:
    - repo: "owner/repo"
      types: [issues, pull_requests]
      state: open
      labels: []
      section: engineering
      track_updates: false
      max_items: 100
      token_env: GITHUB_TOKEN
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import Any

import httpx

from ..common.audit import log_event, new_audit_id
from ..taskboard.store import TaskStore

logger = logging.getLogger(__name__)

_GITHUB_API = "https://api.github.com"

# GitHub ラベル名 → section マッピング
_LABEL_SECTION_MAP: dict[str, str] = {
    "bug": "engineering",
    "enhancement": "engineering",
    "refactor": "engineering",
    "performance": "engineering",
    "security": "engineering",
    "docs": "content",
    "documentation": "content",
    "content": "content",
    "business": "business",
    "legal": "business",
    "workflow": "orchestration",
}


def _stable_key(owner: str, repo: str, item_type: str, number: int) -> str:
    return f"github:{owner}/{repo}/{item_type}/{number}"


def _content_hash(
    owner: str, repo: str, item_type: str, number: int,
    updated_at: str, track_updates: bool,
) -> str:
    key = _stable_key(owner, repo, item_type, number)
    if track_updates:
        key += f"@{updated_at}"
    return "sha256:" + hashlib.sha256(key.encode()).hexdigest()


def _infer_section(labels: list[str], override: str | None) -> str:
    if override:
        return override
    for label in labels:
        section = _LABEL_SECTION_MAP.get(label.lower())
        if section:
            return section
    return "engineering"


def _parse_link_next(link_header: str) -> str | None:
    """Link ヘッダーから next ページ URL を抽出する。"""
    m = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
    return m.group(1) if m else None


def fetch_github_items(
    repo: str,
    item_type: str,
    token: str | None,
    *,
    state: str = "open",
    labels: list[str] | None = None,
    max_items: int = 100,
) -> list[dict[str, Any]]:
    """GitHub API から issues または pull_requests を取得する。

    Args:
        repo:      "owner/repo" 形式
        item_type: "issues" または "pull_requests"
        token:     GitHub API トークン（None で未認証・レート制限 60req/h）
        state:     "open" | "closed" | "all"
        labels:    ラベルフィルター（空/None で全件）
        max_items: 最大取得件数
    """
    owner, repo_name = repo.split("/", 1)
    endpoint = "pulls" if item_type == "pull_requests" else "issues"
    url: str | None = f"{_GITHUB_API}/repos/{owner}/{repo_name}/{endpoint}"

    headers: dict[str, str] = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params: dict[str, Any] = {"state": state, "per_page": min(max_items, 100)}
    if labels:
        params["labels"] = ",".join(labels)

    all_items: list[dict[str, Any]] = []
    with httpx.Client(headers=headers, timeout=30.0) as client:
        first = True
        while url and len(all_items) < max_items:
            resp = client.get(url, params=params if first else {})
            resp.raise_for_status()
            batch: list[dict[str, Any]] = resp.json()
            # issues エンドポイントは PR も返すので除外
            if item_type == "issues":
                batch = [i for i in batch if "pull_request" not in i]
            all_items.extend(batch)
            url = _parse_link_next(resp.headers.get("Link", ""))
            first = False

    return all_items[:max_items]


def github_item_to_task_fields(
    item: dict[str, Any],
    owner: str,
    repo_name: str,
    item_type: str,
    *,
    section_override: str | None = None,
    track_updates: bool = False,
) -> dict[str, Any]:
    """GitHub API レスポンスをタスクフィールドに変換する。"""
    labels = [lb["name"] for lb in item.get("labels", [])]
    number: int = item["number"]
    title: str = item.get("title") or f"#{number}"
    body: str = item.get("body") or ""
    updated_at: str = item.get("updated_at", "")
    html_url: str = item.get("html_url", "")

    milestone = item.get("milestone")
    priority = 3 if milestone else 5

    return {
        "title": title,
        "description": body[:500],
        "tags": labels,
        "priority": priority,
        "section": _infer_section(labels, section_override),
        "features": {},
        "source_note_id": _stable_key(owner, repo_name, item_type, number),
        "content_hash": _content_hash(
            owner, repo_name, item_type, number, updated_at, track_updates
        ),
        "confidential": False,
        "_raw_body": body,
        "_source_path": html_url,
        "_github_number": number,
        "_github_url": html_url,
    }


def run_github_once(
    store: TaskStore,
    github_configs: list[dict[str, Any]],
) -> int:
    """設定された GitHub ソースをスキャンしてタスクを作成する。作成数を返す。"""
    # inbox / in_progress の content_hash を dedup セットに展開
    active_hashes: set[str] = set()
    for status in ("inbox", "in_progress"):
        for t in store.list_by_status(status):
            ch = t.get("content_hash")
            if ch:
                active_hashes.add(ch)

    created = 0
    for cfg in github_configs:
        repo: str = cfg.get("repo", "")
        if not repo or "/" not in repo:
            logger.warning("github_sources: invalid repo %r — skipping", repo)
            continue

        owner, repo_name = repo.split("/", 1)
        token = os.environ.get(cfg.get("token_env", "GITHUB_TOKEN") or "GITHUB_TOKEN")
        item_types: list[str] = cfg.get("types", ["issues"])
        state: str = cfg.get("state", "open")
        labels: list[str] = cfg.get("labels") or []
        section_override: str | None = cfg.get("section")
        track_updates: bool = bool(cfg.get("track_updates", False))
        max_items: int = int(cfg.get("max_items", 100))

        for item_type in item_types:
            try:
                items = fetch_github_items(
                    repo, item_type, token,
                    state=state,
                    labels=labels or None,
                    max_items=max_items,
                )
            except httpx.HTTPStatusError as exc:
                logger.error("github fetch error %s/%s: HTTP %s", repo, item_type, exc.response.status_code)
                continue
            except httpx.RequestError as exc:
                logger.error("github fetch network error %s/%s: %s", repo, item_type, exc)
                continue

            for item in items:
                fields = github_item_to_task_fields(
                    item, owner, repo_name, item_type,
                    section_override=section_override,
                    track_updates=track_updates,
                )
                if fields["content_hash"] in active_hashes:
                    continue

                task = store.create({**fields, "owner": None})
                active_hashes.add(fields["content_hash"])

                log_event(
                    "info", "task.ingested", "github_ingester", new_audit_id(),
                    task_id=task["id"],
                    data={
                        "repo": repo,
                        "type": item_type,
                        "number": fields["_github_number"],
                        "title": task["title"],
                    },
                )
                logger.info(
                    "github.ingested: %s#%d → %s",
                    repo, fields["_github_number"], task["id"],
                )
                created += 1

    return created

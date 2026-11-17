"""GitHub Issues / PR → タスク自動生成 + リポジトリファイル参照索引化。

github_sources:
  Issues / PR を TaskStore に投入する（デフォルト dedup: stable key）。

github_reference_sources:
  コード・README 等のファイルを notes index に登録して
  agents が notes.search で参照できるようにする。

設定例（ingester.yaml）:
  github_sources:
    - repo: "owner/repo"
      types: [issues, pull_requests]
      state: open
      token_env: GITHUB_TOKEN

  github_reference_sources:
    - repo: "owner/repo"
      branch: main
      paths: ["README.md", "docs/", "src/"]
      file_types: [".md", ".py"]
      exclude: ["**/__pycache__/**", "**/test*"]
      token_env: GITHUB_TOKEN
      max_files: 200
"""
from __future__ import annotations

import base64
import fnmatch
import hashlib
import logging
import os
import re
from pathlib import PurePosixPath
from typing import Any

import httpx

from ..common.audit import log_event, new_audit_id
from ..taskboard.store import TaskStore
from .notes_index import load_index, note_id_for_path, save_index, upsert_note

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


# ── GitHub Reference Source（ファイル索引化）────────────────────────────────


_MAX_FILE_BYTES = 524_288  # 512KB — Vault ingester と同じ上限
_DEFAULT_FILE_TYPES = {".md", ".txt", ".rst", ".py", ".ts", ".js", ".go", ".java", ".yaml", ".toml"}
_NOTES_INDEX_PATH_DEFAULT = ".airflow/notes/index.json"


def _build_client(token: str | None) -> httpx.Client:
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(headers=headers, timeout=30.0)


def fetch_repo_tree(
    owner: str,
    repo_name: str,
    branch: str,
    token: str | None,
) -> list[dict[str, Any]]:
    """GitHub Trees API でリポジトリの全ファイル一覧を取得する。"""
    url = f"{_GITHUB_API}/repos/{owner}/{repo_name}/git/trees/{branch}?recursive=1"
    with _build_client(token) as client:
        resp = client.get(url)
        resp.raise_for_status()
    data = resp.json()
    return [item for item in data.get("tree", []) if item.get("type") == "blob"]


def fetch_file_content(
    owner: str,
    repo_name: str,
    path: str,
    branch: str,
    token: str | None,
) -> str | None:
    """GitHub Contents API でファイル内容を取得して文字列で返す。
    ファイルが大きすぎる場合や取得失敗時は None を返す。
    """
    url = f"{_GITHUB_API}/repos/{owner}/{repo_name}/contents/{path}"
    with _build_client(token) as client:
        resp = client.get(url, params={"ref": branch})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()

    data = resp.json()
    size: int = data.get("size", 0)
    if size > _MAX_FILE_BYTES:
        logger.debug("skip oversized file %s (%d bytes)", path, size)
        return None

    encoded = data.get("content", "")
    try:
        raw_bytes = base64.b64decode(encoded.replace("\n", ""))
        return raw_bytes.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("failed to decode %s: %s", path, exc)
        return None


def _should_include(
    file_path: str,
    allowed_paths: list[str],
    allowed_exts: set[str],
    exclude_patterns: list[str],
) -> bool:
    """ファイルを索引化対象にするか判定する。"""
    ext = PurePosixPath(file_path).suffix.lower()
    if ext not in allowed_exts:
        return False
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(file_path, pattern.lstrip("/")):
            return False
    if not allowed_paths:
        return True
    for p in allowed_paths:
        if file_path == p or file_path.startswith(p.rstrip("/") + "/"):
            return True
    return False


def _file_title(file_path: str, content: str) -> str:
    """ファイルパスまたは Markdown H1 からタイトルを決定する。"""
    if file_path.endswith(".md"):
        for line in content.splitlines():
            if line.startswith("# "):
                return line[2:].strip()
    return PurePosixPath(file_path).name


def run_github_reference_once(
    ref_configs: list[dict[str, Any]],
    notes_index_path: str = _NOTES_INDEX_PATH_DEFAULT,
) -> int:
    """GitHub リポジトリのファイルを notes index に登録する。更新件数を返す。"""
    from pathlib import Path as _Path

    index_path = _Path(notes_index_path)
    notes_index = load_index(index_path)
    existing_hashes: dict[str, str] = {
        nid: n["content_hash"]
        for nid, n in notes_index.get("notes", {}).items()
    }

    indexed = 0
    for cfg in ref_configs:
        repo: str = cfg.get("repo", "")
        if not repo or "/" not in repo:
            logger.warning("github_reference_sources: invalid repo %r — skipping", repo)
            continue

        owner, repo_name = repo.split("/", 1)
        branch: str = cfg.get("branch", "main")
        token = os.environ.get(cfg.get("token_env", "GITHUB_TOKEN") or "GITHUB_TOKEN")
        allowed_paths: list[str] = cfg.get("paths") or []
        raw_exts: list[str] = cfg.get("file_types") or []
        allowed_exts = {e if e.startswith(".") else f".{e}" for e in raw_exts} if raw_exts else _DEFAULT_FILE_TYPES
        exclude: list[str] = cfg.get("exclude") or []
        max_files: int = int(cfg.get("max_files", 200))

        try:
            tree = fetch_repo_tree(owner, repo_name, branch, token)
        except httpx.HTTPStatusError as exc:
            logger.error("github_reference: tree fetch failed %s: HTTP %s", repo, exc.response.status_code)
            continue
        except httpx.RequestError as exc:
            logger.error("github_reference: tree fetch network error %s: %s", repo, exc)
            continue

        file_count = 0
        for item in tree:
            if file_count >= max_files:
                break
            file_path: str = item["path"]
            if not _should_include(file_path, allowed_paths, allowed_exts, exclude):
                continue

            source_id = f"github:{owner}/{repo_name}/{branch}/{file_path}"
            note_id = note_id_for_path(source_id)

            content = fetch_file_content(owner, repo_name, file_path, branch, token)
            if content is None:
                continue

            content_hash = "sha256:" + hashlib.sha256(content.encode()).hexdigest()
            if existing_hashes.get(note_id) == content_hash:
                continue  # 内容変化なし → スキップ

            ext = PurePosixPath(file_path).suffix.lower()
            title = _file_title(file_path, content)
            upsert_note(
                notes_index,
                note_id=note_id,
                source_path=source_id,
                title=title,
                content_hash=content_hash,
                frontmatter={
                    "type": "github_reference",
                    "repo": repo,
                    "branch": branch,
                    "path": file_path,
                    "language": ext.lstrip(".") if ext else "text",
                },
                body=content[:4096],
            )
            existing_hashes[note_id] = content_hash
            logger.info("github_reference.indexed: %s/%s", repo, file_path)
            indexed += 1
            file_count += 1

    if indexed > 0:
        save_index(notes_index, index_path)

    return indexed

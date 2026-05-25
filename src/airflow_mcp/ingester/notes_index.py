"""Notes インデックス管理（.airflow/notes/index.json）（DDD-04 §7）。"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from ..common.audit import now_iso

logger = logging.getLogger(__name__)

_DEFAULT_INDEX_PATH = Path(".airflow/notes/index.json")


def note_id_for_path(source_path: str) -> str:
    """ファイルパスから安定した note_id を生成する。"""
    return "note_" + hashlib.sha256(source_path.encode()).hexdigest()[:16]


def load_index(index_path: Path = _DEFAULT_INDEX_PATH) -> dict[str, Any]:
    """notes/index.json を読み込む。存在しない場合は空インデックスを返す。"""
    if not index_path.exists():
        return {"schema_version": "1.0.0", "notes": {}}
    try:
        return json.loads(index_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"schema_version": "1.0.0", "notes": {}}


class _DateTimeEncoder(json.JSONEncoder):
    """datetime オブジェクトを ISO 文字列に変換する JSON エンコーダー。"""
    def default(self, o: Any) -> str:
        import datetime
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        return super().default(o)


def save_index(index: dict[str, Any], index_path: Path = _DEFAULT_INDEX_PATH) -> None:
    """notes/index.json をアトミックに保存する。"""
    index_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = index_path.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(index, ensure_ascii=False, indent=2, cls=_DateTimeEncoder), encoding="utf-8")
        tmp.replace(index_path)
    except OSError as exc:
        logger.error("failed to save notes index to %s: %s", index_path, exc)
        tmp.unlink(missing_ok=True)
        raise


def upsert_note(
    index: dict[str, Any],
    *,
    note_id: str,
    source_path: str,
    title: str,
    content_hash: str,
    frontmatter: dict[str, Any],
    confidential: bool = False,
    internal_links: list[str] | None = None,
) -> None:
    """インデックスに note レコードを登録 / 更新する。"""
    index.setdefault("notes", {})[note_id] = {
        "note_id": note_id,
        "source_path": source_path,
        "title": title,
        "content_hash": content_hash,
        "confidential": confidential,
        "last_indexed": now_iso(),
        "internal_links": internal_links or [],
        "frontmatter": frontmatter,
    }


def delete_note(index: dict[str, Any], note_id: str) -> bool:
    """インデックスから note_id を削除する。削除できた場合 True を返す。"""
    return index.get("notes", {}).pop(note_id, None) is not None

"""ファイル I/O ユーティリティ（YAML / JSON の安全なロード）。

全モジュールで繰り返される safe_load + isinstance dict ガードを一元化する。
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def safe_yaml_load(path: Path) -> dict[str, Any]:
    """YAML ファイルを読み込み dict を返す。

    ファイル不在・読み取り失敗・非 dict 結果の場合はすべて {} を返す。
    """
    if not path.exists():
        return {}
    try:
        result = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(result, dict):
            logger.warning("safe_yaml_load: %s is not a mapping, ignoring", path)
            return {}
        return result
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("safe_yaml_load: failed to load %s: %s", path, exc)
        return {}


def safe_json_load(path: Path) -> dict[str, Any]:
    """JSON ファイルを読み込み dict を返す。

    ファイル不在・読み取り失敗・非 dict 結果の場合はすべて {} を返す。
    """
    if not path.exists():
        return {}
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            logger.warning("safe_json_load: %s is not an object, ignoring", path)
            return {}
        return result
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("safe_json_load: failed to load %s: %s", path, exc)
        return {}


def load_model_registry(
    path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """モデルレジストリ JSON を読み込み {model_id: entry} dict を返す。

    path が None のとき AIRFLOW_REGISTRY 環境変数 → デフォルトパスを使用。
    """
    import os
    if path is None:
        path = Path(os.environ.get("AIRFLOW_REGISTRY", ".airflow/models/registry.json"))
    data = safe_json_load(path)
    return {
        m["id"]: m
        for m in data.get("models", [])
        if isinstance(m, dict) and "id" in m
    }

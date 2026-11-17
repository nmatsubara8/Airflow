"""YAML 設定ファイルのスキーマ検証ユーティリティ（TE-2）。

各 load_config() 呼び出し後にバリデーションを実行し、
型違いや必須キー欠落を起動時に早期検出する。
クラッシュせず、エラーメッセージのリストを返す。
"""
from __future__ import annotations

from typing import Any

# ── スキーマ定義 ──────────────────────────────────────────────
# 各スキーマは {field_path: (expected_type, required)} のフラット辞書。
# field_path は "." 区切り。list の中身はチェックしない（型確認のみ）。

_SCHEMAS: dict[str, dict[str, tuple[type | tuple[type, ...], bool]]] = {
    "dispatch": {
        "routing":                               (dict,  False),
        "routing.default_category":              (str,   False),
        "routing.rules":                         (list,  False),
        "section_agents":                        (dict,  False),
        "priority":                              (dict,  False),
    },
    "auth": {
        "auth":                                  (dict,  True),
        "auth.mode":                             (str,   True),
        "auth.token_header":                     (str,   False),
        "auth.key_env_prefix":                   (str,   False),
        "auth.roles":                            (dict,  False),
        "rate_limit":                            (dict,  False),
        "rate_limit.default":                    (dict,  False),
        "rate_limit.per_action_limits":          (dict,  False),
        "mcp_auth":                              (dict,  False),
        "mcp_auth.require_token":                (bool,  False),
        "mcp_auth.localhost_bypass":             (bool,  False),
    },
    "notifications": {
        "notifications":                         (dict,  True),
        "notifications.channels":                (dict,  False),
        "notifications.retry":                   (dict,  False),
        "notifications.retry.max_attempts":      ((int, float), False),
        "notifications.retry.backoff_s":         ((int, float), False),
    },
    "chains": {
        "chains":                                (list,  True),
    },
    "model-config": {
        "selection":                             (dict,  True),
        "selection.default_mode":                (str,   True),
        "selection.exploration_rate":            ((int, float), False),
        "selection.min_samples_for_auto":        ((int, float), False),
        "knn":                                   (dict,  False),
        "knn.k":                                 ((int, float), False),
        "reroute":                               (dict,  False),
    },
    "ingester": {
        "watch":                                 (dict,  True),
        "watch.paths":                           (list,  True),
        "watch.glob":                            (str,   False),
        "stability":                             (dict,  False),
        "parsing":                               (dict,  False),
        "dedup":                                 (dict,  False),
        "output":                                (dict,  False),
    },
    "parallel": {
        "scheduler":                             (dict,  False),
        "starvation":                            (dict,  False),
        "preload":                               (dict,  False),
    },
    "schedules": {
        "schedules":                             ((dict, list), False),
        "timezone":                              (str,  False),
    },
    "cleaner": {
        "cleaner":                               (dict,  False),
        "cleaner.tasks":                         (dict,  False),
        "cleaner.execution_logs":                (dict,  False),
        "cleaner.dry_run":                       (bool,  False),
        "cleaner.notify_on_error":               (bool,  False),
    },
}


def _get_nested(data: dict[str, Any], path: str) -> tuple[bool, Any]:
    """ドット区切りパスで data の値を取得する。存在フラグと値を返す。"""
    parts = path.split(".")
    cur: Any = data
    for part in parts:
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def validate_config(name: str, data: dict[str, Any]) -> list[str]:
    """設定名 ``name`` の dict ``data`` をスキーマに照合し、エラーを返す。

    Returns:
        エラーメッセージのリスト。空リストなら検証成功。
        「WARNING:」プレフィックスの項目は非致命的な警告。
    """
    if not isinstance(data, dict):
        return [f"[{name}] config root must be a dict, got {type(data).__name__}"]

    schema = _SCHEMAS.get(name, {})
    errors: list[str] = []

    for field, (expected_type, required) in schema.items():
        exists, value = _get_nested(data, field)
        if not exists:
            if required:
                errors.append(f"[{name}] required field '{field}' is missing")
        elif not isinstance(value, expected_type):
            type_name = (
                " | ".join(t.__name__ for t in expected_type)
                if isinstance(expected_type, tuple)
                else expected_type.__name__
            )
            errors.append(
                f"[{name}] field '{field}' must be {type_name}, "
                f"got {type(value).__name__}"
            )

    # 本番セキュリティ警告: localhost_bypass=true は認証を無効化する
    if name == "auth":
        _, bypass = _get_nested(data, "mcp_auth.localhost_bypass")
        if bypass is True:
            errors.append(
                "WARNING: [auth] mcp_auth.localhost_bypass=true disables authentication "
                "for 127.0.0.1 — set to false before production deployment"
            )

    return errors

"""Token Bucket レート制限（DDD-08 §4.2）。

per-token + per-action の呼び出し頻度を制限し、超過時に AUTH_RATE_LIMITED エラーを返す。
action を指定すると (token, action) 複合キーで独立したバケットを使用する。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import AuthRateLimited
from .io import safe_yaml_load

_CONFIG_PATH = Path(".airflow/config/auth.yaml")

_DEFAULT_CAPACITY = 100
_DEFAULT_REFILL_RATE = 10  # tokens / second

# (token, action) 複合キーの区切り文字（token 値には含まれない制御文字）
_KEY_SEP = "\x00"


@dataclass
class _Bucket:
    capacity: float
    refill_rate: float       # tokens added per second
    tokens: float = field(init=False)
    last_refill: float = field(init=False)

    def __post_init__(self) -> None:
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    def consume(self, amount: float = 1.0) -> bool:
        """トークンを消費する。成功時 True、枯渇時 False。"""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

    def retry_after_ms(self, amount: float = 1.0) -> int:
        """必要トークンが補充されるまでの推定ミリ秒。"""
        needed = amount - self.tokens
        return max(0, int(needed / self.refill_rate * 1000))


class RateLimiter:
    """スレッドセーフな Token Bucket レート制限（DDD-08 §4.2）。

    per_action_limits を指定すると、MCP ツール名ごとに独立したバケットを持つ。
    バケットキー: token のみ、または ``token + "\\x00" + action`` の複合キー。
    """

    def __init__(
        self,
        default_capacity: int = _DEFAULT_CAPACITY,
        default_refill_rate: float = _DEFAULT_REFILL_RATE,
        per_action_limits: dict[str, tuple[float, float]] | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._default_capacity = default_capacity
        self._default_refill_rate = default_refill_rate
        # action → (capacity, refill_rate)
        self._per_action: dict[str, tuple[float, float]] = per_action_limits or {}
        self._buckets: dict[str, _Bucket] = {}

    def check(self, token: str, cost: float = 1.0, action: str | None = None) -> None:
        """レート制限をチェックする。超過時は RateLimited を送出。

        action が指定され、かつ per_action_limits に登録されている場合は
        そのアクション専用バケットを使う。それ以外はデフォルトバケット。
        """
        bucket_key = f"{token}{_KEY_SEP}{action}" if action else token
        with self._lock:
            if bucket_key not in self._buckets:
                if action is not None and action in self._per_action:
                    cap, rate = self._per_action[action]
                else:
                    cap, rate = self._default_capacity, self._default_refill_rate
                self._buckets[bucket_key] = _Bucket(capacity=cap, refill_rate=rate)
            bucket = self._buckets[bucket_key]
            if not bucket.consume(cost):
                raise AuthRateLimited(retry_after_ms=bucket.retry_after_ms(cost))

    def reset(self, token: str, action: str | None = None) -> None:
        """バケットをリセット（テスト用）。"""
        bucket_key = f"{token}{_KEY_SEP}{action}" if action else token
        with self._lock:
            self._buckets.pop(bucket_key, None)

    def stats(self, token: str, action: str | None = None) -> dict[str, Any]:
        bucket_key = f"{token}{_KEY_SEP}{action}" if action else token
        with self._lock:
            if bucket_key not in self._buckets:
                if action is not None and action in self._per_action:
                    cap, _ = self._per_action[action]
                else:
                    cap = self._default_capacity
                return {"tokens": cap, "capacity": cap}
            b = self._buckets[bucket_key]
            return {"tokens": round(b.tokens, 2), "capacity": b.capacity}


def _load_rate_limit_config() -> dict[str, Any]:
    return safe_yaml_load(_CONFIG_PATH).get("rate_limit", {})


def build_rate_limiter() -> RateLimiter:
    """auth.yaml から設定を読み込んで RateLimiter を構築する。

    per_action_limits セクションがあればアクション別バケットも設定する。
    """
    cfg = _load_rate_limit_config()
    default = cfg.get("default", {})
    capacity = default.get("capacity", _DEFAULT_CAPACITY)
    refill_rate = default.get("refill_rate", _DEFAULT_REFILL_RATE)

    per_action: dict[str, tuple[float, float]] = {}
    for action, limits in cfg.get("per_action_limits", {}).items():
        if isinstance(limits, dict):
            per_action[action] = (
                float(limits.get("capacity", capacity)),
                float(limits.get("refill_rate", refill_rate)),
            )

    return RateLimiter(
        default_capacity=capacity,
        default_refill_rate=float(refill_rate),
        per_action_limits=per_action,
    )


# モジュールレベルシングルトン（MCP サーバーから参照）
_limiter: RateLimiter = RateLimiter()


_ANONYMOUS_KEY = "__anonymous__"


def check_rate_limit(
    token: str | None,
    cost: float = 1.0,
    action: str | None = None,
) -> None:
    """グローバルレート制限をチェックする。

    未認証リクエストは共有バケットで計測する。
    action を指定するとそのツール専用バケットを使う（per_action_limits 設定時）。
    """
    _limiter.check(token if token is not None else _ANONYMOUS_KEY, cost, action=action)

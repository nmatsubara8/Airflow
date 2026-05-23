"""Token Bucket レート制限（DDD-08 §4.2）。

per-token の呼び出し頻度を制限し、超過時に AUTH_RATE_LIMITED エラーを返す。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .errors import AuthRateLimited

_CONFIG_PATH = Path(".airflow/config/auth.yaml")

_DEFAULT_CAPACITY = 100
_DEFAULT_REFILL_RATE = 10  # tokens / second


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
    """スレッドセーフな Token Bucket レート制限（DDD-08 §4.2）。"""

    def __init__(
        self,
        default_capacity: int = _DEFAULT_CAPACITY,
        default_refill_rate: float = _DEFAULT_REFILL_RATE,
    ) -> None:
        self._lock = threading.Lock()
        self._default_capacity = default_capacity
        self._default_refill_rate = default_refill_rate
        self._buckets: dict[str, _Bucket] = {}

    def check(self, token: str, cost: float = 1.0) -> None:
        """レート制限をチェックする。超過時は RateLimited を送出。"""
        with self._lock:
            if token not in self._buckets:
                self._buckets[token] = _Bucket(
                    capacity=self._default_capacity,
                    refill_rate=self._default_refill_rate,
                )
            bucket = self._buckets[token]
            if not bucket.consume(cost):
                raise AuthRateLimited(retry_after_ms=bucket.retry_after_ms(cost))

    def reset(self, token: str) -> None:
        """バケットをリセット（テスト用）。"""
        with self._lock:
            self._buckets.pop(token, None)

    def stats(self, token: str) -> dict[str, Any]:
        with self._lock:
            if token not in self._buckets:
                return {"tokens": self._default_capacity, "capacity": self._default_capacity}
            b = self._buckets[token]
            return {"tokens": round(b.tokens, 2), "capacity": b.capacity}


def _load_rate_limit_config() -> dict[str, Any]:
    if _CONFIG_PATH.exists():
        cfg = yaml.safe_load(_CONFIG_PATH.read_text())
        return cfg.get("rate_limit", {})
    return {}


def build_rate_limiter() -> RateLimiter:
    """auth.yaml から設定を読み込んで RateLimiter を構築する。"""
    cfg = _load_rate_limit_config()
    default = cfg.get("default", {})
    capacity = default.get("capacity", _DEFAULT_CAPACITY)
    refill_rate = default.get("refill_rate", _DEFAULT_REFILL_RATE)
    return RateLimiter(default_capacity=capacity, default_refill_rate=float(refill_rate))


# モジュールレベルシングルトン（MCP サーバーから参照）
_limiter: RateLimiter = RateLimiter()


def check_rate_limit(token: str | None, cost: float = 1.0) -> None:
    """グローバルレート制限をチェックする。token が None の場合はスキップ。"""
    if token is None:
        return
    _limiter.check(token, cost)

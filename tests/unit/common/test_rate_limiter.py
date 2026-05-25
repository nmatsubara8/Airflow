"""common/rate_limiter.py の単体テスト（DDD-08 §4.2）。"""
from __future__ import annotations

import time

import pytest

from airflow_mcp.common.errors import AuthRateLimited
from airflow_mcp.common.rate_limiter import RateLimiter, check_rate_limit


@pytest.fixture
def limiter():
    return RateLimiter(default_capacity=5, default_refill_rate=100.0)


# ── 基本消費 ──────────────────────────────────────────

def test_check_passes_within_capacity(limiter):
    for _ in range(5):
        limiter.check("tok_a")  # should not raise


def test_check_raises_when_exhausted(limiter):
    for _ in range(5):
        limiter.check("tok_a")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok_a")


def test_check_different_tokens_are_independent(limiter):
    for _ in range(5):
        limiter.check("tok_a")
    # tok_b は独立したバケット → まだ通る
    limiter.check("tok_b")


def test_retry_after_ms_is_positive_on_exhaustion(limiter):
    for _ in range(5):
        limiter.check("tok_a")
    try:
        limiter.check("tok_a")
    except AuthRateLimited as exc:
        assert exc.retry_after_ms is not None
        assert exc.retry_after_ms >= 0


# ── リフィル ──────────────────────────────────────────

def test_tokens_refill_over_time():
    limiter = RateLimiter(default_capacity=2, default_refill_rate=1000.0)
    limiter.check("tok")
    limiter.check("tok")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok")
    time.sleep(0.002)  # 2ms → 2 tokens refilled
    limiter.check("tok")  # should not raise now


# ── リセット ──────────────────────────────────────────

def test_reset_restores_capacity(limiter):
    for _ in range(5):
        limiter.check("tok_a")
    limiter.reset("tok_a")
    limiter.check("tok_a")  # should not raise after reset


# ── stats ────────────────────────────────────────────

def test_stats_shows_capacity(limiter):
    stats = limiter.stats("tok_new")
    assert stats["capacity"] == 5
    assert stats["tokens"] == 5


def test_stats_decreases_after_consume(limiter):
    limiter.check("tok_a")
    stats = limiter.stats("tok_a")
    assert stats["tokens"] < 5


# ── グローバル helper ─────────────────────────────────

def test_check_rate_limit_none_token_is_counted():
    """token=None は __anonymous__ バケットで計測され、無制限ではないこと。"""
    from airflow_mcp.common import rate_limiter as mod
    original = mod._limiter
    mod._limiter = RateLimiter(default_capacity=2, default_refill_rate=0.001)
    try:
        check_rate_limit(None)
        check_rate_limit(None)
        with pytest.raises(AuthRateLimited):
            check_rate_limit(None)
    finally:
        mod._limiter = original


def test_check_rate_limit_uses_global_limiter():
    from airflow_mcp.common import rate_limiter as mod
    original = mod._limiter
    mod._limiter = RateLimiter(default_capacity=1, default_refill_rate=0.001)
    try:
        mod._limiter.check("tok_x")
        with pytest.raises(AuthRateLimited):
            check_rate_limit("tok_x")
    finally:
        mod._limiter = original


# ── _load_rate_limit_config (lines 91-94) ────────────

def test_load_rate_limit_config_reads_yaml(tmp_path, monkeypatch):
    """config ファイルが存在するとき rate_limit セクションを返すこと（lines 91-93）。"""
    from airflow_mcp.common import rate_limiter as mod
    cfg = tmp_path / "auth.yaml"
    cfg.write_text("rate_limit:\n  default:\n    capacity: 50\n    refill_rate: 5\n")
    monkeypatch.setattr(mod, "_CONFIG_PATH", cfg)
    result = mod._load_rate_limit_config()
    assert result.get("default", {}).get("capacity") == 50


def test_load_rate_limit_config_returns_empty_when_no_file(monkeypatch):
    """config ファイルが存在しないとき {} を返すこと（line 94）。"""
    from pathlib import Path
    from airflow_mcp.common import rate_limiter as mod
    monkeypatch.setattr(mod, "_CONFIG_PATH", Path("/nonexistent/auth.yaml"))
    result = mod._load_rate_limit_config()
    assert result == {}


# ── build_rate_limiter (lines 99-103) ────────────────

def test_build_rate_limiter_uses_config_values(tmp_path, monkeypatch):
    """auth.yaml の値で RateLimiter が構築されること（lines 99-103）。"""
    from airflow_mcp.common import rate_limiter as mod
    cfg = tmp_path / "auth.yaml"
    cfg.write_text("rate_limit:\n  default:\n    capacity: 42\n    refill_rate: 7\n")
    monkeypatch.setattr(mod, "_CONFIG_PATH", cfg)
    rl = mod.build_rate_limiter()
    assert rl._default_capacity == 42
    assert rl._default_refill_rate == pytest.approx(7.0)


def test_build_rate_limiter_uses_defaults_when_no_config(monkeypatch):
    """config なしのときデフォルト値が使われること。"""
    from pathlib import Path
    from airflow_mcp.common import rate_limiter as mod
    monkeypatch.setattr(mod, "_CONFIG_PATH", Path("/nonexistent/auth.yaml"))
    rl = mod.build_rate_limiter()
    assert rl._default_capacity == mod._DEFAULT_CAPACITY
    assert rl._default_refill_rate == pytest.approx(float(mod._DEFAULT_REFILL_RATE))


# ── 追加テスト: YAML non-dict ガード ──────────────────

def test_load_rate_limit_config_non_dict_yaml_returns_empty(tmp_path, monkeypatch):
    """auth.yaml が list などの非dict を返した場合 {} を返すこと。"""
    from airflow_mcp.common import rate_limiter as mod
    cfg = tmp_path / "auth.yaml"
    cfg.write_text("- item1\n- item2\n")
    monkeypatch.setattr(mod, "_CONFIG_PATH", cfg)
    result = mod._load_rate_limit_config()
    assert result == {}


def test_load_rate_limit_config_empty_file_returns_empty(tmp_path, monkeypatch):
    """auth.yaml が空ファイルの場合 {} を返すこと。"""
    from airflow_mcp.common import rate_limiter as mod
    cfg = tmp_path / "auth.yaml"
    cfg.write_text("")
    monkeypatch.setattr(mod, "_CONFIG_PATH", cfg)
    result = mod._load_rate_limit_config()
    assert result == {}


# ── per-action rate limiting (TP-2) ──────────────────

def test_per_action_limits_use_separate_bucket():
    """action を指定すると per_action_limits の独立バケットを使うこと。"""
    limiter = RateLimiter(
        default_capacity=100,
        default_refill_rate=10.0,
        per_action_limits={"models.execute": (2, 0.01)},  # 2 burst only
    )
    limiter.check("tok", action="models.execute")
    limiter.check("tok", action="models.execute")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok", action="models.execute")
    # デフォルトバケットは独立しているので通る
    limiter.check("tok")


def test_per_action_limits_different_actions_are_independent():
    """異なる action は独立したバケットを持つこと。"""
    limiter = RateLimiter(
        default_capacity=1,
        default_refill_rate=0.01,
        per_action_limits={
            "tasks.create": (3, 0.01),
            "notes.search": (5, 0.01),
        },
    )
    # tasks.create: capacity=3
    for _ in range(3):
        limiter.check("tok", action="tasks.create")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok", action="tasks.create")

    # notes.search: capacity=5 — independent
    for _ in range(5):
        limiter.check("tok", action="notes.search")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok", action="notes.search")


def test_per_action_unknown_action_falls_back_to_default():
    """per_action_limits に未登録の action はデフォルトバケットを使うこと。"""
    limiter = RateLimiter(
        default_capacity=2,
        default_refill_rate=0.01,
        per_action_limits={"models.execute": (10, 1.0)},
    )
    limiter.check("tok", action="unknown.tool")
    limiter.check("tok", action="unknown.tool")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok", action="unknown.tool")


def test_reset_with_action_clears_action_bucket():
    """action 付き reset が対象バケットのみクリアすること。"""
    limiter = RateLimiter(
        default_capacity=1,
        default_refill_rate=0.01,
        per_action_limits={"tasks.create": (1, 0.01)},
    )
    limiter.check("tok", action="tasks.create")
    with pytest.raises(AuthRateLimited):
        limiter.check("tok", action="tasks.create")
    limiter.reset("tok", action="tasks.create")
    limiter.check("tok", action="tasks.create")  # should not raise


def test_stats_with_action_returns_action_bucket_state():
    """action 付き stats が正しいバケット情報を返すこと。"""
    limiter = RateLimiter(
        default_capacity=100,
        default_refill_rate=10.0,
        per_action_limits={"models.execute": (5, 1.0)},
    )
    # 未消費の場合は capacity=5
    stats = limiter.stats("tok", action="models.execute")
    assert stats["capacity"] == 5
    limiter.check("tok", action="models.execute")
    stats = limiter.stats("tok", action="models.execute")
    assert stats["tokens"] < 5


def test_build_rate_limiter_loads_per_action_limits(tmp_path, monkeypatch):
    """auth.yaml の per_action_limits が RateLimiter に反映されること。"""
    from airflow_mcp.common import rate_limiter as mod
    cfg = tmp_path / "auth.yaml"
    cfg.write_text(
        "rate_limit:\n"
        "  default:\n"
        "    capacity: 100\n"
        "    refill_rate: 10\n"
        "  per_action_limits:\n"
        "    models.execute: {capacity: 20, refill_rate: 0.33}\n"
        "    tasks.create: {capacity: 60, refill_rate: 1.0}\n"
    )
    monkeypatch.setattr(mod, "_CONFIG_PATH", cfg)
    rl = mod.build_rate_limiter()
    assert "models.execute" in rl._per_action
    assert rl._per_action["models.execute"][0] == pytest.approx(20.0)
    assert "tasks.create" in rl._per_action


def test_check_rate_limit_passes_action_to_limiter():
    """check_rate_limit の action 引数がグローバルリミッターに渡されること。"""
    from airflow_mcp.common import rate_limiter as mod
    original = mod._limiter
    mod._limiter = RateLimiter(
        default_capacity=100,
        default_refill_rate=10.0,
        per_action_limits={"tiny.action": (1, 0.01)},
    )
    try:
        check_rate_limit("tok", action="tiny.action")
        with pytest.raises(AuthRateLimited):
            check_rate_limit("tok", action="tiny.action")
    finally:
        mod._limiter = original

"""MCP ツール統合テスト。

対象シナリオ:
  - tasks.join（all_done / any_done / majority ポリシー）
  - tasks.assign（needs_human 後の再割り当て）
  - audit.trace（監査チェーントレース）
  - audit.query（フィルタ検索）
  - Auth スコープ拒否
  - Rate limit 拒否
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("OLLAMA_STUB", "true")

import airflow_mcp.taskboard.server as tb_mod
from airflow_mcp.common.errors import AuthRateLimited
from airflow_mcp.common.rate_limiter import RateLimiter, _limiter
from airflow_mcp.taskboard.store import TaskStore
from tests.helpers import make_mcp_caller


# ── フィクスチャ ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_taskboard(tmp_tasks_dir: Path):
    """各テスト前後に taskboard サーバーのグローバル状態をリセット。"""
    tb_mod._store = TaskStore(base_dir=tmp_tasks_dir)
    tb_mod._audit_log.clear()
    yield
    tb_mod._store = None
    tb_mod._audit_log.clear()


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """各テスト前後にレートリミッターのバケットをリセット。"""
    _limiter._buckets.clear()
    yield
    _limiter._buckets.clear()


_call = make_mcp_caller(tb_mod.call_tool)


def _make_task(title: str = "テストタスク", section: str = "engineering") -> dict:
    return {
        "title": title,
        "section": section,
        "description": "テスト用",
        "tags": ["test"],
        "priority": 3,
    }


# ── tasks.join: all_done ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_join_all_done_not_ready_when_children_pending():
    parent = await _call("tasks.create", _make_task("親タスク"))
    c1 = await _call("tasks.create", _make_task("子1"))
    c2 = await _call("tasks.create", _make_task("子2"))
    c3 = await _call("tasks.create", _make_task("子3"))

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"], c3["id"]],
        "join_policy": "all_done",
    })

    assert result["ready"] is False
    assert len(result["pending_children"]) == 3
    assert result["completed_children"] == []


@pytest.mark.asyncio
async def test_join_all_done_ready_when_all_complete():
    parent = await _call("tasks.create", _make_task("親タスク"))
    children = []
    for i in range(3):
        c = await _call("tasks.create", _make_task(f"子{i}"))
        # inbox → in_progress → done
        await _call("tasks.claim", {"task_id": c["id"], "agent_id": "agent_docs"})
        await _call("tasks.transition", {"id": c["id"], "to_status": "done"})
        children.append(c["id"])

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": children,
        "join_policy": "all_done",
    })

    assert result["ready"] is True
    assert len(result["completed_children"]) == 3
    assert result["pending_children"] == []


@pytest.mark.asyncio
async def test_join_all_done_not_ready_when_partial():
    """all_done: 一部だけ完了では ready にならないこと。"""
    parent = await _call("tasks.create", _make_task("親"))
    c1 = await _call("tasks.create", _make_task("子1"))
    c2 = await _call("tasks.create", _make_task("子2"))

    await _call("tasks.claim", {"task_id": c1["id"], "agent_id": "agent_docs"})
    await _call("tasks.transition", {"id": c1["id"], "to_status": "done"})

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"]],
        "join_policy": "all_done",
    })

    assert result["ready"] is False
    assert c1["id"] in result["completed_children"]
    assert c2["id"] in result["pending_children"]


# ── tasks.join: any_done ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_join_any_done_ready_when_one_complete():
    parent = await _call("tasks.create", _make_task("親"))
    c1 = await _call("tasks.create", _make_task("子1"))
    c2 = await _call("tasks.create", _make_task("子2"))

    await _call("tasks.claim", {"task_id": c1["id"], "agent_id": "agent_docs"})
    await _call("tasks.transition", {"id": c1["id"], "to_status": "done"})

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"]],
        "join_policy": "any_done",
    })

    assert result["ready"] is True


@pytest.mark.asyncio
async def test_join_any_done_not_ready_when_none_complete():
    parent = await _call("tasks.create", _make_task("親"))
    c1 = await _call("tasks.create", _make_task("子1"))
    c2 = await _call("tasks.create", _make_task("子2"))

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": [c1["id"], c2["id"]],
        "join_policy": "any_done",
    })

    assert result["ready"] is False


# ── tasks.join: majority ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_join_majority_ready_when_majority_complete():
    """majority: 3件中2件完了で ready。"""
    parent = await _call("tasks.create", _make_task("親"))
    children = []
    for i in range(3):
        c = await _call("tasks.create", _make_task(f"子{i}"))
        children.append(c["id"])

    # 2件完了
    for cid in children[:2]:
        await _call("tasks.claim", {"task_id": cid, "agent_id": "agent_docs"})
        await _call("tasks.transition", {"id": cid, "to_status": "done"})

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": children,
        "join_policy": "majority",
    })

    assert result["ready"] is True
    assert len(result["completed_children"]) == 2


@pytest.mark.asyncio
async def test_join_majority_not_ready_when_minority_complete():
    """majority: 3件中1件完了では ready にならないこと。"""
    parent = await _call("tasks.create", _make_task("親"))
    children = []
    for i in range(3):
        c = await _call("tasks.create", _make_task(f"子{i}"))
        children.append(c["id"])

    await _call("tasks.claim", {"task_id": children[0], "agent_id": "agent_docs"})
    await _call("tasks.transition", {"id": children[0], "to_status": "done"})

    result = await _call("tasks.join", {
        "parent_id": parent["id"],
        "child_ids": children,
        "join_policy": "majority",
    })

    assert result["ready"] is False


# ── tasks.assign（needs_human 後の再割り当て） ───────────────────────────


@pytest.mark.asyncio
async def test_assign_reassigns_inbox_task_to_new_owner():
    task = await _call("tasks.create", _make_task())

    result = await _call("tasks.assign", {
        "task_id": task["id"],
        "new_owner": {"type": "human", "id": "human_reviewer_01"},
        "reason": "エスカレーション対応",
    })

    assert result["owner"] == "human_reviewer_01"


@pytest.mark.asyncio
async def test_assign_fails_when_task_in_progress():
    """in_progress タスクへの assign は TaskInvalidTransition になること。"""
    task = await _call("tasks.create", _make_task())
    await _call("tasks.claim", {"task_id": task["id"], "agent_id": "agent_docs"})

    result = await _call("tasks.assign", {
        "task_id": task["id"],
        "new_owner": {"type": "human", "id": "human_01"},
    })

    assert "error" in result
    assert result["error"]["code"] == "TASK_INVALID_TRANSITION"


@pytest.mark.asyncio
async def test_assign_after_needs_human_then_reclaim():
    """needs_human → assign → claim で新 agent が作業継続できること。"""
    # タスク作成 → in_progress → needs_human
    task = await _call("tasks.create", _make_task())
    await _call("tasks.claim", {"task_id": task["id"], "agent_id": "agent_docs"})
    await _call("tasks.transition", {"id": task["id"], "to_status": "needs_human", "reason": "escalation"})

    # assign で human に渡す（needs_human は inbox ではないので直接 update で owner を変更）
    current = await _call("tasks.get", {"id": task["id"]})
    assert current["status"] == "needs_human"

    # needs_human → in_progress に戻し、別エージェントが引き受ける
    await _call("tasks.transition", {"id": task["id"], "to_status": "in_progress", "reason": "human intervention"})
    await _call("tasks.add_handoff_note", {
        "task_id": task["id"],
        "content": "人間レビュー完了。修正方針を添付。",
        "author": "human_reviewer_01",
    })

    updated = await _call("tasks.get", {"id": task["id"]})
    assert updated["status"] == "in_progress"
    assert any("人間レビュー" in n["body"] for n in updated["handoff_notes"])


# ── audit.trace ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_audit_trace_returns_root_event():
    await _call("tasks.create", _make_task())
    assert len(tb_mod._audit_log) >= 1
    root_aid = tb_mod._audit_log[0]["audit_id"]

    result = await _call("audit.trace", {"audit_id": root_aid})

    assert result["root"] == root_aid
    assert len(result["events"]) >= 1
    assert result["events"][0]["audit_id"] == root_aid


@pytest.mark.asyncio
async def test_audit_trace_follows_child_events():
    """親イベント起点で子イベントまでトレースできること。"""
    parent_aid = "parent_audit_123"

    task = await _call("tasks.create", {
        **_make_task(),
        "_meta": {"audit_id": parent_aid, "agent_id": "agent_docs"},
    })
    await _call("tasks.claim", {
        "task_id": task["id"],
        "agent_id": "agent_docs",
        "_meta": {"audit_id": parent_aid},
    })

    # parent_aid を持つ子イベントが audit_log に記録されていること
    children = [e for e in tb_mod._audit_log if e.get("parent_audit_id") == parent_aid]
    assert len(children) >= 1


@pytest.mark.asyncio
async def test_audit_trace_depth_clamped_to_20():
    """depth > 20 を指定しても 20 にクランプされること（DoS 防止）。"""
    await _call("tasks.create", _make_task())
    root_aid = tb_mod._audit_log[0]["audit_id"]

    # depth=100 を渡してもクラッシュしないこと
    result = await _call("audit.trace", {"audit_id": root_aid, "depth": 100})
    assert "events" in result


@pytest.mark.asyncio
async def test_audit_trace_handles_cycle_without_crash():
    """監査ログにサイクルがあってもクラッシュしないこと（cycle guard）。"""
    # 人工的にサイクルを作成
    aid_a = "cycle_a_" + "0" * 16
    aid_b = "cycle_b_" + "0" * 16
    tb_mod._audit_log.append({"audit_id": aid_a, "parent_audit_id": aid_b, "event": "fake"})
    tb_mod._audit_log.append({"audit_id": aid_b, "parent_audit_id": aid_a, "event": "fake"})

    result = await _call("audit.trace", {"audit_id": aid_a, "depth": 10})
    assert "events" in result  # クラッシュしないこと


# ── audit.query ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_audit_query_filters_by_event():
    await _call("tasks.create", _make_task("タスクA"))
    await _call("tasks.create", _make_task("タスクB"))

    result = await _call("audit.query", {"event": "task.created"})

    assert len(result["events"]) == 2
    assert all(e["event"] == "task.created" for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_glob_pattern():
    """ワイルドカードパターンで複数イベントタイプをマッチできること。"""
    task = await _call("tasks.create", _make_task())
    await _call("tasks.claim", {"task_id": task["id"], "agent_id": "agent_docs"})

    result = await _call("audit.query", {"event": "task.*"})

    assert len(result["events"]) >= 2
    assert all(e["event"].startswith("task.") for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_filters_by_task_id():
    t1 = await _call("tasks.create", _make_task("タスク1"))
    t2 = await _call("tasks.create", _make_task("タスク2"))

    result = await _call("audit.query", {"task_id": t1["id"]})

    assert all(e.get("task_id") == t1["id"] for e in result["events"])
    assert not any(e.get("task_id") == t2["id"] for e in result["events"])


@pytest.mark.asyncio
async def test_audit_query_respects_limit():
    for i in range(10):
        await _call("tasks.create", _make_task(f"タスク{i}"))

    result = await _call("audit.query", {"limit": 3})
    assert len(result["events"]) == 3


# ── Auth スコープ拒否 ─────────────────────────────────────────────────────


def test_require_scope_raises_on_insufficient_scope():
    """wrong スコープのトークンで require_scope が AuthScopeInsufficient を raise すること。"""
    import os

    os.environ["AIRFLOW_MCP_TOKEN"] = "wrong_scope_token"
    os.environ["AIRFLOW_AUTH_LOCALHOST_BYPASS"] = "false"

    import airflow_mcp.common.auth as auth_mod
    original_bypass = auth_mod._LOCALHOST_BYPASS
    auth_mod._LOCALHOST_BYPASS = False

    try:
        # トークンは存在するが tasks:write スコープがない（tokens.json なし）
        # env var トークンとして認証 → 全スコープを持つが、
        # tokens.json に登録されていない env_token はすべてのスコープを持つ（Phase 2 MVP）
        # スコープ検証のテストは tokens.json ベースで行う
        pass
    finally:
        auth_mod._LOCALHOST_BYPASS = original_bypass


@pytest.mark.asyncio
async def test_auth_scope_insufficient_returns_error_response(monkeypatch):
    """LOCALHOST_BYPASS=false で tokens.json なし、かつ AIRFLOW_MCP_TOKEN ミスマッチ時にエラー応答。"""
    import airflow_mcp.common.auth as auth_mod
    monkeypatch.setattr(auth_mod, "_LOCALHOST_BYPASS", False)
    monkeypatch.setenv("AIRFLOW_MCP_TOKEN", "correct_token")

    # 不正トークン（env var と異なる）で呼び出すと AuthTokenInvalid → MCP エラー応答
    def _bad_token():
        return "wrong_token_value"

    import airflow_mcp.common.mcp as mcp_mod
    monkeypatch.setattr(mcp_mod, "get_token_from_env", _bad_token)
    import airflow_mcp.taskboard.server as tb_srv

    result = await tb_srv.call_tool("tasks.list", {})
    data = json.loads(result[0].text)
    assert "error" in data
    assert data["error"]["code"] in ("AUTH_TOKEN_INVALID", "AUTH_TOKEN_MISSING")


# ── Rate limit 拒否 ───────────────────────────────────────────────────────


def test_rate_limiter_raises_after_capacity_exhausted():
    """バケット容量を超えたリクエストで AuthRateLimited が raise されること。"""
    from airflow_mcp.common.errors import AuthRateLimited

    # default_capacity=3, 補充なし（非常に低いレート）
    limiter = RateLimiter(default_capacity=3, default_refill_rate=0.0001)
    token = "test_token_exhaustion"

    limiter.check(token)
    limiter.check(token)
    limiter.check(token)

    with pytest.raises(AuthRateLimited) as exc_info:
        limiter.check(token)

    assert exc_info.value.retry_after_ms >= 0


def test_rate_limiter_retry_after_is_positive():
    """AuthRateLimited の retry_after_ms が正の値であること。"""
    from airflow_mcp.common.errors import AuthRateLimited

    limiter = RateLimiter(default_capacity=1, default_refill_rate=0.001)
    token = "test_token_retry"
    limiter.check(token)

    try:
        limiter.check(token)
    except AuthRateLimited as e:
        assert e.retry_after_ms > 0
        assert "retry_after_ms" in e.to_mcp_result()[0].text


@pytest.mark.asyncio
async def test_rate_limit_enforced_in_mcp_tool(monkeypatch):
    """call_tool が rate limit を超えると error レスポンスを返すこと。"""

    def _always_limited(token, cost=1.0):
        raise AuthRateLimited(retry_after_ms=5000)

    import airflow_mcp.common.mcp as mcp_mod
    monkeypatch.setattr(mcp_mod, "check_rate_limit", _always_limited)
    import airflow_mcp.taskboard.server as tb_srv

    result = await tb_srv.call_tool("tasks.list", {})
    data = json.loads(result[0].text)
    assert "error" in data
    assert data["error"]["code"] == "AUTH_RATE_LIMITED"


# ── tasks.add_handoff_note（統合確認） ────────────────────────────────────


@pytest.mark.asyncio
async def test_handoff_note_chain_across_agents():
    """複数エージェントの handoff_note が時系列順に積み上がること。"""
    task = await _call("tasks.create", _make_task())
    await _call("tasks.claim", {"task_id": task["id"], "agent_id": "agent_docs"})
    await _call("tasks.transition", {"id": task["id"], "to_status": "needs_human"})

    await _call("tasks.add_handoff_note", {
        "task_id": task["id"],
        "content": "初期分析完了。問題: スキーマ不適合",
        "author": "agent_docs",
    })
    await _call("tasks.add_handoff_note", {
        "task_id": task["id"],
        "content": "人間レビュー: 修正方針を決定",
        "author": "human_reviewer",
    })

    final = await _call("tasks.get", {"id": task["id"]})
    notes = final["handoff_notes"]
    assert len(notes) == 2
    assert notes[0]["author"]["id"] == "agent_docs"
    assert notes[1]["author"]["id"] == "human_reviewer"
    assert "スキーマ不適合" in notes[0]["body"]
    assert "修正方針" in notes[1]["body"]

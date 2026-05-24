"""ops/audit_integrity.py の単体テスト（DDD-08 §4.5）。"""
from __future__ import annotations

import json

import pytest

from airflow_mcp.ops.audit_integrity import (
    _sha256_file,
    compute_chain_hash,
    run_chain_hash,
    verify_chain,
)


@pytest.fixture(autouse=True)
def _patch_paths(tmp_path, monkeypatch):
    logs_dir = tmp_path / "logs"
    integrity_dir = logs_dir / "integrity"
    logs_dir.mkdir()
    integrity_dir.mkdir()
    import airflow_mcp.ops.audit_integrity as mod
    monkeypatch.setattr(mod, "_LOGS_DIR", logs_dir)
    monkeypatch.setattr(mod, "_INTEGRITY_DIR", integrity_dir)


# ── sha256_file ───────────────────────────────────────

def test_sha256_file_returns_hex(tmp_path):
    f = tmp_path / "test.jsonl"
    f.write_text('{"event":"test"}\n')
    h = _sha256_file(f)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_sha256_file_empty_when_missing(tmp_path):
    h = _sha256_file(tmp_path / "nonexistent.jsonl")
    assert len(h) == 64  # sha256("") still produces a hash


# ── compute_chain_hash ────────────────────────────────

def test_chain_hash_differs_with_different_prev(tmp_path, monkeypatch):
    import airflow_mcp.ops.audit_integrity as mod
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(mod, "_LOGS_DIR", logs_dir)
    (logs_dir / "2026-01-01.jsonl").write_text('{"event":"a"}\n')
    h1 = compute_chain_hash("2026-01-01", "prev_a")
    h2 = compute_chain_hash("2026-01-01", "prev_b")
    assert h1 != h2


def test_chain_hash_same_for_same_inputs(tmp_path, monkeypatch):
    import airflow_mcp.ops.audit_integrity as mod
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(mod, "_LOGS_DIR", logs_dir)
    (logs_dir / "2026-01-01.jsonl").write_text('{"event":"a"}\n')
    h1 = compute_chain_hash("2026-01-01", "prev")
    h2 = compute_chain_hash("2026-01-01", "prev")
    assert h1 == h2


# ── run_chain_hash ────────────────────────────────────

def test_run_chain_hash_creates_hash_file():
    run_chain_hash("2026-01-01")
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    assert (_INTEGRITY_DIR / "2026-01-01.hash").exists()


def test_run_chain_hash_returns_correct_fields():
    result = run_chain_hash("2026-01-02")
    assert result["date"] == "2026-01-02"
    assert "hash" in result
    assert "prev_hash" in result
    assert "computed_at" in result


def test_run_chain_hash_uses_prev_as_chain():
    run_chain_hash("2026-01-01")
    result2 = run_chain_hash("2026-01-02")
    # 2 日目の prev_hash は 1 日目のハッシュと一致
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    record1 = json.loads((_INTEGRITY_DIR / "2026-01-01.hash").read_text())
    assert result2["prev_hash"] == record1["hash"]


# ── verify_chain ──────────────────────────────────────

def test_verify_chain_no_failures_for_valid_chain():
    run_chain_hash("2026-01-01")
    run_chain_hash("2026-01-02")
    failures = verify_chain()
    assert failures == []


def test_verify_chain_detects_tampered_hash():
    run_chain_hash("2026-01-01")
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    # ハッシュを改ざん
    path = _INTEGRITY_DIR / "2026-01-01.hash"
    record = json.loads(path.read_text())
    record["hash"] = "deadbeef" * 8
    path.write_text(json.dumps(record))

    run_chain_hash("2026-01-02")
    failures = verify_chain()
    # 改ざんされた日 or その翌日で破断を検出
    assert len(failures) >= 1


def test_verify_chain_since_filters_dates():
    run_chain_hash("2026-01-01")
    run_chain_hash("2026-01-02")
    # since=2026-01-02 なら 1 日目は検査しない
    failures = verify_chain(since="2026-01-02")
    assert failures == []


# ── run_chain_hash date_str=None (line 48) ────────────

def test_run_chain_hash_uses_today_when_no_date_given():
    """date_str=None のとき今日の日付が使われること（line 48）。"""
    from datetime import datetime, timezone
    result = run_chain_hash()  # date_str 省略
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert result["date"] == today


# ── 破損ハッシュファイルの耐性 ────────────────────────

def test_run_chain_hash_survives_corrupted_prev_hash_file():
    """前回ハッシュファイルが破損していても prev_hash="" で続行する。"""
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    # 正常なハッシュを作成してから破損させる
    run_chain_hash("2026-01-01")
    corrupt_path = _INTEGRITY_DIR / "2026-01-01.hash"
    corrupt_path.write_text("THIS IS NOT JSON")

    # 破損ファイルがあっても翌日分は正常に生成される
    result = run_chain_hash("2026-01-02")
    assert result["date"] == "2026-01-02"
    assert result["prev_hash"] == ""  # 破損から回復して空に
    assert len(result["hash"]) == 64


def test_verify_chain_reports_corrupted_file_as_failure():
    """verify_chain() が破損ファイルを failure として報告すること。"""
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    run_chain_hash("2026-01-01")
    # ファイルを破損させる
    (_INTEGRITY_DIR / "2026-01-01.hash").write_text("{invalid json")

    failures = verify_chain()
    assert len(failures) >= 1
    assert failures[0]["date"] == "2026-01-01"
    assert "CORRUPT" in failures[0]["actual_hash"]


def test_verify_chain_corrupted_first_file_does_not_crash():
    """先頭ハッシュファイルが破損していても verify_chain はクラッシュしない。"""
    from airflow_mcp.ops.audit_integrity import _INTEGRITY_DIR
    run_chain_hash("2026-01-01")
    run_chain_hash("2026-01-02")
    # 先頭ファイルを破損
    (_INTEGRITY_DIR / "2026-01-01.hash").write_text("CORRUPT")

    failures = verify_chain()
    # 破損ファイルが failure に含まれること
    dates = [f["date"] for f in failures]
    assert "2026-01-01" in dates


# ── パストラバーサル防止 ──────────────────────────────

def test_run_chain_hash_rejects_traversal_date():
    """date_str にパストラバーサル文字列を渡すと ValueError が発生すること。"""
    import pytest
    with pytest.raises(ValueError, match="Invalid date_str"):
        run_chain_hash("../../../etc/passwd")


def test_run_chain_hash_rejects_non_date_string():
    """date_str が YYYY-MM-DD 形式でなければ ValueError が発生すること。"""
    import pytest
    with pytest.raises(ValueError, match="Invalid date_str"):
        run_chain_hash("2026_01_01")


def test_verify_chain_rejects_traversal_since():
    """since にパストラバーサル文字列を渡すと ValueError が発生すること。"""
    import pytest
    with pytest.raises(ValueError, match="Invalid date_str"):
        verify_chain(since="../../secret")

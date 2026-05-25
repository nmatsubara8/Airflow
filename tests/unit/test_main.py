"""__main__.py の単体テスト。"""
from __future__ import annotations

import sys
import pytest
from unittest.mock import patch


def _main(args: list[str]):
    """sys.argv を差し替えて main() を呼ぶ。importキャッシュ対策のため毎回importする。"""
    import airflow_mcp.__main__ as mod
    with patch.object(sys, "argv", ["airflow-mcp"] + args):
        return mod.main()


# ── no args → usage + exit(1) ────────────────────────

def test_main_no_args_prints_usage_and_exits():
    with patch.object(sys, "argv", ["airflow-mcp"]), \
         patch("airflow_mcp.__main__._print_usage") as mock_usage, \
         pytest.raises(SystemExit) as exc:
        import airflow_mcp.__main__ as mod
        mod.main()
    assert exc.value.code == 1
    mock_usage.assert_called_once()


# ── unknown command → exit(1) ─────────────────────────

def test_main_unknown_command_exits_with_1():
    with pytest.raises(SystemExit) as exc:
        _main(["nonexistent_command"])
    assert exc.value.code == 1


# ── MCP server dispatch ───────────────────────────────

@pytest.mark.parametrize("server_name", ["taskboard", "notes", "models", "gpu", "agents"])
def test_main_mcp_server_calls_asyncio_run(server_name):
    with patch("asyncio.run") as mock_asyncio_run:
        _main([server_name])
    mock_asyncio_run.assert_called_once()


def test_run_mcp_server_raises_for_invalid_name():
    import airflow_mcp.__main__ as mod
    with pytest.raises(ValueError):
        mod._run_mcp_server("invalid_server")


# ── status ────────────────────────────────────────────

def test_main_status_calls_print_status():
    with patch("airflow_mcp.ops.monitor.print_status") as mock_ps:
        _main(["status"])
    mock_ps.assert_called_once()


# ── batch (no --loop) ─────────────────────────────────

def test_main_batch_no_results_prints_no_schedules(capsys):
    with patch("airflow_mcp.orchestrator.batch_scheduler.run_due_schedules", return_value=[]):
        _main(["batch"])
    out = capsys.readouterr().out
    assert "no schedules due" in out


def test_main_batch_with_results_prints_summary(capsys):
    results = [{"name": "nightly", "success": 5, "total": 5, "duration_s": 10.0}]
    with patch("airflow_mcp.orchestrator.batch_scheduler.run_due_schedules", return_value=results):
        _main(["batch"])
    out = capsys.readouterr().out
    assert "nightly" in out


# ── batch --loop ──────────────────────────────────────

def test_main_batch_loop_calls_run_loop():
    with patch("airflow_mcp.orchestrator.batch_scheduler.run_loop") as mock_loop:
        _main(["batch", "--loop"])
    mock_loop.assert_called_once()


# ── audit hash ────────────────────────────────────────

def test_main_audit_hash_prints_result(capsys):
    fake = {"date": "2026-05-23", "hash": "a" * 64, "prev_hash": ""}
    with patch("airflow_mcp.ops.audit_integrity.run_chain_hash", return_value=fake):
        _main(["audit", "hash"])
    out = capsys.readouterr().out
    assert "2026-05-23" in out


# ── audit verify (ok) ─────────────────────────────────

def test_main_audit_verify_ok_prints_chain_ok(capsys):
    with patch("airflow_mcp.ops.audit_integrity.verify_chain", return_value=[]):
        _main(["audit", "verify"])
    out = capsys.readouterr().out
    assert "chain ok" in out


# ── audit verify (failures) → exit(1) ─────────────────

def test_main_audit_verify_failures_exits_with_1():
    failures = [{"date": "2026-05-22",
                 "expected_hash": "e" * 64, "actual_hash": "a" * 64}]
    with patch("airflow_mcp.ops.audit_integrity.verify_chain", return_value=failures), \
         pytest.raises(SystemExit) as exc:
        _main(["audit", "verify"])
    assert exc.value.code == 1


# ── audit verify with since ───────────────────────────

def test_main_audit_verify_passes_since_argument():
    with patch("airflow_mcp.ops.audit_integrity.verify_chain", return_value=[]) as mock_vc:
        _main(["audit", "verify", "2026-01-01"])
    mock_vc.assert_called_once_with(since="2026-01-01")


# ── audit (no sub-command defaults to verify) ─────────

def test_main_audit_no_subcommand_defaults_to_verify(capsys):
    with patch("airflow_mcp.ops.audit_integrity.verify_chain", return_value=[]):
        _main(["audit"])
    out = capsys.readouterr().out
    assert "chain ok" in out


# ── _print_usage writes to stderr ─────────────────────

def test_print_usage_writes_to_stderr(capsys):
    import airflow_mcp.__main__ as mod
    mod._print_usage()
    err = capsys.readouterr().err
    assert "airflow-mcp" in err


# ── if __name__ == "__main__" (line 97) ───────────────

def test_dunder_main_guard_calls_main():
    """if __name__ == '__main__': main() のガード（line 97）。"""
    import runpy
    # runpy は新しい名前空間でモジュールを実行するため sys.argv をセットして
    # main() が正常終了するコマンドを渡す
    with patch.object(sys, "argv", ["airflow-mcp", "status"]), \
         patch("airflow_mcp.ops.monitor.print_status"):
        runpy.run_module("airflow_mcp.__main__", run_name="__main__", alter_sys=False)


# ── auth keygen ───────────────────────────────────────

def test_main_auth_keygen_generates_keypair(tmp_path, capsys):
    with patch("airflow_mcp.common.auth.generate_keypair",
               return_value=(tmp_path / "ed25519.pem", tmp_path / "ed25519.pub")) as mock_gen:
        _main(["auth", "keygen"])
    mock_gen.assert_called_once_with(None)
    out = capsys.readouterr().out
    assert "keypair generated" in out


def test_main_auth_keygen_with_dir(tmp_path, capsys):
    with patch("airflow_mcp.common.auth.generate_keypair",
               return_value=(tmp_path / "ed25519.pem", tmp_path / "ed25519.pub")) as mock_gen:
        _main(["auth", "keygen", "--dir", str(tmp_path)])
    mock_gen.assert_called_once_with(tmp_path)


# ── auth issue ────────────────────────────────────────

def test_main_auth_issue_prints_token(capsys):
    with patch("airflow_mcp.common.auth.issue_jwt", return_value="fake.jwt.token"):
        _main(["auth", "issue", "--agent", "agent_docs", "--scopes", "tasks:read,models:read"])
    out = capsys.readouterr().out
    assert "agent_docs" in out
    assert "fake.jwt.token" in out


def test_main_auth_issue_with_ttl(capsys):
    with patch("airflow_mcp.common.auth.issue_jwt", return_value="tok.x.y") as mock_issue:
        _main(["auth", "issue", "--agent", "agent_qa", "--scopes", "tasks:read", "--ttl", "7200"])
    mock_issue.assert_called_once_with(sub="agent_qa", scopes=["tasks:read"], ttl_sec=7200)


def test_main_auth_issue_missing_agent_exits(capsys):
    with pytest.raises(SystemExit) as exc:
        _main(["auth", "issue", "--scopes", "tasks:read"])
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "--agent" in err


def test_main_auth_unknown_subcommand_exits(capsys):
    with pytest.raises(SystemExit) as exc:
        _main(["auth", "badcmd"])
    assert exc.value.code == 1


def test_main_auth_issue_ignores_unknown_flag(capsys):
    """未知フラグをスキップして --agent が設定されなければ exit(1)（line 125）。"""
    with pytest.raises(SystemExit) as exc:
        _main(["auth", "issue", "--unknown-flag"])
    assert exc.value.code == 1


# ── _print_usage includes auth ────────────────────────

def test_print_usage_includes_auth_command(capsys):
    import airflow_mcp.__main__ as mod
    mod._print_usage()
    err = capsys.readouterr().err
    assert "auth" in err


# ── config validate ────────────────────────────────────

def test_main_config_validate_all_ok(capsys):
    """config validate が全設定 OK のとき 0 で終了すること。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.config_validator.validate_config", return_value=[]), \
         patch("airflow_mcp.common.io.safe_yaml_load", return_value={}), \
         patch("pathlib.Path.exists", return_value=True):
        _main(["config"])
    out = capsys.readouterr().out
    assert "valid" in out.lower()


def test_main_config_validate_errors_exits_1(capsys):
    """validate_config がエラーを返すとき exit(1) すること。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.config_validator.validate_config",
               return_value=["[auth] field 'auth' missing"]), \
         patch("airflow_mcp.common.io.safe_yaml_load", return_value={}), \
         patch("pathlib.Path.exists", return_value=True):
        with pytest.raises(SystemExit) as exc:
            _main(["config"])
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "missing" in err


def test_main_config_validate_skips_missing_file(capsys):
    """設定ファイルが存在しないとき SKIP と表示してクラッシュしないこと。"""
    from unittest.mock import patch
    with patch("pathlib.Path.exists", return_value=False):
        _main(["config"])
    out = capsys.readouterr().out
    assert "SKIP" in out


# ── auth list / revoke / rotate (TF-2) ────────────────────────

def test_main_auth_list_no_tokens(capsys):
    """auth list でトークンなしのとき 'no tokens' を表示すること。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.list_tokens", return_value=[]):
        _main(["auth", "list"])
    assert "no tokens" in capsys.readouterr().out


def test_main_auth_list_shows_tokens(capsys):
    """auth list でトークン一覧を表示すること。"""
    from unittest.mock import patch
    tokens = [{"agent_id": "agent_docs", "scopes": ["tasks:read"], "created_at": "2026-01-01"}]
    with patch("airflow_mcp.common.auth.list_tokens", return_value=tokens):
        _main(["auth", "list"])
    out = capsys.readouterr().out
    assert "agent_docs" in out


def test_main_auth_revoke_success(capsys):
    """auth revoke --agent <id> が成功したとき revoked を表示すること。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.revoke_token", return_value=True):
        _main(["auth", "revoke", "--agent", "agent_docs"])
    assert "revoked" in capsys.readouterr().out


def test_main_auth_revoke_not_found_exits_1(capsys):
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.revoke_token", return_value=False):
        with pytest.raises(SystemExit) as exc:
            _main(["auth", "revoke", "--agent", "agent_docs"])
    assert exc.value.code == 1


def test_main_auth_revoke_missing_agent_exits_1(capsys):
    with pytest.raises(SystemExit) as exc:
        _main(["auth", "revoke"])
    assert exc.value.code == 1


def test_main_auth_rotate_success(capsys):
    """auth rotate が JWT を表示すること。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.rotate_token", return_value="new.jwt.token"):
        _main(["auth", "rotate", "--agent", "agent_docs", "--scopes", "tasks:read"])
    out = capsys.readouterr().out
    assert "rotated" in out
    assert "new.jwt.token" in out


def test_main_auth_rotate_missing_agent_exits_1():
    with pytest.raises(SystemExit) as exc:
        _main(["auth", "rotate"])
    assert exc.value.code == 1


# ── status --watch (TF-3) ─────────────────────────────────────

def test_main_status_watch_calls_print_status_repeatedly(capsys):
    """--watch フラグで print_status が複数回呼ばれること（KeyboardInterrupt で停止）。"""
    from unittest.mock import patch
    call_count = {"n": 0}

    def fake_print_status():
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise KeyboardInterrupt

    with patch("airflow_mcp.ops.monitor.print_status", side_effect=fake_print_status), \
         patch("time.sleep"):
        try:
            _main(["status", "--watch"])
        except KeyboardInterrupt:
            pass
    assert call_count["n"] >= 2


def test_main_status_watch_uses_custom_interval(capsys):
    """--interval N が時間指定として渡されること。"""
    from unittest.mock import patch
    sleep_calls = []

    def fake_print_status():
        if len(sleep_calls) >= 1:
            raise KeyboardInterrupt

    with patch("airflow_mcp.ops.monitor.print_status", side_effect=fake_print_status), \
         patch("time.sleep", side_effect=lambda n: sleep_calls.append(n)):
        try:
            _main(["status", "--watch", "--interval", "10"])
        except KeyboardInterrupt:
            pass
    assert any(n == 10 for n in sleep_calls)


# ── status --watch --interval invalid value ───────────────────

def test_main_status_watch_invalid_interval_uses_default(capsys):
    """--interval に非整数を渡したとき ValueError を無視してデフォルト5sを使うこと (line 41-42)。"""
    from unittest.mock import patch
    call_count = {"n": 0}

    def fake_ps():
        call_count["n"] += 1
        if call_count["n"] >= 1:
            raise KeyboardInterrupt

    with patch("airflow_mcp.ops.monitor.print_status", side_effect=fake_ps), \
         patch("time.sleep"):
        try:
            _main(["status", "--watch", "--interval", "notanint"])
        except KeyboardInterrupt:
            pass
    assert call_count["n"] >= 1


# ── auth rotate --ttl ─────────────────────────────────────────

def test_main_auth_rotate_with_ttl(capsys):
    """auth rotate --ttl N が ttl_sec に渡されること (lines 188-192)。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.rotate_token", return_value="rotated.tok") as mock_rot:
        _main(["auth", "rotate", "--agent", "agent_x", "--scopes", "tasks:read", "--ttl", "7200"])
    mock_rot.assert_called_once_with("agent_x", scopes=["tasks:read"], ttl_sec=7200)


def test_main_auth_rotate_unknown_flag_skipped(capsys):
    """auth rotate に未知フラグがあっても --agent が設定されれば正常動作すること (line 192)。"""
    from unittest.mock import patch
    with patch("airflow_mcp.common.auth.rotate_token", return_value="tok.x.y"):
        _main(["auth", "rotate", "--agent", "agent_x", "--scopes", "tasks:read", "--unknown"])
    out = capsys.readouterr().out
    assert "rotated" in out

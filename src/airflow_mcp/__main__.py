"""airflow-mcp エントリーポイント。

MCP サーバー起動:
    airflow-mcp taskboard
    airflow-mcp notes | models | gpu | agents

運用 CLI（DDD-08 §5, §7）:
    airflow-mcp status           -- ダッシュボード表示
    airflow-mcp batch [--loop]   -- バッチスケジューラ（1回 or ループ）
    airflow-mcp audit verify     -- 監査ログ整合性チェック
    airflow-mcp audit hash       -- 当日ハッシュ計算
"""
import asyncio
import sys


def main() -> None:
    if len(sys.argv) < 2:
        _print_usage()
        sys.exit(1)

    cmd = sys.argv[1].lower()

    # MCP サーバー起動
    if cmd in ("taskboard", "notes", "models", "gpu", "agents"):
        _run_mcp_server(cmd)
        return

    # 運用 CLI
    if cmd == "status":
        from .ops.monitor import print_status
        print_status()
        return

    if cmd == "batch":
        loop = "--loop" in sys.argv[2:]
        if loop:
            from .ops.batch_scheduler import run_loop
            run_loop()
        else:
            from .ops.batch_scheduler import run_due_schedules
            results = run_due_schedules()
            if results:
                for r in results:
                    print(f"[batch] {r['name']}: {r['success']}/{r['total']} ok ({r['duration_s']}s)")
            else:
                print("[batch] no schedules due at this time")
        return

    if cmd == "audit":
        sub = sys.argv[2].lower() if len(sys.argv) > 2 else "verify"
        if sub == "hash":
            from .ops.audit_integrity import run_chain_hash
            result = run_chain_hash()
            print(f"[audit] hash computed for {result['date']}: {result['hash'][:24]}...")
        elif sub == "verify":
            since = sys.argv[3] if len(sys.argv) > 3 else None
            from .ops.audit_integrity import verify_chain
            failures = verify_chain(since=since)
            if failures:
                print(f"[audit] CHAIN BROKEN at {len(failures)} date(s):")
                for f in failures:
                    print(f"  {f['date']}: expected {f['expected_hash'][:16]}... got {f['actual_hash'][:16]}...")
                sys.exit(1)
            else:
                print("[audit] chain ok — no tampering detected")
        return

    print(f"Unknown command: {cmd}", file=sys.stderr)
    _print_usage()
    sys.exit(1)


def _run_mcp_server(server_name: str) -> None:
    if server_name == "taskboard":
        from .taskboard.server import run
    elif server_name == "notes":
        from .notes.server import run
    elif server_name == "models":
        from .models.server import run
    elif server_name == "gpu":
        from .gpu.server import run
    elif server_name == "agents":
        from .agents.server import run
    else:
        raise ValueError(server_name)
    asyncio.run(run())


def _print_usage() -> None:
    print("Usage: airflow-mcp <command>", file=sys.stderr)
    print("  MCP servers : taskboard | notes | models | gpu | agents", file=sys.stderr)
    print("  Ops CLI     : status | batch [--loop] | audit <verify|hash>", file=sys.stderr)


if __name__ == "__main__":
    main()

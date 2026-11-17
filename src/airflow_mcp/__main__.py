"""airflow-mcp エントリーポイント。

MCP サーバー起動:
    airflow-mcp taskboard
    airflow-mcp notes | models | gpu | agents

運用 CLI（DDD-08 §4, §5, §7）:
    airflow-mcp status           -- ダッシュボード表示
    airflow-mcp batch [--loop]   -- バッチスケジューラ（1回 or ループ）
    airflow-mcp audit verify     -- 監査ログ整合性チェック
    airflow-mcp audit hash       -- 当日ハッシュ計算
    airflow-mcp auth keygen      -- Ed25519 キーペア生成
    airflow-mcp auth issue       -- JWT 発行
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
        if "--watch" in sys.argv[2:]:
            interval = 5
            args = sys.argv[2:]
            for i, a in enumerate(args):
                if a == "--interval" and i + 1 < len(args):
                    try:
                        interval = int(args[i + 1])
                    except ValueError:
                        pass
            import time
            while True:
                print("\033[2J\033[H", end="", flush=True)
                print_status()
                time.sleep(interval)
        else:
            print_status()
        return

    if cmd == "batch":
        loop = "--loop" in sys.argv[2:]
        if loop:
            from .orchestrator.batch_scheduler import run_loop
            run_loop()
        else:
            from .orchestrator.batch_scheduler import run_due_schedules
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

    if cmd == "auth":
        _run_auth_command()
        return

    if cmd == "config":
        _run_config_command()
        return

    if cmd == "ingester":
        from .ingester.main import load_config, run_loop
        from .taskboard.store import TaskStore
        store = TaskStore()
        config = load_config()
        run_loop(store, config)
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


def _run_auth_command() -> None:
    sub = sys.argv[2].lower() if len(sys.argv) > 2 else ""
    if sub == "keygen":
        key_dir = None
        if "--dir" in sys.argv:
            idx = sys.argv.index("--dir")
            if idx + 1 < len(sys.argv):
                from pathlib import Path
                key_dir = Path(sys.argv[idx + 1])
        from .common.auth import generate_keypair
        priv, pub = generate_keypair(key_dir)
        print(f"[auth] keypair generated: {priv} / {pub}")
    elif sub == "issue":
        agent_id = None
        scopes: list[str] = []
        ttl = 3600
        args = sys.argv[3:]
        i = 0
        while i < len(args):
            if args[i] == "--agent" and i + 1 < len(args):
                agent_id = args[i + 1]
                i += 2
            elif args[i] == "--scopes" and i + 1 < len(args):
                scopes = [s.strip() for s in args[i + 1].split(",")]
                i += 2
            elif args[i] == "--ttl" and i + 1 < len(args):
                ttl = int(args[i + 1])
                i += 2
            else:
                i += 1
        if not agent_id:
            print("[auth] --agent <id> is required", file=sys.stderr)
            sys.exit(1)
        from .common.auth import issue_jwt
        token = issue_jwt(sub=agent_id, scopes=scopes, ttl_sec=ttl)
        print(f"[auth] JWT issued for {agent_id} (ttl={ttl}s): {token}")
    elif sub == "list":
        from .common.auth import list_tokens
        tokens = list_tokens()
        if not tokens:
            print("[auth] no tokens registered")
        else:
            for t in tokens:
                print(f"[auth] {t['agent_id']:20s}  scopes={','.join(t['scopes'])}  created={t['created_at']}")
    elif sub == "revoke":
        agent_id = None
        args = sys.argv[3:]
        for i, a in enumerate(args):
            if a == "--agent" and i + 1 < len(args):
                agent_id = args[i + 1]
        if not agent_id:
            print("[auth] --agent <id> is required", file=sys.stderr)
            sys.exit(1)
        from .common.auth import revoke_token
        ok = revoke_token(agent_id)
        if ok:
            print(f"[auth] revoked token for {agent_id}")
        else:
            print(f"[auth] no token found for {agent_id}", file=sys.stderr)
            sys.exit(1)
    elif sub == "rotate":
        agent_id = None
        scopes: list[str] = []
        ttl = 3600
        args = sys.argv[3:]
        i = 0
        while i < len(args):
            if args[i] == "--agent" and i + 1 < len(args):
                agent_id = args[i + 1]
                i += 2
            elif args[i] == "--scopes" and i + 1 < len(args):
                scopes = [s.strip() for s in args[i + 1].split(",")]
                i += 2
            elif args[i] == "--ttl" and i + 1 < len(args):
                ttl = int(args[i + 1])
                i += 2
            else:
                i += 1
        if not agent_id:
            print("[auth] --agent <id> is required", file=sys.stderr)
            sys.exit(1)
        from .common.auth import rotate_token
        token = rotate_token(agent_id, scopes=scopes, ttl_sec=ttl)
        print(f"[auth] rotated token for {agent_id} (ttl={ttl}s): {token}")
    else:
        print(f"[auth] unknown sub-command: {sub!r}", file=sys.stderr)
        print("  auth keygen [--dir DIR]", file=sys.stderr)
        print("  auth issue --agent <id> --scopes <s1,s2> [--ttl 3600]", file=sys.stderr)
        print("  auth list", file=sys.stderr)
        print("  auth revoke --agent <id>", file=sys.stderr)
        print("  auth rotate --agent <id> --scopes <s1,s2> [--ttl 3600]", file=sys.stderr)
        sys.exit(1)


def _run_config_command() -> None:
    from pathlib import Path
    from .common.config_validator import validate_config
    from .common.io import safe_yaml_load

    config_dir = Path(".airflow/config")
    names = ["auth", "chains", "cleaner", "dispatch", "ingester",
             "model-config", "notifications", "parallel", "schedules"]
    total_errors = 0
    for name in names:
        path = config_dir / f"{name}.yaml"
        if not path.exists():
            print(f"  SKIP  {name}.yaml (not found)")
            continue
        data = safe_yaml_load(path)
        errs = validate_config(name, data)
        if errs:
            total_errors += len(errs)
            for e in errs:
                print(f"  ERROR {e}", file=sys.stderr)
        else:
            print(f"  OK    {name}.yaml")
    if total_errors:
        print(f"\n{total_errors} validation error(s) found.", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll configs valid.")


def _print_usage() -> None:
    print("Usage: airflow-mcp <command>", file=sys.stderr)
    print("  MCP servers : taskboard | notes | models | gpu | agents", file=sys.stderr)
    print("  Ingester    : ingester", file=sys.stderr)
    print("  Ops CLI     : status [--watch [--interval N]] | batch [--loop]", file=sys.stderr)
    print("              : audit <verify|hash>", file=sys.stderr)
    print("  Config CLI  : config validate", file=sys.stderr)
    print("  Auth CLI    : auth keygen | auth issue | auth list", file=sys.stderr)
    print("              : auth revoke --agent <id> | auth rotate --agent <id>", file=sys.stderr)


if __name__ == "__main__":
    main()

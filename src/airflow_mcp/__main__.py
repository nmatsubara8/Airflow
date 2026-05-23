"""airflow-mcp エントリーポイント。

使い方:
    airflow-mcp taskboard
    airflow-mcp notes
    airflow-mcp models
    airflow-mcp gpu
    airflow-mcp agents

DDD-02 §9.1 の stdio 起動パターン:
    "command": "airflow-mcp", "args": ["taskboard"]
"""
import asyncio
import sys


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: airflow-mcp <server>", file=sys.stderr)
        print("  servers: taskboard | notes | models | gpu | agents", file=sys.stderr)
        sys.exit(1)

    server_name = sys.argv[1].lower()

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
        print(f"Unknown server: {server_name}", file=sys.stderr)
        sys.exit(1)

    asyncio.run(run())


if __name__ == "__main__":
    main()

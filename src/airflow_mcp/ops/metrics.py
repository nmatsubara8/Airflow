"""Prometheus テキスト形式メトリクスエクスポート（DDD-08 §5.3 / DDD-07 §11.1）。

HTTP GET /metrics エンドポイントや CLI から呼び出す。
prometheus_client ライブラリは不要 — テキスト形式を直接生成する。
"""
from __future__ import annotations

import logging
import os
from http.server import HTTPServer
from pathlib import Path
from ..common.http_server import _BaseHandler, start_server
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..taskboard.store import TaskStore

logger = logging.getLogger(__name__)

_METRICS_PORT = int(os.environ.get("AIRFLOW_METRICS_PORT", "9090"))


def collect_task_metrics(store: TaskStore) -> dict[str, Any]:
    """タスクボードから集計メトリクスを収集する。"""
    statuses = ["inbox", "in_progress", "done", "needs_human", "cancelled"]
    counts: dict[str, dict[str, int]] = {}

    for status in statuses:
        try:
            tasks = store.list_by_status(status)
        except Exception as exc:
            logger.debug("collect_task_metrics: list_by_status(%s) failed: %s", status, exc)
            tasks = []
        for t in tasks:
            section = t.get("section", "unknown") if isinstance(t, dict) else "unknown"
            key = (status, section)
            counts[key] = counts.get(key, 0) + 1

    return {"task_counts": counts}


def collect_gpu_metrics() -> dict[str, Any]:
    """GPU / スケジューラ状態メトリクスを収集する。"""
    try:
        import airflow_mcp.gpu.server as gpu_mod
        snap = gpu_mod._gpu.snapshot()
        return {
            "gpu_vram_used_gb": snap.get("vram_used_gb", 0.0),
            "gpu_vram_total_gb": snap.get("vram_total_gb", 16.0),
            "scheduler_degraded": 1 if gpu_mod._degraded else 0,
            "scheduler_queue_depths": dict(gpu_mod._scheduler_queue.model_depths()),
        }
    except Exception as exc:
        logger.debug("collect_gpu_metrics: %s", exc)
        return {}


def collect_execution_metrics() -> dict[str, Any]:
    """実行ログから duration 統計を収集する。"""
    log_path = Path(os.environ.get("AIRFLOW_EXECUTION_LOG", ".airflow/logs/execution.jsonl"))
    if not log_path.exists():
        return {}

    import json

    durations: dict[str, list[float]] = {}
    try:
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            model = entry.get("model_id", "unknown")
            dur = entry.get("duration_s")
            if isinstance(dur, (int, float)):
                durations.setdefault(model, []).append(float(dur))
    except OSError as exc:
        logger.debug("collect_execution_metrics: %s", exc)
        return {}

    result: dict[str, Any] = {}
    for model, vals in durations.items():
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        result[model] = {
            "count": n,
            "sum": sum(vals_sorted),
            "p50": vals_sorted[n // 2] if n else 0.0,
            "p95": vals_sorted[int(n * 0.95)] if n else 0.0,
        }
    return {"execution_durations": result}


def render_prometheus_text(
    task_metrics: dict[str, Any],
    gpu_metrics: dict[str, Any],
    exec_metrics: dict[str, Any],
) -> str:
    """収集済みメトリクスを Prometheus テキスト形式に変換する。"""
    lines: list[str] = []

    # ── タスク数 ──
    lines.append("# HELP airflow_tasks_total Tasks by status and section")
    lines.append("# TYPE airflow_tasks_total gauge")
    for (status, section), count in task_metrics.get("task_counts", {}).items():
        lines.append(f'airflow_tasks_total{{status="{status}",section="{section}"}} {count}')

    # ── GPU / スケジューラ ──
    if gpu_metrics:
        lines.append("# HELP airflow_gpu_vram_used_gb GPU VRAM used in GB")
        lines.append("# TYPE airflow_gpu_vram_used_gb gauge")
        lines.append(f"airflow_gpu_vram_used_gb {gpu_metrics.get('gpu_vram_used_gb', 0.0)}")

        lines.append("# HELP airflow_gpu_vram_total_gb GPU VRAM total in GB")
        lines.append("# TYPE airflow_gpu_vram_total_gb gauge")
        lines.append(f"airflow_gpu_vram_total_gb {gpu_metrics.get('gpu_vram_total_gb', 16.0)}")

        lines.append("# HELP airflow_scheduler_degraded Scheduler degradation mode (1=on)")
        lines.append("# TYPE airflow_scheduler_degraded gauge")
        lines.append(f"airflow_scheduler_degraded {gpu_metrics.get('scheduler_degraded', 0)}")

        lines.append("# HELP airflow_scheduler_queue_depth Tasks queued per model")
        lines.append("# TYPE airflow_scheduler_queue_depth gauge")
        for model_id, depth in gpu_metrics.get("scheduler_queue_depths", {}).items():
            lines.append(f'airflow_scheduler_queue_depth{{model_id="{model_id}"}} {depth}')

    # ── 実行ログ duration ──
    for model_id, stats in exec_metrics.get("execution_durations", {}).items():
        lines.append("# HELP airflow_model_execution_duration_seconds Model execution duration")
        lines.append("# TYPE airflow_model_execution_duration_seconds summary")
        lines.append(
            f'airflow_model_execution_duration_seconds_count{{model="{model_id}"}} {stats["count"]}'
        )
        lines.append(
            f'airflow_model_execution_duration_seconds_sum{{model="{model_id}"}} {stats["sum"]:.3f}'
        )
        lines.append(
            f'airflow_model_execution_duration_seconds{{model="{model_id}",quantile="0.5"}} {stats["p50"]:.3f}'
        )
        lines.append(
            f'airflow_model_execution_duration_seconds{{model="{model_id}",quantile="0.95"}} {stats["p95"]:.3f}'
        )

    return "\n".join(lines) + "\n"


def get_metrics_text(store: TaskStore | None = None) -> str:
    """全メトリクスを収集して Prometheus テキストを返す。"""
    task_m = collect_task_metrics(store) if store is not None else {}
    gpu_m = collect_gpu_metrics()
    exec_m = collect_execution_metrics()
    return render_prometheus_text(task_m, gpu_m, exec_m)


class _MetricsHandler(_BaseHandler):
    """GET /metrics → Prometheus テキスト形式を返す。"""

    _server_name = "metrics-http"
    store: TaskStore | None = None

    def handle_get(self, path: str, qs: dict) -> tuple[int, str, str]:
        if path == "/metrics":
            return 200, "text/plain; version=0.0.4; charset=utf-8", get_metrics_text(self.__class__.store)
        return 404, "text/plain", "Not Found"


def start_metrics_server(store: TaskStore | None = None, port: int = _METRICS_PORT) -> HTTPServer:
    """バックグラウンドスレッドで Prometheus メトリクス HTTP サーバーを起動する。"""
    _MetricsHandler.store = store
    return start_server(_MetricsHandler, port, "metrics-server")

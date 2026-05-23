"""GPU State Manager（DDD-07 §9）。

VRAM 予約・解放・LRU 退避・pinned 制御・スナップショット永続化。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.audit import log_event, new_audit_id, now_iso
from ..common.errors import GpuBusy

logger = logging.getLogger(__name__)

_SNAPSHOT_PATH = Path(".airflow/runtime/gpu_state.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_to_duration_ms(loaded_at_iso: str) -> float:
    """loaded_at ISO文字列から現在までの経過ミリ秒を返す。"""
    try:
        loaded_at = datetime.fromisoformat(loaded_at_iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - loaded_at).total_seconds() * 1000
    except (ValueError, AttributeError):
        return 0.0


class GpuStateManager:
    """スレッドセーフな GPU 状態管理（DDD-07 §9）。"""

    def __init__(
        self,
        vram_available_gb: float = 15.0,
        vram_total_gb: float = 16.0,
        pinned_models: list[str] | None = None,
        snapshot_path: str | Path = _SNAPSHOT_PATH,
        snapshot_interval_s: float = 0,
    ) -> None:
        self._lock = threading.Lock()
        self._vram_available = vram_available_gb
        self._vram_total = vram_total_gb
        self._pinned: set[str] = set(pinned_models or [])
        self._snapshot_path = Path(snapshot_path)
        self._snapshot_timer: threading.Timer | None = None

        # model_id → {id, vram_gb, loaded_at, last_used_at, pending_tasks, pinned}
        self._loaded: dict[str, dict[str, Any]] = {}

        if snapshot_interval_s > 0:
            self._start_periodic_snapshot(snapshot_interval_s)

    # ── 公開 API ──────────────────────────────────────────

    @property
    def free_vram_gb(self) -> float:
        used = sum(m["vram_gb"] for m in self._loaded.values())
        return self._vram_available - used

    @property
    def loaded_model_ids(self) -> list[str]:
        return list(self._loaded.keys())

    def reserve(
        self,
        model_id: str,
        vram_gb: float = 9.0,
        wait: bool = True,
        registry: dict[str, dict] | None = None,
    ) -> dict[str, Any]:
        """VRAM を確保してモデルをロード（DDD-07 §9.2）。

        Returns:
            {"reserved": bool, "loaded_at": str, "evicted_models": [str]}
        """
        with self._lock:
            if model_id in self._loaded:
                self._loaded[model_id]["last_used_at"] = _now_iso()
                return {"reserved": True, "loaded_at": self._loaded[model_id]["loaded_at"],
                        "evicted_models": []}

            actual_vram = vram_gb
            if registry and model_id in registry:
                actual_vram = registry[model_id].get("vram_gb", vram_gb)

            evicted = self._evict_for(actual_vram, wait=wait)

            ts = _now_iso()
            self._loaded[model_id] = {
                "id": model_id,
                "vram_gb": actual_vram,
                "loaded_at": ts,
                "last_used_at": ts,
                "pending_tasks": 0,
                "pinned": model_id in self._pinned,
            }

            aid = new_audit_id()
            log_event("info", "gpu.reserved", "gpu-state", aid,
                      model_id=model_id,
                      data={"evicted": evicted, "vram_gb": actual_vram})
            self._write_snapshot()
            return {"reserved": True, "loaded_at": ts, "evicted_models": evicted}

    def release(self, model_id: str) -> dict[str, Any]:
        """モデルをアンロード（DDD-07 §9.1）。duration_ms を結果に含める。"""
        with self._lock:
            if model_id not in self._loaded:
                return {"released": False, "free_vram_gb": self.free_vram_gb, "duration_ms": 0.0}
            model_info = self._loaded.pop(model_id)
            duration_ms = _iso_to_duration_ms(model_info.get("loaded_at", ""))
            log_event("info", "gpu.released", "gpu-state", new_audit_id(),
                      model_id=model_id, data={"duration_ms": int(duration_ms)})
            self._write_snapshot()
            return {"released": True, "free_vram_gb": self.free_vram_gb, "duration_ms": duration_ms}

    def touch(self, model_id: str) -> None:
        """モデルの last_used_at を更新（LRU 用）。"""
        with self._lock:
            if model_id in self._loaded:
                self._loaded[model_id]["last_used_at"] = _now_iso()

    def set_pending(self, model_id: str, delta: int) -> None:
        """pending_tasks を増減する（+1 でロック、-1 で解放）。"""
        with self._lock:
            if model_id in self._loaded:
                self._loaded[model_id]["pending_tasks"] = max(
                    0, self._loaded[model_id]["pending_tasks"] + delta
                )

    def snapshot(self) -> dict[str, Any]:
        """gpu_state.schema.json 準拠のスナップショットを返す（DDD-07 §9.4）。"""
        with self._lock:
            return self._build_snapshot()

    def _build_snapshot(self) -> dict[str, Any]:
        """ロックを取得せずスナップショットを構築する（ロック保持中から呼ぶ）。"""
        used = sum(m["vram_gb"] for m in self._loaded.values())
        return {
            "schema_version": "1.0.0",
            "snapshot_at": _now_iso(),
            "total_vram_gb": self._vram_total,
            "used_vram_gb": round(used, 2),
            "free_vram_gb": round(self._vram_available - used, 2),
            "loaded_models": [
                {
                    "model_id": m["id"],
                    "vram_gb": m["vram_gb"],
                    "loaded_at": m["loaded_at"],
                    "last_used_at": m.get("last_used_at"),
                    "pending_tasks": m.get("pending_tasks", 0),
                }
                for m in self._loaded.values()
            ],
            "active_profile": None,  # gpu/server.py 側で上書き
        }

    # ── 内部実装 ──────────────────────────────────────────

    def _evict_for(self, vram_needed: float, wait: bool) -> list[str]:
        """必要 VRAM を確保するために LRU 退避する（DDD-07 §9.2）。

        Returns:
            退避したモデル ID のリスト。
        Raises:
            GpuBusy: 退避できない場合（pinned / pending_tasks > 0）。
        """
        evicted: list[str] = []
        if self.free_vram_gb >= vram_needed:
            return evicted

        # 退避候補: pinned でなく pending_tasks == 0 を LRU 順
        candidates = sorted(
            [m for m in self._loaded.values()
             if not m.get("pinned") and m.get("pending_tasks", 0) == 0],
            key=lambda m: m.get("last_used_at", ""),
        )

        freed = 0.0
        for cand in candidates:
            if freed + self.free_vram_gb >= vram_needed:
                break
            freed += cand["vram_gb"]
            evicted.append(cand["id"])
            del self._loaded[cand["id"]]
            log_event("info", "gpu.model_evicted", "gpu-state", new_audit_id(),
                      model_id=cand["id"])

        if self.free_vram_gb < vram_needed:
            if not wait:
                raise GpuBusy(retry_after_ms=5_000)
            raise GpuBusy(retry_after_ms=10_000)

        return evicted

    def _write_snapshot(self) -> None:
        """スナップショットをファイルに書き込む（DDD-07 §9.4）。ロック保持中から呼ぶこと。"""
        try:
            snap = self._build_snapshot()
            self._snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            self._snapshot_path.write_text(
                json.dumps(snap, ensure_ascii=False, indent=2)
            )
        except Exception as exc:
            logger.warning("gpu snapshot write failed: %s", exc)

    def _start_periodic_snapshot(self, interval_s: float) -> None:
        """DDD-07 §9.4: 定期スナップショット書き込みを開始する。"""
        def _tick() -> None:
            with self._lock:
                self._write_snapshot()
            self._snapshot_timer = threading.Timer(interval_s, _tick)
            self._snapshot_timer.daemon = True
            self._snapshot_timer.start()

        self._snapshot_timer = threading.Timer(interval_s, _tick)
        self._snapshot_timer.daemon = True
        self._snapshot_timer.start()

    def stop_periodic_snapshot(self) -> None:
        """定期スナップショットを停止する。"""
        if self._snapshot_timer is not None:
            self._snapshot_timer.cancel()
            self._snapshot_timer = None

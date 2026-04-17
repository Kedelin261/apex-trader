"""
APEX MULTI-MARKET TJR ENGINE
Persistence Manager — Phase 6: Full state persistence and startup recovery.

PERSISTED STATE:
  - open positions (engine view)
  - pending orders (submitted but not confirmed)
  - trade ledger (closed trades)
  - rejection history
  - loop heartbeat timestamps
  - last processed candle timestamps per symbol
  - signal dedup cache (submitted signal IDs to prevent re-execution after restart)
  - risk governor daily counters (handled by RiskGovernor itself)
  - execution state machine state (handled by ExecutionStateMachine itself)

STARTUP RECOVERY:
  1. Load persisted state from disk
  2. Request live broker positions (via BrokerManager)
  3. Reconcile: identify orphaned/missing positions
  4. Rebuild in-memory context from merged state
  5. Mark previously-submitted signals as processed (dedup)
  6. NEVER blindly resubmit prior orders

All files are JSON/JSONL stored in data/ directory.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SIGNAL DEDUP CACHE
# ---------------------------------------------------------------------------

class SignalDedupCache:
    """
    Prevents duplicate signal execution after restart.

    When a signal is submitted, its ID is recorded here.
    On startup, previously-submitted IDs are loaded so the same signal
    is never submitted twice, even after process restart.

    Format: one signal_id per line in data/submitted_signals.jsonl
    """

    def __init__(self, cache_file: Path, max_age_hours: float = 24.0):
        self._file = cache_file
        self._max_age_hours = max_age_hours
        self._submitted: Dict[str, str] = {}   # signal_id → submitted_at ISO
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        """Load submitted signal IDs from disk."""
        if not self._file.exists():
            return
        try:
            cutoff = datetime.now(timezone.utc).timestamp() - self._max_age_hours * 3600
            loaded = 0
            with self._file.open("r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        sid = entry.get("signal_id")
                        ts = entry.get("submitted_at", "")
                        if sid and ts:
                            # Only keep entries within max_age
                            try:
                                entry_ts = datetime.fromisoformat(ts).timestamp()
                                if entry_ts >= cutoff:
                                    self._submitted[sid] = ts
                                    loaded += 1
                            except Exception:
                                pass
                    except Exception:
                        pass
            logger.info(
                f"[SignalDedup] Loaded {loaded} signal IDs "
                f"(within {self._max_age_hours}h)"
            )
        except Exception as e:
            logger.error(f"[SignalDedup] Failed to load: {e}")

    def is_submitted(self, signal_id: str) -> bool:
        """Return True if this signal_id was already submitted."""
        with self._lock:
            return signal_id in self._submitted

    def mark_submitted(self, signal_id: str):
        """Mark a signal as submitted and persist to disk."""
        with self._lock:
            ts = datetime.now(timezone.utc).isoformat()
            self._submitted[signal_id] = ts
            try:
                with self._file.open("a") as f:
                    f.write(json.dumps({
                        "signal_id": signal_id,
                        "submitted_at": ts,
                    }) + "\n")
            except Exception as e:
                logger.error(f"[SignalDedup] Failed to persist {signal_id}: {e}")

    def size(self) -> int:
        with self._lock:
            return len(self._submitted)


# ---------------------------------------------------------------------------
# LOOP HEARTBEAT STORE
# ---------------------------------------------------------------------------

class LoopHeartbeatStore:
    """
    Tracks the last heartbeat timestamp for each scheduler loop.
    Persisted to disk so monitoring can detect missed heartbeats across restarts.
    """

    def __init__(self, heartbeat_file: Path):
        self._file = heartbeat_file
        self._beats: Dict[str, str] = {}   # loop_name → last_beat ISO
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if not self._file.exists():
            return
        try:
            data = json.loads(self._file.read_text())
            self._beats = data.get("heartbeats", {})
        except Exception as e:
            logger.debug(f"[Heartbeats] Load error: {e}")

    def beat(self, loop_name: str):
        """Record a heartbeat for the named loop."""
        ts = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._beats[loop_name] = ts
        self._persist()

    def get_all(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._beats)

    def is_stale(self, loop_name: str, max_age_seconds: float) -> bool:
        """Return True if loop hasn't beaten within max_age_seconds."""
        with self._lock:
            ts_str = self._beats.get(loop_name)
        if not ts_str:
            return True
        try:
            last = datetime.fromisoformat(ts_str)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - last).total_seconds()
            return age > max_age_seconds
        except Exception:
            return True

    def _persist(self):
        try:
            with self._lock:
                data = dict(self._beats)
            self._file.write_text(json.dumps({
                "heartbeats": data,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2))
        except Exception as e:
            logger.debug(f"[Heartbeats] Persist error: {e}")


# ---------------------------------------------------------------------------
# POSITION STATE STORE
# ---------------------------------------------------------------------------

class PositionStateStore:
    """
    Persists engine-view open positions to disk.
    Loaded on startup to rebuild in-memory context.
    """

    def __init__(self, positions_file: Path):
        self._file = positions_file
        self._positions: List[dict] = []
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if not self._file.exists():
            return
        try:
            data = json.loads(self._file.read_text())
            self._positions = data.get("positions", [])
            logger.info(
                f"[PositionStore] Loaded {len(self._positions)} persisted positions"
            )
        except Exception as e:
            logger.error(f"[PositionStore] Load error: {e}")

    def get_all(self) -> List[dict]:
        with self._lock:
            return list(self._positions)

    def upsert(self, position: dict):
        """Add or update a position by id."""
        pos_id = position.get("id")
        with self._lock:
            for i, p in enumerate(self._positions):
                if p.get("id") == pos_id:
                    self._positions[i] = position
                    break
            else:
                self._positions.append(position)
        self._persist()

    def remove(self, position_id: str):
        """Remove a position by id (when closed)."""
        with self._lock:
            self._positions = [
                p for p in self._positions if p.get("id") != position_id
            ]
        self._persist()

    def replace_all(self, positions: List[dict]):
        """Replace entire position list (after reconciliation)."""
        with self._lock:
            self._positions = list(positions)
        self._persist()

    def _persist(self):
        try:
            with self._lock:
                data = list(self._positions)
            self._file.write_text(json.dumps({
                "positions": data,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2))
        except Exception as e:
            logger.error(f"[PositionStore] Persist error: {e}")


# ---------------------------------------------------------------------------
# CANDLE TIMESTAMP CACHE
# ---------------------------------------------------------------------------

class CandleTimestampCache:
    """
    Tracks the last candle timestamp processed per symbol.
    Prevents re-processing old candles after restart.
    """

    def __init__(self, ts_file: Path):
        self._file = ts_file
        self._timestamps: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if not self._file.exists():
            return
        try:
            data = json.loads(self._file.read_text())
            self._timestamps = data.get("last_candle_at", {})
        except Exception:
            pass

    def get(self, symbol: str) -> Optional[str]:
        with self._lock:
            return self._timestamps.get(symbol)

    def update(self, symbol: str, timestamp_iso: str):
        with self._lock:
            self._timestamps[symbol] = timestamp_iso
        self._persist()

    def _persist(self):
        try:
            with self._lock:
                data = dict(self._timestamps)
            self._file.write_text(json.dumps({
                "last_candle_at": data,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# PERSISTENCE MANAGER (orchestrates all stores)
# ---------------------------------------------------------------------------

class PersistenceManager:
    """
    Orchestrates all persistence stores.
    Provides startup recovery and state reconstruction.
    """

    def __init__(self, config: dict):
        self._config = config
        data_dir = Path(config.get("data", {}).get("state_dir", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)

        # Instantiate all stores
        self.signal_dedup = SignalDedupCache(
            cache_file=data_dir / "submitted_signals.jsonl",
            max_age_hours=config.get("autonomy", {}).get(
                "signal_dedup_cache_hours", 24.0
            ),
        )
        self.heartbeats = LoopHeartbeatStore(
            heartbeat_file=data_dir / "loop_heartbeats.json"
        )
        self.positions = PositionStateStore(
            positions_file=data_dir / "open_positions.json"
        )
        self.candle_timestamps = CandleTimestampCache(
            ts_file=data_dir / "candle_timestamps.json"
        )

        logger.info(
            f"[PersistenceManager] Initialised — "
            f"signals_in_cache={self.signal_dedup.size()} "
            f"positions_on_disk={len(self.positions.get_all())}"
        )

    def run_startup_reconciliation(self, broker_manager) -> dict:
        """
        On startup: compare persisted positions against live broker positions.
        Returns reconciliation summary.

        RULE: Never blindly resubmit. Only reconcile state.
        """
        persisted = self.positions.get_all()
        live_positions = []

        if broker_manager and broker_manager.is_connected():
            try:
                broker_positions = broker_manager.get_positions()
                live_positions = [
                    {
                        "id": getattr(p, "order_id", None) or p.__dict__.get("order_id", ""),
                        "symbol": p.instrument,
                        "direction": p.side.value if hasattr(p.side, "value") else str(p.side),
                        "units": p.units,
                        "entry_price": getattr(p, "avg_price", 0.0),
                        "unrealized_pnl": getattr(p, "unrealized_pnl", 0.0),
                    }
                    for p in broker_positions
                ]
            except Exception as e:
                logger.error(f"[PersistenceManager] Failed to get live positions: {e}")

        # Compare
        persisted_ids = {p.get("id") for p in persisted if p.get("id")}
        live_ids = {p.get("id") for p in live_positions if p.get("id")}

        orphaned_in_engine = persisted_ids - live_ids     # Engine knows, broker doesn't
        orphaned_at_broker  = live_ids - persisted_ids    # Broker knows, engine doesn't

        if orphaned_in_engine:
            logger.warning(
                f"[PersistenceManager] {len(orphaned_in_engine)} engine positions "
                f"not found at broker: {orphaned_in_engine} — marking as closed"
            )
            for pos_id in orphaned_in_engine:
                self.positions.remove(pos_id)

        if orphaned_at_broker:
            logger.warning(
                f"[PersistenceManager] {len(orphaned_at_broker)} broker positions "
                f"not tracked in engine: {orphaned_at_broker} — adding to state"
            )
            for pos in live_positions:
                if pos.get("id") in orphaned_at_broker:
                    pos["_recovered_at_startup"] = datetime.now(timezone.utc).isoformat()
                    self.positions.upsert(pos)

        summary = {
            "persisted_count": len(persisted),
            "live_broker_count": len(live_positions),
            "orphaned_in_engine": len(orphaned_in_engine),
            "orphaned_at_broker": len(orphaned_at_broker),
            "final_position_count": len(self.positions.get_all()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(f"[PersistenceManager] Startup reconciliation: {summary}")
        return summary

    def status(self) -> dict:
        """Return persistence manager status."""
        return {
            "signal_dedup_cache_size": self.signal_dedup.size(),
            "persisted_positions": len(self.positions.get_all()),
            "loop_heartbeats": self.heartbeats.get_all(),
        }

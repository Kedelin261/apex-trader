"""
APEX MULTI-MARKET TJR ENGINE
Reconciliation Service — Continuously reconciles internal vs broker state.

Runs:
  - On startup (after broker connects)
  - On a configurable interval (default: 60s)
  - After any order fill or rejection event
  - On operator request via /api/reconcile/run

Outputs to:
  - logs/reconciliation_log.jsonl
  - In-memory history (last 100 results)
  - ESM state updates (blocks live if severe mismatch)
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from brokers.base_connector import ReconciliationResult

logger = logging.getLogger(__name__)


class ReconciliationService:
    """
    Reconciles internal engine state against live broker state.
    Thread-safe; can run in background thread.
    """

    def __init__(
        self,
        connector,         # BaseBrokerConnector
        orchestrator,      # AgentOrchestrator (for open positions, legacy path)
        state_machine,     # ExecutionStateMachine
        log_path: str = "logs/reconciliation_log.jsonl",
        interval_seconds: int = 60,
        auto_block_on_mismatch: bool = True,
        mismatch_threshold_usd: float = 100.0,
        scheduler=None,    # AutonomousScheduler (primary position source)
    ):
        self._connector = connector
        self._orchestrator = orchestrator
        self._esm = state_machine
        self._scheduler = scheduler   # preferred source of truth for positions
        self._log_path = Path(log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._interval = interval_seconds
        self._auto_block = auto_block_on_mismatch
        self._mismatch_threshold = mismatch_threshold_usd
        self._history: List[dict] = []
        self._last_result: Optional[ReconciliationResult] = None
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._lock = threading.Lock()

    def set_scheduler(self, scheduler) -> None:
        """
        Inject the AutonomousScheduler after construction.
        Called by the scheduler's start() so it becomes the primary
        source for _get_internal_positions().
        """
        self._scheduler = scheduler
        logger.info("[RECON] AutonomousScheduler registered as position source")

    # ------------------------------------------------------------------
    # CORE RECONCILIATION
    # ------------------------------------------------------------------

    def run_once(self, operator: str = "system") -> ReconciliationResult:
        """Execute one reconciliation cycle."""
        logger.info(f"[RECON] Running reconciliation (operator={operator})")

        # Gather internal positions
        internal_positions = self._get_internal_positions()

        try:
            result = self._connector.reconcile_state(internal_positions)
        except Exception as e:
            logger.error(f"[RECON] Reconciliation failed: {e}")
            result = ReconciliationResult(
                matched_positions=0,
                mismatched_positions=[],
                internal_only_positions=[],
                broker_only_positions=[],
                net_pnl_discrepancy_usd=0.0,
                is_clean=False,
                notes=[f"Reconciliation exception: {e}"],
            )

        with self._lock:
            self._last_result = result
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operator": operator,
                "result": result.safe_dict(),
            }
            self._history.append(entry)
            if len(self._history) > 100:
                self._history.pop(0)

        # Log to file
        try:
            with open(self._log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.warning(f"[RECON] Failed to write reconciliation log: {e}")

        # Auto-block if severe mismatch
        if self._auto_block and not result.is_clean:
            discrepancy = result.net_pnl_discrepancy_usd
            if discrepancy > self._mismatch_threshold:
                reason = (
                    f"Reconciliation mismatch exceeds ${self._mismatch_threshold:.0f} "
                    f"threshold: discrepancy=${discrepancy:.2f}, "
                    f"mismatches={len(result.mismatched_positions)}, "
                    f"broker-only={result.broker_only_positions}"
                )
                logger.error(f"[RECON] AUTO-BLOCKING live trading: {reason}")
                self._esm.block_live(reason=reason, operator="reconciliation_service")

        level = logging.INFO if result.is_clean else logging.WARNING
        logger.log(
            level,
            f"[RECON] Complete: clean={result.is_clean} "
            f"matched={result.matched_positions} "
            f"discrepancy=${result.net_pnl_discrepancy_usd:.2f}"
        )
        return result

    def _get_internal_positions(self) -> List[Dict[str, Any]]:
        """
        Extract internal open positions.

        Primary source: scheduler's _open_positions registry (populated on
        every ORDER_FILLED event).  Falls back to the legacy orchestrator
        _open_positions list so the old /api/signal path still works.
        """
        # ---- Primary: scheduler registry (plain dicts, always up-to-date) ----
        try:
            if self._scheduler is not None:
                sched_positions = self._scheduler.get_open_positions()
                if sched_positions:
                    return [
                        {
                            "instrument": p.get("symbol", ""),
                            "direction": p.get("direction", ""),
                            "unrealized_pnl": p.get("unrealized_pnl", 0.0),
                            "size_lots": p.get("units", 0.0),
                        }
                        for p in sched_positions
                    ]
        except Exception as e:
            logger.debug(f"[RECON] Scheduler position read failed: {e}")

        # ---- Fallback: legacy orchestrator open positions ----
        try:
            positions = self._orchestrator._open_positions
            return [
                {
                    "instrument": p.instrument,
                    "direction": p.direction.value,
                    "unrealized_pnl": getattr(p, "unrealized_pnl", 0.0),
                    "size_lots": getattr(p, "size_lots", 0.0),
                }
                for p in positions
            ]
        except Exception as e:
            logger.warning(f"[RECON] Failed to get internal positions: {e}")
            return []

    # ------------------------------------------------------------------
    # BACKGROUND SCHEDULING
    # ------------------------------------------------------------------

    def start_background(self) -> None:
        """Start periodic background reconciliation."""
        if self._running:
            return
        self._running = True
        self._schedule_next()
        logger.info(f"[RECON] Background reconciliation started (interval={self._interval}s)")

    def stop_background(self) -> None:
        """Stop background reconciliation."""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None
        logger.info("[RECON] Background reconciliation stopped")

    def _schedule_next(self) -> None:
        if not self._running:
            return
        self._timer = threading.Timer(self._interval, self._background_run)
        self._timer.daemon = True
        self._timer.start()

    def _background_run(self) -> None:
        try:
            if self._esm.is_connected():
                self.run_once(operator="background")
        except Exception as e:
            logger.error(f"[RECON] Background run failed: {e}")
        finally:
            self._schedule_next()

    # ------------------------------------------------------------------
    # RESULTS
    # ------------------------------------------------------------------

    def last_result(self) -> Optional[dict]:
        with self._lock:
            if self._last_result:
                return self._last_result.safe_dict()
        return None

    def history(self, limit: int = 20) -> List[dict]:
        with self._lock:
            return list(self._history[-limit:])

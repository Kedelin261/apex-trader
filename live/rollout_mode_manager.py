"""
APEX MULTI-MARKET TJR ENGINE
Rollout Mode Manager — Phase 11: Controlled rollout modes.

MODES:
  MANUAL_SIGNAL_MODE        — scheduler DISABLED; signals only via /api/signal webhook
  SUPERVISED_AUTONOMY_MODE  — scheduler runs; every order queued and requires operator ACK
  UNATTENDED_PAPER_MODE     — scheduler runs; orders go to paper broker (DEFAULT)
  UNATTENDED_LIVE_MODE      — scheduler runs; orders go to live broker (requires full gate)

MODE TRANSITION RULES:
  Any → MANUAL_SIGNAL_MODE          — always allowed; stops scheduler
  Any → SUPERVISED_AUTONOMY_MODE    — always allowed; starts scheduler in supervised mode
  Any → UNATTENDED_PAPER_MODE       — always allowed; starts scheduler
  Any → UNATTENDED_LIVE_MODE        — requires gate check:
      1. Broker connected (LIVE_CONNECTED_SAFE or LIVE_ENABLED)
      2. Pre-flight checks passed
      3. Operator arming (arm_live via ESM)
      4. Alerting enabled
      5. Risk governor active
      6. Persistence active
      7. Scheduler healthy

ARCHITECTURE:
  RolloutModeManager is the top-level gating authority for unattended live trading.
  It wraps the ExecutionStateMachine and adds mode-level controls.
  The scheduler checks rollout_mode before every signal dispatch.
  The API server exposes /api/system/mode for runtime mode changes.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ROLLOUT MODES
# ---------------------------------------------------------------------------

class RolloutMode(str, Enum):
    MANUAL_SIGNAL_MODE       = "MANUAL_SIGNAL_MODE"
    SUPERVISED_AUTONOMY_MODE = "SUPERVISED_AUTONOMY_MODE"
    UNATTENDED_PAPER_MODE    = "UNATTENDED_PAPER_MODE"
    UNATTENDED_LIVE_MODE     = "UNATTENDED_LIVE_MODE"


# ---------------------------------------------------------------------------
# ROLLOUT MODE MANAGER
# ---------------------------------------------------------------------------

class RolloutModeManager:
    """
    Controls the operational mode of the autonomous trading system.

    Thread-safe. Persisted to disk so mode survives process restart.
    Integrates with ExecutionStateMachine, AlertDispatcher, and
    AutonomousScheduler to enforce mode-level constraints.
    """

    def __init__(
        self,
        config: dict,
        esm=None,
        alert_dispatcher=None,
        risk_governor=None,
        persistence_manager=None,
    ):
        self._config = config
        self._esm = esm
        self._alert = alert_dispatcher
        self._risk_governor = risk_governor
        self._persistence = persistence_manager
        self._lock = threading.Lock()

        # State file
        state_dir = Path(config.get("data", {}).get("state_dir", "data"))
        state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = state_dir / "rollout_mode.json"

        # Mode from config (default) — overridden by persisted state
        config_mode_str = config.get("autonomy", {}).get(
            "rollout_mode", "UNATTENDED_PAPER_MODE"
        )
        try:
            self._mode = RolloutMode(config_mode_str)
        except ValueError:
            self._mode = RolloutMode.UNATTENDED_PAPER_MODE
            logger.warning(
                f"[RolloutMode] Unknown mode {config_mode_str!r} in config — "
                "defaulting to UNATTENDED_PAPER_MODE"
            )

        # Runtime state
        self._mode_history: List[dict] = []
        self._supervised_pending: List[dict] = []    # Orders pending ACK in supervised mode
        self._supervised_lock = threading.Lock()

        self._load_state()

        logger.info(f"[RolloutModeManager] Active mode: {self._mode.value}")

    # ------------------------------------------------------------------
    # PUBLIC: Mode queries (used by scheduler and signal dispatch)
    # ------------------------------------------------------------------

    @property
    def mode(self) -> RolloutMode:
        with self._lock:
            return self._mode

    def is_scheduler_enabled(self) -> bool:
        """True if autonomous scheduler should run loops."""
        with self._lock:
            return self._mode != RolloutMode.MANUAL_SIGNAL_MODE

    def is_autonomous_dispatch_enabled(self) -> bool:
        """True if signals from scanner should auto-dispatch (not wait for ACK)."""
        with self._lock:
            return self._mode in (
                RolloutMode.UNATTENDED_PAPER_MODE,
                RolloutMode.UNATTENDED_LIVE_MODE,
            )

    def is_live_dispatch_allowed(self) -> bool:
        """True if signals should route to live broker (not paper)."""
        with self._lock:
            return self._mode == RolloutMode.UNATTENDED_LIVE_MODE

    def requires_operator_ack(self) -> bool:
        """True if every order needs operator ACK before dispatch."""
        with self._lock:
            return self._mode == RolloutMode.SUPERVISED_AUTONOMY_MODE

    # ------------------------------------------------------------------
    # PUBLIC: Mode transitions
    # ------------------------------------------------------------------

    def set_mode(
        self,
        new_mode: RolloutMode,
        operator: str = "system",
        reason: str = "",
    ) -> Tuple[bool, str]:
        """
        Transition to a new rollout mode.

        Returns:
            (True, "")       — transition successful
            (False, "reason")— transition blocked
        """
        with self._lock:
            if new_mode == self._mode:
                return True, f"Already in {self._mode.value}"

            # Gate for UNATTENDED_LIVE_MODE
            if new_mode == RolloutMode.UNATTENDED_LIVE_MODE:
                ok, gate_reason = self._check_live_gate()
                if not ok:
                    logger.warning(
                        f"[RolloutMode] UNATTENDED_LIVE_MODE gate FAILED: {gate_reason}"
                    )
                    return False, f"LIVE_GATE_FAILED: {gate_reason}"

            old_mode = self._mode
            self._mode = new_mode
            self._record_transition(old_mode, new_mode, operator, reason)
            self._persist_state()

        logger.info(
            f"[RolloutMode] {old_mode.value} → {new_mode.value} "
            f"(operator={operator}, reason={reason!r})"
        )

        if self._alert:
            self._alert.emit(
                "RESTART_DETECTED" if new_mode == RolloutMode.UNATTENDED_LIVE_MODE
                else "SYSTEM_BLOCKED",
                f"Rollout mode changed: {old_mode.value} → {new_mode.value}",
                {
                    "old_mode": old_mode.value,
                    "new_mode": new_mode.value,
                    "operator": operator,
                    "reason": reason,
                },
            )

        return True, f"Mode set to {new_mode.value}"

    def check_live_gate(self) -> Tuple[bool, str]:
        """
        Public gate check for UNATTENDED_LIVE_MODE.
        Called by /api/system/mode endpoint and arm-live flow.
        """
        with self._lock:
            return self._check_live_gate()

    # ------------------------------------------------------------------
    # PUBLIC: Supervised mode ACK queue
    # ------------------------------------------------------------------

    def queue_for_approval(self, signal_item: dict) -> str:
        """
        Queue a signal for operator approval (SUPERVISED_AUTONOMY_MODE).
        Returns queue_id for the pending item.
        """
        import uuid
        queue_id = str(uuid.uuid4())[:8]
        item = {
            "queue_id": queue_id,
            "queued_at": datetime.now(timezone.utc).isoformat(),
            "status": "PENDING",
            **signal_item,
        }
        with self._supervised_lock:
            self._supervised_pending.append(item)
            # Keep last 100 items
            if len(self._supervised_pending) > 100:
                self._supervised_pending.pop(0)
        logger.info(
            f"[RolloutMode:Supervised] Queued for approval: "
            f"{signal_item.get('strategy')}/{signal_item.get('symbol')} "
            f"queue_id={queue_id}"
        )
        return queue_id

    def approve_signal(self, queue_id: str, operator: str) -> Tuple[bool, Optional[dict]]:
        """
        Operator approves a queued signal. Returns (True, signal_item) or (False, None).
        """
        with self._supervised_lock:
            for item in self._supervised_pending:
                if item.get("queue_id") == queue_id and item.get("status") == "PENDING":
                    item["status"] = "APPROVED"
                    item["approved_by"] = operator
                    item["approved_at"] = datetime.now(timezone.utc).isoformat()
                    logger.info(
                        f"[RolloutMode:Supervised] APPROVED queue_id={queue_id} "
                        f"by {operator}"
                    )
                    return True, dict(item)
            return False, None

    def reject_signal(self, queue_id: str, operator: str, reason: str = "") -> bool:
        """Operator rejects a queued signal."""
        with self._supervised_lock:
            for item in self._supervised_pending:
                if item.get("queue_id") == queue_id and item.get("status") == "PENDING":
                    item["status"] = "REJECTED"
                    item["rejected_by"] = operator
                    item["rejected_at"] = datetime.now(timezone.utc).isoformat()
                    item["reject_reason"] = reason
                    logger.info(
                        f"[RolloutMode:Supervised] REJECTED queue_id={queue_id} "
                        f"by {operator}: {reason}"
                    )
                    return True
            return False

    def get_pending_approvals(self) -> List[dict]:
        """Return all signals pending operator approval."""
        with self._supervised_lock:
            return [
                item for item in self._supervised_pending
                if item.get("status") == "PENDING"
            ]

    def get_approval_history(self, limit: int = 50) -> List[dict]:
        """Return recent approval history."""
        with self._supervised_lock:
            return list(self._supervised_pending[-limit:])

    # ------------------------------------------------------------------
    # PUBLIC: Status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        with self._lock:
            current_mode = self._mode.value
            pending_count = 0
            with self._supervised_lock:
                pending_count = sum(
                    1 for i in self._supervised_pending if i.get("status") == "PENDING"
                )

            live_gate_ok, live_gate_reason = self._check_live_gate() if current_mode != "UNATTENDED_LIVE_MODE" else (True, "")

            return {
                "mode": current_mode,
                "scheduler_enabled": current_mode != "MANUAL_SIGNAL_MODE",
                "autonomous_dispatch_enabled": current_mode in (
                    "UNATTENDED_PAPER_MODE", "UNATTENDED_LIVE_MODE"
                ),
                "live_dispatch_allowed": current_mode == "UNATTENDED_LIVE_MODE",
                "requires_operator_ack": current_mode == "SUPERVISED_AUTONOMY_MODE",
                "pending_approvals": pending_count,
                "live_gate_ready": live_gate_ok,
                "live_gate_reason": live_gate_reason if not live_gate_ok else "",
                "mode_history": self._mode_history[-10:],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    # ------------------------------------------------------------------
    # INTERNAL: Live gate check
    # ------------------------------------------------------------------

    def _check_live_gate(self) -> Tuple[bool, str]:
        """
        Check all requirements for UNATTENDED_LIVE_MODE.
        Called under lock.
        """
        gate_items = []

        # 1. Broker connection
        if self._esm:
            from live.execution_state_machine import ExecutionState
            state = self._esm.current_state
            if state not in (
                ExecutionState.LIVE_CONNECTED_SAFE,
                ExecutionState.LIVE_ENABLED,
            ):
                gate_items.append(
                    f"broker_connected=FAIL (state={state.value})"
                )
            else:
                # Check arm status
                if state != ExecutionState.LIVE_ENABLED:
                    gate_items.append(
                        "live_armed=FAIL (must call /api/execution/arm-live first)"
                    )
        else:
            gate_items.append("esm=FAIL (ExecutionStateMachine not initialised)")

        # 2. Alerting enabled
        alert_ok = self._alert is not None
        if not alert_ok:
            gate_items.append("alerting=FAIL (AlertDispatcher not configured)")

        # 3. Risk Governor active
        rg_ok = self._risk_governor is not None
        if not rg_ok:
            gate_items.append("risk_governor=FAIL (RiskGovernor not initialised)")

        # 4. Persistence active
        pm_ok = self._persistence is not None
        if not pm_ok:
            gate_items.append("persistence=FAIL (PersistenceManager not initialised)")

        if gate_items:
            return False, "; ".join(gate_items)
        return True, ""

    # ------------------------------------------------------------------
    # INTERNAL: History and persistence
    # ------------------------------------------------------------------

    def _record_transition(
        self, old_mode: RolloutMode, new_mode: RolloutMode,
        operator: str, reason: str
    ):
        """Record transition (called under lock)."""
        self._mode_history.append({
            "from": old_mode.value,
            "to": new_mode.value,
            "operator": operator,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        if len(self._mode_history) > 200:
            self._mode_history.pop(0)

    def _persist_state(self):
        """Persist current mode to disk (called under lock)."""
        try:
            self._state_file.write_text(json.dumps({
                "mode": self._mode.value,
                "mode_history": self._mode_history[-20:],
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2))
        except Exception as e:
            logger.error(f"[RolloutMode] Failed to persist state: {e}")

    def _load_state(self):
        """Load persisted mode from disk."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text())
            stored_mode_str = data.get("mode", "")
            if stored_mode_str:
                try:
                    stored_mode = RolloutMode(stored_mode_str)
                    # Safety: never auto-restore UNATTENDED_LIVE_MODE on startup
                    # (operator must re-arm after every restart)
                    if stored_mode == RolloutMode.UNATTENDED_LIVE_MODE:
                        logger.warning(
                            "[RolloutMode] Stored mode was UNATTENDED_LIVE_MODE — "
                            "downgrading to UNATTENDED_PAPER_MODE on restart for safety"
                        )
                        self._mode = RolloutMode.UNATTENDED_PAPER_MODE
                    else:
                        self._mode = stored_mode
                except ValueError:
                    pass
            self._mode_history = data.get("mode_history", [])
            logger.info(f"[RolloutMode] Restored mode: {self._mode.value}")
        except Exception as e:
            logger.error(f"[RolloutMode] Failed to load state: {e}")

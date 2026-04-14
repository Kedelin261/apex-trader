"""
APEX MULTI-MARKET TJR ENGINE
Execution State Machine — Manages the full lifecycle of execution modes.

States:
  PAPER_MODE             — No real broker connection. All orders go to paper engine.
  LIVE_CONNECTED_SAFE    — Real broker authenticated; reading data only. Orders blocked.
  LIVE_ENABLED           — Operator has explicitly armed live trading. Real orders go to broker.
  LIVE_BLOCKED           — Transitional; live arming blocked due to preflight failure or risk event.
  KILL_SWITCH_ENGAGED    — All trading halted. Cannot resume without explicit operator action.

Transitions:
  PAPER → LIVE_CONNECTED_SAFE  : broker.authenticate() succeeds
  LIVE_CONNECTED_SAFE → LIVE_ENABLED   : operator arms via /api/execution/arm-live (2-step)
  LIVE_CONNECTED_SAFE → LIVE_BLOCKED   : preflight fails or risk event
  LIVE_ENABLED → LIVE_CONNECTED_SAFE   : operator disarms via /api/execution/disarm-live
  LIVE_ENABLED → LIVE_BLOCKED          : auto-block on risk event
  ANY → KILL_SWITCH_ENGAGED            : kill switch triggered
  KILL_SWITCH_ENGAGED → PAPER_MODE     : explicit operator reset only

Hard rules:
  - LIVE_ENABLED requires 2 explicit steps: connect then arm
  - Kill switch is irreversible until explicit operator reset
  - All transitions are logged with operator ID, timestamp, and reason
  - State is persisted to disk (JSON) so it survives restarts
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


class ExecutionState(Enum):
    PAPER_MODE = "PAPER_MODE"
    LIVE_CONNECTED_SAFE = "LIVE_CONNECTED_SAFE"
    LIVE_ENABLED = "LIVE_ENABLED"
    LIVE_BLOCKED = "LIVE_BLOCKED"
    KILL_SWITCH_ENGAGED = "KILL_SWITCH_ENGAGED"


@dataclass
class StateTransitionEvent:
    from_state: str
    to_state: str
    reason: str
    operator: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    preflight_passed: Optional[bool] = None
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ExecutionStateSnapshot:
    state: str
    broker: Optional[str]
    environment: Optional[str]
    account_id: Optional[str]
    live_safe_lock: bool            # True = armed-and-ready step 1 only
    live_enabled: bool              # True = live trading fully armed
    require_operator_arming: bool
    kill_switch_active: bool
    preflight_passed: Optional[bool]
    preflight_performed_at: Optional[str]
    last_transition_at: Optional[str]
    last_transition_reason: Optional[str]
    last_operator: Optional[str]
    connection_error: Optional[str]
    arming_operator: Optional[str]
    armed_at: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)

    def is_live_trading_allowed(self) -> bool:
        """Returns True ONLY when real orders may be submitted."""
        return (
            self.state == ExecutionState.LIVE_ENABLED.value
            and self.live_enabled
            and not self.kill_switch_active
        )


class ExecutionStateMachine:
    """
    Thread-safe execution state machine.
    All transitions are logged and persisted.
    """

    _ALLOWED_TRANSITIONS = {
        ExecutionState.PAPER_MODE: [
            ExecutionState.LIVE_CONNECTED_SAFE,
            ExecutionState.KILL_SWITCH_ENGAGED,
        ],
        ExecutionState.LIVE_CONNECTED_SAFE: [
            ExecutionState.PAPER_MODE,
            ExecutionState.LIVE_ENABLED,
            ExecutionState.LIVE_BLOCKED,
            ExecutionState.KILL_SWITCH_ENGAGED,
        ],
        ExecutionState.LIVE_ENABLED: [
            ExecutionState.LIVE_CONNECTED_SAFE,
            ExecutionState.LIVE_BLOCKED,
            ExecutionState.KILL_SWITCH_ENGAGED,
        ],
        ExecutionState.LIVE_BLOCKED: [
            ExecutionState.PAPER_MODE,
            ExecutionState.LIVE_CONNECTED_SAFE,
            ExecutionState.KILL_SWITCH_ENGAGED,
        ],
        ExecutionState.KILL_SWITCH_ENGAGED: [
            ExecutionState.PAPER_MODE,  # Only allowed after explicit reset
        ],
    }

    def __init__(
        self,
        state_file: str = "data/execution_state.json",
        require_operator_arming: bool = True,
    ):
        self._lock = threading.Lock()
        self._state = ExecutionState.PAPER_MODE
        self._broker: Optional[str] = None
        self._environment: Optional[str] = None
        self._account_id: Optional[str] = None
        self._live_safe_lock: bool = False
        self._live_enabled: bool = False
        self._require_operator_arming: bool = require_operator_arming
        self._kill_switch: bool = False
        self._preflight_passed: Optional[bool] = None
        self._preflight_at: Optional[str] = None
        self._last_transition_at: Optional[str] = None
        self._last_transition_reason: Optional[str] = None
        self._last_operator: Optional[str] = None
        self._connection_error: Optional[str] = None
        self._arming_operator: Optional[str] = None
        self._armed_at: Optional[str] = None
        self._transition_log: List[StateTransitionEvent] = []
        self._state_file = Path(state_file)
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_persisted_state()

    # ------------------------------------------------------------------
    # PERSISTENCE
    # ------------------------------------------------------------------

    def _load_persisted_state(self) -> None:
        """Load persisted state on startup (e.g., after restart)."""
        if not self._state_file.exists():
            logger.info("[ESM] No persisted state found; starting in PAPER_MODE")
            return
        try:
            data = json.loads(self._state_file.read_text())
            stored = data.get("state", "PAPER_MODE")
            # Safety: on restart, always downgrade LIVE_ENABLED → LIVE_CONNECTED_SAFE
            # and KILL_SWITCH stays as-is
            if stored == ExecutionState.LIVE_ENABLED.value:
                self._state = ExecutionState.LIVE_CONNECTED_SAFE
                self._live_enabled = False
                logger.warning(
                    "[ESM] Restarted from LIVE_ENABLED → downgraded to LIVE_CONNECTED_SAFE. "
                    "Operator must re-arm to enable live trading."
                )
            elif stored == ExecutionState.KILL_SWITCH_ENGAGED.value:
                self._state = ExecutionState.KILL_SWITCH_ENGAGED
                self._kill_switch = True
                logger.critical("[ESM] Restarted with KILL_SWITCH_ENGAGED — trading blocked.")
            else:
                try:
                    self._state = ExecutionState(stored)
                except ValueError:
                    self._state = ExecutionState.PAPER_MODE

            self._broker = data.get("broker")
            self._environment = data.get("environment")
            self._account_id = data.get("account_id")
            self._live_safe_lock = data.get("live_safe_lock", False)
            self._last_transition_reason = data.get("last_transition_reason")
            self._last_operator = data.get("last_operator")
            logger.info(f"[ESM] Loaded state: {self._state.value}")
        except Exception as e:
            logger.error(f"[ESM] Failed to load persisted state: {e}; defaulting to PAPER_MODE")

    def _persist_state(self) -> None:
        """Persist current state to disk."""
        try:
            data = {
                "state": self._state.value,
                "broker": self._broker,
                "environment": self._environment,
                "account_id": self._account_id,
                "live_safe_lock": self._live_safe_lock,
                "kill_switch": self._kill_switch,
                "last_transition_at": self._last_transition_at,
                "last_transition_reason": self._last_transition_reason,
                "last_operator": self._last_operator,
            }
            self._state_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"[ESM] Failed to persist state: {e}")

    # ------------------------------------------------------------------
    # TRANSITION ENGINE
    # ------------------------------------------------------------------

    def _transition(
        self,
        to_state: ExecutionState,
        reason: str,
        operator: str,
        preflight_passed: Optional[bool] = None,
        extra: dict = None,
    ) -> Tuple[bool, str]:
        """Internal: perform state transition with validation."""
        with self._lock:
            allowed = self._ALLOWED_TRANSITIONS.get(self._state, [])
            if to_state not in allowed:
                msg = (
                    f"Transition {self._state.value} → {to_state.value} not allowed. "
                    f"Allowed from {self._state.value}: {[s.value for s in allowed]}"
                )
                logger.error(f"[ESM] BLOCKED transition: {msg}")
                return False, msg

            event = StateTransitionEvent(
                from_state=self._state.value,
                to_state=to_state.value,
                reason=reason,
                operator=operator,
                preflight_passed=preflight_passed,
                extra=extra or {},
            )
            self._transition_log.append(event)
            if len(self._transition_log) > 1000:
                self._transition_log.pop(0)

            prev = self._state
            self._state = to_state
            self._last_transition_at = event.timestamp
            self._last_transition_reason = reason
            self._last_operator = operator
            if preflight_passed is not None:
                self._preflight_passed = preflight_passed
                self._preflight_at = event.timestamp

            self._persist_state()

            logger.warning(
                f"[ESM] State: {prev.value} → {to_state.value} "
                f"| operator={operator} | reason={reason}"
            )
            return True, f"Transition {prev.value} → {to_state.value} complete"

    # ------------------------------------------------------------------
    # PUBLIC TRANSITION METHODS
    # ------------------------------------------------------------------

    def connect_broker(
        self,
        broker: str,
        environment: str,
        account_id: str,
        operator: str,
        preflight_passed: bool,
        preflight_reason: str = "",
    ) -> Tuple[bool, str]:
        """
        Step 1: Broker authenticated successfully → LIVE_CONNECTED_SAFE.
        Called by /api/broker/connect after successful authenticate().
        """
        with self._lock:
            self._broker = broker
            self._environment = environment
            self._account_id = account_id
            self._live_safe_lock = True   # Connection established; safe reads allowed

        if preflight_passed:
            ok, msg = self._transition(
                ExecutionState.LIVE_CONNECTED_SAFE,
                reason=f"Broker connected OK: {preflight_reason}",
                operator=operator,
                preflight_passed=True,
            )
        else:
            ok, msg = self._transition(
                ExecutionState.LIVE_BLOCKED,
                reason=f"Broker connected but preflight failed: {preflight_reason}",
                operator=operator,
                preflight_passed=False,
            )
        return ok, msg

    def arm_live_trading(self, operator: str, confirmation_code: str = "") -> Tuple[bool, str]:
        """
        Step 2: Operator explicitly arms live trading → LIVE_ENABLED.
        Requires current state = LIVE_CONNECTED_SAFE and preflight passed.
        """
        with self._lock:
            if self._kill_switch:
                return False, "Cannot arm: kill switch is ENGAGED"

            if self._state != ExecutionState.LIVE_CONNECTED_SAFE:
                return False, (
                    f"Cannot arm: current state is {self._state.value}. "
                    "Must be in LIVE_CONNECTED_SAFE first."
                )

            if not self._preflight_passed:
                return False, (
                    "Cannot arm: preflight not passed. "
                    "Run /api/broker/preflight and resolve all blocking issues."
                )

        ok, msg = self._transition(
            ExecutionState.LIVE_ENABLED,
            reason=f"Operator armed live trading. Confirmation: {confirmation_code or 'provided'}",
            operator=operator,
        )
        if ok:
            with self._lock:
                self._live_enabled = True
                self._arming_operator = operator
                self._armed_at = datetime.now(timezone.utc).isoformat()
            logger.critical(
                f"[ESM] ⚡ LIVE TRADING ARMED by operator={operator} at {self._armed_at}"
            )
        return ok, msg

    def disarm_live_trading(self, operator: str, reason: str = "Manual disarm") -> Tuple[bool, str]:
        """Disarm live trading → back to LIVE_CONNECTED_SAFE."""
        with self._lock:
            self._live_enabled = False

        ok, msg = self._transition(
            ExecutionState.LIVE_CONNECTED_SAFE,
            reason=reason,
            operator=operator,
        )
        if ok:
            logger.warning(f"[ESM] Live trading DISARMED by {operator}: {reason}")
        return ok, msg

    def block_live(self, reason: str, operator: str = "system") -> Tuple[bool, str]:
        """Block live trading due to risk event or preflight failure."""
        with self._lock:
            self._live_enabled = False

        return self._transition(
            ExecutionState.LIVE_BLOCKED,
            reason=reason,
            operator=operator,
            preflight_passed=False,
        )

    def engage_kill_switch(self, reason: str, operator: str = "system") -> Tuple[bool, str]:
        """
        Engage kill switch from ANY state.
        Irreversible until explicit reset_to_paper().
        """
        with self._lock:
            self._kill_switch = True
            self._live_enabled = False
            self._live_safe_lock = False
            # Force current state to allow kill switch transition
            self._state = ExecutionState.LIVE_ENABLED  # Temporarily allow transition
            # Actually just force it directly
            self._state = ExecutionState.KILL_SWITCH_ENGAGED
            self._last_transition_at = datetime.now(timezone.utc).isoformat()
            self._last_transition_reason = reason
            self._last_operator = operator
            event = StateTransitionEvent(
                from_state="ANY",
                to_state=ExecutionState.KILL_SWITCH_ENGAGED.value,
                reason=reason,
                operator=operator,
            )
            self._transition_log.append(event)
            self._persist_state()

        logger.critical(f"[ESM] 🔴 KILL SWITCH ENGAGED by {operator}: {reason}")
        return True, f"Kill switch engaged: {reason}"

    def reset_to_paper(self, operator: str, reason: str = "Reset to paper mode") -> Tuple[bool, str]:
        """
        Reset from KILL_SWITCH_ENGAGED or LIVE_BLOCKED back to PAPER_MODE.
        Requires explicit operator action.
        """
        with self._lock:
            self._kill_switch = False
            self._live_enabled = False
            self._live_safe_lock = False
            self._broker = None
            self._environment = None
            self._account_id = None

        ok, msg = self._transition(
            ExecutionState.PAPER_MODE,
            reason=reason,
            operator=operator,
        )
        if ok:
            logger.warning(f"[ESM] System reset to PAPER_MODE by {operator}: {reason}")
        return ok, msg

    def set_preflight_result(self, passed: bool, operator: str = "system") -> None:
        with self._lock:
            self._preflight_passed = passed
            self._preflight_at = datetime.now(timezone.utc).isoformat()
        logger.info(f"[ESM] Preflight result set: passed={passed} by {operator}")

    # ------------------------------------------------------------------
    # STATE QUERIES
    # ------------------------------------------------------------------

    def current_state(self) -> ExecutionState:
        return self._state

    def is_live_trading_allowed(self) -> bool:
        """True ONLY when LIVE_ENABLED and kill switch off."""
        with self._lock:
            return (
                self._state == ExecutionState.LIVE_ENABLED
                and self._live_enabled
                and not self._kill_switch
            )

    def is_connected(self) -> bool:
        """True if we have a live broker connection (LIVE_CONNECTED_SAFE or LIVE_ENABLED)."""
        return self._state in (
            ExecutionState.LIVE_CONNECTED_SAFE,
            ExecutionState.LIVE_ENABLED,
        )

    def is_kill_switch_active(self) -> bool:
        return self._kill_switch

    def snapshot(self) -> ExecutionStateSnapshot:
        with self._lock:
            return ExecutionStateSnapshot(
                state=self._state.value,
                broker=self._broker,
                environment=self._environment,
                account_id=self._account_id,
                live_safe_lock=self._live_safe_lock,
                live_enabled=self._live_enabled,
                require_operator_arming=self._require_operator_arming,
                kill_switch_active=self._kill_switch,
                preflight_passed=self._preflight_passed,
                preflight_performed_at=self._preflight_at,
                last_transition_at=self._last_transition_at,
                last_transition_reason=self._last_transition_reason,
                last_operator=self._last_operator,
                connection_error=self._connection_error,
                arming_operator=self._arming_operator,
                armed_at=self._armed_at,
            )

    def transition_history(self, limit: int = 50) -> List[dict]:
        return [e.to_dict() for e in self._transition_log[-limit:]]

    def set_connection_error(self, error: Optional[str]) -> None:
        with self._lock:
            self._connection_error = error

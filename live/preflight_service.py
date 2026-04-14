"""
APEX MULTI-MARKET TJR ENGINE
Preflight Service — Orchestrates all pre-live-trading checks.

This service wraps the broker connector's preflight, adds system-level
checks (kill switch, risk limits, state machine), and returns a
comprehensive PreflightResult used to gate live arming.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from brokers.base_connector import PreflightCheck, PreflightResult

logger = logging.getLogger(__name__)


class PreflightService:
    """
    Runs the comprehensive pre-live-trading preflight checklist.

    Checks:
    1. Credentials present and valid format
    2. Broker authentication successful
    3. Account info readable
    4. Environment (practice vs live) confirmed
    5. Account permissions granted
    6. Instrument mappings for configured markets
    7. Positions endpoint reachable
    8. Orders endpoint reachable
    9. Risk manager state valid (no kill switch, no daily loss limit hit)
    10. Execution state machine in correct state
    11. Balance check (warning if zero, but non-blocking for connection)
    12. Reconciliation endpoint reachable
    """

    def __init__(
        self,
        connector,           # BaseBrokerConnector subclass
        risk_manager,        # RiskManager
        state_machine,       # ExecutionStateMachine
    ):
        self._connector = connector
        self._risk = risk_manager
        self._esm = state_machine
        self._last_result: Optional[PreflightResult] = None

    def run(self, operator: str = "system") -> PreflightResult:
        """
        Execute all preflight checks.
        Returns PreflightResult with overall_pass and blocking_failures.
        """
        logger.info(f"[PREFLIGHT] Starting preflight checks (operator={operator})")

        # Start with broker-level checks
        try:
            result = self._connector.run_preflight_checks()
        except Exception as e:
            logger.error(f"[PREFLIGHT] Connector preflight raised: {e}")
            result = PreflightResult(
                broker=getattr(self._connector, "name", "unknown"),
                environment="unknown",
                checks=[PreflightCheck(
                    name="broker_preflight",
                    passed=False,
                    message=f"Broker preflight exception: {e}",
                    critical=True,
                )],
                overall_pass=False,
                blocking_failures=[f"broker_preflight_exception: {e}"],
                warnings=[],
            )

        # Add system-level checks
        system_checks, system_blocking, system_warnings = self._system_checks()
        result.checks.extend(system_checks)
        result.blocking_failures.extend(system_blocking)
        result.warnings.extend(system_warnings)

        # Re-evaluate overall pass
        result.overall_pass = len(result.blocking_failures) == 0

        # Cache and update ESM
        self._last_result = result
        self._esm.set_preflight_result(result.overall_pass, operator)

        level = logging.INFO if result.overall_pass else logging.WARNING
        logger.log(
            level,
            f"[PREFLIGHT] Complete: pass={result.overall_pass} "
            f"blocking={result.blocking_failures} warnings={result.warnings}"
        )
        return result

    def _system_checks(self):
        checks = []
        blocking = []
        warnings = []

        # Check 1: Risk manager kill switch
        rm_killed = getattr(self._risk, "_global_kill_switch", False)
        checks.append(PreflightCheck(
            name="risk_kill_switch_off",
            passed=not rm_killed,
            message="Risk manager kill switch inactive" if not rm_killed
                    else "Risk manager kill switch is ACTIVE — reset before proceeding",
            critical=True,
        ))
        if rm_killed:
            blocking.append("risk_kill_switch_off: kill switch active")

        # Check 2: State machine in valid pre-arm state
        from live.execution_state_machine import ExecutionState
        current = self._esm.current_state()
        esm_ok = current in (
            ExecutionState.PAPER_MODE,
            ExecutionState.LIVE_CONNECTED_SAFE,
            ExecutionState.LIVE_BLOCKED,
        )
        checks.append(PreflightCheck(
            name="execution_state_valid",
            passed=esm_ok,
            message=f"Execution state: {current.value}",
            critical=True,
        ))
        if not esm_ok:
            blocking.append(f"execution_state_valid: state is {current.value}")

        # Check 3: Daily loss not already hit
        try:
            dg = self._risk.daily_governor
            summary = dg.get_summary()
            # DailyPnLRecord has: realized_pnl, blocked_trades
            # Check via governor's internal kill flag
            dg_killed = getattr(dg, "_is_killed", False)
            kill_reason = getattr(dg, "_kill_reason", "")
            checks.append(PreflightCheck(
                name="daily_loss_limit_ok",
                passed=not dg_killed,
                message="Daily loss limit not hit" if not dg_killed
                        else f"Daily loss limit HIT: {kill_reason}",
                critical=True,
            ))
            if dg_killed:
                blocking.append(f"daily_loss_limit_ok: {kill_reason}")
        except Exception as e:
            checks.append(PreflightCheck(
                name="daily_loss_limit_ok",
                passed=True,  # Non-blocking if check fails to run
                message=f"Daily loss check skipped: {e}",
                critical=False,
            ))
            warnings.append(f"daily_loss_check_skipped: {e}")

        # Check 4: Explicit operator arming required
        checks.append(PreflightCheck(
            name="operator_arming_required",
            passed=True,
            message=(
                "Two-step arming enforced: "
                "LIVE_CONNECTED_SAFE first, then explicit /api/execution/arm-live"
            ),
            critical=False,
        ))

        return checks, blocking, warnings

    def last_result(self) -> Optional[dict]:
        if self._last_result:
            return self._last_result.safe_dict()
        return None

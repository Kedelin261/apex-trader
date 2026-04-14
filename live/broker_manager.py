"""
APEX MULTI-MARKET TJR ENGINE
Broker Manager — Central singleton managing broker connection lifecycle.

Responsibilities:
  - Load credentials securely (never expose in logs)
  - Instantiate the correct connector (OANDA, paper, etc.)
  - Own the ExecutionStateMachine
  - Own the PreflightService and ReconciliationService
  - Own the BrokerSymbolMapper (hydrated on connect; used for pre-submit validation)
  - Route signals: LIVE_ENABLED → real broker | otherwise → paper
  - Pre-submit tradeability validation via check_symbol() before any order
  - Expose status, account, balances, positions, orders for API
  - Handle order rejections and record history
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from brokers.credential_manager import CredentialManager, get_credential_manager
from brokers.base_connector import (
    AccountInfo, BrokerBalance, BrokerOrder, BrokerPosition,
    InstrumentMapping, OrderRequest, OrderResult, OrderSide, OrderType,
    PermissionsCheck, PreflightResult, ReconciliationResult,
)
from brokers.symbol_mapper import BrokerSymbolMapper, TradeabilityResult
from live.execution_state_machine import ExecutionState, ExecutionStateMachine

logger = logging.getLogger(__name__)


class BrokerManager:
    """
    Central broker management layer.
    Thread-safe; designed to be a singleton within the API server.
    """

    def __init__(self, config: dict, orchestrator=None, risk_manager=None):
        self._config = config
        self._orchestrator = orchestrator
        self._risk_manager = risk_manager

        # Credential manager
        self._cred_mgr = get_credential_manager()
        self._cred_mgr.load_jwt_secret()

        # Execution state machine
        self._esm = ExecutionStateMachine(
            state_file=config.get("data", {}).get(
                "execution_state_file", "data/execution_state.json"
            ),
            require_operator_arming=True,
        )

        # Active connector (None until connect() called)
        self._connector = None
        self._preflight_svc = None
        self._recon_svc = None
        self._broker_name: Optional[str] = None

        # Paper broker fallback
        self._paper_broker = PaperBroker()

        # Symbol mapper (hydrated after broker connect)
        self._symbol_mapper: Optional[BrokerSymbolMapper] = None

        # Rejection history
        self._rejection_history: List[dict] = []

    # ------------------------------------------------------------------
    # CONNECT
    # ------------------------------------------------------------------

    def connect(self, broker: str = "oanda", operator: str = "api") -> Tuple[bool, str]:
        """
        Load credentials, create connector, authenticate, run preflight.
        Transitions ESM to LIVE_CONNECTED_SAFE or LIVE_BLOCKED.
        """
        broker = broker.lower()
        self._broker_name = broker

        if broker == "oanda":
            creds = self._cred_mgr.load_oanda()
            if not creds.is_usable():
                msg = f"OANDA credentials invalid: {creds.validation_errors}"
                logger.error(f"[BrokerManager] {msg}")
                self._esm.set_connection_error(msg)
                return False, msg

            from brokers.oanda_connector import OandaConnector
            self._connector = OandaConnector(creds)

        else:
            return False, f"Unsupported broker: {broker}. Supported: oanda"

        # Authenticate
        auth_ok, auth_msg = self._connector.authenticate()
        if not auth_ok:
            self._esm.set_connection_error(auth_msg)
            return False, f"Authentication failed: {auth_msg}"

        # Run preflight
        preflight = self._run_preflight_internal(operator)

        # Hydrate symbol mapper so pre-submit validation has live capability data
        self._symbol_mapper = BrokerSymbolMapper(broker)
        try:
            self._symbol_mapper.hydrate(self._connector)
            logger.info(
                f"[BrokerManager] Symbol mapper hydrated for broker '{broker}' — "
                f"{len(self._symbol_mapper._cache)} instruments cached"
            )
        except Exception as exc:
            logger.warning(
                f"[BrokerManager] Symbol mapper hydration failed: {exc}. "
                "Falling back to static map on first check()."
            )

        # Build services
        if self._orchestrator and self._risk_manager:
            from live.preflight_service import PreflightService
            from live.reconciliation_service import ReconciliationService
            self._preflight_svc = PreflightService(
                self._connector, self._risk_manager, self._esm
            )
            self._recon_svc = ReconciliationService(
                self._connector, self._orchestrator, self._esm
            )
            self._recon_svc.start_background()

        # Transition ESM
        account_id = getattr(self._connector, "_account_id", "unknown")
        ok, msg = self._esm.connect_broker(
            broker=broker,
            environment=getattr(self._connector._creds, "environment", "practice"),
            account_id=account_id,
            operator=operator,
            preflight_passed=preflight.overall_pass,
            preflight_reason=(
                "; ".join(preflight.blocking_failures)
                if not preflight.overall_pass
                else "all checks passed"
            ),
        )
        return ok, msg

    def _run_preflight_internal(self, operator: str) -> PreflightResult:
        try:
            result = self._connector.run_preflight_checks()
            self._esm.set_preflight_result(result.overall_pass, operator)
            return result
        except Exception as e:
            logger.error(f"[BrokerManager] Preflight exception: {e}")
            from brokers.base_connector import PreflightCheck
            return PreflightResult(
                broker=self._broker_name or "unknown",
                environment="unknown",
                checks=[PreflightCheck(
                    name="preflight",
                    passed=False,
                    message=str(e),
                    critical=True,
                )],
                overall_pass=False,
                blocking_failures=[str(e)],
                warnings=[],
            )

    # ------------------------------------------------------------------
    # ARM / DISARM
    # ------------------------------------------------------------------

    def arm_live(self, operator: str, confirmation_code: str = "") -> Tuple[bool, str]:
        return self._esm.arm_live_trading(operator, confirmation_code)

    def disarm_live(self, operator: str, reason: str = "Manual disarm") -> Tuple[bool, str]:
        return self._esm.disarm_live_trading(operator, reason)

    def set_mode(self, mode: str, operator: str) -> Tuple[bool, str]:
        """Switch execution mode by name (paper only for now)."""
        if mode.lower() == "paper":
            return self._esm.reset_to_paper(operator, reason=f"Mode set to paper by {operator}")
        return False, f"Unsupported mode: {mode}"

    def engage_kill_switch(self, reason: str, operator: str = "system") -> Tuple[bool, str]:
        return self._esm.engage_kill_switch(reason, operator)

    def reset_kill_switch(self, operator: str) -> Tuple[bool, str]:
        return self._esm.reset_to_paper(operator, "Kill switch reset to paper mode")

    # ------------------------------------------------------------------
    # ACCOUNT / MARKET DATA
    # ------------------------------------------------------------------

    def get_account_info(self) -> Optional[AccountInfo]:
        if self._connector and self._esm.is_connected():
            try:
                return self._connector.get_account_info()
            except Exception as e:
                logger.error(f"[BrokerManager] get_account_info: {e}")
        return None

    def get_balances(self) -> List[BrokerBalance]:
        if self._connector and self._esm.is_connected():
            try:
                return self._connector.get_balances()
            except Exception as e:
                logger.error(f"[BrokerManager] get_balances: {e}")
        return []

    def get_permissions(self) -> Optional[PermissionsCheck]:
        if self._connector and self._esm.is_connected():
            try:
                return self._connector.get_permissions()
            except Exception as e:
                logger.error(f"[BrokerManager] get_permissions: {e}")
        return None

    def get_positions(self) -> List[BrokerPosition]:
        if self._connector and self._esm.is_connected():
            try:
                return self._connector.get_positions()
            except Exception as e:
                logger.error(f"[BrokerManager] get_positions: {e}")
        return []

    def get_open_orders(self) -> List[BrokerOrder]:
        if self._connector and self._esm.is_connected():
            try:
                return self._connector.get_open_orders()
            except Exception as e:
                logger.error(f"[BrokerManager] get_open_orders: {e}")
        return []

    def run_preflight(self, operator: str = "api") -> PreflightResult:
        if self._preflight_svc:
            return self._preflight_svc.run(operator)
        if self._connector:
            return self._run_preflight_internal(operator)
        from brokers.base_connector import PreflightCheck
        return PreflightResult(
            broker="none",
            environment="none",
            checks=[PreflightCheck(
                name="broker_connected",
                passed=False,
                message="No broker connected. Call /api/broker/connect first.",
                critical=True,
            )],
            overall_pass=False,
            blocking_failures=["No broker connected"],
            warnings=[],
        )

    def test_order_payload(self, request: OrderRequest) -> Tuple[bool, dict]:
        """Build test order payload without submitting."""
        if self._connector and hasattr(self._connector, "build_test_order_payload"):
            return self._connector.build_test_order_payload(request)
        return False, {"error": "Connector does not support test payload"}

    # ------------------------------------------------------------------
    # ORDER SUBMISSION (with state machine gating)
    # ------------------------------------------------------------------

    def submit_order(self, request: OrderRequest, operator: str = "system") -> OrderResult:
        """
        Route order based on execution state:
        - LIVE_ENABLED → real broker
        - All other states → paper broker
        """
        if self._esm.is_kill_switch_active():
            return OrderResult(
                success=False,
                order_id=None,
                status=__import__("brokers.base_connector", fromlist=["OrderStatus"]).OrderStatus.REJECTED,
                reject_reason="Kill switch ENGAGED — all trading halted",
            )

        if self._esm.is_live_trading_allowed():
            logger.info(f"[BrokerManager] Routing LIVE order: {request.instrument} {request.side.value}")
            result = self._connector.submit_order(request)

            # Record rejections
            if not result.success:
                self._rejection_history.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "instrument": request.instrument,
                    "side": request.side.value,
                    "units": request.units,
                    "reason": result.reject_reason,
                    "insufficient_funds": result.insufficient_funds,
                    "mode": "LIVE",
                })
                if len(self._rejection_history) > 200:
                    self._rejection_history.pop(0)

                # Auto-block on too many failures
                if self._connector and self._connector.check_circuit_breaker():
                    self._esm.block_live(
                        reason="Broker circuit breaker open after consecutive failures",
                        operator="broker_manager",
                    )
            return result

        else:
            # Paper mode
            logger.info(f"[BrokerManager] Routing PAPER order: {request.instrument} {request.side.value}")
            return self._paper_broker.submit_order(request)

    def validate_instrument(self, symbol: str) -> Optional[InstrumentMapping]:
        if self._connector:
            return self._connector.validate_instrument_mapping(symbol)
        return None

    # ------------------------------------------------------------------
    # SYMBOL TRADEABILITY CHECK  (Intent Layer pre-submit gate)
    # ------------------------------------------------------------------

    def check_symbol(self, canonical_symbol: str) -> TradeabilityResult:
        """
        Pre-submit tradeability check for *canonical_symbol*.

        Always call this before building an Order.  If is_tradeable is
        False the caller must emit a SIGNAL_REJECTED / EXECUTION_BLOCKED
        ledger message with rejection_code as the reason — never attempt
        to submit the order.

        Diagnostic logging:
          - INFO for XAUUSD (visibility into gold flow)
          - DEBUG for other symbols
          - WARNING for any non-tradeable result
        """
        connected = self._esm.is_connected()

        if self._symbol_mapper is None:
            # Mapper not yet created (broker never connected)
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
                reason_if_not_tradeable="Broker not connected — no symbol mapper available",
                rejection_code="BROKER_NOT_CONNECTED",
            )
        else:
            result = self._symbol_mapper.check(canonical_symbol, connector_connected=connected)

        # Diagnostic logging
        log_fn = logger.info if canonical_symbol == "XAUUSD" else logger.debug
        log_fn(
            f"[BrokerManager.check_symbol] canonical={canonical_symbol!r} "
            f"broker_sym={result.broker_symbol!r} "
            f"asset_class={result.asset_class!r} "
            f"is_supported={result.is_supported} "
            f"is_tradeable={result.is_tradeable} "
            f"rejection_code={result.rejection_code!r}"
        )
        if not result.is_tradeable:
            logger.warning(
                f"[BrokerManager.check_symbol] REJECTED {canonical_symbol!r}: "
                f"{result.rejection_code} — {result.reason_if_not_tradeable}"
            )
        return result

    def get_capability_cache(self) -> list:
        """Return the full symbol capability cache as a list of dicts."""
        if self._symbol_mapper:
            return self._symbol_mapper.dump_cache()
        return []

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def run_reconciliation(self, operator: str = "api") -> Optional[dict]:
        if self._recon_svc:
            result = self._recon_svc.run_once(operator)
            return result.safe_dict()
        if self._connector and self._esm.is_connected():
            try:
                internal = []
                if self._orchestrator:
                    internal = [
                        {"instrument": p.instrument, "unrealized_pnl": getattr(p, "unrealized_pnl", 0)}
                        for p in self._orchestrator._open_positions
                    ]
                result = self._connector.reconcile_state(internal)
                return result.safe_dict()
            except Exception as e:
                return {"error": str(e)}
        return {"error": "Not connected to broker"}

    def last_reconciliation(self) -> Optional[dict]:
        if self._recon_svc:
            return self._recon_svc.last_result()
        return None

    # ------------------------------------------------------------------
    # STATUS
    # ------------------------------------------------------------------

    def broker_status(self) -> dict:
        snap = self._esm.snapshot()
        conn_status = {}
        if self._connector:
            conn_status = self._connector.connection_status()

        return {
            "execution_state": snap.state,
            "broker": snap.broker or "none",
            "environment": snap.environment or "none",
            "account_id": snap.account_id,
            "live_safe_lock": snap.live_safe_lock,
            "live_enabled": snap.live_enabled,
            "kill_switch_active": snap.kill_switch_active,
            "preflight_passed": snap.preflight_passed,
            "preflight_performed_at": snap.preflight_performed_at,
            "last_transition_at": snap.last_transition_at,
            "last_transition_reason": snap.last_transition_reason,
            "last_operator": snap.last_operator,
            "connection_error": snap.connection_error,
            "arming_operator": snap.arming_operator,
            "armed_at": snap.armed_at,
            "connector_status": conn_status,
            "rejection_count": len(self._rejection_history),
            "last_sync": conn_status.get("last_sync"),
        }

    def execution_state(self) -> dict:
        return self._esm.snapshot().to_dict()

    def rejection_history(self, limit: int = 50) -> List[dict]:
        return list(self._rejection_history[-limit:])

    def transition_history(self, limit: int = 50) -> List[dict]:
        return self._esm.transition_history(limit)

    # ------------------------------------------------------------------
    # BROKER INTERFACE PROXIES (used by ExecutionSupervisorAgent)
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        """Proxy: returns True when a real broker is authenticated."""
        return self._esm.is_connected()

    def place_order(self, instrument: str, direction: str,
                    lots: float, stop_loss: float = None,
                    take_profit: float = None) -> dict:
        """
        Legacy-style proxy kept for backward compatibility.
        ExecutionSupervisorAgent now calls submit_order() directly;
        this method forwards to it for any legacy callers.
        """
        from brokers.base_connector import OrderRequest, OrderSide, OrderType as BrokerOrderType
        side = OrderSide.BUY if direction.upper() in ("BUY", "LONG") else OrderSide.SELL
        req = OrderRequest(
            instrument=instrument,
            side=side,
            units=lots,
            order_type=BrokerOrderType.MARKET,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        result = self.submit_order(req)
        return {
            "order_id": result.order_id,
            "fill_price": result.filled_price,
            "success": result.success,
        }


# ---------------------------------------------------------------------------
# PAPER BROKER (in-memory fill simulation, used when not in LIVE_ENABLED)
# ---------------------------------------------------------------------------

class PaperBroker:
    """Lightweight paper fill simulator."""

    def __init__(self):
        self._orders: List[dict] = []

    def submit_order(self, request: OrderRequest) -> OrderResult:
        from brokers.base_connector import OrderStatus
        import uuid
        order_id = str(uuid.uuid4())[:8]
        self._orders.append({
            "order_id": order_id,
            "instrument": request.instrument,
            "side": request.side.value,
            "units": request.units,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "PAPER",
        })
        return OrderResult(
            success=True,
            order_id=order_id,
            status=OrderStatus.FILLED,
            filled_price=request.price or 0.0,
            filled_units=request.units,
            raw={"paper": True},
        )

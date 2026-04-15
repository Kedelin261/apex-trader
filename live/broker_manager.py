"""
APEX MULTI-MARKET TJR ENGINE
Broker Manager — Central singleton managing broker connection lifecycle.

BROKER ROUTING RULES (NON-NEGOTIABLE):
  EURUSD                               → OANDA exclusively
  XAUUSD, GC (Gold Futures)            → IBKR (primary)
  ES, NQ, YM, CL (Futures)            → IBKR (primary)
  AAPL, MSFT, TSLA, NVDA, etc (Stocks)→ IBKR (primary)
  US30, US500, NAS100, etc (Indices)   → IBKR (primary)
  USOIL, UKOIL, NATGAS (Commodities)  → IBKR (primary)
  Non-EURUSD Forex (GBPUSD, etc.)     → IBKR (primary)
  Paper fallback                       → Paper broker (no live arming)

ARCHITECTURE:
  All orders travel through the Intent Layer:
    StrategySignal → StrategyValidator → RiskGuardian → ExecutionSupervisor → BrokerManager
  BrokerManager routes to the correct connector based on symbol routing rules.
  No strategy or agent submits orders directly.

Responsibilities:
  - Load credentials securely (never expose in logs)
  - Instantiate the correct connector (OANDA, IBKR, paper, etc.)
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


# ---------------------------------------------------------------------------
# BROKER ROUTING RULES  (single source of truth — do not duplicate)
# ---------------------------------------------------------------------------

# Symbols that MUST route to OANDA (never IBKR)
OANDA_ROUTING = {
    "EURUSD",
}

# Symbols that route to IBKR as primary broker
IBKR_ROUTING = {
    # Gold / Precious Metals (via Futures at IBKR)
    "XAUUSD", "GC", "XAGUSD",
    # Equity Futures
    "ES", "NQ", "YM",
    # Commodity Futures
    "CL", "USOIL", "UKOIL", "NATGAS", "COPPER",
    # Stocks
    "AAPL", "MSFT", "TSLA", "NVDA", "SPY", "QQQ", "AMZN", "GOOGL", "META",
    # Indices
    "US30", "US500", "NAS100", "GER40", "UK100",
    # Non-EURUSD Forex
    "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "EURJPY", "GBPJPY",
}


def get_broker_for_symbol(canonical_symbol: str) -> str:
    """
    Deterministic broker routing for a canonical symbol.

    Returns:
        "oanda" — for EURUSD only
        "ibkr"  — for all IBKR_ROUTING symbols
        "paper" — unknown/unrouted symbols (fail safe)

    This function is the single source of truth for broker routing.
    It is called by BrokerManager.submit_order() and check_symbol().
    """
    sym = canonical_symbol.upper().strip()
    if sym in OANDA_ROUTING:
        return "oanda"
    if sym in IBKR_ROUTING:
        return "ibkr"
    # Default: unrouted → paper (safe fallback)
    logger.warning(
        f"[BrokerRouter] Symbol {sym!r} not in any routing table — routing to paper. "
        "Add it to OANDA_ROUTING or IBKR_ROUTING in broker_manager.py."
    )
    return "paper"


def get_route_log(canonical_symbol: str) -> dict:
    """
    Return structured routing decision log for a symbol.
    Used by API endpoints for audit/observability.

    Example output:
        {
            "symbol": "XAUUSD",
            "canonical": "XAUUSD",
            "asset_class": "GOLD",
            "broker_selected": "ibkr",
            "route_reason": "GOLD → IBKR (Futures mapping)"
        }
    """
    from brokers.ibkr_connector import IBKR_CONTRACT_MAP

    sym = canonical_symbol.upper().strip()
    broker = get_broker_for_symbol(sym)

    # Determine asset class from IBKR map or known sets
    asset_class = "UNKNOWN"
    route_reason = f"symbol='{sym}' → broker='{broker}'"

    if sym in OANDA_ROUTING:
        asset_class = "FOREX"
        route_reason = "FOREX → OANDA (exclusively)"
    elif sym in IBKR_ROUTING:
        contract = IBKR_CONTRACT_MAP.get(sym)
        if contract:
            asset_class = contract.asset_class.value
            route_reason = (
                f"{asset_class} → IBKR "
                f"(sec_type={contract.sec_type} exchange={contract.exchange})"
            )
        else:
            route_reason = "IBKR routing table match (no contract map entry)"
    else:
        route_reason = "not in routing table → paper fallback"

    return {
        "symbol": sym,
        "canonical": sym,
        "asset_class": asset_class,
        "broker_selected": broker,
        "route_reason": route_reason,
    }


class BrokerManager:
    """
    Central broker management layer.
    Thread-safe; designed to be a singleton within the API server.

    Multi-broker support (v3.0):
      - self._connector       : primary OANDA connector (EURUSD)
      - self._ibkr_connector  : IBKR connector (XAUUSD, Futures, Stocks, Indices, Commodities)
      - Routing is determined by get_broker_for_symbol()
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

        # OANDA connector (EURUSD) — active after connect(broker="oanda")
        self._connector = None
        self._broker_name: Optional[str] = None

        # IBKR connector (XAUUSD / Futures / Stocks / Indices / Commodities)
        # Active after connect(broker="ibkr") or connect(broker="both")
        self._ibkr_connector = None
        self._ibkr_broker_name: Optional[str] = None

        self._preflight_svc = None
        self._recon_svc = None

        # Paper broker fallback
        self._paper_broker = PaperBroker()

        # Symbol mappers (one per broker; hydrated after connect)
        self._symbol_mapper: Optional[BrokerSymbolMapper] = None       # OANDA
        self._ibkr_symbol_mapper: Optional[BrokerSymbolMapper] = None  # IBKR

        # Rejection history
        self._rejection_history: List[dict] = []

    # ------------------------------------------------------------------
    # CONNECT
    # ------------------------------------------------------------------

    def connect(self, broker: str = "oanda", operator: str = "api") -> Tuple[bool, str]:
        """
        Load credentials, create connector(s), authenticate, run preflight.

        broker options:
          "oanda" — connect OANDA only (EURUSD)
          "ibkr"  — connect IBKR only (XAUUSD, Futures, Stocks, etc.)
          "both"  — connect both brokers (recommended for full multi-asset)
        """
        broker = broker.lower()
        results = []

        if broker in ("oanda", "both"):
            ok, msg = self._connect_oanda(operator)
            results.append(("oanda", ok, msg))

        if broker in ("ibkr", "both"):
            ok, msg = self._connect_ibkr(operator)
            results.append(("ibkr", ok, msg))

        if broker not in ("oanda", "ibkr", "both"):
            return False, f"Unsupported broker: {broker!r}. Supported: oanda | ibkr | both"

        # Consider overall success: at least one broker connected
        any_ok = any(ok for _, ok, _ in results)
        all_msgs = "; ".join(f"{b}:{msg}" for b, _, msg in results)

        if any_ok:
            # Transition ESM to connected state
            primary_connector = self._connector or self._ibkr_connector
            account_id = getattr(primary_connector, "_account_id", "unknown") if primary_connector else "unknown"
            env = "practice"
            if primary_connector:
                env = getattr(getattr(primary_connector, "_creds", None), "environment", "practice")

            primary_broker = "both" if (self._connector and self._ibkr_connector) else (
                "oanda" if self._connector else "ibkr"
            )

            # Run preflight on primary connector
            preflight = self._run_preflight_internal(operator)

            ok_esm, esm_msg = self._esm.connect_broker(
                broker=primary_broker,
                environment=env,
                account_id=account_id,
                operator=operator,
                preflight_passed=preflight.overall_pass,
                preflight_reason=(
                    "; ".join(preflight.blocking_failures)
                    if not preflight.overall_pass
                    else "all checks passed"
                ),
            )

            # Build services
            if self._orchestrator and self._risk_manager:
                primary_conn = self._connector or self._ibkr_connector
                if primary_conn:
                    from live.preflight_service import PreflightService
                    from live.reconciliation_service import ReconciliationService
                    self._preflight_svc = PreflightService(
                        primary_conn, self._risk_manager, self._esm
                    )
                    self._recon_svc = ReconciliationService(
                        primary_conn, self._orchestrator, self._esm
                    )
                    self._recon_svc.start_background()

            return ok_esm, all_msgs
        else:
            msg = f"All broker connections failed: {all_msgs}"
            self._esm.set_connection_error(msg)
            return False, msg

    def _connect_oanda(self, operator: str) -> Tuple[bool, str]:
        """Connect OANDA for EURUSD."""
        creds = self._cred_mgr.load_oanda()
        if not creds.is_usable():
            msg = f"OANDA credentials invalid: {creds.validation_errors}"
            logger.error(f"[BrokerManager] {msg}")
            return False, msg

        from brokers.oanda_connector import OandaConnector
        self._connector = OandaConnector(creds)
        self._broker_name = "oanda"

        auth_ok, auth_msg = self._connector.authenticate()
        if not auth_ok:
            self._connector = None
            return False, f"OANDA auth failed: {auth_msg}"

        # Hydrate OANDA symbol mapper
        self._symbol_mapper = BrokerSymbolMapper("oanda")
        try:
            self._symbol_mapper.hydrate(self._connector)
            logger.info(
                f"[BrokerManager] OANDA symbol mapper hydrated — "
                f"{len(self._symbol_mapper._cache)} instruments cached"
            )
        except Exception as exc:
            logger.warning(f"[BrokerManager] OANDA mapper hydration failed: {exc}")

        logger.info(f"[BrokerManager] OANDA connected: {auth_msg}")
        return True, auth_msg

    def _connect_ibkr(self, operator: str) -> Tuple[bool, str]:
        """Connect IBKR for XAUUSD / Futures / Stocks / Indices / Commodities."""
        # First validate environment variables
        env_check = self._cred_mgr.validate_ibkr_env()
        if not env_check["valid"]:
            msg = (
                f"IBKR environment not configured: "
                f"missing={env_check.get('missing',[])} "
                f"invalid={env_check.get('invalid',[])}"
            )
            logger.error(f"[BrokerManager] {msg}")
            return False, msg

        creds = self._cred_mgr.load_ibkr()
        if creds is None or not creds.is_usable():
            msg = (
                f"IBKR credentials invalid: "
                f"{getattr(creds, 'validation_errors', ['load failed'])}"
            )
            logger.error(f"[BrokerManager] {msg}")
            return False, msg

        # Build connector with creds from env vars
        from brokers.ibkr_connector import IBKRConnector
        self._ibkr_connector = IBKRConnector(creds)
        self._ibkr_broker_name = "ibkr"

        auth_ok, auth_msg = self._ibkr_connector.authenticate()
        if not auth_ok:
            logger.warning(f"[BrokerManager] IBKR auth failed (non-fatal): {auth_msg}")
            # IBKR connection failure is non-fatal — system continues with OANDA + paper
            # Return success=False but keep _ibkr_connector for later retry
            return False, f"IBKR auth failed: {auth_msg}"

        # Hydrate IBKR symbol mapper
        self._ibkr_symbol_mapper = BrokerSymbolMapper("ibkr")
        try:
            self._ibkr_symbol_mapper.hydrate(self._ibkr_connector)
            logger.info(
                f"[BrokerManager] IBKR symbol mapper hydrated — "
                f"{len(self._ibkr_symbol_mapper._cache)} instruments cached"
            )
        except Exception as exc:
            logger.warning(f"[BrokerManager] IBKR mapper hydration failed: {exc}")

        logger.info(f"[BrokerManager] IBKR connected: {auth_msg}")
        return True, auth_msg

    def _run_preflight_internal(self, operator: str) -> PreflightResult:
        primary_connector = self._connector or self._ibkr_connector
        if not primary_connector:
            from brokers.base_connector import PreflightCheck
            return PreflightResult(
                broker="none",
                environment="none",
                checks=[PreflightCheck(
                    name="no_broker",
                    passed=False,
                    message="No broker connected",
                    critical=True,
                )],
                overall_pass=False,
                blocking_failures=["No broker connected"],
                warnings=[],
            )
        try:
            result = primary_connector.run_preflight_checks()
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
        connector = self._connector or self._ibkr_connector
        if connector and self._esm.is_connected():
            try:
                return connector.get_account_info()
            except Exception as e:
                logger.error(f"[BrokerManager] get_account_info: {e}")
        return None

    def get_balances(self) -> List[BrokerBalance]:
        balances = []
        for conn in [self._connector, self._ibkr_connector]:
            if conn and self._esm.is_connected():
                try:
                    balances.extend(conn.get_balances())
                except Exception as e:
                    logger.error(f"[BrokerManager] get_balances: {e}")
        return balances

    def get_permissions(self) -> Optional[PermissionsCheck]:
        connector = self._connector or self._ibkr_connector
        if connector and self._esm.is_connected():
            try:
                return connector.get_permissions()
            except Exception as e:
                logger.error(f"[BrokerManager] get_permissions: {e}")
        return None

    def get_positions(self) -> List[BrokerPosition]:
        positions = []
        for conn in [self._connector, self._ibkr_connector]:
            if conn and self._esm.is_connected():
                try:
                    positions.extend(conn.get_positions())
                except Exception as e:
                    logger.error(f"[BrokerManager] get_positions: {e}")
        return positions

    def get_open_orders(self) -> List[BrokerOrder]:
        orders = []
        for conn in [self._connector, self._ibkr_connector]:
            if conn and self._esm.is_connected():
                try:
                    orders.extend(conn.get_open_orders())
                except Exception as e:
                    logger.error(f"[BrokerManager] get_open_orders: {e}")
        return orders

    def run_preflight(self, operator: str = "api") -> PreflightResult:
        if self._preflight_svc:
            return self._preflight_svc.run(operator)
        primary = self._connector or self._ibkr_connector
        if primary:
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
        connector = self._route_connector(request.instrument)
        if connector and hasattr(connector, "build_test_order_payload"):
            return connector.build_test_order_payload(request)
        return False, {"error": "Connector does not support test payload"}

    # ------------------------------------------------------------------
    # BROKER ROUTING — internal helpers
    # ------------------------------------------------------------------

    def _route_connector(self, canonical_symbol: str):
        """
        Return the correct connector for canonical_symbol.

        ROUTING RULES:
          EURUSD → self._connector (OANDA)
          IBKR_ROUTING → self._ibkr_connector (IBKR)
          Otherwise → None (paper fallback)

        Never raises — returns None on failure.
        """
        target = get_broker_for_symbol(canonical_symbol)
        if target == "oanda":
            return self._connector
        elif target == "ibkr":
            return self._ibkr_connector
        return None

    def _route_symbol_mapper(self, canonical_symbol: str) -> Optional[BrokerSymbolMapper]:
        """Return the correct symbol mapper for canonical_symbol."""
        target = get_broker_for_symbol(canonical_symbol)
        if target == "oanda":
            return self._symbol_mapper
        elif target == "ibkr":
            return self._ibkr_symbol_mapper
        return None

    # ------------------------------------------------------------------
    # ORDER SUBMISSION (with state machine gating + routing)
    # ------------------------------------------------------------------

    def submit_order(self, request: OrderRequest, operator: str = "system") -> OrderResult:
        """
        Route order based on symbol routing rules AND execution state:
          - LIVE_ENABLED + correct connector → real broker (OANDA or IBKR per symbol)
          - All other states → paper broker

        v3.0: EURUSD→OANDA, XAUUSD/Futures/Stocks→IBKR routing enforced here.
        """
        from brokers.base_connector import OrderStatus as _OS

        if self._esm.is_kill_switch_active():
            return OrderResult(
                success=False,
                order_id=None,
                status=_OS.REJECTED,
                reject_reason="Kill switch ENGAGED — all trading halted",
                normalized_rejection_code="KILL_SWITCH_ACTIVE",
            )

        target_broker = get_broker_for_symbol(request.instrument)
        connector = self._route_connector(request.instrument)
        mapper = self._route_symbol_mapper(request.instrument)

        if self._esm.is_live_trading_allowed():
            if connector is None:
                # Broker not connected for this symbol — fall back to paper
                logger.warning(
                    f"[BrokerManager] No live connector for {request.instrument!r} "
                    f"(target={target_broker!r}) — routing to paper"
                )
                result = self._paper_broker.submit_order(request)
                return result

            logger.info(
                f"[BrokerManager] Routing LIVE order: {request.instrument!r} "
                f"→ {target_broker!r} connector | {request.side.value}"
            )
            result = connector.submit_order(request)

            if result.success:
                if mapper:
                    mapper.mark_submit_validated(request.instrument)
                logger.info(
                    f"[BrokerManager] LIVE order FILLED: {request.instrument!r} "
                    f"broker={target_broker!r} order_id={result.order_id} price={result.filled_price}"
                )
            else:
                diag = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "canonical_symbol": request.instrument,
                    "target_broker": target_broker,
                    "broker_symbol": (
                        mapper.to_broker_symbol(request.instrument) if mapper else None
                    ),
                    "units": request.units,
                    "side": request.side.value,
                    "reason": result.reject_reason,
                    "raw_broker_error": result.raw_broker_error,
                    "parsed_error_code": result.parsed_error_code,
                    "normalized_rejection_code": result.normalized_rejection_code,
                    "insufficient_funds": result.insufficient_funds,
                    "payload_snapshot": result.payload_snapshot,
                    "mode": "LIVE",
                }
                self._rejection_history.append(diag)
                if len(self._rejection_history) > 200:
                    self._rejection_history.pop(0)

                logger.warning(
                    f"[BrokerManager] LIVE order REJECTED: {request.instrument!r} "
                    f"broker={target_broker!r} "
                    f"raw_broker_error={result.raw_broker_error!r} "
                    f"normalized={result.normalized_rejection_code!r}"
                )

                if connector and hasattr(connector, "check_circuit_breaker"):
                    if connector.check_circuit_breaker():
                        self._esm.block_live(
                            reason="Broker circuit breaker open after consecutive failures",
                            operator="broker_manager",
                        )
            return result

        else:
            logger.info(
                f"[BrokerManager] Routing PAPER order: {request.instrument!r} "
                f"(target={target_broker!r} not live-armed)"
            )
            result = self._paper_broker.submit_order(request)
            if result.success and mapper:
                mapper.mark_submit_validated(request.instrument)
            return result

    def validate_instrument(self, symbol: str) -> Optional[InstrumentMapping]:
        connector = self._route_connector(symbol)
        if connector:
            return connector.validate_instrument_mapping(symbol)
        return None

    # ------------------------------------------------------------------
    # SYMBOL TRADEABILITY CHECK  (Intent Layer pre-submit gate)
    # ------------------------------------------------------------------

    def check_symbol(self, canonical_symbol: str) -> TradeabilityResult:
        """
        Pre-submit tradeability check for *canonical_symbol*.

        Routes to the correct symbol mapper based on broker routing rules.
        EURUSD → OANDA mapper; XAUUSD/Futures/Stocks → IBKR mapper.

        Always call this before building an Order.  If is_tradeable is
        False the caller must emit a SIGNAL_REJECTED / EXECUTION_BLOCKED
        ledger message with rejection_code as the reason.
        """
        connected = self._esm.is_connected()
        target_broker = get_broker_for_symbol(canonical_symbol)
        mapper = self._route_symbol_mapper(canonical_symbol)

        if mapper is None:
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
                reason_if_not_tradeable=(
                    f"No symbol mapper for broker '{target_broker}' — "
                    f"call connect(broker='{target_broker}') first"
                ),
                rejection_code="BROKER_NOT_CONNECTED",
            )
        else:
            result = mapper.check(canonical_symbol, connector_connected=connected)

        # Diagnostic logging
        log_fn = logger.info if canonical_symbol in {"XAUUSD", "EURUSD"} else logger.debug
        log_fn(
            f"[BrokerManager.check_symbol] canonical={canonical_symbol!r} "
            f"target_broker={target_broker!r} "
            f"broker_sym={result.broker_symbol!r} "
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
        """Return the full symbol capability cache across all brokers."""
        cache = []
        if self._symbol_mapper:
            cache.extend(self._symbol_mapper.dump_cache())
        if self._ibkr_symbol_mapper:
            cache.extend(self._ibkr_symbol_mapper.dump_cache())
        return cache

    def get_xau_diagnostics(self, request: Optional[OrderRequest] = None) -> dict:
        """
        Return full XAUUSD diagnostic snapshot.
        XAUUSD routes to IBKR in v3.0 (not OANDA).
        """
        from brokers.symbol_mapper import TradeabilityResult
        xau_cache = None
        if self._ibkr_symbol_mapper:
            result = self._ibkr_symbol_mapper.check(
                "XAUUSD", connector_connected=self._esm.is_connected()
            )
            xau_cache = result.to_dict()
        elif self._symbol_mapper:
            result = self._symbol_mapper.check(
                "XAUUSD", connector_connected=self._esm.is_connected()
            )
            xau_cache = result.to_dict()

        xau_rejections = [
            r for r in self._rejection_history
            if r.get("canonical_symbol") in ("XAUUSD", "XAU_USD", "GC")
        ][-20:]

        payload_probe = None
        if request and self._ibkr_connector and hasattr(self._ibkr_connector, "validate_xau_payload"):
            try:
                payload_probe = self._ibkr_connector.validate_xau_payload(request)
            except Exception as e:
                payload_probe = {"error": str(e)}
        elif request and self._connector and hasattr(self._connector, "validate_xau_payload"):
            try:
                payload_probe = self._connector.validate_xau_payload(request)
            except Exception as e:
                payload_probe = {"error": str(e)}

        connector_rejections = []
        for conn_name, conn in [("ibkr", self._ibkr_connector), ("oanda", self._connector)]:
            if conn and hasattr(conn, "get_rejection_history"):
                all_rej = conn.get_rejection_history()
                filtered = [
                    r for r in all_rej
                    if r.get("canonical_symbol") in ("XAUUSD", "XAU_USD", "GC")
                ][-20:]
                for r in filtered:
                    r["_source_broker"] = conn_name
                connector_rejections.extend(filtered)

        return {
            "canonical_symbol": "XAUUSD",
            "routed_to_broker": "ibkr",
            "ibkr_broker_symbol": "GC",
            "capability_cache": xau_cache,
            "broker_rejection_history": connector_rejections,
            "manager_rejection_history": xau_rejections,
            "payload_probe": payload_probe,
            "broker_connected": self._esm.is_connected(),
            "ibkr_connected": self._ibkr_connector is not None and (
                self._ibkr_connector.is_connected() if hasattr(self._ibkr_connector, "is_connected") else False
            ),
            "live_trading_allowed": self._esm.is_live_trading_allowed(),
        }

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def run_reconciliation(self, operator: str = "api") -> Optional[dict]:
        if self._recon_svc:
            result = self._recon_svc.run_once(operator)
            return result.safe_dict()
        primary = self._connector or self._ibkr_connector
        if primary and self._esm.is_connected():
            try:
                internal = []
                if self._orchestrator:
                    internal = [
                        {"instrument": p.instrument, "unrealized_pnl": getattr(p, "unrealized_pnl", 0)}
                        for p in self._orchestrator._open_positions
                    ]
                result = primary.reconcile_state(internal)
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
        oanda_status = {}
        ibkr_status = {}
        if self._connector:
            oanda_status = self._connector.connection_status()
        if self._ibkr_connector:
            ibkr_status = self._ibkr_connector.connection_status()

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
            "connectors": {
                "oanda": {
                    "connected": self._connector is not None,
                    "status": oanda_status,
                    "routes": list(OANDA_ROUTING),
                },
                "ibkr": {
                    "connected": self._ibkr_connector is not None,
                    "status": ibkr_status,
                    "routes": sorted(IBKR_ROUTING),
                },
            },
            "rejection_count": len(self._rejection_history),
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
        return self._esm.is_connected()

    def place_order(self, instrument: str, direction: str,
                    lots: float, stop_loss: float = None,
                    take_profit: float = None) -> dict:
        """
        Legacy proxy for ExecutionSupervisorAgent backward compatibility.
        Routes to submit_order() which applies proper broker routing.
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

    # ------------------------------------------------------------------
    # IBKR-SPECIFIC HELPERS
    # ------------------------------------------------------------------

    def ibkr_health(self) -> dict:
        """
        Return IBKR-specific health status.

        GET /api/broker/ibkr/health
        """
        import socket as _socket
        import time as _time

        conn = self._ibkr_connector
        if conn is None:
            return {
                "connected": False,
                "state": "NOT_INITIALISED",
                "account_id": None,
                "latency_ms": None,
                "last_heartbeat": None,
                "error": "IBKR connector not initialised. Call POST /api/broker/connect with broker='ibkr'.",
            }

        # Connection state
        state = getattr(conn, "_conn_state", None)
        state_val = state.value if state else "UNKNOWN"
        connected = conn.is_connected() if hasattr(conn, "is_connected") else False
        account_id = getattr(conn, "_account_id", None)
        host = getattr(conn, "_host", "127.0.0.1")
        port = getattr(conn, "_port", 7497)
        env  = getattr(conn, "_environment", "practice")
        circuit_open = getattr(conn, "_circuit_open", False)
        consec_fail  = getattr(conn, "_consecutive_failures", 0)

        # Measure socket latency
        latency_ms = None
        if connected:
            try:
                t0 = _time.monotonic()
                with _socket.create_connection((host, port), timeout=2):
                    pass
                latency_ms = round((_time.monotonic() - t0) * 1000, 2)
            except Exception:
                latency_ms = None

        # Last heartbeat from EClient wrapper (if available)
        ib_app = getattr(conn, "_ib_app", None)
        last_heartbeat = None
        if ib_app and hasattr(ib_app, "last_heartbeat_at"):
            ts = ib_app.last_heartbeat_at
            last_heartbeat = ts.isoformat() if ts else None

        error_state = None
        if not connected:
            error_state = (
                "circuit_breaker_open" if circuit_open
                else "not_connected"
            )

        return {
            "connected": connected,
            "state": state_val,
            "account_id": account_id,
            "environment": env,
            "host": host,
            "port": port,
            "client_id": getattr(conn, "_client_id", 1),
            "latency_ms": latency_ms,
            "last_heartbeat": last_heartbeat,
            "circuit_breaker_open": circuit_open,
            "consecutive_failures": consec_fail,
            "error": error_state,
        }

    def validate_ibkr_env(self) -> dict:
        """Validate IBKR environment variables are present and valid."""
        return self._cred_mgr.validate_ibkr_env()

    def get_ibkr_contract(self, symbol: str) -> Optional[dict]:
        """Return IBKR contract spec for a canonical symbol."""
        from brokers.ibkr_connector import build_ibkr_contract
        try:
            contract = build_ibkr_contract(symbol)
            return contract.to_dict()
        except ValueError as e:
            return {"error": str(e)}

    def get_routing_table(self) -> dict:
        """Return the current broker routing table."""
        return {
            "oanda": sorted(OANDA_ROUTING),
            "ibkr": sorted(IBKR_ROUTING),
            "rule": (
                "EURUSD exclusively routes to OANDA. "
                "XAUUSD, Futures, Stocks, Indices, Commodities route to IBKR. "
                "Unknown symbols fall back to paper."
            ),
        }


# ---------------------------------------------------------------------------
# CREDENTIAL HELPER FOR IBKR
# ---------------------------------------------------------------------------

def _build_ibkr_creds_from_env():
    """
    Build IBKR credentials from environment variables.
    Used when CredentialManager doesn't have load_ibkr() yet.

    Expected env vars:
      APEX_IBKR_ACCOUNT_ID   — IBKR account ID (e.g. U1234567)
      APEX_IBKR_ENVIRONMENT  — practice | live
      APEX_IBKR_HOST         — TWS host (default 127.0.0.1)
      APEX_IBKR_PORT         — TWS port (default 7497 paper, 7496 live)
      APEX_IBKR_CLIENT_ID    — EClient client ID (default 1)
    """
    from dataclasses import dataclass

    @dataclass
    class _IBKRCreds:
        account_id: str
        environment: str
        host: str
        port: int
        client_id: int

        def is_usable(self):
            return bool(self.account_id)

    account_id  = os.getenv("APEX_IBKR_ACCOUNT_ID", "")
    environment = os.getenv("APEX_IBKR_ENVIRONMENT", "practice")
    host        = os.getenv("APEX_IBKR_HOST", "127.0.0.1")
    port_default = "7497" if environment != "live" else "7496"
    port        = int(os.getenv("APEX_IBKR_PORT", port_default))
    client_id   = int(os.getenv("APEX_IBKR_CLIENT_ID", "1"))

    if not account_id:
        logger.warning("[BrokerManager] APEX_IBKR_ACCOUNT_ID not set — IBKR unavailable")
        return None

    return _IBKRCreds(
        account_id=account_id,
        environment=environment,
        host=host,
        port=port,
        client_id=client_id,
    )


# ---------------------------------------------------------------------------
# PAPER BROKER (in-memory fill simulation)
# ---------------------------------------------------------------------------

class PaperBroker:
    """Lightweight paper fill simulator — used when not in LIVE_ENABLED state."""

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

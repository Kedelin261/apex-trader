"""
APEX MULTI-MARKET TJR ENGINE
Base Broker Connector — Abstract interface all broker connectors must implement.

Rules:
- All methods must be implemented; no silent stubs
- Fail-closed: unimplemented methods raise NotImplementedError
- Credentials are never stored in plain logs
- is_live_armed must be explicitly set by operator (not by the connector)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DOMAIN TYPES
# ---------------------------------------------------------------------------

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


class ConnectionState(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    AUTHENTICATED = "AUTHENTICATED"
    ERROR = "ERROR"


@dataclass
class AccountInfo:
    account_id: str
    broker: str
    environment: str          # practice | live
    currency: str
    balance: float
    nav: float                # Net Asset Value
    unrealized_pnl: float
    realized_pnl: float
    margin_used: float
    margin_available: float
    open_trade_count: int
    open_position_count: int
    pending_order_count: int
    raw: Dict[str, Any] = field(default_factory=dict)  # Raw broker response
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def safe_dict(self) -> dict:
        return {
            "account_id": self.account_id,
            "broker": self.broker,
            "environment": self.environment,
            "currency": self.currency,
            "balance": round(self.balance, 2),
            "nav": round(self.nav, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "margin_used": round(self.margin_used, 2),
            "margin_available": round(self.margin_available, 2),
            "open_trade_count": self.open_trade_count,
            "open_position_count": self.open_position_count,
            "pending_order_count": self.pending_order_count,
            "fetched_at": self.fetched_at.isoformat(),
        }


@dataclass
class BrokerBalance:
    currency: str
    balance: float
    available: float
    used: float
    unrealized_pnl: float = 0.0

    def safe_dict(self) -> dict:
        return {
            "currency": self.currency,
            "balance": round(self.balance, 2),
            "available": round(self.available, 2),
            "used": round(self.used, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
        }


@dataclass
class BrokerPosition:
    instrument: str
    side: str          # "LONG" | "SHORT"
    units: float       # Positive = long, negative = short (in broker terms)
    average_price: float
    unrealized_pnl: float
    margin_used: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)

    def safe_dict(self) -> dict:
        return {
            "instrument": self.instrument,
            "side": self.side,
            "units": self.units,
            "average_price": round(self.average_price, 5),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "margin_used": round(self.margin_used, 2),
        }


@dataclass
class BrokerOrder:
    order_id: str
    instrument: str
    side: OrderSide
    order_type: OrderType
    units: float
    price: Optional[float]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    status: OrderStatus
    filled_price: Optional[float] = None
    filled_units: float = 0.0
    reject_reason: Optional[str] = None
    created_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def safe_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "instrument": self.instrument,
            "side": self.side.value,
            "type": self.order_type.value,
            "units": self.units,
            "price": self.price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "status": self.status.value,
            "filled_price": self.filled_price,
            "filled_units": self.filled_units,
            "reject_reason": self.reject_reason,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
        }


@dataclass
class PermissionsCheck:
    can_trade: bool
    can_view_account: bool
    can_view_positions: bool
    can_manage_orders: bool
    restricted_instruments: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class InstrumentMapping:
    """Maps internal symbol to broker-specific symbol."""
    internal: str        # e.g. "XAUUSD"
    broker_symbol: str   # e.g. "XAU_USD" (OANDA format)
    tradeable: bool
    min_units: float
    max_units: float
    precision: int       # Decimal places for price
    margin_rate: float
    spread_typical: float


@dataclass
class OrderRequest:
    """Standardised order request sent to connector."""
    instrument: str        # Internal symbol e.g. "XAUUSD"
    side: OrderSide
    units: float           # Positive units
    order_type: OrderType = OrderType.MARKET
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    client_order_id: Optional[str] = None
    comment: Optional[str] = None
    time_in_force: str = "GTC"

    def safe_dict(self) -> dict:
        return {
            "instrument": self.instrument,
            "side": self.side.value,
            "units": self.units,
            "type": self.order_type.value,
            "price": self.price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "client_order_id": self.client_order_id,
            "comment": self.comment,
            "time_in_force": self.time_in_force,
        }


@dataclass
class OrderResult:
    """Result of an order submission attempt."""
    success: bool
    order_id: Optional[str]
    status: OrderStatus
    filled_price: Optional[float] = None
    filled_units: float = 0.0
    reject_reason: Optional[str] = None
    insufficient_funds: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def safe_dict(self) -> dict:
        return {
            "success": self.success,
            "order_id": self.order_id,
            "status": self.status.value,
            "filled_price": self.filled_price,
            "filled_units": self.filled_units,
            "reject_reason": self.reject_reason,
            "insufficient_funds": self.insufficient_funds,
            "submitted_at": self.submitted_at.isoformat(),
        }


@dataclass
class ReconciliationResult:
    """Result of comparing internal vs broker state."""
    matched_positions: int
    mismatched_positions: List[dict]
    internal_only_positions: List[str]
    broker_only_positions: List[str]
    net_pnl_discrepancy_usd: float
    reconciled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_clean: bool = False
    notes: List[str] = field(default_factory=list)

    def safe_dict(self) -> dict:
        return {
            "matched_positions": self.matched_positions,
            "mismatched_positions": self.mismatched_positions,
            "internal_only_positions": self.internal_only_positions,
            "broker_only_positions": self.broker_only_positions,
            "net_pnl_discrepancy_usd": round(self.net_pnl_discrepancy_usd, 2),
            "reconciled_at": self.reconciled_at.isoformat(),
            "is_clean": self.is_clean,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# ABSTRACT CONNECTOR
# ---------------------------------------------------------------------------

class BaseBrokerConnector(ABC):
    """
    Abstract base class for all broker connectors.
    Subclasses must implement every method without silent stubs.
    """

    def __init__(self, name: str):
        self.name = name
        self._connection_state = ConnectionState.DISCONNECTED
        self._last_error: Optional[str] = None
        self._connected_at: Optional[datetime] = None
        self._consecutive_failures: int = 0
        self._circuit_breaker_open: bool = False
        self._circuit_breaker_threshold: int = 5

    # ------------------------------------------------------------------
    # CORE CONNECTION METHODS (must implement)
    # ------------------------------------------------------------------

    @abstractmethod
    def authenticate(self) -> Tuple[bool, str]:
        """
        Authenticate with broker. Returns (success, message).
        Must not raise; catch all exceptions internally.
        Sets self._connection_state to AUTHENTICATED on success.
        """
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if currently authenticated and connected."""
        ...

    @abstractmethod
    def get_account_info(self) -> AccountInfo:
        """Return full account info. Raise ConnectionError if not connected."""
        ...

    @abstractmethod
    def get_balances(self) -> List[BrokerBalance]:
        """Return list of currency balances."""
        ...

    @abstractmethod
    def get_permissions(self) -> PermissionsCheck:
        """Return what operations are permitted on this account."""
        ...

    @abstractmethod
    def get_positions(self) -> List[BrokerPosition]:
        """Return all open positions."""
        ...

    @abstractmethod
    def get_open_orders(self) -> List[BrokerOrder]:
        """Return all pending/open orders."""
        ...

    @abstractmethod
    def validate_instrument_mapping(self, internal_symbol: str) -> Optional[InstrumentMapping]:
        """
        Validate that an internal symbol maps to a tradeable instrument.
        Return InstrumentMapping if valid, None if not found/tradeable.
        """
        ...

    @abstractmethod
    def submit_order(self, request: OrderRequest) -> OrderResult:
        """
        Submit an order to the broker.
        Must handle insufficient funds, rejections, connectivity errors.
        Must record to order rejection history on failure.
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        """Cancel a pending order. Returns (success, message)."""
        ...

    @abstractmethod
    def get_order_status(self, order_id: str) -> BrokerOrder:
        """Fetch current status of an order by ID."""
        ...

    @abstractmethod
    def reconcile_state(self, internal_positions: List[dict]) -> ReconciliationResult:
        """
        Compare internal position state to broker positions.
        Returns reconciliation report. Does not modify state.
        """
        ...

    @abstractmethod
    def run_preflight_checks(self) -> "PreflightResult":
        """Run full preflight checklist. Returns PreflightResult."""
        ...

    # ------------------------------------------------------------------
    # CIRCUIT BREAKER
    # ------------------------------------------------------------------

    def _record_failure(self, context: str) -> None:
        self._consecutive_failures += 1
        self._last_error = context
        if self._consecutive_failures >= self._circuit_breaker_threshold:
            self._circuit_breaker_open = True
            logger.error(
                f"[{self.name}] Circuit breaker OPEN after "
                f"{self._consecutive_failures} consecutive failures. "
                f"Last error: {context}"
            )

    def _record_success(self) -> None:
        self._consecutive_failures = 0
        self._circuit_breaker_open = False

    def check_circuit_breaker(self) -> bool:
        """Return True if circuit breaker is OPEN (block operations)."""
        return self._circuit_breaker_open

    def reset_circuit_breaker(self) -> None:
        """Manually reset circuit breaker after operator review."""
        self._circuit_breaker_open = False
        self._consecutive_failures = 0
        logger.warning(f"[{self.name}] Circuit breaker manually reset")

    # ------------------------------------------------------------------
    # STATE SUMMARY
    # ------------------------------------------------------------------

    def connection_status(self) -> dict:
        return {
            "broker": self.name,
            "state": self._connection_state.value,
            "connected_at": self._connected_at.isoformat() if self._connected_at else None,
            "last_error": self._last_error,
            "consecutive_failures": self._consecutive_failures,
            "circuit_breaker_open": self._circuit_breaker_open,
        }


# ---------------------------------------------------------------------------
# PREFLIGHT RESULT
# ---------------------------------------------------------------------------

@dataclass
class PreflightCheck:
    name: str
    passed: bool
    message: str
    critical: bool = True  # If critical and failed, block live trading

    def safe_dict(self) -> dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "critical": self.critical,
        }


@dataclass
class PreflightResult:
    """Full preflight checklist result."""
    broker: str
    environment: str
    checks: List[PreflightCheck]
    overall_pass: bool
    blocking_failures: List[str]
    warnings: List[str]
    performed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def safe_dict(self) -> dict:
        return {
            "broker": self.broker,
            "environment": self.environment,
            "overall_pass": self.overall_pass,
            "blocking_failures": self.blocking_failures,
            "warnings": self.warnings,
            "checks": [c.safe_dict() for c in self.checks],
            "performed_at": self.performed_at.isoformat(),
        }

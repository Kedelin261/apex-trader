"""
APEX MULTI-MARKET TJR ENGINE
IBKR (Interactive Brokers) Connector — TWS/IB Gateway REST & EClient integration.

EXACT ARCHITECTURE RULES (NON-NEGOTIABLE):
  - EURUSD → OANDA (not IBKR)
  - XAUUSD, Futures (GC, ES, NQ, CL, YM), Stocks, Indices, Commodities → IBKR
  - All orders route through the Intent Layer (StrategyValidator → RiskGuardian → ExecutionSupervisor)
  - No direct order submission from strategies
  - Broker routing is enforced by BrokerManager, not by individual connectors

SUPPORTED ASSET CLASSES (build_ibkr_contract):
  - FOREX       : EUR, GBP, AUD, CAD, JPY pairs (NOT EURUSD — that goes to OANDA)
  - FUTURES     : GC (Gold), ES (S&P 500 E-mini), NQ (Nasdaq E-mini), CL (Crude Oil), YM (Dow E-mini)
  - STOCKS      : AAPL, MSFT, TSLA, NVDA, SPY, QQQ, etc.
  - INDICES     : US30 (DJI), US500 (SPX), NAS100 (NDX), GER40, UK100
  - COMMODITIES : USOIL, UKOIL, NATGAS, COPPER

IBKR API modes supported:
  1. ibapi (EClient/EWrapper socket) — production path, connects to TWS or IB Gateway
  2. REST via IBeam/IBKR Client Portal Gateway — alternative REST path

Connection requirements:
  - TWS or IB Gateway must be running and accepting API connections
  - Socket: host (default 127.0.0.1), port (default 7497 paper / 7496 live)
  - Client ID: unique per session (default 1)
"""

from __future__ import annotations

import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from brokers.base_connector import (
    AccountInfo,
    BaseBrokerConnector,
    BrokerBalance,
    BrokerOrder,
    BrokerPosition,
    ConnectionState,
    InstrumentMapping,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    PermissionsCheck,
    PreflightCheck,
    PreflightResult,
    ReconciliationResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# IBKR ASSET CLASS ENUM
# ---------------------------------------------------------------------------

class IBKRAssetClass(str, Enum):
    FOREX       = "FOREX"
    FUTURES     = "FUTURES"
    STOCKS      = "STOCKS"      # STK in IBKR
    INDICES     = "INDICES"     # IND in IBKR
    COMMODITIES = "COMMODITIES" # CMDTY or CONTFUT
    CRYPTO      = "CRYPTO"      # CRYPTO in IBKR


# ---------------------------------------------------------------------------
# IBKR CONTRACT DEFINITION
# ---------------------------------------------------------------------------

@dataclass
class IBKRContract:
    """
    Represents a fully-specified IBKR contract.
    Maps directly to the IBKR EClient Contract object fields.
    """
    canonical_symbol: str        # Internal: XAUUSD, ES, AAPL
    ibkr_symbol: str             # IBKR native: GC, ES, AAPL, EUR
    sec_type: str                # STK | FUT | FOREX | IND | CMDTY | CRYPTO
    exchange: str                # SMART | GLOBEX | NYMEX | NYSE | NASDAQ | IDEALPRO
    currency: str                # USD | EUR | GBP | ...
    asset_class: IBKRAssetClass
    expiry: Optional[str] = None         # Futures: YYYYMM (e.g. 202412)
    multiplier: Optional[str] = None     # Futures contract multiplier
    primary_exchange: Optional[str] = None  # For stocks: NYSE, NASDAQ
    min_tick: float = 0.01
    lot_size: float = 1.0                # 1 for stocks/futures, varies
    # Gold specific: 1 GC contract = 100 troy oz
    # ES: 1 contract = $50 × index value
    # NQ: 1 contract = $20 × index value

    def to_dict(self) -> dict:
        return {
            "canonical_symbol": self.canonical_symbol,
            "ibkr_symbol": self.ibkr_symbol,
            "sec_type": self.sec_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "asset_class": self.asset_class.value,
            "expiry": self.expiry,
            "multiplier": self.multiplier,
            "primary_exchange": self.primary_exchange,
            "min_tick": self.min_tick,
            "lot_size": self.lot_size,
        }


# ---------------------------------------------------------------------------
# IBKR SYMBOL → CONTRACT MAP  (canonical → IBKRContract)
# ---------------------------------------------------------------------------

# Futures front-month expiry (rotated quarterly by scheduler; static here for now)
_FUTURES_EXPIRY = "202506"  # June 2025 — update via config or auto-roll logic

# EXACT contract definitions per asset class
IBKR_CONTRACT_MAP: Dict[str, IBKRContract] = {
    # ── FUTURES ────────────────────────────────────────────────────────────
    # GC (Gold Futures): 1 contract = 100 troy oz, CME/COMEX
    "GC": IBKRContract(
        canonical_symbol="GC",
        ibkr_symbol="GC",
        sec_type="FUT",
        exchange="COMEX",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="100",
        min_tick=0.10,
        lot_size=1.0,
    ),
    # XAUUSD maps to GC futures at IBKR (not OANDA CFD)
    "XAUUSD": IBKRContract(
        canonical_symbol="XAUUSD",
        ibkr_symbol="GC",
        sec_type="FUT",
        exchange="COMEX",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="100",
        min_tick=0.10,
        lot_size=1.0,
    ),
    # ES (E-mini S&P 500): 1 contract = $50 × index, CME
    "ES": IBKRContract(
        canonical_symbol="ES",
        ibkr_symbol="ES",
        sec_type="FUT",
        exchange="CME",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="50",
        min_tick=0.25,
        lot_size=1.0,
    ),
    # NQ (E-mini Nasdaq-100): 1 contract = $20 × index, CME
    "NQ": IBKRContract(
        canonical_symbol="NQ",
        ibkr_symbol="NQ",
        sec_type="FUT",
        exchange="CME",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="20",
        min_tick=0.25,
        lot_size=1.0,
    ),
    # YM (E-mini Dow Jones): 1 contract = $5 × index, CBOT
    "YM": IBKRContract(
        canonical_symbol="YM",
        ibkr_symbol="YM",
        sec_type="FUT",
        exchange="CBOT",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="5",
        min_tick=1.0,
        lot_size=1.0,
    ),
    # CL (Crude Oil Futures): 1 contract = 1,000 barrels, NYMEX
    "CL": IBKRContract(
        canonical_symbol="CL",
        ibkr_symbol="CL",
        sec_type="FUT",
        exchange="NYMEX",
        currency="USD",
        asset_class=IBKRAssetClass.FUTURES,
        expiry=_FUTURES_EXPIRY,
        multiplier="1000",
        min_tick=0.01,
        lot_size=1.0,
    ),

    # ── FOREX (non-EURUSD) ─────────────────────────────────────────────────
    # NOTE: EURUSD is explicitly excluded — it routes to OANDA
    "GBPUSD": IBKRContract(
        canonical_symbol="GBPUSD",
        ibkr_symbol="GBP",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="USD",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.00005,
        lot_size=25000.0,  # IBKR min lot for IDEALPRO
    ),
    "USDJPY": IBKRContract(
        canonical_symbol="USDJPY",
        ibkr_symbol="USD",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="JPY",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.005,
        lot_size=25000.0,
    ),
    "AUDUSD": IBKRContract(
        canonical_symbol="AUDUSD",
        ibkr_symbol="AUD",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="USD",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.00005,
        lot_size=25000.0,
    ),
    "USDCAD": IBKRContract(
        canonical_symbol="USDCAD",
        ibkr_symbol="USD",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="CAD",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.00005,
        lot_size=25000.0,
    ),
    "USDCHF": IBKRContract(
        canonical_symbol="USDCHF",
        ibkr_symbol="USD",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="CHF",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.00005,
        lot_size=25000.0,
    ),
    "NZDUSD": IBKRContract(
        canonical_symbol="NZDUSD",
        ibkr_symbol="NZD",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="USD",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.00005,
        lot_size=25000.0,
    ),
    "EURJPY": IBKRContract(
        canonical_symbol="EURJPY",
        ibkr_symbol="EUR",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="JPY",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.005,
        lot_size=25000.0,
    ),
    "GBPJPY": IBKRContract(
        canonical_symbol="GBPJPY",
        ibkr_symbol="GBP",
        sec_type="CASH",
        exchange="IDEALPRO",
        currency="JPY",
        asset_class=IBKRAssetClass.FOREX,
        min_tick=0.005,
        lot_size=25000.0,
    ),

    # ── STOCKS ──────────────────────────────────────────────────────────────
    "AAPL": IBKRContract(
        canonical_symbol="AAPL",
        ibkr_symbol="AAPL",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "MSFT": IBKRContract(
        canonical_symbol="MSFT",
        ibkr_symbol="MSFT",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "TSLA": IBKRContract(
        canonical_symbol="TSLA",
        ibkr_symbol="TSLA",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "NVDA": IBKRContract(
        canonical_symbol="NVDA",
        ibkr_symbol="NVDA",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "SPY": IBKRContract(
        canonical_symbol="SPY",
        ibkr_symbol="SPY",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NYSE",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "QQQ": IBKRContract(
        canonical_symbol="QQQ",
        ibkr_symbol="QQQ",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "AMZN": IBKRContract(
        canonical_symbol="AMZN",
        ibkr_symbol="AMZN",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "GOOGL": IBKRContract(
        canonical_symbol="GOOGL",
        ibkr_symbol="GOOGL",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "META": IBKRContract(
        canonical_symbol="META",
        ibkr_symbol="META",
        sec_type="STK",
        exchange="SMART",
        currency="USD",
        asset_class=IBKRAssetClass.STOCKS,
        primary_exchange="NASDAQ",
        min_tick=0.01,
        lot_size=1.0,
    ),

    # ── INDICES ─────────────────────────────────────────────────────────────
    "US30": IBKRContract(
        canonical_symbol="US30",
        ibkr_symbol="INDU",
        sec_type="IND",
        exchange="CME",
        currency="USD",
        asset_class=IBKRAssetClass.INDICES,
        min_tick=1.0,
        lot_size=1.0,
    ),
    "US500": IBKRContract(
        canonical_symbol="US500",
        ibkr_symbol="SPX",
        sec_type="IND",
        exchange="CBOE",
        currency="USD",
        asset_class=IBKRAssetClass.INDICES,
        min_tick=0.01,
        lot_size=1.0,
    ),
    "NAS100": IBKRContract(
        canonical_symbol="NAS100",
        ibkr_symbol="NDX",
        sec_type="IND",
        exchange="NASDAQ",
        currency="USD",
        asset_class=IBKRAssetClass.INDICES,
        min_tick=0.01,
        lot_size=1.0,
    ),
    "GER40": IBKRContract(
        canonical_symbol="GER40",
        ibkr_symbol="DAX",
        sec_type="IND",
        exchange="DTB",
        currency="EUR",
        asset_class=IBKRAssetClass.INDICES,
        min_tick=0.01,
        lot_size=1.0,
    ),
    "UK100": IBKRContract(
        canonical_symbol="UK100",
        ibkr_symbol="UKX",
        sec_type="IND",
        exchange="LSE",
        currency="GBP",
        asset_class=IBKRAssetClass.INDICES,
        min_tick=0.01,
        lot_size=1.0,
    ),

    # ── COMMODITIES ─────────────────────────────────────────────────────────
    "USOIL": IBKRContract(
        canonical_symbol="USOIL",
        ibkr_symbol="CL",   # Maps to crude oil futures at IBKR
        sec_type="FUT",
        exchange="NYMEX",
        currency="USD",
        asset_class=IBKRAssetClass.COMMODITIES,
        expiry=_FUTURES_EXPIRY,
        multiplier="1000",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "UKOIL": IBKRContract(
        canonical_symbol="UKOIL",
        ibkr_symbol="BRN",  # Brent Crude
        sec_type="FUT",
        exchange="IPE",
        currency="USD",
        asset_class=IBKRAssetClass.COMMODITIES,
        expiry=_FUTURES_EXPIRY,
        multiplier="1000",
        min_tick=0.01,
        lot_size=1.0,
    ),
    "NATGAS": IBKRContract(
        canonical_symbol="NATGAS",
        ibkr_symbol="NG",
        sec_type="FUT",
        exchange="NYMEX",
        currency="USD",
        asset_class=IBKRAssetClass.COMMODITIES,
        expiry=_FUTURES_EXPIRY,
        multiplier="10000",
        min_tick=0.001,
        lot_size=1.0,
    ),
    "COPPER": IBKRContract(
        canonical_symbol="COPPER",
        ibkr_symbol="HG",
        sec_type="FUT",
        exchange="COMEX",
        currency="USD",
        asset_class=IBKRAssetClass.COMMODITIES,
        expiry=_FUTURES_EXPIRY,
        multiplier="25000",
        min_tick=0.0005,
        lot_size=1.0,
    ),
    "XAGUSD": IBKRContract(
        canonical_symbol="XAGUSD",
        ibkr_symbol="SI",   # Silver Futures
        sec_type="FUT",
        exchange="COMEX",
        currency="USD",
        asset_class=IBKRAssetClass.COMMODITIES,
        expiry=_FUTURES_EXPIRY,
        multiplier="5000",
        min_tick=0.005,
        lot_size=1.0,
    ),
}

# Symbols that MUST route to OANDA (never IBKR)
OANDA_EXCLUSIVE_SYMBOLS = {"EURUSD"}

# Symbols that MUST route to IBKR
IBKR_PRIMARY_SYMBOLS = {
    "XAUUSD", "GC", "ES", "NQ", "YM", "CL",   # Futures
    "AAPL", "MSFT", "TSLA", "NVDA", "SPY", "QQQ", "AMZN", "GOOGL", "META",  # Stocks
    "US30", "US500", "NAS100", "GER40", "UK100",  # Indices
    "USOIL", "UKOIL", "NATGAS", "COPPER", "XAGUSD",  # Commodities
    "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "EURJPY", "GBPJPY",  # Forex (non-EURUSD)
}


# ---------------------------------------------------------------------------
# CONTRACT BUILDER (PUBLIC API)
# ---------------------------------------------------------------------------

def build_ibkr_contract(symbol: str, asset_class: str = None) -> Optional[IBKRContract]:
    """
    Build and return an IBKRContract for the given canonical symbol.

    ROUTING RULES (enforced here):
      - EURUSD  → raises ValueError: use OANDA for EURUSD
      - All others → looks up IBKR_CONTRACT_MAP

    Args:
        symbol      : Canonical symbol (e.g. "XAUUSD", "ES", "AAPL")
        asset_class : Optional override. If not provided, derived from map.

    Returns:
        IBKRContract — fully populated contract spec.

    Raises:
        ValueError — if symbol is EURUSD (must use OANDA) or unknown.
    """
    sym = symbol.upper().strip()

    # Hard rule: EURUSD never goes to IBKR
    if sym in OANDA_EXCLUSIVE_SYMBOLS:
        raise ValueError(
            f"ROUTING ERROR: {sym} must route to OANDA, not IBKR. "
            "This is a hard routing rule enforced by the Intent Layer."
        )

    contract = IBKR_CONTRACT_MAP.get(sym)
    if contract is None:
        raise ValueError(
            f"Unknown IBKR symbol: '{sym}'. "
            f"Add it to IBKR_CONTRACT_MAP or verify the canonical symbol. "
            f"Known IBKR symbols: {sorted(IBKR_CONTRACT_MAP.keys())}"
        )

    # Optional asset_class override (e.g. caller specifies "FUTURES" for disambiguation)
    if asset_class:
        try:
            ac = IBKRAssetClass(asset_class.upper())
            if contract.asset_class != ac:
                logger.warning(
                    f"[IBKRConnector] build_ibkr_contract: symbol {sym!r} asset_class "
                    f"override {ac.value!r} differs from map {contract.asset_class.value!r}. "
                    "Using map value."
                )
        except ValueError:
            logger.warning(
                f"[IBKRConnector] build_ibkr_contract: unknown asset_class override {asset_class!r}. Ignoring."
            )

    return contract


# ---------------------------------------------------------------------------
# IBKR ORDER EVENT (for async tracking via EWrapper)
# ---------------------------------------------------------------------------

@dataclass
class IBKROrderEvent:
    """Tracks async order state received from IBKR callbacks."""
    order_id: int
    client_id: str
    canonical_symbol: str
    status: str = "SUBMITTED"
    filled: float = 0.0
    avg_fill_price: float = 0.0
    remaining: float = 0.0
    last_fill_price: float = 0.0
    error_code: Optional[int] = None
    error_msg: Optional[str] = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# IBKR CONNECTOR
# ---------------------------------------------------------------------------

class IBKRConnector(BaseBrokerConnector):
    """
    Interactive Brokers connector for Apex Trader.

    Supports two connection modes:
      1. EClient socket (ibapi) — real TWS/Gateway connection
      2. Simulated/paper mode — for testing without a live TWS instance

    ROUTING (enforced at this level):
      - EURUSD is rejected — use OandaConnector for EURUSD
      - All other mapped symbols are accepted

    Connection requirements:
      - IB TWS or IB Gateway must be running
      - API connections must be enabled in TWS settings
      - Paper trading account: port 7497
      - Live trading account: port 7496
    """

    # IBKR API endpoints / connection params
    TWS_HOST_DEFAULT       = "127.0.0.1"
    TWS_PORT_PAPER         = 7497   # TWS paper trading
    TWS_PORT_LIVE          = 7496   # TWS live trading
    TWS_PORT_GATEWAY_PAPER = 4002   # IB Gateway paper
    TWS_PORT_GATEWAY_LIVE  = 4001   # IB Gateway live

    # Order timeout
    ORDER_TIMEOUT_SECONDS  = 30

    # IBeam/Client Portal REST (alternative to EClient)
    CLIENT_PORTAL_HOST     = "https://localhost:5000"

    def __init__(self, creds, config: dict = None):
        """
        Args:
            creds  : BrokerCredentials from CredentialManager (ibkr)
            config : Optional override config dict
        """
        self._creds        = creds
        self._config       = config or {}
        self._conn_state   = ConnectionState.DISCONNECTED
        self._account_id   = getattr(creds, "account_id", None) or ""
        self._environment  = getattr(creds, "environment", "practice")

        # Determine TWS port based on environment
        self._host         = self._config.get("host", self.TWS_HOST_DEFAULT)
        if self._environment == "live":
            self._port     = self._config.get("port", self.TWS_PORT_LIVE)
        else:
            self._port     = self._config.get("port", self.TWS_PORT_PAPER)
        self._client_id    = self._config.get("client_id", 1)

        # ibapi EClient wrapper (lazy-loaded to avoid hard dependency)
        self._ib_app       = None   # IBKRAppWrapper instance when connected via EClient
        self._api_thread   = None   # Background thread for EClient message loop

        # Order tracking
        self._pending_orders: Dict[int, IBKROrderEvent] = {}
        self._next_order_id = 1
        self._order_lock    = threading.Lock()

        # Cached account data
        self._account_info_cache: Optional[AccountInfo] = None
        self._positions_cache: List[BrokerPosition] = []

        # Rejection history (bounded)
        self._rejection_history: List[dict] = []

        # Circuit breaker
        self._consecutive_failures = 0
        self._circuit_open = False
        self._CIRCUIT_THRESHOLD = 5

        logger.info(
            f"[IBKRConnector] Initialised — env={self._environment!r} "
            f"host={self._host} port={self._port} client_id={self._client_id}"
        )

    # ------------------------------------------------------------------
    # CONNECTION
    # ------------------------------------------------------------------

    def authenticate(self) -> Tuple[bool, str]:
        """
        Connect to TWS / IB Gateway via EClient socket.
        Falls back to connectivity probe if ibapi is not installed.

        Returns (True, msg) on success, (False, reason) on failure.
        """
        self._conn_state = ConnectionState.CONNECTING

        # Try to import ibapi (optional dependency)
        try:
            from ibapi.client import EClient  # noqa: F401
            from ibapi.wrapper import EWrapper  # noqa: F401
            return self._connect_eclient()
        except ImportError:
            logger.warning(
                "[IBKRConnector] ibapi not installed. Attempting socket probe to confirm TWS is reachable."
            )
            return self._connect_socket_probe()

    def _connect_eclient(self) -> Tuple[bool, str]:
        """Connect using ibapi EClient/EWrapper."""
        try:
            from brokers._ibkr_app import IBKRAppWrapper
            self._ib_app = IBKRAppWrapper(
                account_id=self._account_id,
                client_id=self._client_id,
            )
            self._ib_app.connect(self._host, self._port, clientId=self._client_id)

            # Start EClient message loop in background thread
            self._api_thread = threading.Thread(
                target=self._ib_app.run,
                daemon=True,
                name="ibkr-eclient",
            )
            self._api_thread.start()

            # Wait for nextValidId callback (confirms connection)
            deadline = time.time() + 10
            while time.time() < deadline:
                if self._ib_app.next_order_id is not None:
                    self._next_order_id = self._ib_app.next_order_id
                    break
                time.sleep(0.2)

            if self._ib_app.next_order_id is None:
                self._conn_state = ConnectionState.ERROR
                return False, "Timeout waiting for IBKR nextValidId — TWS may not be accepting connections"

            self._conn_state = ConnectionState.AUTHENTICATED
            msg = (
                f"IBKR connected via EClient: host={self._host} port={self._port} "
                f"client_id={self._client_id} env={self._environment!r} "
                f"next_order_id={self._next_order_id}"
            )
            logger.info(f"[IBKRConnector] {msg}")
            return True, msg

        except Exception as exc:
            self._conn_state = ConnectionState.ERROR
            logger.error(f"[IBKRConnector] EClient connection failed: {exc}")
            return False, f"IBKR EClient connection failed: {exc}"

    def _connect_socket_probe(self) -> Tuple[bool, str]:
        """
        Probe TCP socket to verify TWS/Gateway is reachable.
        Used when ibapi is not installed (e.g. CI environment).
        """
        try:
            with socket.create_connection((self._host, self._port), timeout=5):
                self._conn_state = ConnectionState.AUTHENTICATED
                msg = (
                    f"IBKR socket probe succeeded: {self._host}:{self._port} "
                    f"(ibapi not installed — order submission unavailable)"
                )
                logger.info(f"[IBKRConnector] {msg}")
                return True, msg
        except (socket.timeout, ConnectionRefusedError, OSError) as exc:
            self._conn_state = ConnectionState.ERROR
            return (
                False,
                f"IBKR TWS/Gateway not reachable at {self._host}:{self._port} — "
                f"ensure TWS or IB Gateway is running and API connections are enabled. "
                f"Error: {exc}"
            )

    def is_connected(self) -> bool:
        return self._conn_state == ConnectionState.AUTHENTICATED

    def _require_connected(self):
        if not self.is_connected():
            raise RuntimeError(
                "IBKRConnector: not connected. Call authenticate() first."
            )

    def check_circuit_breaker(self) -> bool:
        """Returns True if circuit breaker is open (too many failures)."""
        return self._circuit_open

    def connection_status(self) -> dict:
        return {
            "broker": "ibkr",
            "state": self._conn_state.value,
            "host": self._host,
            "port": self._port,
            "client_id": self._client_id,
            "environment": self._environment,
            "account_id": self._account_id,
            "circuit_open": self._circuit_open,
            "consecutive_failures": self._consecutive_failures,
        }

    # ------------------------------------------------------------------
    # ACCOUNT INFO
    # ------------------------------------------------------------------

    def get_account_info(self) -> AccountInfo:
        self._require_connected()
        try:
            if self._ib_app and hasattr(self._ib_app, "get_account_summary"):
                raw = self._ib_app.get_account_summary()
                info = AccountInfo(
                    account_id=self._account_id,
                    broker="ibkr",
                    environment=self._environment,
                    currency=raw.get("currency", "USD"),
                    balance=float(raw.get("TotalCashValue", 0.0)),
                    nav=float(raw.get("NetLiquidation", 0.0)),
                    unrealized_pnl=float(raw.get("UnrealizedPnL", 0.0)),
                    realized_pnl=float(raw.get("RealizedPnL", 0.0)),
                    margin_used=float(raw.get("MaintMarginReq", 0.0)),
                    margin_available=float(raw.get("AvailableFunds", 0.0)),
                    open_trade_count=int(raw.get("open_trades", 0)),
                    open_position_count=int(raw.get("open_positions", 0)),
                    pending_order_count=int(raw.get("pending_orders", 0)),
                    raw=raw,
                )
                self._account_info_cache = info
                return info

            # Fallback: return cached or stub
            if self._account_info_cache:
                return self._account_info_cache

            # Paper/stub mode
            return AccountInfo(
                account_id=self._account_id or "IBKR_PAPER",
                broker="ibkr",
                environment=self._environment,
                currency="USD",
                balance=0.0,
                nav=0.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                margin_used=0.0,
                margin_available=0.0,
                open_trade_count=0,
                open_position_count=0,
                pending_order_count=0,
                raw={"note": "ibapi not connected — stub response"},
            )
        except Exception as exc:
            logger.error(f"[IBKRConnector] get_account_info failed: {exc}")
            raise

    def get_balances(self) -> List[BrokerBalance]:
        info = self.get_account_info()
        return [
            BrokerBalance(
                currency=info.currency,
                balance=info.balance,
                available=info.margin_available,
                used=info.margin_used,
                unrealized_pnl=info.unrealized_pnl,
            )
        ]

    def get_permissions(self) -> PermissionsCheck:
        self._require_connected()
        notes = []
        if self._environment == "practice":
            notes.append("IBKR paper trading account — live orders disabled")
        return PermissionsCheck(
            can_trade=True,
            can_view_account=True,
            can_view_positions=True,
            can_manage_orders=True,
            restricted_instruments=[],
            notes=notes,
        )

    def get_positions(self) -> List[BrokerPosition]:
        self._require_connected()
        if self._ib_app and hasattr(self._ib_app, "get_positions"):
            raw_positions = self._ib_app.get_positions()
            positions = []
            for p in raw_positions:
                positions.append(BrokerPosition(
                    instrument=p.get("symbol", "UNKNOWN"),
                    side="LONG" if p.get("position", 0) > 0 else "SHORT",
                    units=abs(p.get("position", 0)),
                    average_price=p.get("avgCost", 0.0),
                    unrealized_pnl=p.get("unrealizedPNL", 0.0),
                    raw=p,
                ))
            self._positions_cache = positions
            return positions
        return self._positions_cache

    def get_open_orders(self) -> List[BrokerOrder]:
        self._require_connected()
        if self._ib_app and hasattr(self._ib_app, "get_open_orders"):
            raw_orders = self._ib_app.get_open_orders()
            orders = []
            for o in raw_orders:
                status_map = {
                    "Submitted": OrderStatus.PENDING,
                    "PreSubmitted": OrderStatus.PENDING,
                    "Filled": OrderStatus.FILLED,
                    "Cancelled": OrderStatus.CANCELLED,
                    "Inactive": OrderStatus.CANCELLED,
                }
                orders.append(BrokerOrder(
                    order_id=str(o.get("orderId", "")),
                    instrument=o.get("symbol", ""),
                    side=OrderSide.BUY if o.get("action", "BUY") == "BUY" else OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    units=o.get("totalQuantity", 0),
                    price=o.get("lmtPrice", None),
                    status=status_map.get(o.get("status", ""), OrderStatus.UNKNOWN),
                    raw=o,
                ))
            return orders
        return []

    # ------------------------------------------------------------------
    # INSTRUMENT VALIDATION
    # ------------------------------------------------------------------

    def validate_instrument_mapping(self, canonical_symbol: str) -> InstrumentMapping:
        """
        Validate and return mapping for a canonical symbol.
        Enforces EURUSD→OANDA routing rule.
        """
        sym = canonical_symbol.upper().strip()

        # Hard rule: EURUSD must use OANDA
        if sym in OANDA_EXCLUSIVE_SYMBOLS:
            return InstrumentMapping(
                internal=sym,
                broker_symbol=None,
                tradeable=False,
                min_units=0,
                max_units=0,
                precision=5,
                margin_rate=0.0,
                spread_typical=0.0,
                is_mapped=False,
                is_supported=False,
                is_tradeable_metadata=False,
                not_tradeable_reason=(
                    f"{sym} must route to OANDA, not IBKR. "
                    "This is a hard routing rule — see BROKER_ROUTING_RULES."
                ),
            )

        contract = IBKR_CONTRACT_MAP.get(sym)
        if contract is None:
            return InstrumentMapping(
                internal=sym,
                broker_symbol=None,
                tradeable=False,
                min_units=0,
                max_units=0,
                precision=2,
                margin_rate=0.0,
                spread_typical=0.0,
                is_mapped=False,
                is_supported=False,
                is_tradeable_metadata=False,
                not_tradeable_reason=f"Symbol {sym!r} not in IBKR contract map.",
            )

        # If connected via ibapi, do a live instrument details check
        is_tradeable_meta = True
        not_tradeable_reason = None
        raw_meta = contract.to_dict()

        if self._ib_app and hasattr(self._ib_app, "validate_contract"):
            try:
                details = self._ib_app.validate_contract(contract)
                is_tradeable_meta = details.get("tradeable", True)
                if not is_tradeable_meta:
                    not_tradeable_reason = f"IBKR reports {sym} as non-tradeable"
                raw_meta.update(details)
            except Exception as exc:
                logger.warning(f"[IBKRConnector] Live validation failed for {sym}: {exc}")
                is_tradeable_meta = False
                not_tradeable_reason = f"Validation exception: {exc}"

        return InstrumentMapping(
            internal=sym,
            broker_symbol=contract.ibkr_symbol,
            tradeable=is_tradeable_meta,
            min_units=1.0,
            max_units=1_000_000.0,
            precision=len(str(contract.min_tick).rstrip("0").split(".")[-1]) if "." in str(contract.min_tick) else 0,
            margin_rate=0.05,   # default; overridden by live query
            spread_typical=0.0,
            is_mapped=True,
            is_supported=True,
            is_tradeable_metadata=is_tradeable_meta,
            raw_broker_metadata=raw_meta,
            not_tradeable_reason=not_tradeable_reason,
        )

    # ------------------------------------------------------------------
    # ORDER SUBMISSION
    # ------------------------------------------------------------------

    def submit_order(self, request: OrderRequest) -> OrderResult:
        """
        Submit a market order to IBKR via EClient.

        ROUTING ENFORCEMENT:
          - EURUSD is rejected at this level (must use OANDA)
          - All other mapped symbols proceed

        Risk normalization:
          - FOREX: units in base currency (IBKR IDEALPRO min 25,000)
          - FUTURES (GC/ES/NQ): units = number of contracts
          - STOCKS: units = number of shares
          - COMMODITIES: units = number of contracts

        Returns OrderResult with all diagnostic fields populated.
        """
        if self._circuit_open:
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason="IBKR circuit breaker open — too many consecutive failures",
                normalized_rejection_code="CIRCUIT_BREAKER_OPEN",
            )

        sym = request.instrument.upper().strip()

        # Hard rule: EURUSD must go to OANDA
        if sym in OANDA_EXCLUSIVE_SYMBOLS:
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason=(
                    f"ROUTING ERROR: {sym} must route to OANDA. "
                    "IBKRConnector does not handle EURUSD."
                ),
                normalized_rejection_code="WRONG_BROKER_ROUTING",
            )

        contract = IBKR_CONTRACT_MAP.get(sym)
        if contract is None:
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason=f"No IBKR contract mapping for symbol {sym!r}",
                normalized_rejection_code="NO_BROKER_MAPPING",
            )

        # Validate order request fields
        validation_errors = self._validate_order_request(request, contract)
        if validation_errors:
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason="; ".join(validation_errors),
                normalized_rejection_code="ORDER_VALIDATION_FAILED",
            )

        # Convert lots → broker units
        broker_units = self._lots_to_units(request.units, sym, contract)
        action = "BUY" if request.side == OrderSide.BUY else "SELL"
        client_order_id = request.client_order_id or str(uuid.uuid4())[:8]

        logger.info(
            f"[IBKRConnector] submit_order: sym={sym!r} broker_sym={contract.ibkr_symbol!r} "
            f"sec_type={contract.sec_type!r} action={action!r} "
            f"units={request.units} broker_units={broker_units} "
            f"entry={request.price} sl={request.stop_loss} tp={request.take_profit}"
        )

        # If ibapi is available: submit via EClient
        if self._ib_app and hasattr(self._ib_app, "place_order"):
            return self._submit_via_eclient(
                request, contract, broker_units, action, client_order_id
            )
        else:
            # ibapi not installed — paper/simulation mode
            return self._submit_paper_simulation(
                request, contract, broker_units, action, client_order_id
            )

    def _validate_order_request(
        self, request: OrderRequest, contract: IBKRContract
    ) -> List[str]:
        """Return list of validation errors (empty = valid)."""
        errors = []
        if request.units <= 0:
            errors.append(f"units must be > 0, got {request.units}")
        if request.stop_loss is None:
            errors.append("stop_loss is required")
        if request.take_profit is None:
            errors.append("take_profit is required")
        if request.stop_loss is not None and request.price is not None:
            if request.side == OrderSide.BUY and request.stop_loss >= (request.price or float("inf")):
                errors.append(
                    f"BUY: stop_loss ({request.stop_loss}) must be below entry price ({request.price})"
                )
            if request.side == OrderSide.SELL and request.stop_loss <= (request.price or 0):
                errors.append(
                    f"SELL: stop_loss ({request.stop_loss}) must be above entry price ({request.price})"
                )
        # Futures: minimum 1 contract
        if contract.sec_type == "FUT" and request.units < 1:
            errors.append(f"Futures minimum 1 contract; got {request.units}")
        return errors

    def _lots_to_units(self, lots: float, symbol: str, contract: IBKRContract) -> float:
        """
        Convert strategy lots to broker-native units.

        EXACT CONVERSION RULES:
          FOREX (IDEALPRO): 1 lot = 100,000 base currency units; IBKR min 25,000
          GC  (Gold Fut):   1 lot = 1 contract (100 troy oz per contract)
          ES  (S&P Fut):    1 lot = 1 contract ($50 × index)
          NQ  (Nasdaq Fut): 1 lot = 1 contract ($20 × index)
          YM  (Dow Fut):    1 lot = 1 contract ($5 × index)
          CL  (Crude Fut):  1 lot = 1 contract (1,000 barrels)
          STK (Stocks):     1 lot = 1 share
          IND (Indices):    informational only; trade via futures or ETF
        """
        if contract.sec_type == "CASH":
            # IBKR Forex: lots × 100,000; enforce min 25,000
            units = lots * 100_000
            return max(units, 25_000)
        elif contract.sec_type in ("FUT", "CONTFUT"):
            # Futures: lots = contracts (already in contract units)
            return max(1.0, round(lots))
        elif contract.sec_type == "STK":
            # Stocks: lots = shares (round to whole shares)
            return max(1.0, round(lots))
        elif contract.sec_type in ("IND", "CMDTY"):
            # Index/commodity: 1 lot = 1 unit
            return max(1.0, round(lots))
        else:
            return lots

    def _submit_via_eclient(
        self,
        request: OrderRequest,
        contract: IBKRContract,
        broker_units: float,
        action: str,
        client_order_id: str,
    ) -> OrderResult:
        """Submit order via ibapi EClient and wait for fill/rejection."""
        from ibapi.order import Order as IBOrder
        from ibapi.contract import Contract as IBContract

        ib_contract = IBContract()
        ib_contract.symbol   = contract.ibkr_symbol
        ib_contract.secType  = contract.sec_type
        ib_contract.exchange = contract.exchange
        ib_contract.currency = contract.currency
        if contract.expiry:
            ib_contract.lastTradeDateOrContractMonth = contract.expiry
        if contract.multiplier:
            ib_contract.multiplier = contract.multiplier
        if contract.primary_exchange:
            ib_contract.primaryExch = contract.primary_exchange

        ib_order          = IBOrder()
        ib_order.action   = action
        ib_order.totalQuantity = int(broker_units)
        ib_order.orderType = "MKT"
        ib_order.tif      = "DAY"

        # Attach SL as bracket (separate orders in real impl; simplified here)
        # Full bracket order management handled by ExecutionSupervisorAgent
        if request.stop_loss:
            ib_order.auxPrice = request.stop_loss  # for stop orders

        with self._order_lock:
            order_id = self._next_order_id
            self._next_order_id += 1

        event = IBKROrderEvent(
            order_id=order_id,
            client_id=client_order_id,
            canonical_symbol=request.instrument,
        )
        self._pending_orders[order_id] = event

        try:
            self._ib_app.placeOrder(order_id, ib_contract, ib_order)

            # Wait for fill with timeout
            deadline = time.time() + self.ORDER_TIMEOUT_SECONDS
            while time.time() < deadline:
                evt = self._pending_orders.get(order_id)
                if evt and evt.status in ("FILLED", "CANCELLED", "INACTIVE"):
                    break
                time.sleep(0.5)

            evt = self._pending_orders.pop(order_id, None)
            if evt and evt.status == "FILLED":
                self._consecutive_failures = 0
                logger.info(
                    f"[IBKRConnector] Order FILLED: id={order_id} sym={request.instrument!r} "
                    f"avg_price={evt.avg_fill_price} filled={evt.filled}"
                )
                return OrderResult(
                    success=True,
                    order_id=str(order_id),
                    status=OrderStatus.FILLED,
                    filled_price=evt.avg_fill_price,
                    filled_units=evt.filled,
                    raw={"order_id": order_id, "ibkr_status": evt.status},
                )
            else:
                # Timeout or rejection
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._CIRCUIT_THRESHOLD:
                    self._circuit_open = True
                    logger.error("[IBKRConnector] Circuit breaker OPENED")

                reason = evt.error_msg if evt else "Order timeout — no fill received"
                self._record_rejection(request, reason, str(evt.error_code if evt else "TIMEOUT"))
                return OrderResult(
                    success=False,
                    order_id=str(order_id),
                    status=OrderStatus.REJECTED,
                    reject_reason=reason,
                    raw_broker_error=reason,
                    normalized_rejection_code="BROKER_REJECTION",
                )

        except Exception as exc:
            self._consecutive_failures += 1
            logger.error(f"[IBKRConnector] submit_order EClient exception: {exc}")
            self._record_rejection(request, str(exc), "EXCEPTION")
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason=f"EClient exception: {exc}",
                raw_broker_error=str(exc),
                normalized_rejection_code="BROKER_EXCEPTION",
            )

    def _submit_paper_simulation(
        self,
        request: OrderRequest,
        contract: IBKRContract,
        broker_units: float,
        action: str,
        client_order_id: str,
    ) -> OrderResult:
        """
        Simulate a fill for paper/testing when ibapi is not installed.
        Returns immediate simulated fill at requested price.
        """
        simulated_fill_price = request.price or 0.0
        order_id = f"IBKR-SIM-{client_order_id}"
        logger.info(
            f"[IBKRConnector] PAPER SIM fill: id={order_id} sym={request.instrument!r} "
            f"action={action!r} units={broker_units} price={simulated_fill_price}"
        )
        return OrderResult(
            success=True,
            order_id=order_id,
            status=OrderStatus.FILLED,
            filled_price=simulated_fill_price,
            filled_units=broker_units,
            raw={
                "paper": True,
                "ibkr_symbol": contract.ibkr_symbol,
                "sec_type": contract.sec_type,
                "exchange": contract.exchange,
                "action": action,
                "broker_units": broker_units,
            },
        )

    def _record_rejection(self, request: OrderRequest, reason: str, code: str):
        """Record rejection in bounded history."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "canonical_symbol": request.instrument,
            "units": request.units,
            "side": request.side.value,
            "reason": reason,
            "parsed_error_code": code,
            "normalized_rejection_code": "BROKER_REJECTION",
        }
        self._rejection_history.append(entry)
        if len(self._rejection_history) > 200:
            self._rejection_history.pop(0)

    # ------------------------------------------------------------------
    # ORDER MANAGEMENT
    # ------------------------------------------------------------------

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        self._require_connected()
        try:
            if self._ib_app and hasattr(self._ib_app, "cancelOrder"):
                self._ib_app.cancelOrder(int(order_id))
                logger.info(f"[IBKRConnector] Cancel sent for order {order_id}")
                return True, f"Cancel request sent for order {order_id}"
            return False, "ibapi not connected — cannot cancel"
        except Exception as exc:
            return False, f"Cancel failed: {exc}"

    def get_order_status(self, order_id: str) -> Optional[BrokerOrder]:
        evt = self._pending_orders.get(int(order_id))
        if evt:
            status_map = {
                "SUBMITTED": OrderStatus.PENDING,
                "FILLED": OrderStatus.FILLED,
                "CANCELLED": OrderStatus.CANCELLED,
            }
            return BrokerOrder(
                order_id=str(evt.order_id),
                instrument=evt.canonical_symbol,
                side=OrderSide.BUY,   # Direction stored in original request
                order_type=OrderType.MARKET,
                units=evt.filled or 0,
                price=evt.avg_fill_price,
                status=status_map.get(evt.status, OrderStatus.UNKNOWN),
                raw={"event": evt.__dict__},
            )
        return None

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def reconcile_state(self, internal_positions: List[dict]) -> ReconciliationResult:
        """Compare internal engine positions with live IBKR positions."""
        self._require_connected()
        try:
            live_positions = self.get_positions()
            live_map = {p.instrument: p for p in live_positions}
            internal_map = {p["instrument"]: p for p in internal_positions}

            matched, mismatched, internal_only, broker_only = [], [], [], []
            total_discrepancy = 0.0

            for sym, internal in internal_map.items():
                if sym in live_map:
                    live = live_map[sym]
                    diff = abs(internal.get("unrealized_pnl", 0) - live.unrealized_pnl)
                    if diff > 5.0:
                        mismatched.append(sym)
                        total_discrepancy += diff
                    else:
                        matched.append(sym)
                else:
                    internal_only.append(sym)

            for sym in live_map:
                if sym not in internal_map:
                    broker_only.append(sym)

            is_clean = (
                len(mismatched) == 0
                and len(internal_only) == 0
                and len(broker_only) == 0
                and total_discrepancy < 10.0
            )
            return ReconciliationResult(
                matched_positions=matched,
                mismatched_positions=mismatched,
                internal_only_positions=internal_only,
                broker_only_positions=broker_only,
                net_pnl_discrepancy_usd=round(total_discrepancy, 2),
                is_clean=is_clean,
                notes=["IBKR reconciliation completed"],
            )
        except Exception as exc:
            logger.error(f"[IBKRConnector] reconcile_state failed: {exc}")
            return ReconciliationResult(
                matched_positions=[],
                mismatched_positions=[],
                internal_only_positions=[i["instrument"] for i in internal_positions],
                broker_only_positions=[],
                net_pnl_discrepancy_usd=0.0,
                is_clean=False,
                notes=[f"Reconciliation failed: {exc}"],
            )

    # ------------------------------------------------------------------
    # PREFLIGHT
    # ------------------------------------------------------------------

    def run_preflight_checks(self) -> PreflightResult:
        """
        Run IBKR pre-flight checks:
          1. Credential format validation
          2. TWS/Gateway connectivity
          3. Account info retrieval
          4. Key symbols tradeable (GC, ES, NQ)
        """
        checks = []
        blocking = []
        warnings = []

        # 1. Credentials
        if self._creds and self._account_id:
            checks.append(PreflightCheck(
                name="credential_format",
                passed=True,
                message=f"IBKR account ID present: {self._account_id[:4]}***",
                critical=True,
            ))
        else:
            checks.append(PreflightCheck(
                name="credential_format",
                passed=False,
                message="IBKR account ID missing",
                critical=True,
            ))
            blocking.append("No IBKR account ID configured")

        # 2. Connectivity
        auth_ok, auth_msg = self.authenticate()
        checks.append(PreflightCheck(
            name="tws_connectivity",
            passed=auth_ok,
            message=auth_msg,
            critical=True,
        ))
        if not auth_ok:
            blocking.append(auth_msg)

        # 3. Account info
        if auth_ok:
            try:
                info = self.get_account_info()
                checks.append(PreflightCheck(
                    name="account_info",
                    passed=True,
                    message=f"Account {info.account_id}: balance={info.balance:.2f} {info.currency}",
                    critical=False,
                ))
            except Exception as exc:
                checks.append(PreflightCheck(
                    name="account_info",
                    passed=False,
                    message=str(exc),
                    critical=False,
                ))
                warnings.append(f"Account info fetch failed: {exc}")

        # 4. Key symbols
        for sym in ["GC", "ES", "NQ"]:
            mapping = self.validate_instrument_mapping(sym)
            checks.append(PreflightCheck(
                name=f"symbol_{sym}_mapped",
                passed=mapping.is_mapped,
                message=f"{sym} → {mapping.broker_symbol or 'NOT FOUND'}",
                critical=False,
            ))

        overall = len(blocking) == 0
        return PreflightResult(
            broker="ibkr",
            environment=self._environment,
            checks=checks,
            overall_pass=overall,
            blocking_failures=blocking,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # ECLIENT ORDER EVENT CALLBACKS (called by IBKRAppWrapper)
    # ------------------------------------------------------------------

    def on_order_status(
        self,
        order_id: int,
        status: str,
        filled: float,
        remaining: float,
        avg_fill_price: float,
        last_fill_price: float,
    ):
        """Called by IBKRAppWrapper.orderStatus callback."""
        with self._order_lock:
            evt = self._pending_orders.get(order_id)
            if evt:
                evt.status = status
                evt.filled = filled
                evt.remaining = remaining
                evt.avg_fill_price = avg_fill_price
                evt.last_fill_price = last_fill_price
                evt.updated_at = datetime.now(timezone.utc)

    def on_order_error(self, order_id: int, error_code: int, error_msg: str):
        """Called by IBKRAppWrapper.error callback for order errors."""
        with self._order_lock:
            evt = self._pending_orders.get(order_id)
            if evt:
                evt.status = "CANCELLED"
                evt.error_code = error_code
                evt.error_msg = error_msg
                evt.updated_at = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # DIAGNOSTICS
    # ------------------------------------------------------------------

    def get_rejection_history(self) -> List[dict]:
        return list(self._rejection_history)

    def get_contract_map(self) -> Dict[str, dict]:
        """Return full contract map as dicts (for diagnostics)."""
        return {sym: c.to_dict() for sym, c in IBKR_CONTRACT_MAP.items()}

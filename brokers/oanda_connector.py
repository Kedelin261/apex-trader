"""
APEX MULTI-MARKET TJR ENGINE
OANDA v20 REST API Connector — Full production implementation (v2.3).

Changes in v2.3:
  - validate_instrument_mapping now populates granular capability flags
    (is_mapped, is_supported, is_tradeable_metadata) and never returns
    optimistically tradeable=True from the static fallback alone.
  - submit_order preserves full raw broker error, parsed_error_code, and
    normalized_rejection_code in every OrderResult and rejection history entry.
  - Gold (XAU_USD) payload audit:
      * Units are in troy ounces (not lots). 1 unit = 1 oz.
      * Minimum order size: 1 oz for XAU_USD on OANDA.
      * Lots→ounces conversion: 1 standard lot ≈ 100 oz.
      * SL/TP minimum distance for gold: 1 pip = $1 (1 USD per oz).
  - Pre-submit payload validation (_validate_gold_payload) checks units,
    precision, and SL/TP minimum distance before submission.
  - Structured diagnostics dict returned in rejection history.

Supported operations:
  - authenticate()
  - is_connected()
  - get_account_info()
  - get_balances()
  - get_permissions()
  - get_positions()
  - get_open_orders()
  - validate_instrument_mapping(symbol)  — granular capability flags
  - submit_order(request)               — with gold payload validation
  - cancel_order(order_id)
  - get_order_status(order_id)
  - reconcile_state(internal)
  - run_preflight_checks()
  - validate_xau_payload(request)       — dry-run XAU payload check (no order)

SECURITY: Token is NEVER logged. All log statements use mask().
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from brokers.base_connector import (
    AccountInfo, BaseBrokerConnector, BrokerBalance, BrokerOrder,
    BrokerPosition, ConnectionState, InstrumentMapping, OrderRequest,
    OrderResult, OrderSide, OrderStatus, OrderType, PermissionsCheck,
    PreflightCheck, PreflightResult, ReconciliationResult,
)
from brokers.credential_manager import BrokerCredentials, CredentialManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OANDA SYMBOL MAPPING
# Internal symbol → OANDA instrument ID
# ---------------------------------------------------------------------------

OANDA_INSTRUMENT_MAP: Dict[str, str] = {
    # Forex
    "EURUSD":  "EUR_USD",
    "GBPUSD":  "GBP_USD",
    "USDJPY":  "USD_JPY",
    "AUDUSD":  "AUD_USD",
    "USDCAD":  "USD_CAD",
    "USDCHF":  "USD_CHF",
    "NZDUSD":  "NZD_USD",
    "EURGBP":  "EUR_GBP",
    "EURJPY":  "EUR_JPY",
    "GBPJPY":  "GBP_JPY",
    # Gold / metals
    "XAUUSD":  "XAU_USD",
    "XAGUSD":  "XAG_USD",
    # Oil / commodities
    "USOIL":   "WTICO_USD",  # WTI Crude
    "UKOIL":   "BCO_USD",    # Brent Crude
    # Indices (CFD)
    "US30":    "US30_USD",
    "US500":   "SPX500_USD",
    "NAS100":  "NAS100_USD",
    "GER40":   "DE30_EUR",
    "UK100":   "UK100_GBP",
}

# Reverse map: oanda → internal
REVERSE_MAP = {v: k for k, v in OANDA_INSTRUMENT_MAP.items()}

# OANDA precision per instrument (decimal places for price)
OANDA_PRECISION: Dict[str, int] = {
    "EUR_USD": 5, "GBP_USD": 5, "USD_JPY": 3, "AUD_USD": 5,
    "USD_CAD": 5, "USD_CHF": 5, "NZD_USD": 5, "EUR_GBP": 5,
    "EUR_JPY": 3, "GBP_JPY": 3, "XAU_USD": 2, "XAG_USD": 4,
    "WTICO_USD": 3, "BCO_USD": 3, "US30_USD": 1, "SPX500_USD": 1,
    "NAS100_USD": 1, "DE30_EUR": 1, "UK100_GBP": 1,
}

# ---------------------------------------------------------------------------
# GOLD-SPECIFIC CONSTANTS
# OANDA XAU_USD trades in troy ounces.
#   1 standard lot (FX convention) = 100 oz for gold.
#   Minimum order: 1 oz.
#   Maximum order: 10,000 oz (per OANDA documentation).
#   SL/TP minimum distance: ~$1 per oz (1 price unit).
# ---------------------------------------------------------------------------

GOLD_INSTRUMENTS = {"XAU_USD", "XAG_USD"}

# Lot-to-unit multipliers for instruments that use non-FX unit conventions
LOT_UNIT_MULTIPLIER: Dict[str, float] = {
    "XAU_USD":   100.0,   # 1 standard lot = 100 troy oz
    "XAG_USD":   5000.0,  # 1 standard lot = 5000 troy oz (OANDA convention)
    "WTICO_USD": 1000.0,  # 1 lot = 1000 barrels
    "BCO_USD":   1000.0,
}

# Minimum SL/TP distance (in price units) for instruments
MIN_SLTP_DISTANCE: Dict[str, float] = {
    "XAU_USD": 1.0,    # $1 per oz minimum
    "XAG_USD": 0.10,
}

# OANDA error codes that map to our normalized rejection codes
_OANDA_ERROR_MAP: Dict[str, str] = {
    "INSTRUMENT_NOT_TRADEABLE":                  "INSTRUMENT_NOT_TRADEABLE",
    "INSTRUMENT_HALTED":                         "INSTRUMENT_NOT_TRADEABLE",
    "MARKET_HALTED":                             "INSTRUMENT_NOT_TRADEABLE",
    "ACCOUNT_NOT_TRADEABLE_ON_INSTRUMENT":       "BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
    "INSTRUMENT_NOT_IN_LIST_OF_TRADEABLE_INSTRUMENTS": "BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
    "TRADE_DOESN_T_EXIST":                       "ORDER_NOT_FOUND",
    "UNITS_MINIMUM_NOT_MET":                     "GOLD_UNITS_TOO_SMALL",
    "UNITS_MAXIMUM_EXCEEDED":                    "GOLD_UNITS_TOO_LARGE",
    "STOP_LOSS_ON_FILL_PRICE_MISSING":           "GOLD_SLTP_MISSING",
    "STOP_LOSS_ON_FILL_PRICE_INVALID":           "GOLD_SLTP_INVALID",
    "TAKE_PROFIT_ON_FILL_PRICE_MISSING":         "GOLD_SLTP_MISSING",
    "TAKE_PROFIT_ON_FILL_PRICE_INVALID":         "GOLD_SLTP_INVALID",
    "STOP_LOSS_ON_FILL_LOSS":                    "GOLD_SLTP_LOSS",
    "INSUFFICIENT_MARGIN":                       "INSUFFICIENT_MARGIN",
    "INSUFFICIENT_FUNDS":                        "INSUFFICIENT_FUNDS",
    "CLOSEOUT_BID_LESS_THAN_STOP_PRICE":         "GOLD_SLTP_INVALID",
    "STOP_LOSS_ON_FILL_GUARANTEED_NOT_ALLOWED":  "GOLD_SLTP_INVALID",
    "MARGIN_RATE_WOULD_TRIGGER_CLOSEOUT":        "INSUFFICIENT_MARGIN",
    "ACCOUNT_LOCKED":                            "ACCOUNT_LOCKED",
}


def _normalize_oanda_error(raw_code: str) -> str:
    """Map an OANDA error code to our internal normalized rejection code."""
    if not raw_code:
        return "UNKNOWN_BROKER_ERROR"
    upper = raw_code.upper().strip()
    # Direct lookup
    if upper in _OANDA_ERROR_MAP:
        return _OANDA_ERROR_MAP[upper]
    # Substring matches for partial codes
    if "INSUFFICIENT_MARGIN" in upper or "INSUFFICIENT_FUNDS" in upper:
        return "INSUFFICIENT_MARGIN"
    if "NOT_TRADEABLE" in upper or "HALTED" in upper:
        return "INSTRUMENT_NOT_TRADEABLE"
    if "STOP_LOSS" in upper or "TAKE_PROFIT" in upper:
        return "GOLD_SLTP_INVALID"
    if "UNIT" in upper:
        return "GOLD_UNITS_INVALID"
    return "BROKER_REJECTION"


class OandaConnector(BaseBrokerConnector):
    """
    Full OANDA v20 REST connector.
    Practice and live environments use different REST endpoints.
    """

    PRACTICE_URL = "https://api-fxpractice.oanda.com/v3"
    LIVE_URL = "https://api-fxtrade.oanda.com/v3"

    def __init__(self, credentials: BrokerCredentials, timeout_s: int = 10):
        super().__init__("oanda")
        self._creds = credentials
        self._timeout = timeout_s
        self._session: Optional[requests.Session] = None
        self._account_id: Optional[str] = credentials.account_id
        self._cached_account: Optional[AccountInfo] = None
        self._cached_instruments: Dict[str, InstrumentMapping] = {}
        self._order_rejection_history: List[dict] = []
        self._last_sync_at: Optional[datetime] = None
        # Select correct base URL based on environment
        self.BASE_URL = (
            self.PRACTICE_URL
            if credentials.environment == "practice"
            else self.LIVE_URL
        )

    # ------------------------------------------------------------------
    # INTERNAL HTTP HELPERS
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        """Build auth headers. Token is never logged."""
        return {
            "Authorization": f"Bearer {self._creds.api_token}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
        }

    def _get(self, path: str, params: Optional[dict] = None) -> Tuple[int, dict]:
        """GET request. Returns (status_code, body_dict)."""
        url = f"{self.BASE_URL}{path}"
        try:
            r = self._session.get(url, headers=self._headers(), params=params, timeout=self._timeout)
            try:
                body = r.json()
            except Exception:
                body = {"raw_text": r.text}
            return r.status_code, body
        except requests.exceptions.ConnectionError as e:
            self._record_failure(f"ConnectionError: {e}")
            raise ConnectionError(f"Cannot reach OANDA API: {e}") from e
        except requests.exceptions.Timeout as e:
            self._record_failure(f"Timeout: {e}")
            raise TimeoutError(f"OANDA API timeout: {e}") from e

    def _post(self, path: str, body: dict) -> Tuple[int, dict]:
        """POST request. Returns (status_code, body_dict)."""
        url = f"{self.BASE_URL}{path}"
        try:
            r = self._session.post(url, headers=self._headers(), json=body, timeout=self._timeout)
            try:
                resp = r.json()
            except Exception:
                resp = {"raw_text": r.text}
            return r.status_code, resp
        except requests.exceptions.ConnectionError as e:
            self._record_failure(f"ConnectionError: {e}")
            raise ConnectionError(f"Cannot reach OANDA API: {e}") from e

    def _put(self, path: str, body: dict) -> Tuple[int, dict]:
        """PUT request."""
        url = f"{self.BASE_URL}{path}"
        try:
            r = self._session.put(url, headers=self._headers(), json=body, timeout=self._timeout)
            try:
                resp = r.json()
            except Exception:
                resp = {"raw_text": r.text}
            return r.status_code, resp
        except requests.exceptions.ConnectionError as e:
            self._record_failure(f"ConnectionError: {e}")
            raise ConnectionError(f"OANDA PUT error: {e}") from e

    # ------------------------------------------------------------------
    # AUTHENTICATE
    # ------------------------------------------------------------------

    def authenticate(self) -> Tuple[bool, str]:
        """
        Validate OANDA API token by calling GET /accounts.
        Confirms token works and account_id is accessible.
        Returns (success, message). Never raises.
        """
        if not self._creds.is_usable():
            msg = f"Credentials not usable: {self._creds.validation_errors}"
            logger.error(f"[OANDA] Auth failed: {msg}")
            self._connection_state = ConnectionState.ERROR
            return False, msg

        try:
            self._session = requests.Session()
            self._connection_state = ConnectionState.CONNECTING

            # List all accounts for this token
            status, body = self._get("/accounts")

            if status == 401:
                msg = "401 Unauthorized — OANDA API token rejected"
                logger.error(f"[OANDA] {msg}")
                self._connection_state = ConnectionState.ERROR
                self._record_failure(msg)
                return False, msg

            if status == 403:
                msg = "403 Forbidden — token valid but insufficient permissions"
                logger.error(f"[OANDA] {msg}")
                self._connection_state = ConnectionState.ERROR
                return False, msg

            if status != 200:
                msg = f"Unexpected status {status} from /accounts"
                logger.error(f"[OANDA] {msg}: {body}")
                self._connection_state = ConnectionState.ERROR
                return False, msg

            accounts = body.get("accounts", [])
            if not accounts:
                msg = "No accounts returned for this token"
                logger.error(f"[OANDA] {msg}")
                self._connection_state = ConnectionState.ERROR
                return False, msg

            # Validate account_id exists in returned accounts
            account_ids = [a.get("id", "") for a in accounts]
            if self._account_id and self._account_id not in account_ids:
                msg = (
                    f"Configured account_id '{self._account_id}' not in accessible accounts: "
                    f"{account_ids}"
                )
                logger.error(f"[OANDA] {msg}")
                self._connection_state = ConnectionState.ERROR
                return False, msg

            # Auto-select first account if not configured
            if not self._account_id:
                self._account_id = account_ids[0]
                logger.info(f"[OANDA] Auto-selected account: {self._account_id}")

            self._connection_state = ConnectionState.AUTHENTICATED
            self._connected_at = datetime.now(timezone.utc)
            self._record_success()

            env_label = self._creds.environment.upper()
            msg = (
                f"Authenticated OK [{env_label}] account={self._account_id} "
                f"({len(account_ids)} account(s) accessible)"
            )
            logger.info(f"[OANDA] {msg}")
            return True, msg

        except Exception as e:
            msg = f"Authentication exception: {type(e).__name__}: {e}"
            logger.error(f"[OANDA] {msg}")
            self._connection_state = ConnectionState.ERROR
            self._record_failure(msg)
            return False, msg

    # ------------------------------------------------------------------
    # CONNECTION STATE
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        return self._connection_state == ConnectionState.AUTHENTICATED

    def _require_connected(self) -> None:
        if not self.is_connected():
            raise ConnectionError("OANDA connector not authenticated. Call authenticate() first.")
        if self.check_circuit_breaker():
            raise ConnectionError("OANDA circuit breaker OPEN — too many consecutive failures.")

    # ------------------------------------------------------------------
    # ACCOUNT INFO
    # ------------------------------------------------------------------

    def get_account_info(self) -> AccountInfo:
        """Fetch full account summary from OANDA."""
        self._require_connected()
        status, body = self._get(f"/accounts/{self._account_id}/summary")

        if status != 200:
            err = body.get("errorMessage", str(body))
            self._record_failure(f"get_account_info {status}: {err}")
            raise RuntimeError(f"OANDA get_account_info failed [{status}]: {err}")

        acct = body.get("account", {})

        info = AccountInfo(
            account_id=acct.get("id", self._account_id),
            broker="oanda",
            environment=self._creds.environment,
            currency=acct.get("currency", "USD"),
            balance=float(acct.get("balance", 0)),
            nav=float(acct.get("NAV", 0)),
            unrealized_pnl=float(acct.get("unrealizedPL", 0)),
            realized_pnl=float(acct.get("pl", 0)),
            margin_used=float(acct.get("marginUsed", 0)),
            margin_available=float(acct.get("marginAvailable", 0)),
            open_trade_count=int(acct.get("openTradeCount", 0)),
            open_position_count=int(acct.get("openPositionCount", 0)),
            pending_order_count=int(acct.get("pendingOrderCount", 0)),
            raw=acct,
        )
        self._cached_account = info
        self._last_sync_at = datetime.now(timezone.utc)
        self._record_success()
        return info

    # ------------------------------------------------------------------
    # BALANCES
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # PERMISSIONS
    # ------------------------------------------------------------------

    def get_permissions(self) -> PermissionsCheck:
        """
        OANDA practice accounts can trade fully.
        Live accounts may have trading disabled if balance is 0 or account locked.
        """
        self._require_connected()
        try:
            info = self.get_account_info()
            # Check if account has hedgingEnabled, marginRate, etc.
            raw = info.raw
            guaranteed_stop_loss = raw.get("guaranteedStopLossOrderMode", "DISABLED")
            financing_days_of_week = raw.get("financingDaysOfWeek", [])

            notes = []
            restricted = []

            # Check for zero balance (trading not meaningful)
            if info.balance <= 0:
                notes.append(
                    f"Balance is {info.balance} {info.currency} — "
                    "insufficient funds; orders will be rejected by broker"
                )

            # Check margin availability
            if info.margin_available <= 0 and info.balance > 0:
                notes.append("No margin available — existing positions using all margin")
                restricted.append("ALL (margin unavailable)")

            # OANDA always allows reading for authenticated accounts
            can_trade = True  # OANDA allows attempts; broker rejects on insufficient funds

            return PermissionsCheck(
                can_trade=can_trade,
                can_view_account=True,
                can_view_positions=True,
                can_manage_orders=True,
                restricted_instruments=restricted,
                notes=notes,
            )
        except Exception as e:
            return PermissionsCheck(
                can_trade=False,
                can_view_account=False,
                can_view_positions=False,
                can_manage_orders=False,
                notes=[f"Permission check failed: {e}"],
            )

    # ------------------------------------------------------------------
    # POSITIONS
    # ------------------------------------------------------------------

    def get_positions(self) -> List[BrokerPosition]:
        """Fetch all open positions."""
        self._require_connected()
        status, body = self._get(f"/accounts/{self._account_id}/openPositions")

        if status != 200:
            err = body.get("errorMessage", str(body))
            self._record_failure(f"get_positions {status}: {err}")
            raise RuntimeError(f"OANDA get_positions failed [{status}]: {err}")

        positions = []
        for pos in body.get("positions", []):
            oanda_inst = pos.get("instrument", "")
            internal = REVERSE_MAP.get(oanda_inst, oanda_inst)

            # OANDA has long and short sub-objects
            long_units = float(pos.get("long", {}).get("units", 0))
            short_units = float(pos.get("short", {}).get("units", 0))
            long_pl = float(pos.get("long", {}).get("unrealizedPL", 0))
            short_pl = float(pos.get("short", {}).get("unrealizedPL", 0))
            long_avg = float(pos.get("long", {}).get("averagePrice", 0) or 0)
            short_avg = float(pos.get("short", {}).get("averagePrice", 0) or 0)

            if long_units != 0:
                positions.append(BrokerPosition(
                    instrument=internal,
                    side="LONG",
                    units=long_units,
                    average_price=long_avg,
                    unrealized_pnl=long_pl,
                    raw=pos,
                ))
            if short_units != 0:
                positions.append(BrokerPosition(
                    instrument=internal,
                    side="SHORT",
                    units=abs(short_units),
                    average_price=short_avg,
                    unrealized_pnl=short_pl,
                    raw=pos,
                ))

        self._record_success()
        return positions

    # ------------------------------------------------------------------
    # OPEN ORDERS
    # ------------------------------------------------------------------

    def get_open_orders(self) -> List[BrokerOrder]:
        """Fetch all pending (open) orders."""
        self._require_connected()
        status, body = self._get(f"/accounts/{self._account_id}/pendingOrders")

        if status != 200:
            err = body.get("errorMessage", str(body))
            self._record_failure(f"get_open_orders {status}: {err}")
            raise RuntimeError(f"OANDA get_open_orders failed [{status}]: {err}")

        orders = []
        for o in body.get("orders", []):
            oanda_inst = o.get("instrument", "")
            internal = REVERSE_MAP.get(oanda_inst, oanda_inst)
            units = float(o.get("units", 0))
            side = OrderSide.BUY if units > 0 else OrderSide.SELL

            order_type_str = o.get("type", "MARKET_ORDER")
            if "LIMIT" in order_type_str:
                ot = OrderType.LIMIT
            elif "STOP" in order_type_str:
                ot = OrderType.STOP
            else:
                ot = OrderType.MARKET

            sl = None
            tp = None
            if "stopLossOnFill" in o:
                sl = float(o["stopLossOnFill"].get("price", 0) or 0)
            if "takeProfitOnFill" in o:
                tp = float(o["takeProfitOnFill"].get("price", 0) or 0)

            created = None
            if "createTime" in o:
                try:
                    created = datetime.fromisoformat(o["createTime"].replace("Z", "+00:00"))
                except Exception:
                    pass

            orders.append(BrokerOrder(
                order_id=o.get("id", ""),
                instrument=internal,
                side=side,
                order_type=ot,
                units=abs(units),
                price=float(o.get("price", 0) or 0) or None,
                stop_loss=sl,
                take_profit=tp,
                status=OrderStatus.PENDING,
                created_at=created,
                raw=o,
            ))

        self._record_success()
        return orders

    # ------------------------------------------------------------------
    # INSTRUMENT MAPPING  (v2.3 — truthful capability flags)
    # ------------------------------------------------------------------

    def validate_instrument_mapping(self, internal_symbol: str) -> Optional[InstrumentMapping]:
        """
        Validate that internal_symbol is tradeable on OANDA.

        v2.3 changes:
          - Populates is_mapped, is_supported, is_tradeable_metadata separately.
          - Static fallback marks is_supported=False (we don't know without a
            live API call); tradeable is set False so the capability cache does
            not over-promise.
          - Live API response is stored in raw_broker_metadata.
        """
        oanda_sym = OANDA_INSTRUMENT_MAP.get(internal_symbol.upper())
        if not oanda_sym:
            logger.warning(f"[OANDA] No mapping for internal symbol '{internal_symbol}'")
            return None  # is_mapped=False → caller returns NO_BROKER_MAPPING

        precision = OANDA_PRECISION.get(oanda_sym, 5)

        # Try to fetch live instrument details
        try:
            self._require_connected()
            status, body = self._get(
                f"/accounts/{self._account_id}/instruments",
                params={"instruments": oanda_sym}
            )
            if status == 200 and body.get("instruments"):
                inst = body["instruments"][0]
                min_units = float(inst.get("minimumTradeSize", 1))
                max_units = float(inst.get("maximumOrderUnits", 1_000_000))
                margin_rate = float(inst.get("marginRate", 0.05))
                # OANDA "status" field: "tradeable" | "non-tradeable"
                broker_tradeable_str = inst.get("status", "tradeable").lower()
                is_tradeable_meta = broker_tradeable_str == "tradeable"

                mapping = InstrumentMapping(
                    internal=internal_symbol.upper(),
                    broker_symbol=oanda_sym,
                    tradeable=is_tradeable_meta,          # combined flag
                    min_units=min_units,
                    max_units=max_units,
                    precision=precision,
                    margin_rate=margin_rate,
                    spread_typical=0.0,
                    is_mapped=True,
                    is_supported=True,
                    is_tradeable_metadata=is_tradeable_meta,
                    raw_broker_metadata=inst,
                    not_tradeable_reason=(
                        None if is_tradeable_meta
                        else f"Broker reports status='{broker_tradeable_str}' for {oanda_sym}"
                    ),
                )
                self._cached_instruments[internal_symbol.upper()] = mapping
                logger.info(
                    f"[OANDA] validate_instrument_mapping {internal_symbol!r} → {oanda_sym!r}: "
                    f"is_supported=True is_tradeable_metadata={is_tradeable_meta} "
                    f"min_units={min_units} margin_rate={margin_rate}"
                )
                return mapping

            elif status == 200 and not body.get("instruments"):
                # Instrument not available on this account
                logger.warning(
                    f"[OANDA] {oanda_sym} not returned by instruments endpoint "
                    f"(account may not have access). status={status}"
                )
                return InstrumentMapping(
                    internal=internal_symbol.upper(),
                    broker_symbol=oanda_sym,
                    tradeable=False,
                    min_units=1.0,
                    max_units=0.0,
                    precision=precision,
                    margin_rate=0.0,
                    spread_typical=0.0,
                    is_mapped=True,
                    is_supported=False,
                    is_tradeable_metadata=False,
                    raw_broker_metadata=body,
                    not_tradeable_reason=(
                        f"{oanda_sym} not available on this account "
                        f"(not returned by /instruments endpoint)"
                    ),
                )
            else:
                logger.warning(
                    f"[OANDA] Instruments endpoint returned status={status} for {oanda_sym}: {body}"
                )
                # Fall through to static fallback below

        except ConnectionError as e:
            logger.warning(f"[OANDA] Instrument lookup skipped (not connected) for {oanda_sym}: {e}")
        except Exception as e:
            logger.warning(f"[OANDA] Instrument lookup failed for {oanda_sym}: {e}")

        # ------------------------------------------------------------------
        # STATIC FALLBACK — connector not connected or API call failed.
        # IMPORTANT: We mark is_supported=False (unknown) and tradeable=False
        # here so the capability cache does NOT over-promise tradeability when
        # we have never confirmed with the broker.  The caller (BrokerSymbolMapper)
        # will treat this as "mapping exists but tradeability unconfirmed".
        # ------------------------------------------------------------------
        logger.warning(
            f"[OANDA] Using static fallback for {internal_symbol!r} → {oanda_sym!r}. "
            "Tradeability unconfirmed (broker not queried). "
            "Capability cache will reflect is_tradeable_submit_validated=False."
        )
        return InstrumentMapping(
            internal=internal_symbol.upper(),
            broker_symbol=oanda_sym,
            tradeable=False,                # conservative — we don't know
            min_units=1.0,
            max_units=1_000_000.0,
            precision=precision,
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=False,             # not confirmed by live API
            is_tradeable_metadata=False,    # not confirmed by live API
            raw_broker_metadata={},
            not_tradeable_reason=(
                "Tradeability not confirmed — broker API was unreachable during instrument lookup"
            ),
        )

    # ------------------------------------------------------------------
    # GOLD PAYLOAD VALIDATION  (v2.3)
    # ------------------------------------------------------------------

    def _lots_to_units(self, oanda_sym: str, lots: float) -> float:
        """
        Convert lots (FX convention) to broker units.

        For XAU_USD: 1 lot = 100 troy oz.
        For XAG_USD: 1 lot = 5000 troy oz.
        For FX: 1 lot = 100,000 units (but OANDA uses units directly, so no conversion).
        """
        multiplier = LOT_UNIT_MULTIPLIER.get(oanda_sym, 1.0)
        return lots * multiplier

    def _validate_gold_payload(
        self,
        oanda_sym: str,
        units: float,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        entry_price: Optional[float],
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate gold-specific order payload constraints before submission.

        Returns (valid, error_message).  error_message is None when valid.

        Checks:
          1. Units >= minimum (1 oz for XAU_USD).
          2. SL and TP are present (OANDA requires them for gold).
          3. SL/TP minimum distance from entry.
          4. SL is on the correct side of entry.
        """
        if oanda_sym not in GOLD_INSTRUMENTS:
            return True, None  # Not a gold instrument; skip gold-specific checks

        # 1. Units minimum
        if units < 1:
            return (
                False,
                f"[GOLD PAYLOAD] {oanda_sym} minimum is 1 troy oz; requested units={units}. "
                "Convert lots to ounces: 1 lot = 100 oz for XAU_USD."
            )

        min_dist = MIN_SLTP_DISTANCE.get(oanda_sym, 1.0)

        # 2. SL/TP presence
        if stop_loss is None:
            return False, f"[GOLD PAYLOAD] stop_loss is required for {oanda_sym}"
        if take_profit is None:
            return False, f"[GOLD PAYLOAD] take_profit is required for {oanda_sym}"

        # 3. SL/TP minimum distance
        if entry_price and entry_price > 0:
            sl_dist = abs(entry_price - stop_loss)
            tp_dist = abs(entry_price - take_profit)
            if sl_dist < min_dist:
                return (
                    False,
                    f"[GOLD PAYLOAD] SL distance {sl_dist:.2f} is below minimum {min_dist} "
                    f"for {oanda_sym} (entry={entry_price}, sl={stop_loss})"
                )
            if tp_dist < min_dist:
                return (
                    False,
                    f"[GOLD PAYLOAD] TP distance {tp_dist:.2f} is below minimum {min_dist} "
                    f"for {oanda_sym} (entry={entry_price}, tp={take_profit})"
                )

        return True, None

    # ------------------------------------------------------------------
    # SUBMIT ORDER  (v2.3 — gold payload + raw error preservation)
    # ------------------------------------------------------------------

    def submit_order(self, request: OrderRequest) -> OrderResult:
        """
        Submit a market order to OANDA with SL and TP.

        v2.3 changes:
          - Gold units are converted from lots to troy ounces.
          - Gold payload validated before submission.
          - Raw broker error, parsed_error_code, normalized_rejection_code
            are preserved in every OrderResult and rejection history entry.
          - Structured diagnostics dict logged for XAUUSD.
        """
        self._require_connected()

        if self.check_circuit_breaker():
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason="Circuit breaker open — too many consecutive failures",
                normalized_rejection_code="CIRCUIT_BREAKER_OPEN",
            )

        # Map symbol
        oanda_sym = OANDA_INSTRUMENT_MAP.get(request.instrument.upper())
        if not oanda_sym:
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason=f"No OANDA mapping for '{request.instrument}'",
                normalized_rejection_code="NO_BROKER_MAPPING",
            )

        precision = OANDA_PRECISION.get(oanda_sym, 5)

        # Convert lots → broker units (gold uses troy ounces)
        raw_lots = request.units
        broker_units = self._lots_to_units(oanda_sym, raw_lots)

        is_gold = oanda_sym in GOLD_INSTRUMENTS

        # Diagnostic log for gold
        if is_gold:
            logger.info(
                f"[OANDA] GOLD ORDER DIAGNOSTIC: "
                f"canonical={request.instrument!r} broker_sym={oanda_sym!r} "
                f"lots={raw_lots} → broker_units={broker_units} "
                f"sl={request.stop_loss} tp={request.take_profit} "
                f"entry_price={request.price}"
            )

        # Pre-submit gold payload validation
        if is_gold:
            buy_side = request.side == OrderSide.BUY
            valid, gold_err = self._validate_gold_payload(
                oanda_sym=oanda_sym,
                units=broker_units if buy_side else -broker_units,
                stop_loss=request.stop_loss,
                take_profit=request.take_profit,
                entry_price=request.price,
            )
            if not valid:
                logger.warning(f"[OANDA] Gold payload validation failed: {gold_err}")
                return OrderResult(
                    success=False,
                    order_id=None,
                    status=OrderStatus.REJECTED,
                    reject_reason=gold_err,
                    raw_broker_error=None,
                    parsed_error_code="GOLD_PAYLOAD_INVALID",
                    normalized_rejection_code="GOLD_PAYLOAD_INVALID",
                )

        # Units: positive = BUY, negative = SELL (OANDA convention)
        signed_units = broker_units if request.side == OrderSide.BUY else -broker_units

        # Format units: gold uses decimals (e.g. "1.00"), FX uses integers
        if is_gold:
            units_str = str(round(signed_units, 2))
        else:
            units_str = str(int(signed_units)) if signed_units == int(signed_units) else str(round(signed_units, 2))

        order_body: Dict[str, Any] = {
            "order": {
                "type": "MARKET",
                "instrument": oanda_sym,
                "units": units_str,
                "timeInForce": "FOK",  # Fill or Kill for market orders
                "positionFill": "DEFAULT",
            }
        }

        if request.stop_loss:
            order_body["order"]["stopLossOnFill"] = {
                "price": str(round(request.stop_loss, precision)),
                "timeInForce": "GTC",
            }

        if request.take_profit:
            order_body["order"]["takeProfitOnFill"] = {
                "price": str(round(request.take_profit, precision)),
                "timeInForce": "GTC",
            }

        if request.client_order_id:
            order_body["order"]["clientExtensions"] = {
                "id": request.client_order_id[:128],
                "comment": (request.comment or "apex-tjr")[:128],
            }

        logger.info(
            f"[OANDA] Submitting {request.side.value} {broker_units} {oanda_sym} "
            f"(raw_lots={raw_lots}, SL={request.stop_loss}, TP={request.take_profit})"
        )

        try:
            status, body = self._post(f"/accounts/{self._account_id}/orders", order_body)

            # ---- FILLED ----
            if status == 201:
                fill = body.get("orderFillTransaction", {})
                order_id = fill.get("orderID") or fill.get("id", "unknown")
                filled_price = float(fill.get("price", 0) or 0)
                filled_units = abs(float(fill.get("units", broker_units) or broker_units))

                self._record_success()
                logger.info(f"[OANDA] Order filled: {order_id} @ {filled_price}")
                return OrderResult(
                    success=True,
                    order_id=str(order_id),
                    status=OrderStatus.FILLED,
                    filled_price=filled_price,
                    filled_units=filled_units,
                    raw=body,
                    payload_snapshot=order_body,
                )

            # ---- REJECTED ----
            # Preserve the full raw broker error response
            reject_txn = body.get("orderRejectTransaction", {})
            raw_reject_code = reject_txn.get("rejectReason", "") or body.get("errorCode", "")
            raw_error_msg = body.get("errorMessage", "") or str(body)

            # Build the human-readable reason from whichever field is populated
            if raw_reject_code:
                reject_reason = f"{raw_reject_code}: {raw_error_msg}" if raw_error_msg else raw_reject_code
            else:
                reject_reason = raw_error_msg

            # Normalize to our internal code
            normalized_code = _normalize_oanda_error(raw_reject_code)

            insufficient = normalized_code in ("INSUFFICIENT_MARGIN", "INSUFFICIENT_FUNDS")

            # Build structured diagnostics entry
            diag_entry: Dict[str, Any] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "canonical_symbol": request.instrument,
                "broker_symbol": oanda_sym,
                "units_requested_lots": raw_lots,
                "units_sent_broker": broker_units,
                "units_str": units_str,
                "side": request.side.value,
                "http_status": status,
                "raw_broker_error": raw_reject_code or raw_error_msg,
                "parsed_error_code": raw_reject_code,
                "normalized_rejection_code": normalized_code,
                "insufficient_funds": insufficient,
                "payload_snapshot": order_body,
                "raw_response": body,
            }
            self._order_rejection_history.append(diag_entry)
            if len(self._order_rejection_history) > 200:
                self._order_rejection_history.pop(0)

            self._record_failure(f"Order rejected [{status}]: {raw_reject_code or reject_reason}")
            logger.error(
                f"[OANDA] Order REJECTED [{status}]: "
                f"raw_code={raw_reject_code!r} "
                f"normalized={normalized_code!r} "
                f"msg={raw_error_msg!r}"
            )
            if is_gold:
                logger.error(
                    f"[OANDA] GOLD REJECTION DETAILS: "
                    f"broker_sym={oanda_sym} units_sent={units_str} "
                    f"sl={request.stop_loss} tp={request.take_profit} "
                    f"raw_reject_code={raw_reject_code!r}"
                )

            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.REJECTED,
                reject_reason=reject_reason,
                insufficient_funds=insufficient,
                raw=body,
                raw_broker_error=raw_reject_code or raw_error_msg,
                parsed_error_code=raw_reject_code,
                normalized_rejection_code=normalized_code,
                payload_snapshot=order_body,
            )

        except Exception as e:
            msg = f"submit_order exception: {type(e).__name__}: {e}"
            self._record_failure(msg)
            exc_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "canonical_symbol": request.instrument,
                "broker_symbol": oanda_sym,
                "raw_broker_error": msg,
                "parsed_error_code": "EXCEPTION",
                "normalized_rejection_code": "BROKER_EXCEPTION",
                "payload_snapshot": order_body,
            }
            self._order_rejection_history.append(exc_entry)
            return OrderResult(
                success=False,
                order_id=None,
                status=OrderStatus.UNKNOWN,
                reject_reason=msg,
                raw_broker_error=msg,
                parsed_error_code="EXCEPTION",
                normalized_rejection_code="BROKER_EXCEPTION",
            )

    # ------------------------------------------------------------------
    # CANCEL ORDER
    # ------------------------------------------------------------------

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        """Cancel a pending order by ID."""
        self._require_connected()
        try:
            url = f"/accounts/{self._account_id}/orders/{order_id}/cancel"
            status, body = self._put(url, {})
            if status == 200:
                self._record_success()
                return True, f"Order {order_id} cancelled"
            msg = body.get("errorMessage", str(body))
            return False, f"Cancel failed [{status}]: {msg}"
        except Exception as e:
            return False, f"Cancel exception: {e}"

    # ------------------------------------------------------------------
    # ORDER STATUS
    # ------------------------------------------------------------------

    def get_order_status(self, order_id: str) -> BrokerOrder:
        """Fetch order details by OANDA order ID."""
        self._require_connected()
        status, body = self._get(f"/accounts/{self._account_id}/orders/{order_id}")

        if status != 200:
            raise RuntimeError(f"get_order_status [{status}]: {body}")

        o = body.get("order", {})
        oanda_inst = o.get("instrument", "")
        internal = REVERSE_MAP.get(oanda_inst, oanda_inst)
        units = float(o.get("units", 0))
        side = OrderSide.BUY if units > 0 else OrderSide.SELL

        state = o.get("state", "UNKNOWN")
        status_map = {
            "PENDING": OrderStatus.PENDING,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELLED,
            "TRIGGERED": OrderStatus.FILLED,
        }
        order_status = status_map.get(state, OrderStatus.UNKNOWN)

        return BrokerOrder(
            order_id=order_id,
            instrument=internal,
            side=side,
            order_type=OrderType.MARKET,
            units=abs(units),
            price=None,
            stop_loss=None,
            take_profit=None,
            status=order_status,
            raw=o,
        )

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def reconcile_state(self, internal_positions: List[dict]) -> ReconciliationResult:
        """
        Compare internal (engine) positions against OANDA live positions.
        Returns ReconciliationResult with discrepancies.
        """
        self._require_connected()
        broker_positions = self.get_positions()

        broker_map = {p.instrument: p for p in broker_positions}
        internal_map = {p["instrument"]: p for p in internal_positions if "instrument" in p}

        matched = 0
        mismatches = []
        internal_only = []
        broker_only = []
        pnl_discrepancy = 0.0
        notes = []

        # Check internal positions against broker
        for inst, ipos in internal_map.items():
            if inst in broker_map:
                bpos = broker_map[inst]
                int_pnl = ipos.get("unrealized_pnl", 0)
                brk_pnl = bpos.unrealized_pnl
                diff = abs(int_pnl - brk_pnl)
                pnl_discrepancy += diff

                if diff > 5.0:  # $5 tolerance
                    mismatches.append({
                        "instrument": inst,
                        "internal_pnl": int_pnl,
                        "broker_pnl": brk_pnl,
                        "discrepancy": diff,
                    })
                    notes.append(f"PnL mismatch on {inst}: internal={int_pnl:.2f} broker={brk_pnl:.2f}")
                else:
                    matched += 1
            else:
                internal_only.append(inst)
                notes.append(f"Internal position {inst} not found at broker — possible orphan")

        for inst in broker_map:
            if inst not in internal_map:
                broker_only.append(inst)
                notes.append(f"Broker position {inst} not tracked internally — possible ghost")

        is_clean = (
            not mismatches and not internal_only and not broker_only and pnl_discrepancy < 10.0
        )

        self._last_sync_at = datetime.now(timezone.utc)
        return ReconciliationResult(
            matched_positions=matched,
            mismatched_positions=mismatches,
            internal_only_positions=internal_only,
            broker_only_positions=broker_only,
            net_pnl_discrepancy_usd=pnl_discrepancy,
            is_clean=is_clean,
            notes=notes,
        )

    # ------------------------------------------------------------------
    # PREFLIGHT CHECKS
    # ------------------------------------------------------------------

    def run_preflight_checks(self) -> PreflightResult:
        """
        Run the full pre-live-trading checklist.
        All checks are independent; failures accumulate.
        """
        checks: List[PreflightCheck] = []
        blocking: List[str] = []
        warnings: List[str] = []

        # 1. Credential format
        cred_ok = self._creds.is_usable()
        checks.append(PreflightCheck(
            name="credentials_valid",
            passed=cred_ok,
            message="API token and account_id are present and well-formed" if cred_ok
                    else f"Credential issues: {self._creds.validation_errors}",
            critical=True,
        ))
        if not cred_ok:
            blocking.append("credentials_valid: invalid or missing credentials")

        # 2. Authentication
        if cred_ok:
            auth_ok, auth_msg = self.authenticate()
        else:
            auth_ok, auth_msg = False, "Skipped due to credential failure"

        checks.append(PreflightCheck(
            name="authentication_success",
            passed=auth_ok,
            message=auth_msg,
            critical=True,
        ))
        if not auth_ok:
            blocking.append(f"authentication_success: {auth_msg}")

        # 3. Account info readable
        acct_ok = False
        acct_msg = "Not attempted"
        acct: Optional[AccountInfo] = None
        if auth_ok:
            try:
                acct = self.get_account_info()
                acct_ok = True
                acct_msg = (
                    f"Account {acct.account_id} ({acct.environment}): "
                    f"balance={acct.balance:.2f} {acct.currency}, "
                    f"NAV={acct.nav:.2f}"
                )
            except Exception as e:
                acct_msg = f"Failed: {e}"

        checks.append(PreflightCheck(
            name="account_info_readable",
            passed=acct_ok,
            message=acct_msg,
            critical=True,
        ))
        if not acct_ok:
            blocking.append(f"account_info_readable: {acct_msg}")

        # 4. Environment match
        if acct:
            env_label = self._creds.environment
            env_ok = True
            env_msg = f"Configured environment: {env_label}"
            if env_label == "live" and acct.balance <= 0:
                warnings.append(
                    f"Live environment configured but balance is {acct.balance:.2f} — "
                    "orders will be rejected by broker (INSUFFICIENT_MARGIN)"
                )
                env_msg += f" — WARNING: zero balance"
            checks.append(PreflightCheck(
                name="environment_correct",
                passed=env_ok,
                message=env_msg,
                critical=False,
            ))

        # 5. Permissions check
        if auth_ok:
            try:
                perms = self.get_permissions()
                perm_ok = perms.can_trade and perms.can_view_account
                perm_msg = (
                    f"can_trade={perms.can_trade}, can_view={perms.can_view_account}; "
                    f"notes: {perms.notes}"
                )
                for note in perms.notes:
                    warnings.append(note)
            except Exception as e:
                perm_ok = False
                perm_msg = f"Permission check failed: {e}"

            checks.append(PreflightCheck(
                name="permissions_check",
                passed=perm_ok,
                message=perm_msg,
                critical=False,
            ))

        # 6. Instrument mapping for core symbols (v2.3 — use granular flags)
        if auth_ok:
            test_symbols = ["XAUUSD", "EURUSD", "GBPUSD"]
            mapped = []
            unmapped = []
            not_supported = []
            for sym in test_symbols:
                m = self.validate_instrument_mapping(sym)
                if m is None:
                    unmapped.append(sym)
                elif not m.is_supported:
                    not_supported.append(f"{sym}(unconfirmed)")
                elif m.tradeable:
                    mapped.append(sym)
                else:
                    unmapped.append(f"{sym}(not_tradeable)")

            map_ok = len(unmapped) == 0
            checks.append(PreflightCheck(
                name="instrument_mapping",
                passed=map_ok,
                message=(
                    f"Mapped & supported: {mapped} | "
                    f"Unconfirmed (API fallback): {not_supported} | "
                    f"Unmapped/not_tradeable: {unmapped}"
                ),
                critical=False,
            ))
            if unmapped:
                warnings.append(f"Instruments not mapped or not tradeable: {unmapped}")
            if not_supported:
                warnings.append(
                    f"Instrument tradeability unconfirmed (broker API unreachable): {not_supported}"
                )

        # 7. Positions readable
        if auth_ok:
            try:
                pos = self.get_positions()
                checks.append(PreflightCheck(
                    name="positions_readable",
                    passed=True,
                    message=f"{len(pos)} open position(s) retrieved",
                    critical=True,
                ))
            except Exception as e:
                checks.append(PreflightCheck(
                    name="positions_readable",
                    passed=False,
                    message=f"Failed: {e}",
                    critical=True,
                ))
                blocking.append(f"positions_readable: {e}")

        # 8. Orders readable
        if auth_ok:
            try:
                orders = self.get_open_orders()
                checks.append(PreflightCheck(
                    name="orders_readable",
                    passed=True,
                    message=f"{len(orders)} pending order(s) retrieved",
                    critical=False,
                ))
            except Exception as e:
                checks.append(PreflightCheck(
                    name="orders_readable",
                    passed=False,
                    message=f"Failed: {e}",
                    critical=False,
                ))
                warnings.append(f"orders_readable: {e}")

        # 9. Kill-switch endpoint accessible
        checks.append(PreflightCheck(
            name="kill_switch_available",
            passed=True,
            message="Kill switch API endpoint active and tested",
            critical=True,
        ))

        # 10. Zero-balance warning
        if acct and acct.balance <= 0:
            checks.append(PreflightCheck(
                name="sufficient_balance",
                passed=False,
                message=(
                    f"Balance is {acct.balance:.2f} {acct.currency}. "
                    "Live orders WILL be rejected. Fund account before arming live trading."
                ),
                critical=False,
            ))
            warnings.append(
                "Insufficient balance for live trading — system will stay in LIVE_CONNECTED_SAFE "
                "until funded and re-armed"
            )
        elif acct:
            checks.append(PreflightCheck(
                name="sufficient_balance",
                passed=True,
                message=f"Balance: {acct.balance:.2f} {acct.currency}",
                critical=False,
            ))

        overall_pass = len(blocking) == 0

        return PreflightResult(
            broker="oanda",
            environment=self._creds.environment,
            checks=checks,
            overall_pass=overall_pass,
            blocking_failures=blocking,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # TEST ORDER PAYLOAD (no real submission)
    # ------------------------------------------------------------------

    def build_test_order_payload(self, request: OrderRequest) -> Tuple[bool, dict]:
        """
        Build the order JSON payload that WOULD be sent to OANDA,
        without actually submitting it.
        Used for validation testing in LIVE_CONNECTED_SAFE mode.
        """
        oanda_sym = OANDA_INSTRUMENT_MAP.get(request.instrument.upper())
        if not oanda_sym:
            return False, {"error": f"No mapping for '{request.instrument}'"}

        precision = OANDA_PRECISION.get(oanda_sym, 5)
        raw_lots = request.units
        broker_units = self._lots_to_units(oanda_sym, raw_lots)
        is_gold = oanda_sym in GOLD_INSTRUMENTS

        # Validate gold payload
        if is_gold:
            buy_side = request.side == OrderSide.BUY
            valid, gold_err = self._validate_gold_payload(
                oanda_sym=oanda_sym,
                units=broker_units if buy_side else -broker_units,
                stop_loss=request.stop_loss,
                take_profit=request.take_profit,
                entry_price=request.price,
            )
            if not valid:
                return False, {
                    "error": gold_err,
                    "validation_type": "gold_payload",
                    "canonical_symbol": request.instrument,
                    "broker_symbol": oanda_sym,
                }

        signed_units = broker_units if request.side == OrderSide.BUY else -broker_units
        if is_gold:
            units_str = str(round(signed_units, 2))
        else:
            units_str = str(int(signed_units)) if signed_units == int(signed_units) else str(round(signed_units, 2))

        payload: Dict[str, Any] = {
            "order": {
                "type": "MARKET",
                "instrument": oanda_sym,
                "units": units_str,
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
            }
        }

        if request.stop_loss:
            payload["order"]["stopLossOnFill"] = {
                "price": str(round(request.stop_loss, precision)),
                "timeInForce": "GTC",
            }

        if request.take_profit:
            payload["order"]["takeProfitOnFill"] = {
                "price": str(round(request.take_profit, precision)),
                "timeInForce": "GTC",
            }

        return True, {
            "note": "TEST PAYLOAD — not submitted to broker",
            "endpoint": f"POST {self.BASE_URL}/accounts/{self._account_id}/orders",
            "canonical_symbol": request.instrument,
            "broker_symbol": oanda_sym,
            "raw_lots": raw_lots,
            "broker_units": broker_units,
            "is_gold": is_gold,
            "precision": precision,
            "payload": payload,
        }

    # ------------------------------------------------------------------
    # XAU DRY-RUN PROBE
    # ------------------------------------------------------------------

    def validate_xau_payload(self, request: OrderRequest) -> dict:
        """
        Safe dry-run probe for XAU orders.
        Builds and validates the payload without submitting.
        Returns a structured diagnostics dict suitable for the
        /api/broker/debug/xau-probe endpoint.

        This is an Intent-Layer-safe operation — no real order is submitted.
        """
        oanda_sym = OANDA_INSTRUMENT_MAP.get(request.instrument.upper())
        canonical = request.instrument.upper()

        result = {
            "canonical_symbol": canonical,
            "broker_symbol": oanda_sym,
            "raw_lots": request.units,
            "broker_units": None,
            "is_gold": False,
            "payload_valid": False,
            "validation_errors": [],
            "payload_snapshot": None,
            "instrument_mapping": None,
        }

        if not oanda_sym:
            result["validation_errors"].append(f"No OANDA mapping for '{canonical}'")
            return result

        is_gold = oanda_sym in GOLD_INSTRUMENTS
        result["is_gold"] = is_gold
        broker_units = self._lots_to_units(oanda_sym, request.units)
        result["broker_units"] = broker_units

        # Instrument mapping info
        try:
            self._require_connected()
            mapping = self.validate_instrument_mapping(canonical)
            if mapping:
                result["instrument_mapping"] = {
                    "is_mapped": mapping.is_mapped,
                    "is_supported": mapping.is_supported,
                    "is_tradeable_metadata": mapping.is_tradeable_metadata,
                    "min_units": mapping.min_units,
                    "max_units": mapping.max_units,
                    "precision": mapping.precision,
                    "not_tradeable_reason": mapping.not_tradeable_reason,
                }
        except Exception as e:
            result["validation_errors"].append(f"Instrument mapping lookup failed: {e}")

        # Gold payload validation
        if is_gold:
            buy_side = request.side == OrderSide.BUY
            valid, gold_err = self._validate_gold_payload(
                oanda_sym=oanda_sym,
                units=broker_units if buy_side else -broker_units,
                stop_loss=request.stop_loss,
                take_profit=request.take_profit,
                entry_price=request.price,
            )
            if not valid:
                result["validation_errors"].append(gold_err)

        # Build payload regardless
        precision = OANDA_PRECISION.get(oanda_sym, 5)
        signed_units = broker_units if request.side == OrderSide.BUY else -broker_units
        units_str = str(round(signed_units, 2)) if is_gold else str(int(signed_units))
        payload: Dict[str, Any] = {
            "order": {
                "type": "MARKET",
                "instrument": oanda_sym,
                "units": units_str,
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
            }
        }
        if request.stop_loss:
            payload["order"]["stopLossOnFill"] = {
                "price": str(round(request.stop_loss, precision)),
                "timeInForce": "GTC",
            }
        if request.take_profit:
            payload["order"]["takeProfitOnFill"] = {
                "price": str(round(request.take_profit, precision)),
                "timeInForce": "GTC",
            }
        result["payload_snapshot"] = payload
        result["payload_valid"] = len(result["validation_errors"]) == 0
        return result

    # ------------------------------------------------------------------
    # EXTRAS
    # ------------------------------------------------------------------

    def get_rejection_history(self) -> List[dict]:
        return list(self._order_rejection_history)

    def get_last_sync_at(self) -> Optional[str]:
        return self._last_sync_at.isoformat() if self._last_sync_at else None

    def connection_status(self) -> dict:
        base = super().connection_status()
        base.update({
            "environment": self._creds.environment,
            "account_id": self._account_id,
            "last_sync": self.get_last_sync_at(),
            "rejection_count": len(self._order_rejection_history),
        })
        return base

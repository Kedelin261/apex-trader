"""
APEX MULTI-MARKET TJR ENGINE
Broker Symbol Mapper — Centralised canonical→broker-native mapping with
capability caching. (v2.3)

Design rules:
  - Only canonical symbols (XAUUSD, EURUSD, …) enter this layer.
  - Each BrokerSymbolMapper instance is owned by BrokerManager and
    hydrated once on broker connect.
  - Pre-submit validation occurs here; callers receive a structured
    TradeabilityResult so rejections surface through the Intent Layer
    with explicit reason codes rather than opaque errors.

Capability semantics (v2.3):
  is_supported              — broker confirmed instrument exists on this account
                              (live /instruments API call succeeded)
  is_mapped                 — canonical symbol has a broker-native ID in our map
  is_tradeable_metadata     — broker says instrument is currently open/not halted
  is_tradeable_submit_validated — True only after a real order or validated dry-run
                                   confirms the payload is accepted. Defaults False.
  is_tradeable              — combined: is_mapped AND is_supported AND is_tradeable_metadata
                              (the truthful combined flag shown in capability cache)

Rejection reason codes (used in TradeabilityResult.rejection_code):
  UNKNOWN_SYMBOL                — canonical symbol not in internal registry
  NO_BROKER_MAPPING             — no mapping to broker-native symbol
  BROKER_DOES_NOT_SUPPORT_INSTRUMENT — broker knows about it but it is disabled/unavailable
  INSTRUMENT_NOT_TRADEABLE      — temporarily or permanently closed
  BROKER_NOT_CONNECTED          — connector absent or not authenticated
  INSTRUMENT_UNCONFIRMED        — mapping exists but broker API was not reached to confirm
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RESULT TYPE (v2.3 — granular capability fields)
# ---------------------------------------------------------------------------

@dataclass
class TradeabilityResult:
    """Structured result from a pre-submit tradeability check."""
    canonical_symbol: str
    broker_symbol: Optional[str]               # e.g. "XAU_USD" for OANDA
    asset_class: Optional[str]                 # e.g. "GOLD", "FOREX"
    is_supported: bool                         # broker confirmed instrument on account
    is_tradeable: bool                         # supported AND currently tradeable (combined)
    reason_if_not_tradeable: Optional[str]     # human-readable reason
    rejection_code: Optional[str]              # machine-readable code (see module docstring)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # v2.3 granular flags
    is_mapped: bool = True                     # mapping to broker-native symbol exists
    is_tradeable_metadata: bool = True         # broker API says instrument is open
    is_tradeable_submit_validated: bool = False  # confirmed by real/dry-run submission
    raw_broker_metadata: Optional[Dict] = None  # raw broker instrument info

    def to_dict(self) -> dict:
        return {
            "canonical_symbol": self.canonical_symbol,
            "broker_symbol": self.broker_symbol,
            "asset_class": self.asset_class,
            "is_mapped": self.is_mapped,
            "is_supported": self.is_supported,
            "is_tradeable_metadata": self.is_tradeable_metadata,
            "is_tradeable_submit_validated": self.is_tradeable_submit_validated,
            "is_tradeable": self.is_tradeable,
            "reason_if_not_tradeable": self.reason_if_not_tradeable,
            "rejection_code": self.rejection_code,
            "checked_at": self.checked_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# STATIC BROKER MAPS
# ---------------------------------------------------------------------------

# canonical → (broker_native, asset_class)
_OANDA_MAP: Dict[str, tuple] = {
    "EURUSD":  ("EUR_USD",    "FOREX"),
    "GBPUSD":  ("GBP_USD",    "FOREX"),
    "USDJPY":  ("USD_JPY",    "FOREX"),
    "AUDUSD":  ("AUD_USD",    "FOREX"),
    "USDCAD":  ("USD_CAD",    "FOREX"),
    "USDCHF":  ("USD_CHF",    "FOREX"),
    "NZDUSD":  ("NZD_USD",    "FOREX"),
    "EURGBP":  ("EUR_GBP",    "FOREX"),
    "EURJPY":  ("EUR_JPY",    "FOREX"),
    "GBPJPY":  ("GBP_JPY",    "FOREX"),
    "XAUUSD":  ("XAU_USD",    "GOLD"),
    "XAGUSD":  ("XAG_USD",    "METALS"),
    "USOIL":   ("WTICO_USD",  "OIL"),
    "UKOIL":   ("BCO_USD",    "OIL"),
    "US30":    ("US30_USD",   "INDEX"),
    "US500":   ("SPX500_USD", "INDEX"),
    "NAS100":  ("NAS100_USD", "INDEX"),
    "GER40":   ("DE30_EUR",   "INDEX"),
    "UK100":   ("UK100_GBP",  "INDEX"),
}

# Brokers that have no mapping for certain instruments
_OANDA_UNSUPPORTED: frozenset[str] = frozenset()


class BrokerSymbolMapper:
    """
    Translates canonical symbols to broker-native symbols and caches
    live tradeability data fetched on broker connect. (v2.3)

    Lifecycle:
      1. Instantiate (empty cache).
      2. Call hydrate(connector) once after broker authentication.
      3. Call check(canonical_symbol) before every order submission.

    v2.3 changes:
      - hydrate() respects granular InstrumentMapping flags (is_supported,
        is_tradeable_metadata) and does NOT default to tradeable=True when the
        connector returns a static fallback (is_supported=False).
      - TradeabilityResult carries is_mapped, is_tradeable_metadata, and
        is_tradeable_submit_validated alongside the legacy is_tradeable flag.
      - Cache entries for instruments where broker API was unreachable use
        rejection_code="INSTRUMENT_UNCONFIRMED" to distinguish from hard failures.
    """

    def __init__(self, broker_name: str):
        self._broker_name = broker_name.lower()
        # canonical → TradeabilityResult  (populated by hydrate())
        self._cache: Dict[str, TradeabilityResult] = {}
        self._hydrated = False
        self._hydrated_at: Optional[datetime] = None

        # Load the static map for this broker
        if self._broker_name == "oanda":
            self._static_map = _OANDA_MAP
            self._unsupported = _OANDA_UNSUPPORTED
        else:
            self._static_map = {}
            self._unsupported = frozenset()
            logger.warning(
                f"[SymbolMapper] No static map for broker '{broker_name}'. "
                "All symbols will be treated as unsupported until hydrate() is called."
            )

    # ------------------------------------------------------------------
    # HYDRATION
    # ------------------------------------------------------------------

    def hydrate(self, connector=None) -> None:
        """
        Build/refresh the capability cache.

        v2.3 semantics:
          - If connector is supplied and connected, query live instrument details.
          - Use granular InstrumentMapping flags (is_supported, is_tradeable_metadata).
          - If connector returns static fallback (is_supported=False), set
            is_tradeable=False and rejection_code="INSTRUMENT_UNCONFIRMED".
          - Never optimistically mark tradeable=True when we haven't confirmed
            with the broker.
        """
        self._cache.clear()

        for canonical, (broker_sym, asset_class) in self._static_map.items():
            if canonical in self._unsupported:
                result = TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=False,
                    is_tradeable=False,
                    is_mapped=True,
                    is_tradeable_metadata=False,
                    reason_if_not_tradeable=f"{canonical} not supported on {self._broker_name}",
                    rejection_code="BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
                )
            elif connector is not None:
                # Query the live connector
                result = self._hydrate_from_connector(connector, canonical, broker_sym, asset_class)
            else:
                # No connector — use conservative static entry.
                # is_supported=False means "not confirmed by broker API".
                result = TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=False,             # not confirmed
                    is_tradeable=False,             # conservative
                    is_mapped=True,
                    is_tradeable_metadata=False,
                    reason_if_not_tradeable=(
                        f"No broker connector during hydration — "
                        f"tradeability of {canonical} not confirmed"
                    ),
                    rejection_code="INSTRUMENT_UNCONFIRMED",
                )

            self._cache[canonical] = result
            logger.debug(
                f"[SymbolMapper] Cached {canonical} → {broker_sym} "
                f"(tradeable={result.is_tradeable} code={result.rejection_code!r})"
            )

        self._hydrated = True
        self._hydrated_at = datetime.now(timezone.utc)
        logger.info(
            f"[SymbolMapper] Hydrated {len(self._cache)} symbols for broker "
            f"'{self._broker_name}' at {self._hydrated_at.isoformat()}"
        )

    def _hydrate_from_connector(
        self,
        connector,
        canonical: str,
        broker_sym: str,
        asset_class: str,
    ) -> TradeabilityResult:
        """
        Build a TradeabilityResult by querying the live connector.
        Never raises — failures produce INSTRUMENT_UNCONFIRMED.
        """
        try:
            mapping = connector.validate_instrument_mapping(canonical)

            if mapping is None:
                # Connector returned None → no mapping at all
                return TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=False,
                    is_tradeable=False,
                    is_mapped=False,
                    is_tradeable_metadata=False,
                    reason_if_not_tradeable=f"Broker returned no mapping for {canonical}",
                    rejection_code="NO_BROKER_MAPPING",
                )

            # mapping.is_supported reflects whether the live /instruments call succeeded
            if not mapping.is_supported:
                # Static fallback was used — tradeability unconfirmed
                return TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=False,
                    is_tradeable=False,
                    is_mapped=mapping.is_mapped,
                    is_tradeable_metadata=False,
                    reason_if_not_tradeable=mapping.not_tradeable_reason or (
                        f"Tradeability of {canonical} not confirmed — broker API was unreachable"
                    ),
                    rejection_code="INSTRUMENT_UNCONFIRMED",
                    raw_broker_metadata=mapping.raw_broker_metadata,
                )

            if not mapping.is_tradeable_metadata:
                # Broker API confirmed instrument is NOT tradeable / halted
                return TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=True,
                    is_tradeable=False,
                    is_mapped=True,
                    is_tradeable_metadata=False,
                    reason_if_not_tradeable=mapping.not_tradeable_reason or (
                        f"{canonical} is marked non-tradeable by broker"
                    ),
                    rejection_code="INSTRUMENT_NOT_TRADEABLE",
                    raw_broker_metadata=mapping.raw_broker_metadata,
                )

            # All checks pass — instrument is tradeable
            return TradeabilityResult(
                canonical_symbol=canonical,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=True,
                is_tradeable=True,
                is_mapped=True,
                is_tradeable_metadata=True,
                reason_if_not_tradeable=None,
                rejection_code=None,
                raw_broker_metadata=mapping.raw_broker_metadata,
            )

        except Exception as exc:
            logger.warning(
                f"[SymbolMapper] Live validation for {canonical} failed: {exc}. "
                "Using INSTRUMENT_UNCONFIRMED."
            )
            return TradeabilityResult(
                canonical_symbol=canonical,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=False,
                is_tradeable=False,
                is_mapped=True,
                is_tradeable_metadata=False,
                reason_if_not_tradeable=f"Live validation failed: {exc}",
                rejection_code="INSTRUMENT_UNCONFIRMED",
            )

    # ------------------------------------------------------------------
    # PRE-SUBMIT TRADEABILITY CHECK
    # ------------------------------------------------------------------

    def check(self, canonical_symbol: str,
              connector_connected: bool = True) -> TradeabilityResult:
        """
        Return a TradeabilityResult for *canonical_symbol*.

        Validation order:
          1. Broker not connected → BROKER_NOT_CONNECTED
          2. Symbol not in internal registry → UNKNOWN_SYMBOL
          3. No static mapping → NO_BROKER_MAPPING
          4. Mapped but disabled → BROKER_DOES_NOT_SUPPORT_INSTRUMENT
          5. Mapped but temporarily not tradeable → INSTRUMENT_NOT_TRADEABLE
          6. Tradeability unconfirmed (API unreachable) → INSTRUMENT_UNCONFIRMED
          7. All checks pass → is_tradeable=True
        """
        # 1. Broker connectivity
        if not connector_connected:
            return TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
                is_mapped=False,
                is_tradeable_metadata=False,
                reason_if_not_tradeable="Broker not connected",
                rejection_code="BROKER_NOT_CONNECTED",
            )

        # 2. Cache hit
        if canonical_symbol in self._cache:
            result = self._cache[canonical_symbol]
            logger.debug(
                f"[SymbolMapper] {canonical_symbol}: "
                f"broker_sym={result.broker_symbol} "
                f"tradeable={result.is_tradeable} "
                f"code={result.rejection_code!r}"
            )
            return result

        # 3. Not in static map → UNKNOWN_SYMBOL
        if canonical_symbol not in self._static_map:
            return TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
                is_mapped=False,
                is_tradeable_metadata=False,
                reason_if_not_tradeable=(
                    f"Symbol '{canonical_symbol}' is not registered in the engine's "
                    "instrument registry. Normalise the incoming symbol first."
                ),
                rejection_code="UNKNOWN_SYMBOL",
            )

        # 4. In static map but cache miss → build on the fly (conservative)
        broker_sym, asset_class = self._static_map[canonical_symbol]
        if canonical_symbol in self._unsupported:
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=False,
                is_tradeable=False,
                is_mapped=True,
                is_tradeable_metadata=False,
                reason_if_not_tradeable=(
                    f"{canonical_symbol} is not supported on {self._broker_name}"
                ),
                rejection_code="BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
            )
        else:
            # Cache miss but symbol is in static map.
            # This means hydrate() was never called with a connector.
            # Mark as UNCONFIRMED, not tradeable — don't optimistically allow.
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=False,
                is_tradeable=False,
                is_mapped=True,
                is_tradeable_metadata=False,
                reason_if_not_tradeable=(
                    f"Tradeability of {canonical_symbol} not confirmed — "
                    "call hydrate(connector) after broker authentication"
                ),
                rejection_code="INSTRUMENT_UNCONFIRMED",
            )

        # Populate cache for future calls
        self._cache[canonical_symbol] = result
        return result

    # ------------------------------------------------------------------
    # MARK SUBMIT VALIDATED
    # ------------------------------------------------------------------

    def mark_submit_validated(self, canonical_symbol: str) -> None:
        """
        Mark a symbol as submit-validated after a real or dry-run order
        confirmation.  Called by BrokerManager after a successful fill.
        """
        if canonical_symbol in self._cache:
            entry = self._cache[canonical_symbol]
            self._cache[canonical_symbol] = TradeabilityResult(
                canonical_symbol=entry.canonical_symbol,
                broker_symbol=entry.broker_symbol,
                asset_class=entry.asset_class,
                is_supported=entry.is_supported,
                is_tradeable=entry.is_tradeable,
                is_mapped=entry.is_mapped,
                is_tradeable_metadata=entry.is_tradeable_metadata,
                is_tradeable_submit_validated=True,
                reason_if_not_tradeable=entry.reason_if_not_tradeable,
                rejection_code=entry.rejection_code,
                raw_broker_metadata=entry.raw_broker_metadata,
            )
            logger.info(
                f"[SymbolMapper] {canonical_symbol} marked as submit_validated=True"
            )

    # ------------------------------------------------------------------
    # BROKER SYMBOL LOOKUP
    # ------------------------------------------------------------------

    def to_broker_symbol(self, canonical_symbol: str) -> Optional[str]:
        """
        Return the broker-native symbol for *canonical_symbol*, or None.
        Does not perform connectivity checks — use check() for that.
        """
        if canonical_symbol in self._cache:
            return self._cache[canonical_symbol].broker_symbol
        entry = self._static_map.get(canonical_symbol)
        return entry[0] if entry else None

    # ------------------------------------------------------------------
    # DIAGNOSTICS
    # ------------------------------------------------------------------

    def dump_cache(self) -> list:
        """Return the full capability cache as a list of dicts (for diagnostics)."""
        return [v.to_dict() for v in self._cache.values()]

    @property
    def is_hydrated(self) -> bool:
        return self._hydrated

    @property
    def hydrated_at(self) -> Optional[datetime]:
        return self._hydrated_at

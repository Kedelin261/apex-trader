"""
APEX MULTI-MARKET TJR ENGINE
Broker Symbol Mapper — Centralised canonical→broker-native mapping with
capability caching.

Design rules:
  - Only canonical symbols (XAUUSD, EURUSD, …) enter this layer.
  - Each BrokerSymbolMapper instance is owned by BrokerManager and
    hydrated once on broker connect.
  - Pre-submit validation occurs here; callers receive a structured
    TradeabilityResult so rejections surface through the Intent Layer
    with explicit reason codes rather than opaque errors.

Rejection reason codes (used in TradeabilityResult.rejection_code):
  UNKNOWN_SYMBOL                — canonical symbol not in internal registry
  NO_BROKER_MAPPING             — no mapping to broker-native symbol
  BROKER_DOES_NOT_SUPPORT_INSTRUMENT — broker knows about it but it is disabled
  INSTRUMENT_NOT_TRADEABLE      — temporarily or permanently closed
  BROKER_NOT_CONNECTED          — connector absent or not authenticated
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RESULT TYPE
# ---------------------------------------------------------------------------

@dataclass
class TradeabilityResult:
    """Structured result from a pre-submit tradeability check."""
    canonical_symbol: str
    broker_symbol: Optional[str]           # e.g. "XAU_USD" for OANDA
    asset_class: Optional[str]             # e.g. "GOLD", "FOREX"
    is_supported: bool                     # broker has a mapping for this symbol
    is_tradeable: bool                     # supported AND currently tradeable
    reason_if_not_tradeable: Optional[str] # human-readable reason
    rejection_code: Optional[str]          # machine-readable code (see module docstring)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "canonical_symbol": self.canonical_symbol,
            "broker_symbol": self.broker_symbol,
            "asset_class": self.asset_class,
            "is_supported": self.is_supported,
            "is_tradeable": self.is_tradeable,
            "reason_if_not_tradeable": self.reason_if_not_tradeable,
            "rejection_code": self.rejection_code,
            "checked_at": self.checked_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# STATIC BROKER MAPS
# These mirror the connector's own maps but live here so the mapper can
# answer queries without always going back to the connector.
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

# Brokers that have no mapping for certain instruments can be listed here.
# Currently empty — all symbols above are supported on OANDA practice.
_OANDA_UNSUPPORTED: frozenset[str] = frozenset()


class BrokerSymbolMapper:
    """
    Translates canonical symbols to broker-native symbols and caches
    live tradeability data fetched on broker connect.

    Lifecycle:
      1. Instantiate (empty cache).
      2. Call hydrate(connector) once after broker authentication.
      3. Call check(canonical_symbol) before every order submission.
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

        If *connector* is supplied and connected, it is queried for live
        instrument details; otherwise the static map is used as a fallback.
        """
        self._cache.clear()

        for canonical, (broker_sym, asset_class) in self._static_map.items():
            # Determine support / tradeability
            if canonical in self._unsupported:
                result = TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=False,
                    is_tradeable=False,
                    reason_if_not_tradeable=f"{canonical} not supported on {self._broker_name}",
                    rejection_code="BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
                )
            else:
                # Attempt live validation if connector available
                tradeable = True
                reason = None
                code = None

                if connector is not None:
                    try:
                        mapping = connector.validate_instrument_mapping(canonical)
                        if mapping is None:
                            tradeable = False
                            reason = f"Broker returned no mapping for {canonical}"
                            code = "NO_BROKER_MAPPING"
                        elif not mapping.tradeable:
                            tradeable = False
                            reason = f"{canonical} is marked non-tradeable by broker"
                            code = "INSTRUMENT_NOT_TRADEABLE"
                    except Exception as exc:
                        # Don't fail hard — fall back to static assumption of tradeable
                        logger.warning(
                            f"[SymbolMapper] Live validation for {canonical} failed: {exc}. "
                            "Falling back to static tradeable=True."
                        )

                result = TradeabilityResult(
                    canonical_symbol=canonical,
                    broker_symbol=broker_sym,
                    asset_class=asset_class,
                    is_supported=True,
                    is_tradeable=tradeable,
                    reason_if_not_tradeable=reason,
                    rejection_code=code,
                )

            self._cache[canonical] = result
            logger.debug(
                f"[SymbolMapper] Cached {canonical} → {broker_sym} "
                f"(tradeable={result.is_tradeable})"
            )

        self._hydrated = True
        self._hydrated_at = datetime.now(timezone.utc)
        logger.info(
            f"[SymbolMapper] Hydrated {len(self._cache)} symbols for broker "
            f"'{self._broker_name}' at {self._hydrated_at.isoformat()}"
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
          6. All checks pass → is_tradeable=True

        The cache is populated by hydrate(); if it has never been called
        the static map is consulted directly (graceful degradation).
        """
        # 1. Broker connectivity
        if not connector_connected:
            return TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
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

        # 3. Not in static map → UNKNOWN_SYMBOL (or NO_BROKER_MAPPING if map empty)
        if canonical_symbol not in self._static_map:
            return TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=None,
                asset_class=None,
                is_supported=False,
                is_tradeable=False,
                reason_if_not_tradeable=(
                    f"Symbol '{canonical_symbol}' is not registered in the engine's "
                    "instrument registry. Normalise the incoming symbol first."
                ),
                rejection_code="UNKNOWN_SYMBOL",
            )

        # 4. In static map but cache miss → build on the fly
        broker_sym, asset_class = self._static_map[canonical_symbol]
        if canonical_symbol in self._unsupported:
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=False,
                is_tradeable=False,
                reason_if_not_tradeable=(
                    f"{canonical_symbol} is not supported on {self._broker_name}"
                ),
                rejection_code="BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
            )
        else:
            result = TradeabilityResult(
                canonical_symbol=canonical_symbol,
                broker_symbol=broker_sym,
                asset_class=asset_class,
                is_supported=True,
                is_tradeable=True,
                reason_if_not_tradeable=None,
                rejection_code=None,
            )

        # Populate cache for future calls
        self._cache[canonical_symbol] = result
        return result

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

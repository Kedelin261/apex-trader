"""
APEX MULTI-MARKET TJR ENGINE
Symbol Utilities — Canonical symbol normalization.

Rules:
  - Canonical format: UPPERCASE, no separators (e.g. XAUUSD, EURUSD, BTCUSD)
  - All incoming variants are normalised via normalize_symbol() before
    passing into the Intent Layer.  Nothing downstream ever sees raw
    broker-native formats (XAU_USD) or slash-separated forms (XAU/USD).

Usage:
    from domain.symbol_utils import normalize_symbol, is_known_symbol

    canonical = normalize_symbol("XAU_USD")   # → "XAUUSD"
    canonical = normalize_symbol("xau/usd")   # → "XAUUSD"
    canonical = normalize_symbol("XAUUSD")    # → "XAUUSD"
    known     = is_known_symbol("XAUUSD")     # → True
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ALIAS TABLE
# Known external variants → canonical symbol
# This is the single source of truth for all symbol aliases.
# ---------------------------------------------------------------------------

_ALIAS_MAP: dict[str, str] = {
    # ---------- Gold / metals ----------
    "XAU_USD":   "XAUUSD",
    "XAU/USD":   "XAUUSD",
    "GOLD":      "XAUUSD",
    "GOLD/USD":  "XAUUSD",
    "XAUUSD":    "XAUUSD",
    "XAGUSDT":   "XAGUSD",
    "XAG_USD":   "XAGUSD",
    "XAG/USD":   "XAGUSD",
    "SILVER":    "XAGUSD",
    # ---------- Forex majors ----------
    "EUR_USD": "EURUSD",
    "EUR/USD": "EURUSD",
    "GBP_USD": "GBPUSD",
    "GBP/USD": "GBPUSD",
    "USD_JPY": "USDJPY",
    "USD/JPY": "USDJPY",
    "AUD_USD": "AUDUSD",
    "AUD/USD": "AUDUSD",
    "USD_CAD": "USDCAD",
    "USD/CAD": "USDCAD",
    "USD_CHF": "USDCHF",
    "USD/CHF": "USDCHF",
    "NZD_USD": "NZDUSD",
    "NZD/USD": "NZDUSD",
    "EUR_GBP": "EURGBP",
    "EUR/GBP": "EURGBP",
    "EUR_JPY": "EURJPY",
    "EUR/JPY": "EURJPY",
    "GBP_JPY": "GBPJPY",
    "GBP/JPY": "GBPJPY",
    # ---------- Oil / commodities ----------
    "WTI":       "USOIL",
    "CRUDE":     "USOIL",
    "WTICO_USD": "USOIL",
    "BCO_USD":   "UKOIL",
    "BRENT":     "UKOIL",
    # ---------- Indices ----------
    "US30_USD":    "US30",
    "SPX500_USD":  "US500",
    "SP500":       "US500",
    "NAS100_USD":  "NAS100",
    "NASDAQ":      "NAS100",
    "DE30_EUR":    "GER40",
    "UK100_GBP":   "UK100",
    # ---------- Crypto ----------
    "BTC_USD":  "BTCUSD",
    "BTC/USD":  "BTCUSD",
    "ETH_USD":  "ETHUSD",
    "ETH/USD":  "ETHUSD",
    "XRP_USD":  "XRPUSD",
    "XRP/USD":  "XRPUSD",
    "LTC_USD":  "LTCUSD",
    "LTC/USD":  "LTCUSD",
}

# Complete set of known canonical symbols
_KNOWN_CANONICAL: frozenset[str] = frozenset({
    # Gold / metals
    "XAUUSD", "XAGUSD",
    # Forex
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
    # Oil
    "USOIL", "UKOIL",
    # Indices
    "US30", "US500", "NAS100", "GER40", "UK100",
    # Crypto
    "BTCUSD", "ETHUSD", "XRPUSD", "LTCUSD",
})

# Separator characters to strip during normalization
_SEP_RE = re.compile(r"[_/\-\.\s]+")


def normalize_symbol(raw: str) -> str:
    """
    Convert any incoming symbol variant to canonical uppercase format.

    Steps:
      1. Strip whitespace and upper-case the input.
      2. Look up in the alias table (handles XAU_USD, XAU/USD, GOLD, etc.).
      3. If not in alias table, strip all separators and return uppercase.

    Examples:
        normalize_symbol("XAU_USD")  → "XAUUSD"
        normalize_symbol("xau/usd")  → "XAUUSD"
        normalize_symbol("xauusd")   → "XAUUSD"
        normalize_symbol("GOLD")     → "XAUUSD"
        normalize_symbol("EUR_USD")  → "EURUSD"
        normalize_symbol("btc/usd")  → "BTCUSD"
        normalize_symbol("UNKNOWN")  → "UNKNOWN"

    Returns the canonical string.  Never raises.
    """
    if not raw or not isinstance(raw, str):
        logger.warning(f"[SymbolUtils] normalize_symbol received empty/non-string: {raw!r}")
        return str(raw or "").upper()

    stripped = raw.strip()
    upper = stripped.upper()

    # Direct alias lookup (handles separator variants and aliases like GOLD)
    if upper in _ALIAS_MAP:
        canonical = _ALIAS_MAP[upper]
        if canonical != upper:
            logger.debug(
                f"[SymbolUtils] Normalized symbol: {raw!r} → {canonical!r} (alias)"
            )
        return canonical

    # Strip separators and try again (handles any remaining variants)
    no_sep = _SEP_RE.sub("", upper)
    if no_sep in _ALIAS_MAP:
        canonical = _ALIAS_MAP[no_sep]
        logger.debug(
            f"[SymbolUtils] Normalized symbol: {raw!r} → {canonical!r} (stripped alias)"
        )
        return canonical

    # Return cleaned-up version even if unknown
    if no_sep != upper:
        logger.debug(
            f"[SymbolUtils] Symbol {raw!r} stripped to {no_sep!r} (not in alias table)"
        )
    return no_sep


def is_known_symbol(canonical: str) -> bool:
    """Return True if *canonical* is a recognised canonical symbol."""
    return canonical in _KNOWN_CANONICAL


def get_all_known_symbols() -> frozenset[str]:
    """Return the full set of recognised canonical symbols."""
    return _KNOWN_CANONICAL

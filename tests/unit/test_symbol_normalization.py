"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — Symbol Normalisation (domain/symbol_utils.py)

Covers:
  - All XAUUSD variant inputs → "XAUUSD"
  - EURUSD variant inputs → "EURUSD"
  - is_known_symbol() recognition
  - Unknown symbols pass through cleaned
  - Edge cases: empty string, None-like, mixed case, multiple separators
"""
from __future__ import annotations

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from domain.symbol_utils import normalize_symbol, is_known_symbol, get_all_known_symbols


class TestNormalizeSymbol:
    """normalize_symbol() converts every variant to the canonical uppercase form."""

    # ---- XAUUSD variants ----
    def test_xauusd_canonical_passthrough(self):
        assert normalize_symbol("XAUUSD") == "XAUUSD"

    def test_xau_underscore_usd(self):
        assert normalize_symbol("XAU_USD") == "XAUUSD"

    def test_xau_slash_usd(self):
        assert normalize_symbol("XAU/USD") == "XAUUSD"

    def test_xauusd_lowercase(self):
        assert normalize_symbol("xauusd") == "XAUUSD"

    def test_xau_usd_lowercase_underscore(self):
        assert normalize_symbol("xau_usd") == "XAUUSD"

    def test_xau_usd_lowercase_slash(self):
        assert normalize_symbol("xau/usd") == "XAUUSD"

    def test_gold_alias(self):
        assert normalize_symbol("GOLD") == "XAUUSD"

    def test_gold_alias_lowercase(self):
        assert normalize_symbol("gold") == "XAUUSD"

    def test_gold_slash_usd(self):
        assert normalize_symbol("GOLD/USD") == "XAUUSD"

    def test_xauusd_with_leading_trailing_spaces(self):
        assert normalize_symbol("  XAU_USD  ") == "XAUUSD"

    # ---- EURUSD variants ----
    def test_eurusd_canonical(self):
        assert normalize_symbol("EURUSD") == "EURUSD"

    def test_eur_underscore_usd(self):
        assert normalize_symbol("EUR_USD") == "EURUSD"

    def test_eur_slash_usd(self):
        assert normalize_symbol("EUR/USD") == "EURUSD"

    def test_eurusd_lowercase(self):
        assert normalize_symbol("eurusd") == "EURUSD"

    # ---- Other known symbols ----
    def test_gbpusd(self):
        assert normalize_symbol("GBP_USD") == "GBPUSD"

    def test_usdjpy(self):
        assert normalize_symbol("USD/JPY") == "USDJPY"

    def test_xagusd(self):
        assert normalize_symbol("XAG_USD") == "XAGUSD"

    def test_silver_alias(self):
        assert normalize_symbol("SILVER") == "XAGUSD"

    def test_us500_alias(self):
        assert normalize_symbol("SPX500_USD") == "US500"

    def test_sp500_alias(self):
        assert normalize_symbol("SP500") == "US500"

    def test_nas100_alias(self):
        assert normalize_symbol("NAS100_USD") == "NAS100"

    # ---- Unknown / pass-through ----
    def test_unknown_symbol_returns_uppercase(self):
        result = normalize_symbol("FOOBAR")
        assert result == "FOOBAR"

    def test_unknown_with_separator_stripped(self):
        result = normalize_symbol("FOO_BAR")
        # alias not found → stripped to FOOBAR
        assert result == "FOOBAR"

    # ---- Edge cases ----
    def test_empty_string(self):
        result = normalize_symbol("")
        assert result == ""

    def test_single_char(self):
        result = normalize_symbol("x")
        assert result == "X"


class TestIsKnownSymbol:
    """is_known_symbol() returns True only for canonical symbols in the registry."""

    def test_xauusd_known(self):
        assert is_known_symbol("XAUUSD") is True

    def test_eurusd_known(self):
        assert is_known_symbol("EURUSD") is True

    def test_us500_known(self):
        assert is_known_symbol("US500") is True

    def test_raw_xau_usd_not_canonical(self):
        # Raw broker-native symbol is NOT canonical
        assert is_known_symbol("XAU_USD") is False

    def test_slash_variant_not_canonical(self):
        assert is_known_symbol("EUR/USD") is False

    def test_gold_alias_not_canonical(self):
        assert is_known_symbol("GOLD") is False

    def test_unknown_symbol(self):
        assert is_known_symbol("RANDOMTHING") is False


class TestGetAllKnownSymbols:
    """get_all_known_symbols() returns a non-empty frozenset of canonical symbols."""

    def test_returns_frozenset(self):
        result = get_all_known_symbols()
        assert isinstance(result, frozenset)

    def test_contains_xauusd(self):
        assert "XAUUSD" in get_all_known_symbols()

    def test_contains_eurusd(self):
        assert "EURUSD" in get_all_known_symbols()

    def test_does_not_contain_raw_variant(self):
        assert "XAU_USD" not in get_all_known_symbols()


class TestNormalizeRoundtrip:
    """
    Roundtrip: normalize → is_known should be True for all major instruments.
    """

    @pytest.mark.parametrize("raw", [
        "XAU_USD", "XAU/USD", "xauusd", "GOLD",
        "EUR_USD", "EUR/USD", "eurusd",
        "GBP_USD", "GBP/USD",
        "USD_JPY", "USD/JPY",
        "XAG_USD", "SILVER",
    ])
    def test_normalize_then_is_known(self, raw):
        canonical = normalize_symbol(raw)
        assert is_known_symbol(canonical), (
            f"normalize_symbol({raw!r}) = {canonical!r} is not known"
        )

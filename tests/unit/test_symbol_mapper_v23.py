"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — Broker Symbol Mapper v2.3

Covers:
  1. Capability truthfulness — hydrate() without connector → INSTRUMENT_UNCONFIRMED
  2. Capability truthfulness — hydrate() with connected connector that returns
     static fallback (is_supported=False) → INSTRUMENT_UNCONFIRMED (not tradeable=True)
  3. Capability truthfulness — hydrate() with connector returning live data →
     truthful is_tradeable_metadata flag
  4. check() cache hit returns stored value
  5. check() cache miss uses conservative defaults
  6. mark_submit_validated() updates the cache entry
  7. dump_cache() includes all v2.3 granular fields
  8. TradeabilityResult.to_dict() includes all v2.3 fields
  9. Regression: EURUSD still tradeable when connector confirms
  10. INSTRUMENT_UNCONFIRMED is distinct from INSTRUMENT_NOT_TRADEABLE
"""
from __future__ import annotations

import sys
import os
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from brokers.symbol_mapper import BrokerSymbolMapper, TradeabilityResult
from brokers.base_connector import InstrumentMapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mapping(
    internal: str,
    broker_symbol: str,
    is_supported: bool = True,
    is_tradeable_metadata: bool = True,
    reason: str = None,
) -> InstrumentMapping:
    return InstrumentMapping(
        internal=internal,
        broker_symbol=broker_symbol,
        tradeable=is_supported and is_tradeable_metadata,
        min_units=1.0,
        max_units=1_000_000.0,
        precision=5,
        margin_rate=0.02,
        spread_typical=0.0,
        is_mapped=True,
        is_supported=is_supported,
        is_tradeable_metadata=is_tradeable_metadata,
        raw_broker_metadata={},
        not_tradeable_reason=reason,
    )


def _make_static_fallback(internal: str, broker_symbol: str) -> InstrumentMapping:
    """Simulate the static fallback returned when connector API is unreachable."""
    return InstrumentMapping(
        internal=internal,
        broker_symbol=broker_symbol,
        tradeable=False,           # conservative
        min_units=1.0,
        max_units=1_000_000.0,
        precision=5,
        margin_rate=0.05,
        spread_typical=0.0,
        is_mapped=True,
        is_supported=False,        # not confirmed
        is_tradeable_metadata=False,
        raw_broker_metadata={},
        not_tradeable_reason="Tradeability not confirmed — broker API was unreachable",
    )


# ---------------------------------------------------------------------------
# 1. Truthfulness: hydrate() without connector
# ---------------------------------------------------------------------------

class TestCapabilityTrutfulness:

    def test_hydrate_no_connector_xauusd_is_unconfirmed(self):
        """Without a connector, XAUUSD should be INSTRUMENT_UNCONFIRMED, not tradeable=True."""
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False, (
            "XAUUSD must NOT be marked tradeable when no connector was used to confirm"
        )
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED", (
            f"Expected INSTRUMENT_UNCONFIRMED, got {r.rejection_code!r}"
        )
        assert r.is_mapped is True
        assert r.is_supported is False

    def test_hydrate_no_connector_eurusd_is_unconfirmed(self):
        """EURUSD also unconfirmed without connector."""
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        r = m.check("EURUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED"

    def test_hydrate_no_connector_not_same_as_not_tradeable(self):
        """INSTRUMENT_UNCONFIRMED ≠ INSTRUMENT_NOT_TRADEABLE — distinct codes."""
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        r = m.check("XAUUSD", connector_connected=True)
        assert r.rejection_code != "INSTRUMENT_NOT_TRADEABLE"
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED"

    def test_hydrate_with_static_fallback_connector_marks_unconfirmed(self):
        """
        When connector returns static fallback (is_supported=False),
        the cache should reflect INSTRUMENT_UNCONFIRMED, NOT tradeable=True.
        This was the root bug — static fallback was allowed to return tradeable=True.
        """
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_static_fallback(
            "XAUUSD", "XAU_USD"
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False, (
            "Static fallback (is_supported=False) must NOT produce is_tradeable=True"
        )
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED"
        assert r.is_supported is False

    def test_hydrate_with_live_confirmed_connector_marks_tradeable(self):
        """When connector confirms live tradeability, XAUUSD is tradeable=True."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "XAUUSD", "XAU_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is True
        assert r.is_supported is True
        assert r.is_tradeable_metadata is True
        assert r.rejection_code is None

    def test_hydrate_connector_confirms_non_tradeable(self):
        """When connector confirms is_tradeable_metadata=False, cache reflects that."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "XAUUSD", "XAU_USD",
            is_supported=True,
            is_tradeable_metadata=False,
            reason="Market halted",
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.is_supported is True
        assert r.is_tradeable_metadata is False
        assert r.rejection_code == "INSTRUMENT_NOT_TRADEABLE"
        assert r.reason_if_not_tradeable is not None

    def test_hydrate_connector_raises_marks_unconfirmed(self):
        """Exception during connector call → INSTRUMENT_UNCONFIRMED."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.side_effect = ConnectionError("timeout")
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED"

    def test_hydrate_connector_returns_none_mapping(self):
        """When connector returns None mapping → NO_BROKER_MAPPING."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = None
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "NO_BROKER_MAPPING"
        assert r.is_mapped is False


# ---------------------------------------------------------------------------
# 2. check() validation logic
# ---------------------------------------------------------------------------

class TestCheckMethod:

    def test_broker_not_connected(self):
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        r = m.check("XAUUSD", connector_connected=False)
        assert r.is_tradeable is False
        assert r.rejection_code == "BROKER_NOT_CONNECTED"

    def test_unknown_symbol(self):
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        r = m.check("PEPEUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "UNKNOWN_SYMBOL"
        assert r.broker_symbol is None

    def test_raw_broker_symbol_not_canonical(self):
        """XAU_USD (broker-native) is not in canonical registry → UNKNOWN_SYMBOL."""
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        r = m.check("XAU_USD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "UNKNOWN_SYMBOL"

    def test_cache_hit_returns_stored_value(self):
        """After hydrate(), check() returns the cached TradeabilityResult."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "EURUSD", "EUR_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r1 = m.check("EURUSD", connector_connected=True)
        r2 = m.check("EURUSD", connector_connected=True)
        assert r1 is r2  # Same object (cache hit)

    def test_cache_miss_conservative_default(self):
        """If cache is empty but static map has symbol, return INSTRUMENT_UNCONFIRMED."""
        m = BrokerSymbolMapper("oanda")
        # Do NOT hydrate
        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "INSTRUMENT_UNCONFIRMED"


# ---------------------------------------------------------------------------
# 3. mark_submit_validated
# ---------------------------------------------------------------------------

class TestMarkSubmitValidated:

    def test_mark_updates_cache_entry(self):
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "XAUUSD", "XAU_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        # Initially not submit-validated
        r_before = m.check("XAUUSD", connector_connected=True)
        assert r_before.is_tradeable_submit_validated is False

        m.mark_submit_validated("XAUUSD")

        # Now it should be True
        r_after = m.check("XAUUSD", connector_connected=True)
        assert r_after.is_tradeable_submit_validated is True

    def test_mark_unknown_symbol_does_not_raise(self):
        """mark_submit_validated for uncached symbol is a no-op."""
        m = BrokerSymbolMapper("oanda")
        m.mark_submit_validated("DOGEUSD")  # Should not raise


# ---------------------------------------------------------------------------
# 4. dump_cache granular fields
# ---------------------------------------------------------------------------

class TestDumpCache:

    def test_dump_cache_includes_v23_fields(self):
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "XAUUSD", "XAU_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        cache = m.dump_cache()
        xau_entry = next((e for e in cache if e["canonical_symbol"] == "XAUUSD"), None)
        assert xau_entry is not None

        # All v2.3 fields must be present
        required_fields = {
            "canonical_symbol", "broker_symbol", "asset_class",
            "is_mapped", "is_supported", "is_tradeable_metadata",
            "is_tradeable_submit_validated", "is_tradeable",
            "reason_if_not_tradeable", "rejection_code", "checked_at",
        }
        for f in required_fields:
            assert f in xau_entry, f"Field '{f}' missing from dump_cache entry"

    def test_dump_cache_no_connector_shows_unconfirmed(self):
        """Cache with no connector should show INSTRUMENT_UNCONFIRMED for all instruments."""
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)
        cache = m.dump_cache()
        for entry in cache:
            assert entry["rejection_code"] == "INSTRUMENT_UNCONFIRMED", (
                f"{entry['canonical_symbol']} has code {entry['rejection_code']!r}, "
                "expected INSTRUMENT_UNCONFIRMED"
            )
            assert entry["is_tradeable"] is False


# ---------------------------------------------------------------------------
# 5. TradeabilityResult.to_dict v2.3
# ---------------------------------------------------------------------------

class TestTradeabilityResultV23:

    def test_to_dict_includes_granular_flags(self):
        r = TradeabilityResult(
            canonical_symbol="XAUUSD",
            broker_symbol="XAU_USD",
            asset_class="GOLD",
            is_supported=True,
            is_tradeable=True,
            is_mapped=True,
            is_tradeable_metadata=True,
            is_tradeable_submit_validated=False,
            reason_if_not_tradeable=None,
            rejection_code=None,
        )
        d = r.to_dict()
        assert d["is_mapped"] is True
        assert d["is_supported"] is True
        assert d["is_tradeable_metadata"] is True
        assert d["is_tradeable_submit_validated"] is False
        assert d["is_tradeable"] is True

    def test_to_dict_unconfirmed_entry(self):
        r = TradeabilityResult(
            canonical_symbol="XAUUSD",
            broker_symbol="XAU_USD",
            asset_class="GOLD",
            is_supported=False,
            is_tradeable=False,
            is_mapped=True,
            is_tradeable_metadata=False,
            is_tradeable_submit_validated=False,
            reason_if_not_tradeable="Tradeability not confirmed",
            rejection_code="INSTRUMENT_UNCONFIRMED",
        )
        d = r.to_dict()
        assert d["is_tradeable"] is False
        assert d["rejection_code"] == "INSTRUMENT_UNCONFIRMED"
        assert d["is_supported"] is False


# ---------------------------------------------------------------------------
# 6. Regression: EURUSD still works
# ---------------------------------------------------------------------------

class TestEURUSDRegression:

    def test_eurusd_tradeable_with_live_connector(self):
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "EURUSD", "EUR_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("EURUSD", connector_connected=True)
        assert r.is_tradeable is True
        assert r.broker_symbol == "EUR_USD"
        assert r.asset_class == "FOREX"
        assert r.rejection_code is None

    def test_eurusd_broker_symbol_lookup(self):
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "EURUSD", "EUR_USD", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)
        assert m.to_broker_symbol("EURUSD") == "EUR_USD"

    def test_all_static_symbols_in_cache_after_hydrate(self):
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = _make_mapping(
            "ANY", "ANY_SYM", is_supported=True, is_tradeable_metadata=True
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)
        cache_symbols = {e["canonical_symbol"] for e in m.dump_cache()}
        assert "EURUSD" in cache_symbols
        assert "XAUUSD" in cache_symbols
        assert "GBPUSD" in cache_symbols

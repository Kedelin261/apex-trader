"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — Broker Symbol Mapper (brokers/symbol_mapper.py)

Covers:
  - BrokerSymbolMapper construction and static map loading
  - hydrate() without connector (static fallback)
  - hydrate() with mock connector
  - check() for XAUUSD → XAU_USD, tradeable
  - check() for EURUSD → EUR_USD, tradeable
  - check() for unknown symbol → UNKNOWN_SYMBOL
  - check() when broker not connected → BROKER_NOT_CONNECTED
  - to_broker_symbol() utility
  - Capability cache dump
  - TradeabilityResult.to_dict()
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


class TestTradeabilityResult:
    def test_to_dict_contains_all_keys(self):
        r = TradeabilityResult(
            canonical_symbol="XAUUSD",
            broker_symbol="XAU_USD",
            asset_class="GOLD",
            is_supported=True,
            is_tradeable=True,
            reason_if_not_tradeable=None,
            rejection_code=None,
        )
        d = r.to_dict()
        assert d["canonical_symbol"] == "XAUUSD"
        assert d["broker_symbol"] == "XAU_USD"
        assert d["asset_class"] == "GOLD"
        assert d["is_supported"] is True
        assert d["is_tradeable"] is True
        assert d["rejection_code"] is None
        assert "checked_at" in d

    def test_not_tradeable_has_code(self):
        r = TradeabilityResult(
            canonical_symbol="UNKNOWN",
            broker_symbol=None,
            asset_class=None,
            is_supported=False,
            is_tradeable=False,
            reason_if_not_tradeable="Symbol not known",
            rejection_code="UNKNOWN_SYMBOL",
        )
        d = r.to_dict()
        assert d["rejection_code"] == "UNKNOWN_SYMBOL"
        assert d["is_tradeable"] is False


class TestBrokerSymbolMapper:
    """Tests for BrokerSymbolMapper with OANDA static map."""

    def _make_mapper(self, hydrate: bool = True) -> BrokerSymbolMapper:
        m = BrokerSymbolMapper("oanda")
        if hydrate:
            m.hydrate(connector=None)
        return m

    # ---- Construction ----
    def test_initial_state_not_hydrated(self):
        m = BrokerSymbolMapper("oanda")
        assert m.is_hydrated is False
        assert m.hydrated_at is None

    def test_hydrate_without_connector(self):
        m = self._make_mapper()
        assert m.is_hydrated is True
        assert m.hydrated_at is not None

    def test_hydrate_caches_xauusd(self):
        m = self._make_mapper()
        assert "XAUUSD" in m._cache

    def test_hydrate_caches_eurusd(self):
        m = self._make_mapper()
        assert "EURUSD" in m._cache

    # ---- XAUUSD ----
    def test_xauusd_check_tradeable(self):
        m = self._make_mapper()
        r = m.check("XAUUSD", connector_connected=True)
        assert r.canonical_symbol == "XAUUSD"
        assert r.broker_symbol == "XAU_USD"
        assert r.asset_class == "GOLD"
        assert r.is_supported is True
        assert r.is_tradeable is True
        assert r.rejection_code is None

    def test_xauusd_to_broker_symbol(self):
        m = self._make_mapper()
        assert m.to_broker_symbol("XAUUSD") == "XAU_USD"

    # ---- EURUSD ----
    def test_eurusd_check_tradeable(self):
        m = self._make_mapper()
        r = m.check("EURUSD", connector_connected=True)
        assert r.canonical_symbol == "EURUSD"
        assert r.broker_symbol == "EUR_USD"
        assert r.asset_class == "FOREX"
        assert r.is_tradeable is True

    # ---- Unknown symbol ----
    def test_unknown_symbol_rejected(self):
        m = self._make_mapper()
        r = m.check("DOGECOIN", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "UNKNOWN_SYMBOL"
        assert r.broker_symbol is None

    def test_raw_xau_usd_not_canonical(self):
        """XAU_USD (broker-native) should not be in the mapper; only XAUUSD is."""
        m = self._make_mapper()
        r = m.check("XAU_USD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "UNKNOWN_SYMBOL"

    # ---- Broker not connected ----
    def test_broker_not_connected_rejected(self):
        m = self._make_mapper()
        r = m.check("XAUUSD", connector_connected=False)
        assert r.is_tradeable is False
        assert r.rejection_code == "BROKER_NOT_CONNECTED"

    # ---- With mock connector ----
    def test_hydrate_with_connected_connector(self):
        """When connector returns valid InstrumentMapping, it's used."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = InstrumentMapping(
            internal="XAUUSD",
            broker_symbol="XAU_USD",
            tradeable=True,
            min_units=1.0,
            max_units=1_000_000.0,
            precision=3,
            margin_rate=0.02,
            spread_typical=0.5,
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is True
        assert r.broker_symbol == "XAU_USD"

    def test_hydrate_connector_returns_non_tradeable(self):
        """If connector says not tradeable, cache should reflect that."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.return_value = InstrumentMapping(
            internal="XAUUSD",
            broker_symbol="XAU_USD",
            tradeable=False,
            min_units=1.0,
            max_units=1_000_000.0,
            precision=3,
            margin_rate=0.02,
            spread_typical=0.5,
        )
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False
        assert r.rejection_code == "INSTRUMENT_NOT_TRADEABLE"

    def test_hydrate_connector_raises_falls_back_to_static(self):
        """If connector raises for a symbol, mapper falls back to static tradeable=True."""
        mock_conn = MagicMock()
        mock_conn.validate_instrument_mapping.side_effect = ConnectionError("timeout")
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        # XAUUSD is in the static map and not in unsupported set → should be tradeable
        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is True

    # ---- Dump cache ----
    def test_dump_cache_returns_list(self):
        m = self._make_mapper()
        dump = m.dump_cache()
        assert isinstance(dump, list)
        assert len(dump) > 0

    def test_dump_cache_contains_xauusd(self):
        m = self._make_mapper()
        symbols = {d["canonical_symbol"] for d in m.dump_cache()}
        assert "XAUUSD" in symbols

    # ---- Unknown broker ----
    def test_unknown_broker_empty_static_map(self):
        m = BrokerSymbolMapper("unknown_broker_xyz")
        assert m._static_map == {}
        m.hydrate(connector=None)  # Should not raise
        r = m.check("XAUUSD", connector_connected=True)
        assert r.rejection_code == "UNKNOWN_SYMBOL"

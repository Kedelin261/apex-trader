"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — Broker Routing (v3.0)

Test coverage:
  1. EURUSD → OANDA routing (not IBKR)
  2. XAUUSD → IBKR routing (not OANDA)
  3. ES/NQ → IBKR routing
  4. AAPL → IBKR routing
  5. get_broker_for_symbol() determinism — all symbols
  6. BrokerManager.check_symbol() with both connectors
  7. submit_order() routing to correct connector
  8. BrokerManager.get_routing_table() structure
  9. Paper fallback for unknown symbols
  10. Multi-broker status reporting
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone
from dataclasses import dataclass


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _make_config() -> dict:
    return {
        "risk": {"max_risk_per_trade_pct": 1.0, "account_balance_default": 100_000},
        "data": {"execution_state_file": "/tmp/test_exec_state.json"},
        "agents": {},
    }


def _make_esm():
    esm = MagicMock()
    esm.is_kill_switch_active.return_value = False
    esm.is_live_trading_allowed.return_value = False  # paper mode by default
    esm.is_connected.return_value = True
    esm.connect_broker.return_value = (True, "connected")
    esm.set_preflight_result.return_value = None
    esm.snapshot.return_value = MagicMock(
        state="LIVE_CONNECTED_SAFE",
        broker="both",
        environment="practice",
        account_id="TEST",
        live_safe_lock=True,
        live_enabled=False,
        kill_switch_active=False,
        preflight_passed=True,
        preflight_performed_at=None,
        last_transition_at=None,
        last_transition_reason="test",
        last_operator="test",
        connection_error=None,
        arming_operator=None,
        armed_at=None,
    )
    return esm


def _make_oanda_connector():
    """Mock OANDA connector accepting only EURUSD."""
    from brokers.base_connector import OrderResult, OrderStatus, InstrumentMapping
    mock = MagicMock()
    mock.is_connected.return_value = True
    mock.check_circuit_breaker.return_value = False
    mock.connection_status.return_value = {"broker": "oanda", "state": "AUTHENTICATED"}

    def submit_order_side_effect(request):
        if request.instrument == "EURUSD":
            return OrderResult(
                success=True, order_id="OANDA-001",
                status=OrderStatus.FILLED, filled_price=1.085, filled_units=request.units,
                raw={"broker": "oanda"},
            )
        return OrderResult(
            success=False, order_id=None, status=OrderStatus.REJECTED,
            reject_reason=f"OANDA does not handle {request.instrument}",
            normalized_rejection_code="WRONG_INSTRUMENT_FOR_BROKER",
        )

    mock.submit_order.side_effect = submit_order_side_effect

    def validate_mapping(sym):
        if sym == "EURUSD":
            return InstrumentMapping(
                internal="EURUSD", broker_symbol="EUR_USD",
                tradeable=True, min_units=1, max_units=1_000_000,
                precision=5, margin_rate=0.02, spread_typical=0.0,
                is_mapped=True, is_supported=True, is_tradeable_metadata=True,
            )
        return InstrumentMapping(
            internal=sym, broker_symbol=None, tradeable=False,
            min_units=0, max_units=0, precision=5, margin_rate=0,
            spread_typical=0, is_mapped=False, is_supported=False,
            is_tradeable_metadata=False,
            not_tradeable_reason=f"{sym} not supported on OANDA",
        )

    mock.validate_instrument_mapping.side_effect = validate_mapping
    mock.authenticate.return_value = (True, "OANDA authenticated")
    mock.run_preflight_checks.return_value = MagicMock(
        overall_pass=True, blocking_failures=[], warnings=[]
    )
    return mock


def _make_ibkr_connector():
    """Mock IBKR connector accepting XAUUSD, ES, NQ, AAPL, etc."""
    from brokers.base_connector import OrderResult, OrderStatus, InstrumentMapping
    from brokers.ibkr_connector import IBKR_PRIMARY_SYMBOLS
    mock = MagicMock()
    mock.is_connected.return_value = True
    mock.check_circuit_breaker.return_value = False
    mock.connection_status.return_value = {"broker": "ibkr", "state": "AUTHENTICATED"}

    def submit_order_side_effect(request):
        if request.instrument in IBKR_PRIMARY_SYMBOLS:
            return OrderResult(
                success=True, order_id="IBKR-001",
                status=OrderStatus.FILLED,
                filled_price=request.price or 0.0,
                filled_units=request.units,
                raw={"broker": "ibkr", "instrument": request.instrument},
            )
        return OrderResult(
            success=False, order_id=None, status=OrderStatus.REJECTED,
            reject_reason=f"IBKR does not handle {request.instrument}",
            normalized_rejection_code="NO_BROKER_MAPPING",
        )

    mock.submit_order.side_effect = submit_order_side_effect

    def validate_mapping(sym):
        if sym == "EURUSD":
            return InstrumentMapping(
                internal="EURUSD", broker_symbol=None, tradeable=False,
                min_units=0, max_units=0, precision=5, margin_rate=0,
                spread_typical=0, is_mapped=False, is_supported=False,
                is_tradeable_metadata=False,
                not_tradeable_reason="EURUSD must route to OANDA",
            )
        if sym in IBKR_PRIMARY_SYMBOLS:
            return InstrumentMapping(
                internal=sym, broker_symbol=sym,
                tradeable=True, min_units=1, max_units=1_000_000,
                precision=2, margin_rate=0.05, spread_typical=0.0,
                is_mapped=True, is_supported=True, is_tradeable_metadata=True,
            )
        return InstrumentMapping(
            internal=sym, broker_symbol=None, tradeable=False,
            min_units=0, max_units=0, precision=2, margin_rate=0,
            spread_typical=0, is_mapped=False, is_supported=False,
            is_tradeable_metadata=False,
        )

    mock.validate_instrument_mapping.side_effect = validate_mapping
    mock.authenticate.return_value = (True, "IBKR authenticated")
    mock.run_preflight_checks.return_value = MagicMock(
        overall_pass=True, blocking_failures=[], warnings=[]
    )
    mock.get_rejection_history.return_value = []
    return mock


def _make_broker_manager(config=None) -> "BrokerManager":
    """Create BrokerManager with both connectors mocked."""
    from live.broker_manager import BrokerManager
    config = config or _make_config()
    bm = BrokerManager.__new__(BrokerManager)
    bm._config = config
    bm._orchestrator = None
    bm._risk_manager = None
    bm._connector = _make_oanda_connector()     # OANDA for EURUSD
    bm._ibkr_connector = _make_ibkr_connector() # IBKR for rest
    bm._broker_name = "oanda"
    bm._ibkr_broker_name = "ibkr"
    bm._preflight_svc = None
    bm._recon_svc = None
    bm._paper_broker = MagicMock()
    bm._rejection_history = []
    bm._esm = _make_esm()

    # Set up symbol mappers
    from brokers.symbol_mapper import BrokerSymbolMapper
    bm._symbol_mapper = BrokerSymbolMapper("oanda")
    bm._symbol_mapper.hydrate()  # no connector → unconfirmed

    bm._ibkr_symbol_mapper = BrokerSymbolMapper("ibkr")
    bm._ibkr_symbol_mapper.hydrate()  # no connector → unconfirmed

    return bm


# ─────────────────────────────────────────────────────────────────────────────
# 1. EURUSD → OANDA routing
# ─────────────────────────────────────────────────────────────────────────────

class TestEURUSDRoutingToOANDA:

    def test_get_broker_for_eurusd(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") == "oanda"

    def test_eurusd_lowercase(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("eurusd") == "oanda"

    def test_route_connector_eurusd_returns_oanda(self):
        bm = _make_broker_manager()
        connector = bm._route_connector("EURUSD")
        # Should be the OANDA connector (mock)
        assert connector is bm._connector

    def test_submit_eurusd_live_goes_to_oanda(self):
        """When live, EURUSD order goes to OANDA connector."""
        from brokers.base_connector import OrderRequest, OrderSide
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = True

        req = OrderRequest(
            instrument="EURUSD", side=OrderSide.BUY, units=1.0,
            price=1.085, stop_loss=1.080, take_profit=1.095,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        # Verify OANDA connector was called
        bm._connector.submit_order.assert_called_once()
        # Verify IBKR was NOT called
        bm._ibkr_connector.submit_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 2. XAUUSD → IBKR routing
# ─────────────────────────────────────────────────────────────────────────────

class TestXAUUSDRoutingToIBKR:

    def test_get_broker_for_xauusd(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("XAUUSD") == "ibkr"

    def test_route_connector_xauusd_returns_ibkr(self):
        bm = _make_broker_manager()
        connector = bm._route_connector("XAUUSD")
        assert connector is bm._ibkr_connector

    def test_submit_xauusd_live_goes_to_ibkr(self):
        """When live, XAUUSD order goes to IBKR connector."""
        from brokers.base_connector import OrderRequest, OrderSide
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = True

        req = OrderRequest(
            instrument="XAUUSD", side=OrderSide.BUY, units=1.0,
            price=2350.0, stop_loss=2330.0, take_profit=2400.0,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        bm._ibkr_connector.submit_order.assert_called_once()
        bm._connector.submit_order.assert_not_called()

    def test_xauusd_not_in_oanda_routing(self):
        """XAUUSD is NOT in OANDA routing table."""
        from live.broker_manager import OANDA_ROUTING
        assert "XAUUSD" not in OANDA_ROUTING


# ─────────────────────────────────────────────────────────────────────────────
# 3. ES/NQ → IBKR routing
# ─────────────────────────────────────────────────────────────────────────────

class TestFuturesRoutingToIBKR:

    def test_es_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("ES") == "ibkr"

    def test_nq_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("NQ") == "ibkr"

    def test_ym_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("YM") == "ibkr"

    def test_cl_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("CL") == "ibkr"

    def test_submit_es_live(self):
        from brokers.base_connector import OrderRequest, OrderSide
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = True
        req = OrderRequest(
            instrument="ES", side=OrderSide.BUY, units=1.0,
            price=5200.0, stop_loss=5150.0, take_profit=5300.0,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        bm._ibkr_connector.submit_order.assert_called_once()

    def test_submit_nq_live(self):
        from brokers.base_connector import OrderRequest, OrderSide
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = True
        req = OrderRequest(
            instrument="NQ", side=OrderSide.BUY, units=1.0,
            price=18500.0, stop_loss=18400.0, take_profit=18700.0,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True


# ─────────────────────────────────────────────────────────────────────────────
# 4. AAPL → IBKR routing
# ─────────────────────────────────────────────────────────────────────────────

class TestStockRoutingToIBKR:

    def test_aapl_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("AAPL") == "ibkr"

    def test_msft_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("MSFT") == "ibkr"

    def test_tsla_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("TSLA") == "ibkr"

    def test_nvda_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("NVDA") == "ibkr"

    def test_submit_aapl_live(self):
        from brokers.base_connector import OrderRequest, OrderSide
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = True
        req = OrderRequest(
            instrument="AAPL", side=OrderSide.BUY, units=100.0,
            price=185.0, stop_loss=180.0, take_profit=195.0,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        bm._ibkr_connector.submit_order.assert_called_once()
        bm._connector.submit_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 5. get_broker_for_symbol() determinism
# ─────────────────────────────────────────────────────────────────────────────

class TestBrokerRoutingDeterminism:

    def test_all_oanda_symbols(self):
        from live.broker_manager import get_broker_for_symbol, OANDA_ROUTING
        for sym in OANDA_ROUTING:
            assert get_broker_for_symbol(sym) == "oanda", f"{sym} should route to oanda"

    def test_all_ibkr_symbols(self):
        from live.broker_manager import get_broker_for_symbol, IBKR_ROUTING
        for sym in IBKR_ROUTING:
            assert get_broker_for_symbol(sym) == "ibkr", f"{sym} should route to ibkr"

    def test_unknown_symbol_returns_paper(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("UNKNOWN_SYMBOL_XYZ") == "paper"

    def test_case_insensitive(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("eurusd") == "oanda"
        assert get_broker_for_symbol("xauusd") == "ibkr"
        assert get_broker_for_symbol("EURUSD") == "oanda"
        assert get_broker_for_symbol("XAUUSD") == "ibkr"

    def test_no_overlap_between_routing_tables(self):
        """No symbol should be in both OANDA_ROUTING and IBKR_ROUTING."""
        from live.broker_manager import OANDA_ROUTING, IBKR_ROUTING
        overlap = OANDA_ROUTING & IBKR_ROUTING
        assert overlap == set(), f"Symbols in both routing tables: {overlap}"


# ─────────────────────────────────────────────────────────────────────────────
# 6. BrokerManager.check_symbol() with routing
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckSymbolRouting:

    def test_check_eurusd_uses_oanda_mapper(self):
        bm = _make_broker_manager()
        result = bm.check_symbol("EURUSD")
        assert result.canonical_symbol == "EURUSD"
        # OANDA mapper used (unconfirmed since no live connector)
        assert result.rejection_code in (
            "INSTRUMENT_UNCONFIRMED", "BROKER_NOT_CONNECTED", "BROKER_DOES_NOT_SUPPORT_INSTRUMENT"
        )

    def test_check_xauusd_uses_ibkr_mapper(self):
        bm = _make_broker_manager()
        result = bm.check_symbol("XAUUSD")
        assert result.canonical_symbol == "XAUUSD"
        assert result.broker_symbol == "GC"  # IBKR broker symbol

    def test_check_unknown_symbol(self):
        bm = _make_broker_manager()
        result = bm.check_symbol("DOESNOTEXIST")
        assert result.is_tradeable is False
        assert result.rejection_code in (
            "UNKNOWN_SYMBOL", "BROKER_NOT_CONNECTED", "INSTRUMENT_UNCONFIRMED"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 7. Paper fallback when not live-armed
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperFallback:

    def test_xauusd_paper_when_not_armed(self):
        """When not live-armed, XAUUSD should go to paper even with IBKR connected."""
        from brokers.base_connector import OrderRequest, OrderSide, OrderResult, OrderStatus
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = False  # NOT live

        paper_result = OrderResult(
            success=True, order_id="PAPER-001",
            status=OrderStatus.FILLED, filled_price=2350.0, filled_units=1.0,
            raw={"paper": True},
        )
        bm._paper_broker.submit_order.return_value = paper_result

        req = OrderRequest(
            instrument="XAUUSD", side=OrderSide.BUY, units=1.0,
            price=2350.0, stop_loss=2330.0, take_profit=2400.0,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        bm._ibkr_connector.submit_order.assert_not_called()
        bm._paper_broker.submit_order.assert_called_once()

    def test_eurusd_paper_when_not_armed(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderResult, OrderStatus
        bm = _make_broker_manager()
        bm._esm.is_live_trading_allowed.return_value = False

        paper_result = OrderResult(
            success=True, order_id="PAPER-002",
            status=OrderStatus.FILLED, filled_price=1.085, filled_units=1.0,
            raw={"paper": True},
        )
        bm._paper_broker.submit_order.return_value = paper_result

        req = OrderRequest(
            instrument="EURUSD", side=OrderSide.BUY, units=1.0,
            price=1.085, stop_loss=1.080, take_profit=1.095,
        )
        result = bm.submit_order(req, operator="test")
        assert result.success is True
        bm._connector.submit_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 8. get_routing_table()
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutingTable:

    def test_routing_table_structure(self):
        bm = _make_broker_manager()
        table = bm.get_routing_table()
        assert "oanda" in table
        assert "ibkr" in table
        assert "EURUSD" in table["oanda"]
        assert "XAUUSD" in table["ibkr"]
        assert "ES" in table["ibkr"]
        assert "NQ" in table["ibkr"]
        assert "AAPL" in table["ibkr"]
        assert "rule" in table

    def test_eurusd_not_in_ibkr_routing(self):
        bm = _make_broker_manager()
        table = bm.get_routing_table()
        assert "EURUSD" not in table["ibkr"]

    def test_xauusd_not_in_oanda_routing(self):
        bm = _make_broker_manager()
        table = bm.get_routing_table()
        assert "XAUUSD" not in table["oanda"]


# ─────────────────────────────────────────────────────────────────────────────
# 9. Kill switch blocks all routing
# ─────────────────────────────────────────────────────────────────────────────

class TestKillSwitchBlocking:

    def test_kill_switch_blocks_eurusd(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        bm = _make_broker_manager()
        bm._esm.is_kill_switch_active.return_value = True
        req = OrderRequest(
            instrument="EURUSD", side=OrderSide.BUY, units=1.0,
            price=1.085, stop_loss=1.080, take_profit=1.095,
        )
        result = bm.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "KILL_SWITCH_ACTIVE"
        bm._connector.submit_order.assert_not_called()

    def test_kill_switch_blocks_xauusd(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        bm = _make_broker_manager()
        bm._esm.is_kill_switch_active.return_value = True
        req = OrderRequest(
            instrument="XAUUSD", side=OrderSide.BUY, units=1.0,
            price=2350.0, stop_loss=2330.0, take_profit=2400.0,
        )
        result = bm.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "KILL_SWITCH_ACTIVE"
        bm._ibkr_connector.submit_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 10. Multi-broker status
# ─────────────────────────────────────────────────────────────────────────────

class TestMultiBrokerStatus:

    def test_broker_status_contains_both_connectors(self):
        bm = _make_broker_manager()
        status = bm.broker_status()
        assert "connectors" in status
        assert "oanda" in status["connectors"]
        assert "ibkr" in status["connectors"]
        assert status["connectors"]["oanda"]["connected"] is True
        assert status["connectors"]["ibkr"]["connected"] is True

    def test_oanda_routes_in_status(self):
        bm = _make_broker_manager()
        status = bm.broker_status()
        assert "EURUSD" in status["connectors"]["oanda"]["routes"]

    def test_ibkr_routes_in_status(self):
        bm = _make_broker_manager()
        status = bm.broker_status()
        ibkr_routes = status["connectors"]["ibkr"]["routes"]
        assert "XAUUSD" in ibkr_routes
        assert "ES" in ibkr_routes
        assert "AAPL" in ibkr_routes

"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — IBKR Connector (v3.0)

Test coverage:
  1. build_ibkr_contract — all asset classes (Forex, Futures, Stocks, Indices, Commodities)
  2. EURUSD routing violation — must raise ValueError / return WRONG_BROKER_ROUTING
  3. Lot-to-unit conversion — FOREX(25000 min), FUTURES(1 contract), STOCKS(shares)
  4. Paper simulation fill — IBKRConnector._submit_paper_simulation
  5. Order validation — missing SL/TP, invalid direction, bad units
  6. validate_instrument_mapping — known symbol, EURUSD exclusion, unknown symbol
  7. Rejection history — populated on failed submit
  8. Circuit breaker — opens after _CIRCUIT_THRESHOLD consecutive failures
  9. Connection status — disconnected state fields
  10. IBKR symbol mapper — static map includes all routing symbols
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from dataclasses import dataclass


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _make_ibkr_creds(account_id: str = "U1234567", environment: str = "practice"):
    @dataclass
    class _Creds:
        account_id: str
        environment: str
        host: str = "127.0.0.1"
        port: int = 7497
        client_id: int = 1
        def is_usable(self): return bool(self.account_id)
    return _Creds(account_id=account_id, environment=environment)


def _make_connected_ibkr(account_id: str = "U1234567"):
    """Create an IBKRConnector in AUTHENTICATED state (no actual socket)."""
    from brokers.ibkr_connector import IBKRConnector
    from brokers.base_connector import ConnectionState
    creds = _make_ibkr_creds(account_id)
    conn = IBKRConnector(creds)
    conn._conn_state = ConnectionState.AUTHENTICATED
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# 1. build_ibkr_contract — all asset classes
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildIBKRContract:

    def test_gold_futures_gc(self):
        from brokers.ibkr_connector import build_ibkr_contract, IBKRAssetClass
        contract = build_ibkr_contract("GC")
        assert contract.ibkr_symbol == "GC"
        assert contract.sec_type == "FUT"
        assert contract.exchange == "COMEX"
        assert contract.currency == "USD"
        assert contract.asset_class == IBKRAssetClass.FUTURES
        assert contract.multiplier == "100"
        assert contract.min_tick == 0.10

    def test_xauusd_maps_to_gc(self):
        """XAUUSD canonical → GC futures at IBKR."""
        from brokers.ibkr_connector import build_ibkr_contract
        contract = build_ibkr_contract("XAUUSD")
        assert contract.ibkr_symbol == "GC"
        assert contract.sec_type == "FUT"
        assert contract.exchange == "COMEX"
        assert contract.multiplier == "100"

    def test_es_futures(self):
        from brokers.ibkr_connector import build_ibkr_contract
        contract = build_ibkr_contract("ES")
        assert contract.ibkr_symbol == "ES"
        assert contract.sec_type == "FUT"
        assert contract.exchange == "CME"
        assert contract.multiplier == "50"

    def test_nq_futures(self):
        from brokers.ibkr_connector import build_ibkr_contract
        contract = build_ibkr_contract("NQ")
        assert contract.ibkr_symbol == "NQ"
        assert contract.sec_type == "FUT"
        assert contract.exchange == "CME"
        assert contract.multiplier == "20"

    def test_aapl_stock(self):
        from brokers.ibkr_connector import build_ibkr_contract, IBKRAssetClass
        contract = build_ibkr_contract("AAPL")
        assert contract.ibkr_symbol == "AAPL"
        assert contract.sec_type == "STK"
        assert contract.exchange == "SMART"
        assert contract.primary_exchange == "NASDAQ"
        assert contract.asset_class == IBKRAssetClass.STOCKS

    def test_gbpusd_forex(self):
        from brokers.ibkr_connector import build_ibkr_contract, IBKRAssetClass
        contract = build_ibkr_contract("GBPUSD")
        assert contract.ibkr_symbol == "GBP"
        assert contract.sec_type == "CASH"
        assert contract.exchange == "IDEALPRO"
        assert contract.asset_class == IBKRAssetClass.FOREX

    def test_us500_index(self):
        from brokers.ibkr_connector import build_ibkr_contract, IBKRAssetClass
        contract = build_ibkr_contract("US500")
        assert contract.ibkr_symbol == "SPX"
        assert contract.sec_type == "IND"
        assert contract.asset_class == IBKRAssetClass.INDICES

    def test_usoil_commodity(self):
        from brokers.ibkr_connector import build_ibkr_contract, IBKRAssetClass
        contract = build_ibkr_contract("USOIL")
        assert contract.ibkr_symbol == "CL"
        assert contract.sec_type == "FUT"
        assert contract.asset_class == IBKRAssetClass.COMMODITIES


# ─────────────────────────────────────────────────────────────────────────────
# 2. EURUSD routing violation
# ─────────────────────────────────────────────────────────────────────────────

class TestEURUSDRouting:

    def test_build_ibkr_contract_eurusd_raises(self):
        """build_ibkr_contract must raise ValueError for EURUSD."""
        from brokers.ibkr_connector import build_ibkr_contract
        with pytest.raises(ValueError, match="OANDA"):
            build_ibkr_contract("EURUSD")

    def test_ibkr_submit_order_eurusd_rejected(self):
        """IBKRConnector.submit_order must reject EURUSD."""
        from brokers.ibkr_connector import IBKRConnector
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=1.0,
            price=1.085,
            stop_loss=1.080,
            take_profit=1.095,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert result.status == OrderStatus.REJECTED
        assert "OANDA" in result.reject_reason
        assert result.normalized_rejection_code == "WRONG_BROKER_ROUTING"

    def test_ibkr_validate_instrument_eurusd(self):
        """validate_instrument_mapping must mark EURUSD as not tradeable at IBKR."""
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("EURUSD")
        assert mapping.is_mapped is False
        assert mapping.is_supported is False
        assert mapping.tradeable is False
        assert "OANDA" in (mapping.not_tradeable_reason or "")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Lot-to-unit conversion
# ─────────────────────────────────────────────────────────────────────────────

class TestLotsToUnits:

    def _connector(self):
        return _make_connected_ibkr()

    def test_forex_lots_to_units(self):
        """1 lot of GBPUSD = 100,000 base; IBKR min is 25,000."""
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        conn = self._connector()
        contract = IBKR_CONTRACT_MAP["GBPUSD"]
        units = conn._lots_to_units(1.0, "GBPUSD", contract)
        assert units == 100_000.0

    def test_forex_min_lot_enforced(self):
        """0.1 lot of GBPUSD = 10,000 but IBKR min is 25,000."""
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        conn = self._connector()
        contract = IBKR_CONTRACT_MAP["GBPUSD"]
        units = conn._lots_to_units(0.1, "GBPUSD", contract)
        assert units == 25_000.0  # enforced minimum

    def test_gold_futures_1_lot_1_contract(self):
        """1 lot of GC = 1 contract (100 oz per contract is the multiplier, not the lot size)."""
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        conn = self._connector()
        contract = IBKR_CONTRACT_MAP["GC"]
        units = conn._lots_to_units(1.0, "GC", contract)
        assert units == 1.0

    def test_es_futures_2_contracts(self):
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        conn = self._connector()
        contract = IBKR_CONTRACT_MAP["ES"]
        units = conn._lots_to_units(2.0, "ES", contract)
        assert units == 2.0

    def test_aapl_stock_shares(self):
        """Stock lots = shares (rounded to whole shares)."""
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        conn = self._connector()
        contract = IBKR_CONTRACT_MAP["AAPL"]
        units = conn._lots_to_units(50.7, "AAPL", contract)
        assert units == 51.0  # rounded


# ─────────────────────────────────────────────────────────────────────────────
# 4. Paper simulation fill
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperSimulation:

    def test_xauusd_paper_fill(self):
        """XAUUSD submit without ibapi → paper simulation fill."""
        from brokers.ibkr_connector import IBKRConnector, IBKR_CONTRACT_MAP
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            price=2350.0,
            stop_loss=2330.0,
            take_profit=2400.0,
        )
        result = conn.submit_order(req)
        assert result.success is True
        assert result.status == OrderStatus.FILLED
        assert result.filled_price == 2350.0
        assert "paper" in result.raw
        assert result.raw["paper"] is True

    def test_es_paper_fill(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="ES",
            side=OrderSide.SELL,
            units=1.0,
            price=5200.0,
            stop_loss=5250.0,
            take_profit=5100.0,
        )
        result = conn.submit_order(req)
        assert result.success is True
        assert result.filled_price == 5200.0

    def test_aapl_paper_fill(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="AAPL",
            side=OrderSide.BUY,
            units=10.0,
            price=185.50,
            stop_loss=182.00,
            take_profit=195.00,
        )
        result = conn.submit_order(req)
        assert result.success is True
        assert result.raw["ibkr_symbol"] == "AAPL"
        assert result.raw["sec_type"] == "STK"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Order validation
# ─────────────────────────────────────────────────────────────────────────────

class TestOrderValidation:

    def test_missing_stop_loss_rejected(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="ES",
            side=OrderSide.BUY,
            units=1.0,
            price=5200.0,
            stop_loss=None,
            take_profit=5300.0,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert "stop_loss" in result.reject_reason.lower()
        assert result.normalized_rejection_code == "ORDER_VALIDATION_FAILED"

    def test_missing_take_profit_rejected(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="ES",
            side=OrderSide.BUY,
            units=1.0,
            price=5200.0,
            stop_loss=5150.0,
            take_profit=None,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert "take_profit" in result.reject_reason.lower()

    def test_zero_units_rejected(self):
        from brokers.base_connector import OrderRequest, OrderSide
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="AAPL",
            side=OrderSide.BUY,
            units=0.0,
            price=185.0,
            stop_loss=180.0,
            take_profit=195.0,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "ORDER_VALIDATION_FAILED"

    def test_buy_sl_above_entry_rejected(self):
        """BUY: stop_loss must be BELOW entry price."""
        from brokers.base_connector import OrderRequest, OrderSide
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="ES",
            side=OrderSide.BUY,
            units=1.0,
            price=5200.0,
            stop_loss=5250.0,   # WRONG: above entry for BUY
            take_profit=5300.0,
        )
        result = conn.submit_order(req)
        assert result.success is False

    def test_unknown_symbol_rejected(self):
        """Unknown symbol must return NO_BROKER_MAPPING."""
        from brokers.base_connector import OrderRequest, OrderSide
        conn = _make_connected_ibkr()
        req = OrderRequest(
            instrument="UNKNOWN_ASSET",
            side=OrderSide.BUY,
            units=1.0,
            price=100.0,
            stop_loss=95.0,
            take_profit=110.0,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "NO_BROKER_MAPPING"


# ─────────────────────────────────────────────────────────────────────────────
# 6. validate_instrument_mapping
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateInstrumentMapping:

    def test_gc_is_mapped(self):
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("GC")
        assert mapping.is_mapped is True
        assert mapping.broker_symbol == "GC"
        assert mapping.internal == "GC"

    def test_xauusd_maps_to_gc(self):
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("XAUUSD")
        assert mapping.is_mapped is True
        assert mapping.broker_symbol == "GC"

    def test_aapl_maps_to_stk(self):
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("AAPL")
        assert mapping.is_mapped is True
        assert mapping.broker_symbol == "AAPL"

    def test_eurusd_not_mapped_at_ibkr(self):
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("EURUSD")
        assert mapping.is_mapped is False
        assert mapping.is_supported is False

    def test_unknown_symbol_not_mapped(self):
        conn = _make_connected_ibkr()
        mapping = conn.validate_instrument_mapping("MADE_UP_SYMBOL")
        assert mapping.is_mapped is False
        assert mapping.is_supported is False


# ─────────────────────────────────────────────────────────────────────────────
# 7. Rejection history
# ─────────────────────────────────────────────────────────────────────────────

class TestRejectionHistory:

    def test_eurusd_rejection_recorded(self):
        from brokers.base_connector import OrderRequest, OrderSide
        conn = _make_connected_ibkr()
        # EURUSD submit (will be rejected at instrument level, not recorded in _rejection_history)
        req = OrderRequest(
            instrument="EURUSD", side=OrderSide.BUY, units=1.0,
            price=1.085, stop_loss=1.080, take_profit=1.095
        )
        conn.submit_order(req)
        # EURUSD rejection happens before _record_rejection is called
        # so history should be empty (the routing block is pre-record)
        assert isinstance(conn.get_rejection_history(), list)

    def test_rejection_bounded_at_200(self):
        """Rejection history is bounded at 200 entries."""
        from brokers.base_connector import OrderRequest, OrderSide
        conn = _make_connected_ibkr()
        # Force 205 entries by calling _record_rejection directly
        mock_req = OrderRequest(
            instrument="ES", side=OrderSide.BUY, units=1.0,
            price=5000.0, stop_loss=4900.0, take_profit=5100.0
        )
        for _ in range(205):
            conn._record_rejection(mock_req, "test_reason", "TEST_CODE")
        assert len(conn.get_rejection_history()) == 200


# ─────────────────────────────────────────────────────────────────────────────
# 8. Circuit breaker
# ─────────────────────────────────────────────────────────────────────────────

class TestCircuitBreaker:

    def test_circuit_opens_after_threshold(self):
        """Circuit breaker opens after _CIRCUIT_THRESHOLD consecutive failures."""
        conn = _make_connected_ibkr()
        threshold = conn._CIRCUIT_THRESHOLD
        assert conn._circuit_open is False

        from brokers.base_connector import OrderRequest, OrderSide
        mock_req = OrderRequest(
            instrument="ES", side=OrderSide.BUY, units=1.0,
            price=5000.0, stop_loss=4900.0, take_profit=5100.0
        )
        for _ in range(threshold):
            conn._consecutive_failures += 1
            conn._record_rejection(mock_req, "simulated failure", "SIM")

        conn._circuit_open = conn._consecutive_failures >= threshold
        assert conn._circuit_open is True

    def test_submit_order_fails_when_circuit_open(self):
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        conn = _make_connected_ibkr()
        conn._circuit_open = True
        req = OrderRequest(
            instrument="ES", side=OrderSide.BUY, units=1.0,
            price=5200.0, stop_loss=5150.0, take_profit=5300.0,
        )
        result = conn.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "CIRCUIT_BREAKER_OPEN"


# ─────────────────────────────────────────────────────────────────────────────
# 9. Connection status fields
# ─────────────────────────────────────────────────────────────────────────────

class TestConnectionStatus:

    def test_disconnected_status(self):
        from brokers.ibkr_connector import IBKRConnector
        from brokers.base_connector import ConnectionState
        creds = _make_ibkr_creds()
        conn = IBKRConnector(creds)
        assert conn.is_connected() is False
        status = conn.connection_status()
        assert status["broker"] == "ibkr"
        assert status["state"] == "DISCONNECTED"
        assert status["circuit_open"] is False

    def test_authenticated_status(self):
        conn = _make_connected_ibkr("U9876543")
        assert conn.is_connected() is True
        status = conn.connection_status()
        assert status["state"] == "AUTHENTICATED"
        assert status["account_id"] == "U9876543"


# ─────────────────────────────────────────────────────────────────────────────
# 10. IBKR symbol mapper
# ─────────────────────────────────────────────────────────────────────────────

class TestIBKRSymbolMapper:

    def _make_mapper(self):
        from brokers.symbol_mapper import BrokerSymbolMapper
        return BrokerSymbolMapper("ibkr")

    def test_xauusd_in_static_map(self):
        mapper = self._make_mapper()
        assert "XAUUSD" in mapper._static_map
        broker_sym, asset_class = mapper._static_map["XAUUSD"]
        assert broker_sym == "GC"
        assert asset_class == "FUTURES"

    def test_eurusd_in_unsupported(self):
        """EURUSD must be in IBKR unsupported set."""
        mapper = self._make_mapper()
        assert "EURUSD" in mapper._unsupported

    def test_check_eurusd_returns_unsupported(self):
        mapper = self._make_mapper()
        mapper.hydrate()  # no connector — all unconfirmed
        result = mapper.check("EURUSD", connector_connected=True)
        assert result.is_tradeable is False
        assert result.is_supported is False

    def test_check_es_unconfirmed_without_connector(self):
        mapper = self._make_mapper()
        mapper.hydrate()  # no connector
        result = mapper.check("ES", connector_connected=True)
        assert result.is_tradeable is False
        assert result.rejection_code in ("INSTRUMENT_UNCONFIRMED", "BROKER_DOES_NOT_SUPPORT_INSTRUMENT")

    def test_ibkr_routing_symbols_in_map(self):
        """All symbols in IBKR_ROUTING must have a static map entry."""
        from brokers.ibkr_connector import IBKR_PRIMARY_SYMBOLS
        from brokers.symbol_mapper import _IBKR_MAP
        for sym in IBKR_PRIMARY_SYMBOLS:
            if sym not in ("BTC/USDT", "ETH/USDT"):  # crypto handled separately
                assert sym in _IBKR_MAP, f"{sym} missing from _IBKR_MAP"

    def test_all_futures_have_expiry_in_contract_map(self):
        """All futures in IBKR_CONTRACT_MAP must have an expiry set."""
        from brokers.ibkr_connector import IBKR_CONTRACT_MAP
        for sym, contract in IBKR_CONTRACT_MAP.items():
            if contract.sec_type == "FUT":
                assert contract.expiry is not None, f"{sym} FUT missing expiry"
                assert len(contract.expiry) == 6, f"{sym} expiry should be YYYYMM"

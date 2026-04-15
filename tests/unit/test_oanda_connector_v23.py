"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — OANDA Connector v2.3

Covers:
  1. Gold payload validation (_validate_gold_payload)
       - units minimum enforcement
       - SL/TP presence
       - SL/TP minimum distance
       - non-gold instruments bypass check
  2. Lot-to-unit conversion (_lots_to_units)
       - XAU_USD: 1 lot = 100 oz
       - EUR_USD: 1 lot = 1 unit (no multiplier)
  3. OANDA error code normalization (_normalize_oanda_error)
       - known OANDA codes → internal codes
       - substring matching
       - unknown codes → BROKER_REJECTION
  4. validate_instrument_mapping granular flags
       - static fallback: is_supported=False, tradeable=False
       - live API success: is_supported=True, is_tradeable_metadata from broker
       - live API empty instruments: is_supported=False
  5. submit_order structured rejection
       - raw_broker_error preserved
       - parsed_error_code preserved
       - normalized_rejection_code set
       - gold units conversion in payload
  6. build_test_order_payload
       - gold units converted
       - payload contains correct broker symbol
  7. validate_xau_payload dry-run probe
"""
from __future__ import annotations

import sys
import os
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from brokers.oanda_connector import (
    OandaConnector, OANDA_INSTRUMENT_MAP, OANDA_PRECISION,
    GOLD_INSTRUMENTS, LOT_UNIT_MULTIPLIER, MIN_SLTP_DISTANCE,
    _normalize_oanda_error,
)
from brokers.base_connector import (
    InstrumentMapping, OrderRequest, OrderResult, OrderSide, OrderStatus, OrderType,
    ConnectionState,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_creds(env: str = "practice") -> MagicMock:
    creds = MagicMock()
    creds.api_token = "fake-token-12345"
    creds.account_id = "101-001-test-001"
    creds.environment = env
    creds.is_usable.return_value = True
    creds.validation_errors = []
    return creds


def _make_connected_connector(env: str = "practice") -> OandaConnector:
    """Return a connector whose _connection_state is AUTHENTICATED."""
    creds = _make_creds(env)
    c = OandaConnector(creds)
    c._connection_state = ConnectionState.AUTHENTICATED
    c._session = MagicMock()
    return c


def _mock_get(connector, status: int, body: dict):
    """Patch connector._get to return (status, body)."""
    connector._get = MagicMock(return_value=(status, body))


def _mock_post(connector, status: int, body: dict):
    """Patch connector._post to return (status, body)."""
    connector._post = MagicMock(return_value=(status, body))


# ---------------------------------------------------------------------------
# 1. Gold payload validation
# ---------------------------------------------------------------------------

class TestGoldPayloadValidation:
    """Tests for OandaConnector._validate_gold_payload."""

    def _conn(self):
        return _make_connected_connector()

    def test_non_gold_bypasses_check(self):
        c = self._conn()
        valid, err = c._validate_gold_payload("EUR_USD", 100000, 1.09, 1.11, 1.10)
        assert valid is True
        assert err is None

    def test_gold_min_units_enforced(self):
        """Units < 1 should fail for XAU_USD."""
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 0.5, 2290.0, 2320.0, 2300.0)
        assert valid is False
        assert err is not None
        assert "1 troy oz" in err or "minimum" in err.lower()

    def test_gold_min_units_exact_boundary(self):
        """Units == 1 should pass."""
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 1.0, 2290.0, 2320.0, 2300.0)
        assert valid is True

    def test_gold_min_units_large_order(self):
        """100 units (1 standard lot) should pass."""
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 100.0, 2290.0, 2320.0, 2300.0)
        assert valid is True

    def test_sl_required(self):
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 10.0, None, 2320.0, 2300.0)
        assert valid is False
        assert "stop_loss" in err.lower() or "required" in err.lower()

    def test_tp_required(self):
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 10.0, 2290.0, None, 2300.0)
        assert valid is False
        assert "take_profit" in err.lower() or "required" in err.lower()

    def test_sl_distance_too_small(self):
        """SL closer than $1 (min_dist) should fail."""
        c = self._conn()
        # entry=2300, sl=2299.5 → dist=0.5 < 1.0
        valid, err = c._validate_gold_payload("XAU_USD", 10.0, 2299.5, 2320.0, 2300.0)
        assert valid is False
        assert "SL distance" in err or "minimum" in err.lower()

    def test_tp_distance_too_small(self):
        """TP closer than $1 should fail."""
        c = self._conn()
        # entry=2300, tp=2300.3 → dist=0.3 < 1.0
        valid, err = c._validate_gold_payload("XAU_USD", 10.0, 2290.0, 2300.3, 2300.0)
        assert valid is False
        assert "TP distance" in err or "minimum" in err.lower()

    def test_valid_gold_payload(self):
        """Standard XAU BUY with entry=2300, sl=2290, tp=2325 is valid."""
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 100.0, 2290.0, 2325.0, 2300.0)
        assert valid is True
        assert err is None

    def test_no_entry_price_skips_distance_check(self):
        """If entry_price is None, distance checks are skipped (units still checked)."""
        c = self._conn()
        valid, err = c._validate_gold_payload("XAU_USD", 10.0, 2290.0, 2320.0, None)
        assert valid is True


# ---------------------------------------------------------------------------
# 2. Lot-to-unit conversion
# ---------------------------------------------------------------------------

class TestLotToUnitConversion:

    def _conn(self):
        return _make_connected_connector()

    def test_xau_usd_1_lot_equals_100_oz(self):
        c = self._conn()
        assert c._lots_to_units("XAU_USD", 1.0) == 100.0

    def test_xau_usd_half_lot(self):
        c = self._conn()
        assert c._lots_to_units("XAU_USD", 0.5) == 50.0

    def test_xau_usd_01_lot(self):
        """0.01 lots = 1 oz for XAU_USD."""
        c = self._conn()
        assert c._lots_to_units("XAU_USD", 0.01) == pytest.approx(1.0)

    def test_eur_usd_no_conversion(self):
        """EUR_USD has no multiplier — units pass through unchanged."""
        c = self._conn()
        assert c._lots_to_units("EUR_USD", 0.10) == pytest.approx(0.10)

    def test_unknown_instrument_no_conversion(self):
        c = self._conn()
        assert c._lots_to_units("UNKNOWN_XYZ", 1.0) == 1.0

    def test_xag_usd_multiplier(self):
        c = self._conn()
        assert c._lots_to_units("XAG_USD", 1.0) == 5000.0


# ---------------------------------------------------------------------------
# 3. Error code normalization
# ---------------------------------------------------------------------------

class TestOandaErrorNormalization:

    def test_instrument_not_tradeable(self):
        assert _normalize_oanda_error("INSTRUMENT_NOT_TRADEABLE") == "INSTRUMENT_NOT_TRADEABLE"

    def test_instrument_halted(self):
        assert _normalize_oanda_error("INSTRUMENT_HALTED") == "INSTRUMENT_NOT_TRADEABLE"

    def test_market_halted(self):
        assert _normalize_oanda_error("MARKET_HALTED") == "INSTRUMENT_NOT_TRADEABLE"

    def test_account_not_tradeable(self):
        assert _normalize_oanda_error("ACCOUNT_NOT_TRADEABLE_ON_INSTRUMENT") == "BROKER_DOES_NOT_SUPPORT_INSTRUMENT"

    def test_insufficient_margin(self):
        assert _normalize_oanda_error("INSUFFICIENT_MARGIN") == "INSUFFICIENT_MARGIN"

    def test_insufficient_funds(self):
        assert _normalize_oanda_error("INSUFFICIENT_FUNDS") == "INSUFFICIENT_FUNDS"

    def test_units_minimum_not_met(self):
        assert _normalize_oanda_error("UNITS_MINIMUM_NOT_MET") == "GOLD_UNITS_TOO_SMALL"

    def test_stop_loss_missing(self):
        assert _normalize_oanda_error("STOP_LOSS_ON_FILL_PRICE_MISSING") == "GOLD_SLTP_MISSING"

    def test_take_profit_invalid(self):
        assert _normalize_oanda_error("TAKE_PROFIT_ON_FILL_PRICE_INVALID") == "GOLD_SLTP_INVALID"

    def test_closeout_bid_less_than_stop(self):
        assert _normalize_oanda_error("CLOSEOUT_BID_LESS_THAN_STOP_PRICE") == "GOLD_SLTP_INVALID"

    def test_unknown_code_returns_broker_rejection(self):
        assert _normalize_oanda_error("SOME_RANDOM_CODE") == "BROKER_REJECTION"

    def test_empty_string(self):
        assert _normalize_oanda_error("") == "UNKNOWN_BROKER_ERROR"

    def test_none_like(self):
        assert _normalize_oanda_error(None) == "UNKNOWN_BROKER_ERROR"

    def test_substring_not_tradeable(self):
        """Any code containing NOT_TRADEABLE should normalize."""
        assert _normalize_oanda_error("INSTRUMENT_NOT_TRADEABLE_SOME_SUFFIX") == "INSTRUMENT_NOT_TRADEABLE"

    def test_substring_insufficient_margin(self):
        assert _normalize_oanda_error("MARGIN_RATE_WOULD_TRIGGER_CLOSEOUT") == "INSUFFICIENT_MARGIN"

    def test_case_insensitive(self):
        assert _normalize_oanda_error("instrument_not_tradeable") == "INSTRUMENT_NOT_TRADEABLE"


# ---------------------------------------------------------------------------
# 4. validate_instrument_mapping granular flags
# ---------------------------------------------------------------------------

class TestValidateInstrumentMappingGranular:

    def _make_connector(self):
        return _make_connected_connector()

    def test_static_fallback_marks_is_supported_false(self):
        """When API call raises, static fallback has is_supported=False, tradeable=False."""
        c = self._make_connector()
        c._get = MagicMock(side_effect=ConnectionError("timeout"))

        mapping = c.validate_instrument_mapping("XAUUSD")
        assert mapping is not None
        assert mapping.is_mapped is True
        assert mapping.is_supported is False
        assert mapping.is_tradeable_metadata is False
        assert mapping.tradeable is False
        assert mapping.not_tradeable_reason is not None
        # Message should indicate tradeability could not be confirmed
        reason_lower = mapping.not_tradeable_reason.lower()
        assert "not confirmed" in reason_lower or "unreachable" in reason_lower

    def test_live_api_tradeable_sets_all_flags_true(self):
        """When broker API returns instrument with status=tradeable, all flags are True."""
        c = self._make_connector()
        _mock_get(c, 200, {
            "instruments": [{
                "name": "XAU_USD",
                "status": "tradeable",
                "minimumTradeSize": "1",
                "maximumOrderUnits": "10000",
                "marginRate": "0.05",
            }]
        })
        mapping = c.validate_instrument_mapping("XAUUSD")
        assert mapping is not None
        assert mapping.is_mapped is True
        assert mapping.is_supported is True
        assert mapping.is_tradeable_metadata is True
        assert mapping.tradeable is True
        assert mapping.not_tradeable_reason is None

    def test_live_api_non_tradeable_sets_tradeable_false(self):
        """When broker API returns non-tradeable status, is_tradeable_metadata=False."""
        c = self._make_connector()
        _mock_get(c, 200, {
            "instruments": [{
                "name": "XAU_USD",
                "status": "non-tradeable",
                "minimumTradeSize": "1",
                "maximumOrderUnits": "10000",
                "marginRate": "0.05",
            }]
        })
        mapping = c.validate_instrument_mapping("XAUUSD")
        assert mapping is not None
        assert mapping.is_supported is True
        assert mapping.is_tradeable_metadata is False
        assert mapping.tradeable is False
        assert mapping.not_tradeable_reason is not None

    def test_live_api_empty_instruments_marks_not_supported(self):
        """When broker returns empty instruments list, instrument not on this account."""
        c = self._make_connector()
        _mock_get(c, 200, {"instruments": []})

        mapping = c.validate_instrument_mapping("XAUUSD")
        assert mapping is not None
        assert mapping.is_supported is False
        assert mapping.tradeable is False
        assert "not available" in mapping.not_tradeable_reason.lower()

    def test_unknown_symbol_returns_none(self):
        """Symbol not in OANDA_INSTRUMENT_MAP returns None."""
        c = self._make_connector()
        result = c.validate_instrument_mapping("DOGEUSD")
        assert result is None

    def test_eurusd_live_api_success(self):
        """EURUSD with live API success."""
        c = self._make_connector()
        _mock_get(c, 200, {
            "instruments": [{
                "name": "EUR_USD",
                "status": "tradeable",
                "minimumTradeSize": "1",
                "maximumOrderUnits": "100000000",
                "marginRate": "0.02",
            }]
        })
        mapping = c.validate_instrument_mapping("EURUSD")
        assert mapping is not None
        assert mapping.broker_symbol == "EUR_USD"
        assert mapping.is_supported is True
        assert mapping.tradeable is True

    def test_raw_broker_metadata_preserved(self):
        """raw_broker_metadata should contain the instrument dict from OANDA."""
        c = self._make_connector()
        inst_data = {
            "name": "XAU_USD",
            "status": "tradeable",
            "minimumTradeSize": "1",
            "maximumOrderUnits": "10000",
            "marginRate": "0.05",
            "type": "METAL",
        }
        _mock_get(c, 200, {"instruments": [inst_data]})
        mapping = c.validate_instrument_mapping("XAUUSD")
        assert mapping.raw_broker_metadata == inst_data


# ---------------------------------------------------------------------------
# 5. submit_order structured rejection diagnostics
# ---------------------------------------------------------------------------

class TestSubmitOrderStructuredRejection:

    def _make_xau_request(self, lots: float = 0.10, entry: float = 2300.0) -> OrderRequest:
        return OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=lots,
            order_type=OrderType.MARKET,
            price=entry,
            stop_loss=2290.0,
            take_profit=2325.0,
        )

    def _make_eur_request(self) -> OrderRequest:
        return OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=0.10,
            order_type=OrderType.MARKET,
            price=1.10,
            stop_loss=1.09,
            take_profit=1.12,
        )

    def test_xau_rejection_preserves_raw_broker_error(self):
        """raw_broker_error in OrderResult must contain the OANDA rejectReason."""
        c = _make_connected_connector()
        _mock_post(c, 400, {
            "orderRejectTransaction": {
                "rejectReason": "INSTRUMENT_NOT_TRADEABLE",
                "type": "MARKET_ORDER_REJECT",
            },
            "errorMessage": "The instrument is not tradeable",
        })
        req = self._make_xau_request(lots=1.0)  # 1 lot = 100 oz (passes gold validation)
        result = c.submit_order(req)

        assert result.success is False
        assert result.raw_broker_error == "INSTRUMENT_NOT_TRADEABLE"
        assert result.parsed_error_code == "INSTRUMENT_NOT_TRADEABLE"
        assert result.normalized_rejection_code == "INSTRUMENT_NOT_TRADEABLE"

    def test_xau_rejection_insufficient_margin(self):
        """INSUFFICIENT_MARGIN maps correctly."""
        c = _make_connected_connector()
        _mock_post(c, 400, {
            "orderRejectTransaction": {
                "rejectReason": "INSUFFICIENT_MARGIN",
            },
        })
        req = self._make_xau_request(lots=1.0)
        result = c.submit_order(req)

        assert result.success is False
        assert result.insufficient_funds is True
        assert result.normalized_rejection_code == "INSUFFICIENT_MARGIN"

    def test_xau_fill_clears_diagnostics(self):
        """Successful fill: success=True, reject fields are None/empty."""
        c = _make_connected_connector()
        _mock_post(c, 201, {
            "orderFillTransaction": {
                "orderID": "987654",
                "price": "2301.50",
                "units": "100",
            }
        })
        req = self._make_xau_request(lots=1.0)
        result = c.submit_order(req)

        assert result.success is True
        assert result.order_id == "987654"
        assert result.filled_price == pytest.approx(2301.50)
        assert result.reject_reason is None
        assert result.normalized_rejection_code is None

    def test_gold_units_converted_in_payload(self):
        """0.10 lots → 10 oz should appear in the order body sent to OANDA."""
        c = _make_connected_connector()
        captured_bodies = []

        def capture_post(path, body):
            captured_bodies.append(body)
            return 400, {
                "orderRejectTransaction": {"rejectReason": "INSUFFICIENT_MARGIN"},
            }

        c._post = capture_post
        req = self._make_xau_request(lots=0.10, entry=2300.0)
        c.submit_order(req)

        assert len(captured_bodies) == 1
        units_sent = captured_bodies[0]["order"]["units"]
        # 0.10 lots × 100 oz/lot = 10.0 oz
        assert float(units_sent) == pytest.approx(10.0)

    def test_payload_snapshot_in_rejection_result(self):
        """OrderResult.payload_snapshot must contain the sent order body."""
        c = _make_connected_connector()
        _mock_post(c, 400, {
            "orderRejectTransaction": {"rejectReason": "INSUFFICIENT_MARGIN"},
        })
        req = self._make_xau_request(lots=1.0)
        result = c.submit_order(req)

        assert result.payload_snapshot is not None
        assert "order" in result.payload_snapshot
        assert result.payload_snapshot["order"]["instrument"] == "XAU_USD"

    def test_payload_snapshot_in_rejection_history(self):
        """Rejection history entry must include payload_snapshot."""
        c = _make_connected_connector()
        _mock_post(c, 400, {
            "orderRejectTransaction": {"rejectReason": "INSTRUMENT_NOT_TRADEABLE"},
        })
        req = self._make_xau_request(lots=1.0)
        c.submit_order(req)

        history = c.get_rejection_history()
        assert len(history) >= 1
        last = history[-1]
        assert "payload_snapshot" in last
        assert "canonical_symbol" in last
        assert "normalized_rejection_code" in last

    def test_eurusd_rejection_no_gold_conversion(self):
        """EUR_USD should not have units converted by lot multiplier."""
        c = _make_connected_connector()
        captured_bodies = []

        def capture_post(path, body):
            captured_bodies.append(body)
            return 400, {
                "orderRejectTransaction": {"rejectReason": "INSUFFICIENT_MARGIN"},
            }

        c._post = capture_post
        req = self._make_eur_request()
        c.submit_order(req)

        assert len(captured_bodies) == 1
        units_sent = captured_bodies[0]["order"]["units"]
        # 0.10 lots with no multiplier → 0.10 units (FX)
        # But OANDA actually wants integers for FX; 0.10 → 0 which would be int(0)
        # 0.10 is < 1, so it becomes str(0.10) since it's not int
        assert float(units_sent) == pytest.approx(0.10)

    def test_gold_payload_validation_blocks_tiny_units(self):
        """0.001 lots → 0.1 oz < 1 oz minimum → pre-submit validation blocks it."""
        c = _make_connected_connector()
        mock_called = []
        c._post = lambda *a, **kw: mock_called.append(a) or (400, {})

        req = self._make_xau_request(lots=0.001)  # 0.001 × 100 = 0.1 oz
        result = c.submit_order(req)

        # Should be rejected by gold payload validation, NOT by a broker call
        assert result.success is False
        assert result.normalized_rejection_code == "GOLD_PAYLOAD_INVALID"
        # No broker call should have been made
        assert len(mock_called) == 0

    def test_no_mapping_returns_no_broker_mapping_code(self):
        """Symbol with no OANDA mapping returns NO_BROKER_MAPPING."""
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="DOGEUSD",
            side=OrderSide.BUY,
            units=0.1,
            order_type=OrderType.MARKET,
        )
        result = c.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "NO_BROKER_MAPPING"


# ---------------------------------------------------------------------------
# 6. build_test_order_payload gold conversion
# ---------------------------------------------------------------------------

class TestBuildTestOrderPayload:

    def test_xau_payload_converts_lots_to_ounces(self):
        """build_test_order_payload should show converted units for gold."""
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=0.10,   # 0.10 lots
            order_type=OrderType.MARKET,
            price=2300.0,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        ok, payload = c.build_test_order_payload(req)
        assert ok is True
        assert payload["broker_symbol"] == "XAU_USD"
        assert payload["raw_lots"] == 0.10
        assert payload["broker_units"] == pytest.approx(10.0)  # 0.10 × 100
        assert payload["is_gold"] is True
        # Units string in payload body
        units_in_body = float(payload["payload"]["order"]["units"])
        assert units_in_body == pytest.approx(10.0)

    def test_xau_payload_sl_tp_format(self):
        """SL/TP prices should be rounded to XAU precision (2 d.p.)."""
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            price=2300.12345,
            stop_loss=2290.123,
            take_profit=2325.999,
        )
        ok, payload = c.build_test_order_payload(req)
        assert ok is True
        sl_str = payload["payload"]["order"]["stopLossOnFill"]["price"]
        tp_str = payload["payload"]["order"]["takeProfitOnFill"]["price"]
        # XAU_USD precision is 2 (per OANDA_PRECISION)
        assert sl_str == "2290.12"
        assert tp_str == "2326.0"

    def test_eur_payload_no_gold_conversion(self):
        """EURUSD payload should not apply lot multiplier."""
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=0.10,
            price=1.10,
            stop_loss=1.09,
            take_profit=1.12,
        )
        ok, payload = c.build_test_order_payload(req)
        assert ok is True
        assert payload["is_gold"] is False
        assert payload["broker_units"] == pytest.approx(0.10)

    def test_unknown_symbol_returns_error(self):
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="DOGEUSD",
            side=OrderSide.BUY,
            units=0.1,
        )
        ok, result = c.build_test_order_payload(req)
        assert ok is False
        assert "error" in result

    def test_gold_tiny_lots_validation_fails(self):
        """0.005 lots → 0.5 oz < 1 oz → payload invalid."""
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=0.005,
            price=2300.0,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        ok, result = c.build_test_order_payload(req)
        assert ok is False
        assert "validation_type" in result or "error" in result


# ---------------------------------------------------------------------------
# 7. validate_xau_payload dry-run probe
# ---------------------------------------------------------------------------

class TestValidateXauPayload:

    def test_probe_valid_xau_order(self):
        c = _make_connected_connector()
        # Patch validate_instrument_mapping to succeed
        c.validate_instrument_mapping = MagicMock(return_value=InstrumentMapping(
            internal="XAUUSD",
            broker_symbol="XAU_USD",
            tradeable=True,
            min_units=1.0,
            max_units=10000.0,
            precision=2,
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=True,
            is_tradeable_metadata=True,
        ))
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,      # 1 lot = 100 oz
            price=2300.0,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        result = c.validate_xau_payload(req)
        assert result["payload_valid"] is True
        assert result["is_gold"] is True
        assert result["broker_units"] == pytest.approx(100.0)
        assert len(result["validation_errors"]) == 0

    def test_probe_tiny_units_invalid(self):
        c = _make_connected_connector()
        c.validate_instrument_mapping = MagicMock(return_value=InstrumentMapping(
            internal="XAUUSD",
            broker_symbol="XAU_USD",
            tradeable=True,
            min_units=1.0,
            max_units=10000.0,
            precision=2,
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=True,
            is_tradeable_metadata=True,
        ))
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=0.005,    # 0.005 × 100 = 0.5 oz < 1 oz minimum
            price=2300.0,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        result = c.validate_xau_payload(req)
        assert result["payload_valid"] is False
        assert len(result["validation_errors"]) > 0

    def test_probe_non_gold_symbol(self):
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=0.10,
            price=1.10,
            stop_loss=1.09,
            take_profit=1.12,
        )
        result = c.validate_xau_payload(req)
        assert result["is_gold"] is False
        # EUR_USD passes gold checks (not a gold instrument)
        assert result["payload_valid"] is True

    def test_probe_unknown_symbol(self):
        c = _make_connected_connector()
        req = OrderRequest(
            instrument="DOGEUSD",
            side=OrderSide.BUY,
            units=0.1,
        )
        result = c.validate_xau_payload(req)
        assert len(result["validation_errors"]) > 0
        # broker_symbol should be None
        assert result["broker_symbol"] is None

    def test_probe_payload_snapshot_included(self):
        """Even for invalid payload, payload_snapshot should be present."""
        c = _make_connected_connector()
        c.validate_instrument_mapping = MagicMock(return_value=InstrumentMapping(
            internal="XAUUSD",
            broker_symbol="XAU_USD",
            tradeable=True,
            min_units=1.0,
            max_units=10000.0,
            precision=2,
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=True,
            is_tradeable_metadata=True,
        ))
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            price=2300.0,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        result = c.validate_xau_payload(req)
        assert "payload_snapshot" in result
        assert result["payload_snapshot"] is not None

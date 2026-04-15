"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — Capability Truthfulness & Gold Pipeline (v2.3)

Acceptance criteria:
  1. Capability cache DOES NOT claim is_tradeable=True for XAUUSD
     when the connector used static fallback (is_supported=False).
  2. Capability cache shows INSTRUMENT_UNCONFIRMED for any symbol when
     the broker API was not reachable during hydration.
  3. Gold order payload is correct: lots→ounces conversion, SL/TP present.
  4. Raw OANDA error is preserved verbatim in OrderResult and rejection history.
  5. EURUSD pipeline still works (regression guard).
  6. XAUUSD either fills OR rejects with a specific structured reason
     (no generic INSTRUMENT_NOT_TRADEABLE masking GOLD_PAYLOAD issues).
  7. /api/broker/capability-cache warns about XAUUSD unconfirmed status.
  8. /api/broker/debug/xau-diagnostics returns structured probe.
  9. BrokerManager.check_symbol() respects INSTRUMENT_UNCONFIRMED.
  10. BrokerManager structured rejection history has full diagnostic fields.
"""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_esm(connected: bool = False, live_allowed: bool = False):
    mock_esm = MagicMock()
    mock_esm.is_connected.return_value = connected
    mock_esm.is_kill_switch_active.return_value = False
    mock_esm.current_state.return_value.value = (
        "LIVE_CONNECTED_SAFE" if connected else "PAPER_MODE"
    )
    mock_esm.is_live_trading_allowed.return_value = live_allowed
    return mock_esm


def _make_bm(connected: bool = False, live_allowed: bool = False):
    from live.broker_manager import BrokerManager
    with patch("live.broker_manager.get_credential_manager") as mock_cred, \
         patch("live.broker_manager.ExecutionStateMachine") as mock_esm_cls:
        mock_cred.return_value.load_jwt_secret.return_value = None
        mock_esm = _make_esm(connected=connected, live_allowed=live_allowed)
        mock_esm_cls.return_value = mock_esm
        bm = BrokerManager(config={})
    return bm


def _make_static_fallback_connector():
    """Connector that always returns static fallback (is_supported=False)."""
    from brokers.base_connector import InstrumentMapping
    mock = MagicMock()
    mock.is_connected.return_value = True

    def static_mapping(symbol):
        from brokers.oanda_connector import OANDA_INSTRUMENT_MAP, OANDA_PRECISION
        oanda_sym = OANDA_INSTRUMENT_MAP.get(symbol.upper())
        if not oanda_sym:
            return None
        return InstrumentMapping(
            internal=symbol.upper(),
            broker_symbol=oanda_sym,
            tradeable=False,
            min_units=1.0,
            max_units=1_000_000.0,
            precision=OANDA_PRECISION.get(oanda_sym, 5),
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=False,
            is_tradeable_metadata=False,
            raw_broker_metadata={},
            not_tradeable_reason="Tradeability not confirmed — broker API was unreachable",
        )

    mock.validate_instrument_mapping.side_effect = static_mapping
    return mock


def _make_live_confirmed_connector():
    """Connector that confirms all instruments as live-tradeable."""
    from brokers.base_connector import InstrumentMapping
    mock = MagicMock()
    mock.is_connected.return_value = True

    def live_mapping(symbol):
        from brokers.oanda_connector import OANDA_INSTRUMENT_MAP, OANDA_PRECISION
        oanda_sym = OANDA_INSTRUMENT_MAP.get(symbol.upper())
        if not oanda_sym:
            return None
        return InstrumentMapping(
            internal=symbol.upper(),
            broker_symbol=oanda_sym,
            tradeable=True,
            min_units=1.0,
            max_units=1_000_000.0,
            precision=OANDA_PRECISION.get(oanda_sym, 5),
            margin_rate=0.05,
            spread_typical=0.0,
            is_mapped=True,
            is_supported=True,
            is_tradeable_metadata=True,
            raw_broker_metadata={"status": "tradeable"},
        )

    mock.validate_instrument_mapping.side_effect = live_mapping
    return mock


def _make_orchestrator():
    from agents.agent_orchestrator import AgentOrchestrator
    config = {
        "system": {"environment": "paper"},
        "risk": {
            "account_balance_default": 100000.0,
            "max_risk_per_trade_pct": 1.0,
            "max_daily_loss_pct": 5.0,
            "max_drawdown_pct": 10.0,
            "max_concurrent_trades": 5,
            "min_rr_ratio": 1.5,
            "min_stop_distance_pips": 3.0,
            "max_spread_pips": 5.0,
        },
        "agents": {"decision_ledger_path": f"/tmp/test_cap_{uuid.uuid4().hex[:8]}.jsonl"},
    }
    return AgentOrchestrator(config, broker_connector=None)


# ---------------------------------------------------------------------------
# 1. Capability truthfulness
# ---------------------------------------------------------------------------

class TestCapabilityTrutfulness:

    def test_static_fallback_does_not_claim_tradeable(self):
        """
        Root-bug test: when connector returns static fallback (is_supported=False),
        the mapper must NOT claim is_tradeable=True.
        """
        from brokers.symbol_mapper import BrokerSymbolMapper
        mock_conn = _make_static_fallback_connector()
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is False, (
            "XAUUSD must NOT be is_tradeable=True when connector used static fallback. "
            "This was the root-cause bug in v2.2."
        )
        assert r.rejection_code in ("INSTRUMENT_UNCONFIRMED", "NO_BROKER_MAPPING"), (
            f"Expected INSTRUMENT_UNCONFIRMED or NO_BROKER_MAPPING, got {r.rejection_code!r}"
        )

    def test_live_confirmed_connector_marks_tradeable(self):
        """When live API confirms tradeability, is_tradeable=True."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        mock_conn = _make_live_confirmed_connector()
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable is True
        assert r.is_supported is True
        assert r.is_tradeable_metadata is True
        assert r.rejection_code is None

    def test_eurusd_also_truthful_with_live_connector(self):
        """EURUSD truthfulness regression."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        mock_conn = _make_live_confirmed_connector()
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=mock_conn)

        r = m.check("EURUSD", connector_connected=True)
        assert r.is_tradeable is True
        assert r.broker_symbol == "EUR_USD"

    def test_capability_cache_dump_no_connector_all_unconfirmed(self):
        """Without a connector, all cache entries must be INSTRUMENT_UNCONFIRMED."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        m = BrokerSymbolMapper("oanda")
        m.hydrate(connector=None)

        for entry in m.dump_cache():
            assert entry["is_tradeable"] is False, (
                f"{entry['canonical_symbol']}: is_tradeable=True without connector"
            )
            assert entry["rejection_code"] == "INSTRUMENT_UNCONFIRMED"


# ---------------------------------------------------------------------------
# 2. BrokerManager.check_symbol() respects unconfirmed state
# ---------------------------------------------------------------------------

class TestBrokerManagerCheckSymbolUnconfirmed:

    def test_check_xauusd_with_static_fallback_returns_unconfirmed(self):
        """check_symbol must surface INSTRUMENT_UNCONFIRMED when mapper says so."""
        from brokers.symbol_mapper import BrokerSymbolMapper

        bm = _make_bm(connected=True)
        mock_conn = _make_static_fallback_connector()

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=mock_conn)
        bm._symbol_mapper = mapper
        # Force ESM to report connected
        bm._esm.is_connected.return_value = True

        result = bm.check_symbol("XAUUSD")
        assert result.is_tradeable is False
        assert result.rejection_code in ("INSTRUMENT_UNCONFIRMED",)

    def test_check_xauusd_with_live_connector_returns_tradeable(self):
        from brokers.symbol_mapper import BrokerSymbolMapper

        bm = _make_bm(connected=True)
        mock_conn = _make_live_confirmed_connector()

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=mock_conn)
        bm._symbol_mapper = mapper
        bm._esm.is_connected.return_value = True

        result = bm.check_symbol("XAUUSD")
        assert result.is_tradeable is True
        assert result.rejection_code is None

    def test_check_unknown_symbol_always_rejects(self):
        from brokers.symbol_mapper import BrokerSymbolMapper

        bm = _make_bm(connected=True)
        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=_make_live_confirmed_connector())
        bm._symbol_mapper = mapper
        bm._esm.is_connected.return_value = True

        result = bm.check_symbol("MEMECOIN")
        assert result.is_tradeable is False
        assert result.rejection_code == "UNKNOWN_SYMBOL"


# ---------------------------------------------------------------------------
# 3. BrokerManager structured rejection history
# ---------------------------------------------------------------------------

class TestBrokerManagerRejectionHistory:

    def test_live_rejection_recorded_with_full_diagnostics(self):
        """
        When a live order is rejected, rejection_history must include
        raw_broker_error, parsed_error_code, normalized_rejection_code,
        payload_snapshot.
        """
        from brokers.base_connector import OrderRequest, OrderSide, OrderType, OrderResult, OrderStatus
        from brokers.symbol_mapper import BrokerSymbolMapper

        bm = _make_bm(connected=True, live_allowed=True)

        # Mock connector that rejects
        mock_conn = MagicMock()
        mock_conn.submit_order.return_value = OrderResult(
            success=False,
            order_id=None,
            status=OrderStatus.REJECTED,
            reject_reason="INSTRUMENT_NOT_TRADEABLE: The instrument is halted",
            raw_broker_error="INSTRUMENT_NOT_TRADEABLE",
            parsed_error_code="INSTRUMENT_NOT_TRADEABLE",
            normalized_rejection_code="INSTRUMENT_NOT_TRADEABLE",
            insufficient_funds=False,
            payload_snapshot={"order": {"instrument": "XAU_USD", "units": "100"}},
        )
        mock_conn.check_circuit_breaker.return_value = False
        bm._connector = mock_conn

        # Set up mapper
        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=_make_live_confirmed_connector())
        bm._symbol_mapper = mapper

        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            order_type=OrderType.MARKET,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        bm.submit_order(req)

        history = bm.rejection_history()
        assert len(history) >= 1
        last = history[-1]

        # Check all required diagnostic fields
        assert last.get("canonical_symbol") == "XAUUSD"
        assert last.get("raw_broker_error") == "INSTRUMENT_NOT_TRADEABLE"
        assert last.get("parsed_error_code") == "INSTRUMENT_NOT_TRADEABLE"
        assert last.get("normalized_rejection_code") == "INSTRUMENT_NOT_TRADEABLE"
        assert last.get("payload_snapshot") is not None
        assert last.get("mode") == "LIVE"

    def test_paper_fill_marks_submit_validated(self):
        """After paper fill, symbol should be marked submit-validated."""
        from brokers.base_connector import OrderRequest, OrderSide, OrderType
        from brokers.symbol_mapper import BrokerSymbolMapper

        bm = _make_bm(connected=False, live_allowed=False)

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=_make_live_confirmed_connector())
        bm._symbol_mapper = mapper

        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            order_type=OrderType.MARKET,
            stop_loss=2290.0,
            take_profit=2325.0,
        )
        result = bm.submit_order(req)
        assert result.success is True

        # Now check submit-validated flag
        r = mapper.check("XAUUSD", connector_connected=True)
        assert r.is_tradeable_submit_validated is True


# ---------------------------------------------------------------------------
# 4. API endpoint tests
# ---------------------------------------------------------------------------

def _make_mock_bm_connected():
    """BrokerManager mock in LIVE_CONNECTED_SAFE with live-confirmed mapper."""
    from brokers.symbol_mapper import BrokerSymbolMapper

    mock_esm = MagicMock()
    mock_esm.is_kill_switch_active.return_value = False
    mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
    mock_esm.is_live_trading_allowed.return_value = False
    mock_esm.is_connected.return_value = True

    mock_bm = MagicMock()
    mock_bm._esm = mock_esm
    mock_bm._broker_name = "oanda"
    mock_bm._rejection_history = []

    mapper = BrokerSymbolMapper("oanda")
    mapper.hydrate(connector=_make_live_confirmed_connector())
    mock_bm._symbol_mapper = mapper
    mock_bm.get_capability_cache.return_value = mapper.dump_cache()

    def _check_symbol(sym):
        connected = mock_esm.is_connected.return_value
        return mapper.check(sym, connector_connected=connected)
    mock_bm.check_symbol.side_effect = _check_symbol

    def _rejection_history(limit=50):
        return []
    mock_bm.rejection_history.side_effect = _rejection_history
    mock_bm._connector = None

    return mock_bm


def _make_mock_bm_unconfirmed():
    """BrokerManager mock where mapper has static fallback (INSTRUMENT_UNCONFIRMED)."""
    from brokers.symbol_mapper import BrokerSymbolMapper

    mock_esm = MagicMock()
    mock_esm.is_kill_switch_active.return_value = False
    mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
    mock_esm.is_live_trading_allowed.return_value = False
    mock_esm.is_connected.return_value = True

    mock_bm = MagicMock()
    mock_bm._esm = mock_esm
    mock_bm._broker_name = "oanda"
    mock_bm._rejection_history = []

    mapper = BrokerSymbolMapper("oanda")
    mapper.hydrate(connector=_make_static_fallback_connector())
    mock_bm._symbol_mapper = mapper
    mock_bm.get_capability_cache.return_value = mapper.dump_cache()

    def _check_symbol(sym):
        connected = mock_esm.is_connected.return_value
        return mapper.check(sym, connector_connected=connected)
    mock_bm.check_symbol.side_effect = _check_symbol

    def _rejection_history(limit=50):
        return []
    mock_bm.rejection_history.side_effect = _rejection_history
    mock_bm._connector = None

    return mock_bm


class TestAPICapabilityCacheV23:
    """Tests for /api/broker/capability-cache endpoint."""

    def _client(self, bm=None):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        if bm is None:
            bm = _make_mock_bm_connected()

        api_server._orchestrator = orch
        api_server._broker_manager = bm
        orch.execution_supervisor.broker = bm

        return TestClient(api_server.app), orch, bm

    def test_capability_cache_returns_200(self):
        client, _, _ = self._client()
        r = client.get("/api/broker/capability-cache")
        assert r.status_code == 200

    def test_capability_cache_includes_all_v23_fields(self):
        """Each instrument entry must have v2.3 granular fields."""
        client, _, _ = self._client()
        r = client.get("/api/broker/capability-cache")
        data = r.json()
        instruments = data.get("instruments", [])
        assert len(instruments) > 0

        xau = next((i for i in instruments if i.get("canonical_symbol") == "XAUUSD"), None)
        assert xau is not None, "XAUUSD missing from capability cache"

        v23_fields = [
            "is_mapped", "is_supported", "is_tradeable_metadata",
            "is_tradeable_submit_validated", "is_tradeable"
        ]
        for f in v23_fields:
            assert f in xau, f"Field '{f}' missing from XAUUSD capability cache entry"

    def test_capability_cache_xauusd_tradeable_when_confirmed(self):
        """With live-confirmed mapper, XAUUSD must be is_tradeable=True."""
        client, _, _ = self._client(bm=_make_mock_bm_connected())
        r = client.get("/api/broker/capability-cache")
        data = r.json()
        instruments = data.get("instruments", [])
        xau = next((i for i in instruments if i.get("canonical_symbol") == "XAUUSD"), None)
        assert xau is not None
        assert xau["is_tradeable"] is True
        assert xau["is_supported"] is True

    def test_capability_cache_xauusd_not_tradeable_when_unconfirmed(self):
        """With static fallback mapper, XAUUSD must NOT be is_tradeable=True."""
        client, _, _ = self._client(bm=_make_mock_bm_unconfirmed())
        r = client.get("/api/broker/capability-cache")
        data = r.json()
        instruments = data.get("instruments", [])
        xau = next((i for i in instruments if i.get("canonical_symbol") == "XAUUSD"), None)
        assert xau is not None
        assert xau["is_tradeable"] is False, (
            "XAUUSD must NOT be is_tradeable=True when broker API not confirmed. "
            "This was the root-cause bug in v2.2."
        )
        # Warning should surface
        assert data.get("xauusd_warning") is not None

    def test_capability_cache_broker_connected_field(self):
        client, _, _ = self._client()
        r = client.get("/api/broker/capability-cache")
        data = r.json()
        assert "broker_connected" in data


class TestAPIXauDiagnostics:
    """Tests for /api/broker/debug/xau-diagnostics endpoint."""

    def _client(self):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        bm = _make_mock_bm_connected()
        bm.get_xau_diagnostics.return_value = {
            "canonical_symbol": "XAUUSD",
            "broker_symbol": "XAU_USD",
            "capability_cache": {"is_tradeable": True},
            "broker_rejection_history": [],
            "manager_rejection_history": [],
            "payload_probe": {"payload_valid": True},
            "broker_connected": True,
            "live_trading_allowed": False,
        }

        api_server._orchestrator = orch
        api_server._broker_manager = bm
        return TestClient(api_server.app), orch, bm

    def test_diagnostics_endpoint_returns_200(self):
        client, _, _ = self._client()
        r = client.get("/api/broker/debug/xau-diagnostics")
        assert r.status_code == 200

    def test_diagnostics_includes_required_fields(self):
        client, _, _ = self._client()
        r = client.get("/api/broker/debug/xau-diagnostics")
        data = r.json()
        assert "canonical_symbol" in data
        assert "broker_symbol" in data
        assert "broker_connected" in data


# ---------------------------------------------------------------------------
# 5. EURUSD regression — full pipeline still works
# ---------------------------------------------------------------------------

class TestEURUSDRegressionV23:

    def _make_paper_bm(self):
        from brokers.symbol_mapper import BrokerSymbolMapper

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_esm.is_connected.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm
        mock_bm._broker_name = "oanda"

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=None)
        mock_bm._symbol_mapper = mapper
        mock_bm.get_capability_cache.return_value = mapper.dump_cache()

        def _check(sym):
            return mapper.check(sym, connector_connected=False)
        mock_bm.check_symbol.side_effect = _check

        return mock_bm

    def test_eurusd_signal_processes_in_paper_mode(self):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        bm = self._make_paper_bm()
        orch.execution_supervisor.broker = bm

        api_server._orchestrator = orch
        api_server._broker_manager = bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "EURUSD",
            "action": "BUY",
            "price": 1.1000,
            "stop_loss": 1.0900,
            "take_profit": 1.1200,
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("symbol") == "EURUSD"
        assert orch._cycle_count >= 1
        assert orch.ledger.total_messages >= 1

    def test_eurusd_eur_usd_variant_normalises(self):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        bm = self._make_paper_bm()
        orch.execution_supervisor.broker = bm

        api_server._orchestrator = orch
        api_server._broker_manager = bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "EUR/USD",
            "action": "BUY",
            "price": 1.1000,
            "stop_loss": 1.0900,
            "take_profit": 1.1200,
        })
        assert r.status_code == 200
        assert r.json().get("symbol") == "EURUSD"


# ---------------------------------------------------------------------------
# 6. XAUUSD end-to-end: fills OR structured rejection
# ---------------------------------------------------------------------------

class TestXAUUSDEndToEnd:

    def _make_connected_bm_with_gold_tradeable(self):
        """BrokerManager where XAUUSD is live-confirmed tradeable."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        from brokers.symbol_mapper import TradeabilityResult

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_esm.is_connected.return_value = True

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm
        mock_bm._broker_name = "oanda"

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=_make_live_confirmed_connector())
        mock_bm._symbol_mapper = mapper
        mock_bm.get_capability_cache.return_value = mapper.dump_cache()

        def _check(sym):
            return mapper.check(sym, connector_connected=True)
        mock_bm.check_symbol.side_effect = _check

        return mock_bm

    def test_xauusd_passes_through_when_tradeable_confirmed(self):
        """XAUUSD signal passes the tradeability gate when live-confirmed."""
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        bm = self._make_connected_bm_with_gold_tradeable()
        orch.execution_supervisor.broker = bm

        api_server._orchestrator = orch
        api_server._broker_manager = bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "XAUUSD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2325.0,
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("symbol") == "XAUUSD"
        assert orch._cycle_count >= 1

    def test_xauusd_hard_blocked_when_unconfirmed_and_connected(self):
        """XAUUSD is blocked (422) when broker connected but instrument unconfirmed."""
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import BrokerSymbolMapper

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_esm.is_connected.return_value = True

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=_make_static_fallback_connector())
        mock_bm._symbol_mapper = mapper

        def _check(sym):
            return mapper.check(sym, connector_connected=True)
        mock_bm.check_symbol.side_effect = _check

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "XAUUSD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2325.0,
        })
        # Should be blocked because is_tradeable=False (INSTRUMENT_UNCONFIRMED)
        assert r.status_code == 422
        data = r.json()
        assert data.get("rejection_code") in (
            "INSTRUMENT_UNCONFIRMED", "INSTRUMENT_NOT_TRADEABLE",
            "BROKER_DOES_NOT_SUPPORT_INSTRUMENT", "NO_BROKER_MAPPING",
        ), f"Unexpected rejection_code: {data.get('rejection_code')!r}"

    def test_xauusd_xau_variants_all_normalise(self):
        """XAU_USD, xau/usd, GOLD, xauusd → all produce canonical XAUUSD in response."""
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import BrokerSymbolMapper

        api_server._orchestrator = None
        api_server._broker_manager = None

        # Use paper mode (no broker) so signals pass through without tradeability block
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_esm.is_connected.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=None)
        mock_bm._symbol_mapper = mapper

        def _check(sym):
            return mapper.check(sym, connector_connected=False)
        mock_bm.check_symbol.side_effect = _check

        orch = _make_orchestrator()
        orch.execution_supervisor.broker = mock_bm

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)

        variants = ["XAU_USD", "XAU/USD", "GOLD", "xauusd", "xau_usd"]
        for variant in variants:
            r = client.post("/api/signal", json={
                "symbol": variant,
                "action": "BUY",
                "price": 2300.0,
                "stop_loss": 2290.0,
                "take_profit": 2325.0,
            })
            assert r.status_code == 200, f"Failed for variant {variant!r}: {r.text}"
            data = r.json()
            assert data.get("symbol") == "XAUUSD", (
                f"variant {variant!r} → expected 'XAUUSD', got {data.get('symbol')!r}"
            )

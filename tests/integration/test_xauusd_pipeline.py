"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — XAUUSD Symbol Patch (v2.2)

Acceptance criteria:
  1. normalize_symbol converts XAU_USD / xau/usd / GOLD → XAUUSD.
  2. BrokerManager.check_symbol("XAUUSD") returns is_tradeable=True (OANDA mock).
  3. BrokerManager.check_symbol("UNKNOWN") returns rejection_code UNKNOWN_SYMBOL.
  4. BrokerManager.check_symbol("XAU_USD") returns rejection_code UNKNOWN_SYMBOL
     (raw broker-native symbol must not bypass normalisation).
  5. process_external_signal with instrument="XAU_USD" normalises to "XAUUSD"
     and runs the full pipeline (cycle increments, ledger fills, trade or rejection).
  6. EURUSD still works (regression).
  7. /api/signal with symbol="XAU_USD" returns 200 and canonical symbol XAUUSD.
  8. /api/signal with symbol="xau/usd" returns 200 and canonical symbol XAUUSD.
  9. /api/signal with symbol="GOLD" returns 200 and canonical symbol XAUUSD.
  10. /api/signal with unknown symbol returns 200 (paper, no broker) or 422 (live, not tradeable).
  11. /api/broker/capability-cache returns XAUUSD entry when broker connected.
  12. Broker connected + XAUUSD → order routed through broker layer with XAU_USD symbol.
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

def _make_esm(preflight_passed: bool = True, armed: bool = False):
    from live.execution_state_machine import ExecutionStateMachine
    tf = tempfile.mktemp(suffix=".json")
    esm = ExecutionStateMachine(state_file=tf, require_operator_arming=True)
    if preflight_passed or armed:
        esm.connect_broker(
            broker="oanda",
            environment="practice",
            account_id="101-001-test-001",
            operator="test",
            preflight_passed=preflight_passed,
            preflight_reason="all checks passed" if preflight_passed else "test failure",
        )
    if armed and preflight_passed:
        esm.arm_live_trading(operator="test_operator")
    return esm


def _make_orchestrator(broker=None):
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
        "agents": {"decision_ledger_path": f"/tmp/test_ledger_{uuid.uuid4().hex[:8]}.jsonl"},
    }
    orch = AgentOrchestrator(config, broker_connector=broker)
    return orch


def _make_signal(instrument: str = "XAUUSD",
                 direction: str = "BUY",
                 entry: float = 2300.0,
                 sl: float = 2290.0,
                 tp: float = 2320.0,
                 is_paper: bool = True):
    """Build a valid gold signal."""
    from domain.models import (
        Signal, SignalDirection, StrategyFamily, SessionType,
        InstrumentType, VenueType, SignalStatus,
    )
    pip_size = 1.0  # Gold pip
    stop_dist = abs(entry - sl) / pip_size
    tgt_dist = abs(tp - entry) / pip_size
    rr = tgt_dist / stop_dist if stop_dist > 0 else 2.0

    return Signal(
        signal_id=str(uuid.uuid4()),
        instrument=instrument,
        instrument_type=InstrumentType.GOLD,
        venue_type=VenueType.CFD_BROKER,
        timeframe="M15",
        session=SessionType.LONDON,
        direction=SignalDirection(direction),
        status=SignalStatus.CANDIDATE,
        entry_price=entry,
        stop_loss=sl,
        take_profit=tp,
        position_size_lots=0.10,
        risk_amount_usd=100.0,
        reward_to_risk=round(rr, 2),
        stop_distance_pips=round(stop_dist, 1),
        target_distance_pips=round(tgt_dist, 1),
        strategy_family=StrategyFamily.TJR,
        strategy_reason="Test webhook signal",
        confidence_score=0.75,
        is_paper_trade=is_paper,
    )


def _make_eurusd_signal(direction: str = "BUY",
                        entry: float = 1.1000,
                        sl: float = 1.0900,
                        tp: float = 1.1200,
                        is_paper: bool = True):
    from domain.models import (
        Signal, SignalDirection, StrategyFamily, SessionType,
        InstrumentType, VenueType, SignalStatus,
    )
    pip_size = 0.0001
    stop_dist = abs(entry - sl) / pip_size
    tgt_dist = abs(tp - entry) / pip_size
    rr = tgt_dist / stop_dist if stop_dist > 0 else 2.0
    return Signal(
        signal_id=str(uuid.uuid4()),
        instrument="EURUSD",
        instrument_type=InstrumentType.FOREX,
        venue_type=VenueType.FOREX_BROKER,
        timeframe="M15",
        session=SessionType.LONDON,
        direction=SignalDirection(direction),
        status=SignalStatus.CANDIDATE,
        entry_price=entry,
        stop_loss=sl,
        take_profit=tp,
        position_size_lots=0.10,
        risk_amount_usd=50.0,
        reward_to_risk=round(rr, 2),
        stop_distance_pips=round(stop_dist, 1),
        target_distance_pips=round(tgt_dist, 1),
        strategy_family=StrategyFamily.TJR,
        strategy_reason="EURUSD regression test",
        confidence_score=0.75,
        is_paper_trade=is_paper,
    )


# ---------------------------------------------------------------------------
# 1. Symbol normalisation unit smoke tests
# ---------------------------------------------------------------------------

class TestSymbolNormalisationSmoke:
    def test_xau_usd_underscore(self):
        from domain.symbol_utils import normalize_symbol
        assert normalize_symbol("XAU_USD") == "XAUUSD"

    def test_xau_usd_slash(self):
        from domain.symbol_utils import normalize_symbol
        assert normalize_symbol("XAU/USD") == "XAUUSD"

    def test_gold_alias(self):
        from domain.symbol_utils import normalize_symbol
        assert normalize_symbol("GOLD") == "XAUUSD"

    def test_lowercase_variants(self):
        from domain.symbol_utils import normalize_symbol
        for raw in ("xau_usd", "xau/usd", "xauusd", "gold"):
            assert normalize_symbol(raw) == "XAUUSD", f"Failed for {raw!r}"

    def test_eurusd_variants(self):
        from domain.symbol_utils import normalize_symbol
        for raw in ("EUR_USD", "EUR/USD", "eurusd", "eur/usd"):
            assert normalize_symbol(raw) == "EURUSD", f"Failed for {raw!r}"


# ---------------------------------------------------------------------------
# 2. BrokerManager.check_symbol tests
# ---------------------------------------------------------------------------

class TestBrokerManagerCheckSymbol:

    def _make_bm_no_broker(self):
        """BrokerManager with no live connector — PAPER_MODE."""
        import tempfile
        from live.broker_manager import BrokerManager
        from unittest.mock import patch, MagicMock
        with patch("live.broker_manager.get_credential_manager") as mock_cred, \
             patch("live.broker_manager.ExecutionStateMachine") as mock_esm_cls:
            mock_cred.return_value.load_jwt_secret.return_value = None
            # ESM in paper mode
            mock_esm = MagicMock()
            mock_esm.is_connected.return_value = False
            mock_esm.is_kill_switch_active.return_value = False
            mock_esm.is_live_trading_allowed.return_value = False
            mock_esm_cls.return_value = mock_esm
            bm = BrokerManager(config={})
        return bm

    def test_check_xauusd_no_broker(self):
        """Without broker, XAUUSD check returns BROKER_NOT_CONNECTED."""
        bm = self._make_bm_no_broker()
        result = bm.check_symbol("XAUUSD")
        assert result.is_tradeable is False
        assert result.rejection_code == "BROKER_NOT_CONNECTED"

    def test_check_xauusd_with_mapper_hydrated(self):
        """With symbol mapper hydrated (no live connector), XAUUSD is tradeable."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        from live.broker_manager import BrokerManager
        from unittest.mock import patch, MagicMock

        with patch("live.broker_manager.get_credential_manager") as mock_cred, \
             patch("live.broker_manager.ExecutionStateMachine") as mock_esm_cls:
            mock_cred.return_value.load_jwt_secret.return_value = None
            mock_esm = MagicMock()
            mock_esm.is_connected.return_value = True  # connected
            mock_esm.is_kill_switch_active.return_value = False
            mock_esm.is_live_trading_allowed.return_value = False
            mock_esm_cls.return_value = mock_esm

            bm = BrokerManager(config={})
            # Manually hydrate mapper
            bm._symbol_mapper = BrokerSymbolMapper("oanda")
            bm._symbol_mapper.hydrate(connector=None)

            result = bm.check_symbol("XAUUSD")

        assert result.is_tradeable is True
        assert result.broker_symbol == "XAU_USD"
        assert result.asset_class == "GOLD"
        assert result.rejection_code is None

    def test_check_unknown_symbol(self):
        """Unknown symbols return UNKNOWN_SYMBOL."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        from live.broker_manager import BrokerManager
        from unittest.mock import patch, MagicMock

        with patch("live.broker_manager.get_credential_manager") as mock_cred, \
             patch("live.broker_manager.ExecutionStateMachine") as mock_esm_cls:
            mock_cred.return_value.load_jwt_secret.return_value = None
            mock_esm = MagicMock()
            mock_esm.is_connected.return_value = True
            mock_esm_cls.return_value = mock_esm

            bm = BrokerManager(config={})
            bm._symbol_mapper = BrokerSymbolMapper("oanda")
            bm._symbol_mapper.hydrate(connector=None)

            result = bm.check_symbol("DOGEUSD")

        assert result.is_tradeable is False
        assert result.rejection_code == "UNKNOWN_SYMBOL"

    def test_check_raw_broker_symbol_rejected(self):
        """XAU_USD (broker-native) should NOT be a valid canonical symbol."""
        from brokers.symbol_mapper import BrokerSymbolMapper
        from live.broker_manager import BrokerManager
        from unittest.mock import patch, MagicMock

        with patch("live.broker_manager.get_credential_manager") as mock_cred, \
             patch("live.broker_manager.ExecutionStateMachine") as mock_esm_cls:
            mock_cred.return_value.load_jwt_secret.return_value = None
            mock_esm = MagicMock()
            mock_esm.is_connected.return_value = True
            mock_esm_cls.return_value = mock_esm

            bm = BrokerManager(config={})
            bm._symbol_mapper = BrokerSymbolMapper("oanda")
            bm._symbol_mapper.hydrate(connector=None)

            result = bm.check_symbol("XAU_USD")

        assert result.is_tradeable is False
        assert result.rejection_code == "UNKNOWN_SYMBOL"


# ---------------------------------------------------------------------------
# 3. Orchestrator symbol normalisation
# ---------------------------------------------------------------------------

class TestOrchestratorSymbolNormalisation:
    """process_external_signal normalises symbol before pipeline runs."""

    def test_xau_usd_normalised_before_pipeline(self):
        """Signal with instrument='XAU_USD' is normalised to 'XAUUSD' in pipeline."""
        orch = _make_orchestrator()
        signal = _make_signal(instrument="XAU_USD")
        assert signal.instrument == "XAU_USD"  # pre-call

        result = orch.process_external_signal(signal)

        # Regardless of outcome, cycle should increment
        assert orch._cycle_count == 1

        # Ledger should contain at least the SIGNAL_CANDIDATE entry
        assert orch.ledger.total_messages >= 1

        # If a trade was recorded, it should use the canonical symbol
        if orch._all_trades:
            assert orch._all_trades[0].instrument == "XAUUSD"

    def test_gold_alias_normalised(self):
        orch = _make_orchestrator()
        signal = _make_signal(instrument="GOLD")
        result = orch.process_external_signal(signal)
        assert orch._cycle_count == 1
        if orch._all_trades:
            assert orch._all_trades[0].instrument == "XAUUSD"

    def test_xauusd_canonical_passes_unchanged(self):
        orch = _make_orchestrator()
        signal = _make_signal(instrument="XAUUSD")
        result = orch.process_external_signal(signal)
        assert orch._cycle_count == 1

    def test_eurusd_regression(self):
        """EURUSD must still process without regression."""
        orch = _make_orchestrator()
        signal = _make_eurusd_signal()
        result = orch.process_external_signal(signal)
        assert orch._cycle_count == 1
        assert result["status"] not in ("", None)
        assert orch.ledger.total_messages >= 1

    def test_cycle_increments_per_call(self):
        """Each call to process_external_signal increments cycle_count."""
        orch = _make_orchestrator()
        for i in range(1, 4):
            result = orch.process_external_signal(_make_signal())
            assert orch._cycle_count == i

    def test_ledger_contains_signal_candidate_for_xauusd(self):
        from protocol.agent_protocol import AgentMessageType
        orch = _make_orchestrator()
        orch.process_external_signal(_make_signal(instrument="XAUUSD"))
        msgs = orch.ledger.get_recent(20)
        types = [m.message_type for m in msgs]
        assert AgentMessageType.SIGNAL_CANDIDATE in types, (
            f"SIGNAL_CANDIDATE missing. Types found: {types}"
        )


# ---------------------------------------------------------------------------
# 4. API endpoint tests (FastAPI TestClient)
# ---------------------------------------------------------------------------

def _make_mock_bm_paper():
    """Return a minimal BrokerManager mock in PAPER_MODE."""
    from brokers.symbol_mapper import BrokerSymbolMapper
    mock_esm = MagicMock()
    mock_esm.is_kill_switch_active.return_value = False
    mock_esm.current_state.return_value.value = "PAPER_MODE"
    mock_esm.is_live_trading_allowed.return_value = False
    mock_esm.is_connected.return_value = False

    mock_bm = MagicMock()
    mock_bm._esm = mock_esm

    # Wire a real symbol mapper (no connector → static map)
    mapper = BrokerSymbolMapper("oanda")
    mapper.hydrate(connector=None)
    mock_bm._symbol_mapper = mapper
    mock_bm._broker_name = "oanda"
    mock_bm.get_capability_cache.return_value = mapper.dump_cache()

    # check_symbol delegates to real mapper
    def _check_symbol(sym):
        return mapper.check(sym, connector_connected=False)
    mock_bm.check_symbol.side_effect = _check_symbol

    return mock_bm


class TestAPISignalXAUUSD:

    def _client(self):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None
        api_server._data_service = None

        orch = _make_orchestrator()
        mock_bm = _make_mock_bm_paper()

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        # Wire broker into execution supervisor
        orch.execution_supervisor.broker = mock_bm

        return TestClient(api_server.app), orch, mock_bm

    def test_xau_usd_webhook_returns_200(self):
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 200

    def test_xau_usd_normalised_in_response(self):
        """Response symbol should be 'XAUUSD', not 'XAU_USD'."""
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        data = r.json()
        assert data.get("symbol") == "XAUUSD", (
            f"Expected canonical 'XAUUSD', got {data.get('symbol')!r}"
        )

    def test_xau_slash_usd_normalised(self):
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "XAU/USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 200
        assert r.json().get("symbol") == "XAUUSD"

    def test_gold_alias_normalised(self):
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "GOLD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 200
        assert r.json().get("symbol") == "XAUUSD"

    def test_xauusd_lowercase_normalised(self):
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "xauusd",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 200
        assert r.json().get("symbol") == "XAUUSD"

    def test_xauusd_cycle_increments(self):
        client, orch, _ = self._client()
        initial = orch._cycle_count
        client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert orch._cycle_count == initial + 1

    def test_xauusd_ledger_has_entries(self):
        client, orch, _ = self._client()
        client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert orch.ledger.total_messages >= 1

    def test_xauusd_trade_or_rejection_recorded(self):
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        data = r.json()
        valid_statuses = {
            "FILLED", "SIGNAL_REJECTED", "RISK_REJECTED",
            "EXECUTION_BLOCKED", "ORDER_FAILED",
        }
        assert data["status"] in valid_statuses, (
            f"Unexpected status: {data['status']!r}"
        )

    def test_eurusd_regression_still_works(self):
        """EURUSD must process correctly (regression guard)."""
        client, orch, _ = self._client()
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

    def test_eur_slash_usd_regression(self):
        """EUR/USD format normalises to EURUSD."""
        client, orch, _ = self._client()
        r = client.post("/api/signal", json={
            "symbol": "EUR/USD",
            "action": "BUY",
            "price": 1.1000,
            "stop_loss": 1.0900,
            "take_profit": 1.1200,
        })
        assert r.status_code == 200
        assert r.json().get("symbol") == "EURUSD"


class TestAPICapabilityCache:

    def test_capability_cache_endpoint(self):
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import BrokerSymbolMapper

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        mock_bm = _make_mock_bm_paper()
        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)
        r = client.get("/api/broker/capability-cache")
        assert r.status_code == 200
        data = r.json()
        assert "instruments" in data

    def test_capability_cache_contains_xauusd(self):
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import BrokerSymbolMapper

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        mock_bm = _make_mock_bm_paper()

        # Set up a real mapper in bm
        mapper = BrokerSymbolMapper("oanda")
        mapper.hydrate(connector=None)
        mock_bm.get_capability_cache.return_value = mapper.dump_cache()
        mock_bm._symbol_mapper = mapper

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)
        r = client.get("/api/broker/capability-cache")
        data = r.json()
        symbols = {inst["canonical_symbol"] for inst in data.get("instruments", [])}
        assert "XAUUSD" in symbols, f"XAUUSD missing from capability cache: {symbols}"


# ---------------------------------------------------------------------------
# 5. Broker connected + XAUUSD hard-block test
# ---------------------------------------------------------------------------

class TestXAUUSDHardBlock:
    """When broker is connected and XAUUSD is not tradeable, API returns 422."""

    def test_non_tradeable_symbol_blocked_when_connected(self):
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import TradeabilityResult

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_esm.is_connected.return_value = True  # broker IS connected

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        # Simulate instrument NOT tradeable
        mock_bm.check_symbol.return_value = TradeabilityResult(
            canonical_symbol="XAUUSD",
            broker_symbol=None,
            asset_class="GOLD",
            is_supported=False,
            is_tradeable=False,
            reason_if_not_tradeable="XAUUSD not supported on this account",
            rejection_code="BROKER_DOES_NOT_SUPPORT_INSTRUMENT",
        )

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "XAUUSD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 422
        data = r.json()
        assert data["rejection_code"] == "BROKER_DOES_NOT_SUPPORT_INSTRUMENT"

    def test_tradeable_symbol_passes_when_connected(self):
        """XAUUSD passes through when broker is connected and mapper says tradeable."""
        from fastapi.testclient import TestClient
        from live import api_server
        from brokers.symbol_mapper import TradeabilityResult

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
        mock_bm.check_symbol.return_value = TradeabilityResult(
            canonical_symbol="XAUUSD",
            broker_symbol="XAU_USD",
            asset_class="GOLD",
            is_supported=True,
            is_tradeable=True,
            reason_if_not_tradeable=None,
            rejection_code=None,
        )

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        orch.execution_supervisor.broker = mock_bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
        })
        assert r.status_code == 200
        data = r.json()
        assert data["symbol"] == "XAUUSD"
        # cycle must have incremented
        assert orch._cycle_count >= 1


# ---------------------------------------------------------------------------
# 6. Full proof-of-success (paper mode)
# ---------------------------------------------------------------------------

class TestFullProofOfSuccessXAUUSD:
    """Simulate exactly what was described in the acceptance criteria."""

    def test_xauusd_full_pipeline_paper_mode(self):
        """
        XAU_USD BUY → canonical=XAUUSD → pipeline runs →
        ledger has entries → cycle > 0 → trade or rejection recorded.
        """
        from fastapi.testclient import TestClient
        from live import api_server

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator()
        mock_bm = _make_mock_bm_paper()
        orch.execution_supervisor.broker = mock_bm

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)

        # Fire XAU_USD BUY webhook
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
            "timeframe": "M15",
            "strategy": "TJR",
        })

        # 1. HTTP 200
        assert r.status_code == 200, f"Non-200: {r.text}"
        data = r.json()

        # 2. Canonical symbol in response
        assert data["symbol"] == "XAUUSD"

        # 3. cycle_count > 0
        assert orch._cycle_count > 0, "cycle_count still 0!"

        # 4. Ledger not empty
        assert orch.ledger.total_messages > 0, "Ledger still empty!"

        # 5. Status is a real pipeline status (not fake RECEIVED)
        assert data["status"] != "RECEIVED"
        valid_statuses = {
            "FILLED", "SIGNAL_REJECTED", "RISK_REJECTED",
            "EXECUTION_BLOCKED", "ORDER_FAILED",
        }
        assert data["status"] in valid_statuses, (
            f"Unexpected status: {data['status']!r}"
        )

        # 6. Trade or rejection recorded
        trades_r = client.get("/api/trades?limit=10")
        trades_data = trades_r.json()
        has_trade = trades_data["total"] > 0
        was_rejected = data["status"] in (
            "SIGNAL_REJECTED", "RISK_REJECTED", "EXECUTION_BLOCKED", "ORDER_FAILED"
        )
        assert has_trade or was_rejected, (
            f"Neither trade nor rejection — status={data['status']!r}"
        )

        # 7. Ledger has SIGNAL_CANDIDATE
        ledger_r = client.get("/api/ledger?limit=20")
        msgs = ledger_r.json()["messages"]
        msg_types = [m["type"] for m in msgs]
        assert any("SIGNAL_CANDIDATE" in t for t in msg_types), (
            f"SIGNAL_CANDIDATE missing from ledger: {msg_types}"
        )

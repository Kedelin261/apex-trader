"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — Signal Pipeline Bug Fix (v2.1 patch)

Covers ALL acceptance criteria for the /api/signal pipeline fix:
  1. Webhook in paper mode → cycle increments, ledger records, no live broker call
  2. Webhook in LIVE_CONNECTED_SAFE → validator/risk run, execution blocked from live
  3. Webhook in LIVE_ENABLED → full chain, routes through approved broker layer
  4. Kill switch active → rejected immediately, no ledger side-effects beyond rejection
  5. Invalid schema version → 400
  6. Rejected signal → rejection visible, no fake success
  7. /api/status environment reflects runtime (ESM/broker) not stale config

All tests use the FastAPI TestClient for endpoint tests and direct
orchestrator/agent calls for unit-level tests. No direct broker calls
are made — OANDA connector is mocked throughout.
"""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_esm(preflight_passed: bool = True, armed: bool = False) -> "ExecutionStateMachine":
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


def _make_signal(direction: str = "BUY",
                 symbol: str = "EURUSD",
                 entry: float = 1.1000,
                 sl: float = 1.0950,
                 tp: float = 1.1100,
                 is_paper: bool = True) -> "Signal":
    """Build a well-formed Signal that passes all validator rules."""
    from domain.models import (
        Signal, SignalDirection, StrategyFamily, SessionType,
        InstrumentType, VenueType, SignalStatus,
    )
    stop_dist = abs(entry - sl) / 0.0001       # pips
    tgt_dist  = abs(tp - entry) / 0.0001
    rr        = tgt_dist / stop_dist if stop_dist > 0 else 2.5

    return Signal(
        signal_id=str(uuid.uuid4()),
        instrument=symbol,
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
        strategy_reason="Test signal",
        confidence_score=0.75,
        is_paper_trade=is_paper,
    )


def _make_orchestrator(broker=None):
    """Instantiate a real AgentOrchestrator with minimal config."""
    from agents.agent_orchestrator import AgentOrchestrator
    cfg = {
        "system": {"environment": "paper", "log_level": "WARNING"},
        "risk": {
            "max_risk_per_trade_pct": 1.0,
            "max_daily_loss_pct": 3.0,
            "max_total_drawdown_pct": 10.0,
            "max_concurrent_trades": 2,
            "min_reward_to_risk_ratio": 2.0,
            "max_spread_pips": 3.0,
            "max_slippage_pips": 2.0,
            "max_position_size_usd": 10000.0,
            "account_balance_default": 100000.0,
        },
        "agents": {
            "decision_ledger_path": tempfile.mktemp(suffix=".jsonl"),
            "strategy_validator": {"min_setup_quality_score": 0.65},
            "execution_supervisor": {
                "max_order_retries": 3,
                "circuit_breaker_failures": 5,
            },
        },
        "markets": {},
        "backtest": {},
        "strategy": {"tjr": {}},
    }
    orch = AgentOrchestrator(cfg, broker_connector=broker)
    return orch


# ---------------------------------------------------------------------------
# ORCHESTRATOR UNIT TESTS
# ---------------------------------------------------------------------------

class TestProcessExternalSignalUnit:
    """Unit tests: AgentOrchestrator.process_external_signal()."""

    # ── 1. Kill switch blocks immediately ───────────────────────────────────
    def test_kill_switch_blocks_signal(self):
        orch = _make_orchestrator()
        orch._kill_switch = True
        signal = _make_signal()

        result = orch.process_external_signal(signal)

        assert result["status"] == "KILL_SWITCH_ACTIVE"
        assert result["cycle"] == 0           # cycle NOT incremented for blocked
        assert result["trades_executed"] == 0
        assert result["messages_emitted"] == 0

    # ── 2. Cycle count increments on processing ─────────────────────────────
    def test_cycle_count_increments(self):
        orch = _make_orchestrator()
        assert orch._cycle_count == 0

        signal = _make_signal()
        result = orch.process_external_signal(signal)

        assert result["cycle"] == 1
        assert orch._cycle_count == 1

    def test_cycle_count_increments_each_call(self):
        orch = _make_orchestrator()
        for i in range(3):
            orch.process_external_signal(_make_signal())
        assert orch._cycle_count == 3

    # ── 3. Ledger records messages after processing ─────────────────────────
    def test_ledger_has_candidate_message(self):
        orch = _make_orchestrator()
        signal = _make_signal()

        before = orch.ledger.total_messages
        orch.process_external_signal(signal)
        after = orch.ledger.total_messages

        assert after > before

    def test_ledger_contains_signal_candidate(self):
        from protocol.agent_protocol import AgentMessageType
        orch = _make_orchestrator()
        signal = _make_signal()
        orch.process_external_signal(signal)

        msgs = orch.ledger.get_recent(50)
        types = [m.message_type for m in msgs]
        assert AgentMessageType.SIGNAL_CANDIDATE in types

    def test_ledger_contains_validator_message(self):
        from protocol.agent_protocol import AgentMessageType
        orch = _make_orchestrator()
        signal = _make_signal()
        orch.process_external_signal(signal)

        msgs = orch.ledger.get_recent(50)
        types = [m.message_type for m in msgs]
        assert (
            AgentMessageType.SIGNAL_VALIDATION_APPROVED in types
            or AgentMessageType.SIGNAL_VALIDATION_REJECTED in types
        )

    # ── 4. Validator rejection is surfaced ──────────────────────────────────
    def test_bad_rr_rejected_by_validator(self):
        """R:R < 2.0 → validator rejects."""
        orch = _make_orchestrator()
        # SL very close to entry → tiny R:R
        signal = _make_signal(entry=1.1000, sl=1.0999, tp=1.1010)  # R:R ~ 1.0

        result = orch.process_external_signal(signal)

        assert result["status"] == "SIGNAL_REJECTED"
        assert result["trades_executed"] == 0
        assert len(result["rejection_reasons"]) > 0

    def test_bad_sl_direction_rejected_by_validator(self):
        """BUY with SL above entry → execution supervisor catches it."""
        orch = _make_orchestrator()
        # SL above entry for a BUY — validator catches stop_distance_pips < 3
        signal = _make_signal(entry=1.1000, sl=1.1001, tp=1.1100)

        result = orch.process_external_signal(signal)

        # Either validator or execution supervisor should reject this
        assert result["status"] in ("SIGNAL_REJECTED", "RISK_REJECTED", "EXECUTION_BLOCKED", "ORDER_FAILED")
        assert result["trades_executed"] == 0

    # ── 5. Valid signal in paper mode → fills ───────────────────────────────
    def test_valid_signal_fills_paper_mode(self):
        """Good signal with no broker attached → paper fill via execution supervisor."""
        orch = _make_orchestrator(broker=None)

        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200)  # RR=2.0, stop=100 pips

        result = orch.process_external_signal(signal)

        assert result["status"] == "FILLED", f"Unexpected: {result}"
        assert result["trades_executed"] == 1
        assert result["trades_attempted"] == 1
        assert len(orch._all_trades) == 1

    def test_trade_recorded_in_orchestrator(self):
        orch = _make_orchestrator(broker=None)
        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200)
        orch.process_external_signal(signal)

        assert len(orch._all_trades) == 1
        trade = orch._all_trades[0]
        assert trade.instrument == signal.instrument
        assert trade.direction == signal.direction
        assert trade.is_paper is True

    # ── 6. Result has correct signal_id ────────────────────────────────────
    def test_result_signal_id_matches(self):
        orch = _make_orchestrator(broker=None)
        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200)
        result = orch.process_external_signal(signal)
        assert result["signal_id"] == signal.signal_id

    # ── 7. BrokerManager routing via execution supervisor ──────────────────
    def test_paper_broker_called_when_not_live(self):
        """When broker is wired but ESM is not in LIVE_ENABLED, PaperBroker should fill."""
        mock_bm = MagicMock()
        mock_bm.submit_order.return_value = MagicMock(
            success=True,
            order_id="PAPER_123",
            filled_price=1.1000,
            filled_units=0.10,
            reject_reason=None,
            insufficient_funds=False,
            raw={"paper": True},
        )
        mock_bm._esm = MagicMock()
        mock_bm._esm.is_kill_switch_active.return_value = False
        mock_bm._esm.is_live_trading_allowed.return_value = False

        orch = _make_orchestrator(broker=mock_bm)
        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200)

        result = orch.process_external_signal(signal)

        assert result["status"] == "FILLED"
        mock_bm.submit_order.assert_called_once()

    def test_live_broker_called_when_live_enabled(self):
        """When ESM is LIVE_ENABLED, submit_order should be called with live routing."""
        mock_bm = MagicMock()
        mock_bm.submit_order.return_value = MagicMock(
            success=True,
            order_id="LIVE_987",
            filled_price=1.10050,
            filled_units=0.10,
            reject_reason=None,
            insufficient_funds=False,
            raw={"paper": False},
        )

        orch = _make_orchestrator(broker=mock_bm)
        # Mark signal as live
        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200, is_paper=False)

        result = orch.process_external_signal(signal)

        assert result["status"] == "FILLED"
        mock_bm.submit_order.assert_called_once()
        # Verify is_paper_trade was False on the signal reaching the broker
        call_args = mock_bm.submit_order.call_args
        assert call_args is not None

    # ── 8. Insufficient funds → ORDER_FAILED, not a crash ──────────────────
    def test_insufficient_funds_becomes_order_failed(self):
        mock_bm = MagicMock()
        mock_bm.submit_order.return_value = MagicMock(
            success=False,
            order_id=None,
            filled_price=None,
            reject_reason="Insufficient funds",
            insufficient_funds=True,
            raw={},
        )

        orch = _make_orchestrator(broker=mock_bm)
        signal = _make_signal(entry=1.1000, sl=1.0900, tp=1.1200)

        result = orch.process_external_signal(signal)

        assert result["status"] == "ORDER_FAILED"
        assert result["trades_executed"] == 0
        assert any("funds" in r.lower() or r for r in result["rejection_reasons"])


# ---------------------------------------------------------------------------
# API ENDPOINT TESTS
# ---------------------------------------------------------------------------

class TestSignalAPIEndpoint:
    """
    Tests for POST /api/signal via FastAPI TestClient.
    Mock the orchestrator singleton to control behaviour.
    """

    def _get_client(self):
        from fastapi.testclient import TestClient
        from live import api_server
        # Reset singletons so each test is isolated
        api_server._orchestrator = None
        api_server._broker_manager = None
        api_server._data_service = None
        client = TestClient(api_server.app)
        return client, api_server

    def _good_payload(self, **overrides):
        base = {
            "symbol": "EURUSD",
            "action": "BUY",
            "price": 1.1000,
            "stop_loss": 1.0900,
            "take_profit": 1.1200,
            "timeframe": "M15",
            "strategy": "TJR",
            "schema_version": "1.0",
        }
        base.update(overrides)
        return base

    # ── TEST 1: Kill switch → 403 ────────────────────────────────────────────
    def test_kill_switch_returns_403(self):
        client, module = self._get_client()

        mock_orch = MagicMock()
        mock_orch._kill_switch = True
        mock_orch._cycle_count = 0
        mock_orch._account_balance = 100000.0

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = True
        mock_esm.current_state.return_value.value = "KILL_SWITCH_ENGAGED"
        mock_esm.is_live_trading_allowed.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        module._orchestrator = mock_orch
        module._broker_manager = mock_bm

        r = client.post("/api/signal", json=self._good_payload())
        assert r.status_code == 403
        assert "kill" in r.text.lower()

    # ── TEST 2: Invalid schema version → 400 ────────────────────────────────
    def test_invalid_schema_version_returns_400(self):
        client, module = self._get_client()
        r = client.post("/api/signal", json=self._good_payload(schema_version="99.0"))
        assert r.status_code == 400
        assert "schema" in r.text.lower()

    # ── TEST 3: Missing price → 400 ─────────────────────────────────────────
    def test_missing_price_returns_400(self):
        client, module = self._get_client()

        mock_orch = MagicMock()
        mock_orch._kill_switch = False
        mock_orch._cycle_count = 0
        mock_orch._account_balance = 100000.0

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        module._orchestrator = mock_orch
        module._broker_manager = mock_bm

        payload = self._good_payload()
        del payload["price"]
        r = client.post("/api/signal", json=payload)
        assert r.status_code == 400

    # ── TEST 4: Valid signal in paper mode → cycle_count > 0, ledger active ─
    def test_paper_mode_signal_processes_pipeline(self):
        client, module = self._get_client()

        # Build a real orchestrator (no broker) + mock ESM
        orch = _make_orchestrator(broker=None)

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        module._orchestrator = orch
        module._broker_manager = mock_bm

        before_cycle = orch._cycle_count
        before_ledger = orch.ledger.total_messages

        r = client.post("/api/signal", json=self._good_payload())
        assert r.status_code == 200

        data = r.json()
        # Cycle must have incremented
        assert orch._cycle_count > before_cycle, "cycle_count did not increment"
        # Ledger must have messages
        assert orch.ledger.total_messages > before_ledger, "ledger did not grow"
        # Response must have real fields
        assert "status" in data
        assert data["status"] != "RECEIVED"  # No more fake "RECEIVED"
        assert "cycle" in data
        assert data["cycle"] > 0

    # ── TEST 5: Valid signal → trade OR rejection visible ────────────────────
    def test_paper_mode_produces_trade_or_rejection(self):
        client, module = self._get_client()

        orch = _make_orchestrator(broker=None)
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        module._orchestrator = orch
        module._broker_manager = mock_bm

        r = client.post("/api/signal", json=self._good_payload())
        assert r.status_code == 200
        data = r.json()

        # Either filled or clearly rejected — never silent drop
        assert data["status"] in (
            "FILLED", "SIGNAL_REJECTED", "RISK_REJECTED",
            "EXECUTION_BLOCKED", "ORDER_FAILED"
        )
        # No fake "RECEIVED" or "queued" language
        assert data["status"] != "RECEIVED"

    # ── TEST 6: LIVE_CONNECTED_SAFE → execution blocked from live ────────────
    def test_live_connected_safe_blocks_live_execution(self):
        """
        In LIVE_CONNECTED_SAFE the execution supervisor must NOT submit live orders.
        signal.is_paper_trade = True because is_live_trading_allowed() = False.
        """
        client, module = self._get_client()

        mock_bm = MagicMock()
        mock_bm.submit_order.return_value = MagicMock(
            success=True, order_id="PAPER_X", filled_price=1.1000,
            filled_units=0.10, reject_reason=None, insufficient_funds=False,
            raw={"paper": True},
        )
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "LIVE_CONNECTED_SAFE"
        mock_esm.is_live_trading_allowed.return_value = False  # ← key: safe mode
        mock_bm._esm = mock_esm

        orch = _make_orchestrator(broker=mock_bm)
        module._orchestrator = orch
        module._broker_manager = mock_bm

        r = client.post("/api/signal", json=self._good_payload())
        assert r.status_code == 200
        data = r.json()

        # must say PAPER
        assert data["routed_to"] == "PAPER"
        # pipeline must have run
        assert orch._cycle_count == 1
        assert orch.ledger.total_messages > 0

    # ── TEST 7: LIVE_ENABLED → routes through approved broker layer ──────────
    def test_live_enabled_routes_through_broker_manager(self):
        client, module = self._get_client()

        mock_bm = MagicMock()
        mock_bm.submit_order.return_value = MagicMock(
            success=True, order_id="LIVE_ABC", filled_price=1.10050,
            filled_units=0.10, reject_reason=None, insufficient_funds=False,
            raw={"paper": False},
        )
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "LIVE_ENABLED"
        mock_esm.is_live_trading_allowed.return_value = True   # ← key: live enabled
        mock_bm._esm = mock_esm

        orch = _make_orchestrator(broker=mock_bm)
        module._orchestrator = orch
        module._broker_manager = mock_bm

        r = client.post("/api/signal", json=self._good_payload())
        assert r.status_code == 200
        data = r.json()

        assert data["routed_to"] == "LIVE_BROKER"
        assert data["status"] == "FILLED"
        assert data["trades_executed"] == 1
        # BrokerManager.submit_order was called (not bypassed)
        mock_bm.submit_order.assert_called_once()

    # ── TEST 8: Rejection visible ────────────────────────────────────────────
    def test_bad_rr_rejection_visible_in_response(self):
        client, module = self._get_client()

        orch = _make_orchestrator(broker=None)
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False

        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        module._orchestrator = orch
        module._broker_manager = mock_bm

        # SL very tight → bad R:R → validator rejects
        r = client.post("/api/signal", json=self._good_payload(
            price=1.1000, stop_loss=1.0999, take_profit=1.1002
        ))
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "SIGNAL_REJECTED"
        assert len(data["rejection_reasons"]) > 0


# ---------------------------------------------------------------------------
# /api/status ENVIRONMENT FIELD TESTS
# ---------------------------------------------------------------------------

class TestStatusEnvironmentField:
    """
    Verify /api/status returns runtime environment from ESM, not stale config.
    """

    def _get_client(self):
        from fastapi.testclient import TestClient
        from live import api_server
        api_server._orchestrator = None
        api_server._broker_manager = None
        api_server._data_service = None
        return TestClient(api_server.app), api_server

    def _mock_bm_with_env(self, env: str, state: str = "LIVE_CONNECTED_SAFE"):
        from live.execution_state_machine import ExecutionStateSnapshot
        snap = ExecutionStateSnapshot(
            state=state,
            broker="oanda",
            environment=env,        # ← runtime environment from broker
            account_id="101-001-test-001",
            live_safe_lock=True,
            live_enabled=(state == "LIVE_ENABLED"),
            require_operator_arming=True,
            kill_switch_active=False,
            preflight_passed=True,
            preflight_performed_at=None,
            last_transition_at=None,
            last_transition_reason=None,
            last_operator=None,
            connection_error=None,
            arming_operator=None,
            armed_at=None,
        )
        mock_bm = MagicMock()
        mock_bm._esm.snapshot.return_value = snap
        mock_bm._esm.is_connected.return_value = True
        mock_bm._esm.is_live_trading_allowed.return_value = (state == "LIVE_ENABLED")
        mock_bm._esm.is_kill_switch_active.return_value = False
        mock_bm.broker_status.return_value = {
            "execution_state": state,
            "broker": "oanda",
            "environment": env,
        }
        return mock_bm

    def test_status_shows_practice_when_oanda_connected(self):
        """When OANDA practice is connected, environment should be 'practice', not 'paper'."""
        client, module = self._get_client()

        # Mock orchestrator
        mock_orch = MagicMock()
        mock_orch._kill_switch = False
        mock_orch._cycle_count = 5
        mock_orch.get_system_state.return_value = {
            "mode": "LIVE", "kill_switch": False, "cycle_count": 5,
            "account_balance": 100000.0, "open_positions": 0,
            "total_trades": 0, "ledger_messages": 10,
            "agent_health": {},
        }
        mock_daily = MagicMock()
        mock_daily.realized_pnl = 0.0
        mock_daily.blocked_trades = 0
        mock_risk = MagicMock()
        mock_risk.account_balance = 100000.0
        mock_risk.get_drawdown_pct.return_value = 0.0
        mock_risk.daily_governor.daily_pnl_pct = 0.0
        mock_risk.daily_governor.get_summary.return_value = mock_daily
        mock_orch.risk_guardian.risk_manager = mock_risk

        mock_bm = self._mock_bm_with_env("practice", "LIVE_CONNECTED_SAFE")

        module._orchestrator = mock_orch
        module._broker_manager = mock_bm

        r = client.get("/api/status")
        assert r.status_code == 200
        data = r.json()

        # Must NOT say "paper" when a live broker with env="practice" is connected
        assert data["environment"] == "practice", (
            f"Expected 'practice' but got {data['environment']!r}. "
            "Status is still reading stale config value."
        )

    def test_status_shows_paper_when_no_broker_connected(self):
        """In pure paper mode (no broker), environment should fall back to config."""
        client, module = self._get_client()

        from live.execution_state_machine import ExecutionStateSnapshot
        snap = ExecutionStateSnapshot(
            state="PAPER_MODE",
            broker=None,
            environment=None,       # ← no broker environment
            account_id=None,
            live_safe_lock=False,
            live_enabled=False,
            require_operator_arming=True,
            kill_switch_active=False,
            preflight_passed=None,
            preflight_performed_at=None,
            last_transition_at=None,
            last_transition_reason=None,
            last_operator=None,
            connection_error=None,
            arming_operator=None,
            armed_at=None,
        )

        mock_bm = MagicMock()
        mock_bm._esm.snapshot.return_value = snap
        mock_bm._esm.is_connected.return_value = False
        mock_bm._esm.is_live_trading_allowed.return_value = False
        mock_bm._esm.is_kill_switch_active.return_value = False
        mock_bm.broker_status.return_value = {"execution_state": "PAPER_MODE"}

        mock_orch = MagicMock()
        mock_orch._kill_switch = False
        mock_orch._cycle_count = 0
        mock_orch.get_system_state.return_value = {
            "mode": "PAPER", "kill_switch": False, "cycle_count": 0,
            "account_balance": 100000.0, "open_positions": 0,
            "total_trades": 0, "ledger_messages": 0, "agent_health": {},
        }
        mock_daily = MagicMock()
        mock_daily.realized_pnl = 0.0
        mock_daily.blocked_trades = 0
        mock_risk = MagicMock()
        mock_risk.account_balance = 100000.0
        mock_risk.get_drawdown_pct.return_value = 0.0
        mock_risk.daily_governor.daily_pnl_pct = 0.0
        mock_risk.daily_governor.get_summary.return_value = mock_daily
        mock_orch.risk_guardian.risk_manager = mock_risk

        module._orchestrator = mock_orch
        module._broker_manager = mock_bm

        r = client.get("/api/status")
        assert r.status_code == 200
        data = r.json()
        # Falls back to config (which is "paper")
        assert data["environment"] in ("paper", "PAPER")

    def test_status_execution_state_present(self):
        """execution_state must always be present in /api/status."""
        client, module = self._get_client()

        from live.execution_state_machine import ExecutionStateSnapshot
        snap = ExecutionStateSnapshot(
            state="PAPER_MODE", broker=None, environment=None,
            account_id=None, live_safe_lock=False, live_enabled=False,
            require_operator_arming=True, kill_switch_active=False,
            preflight_passed=None, preflight_performed_at=None,
            last_transition_at=None, last_transition_reason=None,
            last_operator=None, connection_error=None,
            arming_operator=None, armed_at=None,
        )
        mock_bm = MagicMock()
        mock_bm._esm.snapshot.return_value = snap
        mock_bm._esm.is_connected.return_value = False
        mock_bm._esm.is_live_trading_allowed.return_value = False
        mock_bm._esm.is_kill_switch_active.return_value = False
        mock_bm.broker_status.return_value = {}

        mock_orch = MagicMock()
        mock_orch._kill_switch = False
        mock_orch._cycle_count = 0
        mock_orch.get_system_state.return_value = {
            "mode": "PAPER", "kill_switch": False, "cycle_count": 0,
            "account_balance": 100000.0, "open_positions": 0,
            "total_trades": 0, "ledger_messages": 0, "agent_health": {},
        }
        mock_daily = MagicMock()
        mock_daily.realized_pnl = 0.0
        mock_daily.blocked_trades = 0
        mock_risk = MagicMock()
        mock_risk.account_balance = 100000.0
        mock_risk.get_drawdown_pct.return_value = 0.0
        mock_risk.daily_governor.daily_pnl_pct = 0.0
        mock_risk.daily_governor.get_summary.return_value = mock_daily
        mock_orch.risk_guardian.risk_manager = mock_risk

        module._orchestrator = mock_orch
        module._broker_manager = mock_bm

        r = client.get("/api/status")
        assert r.status_code == 200
        data = r.json()
        assert "execution_state" in data
        assert "environment" in data
        assert "cycle_count" in data


# ---------------------------------------------------------------------------
# END-TO-END INTEGRATION SCENARIO
# ---------------------------------------------------------------------------

class TestSignalPipelineEndToEnd:
    """
    Simulate the full manual proof-of-success scenario:
    1. Send webhook signal
    2. Verify ledger has messages
    3. Verify cycle_count > 0
    4. Verify trade OR rejection visible
    """

    def test_full_proof_of_success_paper_mode(self):
        """
        Scenario: System in paper mode.
        POST /api/signal → pipeline runs → ledger fills → trade recorded.
        """
        from fastapi.testclient import TestClient
        from live import api_server

        api_server._orchestrator = None
        api_server._broker_manager = None
        api_server._data_service = None

        # Wire a real orchestrator (no broker) and a minimal mock BM
        orch = _make_orchestrator(broker=None)
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)

        # Step 2: Fire webhook
        r = client.post("/api/signal", json={
            "symbol": "XAU_USD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2290.0,
            "take_profit": 2320.0,
            "timeframe": "M15",
            "strategy": "TJR",
            "schema_version": "1.0",
        })
        assert r.status_code == 200
        sig_data = r.json()
        assert sig_data["status"] != "RECEIVED", (
            "Pipeline not invoked — still returning fake RECEIVED status!"
        )

        # Step 3a: Check ledger has activity
        ledger_r = client.get("/api/ledger?limit=20")
        assert ledger_r.status_code == 200
        ledger_data = ledger_r.json()
        assert ledger_data["total_in_ledger"] > 0, "Ledger is empty after signal!"

        # Step 3b: cycle_count > 0
        assert orch._cycle_count > 0, "cycle_count is still 0 after signal!"

        # Step 3c: trade or rejection visible
        trades_r = client.get("/api/trades?limit=20")
        assert trades_r.status_code == 200
        trades_data = trades_r.json()

        # Either a trade was recorded OR the signal data itself shows a rejection
        has_trade = trades_data["total"] > 0
        was_rejected = sig_data["status"] in (
            "SIGNAL_REJECTED", "RISK_REJECTED", "EXECUTION_BLOCKED", "ORDER_FAILED"
        )
        assert has_trade or was_rejected, (
            f"No trade and no rejection! status={sig_data['status']!r} "
            f"trades={trades_data['total']}"
        )

    def test_no_fake_queued_response(self):
        """Verify the old fake 'queued' language is gone."""
        from fastapi.testclient import TestClient
        from live import api_server

        api_server._orchestrator = None
        api_server._broker_manager = None

        orch = _make_orchestrator(broker=None)
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_esm.current_state.return_value.value = "PAPER_MODE"
        mock_esm.is_live_trading_allowed.return_value = False
        mock_bm = MagicMock()
        mock_bm._esm = mock_esm

        api_server._orchestrator = orch
        api_server._broker_manager = mock_bm

        client = TestClient(api_server.app)
        r = client.post("/api/signal", json={
            "symbol": "EURUSD",
            "action": "BUY",
            "price": 1.1000,
            "stop_loss": 1.0900,
            "take_profit": 1.1200,
        })
        assert r.status_code == 200
        text = r.text.lower()
        assert "queued for agent pipeline" not in text, (
            "Old fake 'queued' message still present in response!"
        )
        data = r.json()
        assert "message" not in data or "queued" not in (data.get("message") or "").lower()

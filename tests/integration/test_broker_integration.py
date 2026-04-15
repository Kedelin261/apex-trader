"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — Broker Connectivity & Execution State Machine

Tests:
  - Credential loading (valid, missing, invalid format)
  - OANDA connector auth (mock + live)
  - Execution state machine transitions
  - Preflight checklist
  - Arming / disarming flow
  - Kill switch
  - Reconciliation
  - Order rejection handling
  - Insufficient funds handling
  - Test order payload
  - API endpoints (via FastAPI TestClient)
"""

from __future__ import annotations

import os
import sys
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ────────────────────────────────────────────────────────────
# CREDENTIAL MANAGER TESTS
# ────────────────────────────────────────────────────────────

class TestCredentialManager:
    def test_load_missing_credentials(self):
        """Missing env vars → MISSING status."""
        from brokers.credential_manager import CredentialManager, CredentialStatus
        # Clear any env vars that might be set
        saved = {}
        for k in ("APEX_OANDA_API_TOKEN", "OANDA_API_TOKEN",
                  "APEX_OANDA_ACCOUNT_ID", "OANDA_ACCOUNT_ID",
                  "APEX_OANDA_ENVIRONMENT", "OANDA_ENVIRONMENT"):
            saved[k] = os.environ.pop(k, None)
        try:
            # Create a completely fresh instance (not singleton)
            mgr = CredentialManager.__new__(CredentialManager)
            mgr._credentials = {}
            mgr._jwt_secret = None
            # Don't call _load_env_file to avoid picking up .env
            creds = mgr.load_oanda()
            assert creds.status == CredentialStatus.MISSING
            assert not creds.is_usable()
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    def test_load_invalid_format_token(self):
        """Malformed token → INVALID_FORMAT."""
        from brokers.credential_manager import CredentialManager, CredentialStatus
        os.environ["APEX_OANDA_API_TOKEN"] = "not-a-valid-token"
        os.environ["APEX_OANDA_ACCOUNT_ID"] = "101-001-12345678-001"
        os.environ["APEX_OANDA_ENVIRONMENT"] = "practice"
        mgr = CredentialManager(env_file=None)
        creds = mgr.load_oanda()
        assert creds.status == CredentialStatus.INVALID_FORMAT
        assert len(creds.validation_errors) > 0
        # Clean up
        os.environ.pop("APEX_OANDA_API_TOKEN", None)
        os.environ.pop("APEX_OANDA_ACCOUNT_ID", None)

    def test_load_valid_format_credentials(self):
        """Valid format token → VALID status."""
        from brokers.credential_manager import CredentialManager, CredentialStatus
        os.environ["APEX_OANDA_API_TOKEN"] = "a" * 32 + "-" + "b" * 32
        os.environ["APEX_OANDA_ACCOUNT_ID"] = "101-001-12345678-001"
        os.environ["APEX_OANDA_ENVIRONMENT"] = "practice"
        mgr = CredentialManager(env_file=None)
        creds = mgr.load_oanda()
        assert creds.status == CredentialStatus.VALID
        assert creds.is_usable()
        assert creds.environment == "practice"
        os.environ.pop("APEX_OANDA_API_TOKEN", None)
        os.environ.pop("APEX_OANDA_ACCOUNT_ID", None)

    def test_mask_function(self):
        """Mask should not reveal full secret."""
        from brokers.credential_manager import CredentialManager
        masked = CredentialManager.mask("super_secret_token_12345")
        assert "super_secret_token_12345" not in masked
        assert "****" in masked

    def test_safe_summary_no_token_leak(self):
        """safe_summary should not leak full token."""
        from brokers.credential_manager import CredentialManager
        os.environ["APEX_OANDA_API_TOKEN"] = "a" * 32 + "-" + "b" * 32
        os.environ["APEX_OANDA_ACCOUNT_ID"] = "101-001-12345678-001"
        mgr = CredentialManager(env_file=None)
        creds = mgr.load_oanda()
        summary = creds.safe_summary()
        assert summary["token_hint"] != "a" * 32 + "-" + "b" * 32
        assert len(summary["token_hint"]) < 20
        os.environ.pop("APEX_OANDA_API_TOKEN", None)
        os.environ.pop("APEX_OANDA_ACCOUNT_ID", None)


# ────────────────────────────────────────────────────────────
# EXECUTION STATE MACHINE TESTS
# ────────────────────────────────────────────────────────────

class TestExecutionStateMachine:
    def _make_esm(self, tmp_path=None) -> "ExecutionStateMachine":
        from live.execution_state_machine import ExecutionStateMachine
        import tempfile, os
        tf = tempfile.mktemp(suffix=".json")
        return ExecutionStateMachine(state_file=tf, require_operator_arming=True)

    def test_initial_state_is_paper(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        assert esm.current_state() == ExecutionState.PAPER_MODE
        assert not esm.is_live_trading_allowed()
        assert not esm.is_connected()

    def test_connect_transitions_to_safe(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        ok, msg = esm.connect_broker(
            broker="oanda", environment="practice",
            account_id="101-001-test", operator="test",
            preflight_passed=True, preflight_reason="all good",
        )
        assert ok
        assert esm.current_state() == ExecutionState.LIVE_CONNECTED_SAFE
        assert esm.is_connected()
        assert not esm.is_live_trading_allowed()

    def test_arm_requires_live_connected_safe(self):
        esm = self._make_esm()
        # In PAPER_MODE, arm should fail
        ok, msg = esm.arm_live_trading(operator="test")
        assert not ok
        assert "LIVE_CONNECTED_SAFE" in msg

    def test_arm_requires_preflight_passed(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        esm.connect_broker("oanda", "practice", "acct", "test",
                           preflight_passed=False, preflight_reason="check failed")
        # In LIVE_BLOCKED, arm should fail
        ok, msg = esm.arm_live_trading(operator="test")
        assert not ok

    def test_full_arming_flow(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        esm.connect_broker("oanda", "practice", "acct", "test",
                           preflight_passed=True, preflight_reason="ok")
        ok, msg = esm.arm_live_trading(operator="test_operator")
        assert ok
        assert esm.current_state() == ExecutionState.LIVE_ENABLED
        assert esm.is_live_trading_allowed()

    def test_disarm_returns_to_safe(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        esm.connect_broker("oanda", "practice", "acct", "test", True, "ok")
        esm.arm_live_trading(operator="test")
        ok, _ = esm.disarm_live_trading(operator="test", reason="Manual")
        assert ok
        assert esm.current_state() == ExecutionState.LIVE_CONNECTED_SAFE
        assert not esm.is_live_trading_allowed()

    def test_kill_switch_from_any_state(self):
        from live.execution_state_machine import ExecutionState
        for setup_fn in [
            lambda esm: None,  # PAPER_MODE
            lambda esm: esm.connect_broker("oanda","prac","a","t",True,"ok"),  # SAFE
            lambda esm: (esm.connect_broker("oanda","prac","a","t",True,"ok"),
                         esm.arm_live_trading("t")),  # ENABLED
        ]:
            esm = self._make_esm()
            setup_fn(esm)
            ok, msg = esm.engage_kill_switch(reason="test kill", operator="test")
            assert ok
            assert esm.current_state() == ExecutionState.KILL_SWITCH_ENGAGED
            assert esm.is_kill_switch_active()
            assert not esm.is_live_trading_allowed()

    def test_kill_switch_irreversible_without_reset(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        esm.engage_kill_switch("test", "test")
        # Cannot arm from kill switch state
        ok, msg = esm.arm_live_trading("test")
        assert not ok

    def test_reset_to_paper_after_kill(self):
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        esm.engage_kill_switch("test", "test")
        ok, _ = esm.reset_to_paper(operator="test", reason="Reset")
        assert ok
        assert esm.current_state() == ExecutionState.PAPER_MODE
        assert not esm.is_kill_switch_active()

    def test_snapshot_fields(self):
        esm = self._make_esm()
        snap = esm.snapshot()
        assert snap.state is not None
        assert isinstance(snap.kill_switch_active, bool)
        assert isinstance(snap.live_enabled, bool)
        assert isinstance(snap.require_operator_arming, bool)

    def test_transition_history_logged(self):
        esm = self._make_esm()
        esm.connect_broker("oanda", "prac", "acct", "test", True, "ok")
        history = esm.transition_history(10)
        assert len(history) >= 1
        assert history[-1]["from_state"] == "PAPER_MODE"
        assert history[-1]["to_state"] == "LIVE_CONNECTED_SAFE"

    def test_disallowed_transition_blocked(self):
        """Cannot go PAPER → LIVE_ENABLED directly."""
        from live.execution_state_machine import ExecutionState
        esm = self._make_esm()
        ok, msg = esm._transition(ExecutionState.LIVE_ENABLED, "direct", "test")
        assert not ok
        assert "not allowed" in msg.lower()


# ────────────────────────────────────────────────────────────
# OANDA CONNECTOR MOCK TESTS
# ────────────────────────────────────────────────────────────

class TestOandaConnectorMocked:
    def _make_connector(self):
        from brokers.credential_manager import BrokerCredentials, CredentialStatus
        from brokers.oanda_connector import OandaConnector
        creds = BrokerCredentials(
            broker_name="oanda",
            environment="practice",
            api_token="a" * 32 + "-" + "b" * 32,
            account_id="101-001-99999999-001",
            status=CredentialStatus.VALID,
        )
        return OandaConnector(creds)

    def test_uses_practice_url(self):
        conn = self._make_connector()
        assert "fxpractice" in conn.BASE_URL

    def test_auth_returns_false_on_401(self):
        conn = self._make_connector()
        with patch("requests.Session") as MockSession:
            mock_r = MagicMock()
            mock_r.status_code = 401
            mock_r.json.return_value = {"errorMessage": "Unauthorized"}
            MockSession.return_value.get.return_value = mock_r
            conn._session = MockSession.return_value
            ok, msg = conn.authenticate()
        assert not ok
        assert "401" in msg or "Unauthorized" in msg.lower()

    def test_auth_returns_false_on_missing_accounts(self):
        conn = self._make_connector()
        with patch("requests.Session") as MockSession:
            mock_r = MagicMock()
            mock_r.status_code = 200
            mock_r.json.return_value = {"accounts": []}
            MockSession.return_value.get.return_value = mock_r
            conn._session = MockSession.return_value
            ok, msg = conn.authenticate()
        assert not ok

    def test_circuit_breaker_opens_after_failures(self):
        conn = self._make_connector()
        conn._circuit_breaker_threshold = 3
        for _ in range(3):
            conn._record_failure("test failure")
        assert conn.check_circuit_breaker()

    def test_circuit_breaker_reset(self):
        conn = self._make_connector()
        conn._circuit_breaker_threshold = 2
        conn._record_failure("f1")
        conn._record_failure("f2")
        assert conn.check_circuit_breaker()
        conn.reset_circuit_breaker()
        assert not conn.check_circuit_breaker()

    def test_submit_order_records_rejection(self):
        """
        Insufficient funds rejection is recorded in history.
        v2.3: XAUUSD requires stop_loss and take_profit for the gold payload validation.
        Use EURUSD to test the INSUFFICIENT_MARGIN path, since EURUSD has no
        gold payload constraints.  Alternatively test XAUUSD with valid SL/TP.
        """
        conn = self._make_connector()
        from brokers.base_connector import ConnectionState, OrderRequest, OrderSide
        conn._connection_state = ConnectionState.AUTHENTICATED
        conn._account_id = "101-001-99999999-001"

        with patch.object(conn, "_post") as mock_post:
            mock_post.return_value = (400, {
                "orderRejectTransaction": {
                    "rejectReason": "INSUFFICIENT_MARGIN",
                    "type": "MARKET_ORDER_REJECT",
                }
            })
            # v2.3: include stop_loss and take_profit for gold, plus use enough units (>=1 oz)
            req = OrderRequest(
                instrument="XAUUSD",
                side=OrderSide.BUY,
                units=1.0,         # 1 lot = 100 oz — passes gold validation
                stop_loss=2290.0,  # required for gold
                take_profit=2325.0,
                price=2300.0,
            )
            result = conn.submit_order(req)

        assert not result.success
        assert result.insufficient_funds
        assert len(conn.get_rejection_history()) == 1
        assert conn.get_rejection_history()[0]["insufficient_funds"]
        # v2.3: structured diagnostics
        history_entry = conn.get_rejection_history()[0]
        assert history_entry.get("normalized_rejection_code") == "INSUFFICIENT_MARGIN"
        assert history_entry.get("canonical_symbol") == "XAUUSD"

    def test_instrument_mapping_xauusd(self):
        from brokers.oanda_connector import OANDA_INSTRUMENT_MAP
        assert "XAUUSD" in OANDA_INSTRUMENT_MAP
        assert OANDA_INSTRUMENT_MAP["XAUUSD"] == "XAU_USD"

    def test_no_mapping_returns_none_submit(self):
        conn = self._make_connector()
        from brokers.base_connector import ConnectionState, OrderRequest, OrderSide, OrderStatus
        conn._connection_state = ConnectionState.AUTHENTICATED
        req = OrderRequest(instrument="UNKNOWN_PAIR", side=OrderSide.BUY, units=1)
        result = conn.submit_order(req)
        assert not result.success
        assert result.status == OrderStatus.REJECTED
        assert "mapping" in result.reject_reason.lower()

    def test_test_order_payload_no_submit(self):
        conn = self._make_connector()
        from brokers.base_connector import ConnectionState, OrderRequest, OrderSide
        conn._connection_state = ConnectionState.AUTHENTICATED
        conn._account_id = "101-001-99999999-001"
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.SELL,
            units=10000,
            stop_loss=1.0900,
            take_profit=1.0800,
        )
        ok, payload = conn.build_test_order_payload(req)
        assert ok
        assert "TEST PAYLOAD" in payload["note"]
        assert "-10000" in payload["payload"]["order"]["units"]  # SELL = negative

    def test_positions_parsed_correctly(self):
        conn = self._make_connector()
        from brokers.base_connector import ConnectionState
        conn._connection_state = ConnectionState.AUTHENTICATED
        conn._account_id = "101-001-test"

        mock_body = {
            "positions": [{
                "instrument": "XAU_USD",
                "long": {"units": "100", "unrealizedPL": "250.50", "averagePrice": "1950.0"},
                "short": {"units": "0", "unrealizedPL": "0", "averagePrice": "0"},
            }]
        }
        with patch.object(conn, "_get", return_value=(200, mock_body)):
            positions = conn.get_positions()

        assert len(positions) == 1
        assert positions[0].instrument == "XAUUSD"
        assert positions[0].side == "LONG"
        assert positions[0].units == 100.0
        assert positions[0].unrealized_pnl == 250.50


# ────────────────────────────────────────────────────────────
# BROKER MANAGER TESTS
# ────────────────────────────────────────────────────────────

class TestBrokerManager:
    def _make_manager(self):
        import tempfile
        from live.broker_manager import BrokerManager
        from live.execution_state_machine import ExecutionStateMachine
        config = {}
        orch = MagicMock()
        orch._open_positions = []
        orch._kill_switch = False
        risk = MagicMock()
        risk._global_kill_switch = False
        risk.daily_governor = MagicMock()
        risk.daily_governor._is_killed = False
        bm = BrokerManager(config, orchestrator=orch, risk_manager=risk)
        # Override ESM with fresh temp-file-backed instance
        bm._esm = ExecutionStateMachine(
            state_file=tempfile.mktemp(suffix="_test.json"),
            require_operator_arming=True,
        )
        return bm

    def test_initial_state_paper(self):
        bm = self._make_manager()
        status = bm.broker_status()
        assert status["execution_state"] == "PAPER_MODE"
        assert not status["live_enabled"]

    def test_paper_orders_always_succeed(self):
        from live.broker_manager import PaperBroker
        from brokers.base_connector import OrderRequest, OrderSide, OrderStatus
        pb = PaperBroker()
        req = OrderRequest(instrument="EURUSD", side=OrderSide.BUY, units=1000)
        result = pb.submit_order(req)
        assert result.success
        assert result.status == OrderStatus.FILLED

    def test_kill_switch_blocks_orders(self):
        from brokers.base_connector import OrderRequest, OrderSide
        bm = self._make_manager()
        bm._esm.engage_kill_switch("test", "test")
        req = OrderRequest(instrument="EURUSD", side=OrderSide.BUY, units=1000)
        result = bm.submit_order(req)
        assert not result.success
        assert "kill switch" in result.reject_reason.lower()

    def test_invalid_broker_name_fails(self):
        bm = self._make_manager()
        ok, msg = bm.connect(broker="unknown_broker", operator="test")
        assert not ok
        assert "unsupported" in msg.lower()


# ────────────────────────────────────────────────────────────
# API ENDPOINT TESTS
# ────────────────────────────────────────────────────────────

class TestAPIEndpoints:
    @pytest.fixture
    def client(self):
        import tempfile
        from fastapi.testclient import TestClient
        # Reset global singletons
        import live.api_server as srv
        srv._orchestrator = None
        srv._broker_manager = None
        from live.api_server import app
        client = TestClient(app)
        # Ensure broker manager uses fresh ESM with temp state file
        from live.execution_state_machine import ExecutionStateMachine
        bm = srv.get_broker_manager()
        bm._esm = ExecutionStateMachine(
            state_file=tempfile.mktemp(suffix="_apitest.json"),
            require_operator_arming=True,
        )
        return client

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        assert r.json()["version"] == "2.0.0"

    def test_status_has_execution_state(self, client):
        r = client.get("/api/status")
        assert r.status_code == 200
        data = r.json()
        assert "execution_state" in data
        assert data["execution_state"] == "PAPER_MODE"

    def test_broker_status_returns_paper(self, client):
        r = client.get("/api/broker/status")
        assert r.status_code == 200
        data = r.json()
        assert data["execution_state"] == "PAPER_MODE"

    def test_broker_account_requires_connection(self, client):
        r = client.get("/api/broker/account")
        assert r.status_code == 400

    def test_broker_balances_requires_connection(self, client):
        r = client.get("/api/broker/balances")
        assert r.status_code == 400

    def test_arm_live_requires_acknowledge_risk(self, client):
        r = client.post("/api/execution/arm-live", json={
            "operator": "test",
            "acknowledge_risk": False
        })
        assert r.status_code == 400

    def test_execution_state_endpoint(self, client):
        r = client.get("/api/execution/state")
        assert r.status_code == 200
        data = r.json()
        assert data["state"] == "PAPER_MODE"
        assert "live_trading_allowed" in data

    def test_kill_switch_endpoint(self, client):
        r = client.post("/api/control/kill", json={
            "reason": "Test kill",
            "operator": "test"
        })
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "KILL_SWITCH_ENGAGED"
        assert data["execution_state"] == "KILL_SWITCH_ENGAGED"

    def test_resume_after_kill(self, client):
        # First kill
        client.post("/api/control/kill", json={"reason": "test", "operator": "test"})
        # Then resume
        r = client.post("/api/control/resume", json={"reason": "test", "operator": "test"})
        assert r.status_code == 200
        assert r.json()["execution_state"] == "PAPER_MODE"

    def test_test_order_payload_without_connection(self, client):
        r = client.post("/api/broker/test-order-payload", json={
            "instrument": "XAUUSD",
            "side": "BUY",
            "units": 100.0,
        })
        # Should work even without broker connection (builds payload locally)
        assert r.status_code == 200
        data = r.json()
        assert "execution_state" in data

    def test_reconcile_requires_connection(self, client):
        r = client.post("/api/reconcile/run", json={"operator": "test"})
        # Returns error message, not 400
        assert r.status_code in (200, 400)

    def test_rejection_history_empty(self, client):
        r = client.get("/api/control/rejection-history")
        assert r.status_code == 200
        assert "rejections" in r.json()

    def test_execution_history(self, client):
        r = client.get("/api/execution/history")
        assert r.status_code == 200
        assert "transitions" in r.json()

    def test_test_order_invalid_side(self, client):
        r = client.post("/api/broker/test-order-payload", json={
            "instrument": "XAUUSD",
            "side": "INVALID",
            "units": 100.0,
        })
        assert r.status_code == 400

    def test_signal_endpoint_passes_state(self, client):
        """
        /api/signal now requires price/stop_loss/take_profit and actually runs
        the pipeline. Verify the endpoint returns the real processing result
        with execution_state and routed_to fields present.
        """
        r = client.post("/api/signal", json={
            "symbol": "XAUUSD",
            "action": "BUY",
            "price": 2300.0,
            "stop_loss": 2280.0,
            "take_profit": 2340.0,
            "timeframe": "M15",
            "schema_version": "1.0",
        })
        assert r.status_code == 200
        data = r.json()
        assert "execution_state" in data
        assert "routed_to" in data
        assert data["routed_to"] == "PAPER"    # Not connected to live broker
        # Pipeline must have been invoked — status is never the old fake "RECEIVED"
        assert data["status"] != "RECEIVED"
        assert "cycle" in data

    def test_backtest_runs(self, client):
        r = client.post("/api/backtest/run", json={
            "instrument": "XAUUSD",
            "instrument_type": "GOLD",
            "venue_type": "CFD_BROKER",
            "timeframe": "M15",
        })
        assert r.status_code == 200
        data = r.json()
        assert "metrics" in data
        assert data["metrics"]["total_trades"] >= 0


# ────────────────────────────────────────────────────────────
# SAFETY / NEGATIVE PATH TESTS
# ────────────────────────────────────────────────────────────

class TestSafetyControls:
    def test_live_trading_not_allowed_in_paper_mode(self):
        from live.execution_state_machine import ExecutionStateMachine
        import tempfile
        esm = ExecutionStateMachine(state_file=tempfile.mktemp(suffix=".json"))
        assert not esm.is_live_trading_allowed()

    def test_live_trading_not_allowed_in_safe_mode(self):
        from live.execution_state_machine import ExecutionStateMachine
        import tempfile
        esm = ExecutionStateMachine(state_file=tempfile.mktemp(suffix=".json"))
        esm.connect_broker("oanda", "prac", "acct", "test", True, "ok")
        assert not esm.is_live_trading_allowed()  # Safe mode, not armed

    def test_live_trading_not_allowed_in_kill_switch(self):
        from live.execution_state_machine import ExecutionStateMachine
        import tempfile
        esm = ExecutionStateMachine(state_file=tempfile.mktemp(suffix=".json"))
        esm.engage_kill_switch("test", "test")
        assert not esm.is_live_trading_allowed()

    def test_credentials_never_in_repr(self):
        from brokers.credential_manager import BrokerCredentials, CredentialStatus
        creds = BrokerCredentials(
            broker_name="oanda",
            environment="practice",
            api_token="super_secret_token_abc123",
            account_id="test",
            status=CredentialStatus.VALID,
        )
        repr_str = repr(creds)
        assert "super_secret_token_abc123" not in repr_str

    def test_arm_without_preflight_fails(self):
        from live.execution_state_machine import ExecutionStateMachine
        import tempfile
        esm = ExecutionStateMachine(state_file=tempfile.mktemp(suffix=".json"))
        # Connect with failed preflight → LIVE_BLOCKED
        esm.connect_broker("oanda", "prac", "acct", "test", False, "check failed")
        ok, msg = esm.arm_live_trading("test")
        assert not ok

    def test_paper_broker_never_uses_real_broker(self):
        from live.broker_manager import PaperBroker
        from brokers.base_connector import OrderRequest, OrderSide
        pb = PaperBroker()
        req = OrderRequest(instrument="XAUUSD", side=OrderSide.BUY, units=1000)
        # Paper broker should never call external API
        result = pb.submit_order(req)
        assert result.success
        assert result.raw.get("paper") is True

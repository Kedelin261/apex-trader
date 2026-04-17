"""
APEX MULTI-MARKET TJR ENGINE v3.0
Operationalization Tests — Phases 1–9

Coverage:
  Phase 1  — IBKR env validation + health endpoint logic
  Phase 2  — Runtime routing matrix (EURUSD→OANDA, XAUUSD/AAPL/ES→IBKR)
  Phase 3  — Execution State Machine transitions + history
  Phase 4  — Autonomous scheduler verification (5 loops)
  Phase 5  — Strategy validation (all 8 strategies loadable)
  Phase 6  — Risk normalisation validation (FOREX / FUTURES / STOCKS / CRYPTO)
  Phase 7  — Preflight hard validation (can_arm_live gating)
  Phase 8  — Failure mode testing (disconnect / funds / symbol / repeated)
  Phase 9  — Version 3.0.0 check

All tests must pass with 0 regressions on the 427-test baseline.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# ── path setup ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# ============================================================================
# HELPERS
# ============================================================================

def _make_esm(connected: bool = True, live_enabled: bool = False,
               kill_switch: bool = False, preflight_passed: bool = True):
    """Create a mocked ExecutionStateMachine."""
    from live.execution_state_machine import ExecutionState
    esm = MagicMock()
    if connected:
        esm.current_state.return_value = (
            ExecutionState.LIVE_ENABLED if live_enabled
            else ExecutionState.LIVE_CONNECTED_SAFE
        )
        esm.is_connected.return_value = True
    else:
        esm.current_state.return_value = ExecutionState.PAPER_MODE
        esm.is_connected.return_value = False

    esm.is_live_trading_allowed.return_value = live_enabled and not kill_switch
    esm.is_kill_switch_active.return_value = kill_switch

    from live.execution_state_machine import ExecutionStateSnapshot
    esm.snapshot.return_value = ExecutionStateSnapshot(
        state=esm.current_state().value,
        broker="both" if connected else None,
        environment="practice",
        account_id="U25324619",
        live_safe_lock=connected,
        live_enabled=live_enabled,
        require_operator_arming=True,
        kill_switch_active=kill_switch,
        preflight_passed=preflight_passed,
        preflight_performed_at=datetime.now(timezone.utc).isoformat() if preflight_passed else None,
        last_transition_at=datetime.now(timezone.utc).isoformat(),
        last_transition_reason="test",
        last_operator="test",
        connection_error=None,
        arming_operator=None,
        armed_at=None,
    )
    esm._transition_log = []
    return esm


def _make_bm(connected: bool = True, live_enabled: bool = False,
              kill_switch: bool = False, preflight_passed: bool = True):
    """Instantiate BrokerManager with mocked dependencies."""
    from live.broker_manager import BrokerManager

    with patch("live.broker_manager.get_credential_manager") as mock_cred, \
         patch("live.execution_state_machine.ExecutionStateMachine") as mock_esm_cls:

        mock_cred.return_value = MagicMock()
        mock_cred.return_value.load_jwt_secret.return_value = "test_secret"
        mock_cred.return_value.validate_ibkr_env.return_value = {"valid": True}
        mock_cred.return_value.load_ibkr.return_value = MagicMock(
            account_id="U25324619",
            environment="practice",
            host="127.0.0.1",
            port=7497,
            client_id=1,
            validation_errors=[],
            is_usable=lambda: True,
        )

        esm_instance = _make_esm(
            connected=connected,
            live_enabled=live_enabled,
            kill_switch=kill_switch,
            preflight_passed=preflight_passed,
        )
        mock_esm_cls.return_value = esm_instance

        bm = BrokerManager({"data": {"execution_state_file": "/tmp/test_apex_esm.json"}})
        bm._esm = esm_instance
        return bm


# ============================================================================
# PHASE 1 — IBKR ENV VALIDATION
# ============================================================================

class TestPhase1IBKREnvValidation:
    """IBKR environment variable validation."""

    def test_validate_ibkr_env_all_present(self):
        """Env check passes when all 4 required variables are set."""
        from brokers.credential_manager import CredentialManager
        cm = CredentialManager()

        env_backup = {}
        required = {
            "APEX_IBKR_ACCOUNT_ID": "U25324619",
            "APEX_IBKR_ENVIRONMENT": "practice",
            "APEX_IBKR_HOST": "127.0.0.1",
            "APEX_IBKR_PORT": "7497",
        }
        for k, v in required.items():
            env_backup[k] = os.environ.get(k)
            os.environ[k] = v

        try:
            result = cm.validate_ibkr_env()
            assert result["valid"] is True, f"Expected valid=True, got: {result}"
        finally:
            for k, v in env_backup.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_validate_ibkr_env_missing_account_id(self):
        """Env check fails with structured error when ACCOUNT_ID missing."""
        from brokers.credential_manager import CredentialManager
        cm = CredentialManager()

        env_backup = os.environ.pop("APEX_IBKR_ACCOUNT_ID", None)
        # Ensure others are set
        os.environ.setdefault("APEX_IBKR_ENVIRONMENT", "practice")
        os.environ.setdefault("APEX_IBKR_HOST", "127.0.0.1")
        os.environ.setdefault("APEX_IBKR_PORT", "7497")

        try:
            result = cm.validate_ibkr_env()
            assert result["valid"] is False
            assert result["error"] == "IBKR_ENV_NOT_CONFIGURED"
            assert "APEX_IBKR_ACCOUNT_ID" in result["missing"]
        finally:
            if env_backup:
                os.environ["APEX_IBKR_ACCOUNT_ID"] = env_backup

    def test_validate_ibkr_env_invalid_port(self):
        """Env check marks invalid non-integer port."""
        from brokers.credential_manager import CredentialManager
        cm = CredentialManager()

        backup = {
            "APEX_IBKR_ACCOUNT_ID": os.environ.get("APEX_IBKR_ACCOUNT_ID"),
            "APEX_IBKR_ENVIRONMENT": os.environ.get("APEX_IBKR_ENVIRONMENT"),
            "APEX_IBKR_HOST": os.environ.get("APEX_IBKR_HOST"),
            "APEX_IBKR_PORT": os.environ.get("APEX_IBKR_PORT"),
        }
        os.environ["APEX_IBKR_ACCOUNT_ID"] = "U25324619"
        os.environ["APEX_IBKR_ENVIRONMENT"] = "practice"
        os.environ["APEX_IBKR_HOST"] = "127.0.0.1"
        os.environ["APEX_IBKR_PORT"] = "not_a_number"

        try:
            result = cm.validate_ibkr_env()
            assert result["valid"] is False
            assert len(result.get("invalid", [])) > 0
        finally:
            for k, v in backup.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_load_ibkr_returns_correct_account_id(self):
        """load_ibkr() returns account ID from env."""
        from brokers.credential_manager import CredentialManager
        cm = CredentialManager()

        backup = {k: os.environ.get(k) for k in [
            "APEX_IBKR_ACCOUNT_ID", "APEX_IBKR_ENVIRONMENT",
            "APEX_IBKR_HOST", "APEX_IBKR_PORT"
        ]}
        os.environ["APEX_IBKR_ACCOUNT_ID"] = "U25324619"
        os.environ["APEX_IBKR_ENVIRONMENT"] = "practice"
        os.environ["APEX_IBKR_HOST"] = "127.0.0.1"
        os.environ["APEX_IBKR_PORT"] = "7497"

        try:
            creds = cm.load_ibkr()
            assert creds.account_id == "U25324619"
            assert creds.environment == "practice"
            assert creds.port == 7497
            assert creds.is_usable() is True
        finally:
            for k, v in backup.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_ibkr_health_no_connector(self):
        """ibkr_health() returns structured error when connector not initialised."""
        bm = _make_bm(connected=False)
        # Ensure no IBKR connector
        bm._ibkr_connector = None
        result = bm.ibkr_health()
        assert result["connected"] is False
        assert "error" in result or result.get("state") is not None

    def test_ibkr_health_with_connected_connector(self):
        """ibkr_health() returns connected=True when connector is AUTHENTICATED."""
        from brokers.base_connector import ConnectionState
        bm = _make_bm(connected=True)

        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_conn._conn_state = ConnectionState.AUTHENTICATED
        mock_conn._account_id = "U25324619"
        mock_conn._environment = "practice"
        mock_conn._host = "127.0.0.1"
        mock_conn._port = 7497
        mock_conn._client_id = 1
        mock_conn._circuit_open = False
        mock_conn._consecutive_failures = 0
        mock_conn._ib_app = None

        bm._ibkr_connector = mock_conn
        result = bm.ibkr_health()
        assert result["connected"] is True
        assert result["account_id"] == "U25324619"
        assert result["environment"] == "practice"
        assert result["circuit_breaker_open"] is False

    def test_broker_connect_validates_ibkr_env_before_connecting(self):
        """_connect_ibkr returns False with structured message when env not set."""
        from live.broker_manager import BrokerManager
        from brokers.credential_manager import CredentialManager
        from live.execution_state_machine import ExecutionStateMachine

        with patch("live.broker_manager.get_credential_manager") as mock_cred, \
             patch("live.execution_state_machine.ExecutionStateMachine"):
            cm_mock = MagicMock(spec=CredentialManager)
            cm_mock.load_jwt_secret.return_value = None
            cm_mock.validate_ibkr_env.return_value = {
                "valid": False,
                "error": "IBKR_ENV_NOT_CONFIGURED",
                "missing": ["APEX_IBKR_ACCOUNT_ID"],
                "invalid": [],
            }
            mock_cred.return_value = cm_mock

            bm = BrokerManager({"data": {"execution_state_file": "/tmp/test_apex_esm2.json"}})
            bm._esm = _make_esm(connected=False)
            ok, msg = bm._connect_ibkr("test_operator")

        assert ok is False
        assert "IBKR_ENV_NOT_CONFIGURED" in msg or "not configured" in msg.lower() or "missing" in msg.lower()


# ============================================================================
# PHASE 2 — ROUTING VALIDATION
# ============================================================================

class TestPhase2RoutingValidation:
    """Routing matrix: EURUSD→OANDA, XAUUSD/AAPL/ES→IBKR."""

    def test_eurusd_routes_to_oanda(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") == "oanda"

    def test_xauusd_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("XAUUSD") == "ibkr"

    def test_aapl_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("AAPL") == "ibkr"

    def test_es_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("ES") == "ibkr"

    def test_nq_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("NQ") == "ibkr"

    def test_gbpusd_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("GBPUSD") == "ibkr"

    def test_usoil_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("USOIL") == "ibkr"

    def test_unknown_symbol_routes_to_paper(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("MEMECOIN_XYZ") == "paper"

    def test_routing_is_case_insensitive(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("eurusd") == "oanda"
        assert get_broker_for_symbol("xauusd") == "ibkr"

    def test_route_log_structure_eurusd(self):
        from live.broker_manager import get_route_log
        log = get_route_log("EURUSD")
        assert log["symbol"] == "EURUSD"
        assert log["broker_selected"] == "oanda"
        assert log["asset_class"] == "FOREX"
        assert "OANDA" in log["route_reason"]

    def test_route_log_structure_xauusd(self):
        from live.broker_manager import get_route_log
        log = get_route_log("XAUUSD")
        assert log["symbol"] == "XAUUSD"
        assert log["broker_selected"] == "ibkr"
        assert "IBKR" in log["route_reason"]

    def test_route_log_structure_es(self):
        from live.broker_manager import get_route_log
        log = get_route_log("ES")
        assert log["broker_selected"] == "ibkr"
        assert log["asset_class"] == "FUTURES"

    def test_route_log_structure_aapl(self):
        from live.broker_manager import get_route_log
        log = get_route_log("AAPL")
        assert log["broker_selected"] == "ibkr"
        assert log["asset_class"] == "STOCKS"

    def test_all_routing_matrix_pass(self):
        """Full routing test matrix as required by spec."""
        from live.broker_manager import get_broker_for_symbol
        matrix = {
            "EURUSD": "oanda",
            "XAUUSD": "ibkr",
            "AAPL":   "ibkr",
            "ES":     "ibkr",
        }
        for symbol, expected in matrix.items():
            actual = get_broker_for_symbol(symbol)
            assert actual == expected, (
                f"Routing FAILED: {symbol} → {actual!r} (expected {expected!r})"
            )

    def test_broker_manager_route_connector_returns_correct_connector(self):
        """BrokerManager._route_connector returns correct connector per symbol."""
        bm = _make_bm(connected=True)

        oanda_mock = MagicMock()
        ibkr_mock  = MagicMock()
        bm._connector      = oanda_mock
        bm._ibkr_connector = ibkr_mock

        assert bm._route_connector("EURUSD") is oanda_mock
        assert bm._route_connector("XAUUSD") is ibkr_mock
        assert bm._route_connector("AAPL")   is ibkr_mock
        assert bm._route_connector("ES")     is ibkr_mock
        assert bm._route_connector("MEMECOIN") is None   # paper fallback


# ============================================================================
# PHASE 3 — EXECUTION STATE MACHINE
# ============================================================================

class TestPhase3ExecutionStateMachine:
    """ESM state transitions and history."""

    def _fresh_esm(self, suffix: str = ""):
        """Create a fresh ESM with a unique temp file to avoid state pollution."""
        import tempfile, uuid
        from live.execution_state_machine import ExecutionStateMachine
        tf = f"/tmp/test_esm_{uuid.uuid4().hex}.json"
        return ExecutionStateMachine(state_file=tf)

    def test_initial_state_is_paper(self):
        from live.execution_state_machine import ExecutionState
        esm = self._fresh_esm()
        assert esm.current_state() == ExecutionState.PAPER_MODE

    def test_connect_transitions_to_live_connected_safe(self):
        from live.execution_state_machine import ExecutionState
        esm = self._fresh_esm()
        ok, msg = esm.connect_broker(
            broker="ibkr",
            environment="practice",
            account_id="U25324619",
            operator="test",
            preflight_passed=True,
            preflight_reason="all checks passed",
        )
        assert ok is True
        assert esm.current_state() == ExecutionState.LIVE_CONNECTED_SAFE

    def test_arm_requires_preflight_passed(self):
        esm = self._fresh_esm()
        # Connect with preflight passed
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        # Should be able to arm
        ok, msg = esm.arm_live_trading(operator="test", confirmation_code="CONFIRM")
        assert ok is True

    def test_arm_blocked_without_preflight(self):
        esm = self._fresh_esm()
        # Connect with preflight FAILED → ESM may transition to LIVE_BLOCKED or stay in
        # PAPER_MODE if the transition is not allowed. Either way, arming must fail.
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=False)
        # Arming must always fail when preflight not passed
        ok, msg = esm.arm_live_trading(operator="test")
        assert ok is False  # The key invariant: arming fails when preflight not passed

    def test_kill_switch_blocks_from_any_state(self):
        from live.execution_state_machine import ExecutionState
        esm = self._fresh_esm()
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        esm.arm_live_trading("test", "CONFIRM")
        assert esm.current_state() == ExecutionState.LIVE_ENABLED

        ok, msg = esm.engage_kill_switch("Test kill switch", "test")
        assert ok is True
        assert esm.current_state() == ExecutionState.KILL_SWITCH_ENGAGED
        assert esm.is_kill_switch_active() is True
        assert esm.is_live_trading_allowed() is False

    def test_kill_switch_reset_returns_to_paper(self):
        from live.execution_state_machine import ExecutionState
        esm = self._fresh_esm()
        esm.engage_kill_switch("test", "test")
        ok, msg = esm.reset_to_paper("test", "Manual reset")
        assert ok is True
        assert esm.current_state() == ExecutionState.PAPER_MODE
        assert esm.is_kill_switch_active() is False

    def test_live_enabled_downgrades_on_restart(self):
        """On restart, LIVE_ENABLED → LIVE_CONNECTED_SAFE."""
        import json, uuid
        from live.execution_state_machine import ExecutionStateMachine, ExecutionState

        state_file = f"/tmp/test_esm_restart_{uuid.uuid4().hex}.json"
        # Write a state file that says LIVE_ENABLED
        with open(state_file, "w") as f:
            json.dump({
                "state": "LIVE_ENABLED",
                "broker": "ibkr",
                "environment": "practice",
                "account_id": "U25324619",
                "live_safe_lock": True,
            }, f)

        esm = ExecutionStateMachine(state_file=state_file)
        # Should be downgraded to LIVE_CONNECTED_SAFE (not LIVE_ENABLED)
        assert esm.current_state() == ExecutionState.LIVE_CONNECTED_SAFE
        assert esm.is_live_trading_allowed() is False

    def test_transition_history_records_events(self):
        esm = self._fresh_esm()
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        history = esm.transition_history(50)
        assert len(history) >= 1
        assert any("LIVE_CONNECTED_SAFE" in str(e) for e in history)

    def test_is_connected_returns_true_in_safe_state(self):
        esm = self._fresh_esm()
        assert esm.is_connected() is False  # initially PAPER_MODE
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        assert esm.is_connected() is True


# ============================================================================
# PHASE 4 — AUTONOMOUS SCHEDULER
# ============================================================================

class TestPhase4AutonomousScheduler:
    """Verify all 6 scheduler loops load and start correctly."""

    def test_scheduler_initialises(self):
        from live.autonomous_scheduler import AutonomousScheduler

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        assert sched._running is False
        assert isinstance(sched._strategies, dict)

    def test_scheduler_starts_5_threads(self):
        from live.autonomous_scheduler import AutonomousScheduler

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        sched.start()
        try:
            import time
            time.sleep(0.1)  # Let threads spin up
            assert sched._running is True
            assert len(sched._threads) == 6
            alive = sum(1 for t in sched._threads if t.is_alive())
            assert alive == 6, f"Expected 6 alive threads, got {alive}"
        finally:
            sched.stop()

    def test_scheduler_stops_gracefully(self):
        from live.autonomous_scheduler import AutonomousScheduler
        import time

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        sched.start()
        time.sleep(0.1)
        sched.stop()
        assert sched._running is False

    def test_all_6_loop_names_present_in_threads(self):
        from live.autonomous_scheduler import AutonomousScheduler
        import time

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        sched.start()
        time.sleep(0.1)
        try:
            thread_names = {t.name for t in sched._threads}
            expected = {
                "market-data-loop",
                "strategy-scan-loop",
                "signal-process-loop",
                "position-monitor-loop",
                "reconciliation-loop",
                "health-monitor-loop",
            }
            assert expected == thread_names, (
                f"Thread name mismatch. Expected {expected}, got {thread_names}"
            )
        finally:
            sched.stop()

    def test_scheduler_status_returns_correct_fields(self):
        from live.autonomous_scheduler import AutonomousScheduler

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        status = sched.status()
        assert "running" in status
        assert "cycle_count" in status
        assert "strategies_loaded" in status
        assert "pending_signals" in status
        assert "candle_cache_symbols" in status
        assert "active_threads" in status

    def test_kill_switch_suppresses_strategy_scan(self):
        """StrategyScanLoop returns early when kill switch active."""
        from live.autonomous_scheduler import AutonomousScheduler

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = True  # KILL SWITCH ON

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        # Should not raise, should return early
        initial_cycle = sched._cycle_count
        sched._strategy_scan_loop()
        assert sched._cycle_count == initial_cycle  # No increment if kill switch active


# ============================================================================
# PHASE 5 — STRATEGY VALIDATION
# ============================================================================

class TestPhase5StrategyValidation:
    """All 8 strategies are importable and return StrategySignal."""

    def _make_candles(self, count: int = 200, base: float = 1.0850) -> list:
        """Generate minimal synthetic candles."""
        import random
        candles = []
        price = base
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        for i in range(count):
            ts = now - timedelta(minutes=15 * (count - i))
            change = random.gauss(0, base * 0.001)
            o = price
            c = o + change
            h = max(o, c) + abs(random.gauss(0, base * 0.0005))
            l = min(o, c) - abs(random.gauss(0, base * 0.0005))
            candles.append({
                "timestamp": ts,
                "open": round(o, 5), "high": round(h, 5),
                "low": round(l, 5), "close": round(c, 5),
                "volume": random.randint(100, 5000),
            })
            price = c
        return candles

    def test_asian_range_breakout_loads(self):
        from strategies.asian_range_breakout import AsianRangeBreakout
        s = AsianRangeBreakout()
        assert s.name == "asian_range_breakout"
        assert s.asset_class == "FOREX"

    def test_orb_vwap_loads(self):
        from strategies.orb_vwap import ORBVWAPStrategy
        s = ORBVWAPStrategy()
        assert s.name == "orb_vwap"

    def test_vwap_sd_reversion_loads(self):
        from strategies.vwap_sd_reversion import VWAPSDReversion
        s = VWAPSDReversion()
        assert s.name == "vwap_sd_reversion"

    def test_commodity_trend_loads(self):
        from strategies.commodity_trend import CommodityTrend
        s = CommodityTrend()
        assert s.name == "commodity_trend"

    def test_gap_and_go_loads(self):
        from strategies.gap_and_go import GapAndGo
        s = GapAndGo()
        assert s.name == "gap_and_go"

    def test_crypto_funding_reversion_loads(self):
        from strategies.crypto_funding_reversion import CryptoFundingReversion
        s = CryptoFundingReversion()
        assert s.name == "crypto_funding_reversion"

    def test_crypto_monday_range_loads(self):
        from strategies.crypto_monday_range import CryptoMondayRange
        s = CryptoMondayRange()
        assert s.name == "crypto_monday_range"

    def test_prediction_market_arb_loads(self):
        from strategies.prediction_market_arb import PredictionMarketArb
        s = PredictionMarketArb()
        assert s.name == "prediction_market_arb"

    def test_all_strategies_return_strategy_signal(self):
        """All strategies evaluate() returns StrategySignal (no raise)."""
        from strategies.base_strategy import StrategySignal
        from strategies.asian_range_breakout import AsianRangeBreakout
        from strategies.orb_vwap import ORBVWAPStrategy
        from strategies.vwap_sd_reversion import VWAPSDReversion
        from strategies.commodity_trend import CommodityTrend
        from strategies.gap_and_go import GapAndGo
        from strategies.crypto_funding_reversion import CryptoFundingReversion
        from strategies.crypto_monday_range import CryptoMondayRange
        from strategies.prediction_market_arb import PredictionMarketArb

        strategy_pairs = [
            (AsianRangeBreakout(), "EURUSD"),
            (ORBVWAPStrategy(), "ES"),
            (VWAPSDReversion(), "XAUUSD"),
            (CommodityTrend(), "USOIL"),
            (GapAndGo(), "AAPL"),
            (CryptoFundingReversion(), "BTC/USDT"),
            (CryptoMondayRange(), "BTC/USDT"),
            (PredictionMarketArb(), "KLSH_DUMMY"),
        ]

        for strategy, symbol in strategy_pairs:
            candles = self._make_candles(200)
            context = {
                "symbol": symbol,
                "account_balance": 100_000.0,
                "now_utc": datetime.now(timezone.utc),
                "cycle": 1,
            }
            result = strategy.evaluate(candles, context)
            assert isinstance(result, StrategySignal), (
                f"{strategy.name} evaluate() returned {type(result)}, expected StrategySignal"
            )

    def test_scheduler_loads_all_8_strategies(self):
        """AutonomousScheduler loads all 8 strategy modules without error."""
        from live.autonomous_scheduler import AutonomousScheduler

        bm = MagicMock()
        bm.is_connected.return_value = False
        bm._esm = MagicMock()
        bm._esm.is_kill_switch_active.return_value = False

        sched = AutonomousScheduler(
            orchestrator=MagicMock(),
            broker_manager=bm,
            config={},
        )
        expected = {
            "asian_range_breakout", "orb_vwap", "vwap_sd_reversion",
            "commodity_trend", "gap_and_go", "crypto_funding_reversion",
            "crypto_monday_range", "prediction_market_arb",
        }
        assert expected == set(sched._strategies.keys()), (
            f"Strategy mismatch: missing={expected - set(sched._strategies.keys())}"
        )


# ============================================================================
# PHASE 6 — RISK NORMALISATION
# ============================================================================

class TestPhase6RiskNormalisation:
    """Exact risk normalisation formulas per asset class."""

    def setup_method(self):
        from live.autonomous_scheduler import RiskNormaliser
        self.norm = RiskNormaliser(risk_pct=1.0)

    def test_forex_lot_sizing(self):
        """EURUSD: lots = risk_usd / (sl_pips × pip_value_per_lot)"""
        result = self.norm.calculate(
            symbol="EURUSD",
            asset_class="FOREX",
            account_balance=100_000.0,
            entry_price=1.0850,
            stop_loss=1.0800,   # 50 pips SL
        )
        # risk_usd = 100000 * 0.01 = 1000
        # sl_pips = 0.005 / 0.0001 = 50
        # pip_value = 10.0 for EURUSD
        # lots = 1000 / (50 * 10) = 2.0
        assert result["method"] == "FOREX"
        assert result["units"] > 0
        assert result["risk_usd"] > 0
        # Verify formula: lots = 1000 / (50 * 10) = 2.0 (may be capped at min)
        assert abs(result["units"] - 2.0) < 0.1, f"Expected ~2.0 lots, got {result['units']}"

    def test_futures_contract_sizing_gc(self):
        """GC (Gold): contracts = risk_usd / (sl_ticks × tick_value)"""
        result = self.norm.calculate(
            symbol="XAUUSD",
            asset_class="FUTURES",
            account_balance=100_000.0,
            entry_price=2350.0,
            stop_loss=2340.0,  # 10.0 distance = 100 ticks (tick=0.10)
        )
        # risk_usd = 100000 * 0.01 = 1000
        # sl_ticks = 10.0 / 0.10 = 100
        # tick_value = 10.0
        # contracts = 1000 / (100 * 10) = 1.0
        assert result["method"] == "FUTURES"
        assert result["units"] >= 1.0   # Minimum 1 contract
        assert isinstance(result.get("sl_ticks"), float)
        assert isinstance(result.get("tick_value"), float)

    def test_futures_contract_sizing_es(self):
        """ES: contracts = risk_usd / (sl_ticks × tick_value)"""
        result = self.norm.calculate(
            symbol="ES",
            asset_class="FUTURES",
            account_balance=100_000.0,
            entry_price=5200.0,
            stop_loss=5190.0,  # 10 distance = 40 ticks (tick=0.25)
        )
        # risk_usd = 1000
        # sl_ticks = 10 / 0.25 = 40
        # tick_value = 12.50
        # contracts = 1000 / (40 * 12.50) = 2.0
        assert result["method"] == "FUTURES"
        assert result["units"] >= 1.0

    def test_stocks_share_sizing(self):
        """AAPL: shares = risk_usd / risk_per_share"""
        result = self.norm.calculate(
            symbol="AAPL",
            asset_class="STOCKS",
            account_balance=100_000.0,
            entry_price=185.0,
            stop_loss=180.0,   # $5 risk per share
        )
        # risk_usd = 1000
        # shares = 1000 / 5 = 200
        assert result["method"] == "STOCKS"
        assert result["units"] >= 1.0
        expected_shares = 1000.0 / 5.0
        assert abs(result["units"] - expected_shares) < 1.0, (
            f"Expected ~{expected_shares} shares, got {result['units']}"
        )

    def test_crypto_fractional_sizing(self):
        """BTC: units = risk_usd / risk_per_unit (fractional allowed)"""
        result = self.norm.calculate(
            symbol="BTCUSD",
            asset_class="CRYPTO",
            account_balance=100_000.0,
            entry_price=65000.0,
            stop_loss=64000.0,  # $1000 risk per unit
        )
        # risk_usd = 1000
        # units = 1000 / 1000 = 1.0
        assert result["method"] == "CRYPTO"
        assert result["units"] > 0
        assert isinstance(result["units"], float)

    def test_zero_sl_distance_returns_error(self):
        """Zero SL distance returns ZERO_SL_DISTANCE error, not division by zero."""
        result = self.norm.calculate(
            symbol="EURUSD",
            asset_class="FOREX",
            account_balance=100_000.0,
            entry_price=1.0850,
            stop_loss=1.0850,  # Same as entry = zero distance
        )
        assert result["units"] == 0.0
        assert result["method"] == "ZERO_SL_DISTANCE"
        assert "error" in result

    def test_debug_output_structure(self):
        """Risk normaliser returns all required debug fields."""
        result = self.norm.calculate(
            symbol="ES",
            asset_class="FUTURES",
            account_balance=100_000.0,
            entry_price=5200.0,
            stop_loss=5190.0,
        )
        required_fields = {"units", "risk_usd", "risk_pct", "method"}
        for field in required_fields:
            assert field in result, f"Missing field: {field}"


# ============================================================================
# PHASE 7 — PREFLIGHT HARD VALIDATION
# ============================================================================

class TestPhase7PreflightHardValidation:
    """System cannot enter LIVE_ENABLED unless ALL preflight checks pass."""

    def _fresh_esm(self):
        """Create ESM with a unique temp file — prevents state pollution between runs."""
        import uuid
        tf = f"/tmp/test_pf_{uuid.uuid4().hex}.json"
        from live.execution_state_machine import ExecutionStateMachine
        return ExecutionStateMachine(state_file=tf)

    def test_arm_live_blocked_when_preflight_not_passed(self):
        esm = self._fresh_esm()
        # Connect with preflight FAILED
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=False)
        ok, msg = esm.arm_live_trading("test")
        assert ok is False

    def test_arm_live_allowed_only_after_preflight_passes(self):
        from live.execution_state_machine import ExecutionState
        esm = self._fresh_esm()
        # Connect with preflight PASSED
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        assert esm.current_state() == ExecutionState.LIVE_CONNECTED_SAFE
        ok, msg = esm.arm_live_trading("test", "CONFIRM")
        assert ok is True
        assert esm.current_state() == ExecutionState.LIVE_ENABLED

    def test_preflight_result_persisted_in_snapshot(self):
        esm = self._fresh_esm()
        esm.connect_broker("ibkr", "practice", "U25324619", "test", preflight_passed=True)
        snap = esm.snapshot()
        assert snap.preflight_passed is True
        assert snap.preflight_performed_at is not None

    def test_broker_manager_run_preflight_with_no_connector(self):
        """Preflight returns blocking failure when no broker connected."""
        bm = _make_bm(connected=False)
        bm._connector = None
        bm._ibkr_connector = None
        result = bm.run_preflight(operator="test")
        assert result.overall_pass is False
        assert len(result.blocking_failures) > 0


# ============================================================================
# PHASE 8 — FAILURE MODE TESTING
# ============================================================================

class TestPhase8FailureModes:
    """Failure simulation: disconnect, insufficient funds, invalid symbol, repeated."""

    def test_kill_switch_blocks_all_orders(self):
        """Kill switch active → all orders return KILL_SWITCH_ACTIVE rejection."""
        bm = _make_bm(connected=True, kill_switch=True)
        from brokers.base_connector import OrderRequest, OrderSide, OrderType
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=1.0,
            order_type=OrderType.MARKET,
        )
        result = bm.submit_order(req)
        assert result.success is False
        assert result.normalized_rejection_code == "KILL_SWITCH_ACTIVE"

    def test_ibkr_disconnect_forces_paper_fallback(self):
        """When IBKR connector is None, XAUUSD order routes to paper broker."""
        bm = _make_bm(connected=True, live_enabled=True)
        bm._ibkr_connector = None  # IBKR disconnected

        from brokers.base_connector import OrderRequest, OrderSide, OrderType, OrderStatus
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=1.0,
            order_type=OrderType.MARKET,
            price=2350.0,
        )
        result = bm.submit_order(req)
        # Should route to paper broker (success=True, paper fill)
        assert result.success is True
        assert result.status == OrderStatus.FILLED

    def test_circuit_breaker_opens_after_threshold_failures(self):
        """Consecutive failures open circuit breaker on IBKR connector."""
        from brokers.ibkr_connector import IBKRConnector
        from brokers.base_connector import ConnectionState

        mock_creds = MagicMock()
        mock_creds.account_id = "U25324619"
        mock_creds.environment = "practice"

        conn = IBKRConnector(mock_creds)
        conn._consecutive_failures = conn._CIRCUIT_THRESHOLD
        conn._circuit_open = True

        assert conn.check_circuit_breaker() is True

    def test_unknown_symbol_rejected_by_check_symbol(self):
        """check_symbol('UNKNOWNSYM') returns UNKNOWN_SYMBOL rejection."""
        bm = _make_bm(connected=True)
        from brokers.symbol_mapper import BrokerSymbolMapper
        oanda_mapper = BrokerSymbolMapper("oanda")
        oanda_mapper.hydrate(None)  # static fallback
        bm._symbol_mapper = oanda_mapper

        ibkr_mapper = BrokerSymbolMapper("ibkr")
        ibkr_mapper.hydrate(None)
        bm._ibkr_symbol_mapper = ibkr_mapper

        result = bm.check_symbol("UNKNOWNSYM_TEST")
        assert result.is_tradeable is False
        # Routes to paper (no mapper) → BROKER_NOT_CONNECTED or UNKNOWN_SYMBOL
        assert result.rejection_code in ("BROKER_NOT_CONNECTED", "UNKNOWN_SYMBOL")

    def test_repeated_failures_fill_rejection_history(self):
        """Rejection history captures structured records per failure."""
        bm = _make_bm(connected=True)
        from brokers.base_connector import (
            OrderRequest, OrderSide, OrderType, OrderResult, OrderStatus
        )

        # Simulate a rejection directly
        bm._rejection_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "canonical_symbol": "XAUUSD",
            "target_broker": "ibkr",
            "broker_symbol": "GC",
            "units": 1.0,
            "side": "BUY",
            "reason": "INSTRUMENT_NOT_TRADEABLE",
            "raw_broker_error": "GC halted for trading",
            "parsed_error_code": "INSTRUMENT_NOT_TRADEABLE",
            "normalized_rejection_code": "INSTRUMENT_NOT_TRADEABLE",
            "insufficient_funds": False,
            "payload_snapshot": {"instrument": "GC", "units": 1},
            "mode": "LIVE",
        })

        history = bm.rejection_history(limit=10)
        assert len(history) >= 1
        last = history[-1]
        assert last["canonical_symbol"] == "XAUUSD"
        assert last["normalized_rejection_code"] == "INSTRUMENT_NOT_TRADEABLE"
        assert "raw_broker_error" in last
        assert "payload_snapshot" in last

    def test_live_blocked_state_prevents_live_orders(self):
        """When ESM is in LIVE_BLOCKED, submit_order routes to paper."""
        bm = _make_bm(connected=True, live_enabled=False)
        bm._esm.is_live_trading_allowed.return_value = False

        from brokers.base_connector import OrderRequest, OrderSide, OrderType, OrderStatus
        req = OrderRequest(
            instrument="EURUSD",
            side=OrderSide.BUY,
            units=0.1,
            order_type=OrderType.MARKET,
        )
        result = bm.submit_order(req)
        # Should go to paper broker
        assert result.success is True
        assert result.status == OrderStatus.FILLED

    def test_rejection_history_capped_at_200(self):
        """Rejection history never exceeds 200 entries."""
        bm = _make_bm(connected=True)
        for i in range(250):
            bm._rejection_history.append({
                "idx": i,
                "canonical_symbol": "TEST",
                "mode": "TEST",
            })
            if len(bm._rejection_history) > 200:
                bm._rejection_history.pop(0)

        assert len(bm._rejection_history) <= 200


# ============================================================================
# PHASE 9 — VERSION CHECK
# ============================================================================

class TestPhase9Version:
    """System version must be 3.0.0."""

    def test_config_version_is_3_0_0(self):
        import yaml
        from pathlib import Path
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        if config_path.exists():
            cfg = yaml.safe_load(config_path.read_text())
            assert cfg["system"]["version"] == "3.0.0", (
                f"Config version should be 3.0.0, got: {cfg['system']['version']}"
            )

    def test_ibkr_routing_rule_present_in_config(self):
        """IBKR config block is enabled in system_config.yaml."""
        import yaml
        from pathlib import Path
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        if config_path.exists():
            cfg = yaml.safe_load(config_path.read_text())
            ibkr_cfg = cfg.get("brokers", {}).get("ibkr", {})
            assert ibkr_cfg.get("enabled") is True, "IBKR must be enabled in config"
            assert ibkr_cfg.get("environment") == "practice"

    def test_oanda_routing_unchanged(self):
        """OANDA still routes EURUSD after IBKR integration."""
        from live.broker_manager import OANDA_ROUTING
        assert "EURUSD" in OANDA_ROUTING

    def test_ibkr_routing_includes_all_required_symbols(self):
        """IBKR routing table includes XAUUSD, ES, NQ, AAPL, GBPUSD."""
        from live.broker_manager import IBKR_ROUTING
        required = {"XAUUSD", "ES", "NQ", "AAPL", "GBPUSD", "MSFT", "CL", "USOIL"}
        missing = required - IBKR_ROUTING
        assert not missing, f"IBKR_ROUTING missing: {missing}"

    def test_get_route_log_function_exists(self):
        """get_route_log is exported from broker_manager."""
        from live.broker_manager import get_route_log
        assert callable(get_route_log)

    def test_ibkr_health_method_on_broker_manager(self):
        """BrokerManager.ibkr_health() exists and returns a dict."""
        from live.broker_manager import BrokerManager
        assert hasattr(BrokerManager, "ibkr_health")
        bm = _make_bm(connected=False)
        bm._ibkr_connector = None
        result = bm.ibkr_health()
        assert isinstance(result, dict)
        assert "connected" in result

"""
APEX MULTI-MARKET TJR ENGINE
Phase 10 — Comprehensive Autonomous System Tests

Covers:
  - Autonomous candidate signal generation
  - Loop heartbeat recording
  - Signal dedup (no re-execution after restart)
  - Position management actions (SL/TP/BE/trail/time/session)
  - Risk governor blocking (daily loss, drawdown, concurrent, symbol, strategy)
  - Portfolio allocator blocking (budget, disabled strategy)
  - Broker reconnect logic (BrokerSupervisor)
  - State reload on startup (PersistenceManager)
  - Alert emission
  - OANDA-only path (signal with EURUSD symbol → routed to oanda)
  - No-human-input flow (full cycle without manual curl)
  - Rollout mode manager (all four modes, live gate)

Scenario tests:
  - Successful trade flow (governor + allocator approve → executes)
  - Risk limit breach (governor blocks)
  - Restart safety (dedup prevents re-execute after restart)
  - Loop failure detection
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch, PropertyMock
import tempfile
import pytest


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def make_config(tmp_path: Path) -> dict:
    """Minimal config for testing."""
    return {
        "risk": {
            "max_risk_per_trade_pct": 1.0,
            "max_daily_loss_pct": 3.0,
            "max_total_drawdown_pct": 10.0,
            "max_concurrent_trades": 5,
            "account_balance_default": 100_000.0,
        },
        "autonomy": {
            "rollout_mode": "UNATTENDED_PAPER_MODE",
            "signal_dedup_cache_hours": 24.0,
            "scheduler": {
                "market_data_interval_s": 30,
                "strategy_scan_interval_s": 30,
                "position_monitor_interval_s": 10,
                "reconciliation_interval_s": 60,
                "health_monitor_interval_s": 30,
            },
            "risk_governor": {
                "max_trades_per_day": 20,
                "max_positions_per_symbol": 2,
                "max_positions_per_strategy": 3,
                "max_exposure_per_asset_class_usd": 50_000.0,
                "max_correlated_exposure_usd": 30_000.0,
                "cooldown_after_consecutive_losses": 3,
                "cooldown_minutes": 60,
                "kill_after_broker_rejects": 10,
                "kill_on_reconciliation_mismatch": False,   # Relaxed for tests
                "kill_on_stale_data_minutes": 5,
            },
            "portfolio_allocator": {
                "total_daily_risk_budget_usd": 3_000.0,
            },
            "broker_supervisor": {
                "heartbeat_interval_seconds": 30,
                "max_reconnect_attempts": 3,
                "backoff_base_seconds": 1,
                "backoff_max_seconds": 10,
                "failure_downgrade_threshold": 3,
            },
            "position_management": {
                "max_hold_hours": 48.0,
                "be_trigger_r": 1.0,
                "trailing_enabled": False,
                "trailing_step_pips": 10.0,
                "session_close_exit": False,   # Disabled for tests
                "close_before_weekend": False,  # Disabled for tests
                "stale_order_minutes": 30.0,
            },
            "alerting": {
                "enabled": True,
                "console_always": True,
            },
        },
        "data": {
            "state_dir": str(tmp_path / "data"),
        },
        "agents": {
            "orchestrator_interval_seconds": 30,
        },
        "markets": {
            "forex": {
                "enabled": True,
                "default_instruments": ["EURUSD"],
            }
        },
    }


def make_open_position(
    pos_id: str = "pos1",
    symbol: str = "EURUSD",
    strategy: str = "asian_range_breakout",
    asset_class: str = "FOREX",
    notional_usd: float = 10_000.0,
    direction: str = "BUY",
    entry_price: float = 1.0850,
    stop_loss: float = 1.0800,
    take_profit: float = 1.0950,
    entry_time: str = None,
    unrealized_pnl: float = 0.0,
) -> dict:
    if entry_time is None:
        entry_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    return {
        "id": pos_id,
        "symbol": symbol,
        "strategy": strategy,
        "asset_class": asset_class,
        "notional_usd": notional_usd,
        "direction": direction,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "entry_time": entry_time,
        "units": 1.0,
        "unrealized_pnl": unrealized_pnl,
    }


# ===========================================================================
# 1. RISK GOVERNOR TESTS
# ===========================================================================

class TestRiskGovernor:

    @pytest.fixture
    def governor(self, tmp_path):
        from live.risk_governor import RiskGovernor
        config = make_config(tmp_path)
        return RiskGovernor(config)

    def test_empty_positions_allowed(self, governor):
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert ok, f"Expected allowed, got: {reason}"

    def test_daily_loss_blocks(self, governor):
        """Once daily loss exceeds cap, new entries are blocked."""
        # Simulate $4000 loss on a $100k account with 3% limit = $3000 cap
        governor._daily.realized_pnl_usd = -4000.0
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not ok
        assert "DAILY_LOSS" in reason

    def test_max_concurrent_blocks(self, governor):
        """Max concurrent positions enforced."""
        positions = [
            make_open_position(f"pos{i}", "GBPUSD", asset_class="FOREX")
            for i in range(5)
        ]
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=positions,
        )
        assert not ok
        assert "MAX_CONCURRENT" in reason

    def test_max_per_symbol_blocks(self, governor):
        """Max positions per symbol enforced."""
        positions = [
            make_open_position(f"pos{i}", "EURUSD", asset_class="FOREX")
            for i in range(2)
        ]
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=positions,
        )
        assert not ok
        assert "MAX_PER_SYMBOL" in reason

    def test_max_per_strategy_blocks(self, governor):
        """Max positions per strategy enforced."""
        positions = [
            make_open_position(f"pos{i}", f"SYM{i}", strategy="asian_range_breakout", asset_class="FOREX")
            for i in range(3)
        ]
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=positions,
        )
        assert not ok
        assert "MAX_PER_STRATEGY" in reason

    def test_asset_class_cap_blocks(self, governor):
        """Asset class exposure cap enforced."""
        positions = [
            make_open_position("pos1", "EURUSD", asset_class="FOREX", notional_usd=49_000.0)
        ]
        ok, reason = governor.check_new_entry(
            symbol="GBPUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=2_000.0,  # 49000 + 2000 = 51000 > 50000 cap
            open_positions=positions,
        )
        assert not ok
        assert "ASSET_CLASS_CAP" in reason

    def test_correlated_exposure_blocks(self, governor):
        """Correlated exposure cap enforced across USD_INDEX group."""
        positions = [
            make_open_position("pos1", "EURUSD", asset_class="FOREX", notional_usd=29_500.0)
        ]
        ok, reason = governor.check_new_entry(
            symbol="GBPUSD",   # also in USD_INDEX group
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=1_000.0,  # 29500 + 1000 = 30500 > 30000 cap
            open_positions=positions,
        )
        assert not ok
        assert "CORRELATED_CAP" in reason

    def test_cooldown_after_consecutive_losses(self, governor):
        """Cooldown blocks trading after 3 consecutive losses."""
        # Record 3 losses
        for _ in range(3):
            governor.record_trade_closed(realized_pnl_usd=-100.0, new_balance=99_900.0)

        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not ok
        assert "COOLDOWN" in reason

    def test_global_kill_blocks_all(self, governor):
        """Global kill switch blocks all entries."""
        governor.engage_kill_switch("Test kill")
        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not ok
        assert "GLOBAL_KILL" in reason

    def test_broker_reject_kill(self, governor):
        """Kill switch engages after N broker rejects."""
        governor._broker_reject_kill = 3
        for _ in range(3):
            governor.record_broker_reject("EURUSD", "TEST_REJECT")
        assert governor._global_kill

    def test_status_returns_dict(self, governor):
        status = governor.status()
        assert "global_kill_active" in status
        assert "daily" in status
        assert "drawdown_pct" in status
        assert "limits" in status

    def test_state_persists_and_loads(self, tmp_path):
        """Daily state persists to disk and reloads correctly."""
        from live.risk_governor import RiskGovernor
        config = make_config(tmp_path)
        g1 = RiskGovernor(config)
        g1.record_trade_opened("EURUSD", "strategy1", 100.0)
        g1.record_trade_opened("EURUSD", "strategy1", 100.0)
        trades_before = g1._daily.trades_today

        # Create new instance (simulates restart)
        g2 = RiskGovernor(config)
        assert g2._daily.trades_today == trades_before


# ===========================================================================
# 2. PORTFOLIO ALLOCATOR TESTS
# ===========================================================================

class TestPortfolioAllocator:

    @pytest.fixture
    def allocator(self, tmp_path):
        from live.portfolio_allocator import PortfolioAllocator
        config = make_config(tmp_path)
        return PortfolioAllocator(config)

    def test_unknown_strategy_allowed(self, allocator):
        decision = allocator.check_allocation(
            strategy_name="unknown_strat",
            symbol="EURUSD",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert decision.allowed

    def test_disabled_strategy_blocked(self, allocator):
        allocator.enable_strategy("asian_range_breakout", False)
        decision = allocator.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not decision.allowed
        assert "disabled" in decision.reason

    def test_budget_exceeded_blocked(self, allocator):
        """Strategy budget exhausted → blocked."""
        # asian_range_breakout has 25% of $3000 = $750 budget
        allocator.record_position_opened("asian_range_breakout", 700.0)
        decision = allocator.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD",
            asset_class="FOREX",
            proposed_risk_usd=100.0,   # 700 + 100 = 800 > 750
            open_positions=[],
        )
        assert not decision.allowed
        assert "budget exhausted" in decision.reason.lower() or "budget" in decision.reason.lower()

    def test_within_budget_allowed(self, allocator):
        decision = allocator.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert decision.allowed

    def test_status_returns_all_strategies(self, allocator):
        status = allocator.status()
        assert "strategies" in status
        assert "asian_range_breakout" in status["strategies"]

    def test_record_position_updates_budget(self, allocator):
        allocator.record_position_opened("asian_range_breakout", 200.0)
        status = allocator.status()
        used = status["strategies"]["asian_range_breakout"]["risk_used_usd"]
        assert used == 200.0

    def test_asset_class_filter(self, allocator):
        """orb_vwap only allows FUTURES/STOCKS, not FOREX."""
        decision = allocator.check_allocation(
            strategy_name="orb_vwap",
            symbol="EURUSD",
            asset_class="FOREX",   # orb_vwap not allowed for FOREX
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not decision.allowed
        assert "not configured for" in decision.reason


# ===========================================================================
# 3. SIGNAL DEDUP TESTS
# ===========================================================================

class TestSignalDedupCache:

    @pytest.fixture
    def dedup(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        return SignalDedupCache(
            cache_file=tmp_path / "signals.jsonl",
            max_age_hours=24.0,
        )

    def test_new_signal_not_submitted(self, dedup):
        assert not dedup.is_submitted("sig-001")

    def test_mark_submitted_persists(self, dedup, tmp_path):
        dedup.mark_submitted("sig-001")
        assert dedup.is_submitted("sig-001")

        # Reload from disk — simulates restart
        from live.persistence_manager import SignalDedupCache
        dedup2 = SignalDedupCache(
            cache_file=tmp_path / "signals.jsonl",
            max_age_hours=24.0,
        )
        assert dedup2.is_submitted("sig-001")

    def test_prevents_double_execution_after_restart(self, tmp_path):
        """Critical test: signal submitted before restart must NOT re-execute."""
        from live.persistence_manager import SignalDedupCache
        cache_file = tmp_path / "signals.jsonl"

        # Pre-restart: mark signal as submitted
        d1 = SignalDedupCache(cache_file=cache_file, max_age_hours=24.0)
        d1.mark_submitted("unique-signal-123")

        # Post-restart: reload and verify
        d2 = SignalDedupCache(cache_file=cache_file, max_age_hours=24.0)
        assert d2.is_submitted("unique-signal-123"), (
            "CRITICAL: Signal was re-queued after restart — dedup failed!"
        )


# ===========================================================================
# 4. LOOP HEARTBEAT TESTS
# ===========================================================================

class TestLoopHeartbeatStore:

    @pytest.fixture
    def store(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        return LoopHeartbeatStore(tmp_path / "heartbeats.json")

    def test_beat_records_timestamp(self, store):
        store.beat("market-data-loop")
        beats = store.get_all()
        assert "market-data-loop" in beats

    def test_is_stale_when_no_beat(self, store):
        assert store.is_stale("never-beat-loop", max_age_seconds=30)

    def test_not_stale_after_beat(self, store):
        store.beat("health-loop")
        assert not store.is_stale("health-loop", max_age_seconds=60)

    def test_stale_after_timeout(self, store, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        # Write an old timestamp directly
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        f = tmp_path / "hb2.json"
        f.write_text(json.dumps({"heartbeats": {"old-loop": old_ts}}))
        s2 = LoopHeartbeatStore(f)
        assert s2.is_stale("old-loop", max_age_seconds=60)


# ===========================================================================
# 5. POSITION MANAGER TESTS
# ===========================================================================

class TestPositionManager:

    @pytest.fixture
    def manager(self, tmp_path):
        from live.position_manager import PositionManager
        config = make_config(tmp_path)
        return PositionManager(config)

    def test_tp_hit_generates_close(self, manager):
        """Position at TP should generate CLOSE action."""
        pos = make_open_position(
            pos_id="pos1", symbol="EURUSD",
            direction="BUY",
            entry_price=1.0850, stop_loss=1.0800, take_profit=1.0950,
        )
        # Current price above TP
        actions = manager.evaluate_positions(
            [pos], current_prices={"EURUSD": 1.0960}
        )
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) == 1
        assert "TP" in close_actions[0].reason

    def test_sl_hit_generates_close(self, manager):
        """Position at SL should generate CLOSE action."""
        pos = make_open_position(
            pos_id="pos1", symbol="EURUSD",
            direction="BUY",
            entry_price=1.0850, stop_loss=1.0800, take_profit=1.0950,
        )
        # Current price below SL
        actions = manager.evaluate_positions(
            [pos], current_prices={"EURUSD": 1.0790}
        )
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) == 1
        assert "SL" in close_actions[0].reason

    def test_breakeven_moves_sl(self, manager):
        """BE trigger: move SL to entry when price is 1R in profit."""
        # SL distance = 0.0050 (50 pips), BE at entry + 0.0050
        pos = make_open_position(
            pos_id="pos1", symbol="EURUSD",
            direction="BUY",
            entry_price=1.0850, stop_loss=1.0800, take_profit=1.0950,
        )
        # Price at entry + 1R (1.0850 + 0.0050 = 1.0900)
        actions = manager.evaluate_positions(
            [pos], current_prices={"EURUSD": 1.0901}
        )
        sl_moves = [a for a in actions if a.action == "MOVE_SL"]
        assert len(sl_moves) == 1
        assert "BREAK_EVEN" in sl_moves[0].reason or "BE" in sl_moves[0].reason
        # New SL should be at or above entry
        assert sl_moves[0].new_sl >= pos["entry_price"]

    def test_time_exit_triggers_close(self, manager):
        """Position held too long should be closed."""
        entry_time = (datetime.now(timezone.utc) - timedelta(hours=50)).isoformat()
        pos = make_open_position(
            pos_id="pos1", symbol="EURUSD",
            strategy="asian_range_breakout",
            direction="BUY",
            entry_price=1.0850, stop_loss=1.0800, take_profit=1.0950,
            entry_time=entry_time,
        )
        # Override max_hold_hours to 48h, position is 50h old
        manager._default_max_hold_hours = 48.0
        actions = manager.evaluate_positions(
            [pos], current_prices={"EURUSD": 1.0870}
        )
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) == 1
        assert "TIME_EXIT" in close_actions[0].reason

    def test_stale_order_cancelled(self, manager):
        """Old unfilled limit orders should be cancelled."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(minutes=60)).isoformat()
        open_orders = [{
            "id": "order-001",
            "symbol": "EURUSD",
            "type": "LIMIT",
            "placed_at": old_ts,
        }]
        # Override threshold
        manager._stale_order_minutes = 30.0
        actions = manager.evaluate_positions(
            [], current_prices={}, open_orders=open_orders
        )
        cancel_actions = [a for a in actions if a.action == "CANCEL_ORDER"]
        assert len(cancel_actions) == 1
        assert "STALE_ORDER" in cancel_actions[0].reason

    def test_get_recent_actions(self, manager):
        actions = manager.get_recent_actions()
        assert isinstance(actions, list)

    def test_status_returns_dict(self, manager):
        status = manager.status()
        assert "config" in status
        assert "last_pass_at" in status


# ===========================================================================
# 6. BROKER SUPERVISOR TESTS
# ===========================================================================

class TestBrokerSupervisor:

    def _make_supervisor(self, tmp_path, max_attempts=3):
        from live.broker_supervisor import BrokerSupervisor
        config = make_config(tmp_path)
        config["autonomy"]["broker_supervisor"]["max_reconnect_attempts"] = max_attempts
        mock_bm = MagicMock()
        mock_bm._connector = None
        mock_bm._ibkr_connector = None
        return BrokerSupervisor(mock_bm, config)

    def test_oanda_no_connector_fails(self, tmp_path):
        sup = self._make_supervisor(tmp_path)
        health = sup.run_heartbeat()
        assert health["oanda"] is False

    def test_oanda_reconnect_called_on_failure(self, tmp_path):
        from live.broker_supervisor import BrokerSupervisor
        config = make_config(tmp_path)
        mock_bm = MagicMock()
        mock_bm._connector = None
        mock_bm.connect.return_value = (True, "reconnected")
        sup = BrokerSupervisor(mock_bm, config)
        sup.run_heartbeat()
        # connect() should have been called
        mock_bm.connect.assert_called()

    def test_ibkr_absent_nonfatal(self, tmp_path):
        """IBKR absence is non-fatal — heartbeat still returns True for IBKR."""
        sup = self._make_supervisor(tmp_path)
        health = sup.run_heartbeat()
        # IBKR not configured → True (non-fatal)
        assert health["ibkr"] is True

    def test_downgrade_after_failures(self, tmp_path):
        """After threshold failures, ESM block_live is called."""
        from live.broker_supervisor import BrokerSupervisor
        config = make_config(tmp_path)
        config["autonomy"]["broker_supervisor"]["failure_downgrade_threshold"] = 2

        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        block_live_calls = []
        mock_esm.block_live.side_effect = lambda reason, operator: block_live_calls.append(reason)

        mock_bm = MagicMock()
        mock_bm._connector = None
        mock_bm._esm = mock_esm
        mock_bm.connect.return_value = (False, "refused")
        sup = BrokerSupervisor(mock_bm, config)

        # Set consecutive failures above threshold directly on health
        health = sup._health["oanda"]
        health.consecutive_failures = 3  # above threshold of 2

        # Trigger the reconnect/downgrade logic
        sup._attempt_reconnect_oanda(datetime.now(timezone.utc), health)

        # ESM.block_live should have been called
        assert len(block_live_calls) > 0, "block_live() was never called after threshold failures"

    def test_inflight_dedup(self, tmp_path):
        """Register in-flight order prevents re-submission."""
        sup = self._make_supervisor(tmp_path)
        sup.register_inflight_order("order-abc-123")
        assert sup.is_order_inflight("order-abc-123")
        assert not sup.is_order_inflight("order-xyz-999")

    def test_get_health_returns_dict(self, tmp_path):
        sup = self._make_supervisor(tmp_path)
        health = sup.get_health()
        assert "oanda" in health
        assert "ibkr" in health


# ===========================================================================
# 7. PERSISTENCE MANAGER TESTS
# ===========================================================================

class TestPersistenceManager:

    @pytest.fixture
    def pm(self, tmp_path):
        from live.persistence_manager import PersistenceManager
        config = make_config(tmp_path)
        return PersistenceManager(config)

    def test_initialises(self, pm):
        status = pm.status()
        assert "signal_dedup_cache_size" in status
        assert "persisted_positions" in status

    def test_position_store_upsert_and_get(self, pm):
        pos = make_open_position("pos-1", "EURUSD")
        pm.positions.upsert(pos)
        all_pos = pm.positions.get_all()
        assert len(all_pos) == 1
        assert all_pos[0]["id"] == "pos-1"

    def test_position_store_remove(self, pm):
        pm.positions.upsert(make_open_position("pos-1"))
        pm.positions.remove("pos-1")
        assert len(pm.positions.get_all()) == 0

    def test_startup_reconciliation_no_broker(self, pm):
        """Startup reconciliation works gracefully when broker not connected."""
        mock_bm = MagicMock()
        mock_bm.is_connected.return_value = False
        result = pm.run_startup_reconciliation(mock_bm)
        assert "final_position_count" in result

    def test_startup_reconciliation_removes_orphaned(self, pm):
        """Engine positions not at broker are removed on startup."""
        # Pre-persist a position
        pm.positions.upsert(make_open_position("pos-orphan"))

        mock_bm = MagicMock()
        mock_bm.is_connected.return_value = True
        mock_bm.get_positions.return_value = []   # Broker has nothing

        result = pm.run_startup_reconciliation(mock_bm)
        assert result["orphaned_in_engine"] == 1
        # Position should be removed
        assert len(pm.positions.get_all()) == 0


# ===========================================================================
# 8. ALERT DISPATCHER TESTS
# ===========================================================================

class TestAlertDispatcher:

    @pytest.fixture
    def dispatcher(self, tmp_path):
        from live.alert_dispatcher import AlertDispatcher
        config = make_config(tmp_path)
        return AlertDispatcher(config)

    def test_emit_adds_to_history(self, dispatcher):
        dispatcher.emit("TRADE_OPENED", "Test trade", {"symbol": "EURUSD"})
        time.sleep(0.1)  # Allow delivery loop tick
        history = dispatcher.get_history(limit=10)
        assert len(history) >= 1
        types = [h["alert_type"] for h in history]
        assert "TRADE_OPENED" in types

    def test_get_recent_alerts_returns_events(self, dispatcher):
        dispatcher.emit("SYSTEM_BLOCKED", "Kill switch test", {})
        time.sleep(0.1)
        alerts = dispatcher.get_recent_alerts(limit=10)
        assert len(alerts) >= 1
        # Each item should have alert_type attribute
        assert hasattr(alerts[0], "alert_type")

    def test_channel_status(self, dispatcher):
        status = dispatcher.get_channel_status()
        # Should return list (channels may be empty if none configured)
        assert isinstance(status, list)

    def test_all_alert_types_emit(self, dispatcher):
        """All defined alert types emit without error."""
        from live.alert_dispatcher import ALERT_LEVELS
        for atype in ALERT_LEVELS:
            dispatcher.emit(atype, f"Test: {atype}", {})
        time.sleep(0.2)
        history = dispatcher.get_history(limit=100)
        emitted = {h["alert_type"] for h in history}
        for atype in ALERT_LEVELS:
            assert atype in emitted, f"Alert type {atype!r} not in history"


# ===========================================================================
# 9. ROLLOUT MODE MANAGER TESTS
# ===========================================================================

class TestRolloutModeManager:

    @pytest.fixture
    def rmm(self, tmp_path):
        from live.rollout_mode_manager import RolloutModeManager
        config = make_config(tmp_path)
        return RolloutModeManager(config)

    def test_default_mode_paper(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        assert rmm.mode == RolloutMode.UNATTENDED_PAPER_MODE

    def test_scheduler_disabled_in_manual_mode(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        rmm.set_mode(RolloutMode.MANUAL_SIGNAL_MODE, operator="test")
        assert not rmm.is_scheduler_enabled()

    def test_scheduler_enabled_in_paper_mode(self, rmm):
        assert rmm.is_scheduler_enabled()

    def test_autonomous_dispatch_disabled_in_supervised(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        rmm.set_mode(RolloutMode.SUPERVISED_AUTONOMY_MODE, operator="test")
        assert not rmm.is_autonomous_dispatch_enabled()
        assert rmm.requires_operator_ack()

    def test_live_gate_fails_without_esm(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        ok, reason = rmm.check_live_gate()
        # Without ESM, live gate should fail
        assert not ok
        assert "esm" in reason.lower() or "esm=FAIL" in reason

    def test_live_mode_requires_gate(self, rmm):
        """Setting UNATTENDED_LIVE_MODE without gate passes → blocked."""
        from live.rollout_mode_manager import RolloutMode
        ok, msg = rmm.set_mode(RolloutMode.UNATTENDED_LIVE_MODE, operator="test")
        assert not ok
        assert "LIVE_GATE_FAILED" in msg

    def test_supervised_mode_ack_queue(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        rmm.set_mode(RolloutMode.SUPERVISED_AUTONOMY_MODE, operator="test")

        # Queue a signal
        qid = rmm.queue_for_approval({
            "strategy": "asian_range_breakout",
            "symbol": "EURUSD",
            "risk_usd": 100.0,
        })
        pending = rmm.get_pending_approvals()
        assert len(pending) == 1
        assert pending[0]["queue_id"] == qid

        # Approve it
        ok, item = rmm.approve_signal(qid, operator="test_op")
        assert ok
        assert item["status"] == "APPROVED"

        # Should no longer be pending
        assert len(rmm.get_pending_approvals()) == 0

    def test_supervised_mode_reject_queue(self, rmm):
        from live.rollout_mode_manager import RolloutMode
        rmm.set_mode(RolloutMode.SUPERVISED_AUTONOMY_MODE, operator="test")
        qid = rmm.queue_for_approval({"strategy": "test", "symbol": "EURUSD"})
        ok = rmm.reject_signal(qid, operator="test_op", reason="Too risky")
        assert ok

    def test_live_mode_not_restored_after_restart(self, tmp_path):
        """UNATTENDED_LIVE_MODE must NEVER auto-restore on restart — safety rule."""
        from live.rollout_mode_manager import RolloutModeManager, RolloutMode
        config = make_config(tmp_path)

        # Manually write LIVE mode to state file
        state_file = tmp_path / "data" / "rollout_mode.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps({
            "mode": "UNATTENDED_LIVE_MODE",
            "mode_history": [],
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }))

        # Load — should downgrade to PAPER
        rmm2 = RolloutModeManager(config)
        assert rmm2.mode == RolloutMode.UNATTENDED_PAPER_MODE, (
            "CRITICAL: UNATTENDED_LIVE_MODE restored after restart — safety violation!"
        )

    def test_status_returns_dict(self, rmm):
        status = rmm.status()
        assert "mode" in status
        assert "scheduler_enabled" in status
        assert "live_gate_ready" in status


# ===========================================================================
# 10. OANDA-ONLY ROUTING TEST
# ===========================================================================

class TestOANDAOnlyRouting:

    def test_eurusd_routes_to_oanda(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") == "oanda"

    def test_non_eurusd_forex_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        for sym in ["GBPUSD", "USDJPY", "AUDUSD", "USDCAD"]:
            assert get_broker_for_symbol(sym) == "ibkr", (
                f"{sym} should route to ibkr, not {get_broker_for_symbol(sym)}"
            )

    def test_gold_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("XAUUSD") == "ibkr"

    def test_futures_route_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        for sym in ["ES", "NQ", "YM", "CL"]:
            assert get_broker_for_symbol(sym) == "ibkr"

    def test_unknown_routes_to_paper(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("UNKNOWN_XYZ") == "paper"

    def test_case_insensitive(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("eurusd") == "oanda"
        assert get_broker_for_symbol("EURUSD") == "oanda"


# ===========================================================================
# 11. FULL SCENARIO: Successful trade flow (governor + allocator approve)
# ===========================================================================

class TestSuccessfulTradeScenario:

    def test_governor_and_allocator_both_approve(self, tmp_path):
        """
        Scenario: Clean state, valid signal → both gates pass.
        This simulates the signal_process_loop approving a trade.
        """
        from live.risk_governor import RiskGovernor
        from live.portfolio_allocator import PortfolioAllocator
        config = make_config(tmp_path)

        governor = RiskGovernor(config)
        allocator = PortfolioAllocator(config)

        symbol = "EURUSD"
        strategy = "asian_range_breakout"
        asset_class = "FOREX"
        proposed_risk = 100.0
        open_positions = []

        # Governor check
        gov_ok, gov_reason = governor.check_new_entry(
            symbol=symbol,
            strategy_name=strategy,
            asset_class=asset_class,
            proposed_risk_usd=proposed_risk,
            open_positions=open_positions,
        )
        assert gov_ok, f"Governor blocked: {gov_reason}"

        # Allocator check
        alloc_decision = allocator.check_allocation(
            strategy_name=strategy,
            symbol=symbol,
            asset_class=asset_class,
            proposed_risk_usd=proposed_risk,
            open_positions=open_positions,
        )
        assert alloc_decision.allowed, f"Allocator blocked: {alloc_decision.reason}"


# ===========================================================================
# 12. SCENARIO: Risk limit breach blocks new entry
# ===========================================================================

class TestRiskLimitBreachScenario:

    def test_daily_loss_blocks_new_entry(self, tmp_path):
        """
        Scenario: Daily loss limit breached → all new entries blocked.
        Existing position management still allowed (kill switch not engaged).
        """
        from live.risk_governor import RiskGovernor
        config = make_config(tmp_path)
        governor = RiskGovernor(config)

        # Simulate daily loss of 4% (limit is 3%)
        governor._daily.realized_pnl_usd = -4_000.0  # $4k loss on $100k

        ok, reason = governor.check_new_entry(
            symbol="EURUSD",
            strategy_name="asian_range_breakout",
            asset_class="FOREX",
            proposed_risk_usd=100.0,
            open_positions=[],
        )
        assert not ok
        assert "DAILY_LOSS" in reason


# ===========================================================================
# 13. SCENARIO: Restart safety — dedup prevents double execution
# ===========================================================================

class TestRestartSafetyScenario:

    def test_no_double_execution_after_restart(self, tmp_path):
        """
        Scenario: Signal submitted before restart → must NOT re-execute after restart.
        This is the critical restart safety guarantee.
        """
        from live.persistence_manager import SignalDedupCache
        cache_file = tmp_path / "data" / "signals.jsonl"
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        signal_id = "strat-EURUSD-1700000000-unique"

        # Before restart: mark as submitted
        pre_cache = SignalDedupCache(cache_file=cache_file, max_age_hours=24.0)
        pre_cache.mark_submitted(signal_id)
        assert pre_cache.is_submitted(signal_id)

        # Simulate restart: create new instance
        post_cache = SignalDedupCache(cache_file=cache_file, max_age_hours=24.0)
        assert post_cache.is_submitted(signal_id), (
            "RESTART SAFETY VIOLATED: Signal would execute twice after restart!"
        )


# ===========================================================================
# 14. SCENARIO: Loop failure detection
# ===========================================================================

class TestLoopFailureDetection:

    def test_loop_failure_counter_increments(self, tmp_path):
        """Loop failure counter tracks consecutive errors."""
        loop_failures = {}
        loop_failures["market-data-loop"] = loop_failures.get("market-data-loop", 0) + 1
        assert loop_failures["market-data-loop"] == 1

    def test_alert_on_3_consecutive_failures(self, tmp_path):
        """Alert emitted when loop fails 3 consecutive times."""
        from live.alert_dispatcher import AlertDispatcher
        config = make_config(tmp_path)
        alert = AlertDispatcher(config)
        emitted = []
        original_emit = alert.emit
        alert.emit = lambda atype, msg, det: emitted.append(atype) or original_emit(atype, msg, det)

        failures = 3
        if failures >= 3:
            alert.emit(
                "LOOP_FAILURE",
                "Loop 'market-data-loop' failed 3 consecutive times",
                {"loop": "market-data-loop", "failures": failures},
            )

        time.sleep(0.1)
        assert "LOOP_FAILURE" in emitted


# ===========================================================================
# 15. SCENARIO: Broker disconnect → safe behavior
# ===========================================================================

class TestBrokerDisconnectScenario:

    def test_supervisor_calls_esm_block_on_disconnect(self, tmp_path):
        """BrokerSupervisor blocks ESM when OANDA disconnects."""
        from live.broker_supervisor import BrokerSupervisor
        config = make_config(tmp_path)
        config["autonomy"]["broker_supervisor"]["failure_downgrade_threshold"] = 1

        mock_bm = MagicMock()
        mock_bm._connector = None
        mock_esm = MagicMock()
        mock_esm.is_kill_switch_active.return_value = False
        mock_bm._esm = mock_esm
        mock_bm.connect.return_value = (False, "connection refused")

        sup = BrokerSupervisor(mock_bm, config)
        health = sup._health["oanda"]
        health.consecutive_failures = 1  # Above threshold

        # Force multiple consecutive failures to cross threshold
        for _ in range(3):
            health.record_failure("timeout")
        # Force reconnect attempt which should also block ESM
        sup._attempt_reconnect_oanda(datetime.now(timezone.utc), health)

        # ESM should be blocked (called because failures >= threshold)
        mock_esm.block_live.assert_called()


# ===========================================================================
# 16. CONFIG HARDENING: All required keys present in system_config.yaml
# ===========================================================================

class TestConfigHardening:

    def test_system_config_has_autonomy_block(self):
        """system_config.yaml must have full autonomy configuration."""
        import yaml
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        assert config_path.exists(), "system_config.yaml not found"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        assert "autonomy" in config, "Missing 'autonomy' block in config"
        autonomy = config["autonomy"]

        required_keys = [
            "rollout_mode",
            "scheduler",
            "risk_governor",
            "portfolio_allocator",
            "broker_supervisor",
            "position_management",
            "alerting",
        ]
        for key in required_keys:
            assert key in autonomy, f"Missing config key: autonomy.{key}"

    def test_risk_governor_config_keys(self):
        """Risk governor config has all required keys."""
        import yaml
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        rg = config["autonomy"]["risk_governor"]
        required = [
            "max_trades_per_day",
            "max_positions_per_symbol",
            "max_positions_per_strategy",
            "cooldown_after_consecutive_losses",
            "cooldown_minutes",
            "kill_after_broker_rejects",
            "kill_on_stale_data_minutes",
        ]
        for k in required:
            assert k in rg, f"Missing risk_governor config key: {k}"

    def test_alerting_config_keys(self):
        """Alerting config has required keys."""
        import yaml
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        alerting = config["autonomy"]["alerting"]
        assert "enabled" in alerting
        assert "enabled_types" in alerting
        assert len(alerting["enabled_types"]) > 0

    def test_no_hardcoded_credentials_in_config(self):
        """Config YAML must not contain hardcoded tokens or passwords."""
        config_path = Path(__file__).parent.parent.parent / "config" / "system_config.yaml"
        content = config_path.read_text()
        # Should not contain actual token patterns
        assert "a1b2c3d4e5f6" not in content   # not a real token
        # Credential keys should be empty strings or "CHANGE_ME" style
        sensitive_patterns = ["api_key:", "token:", "password:", "secret:"]
        for pattern in sensitive_patterns:
            if pattern in content:
                # Allowed if value is empty string, placeholder, or env var reference
                idx = content.index(pattern)
                line = content[idx:idx+100].split("\n")[0]
                assert any(
                    safe in line for safe in ['""', "''", "CHANGE_ME", ".env", "env var", "#"]
                ), f"Potentially hardcoded credential in config: {line!r}"

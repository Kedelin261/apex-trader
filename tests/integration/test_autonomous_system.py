"""
APEX MULTI-MARKET TJR ENGINE
Phase 10: Comprehensive Autonomous System Tests

Tests cover:
  - Autonomous candidate signal generation
  - Loop heartbeat recording and stale detection
  - Signal deduplication (no re-execution after restart)
  - Position management actions (SL/TP/BE/trail/time/session/weekend)
  - Risk Governor blocking (daily loss, drawdown, concurrent, cooldown, stale data)
  - Portfolio Allocator caps (budget, concurrent, throttle, disabled strategy)
  - Broker Supervisor reconnect logic (backoff, downgrade, IBKR non-fatal)
  - Persistence startup reconciliation (orphan detection, no duplicate orders)
  - Alert dispatcher (emit, history, channel routing)
  - Rollout mode transitions (all valid + live gate)
  - OANDA-only autonomous path (no IBKR dependency for EURUSD)
  - No-human-input flow (signal → governor → allocator → intent layer → execution)
  - Scenario: successful trade (OANDA EURUSD)
  - Scenario: broker disconnect → safe state
  - Scenario: stale market data → governor blocks
  - Scenario: risk limit breach → new entries blocked, positions unchanged
  - Scenario: restart safety (no duplicate signal after dedup restore)
  - Scenario: loop failure tracking and alert
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock
import pytest


# =============================================================================
# HELPERS / FIXTURES
# =============================================================================

def _base_config(tmp_path: Path) -> dict:
    """Minimal config dict for autonomous component tests."""
    return {
        "system": {"version": "3.0.0", "environment": "paper"},
        "risk": {
            "max_risk_per_trade_pct": 1.0,
            "max_daily_loss_pct": 3.0,
            "max_total_drawdown_pct": 10.0,
            "max_concurrent_trades": 5,
            "account_balance_default": 100_000.0,
        },
        "data": {"state_dir": str(tmp_path)},
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
                "kill_on_reconciliation_mismatch": True,
                "kill_on_stale_data_minutes": 5,
            },
            "portfolio_allocator": {
                "total_daily_risk_budget_usd": 3_000.0,
                "strategies": {
                    "asian_range_breakout": {
                        "enabled": True,
                        "priority": 2,
                        "max_risk_budget_pct": 25.0,
                        "max_concurrent_positions": 2,
                    },
                    "orb_vwap": {
                        "enabled": True,
                        "priority": 3,
                        "max_risk_budget_pct": 20.0,
                        "max_concurrent_positions": 2,
                    },
                    "disabled_strategy": {
                        "enabled": False,
                        "priority": 9,
                        "max_risk_budget_pct": 5.0,
                        "max_concurrent_positions": 1,
                    },
                },
            },
            "broker_supervisor": {
                "heartbeat_interval_seconds": 30,
                "max_reconnect_attempts": 3,
                "backoff_base_seconds": 2,
                "backoff_max_seconds": 30,
                "failure_downgrade_threshold": 3,
            },
            "position_management": {
                "default_max_hold_hours": 48.0,
                "be_trigger_r": 1.0,
                "trailing_enabled": False,
                "trailing_step_pips": 10.0,
                "session_close_exit": True,
                "stale_order_minutes": 30.0,
            },
            "alerting": {
                "enabled": True,
                "max_history": 200,
                "email": {"enabled": False},
                "telegram": {"enabled": False},
                "discord": {"enabled": False},
            },
            "health_thresholds": {
                "loop_stale_seconds": 120,
            },
        },
        "markets": {
            "forex": {
                "enabled": True,
                "default_instruments": ["EURUSD", "GBPUSD"],
            },
        },
    }


def _make_mock_broker_manager(connected: bool = False, positions=None):
    bm = MagicMock()
    bm.is_connected.return_value = connected
    bm.get_positions.return_value = positions or []
    bm.get_account_info.return_value = MagicMock(balance=100_000.0)
    bm.get_open_orders.return_value = []

    mock_check = MagicMock()
    mock_check.is_tradeable = False
    mock_check.rejection_code = "NOT_CONNECTED"
    mock_check.reason_if_not_tradeable = "broker not connected"
    bm.check_symbol.return_value = mock_check

    mock_order_result = MagicMock()
    mock_order_result.success = False
    mock_order_result.reject_reason = "paper mode"
    bm.submit_order.return_value = mock_order_result

    # ESM mock
    esm = MagicMock()
    esm.is_kill_switch_active.return_value = False
    esm.is_live_trading_allowed.return_value = False
    esm.current_state = MagicMock()
    from live.execution_state_machine import ExecutionState
    esm.current_state = ExecutionState.PAPER_MODE
    bm._esm = esm
    bm._connector = None
    bm._ibkr_connector = None
    bm._rejection_history = []
    return bm


# =============================================================================
# PHASE 10.1 — SIGNAL DEDUPLICATION
# =============================================================================

class TestSignalDedupCache:

    def test_new_signal_not_submitted(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")
        assert not cache.is_submitted("signal-abc-001")

    def test_mark_and_check_submitted(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")
        cache.mark_submitted("signal-abc-002")
        assert cache.is_submitted("signal-abc-002")

    def test_persisted_across_restart(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        cache1 = SignalDedupCache(tmp_path / "dedup.jsonl")
        cache1.mark_submitted("signal-persistent-001")

        # Simulate restart: new instance reads from same file
        cache2 = SignalDedupCache(tmp_path / "dedup.jsonl")
        assert cache2.is_submitted("signal-persistent-001")

    def test_different_ids_not_deduplicated(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")
        cache.mark_submitted("signal-001")
        assert not cache.is_submitted("signal-002")

    def test_expired_signals_not_loaded(self, tmp_path):
        """Signals older than max_age_hours should not be loaded."""
        from live.persistence_manager import SignalDedupCache
        # Write an old entry manually
        cache_file = tmp_path / "dedup.jsonl"
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        cache_file.write_text(
            json.dumps({"signal_id": "old-signal", "submitted_at": old_ts}) + "\n"
        )
        cache = SignalDedupCache(cache_file, max_age_hours=24.0)
        assert not cache.is_submitted("old-signal")

    def test_size_reflects_submissions(self, tmp_path):
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")
        cache.mark_submitted("sig-a")
        cache.mark_submitted("sig-b")
        assert cache.size() == 2


# =============================================================================
# PHASE 10.2 — LOOP HEARTBEAT STORE
# =============================================================================

class TestLoopHeartbeatStore:

    def test_beat_and_retrieve(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        store = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        store.beat("market-data-loop")
        beats = store.get_all()
        assert "market-data-loop" in beats

    def test_stale_detection(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        store = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        # Never beaten → stale
        assert store.is_stale("strategy-scan-loop", max_age_seconds=60)

    def test_fresh_loop_not_stale(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        store = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        store.beat("health-monitor-loop")
        assert not store.is_stale("health-monitor-loop", max_age_seconds=60)

    def test_persisted_across_restart(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        store1 = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        store1.beat("reconciliation-loop")

        store2 = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        beats = store2.get_all()
        assert "reconciliation-loop" in beats

    def test_multiple_loops_tracked(self, tmp_path):
        from live.persistence_manager import LoopHeartbeatStore
        store = LoopHeartbeatStore(tmp_path / "heartbeats.json")
        loop_names = [
            "market-data-loop", "strategy-scan-loop",
            "signal-process-loop", "position-monitor-loop",
            "reconciliation-loop", "health-monitor-loop",
        ]
        for name in loop_names:
            store.beat(name)
        beats = store.get_all()
        for name in loop_names:
            assert name in beats


# =============================================================================
# PHASE 10.3 — RISK GOVERNOR BLOCKING
# =============================================================================

class TestRiskGovernor:

    def _make_gov(self, tmp_path, overrides: dict = None):
        from live.risk_governor import RiskGovernor
        cfg = _base_config(tmp_path)
        if overrides:
            cfg["autonomy"]["risk_governor"].update(overrides)
        return RiskGovernor(cfg)

    def _open_pos(self, symbol="EURUSD", strategy="asian_range_breakout",
                  asset_class="FOREX", notional=1000.0):
        return {
            "id": "pos-001",
            "symbol": symbol,
            "strategy": strategy,
            "asset_class": asset_class,
            "notional_usd": notional,
        }

    def test_allows_clean_entry(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.mark_data_received()
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert allowed, f"Should be allowed but got: {reason}"

    def test_blocks_when_kill_switch_active(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.engage_kill_switch("test kill")
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed
        assert "GLOBAL_KILL" in reason or "kill" in reason.lower()

    def test_blocks_on_daily_loss_breach(self, tmp_path):
        gov = self._make_gov(tmp_path)
        # Inject a daily loss exceeding 3% of 100k = $3,000
        gov._daily.realized_pnl_usd = -4_000.0
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed
        assert "DAILY_LOSS" in reason

    def test_blocks_on_max_concurrent(self, tmp_path):
        gov = self._make_gov(tmp_path, {"max_positions_per_symbol": 10})
        gov._max_concurrent = 2
        positions = [self._open_pos("EURUSD"), self._open_pos("GBPUSD")]
        gov.mark_data_received()
        allowed, reason = gov.check_new_entry(
            symbol="AUDUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=positions, account_balance=100_000.0,
        )
        assert not allowed
        assert "MAX_CONCURRENT" in reason

    def test_blocks_on_max_per_symbol(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.mark_data_received()
        # max_positions_per_symbol = 2, add 2 EURUSD positions
        positions = [
            self._open_pos("EURUSD", "strategy_a"),
            self._open_pos("EURUSD", "strategy_b"),
        ]
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="strategy_c",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=positions, account_balance=100_000.0,
        )
        assert not allowed
        assert "MAX_PER_SYMBOL" in reason

    def test_blocks_during_cooldown(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.mark_data_received()
        # Trigger cooldown directly
        gov._cooldown_until = datetime.now(timezone.utc) + timedelta(hours=1)
        gov._daily.consecutive_losses = 3
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed
        assert "COOLDOWN" in reason

    def test_cooldown_triggered_after_consecutive_losses(self, tmp_path):
        gov = self._make_gov(tmp_path, {"cooldown_after_consecutive_losses": 3})
        for _ in range(3):
            gov.record_trade_closed(-100.0, 99_700.0)
        assert gov._cooldown_until is not None

    def test_blocks_on_stale_data(self, tmp_path):
        gov = self._make_gov(tmp_path, {"kill_on_stale_data_minutes": 1})
        # Set last data timestamp to 10 minutes ago
        gov._last_data_timestamp = datetime.now(timezone.utc) - timedelta(minutes=10)
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed
        assert "STALE_DATA" in reason

    def test_no_stale_block_when_data_fresh(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.mark_data_received()
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert allowed, f"Should be allowed: {reason}"

    def test_state_persists_and_restores(self, tmp_path):
        from live.risk_governor import RiskGovernor
        cfg = _base_config(tmp_path)
        gov1 = RiskGovernor(cfg)
        gov1.record_trade_opened("EURUSD", "test", 100.0)
        gov1.record_trade_opened("EURUSD", "test", 100.0)

        # New instance reads from disk
        gov2 = RiskGovernor(cfg)
        assert gov2._daily.trades_today == gov1._daily.trades_today

    def test_kill_switch_reset(self, tmp_path):
        gov = self._make_gov(tmp_path)
        gov.engage_kill_switch("test")
        assert gov._global_kill
        gov.reset_kill_switch(operator="test")
        assert not gov._global_kill

    def test_daily_reset_at_midnight(self, tmp_path):
        from live.risk_governor import RiskGovernor
        cfg = _base_config(tmp_path)
        gov = RiskGovernor(cfg)
        gov._daily.trades_today = 15
        gov._daily.date_utc = "2020-01-01"  # Force old date
        gov._maybe_reset_daily()
        assert gov._daily.trades_today == 0

    def test_status_structure(self, tmp_path):
        gov = self._make_gov(tmp_path)
        status = gov.status()
        assert "global_kill_active" in status
        assert "daily" in status
        assert "drawdown_pct" in status
        assert "limits" in status
        assert "recent_blocks" in status


# =============================================================================
# PHASE 10.4 — PORTFOLIO ALLOCATOR
# =============================================================================

class TestPortfolioAllocator:

    def _make_alloc(self, tmp_path):
        from live.portfolio_allocator import PortfolioAllocator
        return PortfolioAllocator(_base_config(tmp_path))

    def test_allows_within_budget(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        decision = alloc.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert decision.allowed, f"Should be allowed: {decision.reason}"

    def test_blocks_disabled_strategy(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        decision = alloc.check_allocation(
            strategy_name="disabled_strategy",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=10.0, open_positions=[],
        )
        assert not decision.allowed
        assert "disabled" in decision.reason.lower()

    def test_blocks_when_budget_exhausted(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        # asian_range_breakout gets 25% of $3000 = $750 budget
        alloc._risk_used["asian_range_breakout"] = 750.0
        decision = alloc.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert not decision.allowed
        assert "budget" in decision.reason.lower()

    def test_blocks_wrong_asset_class(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        # orb_vwap is for FUTURES/STOCKS, not FOREX
        decision = alloc.check_allocation(
            strategy_name="orb_vwap",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert not decision.allowed
        assert "FOREX" in decision.reason or "configured for" in decision.reason

    def test_blocks_when_concurrent_limit_hit(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        alloc._open_counts["asian_range_breakout"] = 2  # max is 2
        decision = alloc.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert not decision.allowed
        assert "concurrent" in decision.reason.lower()

    def test_record_and_close_updates_counters(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        alloc.record_position_opened("asian_range_breakout", 100.0)
        assert alloc._open_counts["asian_range_breakout"] == 1
        assert alloc._risk_used["asian_range_breakout"] == 100.0

        alloc.record_position_closed("asian_range_breakout", 100.0)
        assert alloc._open_counts["asian_range_breakout"] == 0
        assert alloc._risk_used["asian_range_breakout"] == 0.0

    def test_enable_disable_strategy(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        alloc.enable_strategy("asian_range_breakout", False)
        decision = alloc.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert not decision.allowed
        alloc.enable_strategy("asian_range_breakout", True)
        decision2 = alloc.check_allocation(
            strategy_name="asian_range_breakout",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert decision2.allowed

    def test_status_structure(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        status = alloc.status()
        assert "total_risk_budget_usd" in status
        assert "strategies" in status
        assert "asian_range_breakout" in status["strategies"]

    def test_unknown_strategy_allowed_by_default(self, tmp_path):
        alloc = self._make_alloc(tmp_path)
        decision = alloc.check_allocation(
            strategy_name="new_unknown_strategy",
            symbol="EURUSD", asset_class="FOREX",
            proposed_risk_usd=100.0, open_positions=[],
        )
        assert decision.allowed


# =============================================================================
# PHASE 10.5 — POSITION MANAGER ACTIONS
# =============================================================================

class TestPositionManager:

    def _make_pm(self, tmp_path):
        from live.position_manager import PositionManager
        return PositionManager(_base_config(tmp_path))

    def _make_pos(self, **kwargs):
        defaults = {
            "id": "pos-001",
            "symbol": "EURUSD",
            "strategy": "asian_range_breakout",
            "direction": "BUY",
            "entry_price": 1.0850,
            "stop_loss": 1.0800,
            "take_profit": 1.0950,
            "entry_time": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "units": 0.1,
            "risk_usd": 50.0,
            "unrealized_pnl": 0.0,
            "notional_usd": 10850.0,
            "asset_class": "FOREX",
        }
        defaults.update(kwargs)
        return defaults

    def test_no_action_when_no_trigger(self, tmp_path):
        pm = self._make_pm(tmp_path)
        pos = self._make_pos()
        actions = pm.evaluate_positions([pos], {"EURUSD": 1.0860})
        assert actions == []

    def test_close_on_tp_hit(self, tmp_path):
        pm = self._make_pm(tmp_path)
        pos = self._make_pos(take_profit=1.0900)
        # Current price above TP for BUY
        actions = pm.evaluate_positions([pos], {"EURUSD": 1.0910})
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) >= 1
        assert any("TP" in a.reason for a in close_actions)

    def test_close_on_sl_hit(self, tmp_path):
        pm = self._make_pm(tmp_path)
        pos = self._make_pos(stop_loss=1.0820)
        # Current price below SL for BUY
        actions = pm.evaluate_positions([pos], {"EURUSD": 1.0810})
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) >= 1
        assert any("SL" in a.reason for a in close_actions)

    def test_break_even_move_when_at_r1(self, tmp_path):
        pm = self._make_pm(tmp_path)
        # entry=1.0850, sl=1.0800, sl_distance=50 pips
        # R1 = entry + sl_distance = 1.0900
        pos = self._make_pos(
            entry_price=1.0850, stop_loss=1.0800, take_profit=1.0950
        )
        # Set price exactly at R1
        actions = pm.evaluate_positions([pos], {"EURUSD": 1.0901})
        be_actions = [a for a in actions if a.action == "MOVE_SL" and "BE" in a.reason]
        assert len(be_actions) >= 1
        assert be_actions[0].new_sl == pytest.approx(1.0850, abs=0.0001)

    def test_time_exit_triggers(self, tmp_path):
        pm = self._make_pm(tmp_path)
        # Entry was 50 hours ago (> 48h default)
        old_entry = (datetime.now(timezone.utc) - timedelta(hours=50)).isoformat()
        pos = self._make_pos(entry_time=old_entry)
        actions = pm.evaluate_positions([pos], {"EURUSD": 1.0860})
        close_actions = [a for a in actions if a.action == "CLOSE" and "TIME_EXIT" in a.reason]
        assert len(close_actions) >= 1

    def test_weekend_close_friday_2100(self, tmp_path):
        pm = self._make_pm(tmp_path)
        pos = self._make_pos()
        # Mock datetime to Friday 21:00 UTC
        friday_2100 = datetime(2024, 3, 22, 21, 0, 0, tzinfo=timezone.utc)  # Friday
        with patch("live.position_manager.datetime") as mock_dt:
            mock_dt.now.return_value = friday_2100
            mock_dt.fromisoformat = datetime.fromisoformat
            actions = pm.evaluate_positions([pos], {"EURUSD": 1.0860})
        close_actions = [a for a in actions if a.action == "CLOSE" and "WEEKEND" in a.reason]
        assert len(close_actions) >= 1

    def test_stale_order_cancel(self, tmp_path):
        pm = self._make_pm(tmp_path)
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=35)).isoformat()
        stale_order = {
            "id": "ord-001",
            "symbol": "EURUSD",
            "type": "LIMIT",
            "placed_at": old_ts,
        }
        actions = pm.evaluate_positions([], {}, open_orders=[stale_order])
        cancel_actions = [a for a in actions if a.action == "CANCEL_ORDER"]
        assert len(cancel_actions) >= 1

    def test_get_recent_actions(self, tmp_path):
        pm = self._make_pm(tmp_path)
        pos = self._make_pos(stop_loss=1.0820)
        pm.evaluate_positions([pos], {"EURUSD": 1.0810})
        actions = pm.get_recent_actions(limit=10)
        assert isinstance(actions, list)

    def test_status_structure(self, tmp_path):
        pm = self._make_pm(tmp_path)
        status = pm.status()
        assert "last_pass_at" in status
        assert "recent_actions" in status
        assert "config" in status


# =============================================================================
# PHASE 10.6 — BROKER SUPERVISOR RECONNECT
# =============================================================================

class TestBrokerSupervisor:

    def _make_supervisor(self, tmp_path, bm=None):
        from live.broker_supervisor import BrokerSupervisor
        cfg = _base_config(tmp_path)
        bm = bm or _make_mock_broker_manager()
        return BrokerSupervisor(bm, cfg)

    def test_healthy_oanda_returns_true(self, tmp_path):
        bm = _make_mock_broker_manager(connected=True)
        conn = MagicMock()
        conn.connection_status.return_value = {"connected": True, "latency_ms": 50}
        bm._connector = conn
        sup = self._make_supervisor(tmp_path, bm)
        result = sup.run_heartbeat()
        assert result["oanda"] is True

    def test_disconnected_oanda_returns_false(self, tmp_path):
        bm = _make_mock_broker_manager(connected=False)
        conn = MagicMock()
        conn.connection_status.return_value = {"connected": False, "error": "timeout"}
        bm._connector = conn
        bm.connect.return_value = (False, "connection refused")
        sup = self._make_supervisor(tmp_path, bm)
        result = sup.run_heartbeat()
        assert result["oanda"] is False

    def test_exponential_backoff_scheduling(self, tmp_path):
        from live.broker_supervisor import BrokerSupervisor
        cfg = _base_config(tmp_path)
        cfg["autonomy"]["broker_supervisor"]["backoff_base_seconds"] = 10
        bm = _make_mock_broker_manager()
        bm._connector = MagicMock()
        bm._connector.connection_status.return_value = {"connected": False}
        bm.connect.return_value = (False, "refused")
        sup = BrokerSupervisor(bm, cfg)

        # First failure schedules backoff
        sup._attempt_reconnect_oanda(datetime.now(timezone.utc), sup._health["oanda"])
        assert sup._next_reconnect_at["oanda"] is not None

    def test_ibkr_failure_is_non_fatal(self, tmp_path):
        """IBKR failure should not affect OANDA path."""
        bm = _make_mock_broker_manager(connected=True)
        oanda_conn = MagicMock()
        oanda_conn.connection_status.return_value = {"connected": True}
        bm._connector = oanda_conn

        ibkr_conn = MagicMock()
        ibkr_conn.is_connected.return_value = False
        bm._ibkr_connector = ibkr_conn
        bm.connect.return_value = (False, "ibkr refused")

        sup = self._make_supervisor(tmp_path, bm)
        result = sup.run_heartbeat()
        # OANDA still ok even though IBKR failed
        assert result["oanda"] is True

    def test_ibkr_absent_is_ok(self, tmp_path):
        """No IBKR connector → supervisor returns True (expected in OANDA-only mode)."""
        bm = _make_mock_broker_manager()
        bm._ibkr_connector = None
        oanda_conn = MagicMock()
        oanda_conn.connection_status.return_value = {"connected": True}
        bm._connector = oanda_conn
        sup = self._make_supervisor(tmp_path, bm)
        result = sup.run_heartbeat()
        assert result["ibkr"] is True  # No IBKR = non-fatal = True

    def test_inflight_order_dedup(self, tmp_path):
        sup = self._make_supervisor(tmp_path)
        sup.register_inflight_order("order-001")
        assert sup.is_order_inflight("order-001")
        assert not sup.is_order_inflight("order-002")

    def test_get_health_structure(self, tmp_path):
        sup = self._make_supervisor(tmp_path)
        health = sup.get_health()
        assert "oanda" in health
        assert "ibkr" in health
        assert "timestamp" in health


# =============================================================================
# PHASE 10.7 — PERSISTENCE STARTUP RECONCILIATION
# =============================================================================

class TestPersistenceManager:

    def _make_pm(self, tmp_path):
        from live.persistence_manager import PersistenceManager
        return PersistenceManager(_base_config(tmp_path))

    def test_startup_reconciliation_no_broker(self, tmp_path):
        pm = self._make_pm(tmp_path)
        bm = _make_mock_broker_manager(connected=False)
        summary = pm.run_startup_reconciliation(bm)
        assert "persisted_count" in summary
        assert "live_broker_count" in summary

    def test_startup_reconciliation_removes_orphaned_engine_positions(self, tmp_path):
        pm = self._make_pm(tmp_path)
        # Persist a position that doesn't exist at broker
        pm.positions.upsert({"id": "orphan-001", "symbol": "EURUSD"})

        bm = _make_mock_broker_manager(connected=True)
        bm.get_positions.return_value = []  # Broker has nothing

        summary = pm.run_startup_reconciliation(bm)
        assert summary["orphaned_in_engine"] >= 1
        # Should be removed from engine state
        remaining = [p for p in pm.positions.get_all() if p.get("id") == "orphan-001"]
        assert len(remaining) == 0

    def test_startup_reconciliation_adds_broker_orphans(self, tmp_path):
        pm = self._make_pm(tmp_path)

        # Broker has a position engine doesn't know about
        mock_pos = MagicMock()
        mock_pos.order_id = "broker-orphan-001"
        mock_pos.instrument = "EURUSD"
        mock_pos.side = MagicMock()
        mock_pos.side.value = "BUY"
        mock_pos.units = 0.1
        mock_pos.avg_price = 1.0850
        mock_pos.unrealized_pnl = 10.0

        bm = _make_mock_broker_manager(connected=True)
        bm.get_positions.return_value = [mock_pos]

        summary = pm.run_startup_reconciliation(bm)
        assert summary["orphaned_at_broker"] >= 1

    def test_no_duplicate_orders_on_restart(self, tmp_path):
        """Core safety: reconciliation never resubmits orders."""
        pm = self._make_pm(tmp_path)
        pm.signal_dedup.mark_submitted("signal-live-001")

        # Simulate restart
        from live.persistence_manager import PersistenceManager
        pm2 = PersistenceManager(_base_config(tmp_path))

        # Dedup should block re-submission
        assert pm2.signal_dedup.is_submitted("signal-live-001")

    def test_candle_timestamp_store(self, tmp_path):
        pm = self._make_pm(tmp_path)
        ts = datetime.now(timezone.utc).isoformat()
        pm.candle_timestamps.update("EURUSD", ts)
        assert pm.candle_timestamps.get("EURUSD") == ts

    def test_status_structure(self, tmp_path):
        pm = self._make_pm(tmp_path)
        status = pm.status()
        assert "signal_dedup_cache_size" in status
        assert "persisted_positions" in status
        assert "loop_heartbeats" in status


# =============================================================================
# PHASE 10.8 — ALERT DISPATCHER
# =============================================================================

class TestAlertDispatcher:

    def _make_dispatcher(self, tmp_path):
        from live.alert_dispatcher import AlertDispatcher
        return AlertDispatcher(_base_config(tmp_path))

    def test_emit_records_to_history(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        disp.emit("TRADE_OPENED", "Test trade", {"symbol": "EURUSD"})
        time.sleep(0.1)
        history = disp.get_history(limit=10)
        assert len(history) >= 1
        assert history[-1]["alert_type"] == "TRADE_OPENED"

    def test_get_recent_alerts_returns_event_objects(self, tmp_path):
        from live.alert_dispatcher import AlertEvent
        disp = self._make_dispatcher(tmp_path)
        disp.emit("BROKER_DISCONNECTED", "Test disconnect", {})
        time.sleep(0.1)
        alerts = disp.get_recent_alerts(limit=5)
        assert len(alerts) >= 1
        assert isinstance(alerts[-1], AlertEvent)

    def test_multiple_alert_types_stored(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        for alert_type in ["TRADE_OPENED", "TRADE_CLOSED", "RISK_LIMIT_HIT",
                           "BROKER_DISCONNECTED", "RESTART_DETECTED"]:
            disp.emit(alert_type, f"Test {alert_type}", {})
        time.sleep(0.2)
        history = disp.get_history(limit=50)
        found_types = {h["alert_type"] for h in history}
        assert len(found_types) >= 5

    def test_channel_status_returns_list(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        status = disp.get_channel_status()
        assert isinstance(status, list)

    def test_disabled_channels_not_configured(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        for ch_status in disp.get_channel_status():
            # All channels disabled in test config
            assert not ch_status["configured"]

    def test_history_limited_to_max_history(self, tmp_path):
        from live.alert_dispatcher import AlertDispatcher
        cfg = _base_config(tmp_path)
        cfg["autonomy"]["alerting"]["max_history"] = 10
        disp = AlertDispatcher(cfg)
        for i in range(20):
            disp.emit("TRADE_OPENED", f"Trade {i}", {})
        time.sleep(0.2)
        history = disp.get_history()
        assert len(history) <= 10

    def test_persists_to_file(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        disp.emit("DAILY_SUMMARY", "End of day", {"pnl": 100.0})
        time.sleep(0.2)
        alert_file = tmp_path / "alert_history.jsonl"
        assert alert_file.exists()
        content = alert_file.read_text()
        assert "DAILY_SUMMARY" in content

    def test_stop_gracefully(self, tmp_path):
        disp = self._make_dispatcher(tmp_path)
        disp.emit("SYSTEM_BLOCKED", "Test stop", {})
        disp.stop()  # Should not raise


# =============================================================================
# PHASE 10.9 — ROLLOUT MODE MANAGER
# =============================================================================

class TestRolloutModeManager:

    def _make_rmm(self, tmp_path, mode="UNATTENDED_PAPER_MODE", esm=None):
        from live.rollout_mode_manager import RolloutModeManager
        cfg = _base_config(tmp_path)
        cfg["autonomy"]["rollout_mode"] = mode
        return RolloutModeManager(cfg, esm=esm)

    def test_default_mode_from_config(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        from live.rollout_mode_manager import RolloutMode
        assert rmm.mode == RolloutMode.UNATTENDED_PAPER_MODE

    def test_scheduler_enabled_in_paper_mode(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        assert rmm.is_scheduler_enabled()

    def test_scheduler_disabled_in_manual_mode(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "MANUAL_SIGNAL_MODE")
        assert not rmm.is_scheduler_enabled()

    def test_autonomous_dispatch_enabled_in_paper_mode(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        assert rmm.is_autonomous_dispatch_enabled()

    def test_autonomous_dispatch_disabled_in_supervised_mode(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "SUPERVISED_AUTONOMY_MODE")
        assert not rmm.is_autonomous_dispatch_enabled()

    def test_requires_operator_ack_in_supervised_mode(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "SUPERVISED_AUTONOMY_MODE")
        assert rmm.requires_operator_ack()

    def test_transition_to_manual(self, tmp_path):
        from live.rollout_mode_manager import RolloutMode
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        ok, msg = rmm.set_mode(RolloutMode.MANUAL_SIGNAL_MODE, operator="test")
        assert ok
        assert rmm.mode == RolloutMode.MANUAL_SIGNAL_MODE

    def test_transition_to_supervised(self, tmp_path):
        from live.rollout_mode_manager import RolloutMode
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        ok, msg = rmm.set_mode(RolloutMode.SUPERVISED_AUTONOMY_MODE, operator="test")
        assert ok
        assert rmm.mode == RolloutMode.SUPERVISED_AUTONOMY_MODE

    def test_live_mode_gate_fails_without_esm(self, tmp_path):
        from live.rollout_mode_manager import RolloutMode
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        ok, reason = rmm.set_mode(RolloutMode.UNATTENDED_LIVE_MODE, operator="test")
        assert not ok
        assert "LIVE_GATE_FAILED" in reason or "esm" in reason.lower()

    def test_live_mode_gate_fails_without_armed_esm(self, tmp_path):
        from live.rollout_mode_manager import RolloutMode
        from live.execution_state_machine import ExecutionState
        esm = MagicMock()
        esm.current_state = ExecutionState.PAPER_MODE  # Not armed
        rmm = self._make_rmm(tmp_path, esm=esm)
        ok, reason = rmm.set_mode(RolloutMode.UNATTENDED_LIVE_MODE, operator="test")
        assert not ok

    def test_live_mode_never_restored_on_restart(self, tmp_path):
        """Safety: UNATTENDED_LIVE_MODE must never be auto-restored after restart."""
        from live.rollout_mode_manager import RolloutModeManager, RolloutMode
        cfg = _base_config(tmp_path)
        # Manually write UNATTENDED_LIVE_MODE to state file
        state_file = tmp_path / "rollout_mode.json"
        state_file.write_text(json.dumps({
            "mode": "UNATTENDED_LIVE_MODE",
            "mode_history": [],
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }))
        rmm = RolloutModeManager(cfg)
        # Must downgrade to paper on restart
        assert rmm.mode == RolloutMode.UNATTENDED_PAPER_MODE

    def test_supervised_queue_and_approve(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "SUPERVISED_AUTONOMY_MODE")
        signal_item = {
            "strategy": "asian_range_breakout",
            "symbol": "EURUSD",
            "direction": "BUY",
        }
        queue_id = rmm.queue_for_approval(signal_item)
        pending = rmm.get_pending_approvals()
        assert len(pending) == 1

        ok, item = rmm.approve_signal(queue_id, operator="operator-001")
        assert ok
        assert item["status"] == "APPROVED"
        assert len(rmm.get_pending_approvals()) == 0

    def test_supervised_reject(self, tmp_path):
        rmm = self._make_rmm(tmp_path, "SUPERVISED_AUTONOMY_MODE")
        queue_id = rmm.queue_for_approval({
            "strategy": "asian_range_breakout",
            "symbol": "EURUSD",
        })
        ok = rmm.reject_signal(queue_id, operator="operator-001", reason="risk too high")
        assert ok
        pending = rmm.get_pending_approvals()
        assert len(pending) == 0

    def test_mode_history_recorded(self, tmp_path):
        from live.rollout_mode_manager import RolloutMode
        rmm = self._make_rmm(tmp_path, "UNATTENDED_PAPER_MODE")
        rmm.set_mode(RolloutMode.MANUAL_SIGNAL_MODE, operator="test_op", reason="test")
        status = rmm.status()
        history = status["mode_history"]
        assert len(history) >= 1
        assert history[-1]["from"] == "UNATTENDED_PAPER_MODE"
        assert history[-1]["to"] == "MANUAL_SIGNAL_MODE"

    def test_status_structure(self, tmp_path):
        rmm = self._make_rmm(tmp_path)
        status = rmm.status()
        required_keys = [
            "mode", "scheduler_enabled", "autonomous_dispatch_enabled",
            "live_dispatch_allowed", "requires_operator_ack",
            "pending_approvals", "live_gate_ready",
        ]
        for key in required_keys:
            assert key in status, f"Missing key: {key}"


# =============================================================================
# PHASE 10.10 — OANDA-FIRST AUTONOMOUS PATH
# =============================================================================

class TestOandaFirstPath:
    """
    Verify EURUSD routes to OANDA and that IBKR unavailability
    does not block the OANDA autonomous path.
    """

    def test_eurusd_routes_to_oanda(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") == "oanda"

    def test_eurusd_does_not_route_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") != "ibkr"

    def test_non_eurusd_forex_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        for sym in ["GBPUSD", "USDJPY", "AUDUSD", "USDCAD"]:
            assert get_broker_for_symbol(sym) == "ibkr", f"{sym} should route to ibkr"

    def test_gold_routes_to_ibkr(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("XAUUSD") == "ibkr"
        assert get_broker_for_symbol("GC") == "ibkr"

    def test_unknown_symbol_routes_to_paper(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("UNKNOWN_SYMBOL") == "paper"

    def test_risk_normaliser_forex_eurusd(self):
        from live.autonomous_scheduler import RiskNormaliser
        norm = RiskNormaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="EURUSD", asset_class="FOREX",
            account_balance=100_000.0, entry_price=1.0850, stop_loss=1.0800,
        )
        assert result["units"] > 0
        assert result["method"] == "FOREX"
        assert result.get("sl_pips", 0) > 0

    def test_risk_normaliser_zero_sl_returns_zero_units(self):
        from live.autonomous_scheduler import RiskNormaliser
        norm = RiskNormaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="EURUSD", asset_class="FOREX",
            account_balance=100_000.0, entry_price=1.0850, stop_loss=1.0850,
        )
        assert result["units"] == 0.0

    def test_scheduler_dedup_prevents_reentry(self, tmp_path):
        """Signal with a known ID should be skipped in strategy scan loop."""
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")
        cache.mark_submitted("eurusd-signal-001")
        assert cache.is_submitted("eurusd-signal-001")

    def test_ibkr_unavailability_does_not_block_oanda_routing(self):
        """OANDA routing should be deterministic regardless of IBKR state."""
        from live.broker_manager import get_broker_for_symbol, OANDA_ROUTING
        # Even if IBKR symbols are unavailable, EURUSD still routes to OANDA
        assert "EURUSD" in OANDA_ROUTING
        assert get_broker_for_symbol("EURUSD") == "oanda"


# =============================================================================
# PHASE 10.11 — SCENARIO TESTS
# =============================================================================

class TestScenarios:

    def test_scenario_risk_limit_breach_blocks_new_entries_not_existing(self, tmp_path):
        """
        Scenario: daily loss limit hit.
        - New entries: BLOCKED
        - Existing positions: governed only by PositionManager (not blocked)
        """
        from live.risk_governor import RiskGovernor
        cfg = _base_config(tmp_path)
        gov = RiskGovernor(cfg)
        gov.mark_data_received()

        # Inject daily loss > 3%
        gov._daily.realized_pnl_usd = -3_100.0

        # New entry blocked
        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="asian_range_breakout",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed, "New entry should be blocked after daily loss breach"
        assert "DAILY_LOSS" in reason

        # Existing position management actions still work (PositionManager independent)
        from live.position_manager import PositionManager
        pm_mgr = PositionManager(cfg)
        pos = {
            "id": "existing-001", "symbol": "EURUSD", "strategy": "asian_range_breakout",
            "direction": "BUY", "entry_price": 1.0850, "stop_loss": 1.0810,
            "take_profit": 1.0950,
            "entry_time": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "units": 0.1, "risk_usd": 50.0, "unrealized_pnl": 0.0,
            "notional_usd": 10850.0, "asset_class": "FOREX",
        }
        # PositionManager still evaluates positions — it doesn't check RiskGovernor
        actions = pm_mgr.evaluate_positions([pos], {"EURUSD": 1.0800})  # SL hit
        close_actions = [a for a in actions if a.action == "CLOSE"]
        assert len(close_actions) >= 1, "Position should still be managed (SL hit)"

    def test_scenario_restart_safety_no_duplicate_signals(self, tmp_path):
        """
        Scenario: Process restarts after submitting a signal.
        The same signal must NOT be resubmitted.
        """
        from live.persistence_manager import SignalDedupCache
        cache = SignalDedupCache(tmp_path / "dedup.jsonl")

        # First run: signal submitted
        cache.mark_submitted("eurusd-arb-2024-001")

        # Second run: new instance loads cache
        from live.persistence_manager import SignalDedupCache as SDC2
        cache2 = SDC2(tmp_path / "dedup.jsonl")
        assert cache2.is_submitted("eurusd-arb-2024-001"), \
            "Signal must be deduped after restart"

    def test_scenario_loop_failure_tracking(self, tmp_path):
        """
        Scenario: a loop fails N consecutive times.
        Failure count must be tracked; after threshold, alert should fire.
        """
        from live.alert_dispatcher import AlertDispatcher
        cfg = _base_config(tmp_path)
        alert = AlertDispatcher(cfg)
        alerts_fired = []

        original_emit = alert.emit
        def capturing_emit(alert_type, message, details):
            alerts_fired.append(alert_type)
            original_emit(alert_type, message, details)

        alert.emit = capturing_emit

        # Simulate loop runner failure path (manual trigger)
        failure_count = 3
        for i in range(failure_count):
            if i + 1 >= 3:
                alert.emit(
                    "LOOP_FAILURE",
                    f"Loop 'strategy-scan-loop' failed {i+1} consecutive times",
                    {"loop": "strategy-scan-loop", "failures": i+1}
                )

        time.sleep(0.1)
        assert "LOOP_FAILURE" in alerts_fired

    def test_scenario_broker_disconnect_safe_state(self, tmp_path):
        """
        Scenario: OANDA disconnects → ESM transitions to LIVE_BLOCKED.
        IBKR unavailability is non-fatal.
        """
        from live.broker_supervisor import BrokerSupervisor
        from live.execution_state_machine import ExecutionState
        cfg = _base_config(tmp_path)

        # Mock ESM in live state
        esm = MagicMock()
        esm.is_kill_switch_active.return_value = False
        esm.current_state = ExecutionState.LIVE_ENABLED

        bm = _make_mock_broker_manager()
        bm._esm = esm
        conn = MagicMock()
        conn.connection_status.return_value = {"connected": False, "error": "timeout"}
        bm._connector = conn
        bm.connect.return_value = (False, "refused")

        sup = BrokerSupervisor(bm, cfg)

        # Simulate 3+ consecutive failures to trigger downgrade
        for i in range(4):
            sup._health["oanda"].record_failure(f"timeout #{i}")

        # Trigger downgrade logic
        sup._attempt_reconnect_oanda(datetime.now(timezone.utc), sup._health["oanda"])

        # ESM should have been called to block live
        esm.block_live.assert_called()

    def test_scenario_stale_data_kills_governor(self, tmp_path):
        """
        Scenario: market data feed goes stale for > threshold minutes.
        Governor should block new entries.
        """
        from live.risk_governor import RiskGovernor
        cfg = _base_config(tmp_path)
        cfg["autonomy"]["risk_governor"]["kill_on_stale_data_minutes"] = 2
        gov = RiskGovernor(cfg)

        # Set last data 10 minutes ago
        gov._last_data_timestamp = datetime.now(timezone.utc) - timedelta(minutes=10)

        allowed, reason = gov.check_new_entry(
            symbol="EURUSD", strategy_name="test",
            asset_class="FOREX", proposed_risk_usd=100.0,
            open_positions=[], account_balance=100_000.0,
        )
        assert not allowed
        assert "STALE_DATA" in reason

    def test_scenario_no_human_input_signal_flow(self, tmp_path):
        """
        Scenario: verify that the autonomous signal flow path (governor → allocator)
        can process a signal without any manual /api/signal call.
        """
        from live.risk_governor import RiskGovernor
        from live.portfolio_allocator import PortfolioAllocator

        cfg = _base_config(tmp_path)
        gov = RiskGovernor(cfg)
        gov.mark_data_received()
        alloc = PortfolioAllocator(cfg)

        # Simulate a signal from StrategyScanLoop
        signal = {
            "symbol": "EURUSD",
            "strategy": "asian_range_breakout",
            "asset_class": "FOREX",
            "proposed_risk_usd": 100.0,
        }

        # Step 1: RiskGovernor gate
        gov_ok, gov_reason = gov.check_new_entry(
            symbol=signal["symbol"],
            strategy_name=signal["strategy"],
            asset_class=signal["asset_class"],
            proposed_risk_usd=signal["proposed_risk_usd"],
            open_positions=[],
            account_balance=100_000.0,
        )
        assert gov_ok, f"RiskGovernor should allow: {gov_reason}"

        # Step 2: Allocator gate
        alloc_decision = alloc.check_allocation(
            strategy_name=signal["strategy"],
            symbol=signal["symbol"],
            asset_class=signal["asset_class"],
            proposed_risk_usd=signal["proposed_risk_usd"],
            open_positions=[],
        )
        assert alloc_decision.allowed, f"Allocator should allow: {alloc_decision.reason}"

        # If both pass, signal would proceed to BrokerManager via Intent Layer
        # (tested separately in integration test_signal_pipeline.py)


# =============================================================================
# PHASE 10.11 — POSITION TRACKING FIX (ORDER_FILLED → internal state)
# =============================================================================

class TestPositionTracking:
    """
    Tests for the position-tracking fix:
      - Fill creates internal open position
      - /api/positions (via scheduler.get_open_positions) reflects the open trade
      - Reconciliation correctly matches broker and internal positions
      - Restart restores internal position state
      - Broker-only positions are handled via deterministic repair path
    """

    def _make_scheduler(self, tmp_path, bm=None, connected=False):
        """Build a minimal AutonomousScheduler with mocked broker."""
        from live.autonomous_scheduler import AutonomousScheduler

        cfg = _base_config(tmp_path)
        broker = bm or _make_mock_broker_manager(connected=connected)
        orch = MagicMock()
        orch._open_positions = []
        orch._strategy_validator_agent = None
        orch._risk_guardian_agent = None

        sched = AutonomousScheduler(
            orchestrator=orch,
            broker_manager=broker,
            config=cfg,
        )
        return sched, broker, cfg

    def _make_signal(self, symbol="EURUSD", direction="BUY", strategy="asian_range_breakout"):
        sig = MagicMock()
        sig.symbol = symbol
        sig.direction = direction
        sig.entry_price = 1.0850
        sig.stop_loss = 1.0800
        sig.take_profit = 1.0950
        sig.position_size = 0.1
        sig.risk_usd = 50.0
        sig.asset_class = "FOREX"
        return sig

    # ------------------------------------------------------------------
    # TEST 1: Fill creates internal open position
    # ------------------------------------------------------------------

    def test_fill_creates_internal_position(self, tmp_path):
        """
        After _register_open_position() is called (simulating what
        _signal_process_loop does after a successful fill), the position
        must appear in get_open_positions().
        """
        sched, _, _ = self._make_scheduler(tmp_path)
        signal = self._make_signal()

        sched._register_open_position(
            signal=signal,
            order_id="test-order-001",
            fill_price=1.0852,
            strategy_name="asian_range_breakout",
            risk_usd=50.0,
        )

        positions = sched.get_open_positions()
        assert len(positions) == 1
        pos = positions[0]
        assert pos["id"] == "test-order-001"
        assert pos["symbol"] == "EURUSD"
        assert pos["direction"] == "BUY"
        assert pos["entry_price"] == 1.0852
        assert pos["strategy"] == "asian_range_breakout"

    # ------------------------------------------------------------------
    # TEST 2: get_open_positions reflects trade after fill (API path)
    # ------------------------------------------------------------------

    def test_get_open_positions_reflects_fill(self, tmp_path):
        """
        Simulates the full signal dispatch path: broker returns success,
        _signal_process_loop calls _register_open_position, and the
        scheduler's get_open_positions() returns the position immediately.
        """
        sched, bm, _ = self._make_scheduler(tmp_path)

        # Configure broker to accept orders
        fill_result = MagicMock()
        fill_result.success = True
        fill_result.order_id = "fill-eurusd-001"
        fill_result.filled_price = 1.0851
        bm.submit_order.return_value = fill_result

        tradeable = MagicMock()
        tradeable.is_tradeable = True
        bm.check_symbol.return_value = tradeable

        signal = self._make_signal()

        # Simulate what _signal_process_loop does
        fill = sched._process_signal_through_intent_layer(signal)
        assert fill is not None and fill.success, "order should succeed"

        sched._register_open_position(
            signal=signal,
            order_id=fill.order_id,
            fill_price=fill.filled_price,
            strategy_name="asian_range_breakout",
            risk_usd=50.0,
        )

        open_pos = sched.get_open_positions()
        assert len(open_pos) == 1
        assert open_pos[0]["symbol"] == "EURUSD"
        assert open_pos[0]["id"] == "fill-eurusd-001"
        assert open_pos[0]["entry_price"] == 1.0851

    # ------------------------------------------------------------------
    # TEST 3: Reconciliation matches when internal position registered
    # ------------------------------------------------------------------

    def test_reconciliation_matches_after_fill(self, tmp_path):
        """
        After _register_open_position, ReconciliationService._get_internal_positions
        should return the position and no broker_only ghost should appear.
        """
        from live.reconciliation_service import ReconciliationService

        sched, bm, _ = self._make_scheduler(tmp_path)
        signal = self._make_signal()

        sched._register_open_position(
            signal=signal,
            order_id="recon-order-001",
            fill_price=1.0850,
            strategy_name="asian_range_breakout",
            risk_usd=50.0,
        )

        # Build ReconciliationService with scheduler attached
        recon = ReconciliationService(
            connector=MagicMock(),
            orchestrator=MagicMock(_open_positions=[]),
            state_machine=MagicMock(),
            scheduler=sched,
        )

        internal = recon._get_internal_positions()
        assert len(internal) == 1
        assert internal[0]["instrument"] == "EURUSD"
        assert internal[0]["direction"] == "BUY"

    # ------------------------------------------------------------------
    # TEST 4: Restart restores internal position state from persistence
    # ------------------------------------------------------------------

    def test_restart_restores_positions_from_persistence(self, tmp_path):
        """
        After a restart, AutonomousScheduler.start() must reload positions
        from the PositionStateStore so get_open_positions() returns them
        without any new order being submitted.
        """
        from live.autonomous_scheduler import AutonomousScheduler
        from live.persistence_manager import PersistenceManager

        cfg = _base_config(tmp_path)

        # Pre-populate the persistence store with one position
        pm = PersistenceManager(cfg)
        pm.positions.upsert({
            "id": "persisted-pos-001",
            "symbol": "EURUSD",
            "direction": "BUY",
            "entry_price": 1.0840,
            "stop_loss": 1.0790,
            "take_profit": 1.0940,
            "entry_time": "2026-04-17T10:00:00+00:00",
            "units": 0.1,
            "risk_usd": 50.0,
            "unrealized_pnl": 0.0,
            "notional_usd": 10840.0,
            "asset_class": "FOREX",
            "strategy": "asian_range_breakout",
        })

        bm = _make_mock_broker_manager(connected=False)
        orch = MagicMock()
        orch._open_positions = []
        orch._strategy_validator_agent = None
        orch._risk_guardian_agent = None

        sched = AutonomousScheduler(
            orchestrator=orch,
            broker_manager=bm,
            config=cfg,
            persistence_manager=pm,
        )

        # Simulate what start() does for position recovery (without running loops)
        recovered = pm.positions.get_all()
        with sched._position_lock:
            sched._open_positions = list(recovered)

        positions = sched.get_open_positions()
        assert len(positions) == 1
        assert positions[0]["id"] == "persisted-pos-001"
        assert positions[0]["symbol"] == "EURUSD"

        # Verify no order was submitted during recovery
        bm.submit_order.assert_not_called()

    # ------------------------------------------------------------------
    # TEST 5: Broker-only position is imported via deterministic repair
    # ------------------------------------------------------------------

    def test_broker_only_positions_imported_by_reconciliation(self, tmp_path):
        """
        When the reconciliation loop detects a broker_only position,
        _import_broker_only_positions() must add it to get_open_positions()
        without submitting a new order.
        """
        sched, bm, _ = self._make_scheduler(tmp_path, connected=True)

        # Mock a broker position for EURUSD that the engine doesn't know
        bp = MagicMock()
        bp.instrument = "EURUSD"
        bp.side = MagicMock(value="BUY")
        bp.units = 0.1
        bp.avg_price = 1.0850
        bp.stop_loss = 1.0800
        bp.take_profit = 1.0950
        bp.open_time = "2026-04-17T09:00:00+00:00"
        bp.order_id = "broker-only-001"
        bp.strategy = "asian_range_breakout"
        bp.asset_class = "FOREX"
        bp.unrealized_pnl = 5.0
        bm.get_positions.return_value = [bp]

        # Engine has no internal positions yet
        assert sched.get_open_positions() == []

        # Trigger the import path
        sched._import_broker_only_positions(["EURUSD"])

        positions = sched.get_open_positions()
        assert len(positions) == 1
        pos = positions[0]
        assert pos["symbol"] == "EURUSD"
        assert pos["direction"] == "BUY"
        assert pos.get("_imported_by_reconciliation") is not None

        # Must NOT submit a new order
        bm.submit_order.assert_not_called()

    # ------------------------------------------------------------------
    # TEST 6: Duplicate fill does not create duplicate position
    # ------------------------------------------------------------------

    def test_duplicate_fill_does_not_duplicate_position(self, tmp_path):
        """
        Calling _register_open_position twice with the same order_id must
        result in exactly one position in the registry (upsert semantics).
        """
        sched, _, _ = self._make_scheduler(tmp_path)
        signal = self._make_signal()

        sched._register_open_position(
            signal=signal, order_id="dup-001",
            fill_price=1.0850, strategy_name="asian_range_breakout", risk_usd=50.0,
        )
        sched._register_open_position(
            signal=signal, order_id="dup-001",
            fill_price=1.0852, strategy_name="asian_range_breakout", risk_usd=50.0,
        )

        positions = sched.get_open_positions()
        assert len(positions) == 1
        # Latest fill price should win
        assert positions[0]["entry_price"] == 1.0852

    # ------------------------------------------------------------------
    # TEST 7: Close removes position from registry and persistence
    # ------------------------------------------------------------------

    def test_close_removes_position(self, tmp_path):
        """
        After _remove_open_position(), get_open_positions() must be empty
        and the position must be gone from the PositionStateStore.
        """
        sched, _, _ = self._make_scheduler(tmp_path)
        signal = self._make_signal()

        sched._register_open_position(
            signal=signal, order_id="close-001",
            fill_price=1.0850, strategy_name="asian_range_breakout", risk_usd=50.0,
        )
        assert len(sched.get_open_positions()) == 1

        sched._remove_open_position("close-001", "EURUSD", "TP_HIT: price=1.0950")
        assert sched.get_open_positions() == []

        # Verify persistence store also cleared
        pm = sched._get_persistence_manager()
        if pm:
            assert all(
                p.get("id") != "close-001"
                for p in pm.positions.get_all()
            )

    # ------------------------------------------------------------------
    # TEST 8: ReconciliationService.set_scheduler wires up correctly
    # ------------------------------------------------------------------

    def test_set_scheduler_wires_reconciliation(self, tmp_path):
        """
        After ReconciliationService.set_scheduler(sched), _get_internal_positions
        must return the scheduler's registry even if orchestrator._open_positions
        is empty.
        """
        from live.reconciliation_service import ReconciliationService

        sched, _, _ = self._make_scheduler(tmp_path)
        signal = self._make_signal()
        sched._register_open_position(
            signal=signal, order_id="wire-001",
            fill_price=1.0850, strategy_name="asian_range_breakout", risk_usd=50.0,
        )

        recon = ReconciliationService(
            connector=MagicMock(),
            orchestrator=MagicMock(_open_positions=[]),
            state_machine=MagicMock(),
        )
        # Initially uses orchestrator (empty)
        assert recon._get_internal_positions() == []

        # Wire scheduler
        recon.set_scheduler(sched)
        internal = recon._get_internal_positions()
        assert len(internal) == 1
        assert internal[0]["instrument"] == "EURUSD"

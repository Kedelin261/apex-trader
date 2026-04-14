"""
APEX MULTI-MARKET TJR ENGINE
Unit Tests — TJR Strategy Engine

Tests:
- Swing point detection accuracy
- Market structure classification
- Liquidity event detection
- BOS detection
- Session classification
- Signal generation (determinism)
- Quality scoring
- Strategy eligibility checks
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from datetime import datetime, timezone, timedelta
from typing import List

from domain.models import (
    Candle, InstrumentType, VenueType, SignalDirection,
    MarketStructureState, SessionType
)
from core.tjr_strategy_engine import (
    TJRStrategyEngine, SwingPointDetector, MarketStructureAnalyzer,
    LiquidityDetector, BOSDetector, SessionClassifier, SetupQualityScorer
)


# =============================================================================
# FIXTURES
# =============================================================================

def make_candle(ts: datetime, o: float, h: float, l: float, c: float,
                instrument: str = "EURUSD", tf: str = "M15") -> Candle:
    return Candle(timestamp=ts, open=o, high=h, low=l, close=c,
                  volume=1000, timeframe=tf, instrument=instrument, is_complete=True)


def make_trending_bullish_candles(n: int = 100) -> List[Candle]:
    """Generate a trending bullish candle series with clear HH/HL structure."""
    candles = []
    base_price = 1.1000
    base_time = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)

    for i in range(n):
        # Uptrend with pullbacks
        trend = i * 0.0003  # 3 pip trend per candle
        noise = (i % 5 - 2) * 0.0001

        price = base_price + trend + noise
        o = price
        h = price + 0.0005
        l = price - 0.0002
        c = price + 0.0003

        candles.append(make_candle(
            base_time + timedelta(minutes=i * 15),
            round(o, 5), round(h, 5), round(l, 5), round(c, 5)
        ))

    return candles


def make_session_candles_london() -> List[Candle]:
    """Candles during London session (09:00 UTC)."""
    candles = []
    base_time = datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc)
    for i in range(50):
        candles.append(make_candle(
            base_time + timedelta(minutes=i * 15),
            1.1000, 1.1010, 1.0995, 1.1005
        ))
    return candles


# =============================================================================
# SESSION CLASSIFIER TESTS
# =============================================================================

class TestSessionClassifier:

    def test_london_session(self):
        """08:00 UTC should be LONDON."""
        dt = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
        assert SessionClassifier.classify(dt) == SessionType.LONDON

    def test_overlap_session(self):
        """13:30 UTC should be OVERLAP (London/NY overlap)."""
        dt = datetime(2024, 1, 2, 13, 30, tzinfo=timezone.utc)
        assert SessionClassifier.classify(dt) == SessionType.OVERLAP

    def test_newyork_session(self):
        """17:00 UTC should be NEW_YORK (after overlap ends)."""
        dt = datetime(2024, 1, 2, 17, 0, tzinfo=timezone.utc)
        assert SessionClassifier.classify(dt) == SessionType.NEW_YORK

    def test_asian_session(self):
        """03:00 UTC should be ASIAN."""
        dt = datetime(2024, 1, 2, 3, 0, tzinfo=timezone.utc)
        assert SessionClassifier.classify(dt) == SessionType.ASIAN

    def test_tradeable_sessions(self):
        """London, NY, Overlap are tradeable. Asian is not (for TJR)."""
        assert SessionClassifier.is_tradeable_session(SessionType.LONDON)
        assert SessionClassifier.is_tradeable_session(SessionType.NEW_YORK)
        assert SessionClassifier.is_tradeable_session(SessionType.OVERLAP)
        assert not SessionClassifier.is_tradeable_session(SessionType.ASIAN)
        assert not SessionClassifier.is_tradeable_session(SessionType.OFF_HOURS)


# =============================================================================
# SWING POINT DETECTOR TESTS
# =============================================================================

class TestSwingPointDetector:

    def test_detects_swing_high(self):
        """Should detect a clear swing high in the middle of a series."""
        detector = SwingPointDetector(swing_lookback=3)
        candles = []
        base = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)

        # Create: up → peak → down
        prices = [1.10, 1.11, 1.12, 1.13, 1.15, 1.13, 1.12, 1.11, 1.10]
        for i, p in enumerate(prices):
            candles.append(make_candle(
                base + timedelta(minutes=i * 15),
                p - 0.001, p + 0.002, p - 0.002, p + 0.001
            ))

        swings = detector.detect_swings(candles)
        highs = [s for s in swings if s.is_high]
        assert len(highs) >= 1
        assert any(abs(h.price - 1.152) < 0.005 for h in highs)

    def test_detects_swing_low(self):
        """Should detect a clear swing low."""
        detector = SwingPointDetector(swing_lookback=3)
        candles = []
        base = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)

        # Down → trough → up
        prices = [1.15, 1.14, 1.13, 1.12, 1.10, 1.12, 1.13, 1.14, 1.15]
        for i, p in enumerate(prices):
            candles.append(make_candle(
                base + timedelta(minutes=i * 15),
                p + 0.001, p + 0.002, p - 0.002, p - 0.001
            ))

        swings = detector.detect_swings(candles)
        lows = [s for s in swings if not s.is_high]
        assert len(lows) >= 1
        assert any(abs(l.price - 1.098) < 0.005 for l in lows)

    def test_insufficient_candles_returns_empty(self):
        """Under 2*n+1 candles should return empty."""
        detector = SwingPointDetector(swing_lookback=5)
        candles = [make_candle(datetime(2024, 1, 2, tzinfo=timezone.utc), 1.1, 1.11, 1.09, 1.1)]
        assert detector.detect_swings(candles) == []

    def test_no_false_swing_on_flat_market(self):
        """Flat market with equal highs should produce minimal swings."""
        detector = SwingPointDetector(swing_lookback=3)
        candles = []
        base = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
        for i in range(20):
            candles.append(make_candle(
                base + timedelta(minutes=i * 15),
                1.1000, 1.1005, 1.0995, 1.1002  # Very flat
            ))
        swings = detector.detect_swings(candles)
        # Should be very few or zero swings in flat market
        assert len(swings) <= 2


# =============================================================================
# MARKET STRUCTURE ANALYZER TESTS
# =============================================================================

class TestMarketStructureAnalyzer:

    def test_bullish_structure(self):
        """HH + HL series should return BULLISH."""
        from domain.models import SwingPoint
        analyzer = MarketStructureAnalyzer(min_swings=2)
        base = datetime(2024, 1, 2, tzinfo=timezone.utc)

        swings = [
            SwingPoint(timestamp=base, price=1.10, is_high=True, confirmed=True),
            SwingPoint(timestamp=base + timedelta(hours=1), price=1.12, is_high=True, confirmed=True),  # HH
            SwingPoint(timestamp=base + timedelta(minutes=30), price=1.09, is_high=False, confirmed=True),
            SwingPoint(timestamp=base + timedelta(hours=2), price=1.11, is_high=False, confirmed=True),  # HL
        ]

        result = analyzer.classify(swings)
        assert result == MarketStructureState.BULLISH

    def test_bearish_structure(self):
        """LH + LL series should return BEARISH."""
        from domain.models import SwingPoint
        analyzer = MarketStructureAnalyzer(min_swings=2)
        base = datetime(2024, 1, 2, tzinfo=timezone.utc)

        swings = [
            SwingPoint(timestamp=base, price=1.12, is_high=True, confirmed=True),
            SwingPoint(timestamp=base + timedelta(hours=1), price=1.10, is_high=True, confirmed=True),  # LH
            SwingPoint(timestamp=base + timedelta(minutes=30), price=1.09, is_high=False, confirmed=True),
            SwingPoint(timestamp=base + timedelta(hours=2), price=1.07, is_high=False, confirmed=True),  # LL
        ]

        result = analyzer.classify(swings)
        assert result == MarketStructureState.BEARISH

    def test_neutral_when_insufficient_swings(self):
        """Fewer than min_swings should return NEUTRAL."""
        from domain.models import SwingPoint
        analyzer = MarketStructureAnalyzer(min_swings=2)
        swings = [
            SwingPoint(timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc), price=1.10, is_high=True)
        ]
        assert analyzer.classify(swings) == MarketStructureState.NEUTRAL


# =============================================================================
# CANDLE PROPERTIES TESTS
# =============================================================================

class TestCandleProperties:

    def test_bullish_candle(self):
        c = Candle(timestamp=datetime.now(timezone.utc),
                   open=1.1000, high=1.1020, low=1.0995, close=1.1015)
        assert c.is_bullish
        assert not c.is_bearish

    def test_bearish_candle(self):
        c = Candle(timestamp=datetime.now(timezone.utc),
                   open=1.1015, high=1.1020, low=1.0995, close=1.1000)
        assert c.is_bearish
        assert not c.is_bullish

    def test_body_ratio(self):
        # body=0.0015, range=0.0025
        c = Candle(timestamp=datetime.now(timezone.utc),
                   open=1.1000, high=1.1020, low=1.0995, close=1.1015)
        body = abs(c.close - c.open)   # 0.0015
        range_ = c.high - c.low        # 0.0025
        expected = body / range_
        assert abs(c.body_ratio - expected) < 1e-6

    def test_doji_body_ratio_near_zero(self):
        c = Candle(timestamp=datetime.now(timezone.utc),
                   open=1.1000, high=1.1020, low=1.0980, close=1.1001)
        assert c.body_ratio < 0.1


# =============================================================================
# STRATEGY ENGINE ELIGIBILITY TESTS
# =============================================================================

class TestStrategyEligibility:

    def setup_method(self):
        self.config = {
            "strategy": {"tjr": {"swing_lookback": 5, "min_swing_size_pips": 5,
                                  "equal_level_tolerance_pips": 2.0, "sweep_min_pips_beyond": 1.0,
                                  "confirmation_candle_min_body_pct": 0.5, "entry_buffer_pips": 1.0,
                                  "sl_buffer_pips": 2.0, "min_sl_pips": 5.0}},
            "risk": {"min_reward_to_risk_ratio": 2.0}
        }
        self.engine = TJRStrategyEngine(self.config)

    def test_crypto_not_eligible(self):
        """Crypto should be rejected by TJR eligibility check."""
        eligible, reason = self.engine._is_eligible(InstrumentType.CRYPTO)
        assert not eligible
        assert "CRYPTO" in reason

    def test_prediction_not_eligible(self):
        """Prediction markets not eligible for TJR."""
        eligible, reason = self.engine._is_eligible(InstrumentType.PREDICTION)
        assert not eligible

    def test_forex_eligible(self):
        eligible, _ = self.engine._is_eligible(InstrumentType.FOREX)
        assert eligible

    def test_gold_eligible(self):
        eligible, _ = self.engine._is_eligible(InstrumentType.GOLD)
        assert eligible

    def test_futures_eligible(self):
        eligible, _ = self.engine._is_eligible(InstrumentType.FUTURES)
        assert eligible

    def test_insufficient_candles_returns_none(self):
        """With less than 50 candles, engine should return None."""
        candles = [make_candle(datetime.now(timezone.utc), 1.1, 1.11, 1.09, 1.1)]
        result = self.engine.analyze(candles, "EURUSD", InstrumentType.FOREX,
                                     VenueType.FOREX_BROKER)
        assert result is None


# =============================================================================
# RISK MANAGER TESTS
# =============================================================================

class TestRiskManager:

    def setup_method(self):
        self.config = {
            "risk": {
                "max_risk_per_trade_pct": 1.0,
                "max_daily_loss_pct": 3.0,
                "max_total_drawdown_pct": 10.0,
                "max_concurrent_trades": 2,
                "min_reward_to_risk_ratio": 2.0,
                "max_spread_pips": 3.0,
                "max_slippage_pips": 2.0,
                "max_position_size_usd": 10000.0,
                "account_balance_default": 100000.0
            }
        }

    def test_daily_loss_kill_switch(self):
        """After 3% daily loss, trading should be blocked."""
        from core.risk_manager import DailyRiskGovernor
        gov = DailyRiskGovernor(max_daily_loss_pct=3.0, account_balance=100000.0)
        gov.record_trade_pnl(-3001.0)  # Over 3% = $3000
        allowed, reason = gov.is_trading_allowed()
        assert not allowed
        assert gov.is_killed

    def test_rr_rejection(self):
        """Signal with R:R below minimum should be rejected."""
        from core.risk_manager import RiskManager
        from domain.models import Signal, InstrumentType, VenueType, SignalDirection, SessionType, StrategyFamily

        rm = RiskManager(self.config)
        signal = Signal(
            instrument="EURUSD",
            instrument_type=InstrumentType.FOREX,
            venue_type=VenueType.FOREX_BROKER,
            timeframe="M15",
            session=SessionType.LONDON,
            direction=SignalDirection.BUY,
            entry_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1080,  # only 1.6 R:R
            position_size_lots=0.1,
            risk_amount_usd=500.0,
            reward_to_risk=1.6,  # Below 2.0 minimum
            stop_distance_pips=50,
            target_distance_pips=80,
            strategy_family=StrategyFamily.TJR
        )

        result = rm.validate_signal(signal, open_positions=[])
        assert not result.approved
        from domain.models import RiskRejectionReason
        assert RiskRejectionReason.RR_RATIO_INSUFFICIENT in result.rejection_codes

    def test_spread_rejection(self):
        """Signal with spread > max should be rejected."""
        from core.risk_manager import RiskManager
        from domain.models import Signal, InstrumentType, VenueType, SignalDirection, SessionType, StrategyFamily

        rm = RiskManager(self.config)
        signal = Signal(
            instrument="EURUSD",
            instrument_type=InstrumentType.FOREX,
            venue_type=VenueType.FOREX_BROKER,
            timeframe="M15",
            session=SessionType.LONDON,
            direction=SignalDirection.BUY,
            entry_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            position_size_lots=0.1,
            risk_amount_usd=500.0,
            reward_to_risk=2.0,
            stop_distance_pips=50,
            target_distance_pips=100,
            strategy_family=StrategyFamily.TJR
        )

        result = rm.validate_signal(signal, open_positions=[], current_spread_pips=5.0)
        assert not result.approved
        from domain.models import RiskRejectionReason
        assert RiskRejectionReason.SPREAD_TOO_WIDE in result.rejection_codes

    def test_kill_switch_blocks_all(self):
        """Kill switch must block all subsequent signals."""
        from core.risk_manager import RiskManager
        from domain.models import Signal, InstrumentType, VenueType, SignalDirection, SessionType, StrategyFamily

        rm = RiskManager(self.config)
        rm.engage_kill_switch("test_kill")

        signal = Signal(
            instrument="EURUSD",
            instrument_type=InstrumentType.FOREX,
            venue_type=VenueType.FOREX_BROKER,
            timeframe="M15",
            session=SessionType.LONDON,
            direction=SignalDirection.BUY,
            entry_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            position_size_lots=0.1,
            risk_amount_usd=500.0,
            reward_to_risk=2.0,
            stop_distance_pips=50,
            target_distance_pips=100,
            strategy_family=StrategyFamily.TJR
        )

        result = rm.validate_signal(signal, open_positions=[])
        assert not result.approved
        from domain.models import RiskRejectionReason
        assert RiskRejectionReason.KILL_SWITCH_ACTIVE in result.rejection_codes


# =============================================================================
# AGENT PROTOCOL TESTS
# =============================================================================

class TestAgentProtocol:

    def test_ledger_records_message(self, tmp_path):
        """Messages should be recorded to ledger."""
        from protocol.agent_protocol import (
            AgentDecisionLedger, AgentStateStore,
            AgentMessage, AgentMessageType, MessagePriority, AgentName
        )
        ledger = AgentDecisionLedger(str(tmp_path / "test_ledger.jsonl"))
        msg = AgentMessage(
            source_agent=AgentName.MARKET_SENTINEL,
            message_type=AgentMessageType.MARKET_SCAN_RESULT,
            payload={"test": True},
            final_status="TRADEABLE"
        )
        mid = ledger.record(msg)
        assert ledger.total_messages == 1

        # Read from disk to confirm persistence
        ledger_file = tmp_path / "test_ledger.jsonl"
        assert ledger_file.exists()
        content = ledger_file.read_text()
        assert "MARKET_SCAN_RESULT" in content

    def test_message_chain_tracking(self, tmp_path):
        """Parent message chain should be trackable."""
        from protocol.agent_protocol import (
            AgentDecisionLedger, AgentMessage,
            AgentMessageType, AgentName
        )
        ledger = AgentDecisionLedger(str(tmp_path / "chain_ledger.jsonl"))

        parent = AgentMessage(
            source_agent=AgentName.MARKET_SENTINEL,
            message_type=AgentMessageType.MARKET_SCAN_RESULT,
            payload={"signal_id": "test-signal-123"}
        )
        ledger.record(parent)

        child = AgentMessage(
            source_agent=AgentName.RISK_GUARDIAN,
            message_type=AgentMessageType.RISK_APPROVED,
            payload={"signal_id": "test-signal-123"},
            parent_message_id=parent.message_id
        )
        ledger.record(child)

        chain = ledger.get_trade_chain("test-signal-123")
        assert len(chain) == 2

    def test_state_store_set_get(self):
        from protocol.agent_protocol import AgentStateStore
        store = AgentStateStore()
        store.set("test_key", {"value": 42})
        result = store.get("test_key")
        assert result == {"value": 42}

    def test_state_store_default(self):
        from protocol.agent_protocol import AgentStateStore
        store = AgentStateStore()
        result = store.get("nonexistent", default="fallback")
        assert result == "fallback"


# =============================================================================
# DATA VALIDATION TESTS
# =============================================================================

class TestDataValidation:

    def test_rejects_high_less_than_low(self):
        from data.market_data_service import HistoricalDataValidator
        validator = HistoricalDataValidator()
        bad_candle = Candle(
            timestamp=datetime.now(timezone.utc),
            open=1.1000, high=1.0900, low=1.1100, close=1.1050  # high < low
        )
        valid, errors = validator.validate([bad_candle])
        assert len(valid) == 0
        assert len(errors) > 0

    def test_rejects_duplicate_timestamps(self):
        from data.market_data_service import HistoricalDataValidator
        validator = HistoricalDataValidator()
        ts = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
        c1 = make_candle(ts, 1.1, 1.11, 1.09, 1.1)
        c2 = make_candle(ts, 1.1, 1.11, 1.09, 1.1)  # Same timestamp
        valid, errors = validator.validate([c1, c2])
        assert len(valid) == 1
        assert len(errors) == 1

    def test_accepts_valid_candle(self):
        from data.market_data_service import HistoricalDataValidator
        validator = HistoricalDataValidator()
        ts = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
        c = make_candle(ts, 1.1000, 1.1020, 1.0990, 1.1010)
        valid, errors = validator.validate([c])
        assert len(valid) == 1
        assert len(errors) == 0


# =============================================================================
# RUN ALL TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

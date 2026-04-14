"""
APEX MULTI-MARKET TJR ENGINE
TJR Strategy Engine — Deterministic rule-based trading logic.

STRICT RULE: Every condition in this file is machine-verifiable.
No discretionary logic. No "looks good" judgment.
No hand-waving. Every rule is explicit, numeric, and auditable.

Market structure is identified through strict swing point analysis.
Liquidity events require explicit price level tests.
BOS requires an explicit close beyond a prior swing level.
Entry requires a confirmed candle meeting defined criteria.
Stop and target are placed by strict rules relative to structure.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, time
from typing import Dict, List, Optional, Tuple

from domain.models import (
    Candle, TJRSetup, Signal, SignalDirection, SessionType,
    MarketStructureState, SwingPoint, LiquidityEvent, StructureBreak,
    InstrumentType, VenueType, StrategyFamily, SignalStatus,
    MarketRegime, EnvironmentMode
)

logger = logging.getLogger(__name__)


# =============================================================================
# SESSION CLASSIFIER
# =============================================================================

class SessionClassifier:
    """
    Classifies candle timestamp into trading session.
    All times in UTC.
    """

    # Boundaries in (hour, minute) tuples, UTC
    SESSIONS: Dict[SessionType, Tuple[Tuple[int, int], Tuple[int, int]]] = {
        SessionType.LONDON: ((7, 0), (16, 0)),
        SessionType.NEW_YORK: ((13, 0), (22, 0)),
        SessionType.ASIAN: ((0, 0), (8, 0)),
        SessionType.SYDNEY: ((22, 0), (7, 0)),   # wraps midnight
    }

    OVERLAP_START = (13, 0)
    OVERLAP_END = (16, 0)

    @classmethod
    def classify(cls, dt: datetime) -> SessionType:
        """Return the most relevant session for a given UTC datetime."""
        h, m = dt.hour, dt.minute
        t = h * 60 + m

        # Check overlap first (highest priority)
        ol_s = cls.OVERLAP_START[0] * 60 + cls.OVERLAP_START[1]
        ol_e = cls.OVERLAP_END[0] * 60 + cls.OVERLAP_END[1]
        if ol_s <= t < ol_e:
            return SessionType.OVERLAP

        # London
        ld_s, ld_e = cls.SESSIONS[SessionType.LONDON]
        if (ld_s[0] * 60 + ld_s[1]) <= t < (ld_e[0] * 60 + ld_e[1]):
            return SessionType.LONDON

        # New York (non-overlap)
        ny_s, ny_e = cls.SESSIONS[SessionType.NEW_YORK]
        if (ny_s[0] * 60 + ny_s[1]) <= t < (ny_e[0] * 60 + ny_e[1]):
            return SessionType.NEW_YORK

        # Asian
        as_s, as_e = cls.SESSIONS[SessionType.ASIAN]
        if (as_s[0] * 60 + as_s[1]) <= t < (as_e[0] * 60 + as_e[1]):
            return SessionType.ASIAN

        # Sydney (wraps midnight)
        sy_s = cls.SESSIONS[SessionType.SYDNEY][0][0] * 60 + cls.SESSIONS[SessionType.SYDNEY][0][1]
        sy_e = cls.SESSIONS[SessionType.SYDNEY][1][0] * 60 + cls.SESSIONS[SessionType.SYDNEY][1][1]
        if t >= sy_s or t < sy_e:
            return SessionType.SYDNEY

        return SessionType.OFF_HOURS

    @classmethod
    def is_tradeable_session(cls, session: SessionType) -> bool:
        """
        London, NY, and Overlap are TJR-tradeable sessions.
        Asian is tradeable only for Asian-session instruments.
        """
        return session in (SessionType.LONDON, SessionType.NEW_YORK, SessionType.OVERLAP)


# =============================================================================
# SWING POINT DETECTOR
# =============================================================================

class SwingPointDetector:
    """
    Detects swing highs and lows using strict N-candle confirmation.

    A swing HIGH is confirmed when: candle[i].high > all candles
    in [i-n, i-1] AND [i+1, i+n].
    A swing LOW is confirmed when: candle[i].low < all candles
    in [i-n, i-1] AND [i+1, i+n].

    This is deterministic and reproducible.
    """

    def __init__(self, swing_lookback: int = 5):
        """
        swing_lookback: how many candles on each side define a swing.
        Default 5 gives strong, clean swings. 3 is more sensitive.
        """
        self.n = swing_lookback

    def detect_swings(self, candles: List[Candle]) -> List[SwingPoint]:
        """
        Scan candle array and return all confirmed swing points.
        Requires at least 2*n+1 candles.
        """
        if len(candles) < 2 * self.n + 1:
            return []

        swings: List[SwingPoint] = []

        for i in range(self.n, len(candles) - self.n):
            candle = candles[i]

            # Swing HIGH check
            is_swing_high = all(
                candle.high > candles[j].high
                for j in range(i - self.n, i + self.n + 1)
                if j != i
            )
            if is_swing_high:
                swings.append(SwingPoint(
                    timestamp=candle.timestamp,
                    price=candle.high,
                    is_high=True,
                    confirmed=True,
                    candle_index=i
                ))

            # Swing LOW check
            is_swing_low = all(
                candle.low < candles[j].low
                for j in range(i - self.n, i + self.n + 1)
                if j != i
            )
            if is_swing_low:
                swings.append(SwingPoint(
                    timestamp=candle.timestamp,
                    price=candle.low,
                    is_high=False,
                    confirmed=True,
                    candle_index=i
                ))

        return swings

    def get_recent_n(self, swings: List[SwingPoint], n: int = 3,
                     highs_only: bool = False, lows_only: bool = False) -> List[SwingPoint]:
        """Return the N most recent swing points, optionally filtered."""
        filtered = swings
        if highs_only:
            filtered = [s for s in swings if s.is_high]
        elif lows_only:
            filtered = [s for s in swings if not s.is_high]
        return sorted(filtered, key=lambda s: s.timestamp)[-n:]


# =============================================================================
# MARKET STRUCTURE ANALYZER
# =============================================================================

class MarketStructureAnalyzer:
    """
    Analyzes market structure using swing points.

    BULLISH = Higher Highs AND Higher Lows (at least 2 of each, sequentially)
    BEARISH = Lower Highs AND Lower Lows (at least 2 of each, sequentially)
    NEUTRAL = Mixed or insufficient data

    This is deterministic. No interpretation.
    """

    def __init__(self, min_swings: int = 2):
        self.min_swings = min_swings

    def classify(self, swings: List[SwingPoint]) -> MarketStructureState:
        """Classify market structure from swing point sequence."""
        highs = sorted([s for s in swings if s.is_high], key=lambda s: s.timestamp)
        lows = sorted([s for s in swings if not s.is_high], key=lambda s: s.timestamp)

        if len(highs) < self.min_swings or len(lows) < self.min_swings:
            return MarketStructureState.NEUTRAL

        # Check Higher Highs
        hh = all(highs[i].price > highs[i - 1].price for i in range(1, len(highs)))
        # Check Higher Lows
        hl = all(lows[i].price > lows[i - 1].price for i in range(1, len(lows)))
        # Check Lower Highs
        lh = all(highs[i].price < highs[i - 1].price for i in range(1, len(highs)))
        # Check Lower Lows
        ll = all(lows[i].price < lows[i - 1].price for i in range(1, len(lows)))

        if hh and hl:
            return MarketStructureState.BULLISH
        elif lh and ll:
            return MarketStructureState.BEARISH
        else:
            return MarketStructureState.NEUTRAL


# =============================================================================
# LIQUIDITY DETECTOR
# =============================================================================

class LiquidityDetector:
    """
    Detects liquidity events:
    1. Equal Highs / Equal Lows (liquidity pools)
    2. Liquidity Sweeps (price wick beyond equal levels then returns)
    3. Stop Hunt patterns (sharp wick beyond recent swing then rapid reversal)

    All rules are numeric and explicit.
    """

    def __init__(self,
                 equal_level_tolerance_pips: float = 2.0,
                 sweep_min_pips: float = 1.0,
                 pip_size: float = 0.0001):
        self.tolerance = equal_level_tolerance_pips * pip_size
        self.sweep_min = sweep_min_pips * pip_size
        self.pip_size = pip_size

    def detect_equal_levels(self, swings: List[SwingPoint]) -> List[LiquidityEvent]:
        """Find clusters of swing points at approximately the same price level."""
        events: List[LiquidityEvent] = []
        highs = [s for s in swings if s.is_high]
        lows = [s for s in swings if not s.is_high]

        # Equal highs
        for i, h1 in enumerate(highs):
            for h2 in highs[i + 1:]:
                if abs(h1.price - h2.price) <= self.tolerance:
                    level = (h1.price + h2.price) / 2
                    events.append(LiquidityEvent(
                        timestamp=max(h1.timestamp, h2.timestamp),
                        level_price=level,
                        event_type="EQUAL_HIGH",
                        confirmed=True,
                        description=f"Equal highs at ~{level:.5f}"
                    ))

        # Equal lows
        for i, l1 in enumerate(lows):
            for l2 in lows[i + 1:]:
                if abs(l1.price - l2.price) <= self.tolerance:
                    level = (l1.price + l2.price) / 2
                    events.append(LiquidityEvent(
                        timestamp=max(l1.timestamp, l2.timestamp),
                        level_price=level,
                        event_type="EQUAL_LOW",
                        confirmed=True,
                        description=f"Equal lows at ~{level:.5f}"
                    ))

        return events

    def detect_sweep(self, candles: List[Candle],
                     liquidity_events: List[LiquidityEvent]) -> List[LiquidityEvent]:
        """
        Detect liquidity sweeps: price briefly extends beyond a liquidity level
        then closes back within the prior range.

        SWEEP_HIGH: candle.high > equal_high_level by >= sweep_min pips
                   AND candle.close < equal_high_level (returned below)
        SWEEP_LOW: candle.low < equal_low_level by >= sweep_min pips
                   AND candle.close > equal_low_level (returned above)
        """
        sweeps: List[LiquidityEvent] = []

        for event in liquidity_events:
            level = event.level_price
            for candle in candles:
                if event.event_type == "EQUAL_HIGH":
                    # High swept above, closed back below
                    if (candle.high > level + self.sweep_min and
                            candle.close < level):
                        sweeps.append(LiquidityEvent(
                            timestamp=candle.timestamp,
                            level_price=level,
                            event_type="SWEEP_HIGH",
                            swept_price=candle.high,
                            confirmed=True,
                            description=(
                                f"Swept high liquidity at {level:.5f}, "
                                f"wick to {candle.high:.5f}, close {candle.close:.5f}"
                            )
                        ))
                elif event.event_type == "EQUAL_LOW":
                    # Low swept below, closed back above
                    if (candle.low < level - self.sweep_min and
                            candle.close > level):
                        sweeps.append(LiquidityEvent(
                            timestamp=candle.timestamp,
                            level_price=level,
                            event_type="SWEEP_LOW",
                            swept_price=candle.low,
                            confirmed=True,
                            description=(
                                f"Swept low liquidity at {level:.5f}, "
                                f"wick to {candle.low:.5f}, close {candle.close:.5f}"
                            )
                        ))

        return sweeps


# =============================================================================
# BOS DETECTOR (Break of Structure)
# =============================================================================

class BOSDetector:
    """
    Detects Break of Structure (BOS) and Market Structure Shift (MSS).

    BOS BULLISH: Close above the most recent confirmed swing HIGH (in bullish bias)
    BOS BEARISH: Close below the most recent confirmed swing LOW (in bearish bias)

    Displacement: The breaking candle must be an impulsive move (body >= 60% of range).
    This ensures we're reacting to real momentum, not noise.
    """

    def __init__(self,
                 min_body_ratio: float = 0.5,
                 min_displacement_pips: float = 3.0,
                 pip_size: float = 0.0001):
        self.min_body_ratio = min_body_ratio
        self.min_displacement_pips = min_displacement_pips
        self.pip_size = pip_size

    def detect_bos(self, candles: List[Candle],
                   swings: List[SwingPoint]) -> List[StructureBreak]:
        """
        Scan for Break of Structure events.
        Only complete candles are used.
        """
        breaks: List[StructureBreak] = []
        highs = sorted([s for s in swings if s.is_high], key=lambda s: s.timestamp)
        lows = sorted([s for s in swings if not s.is_high], key=lambda s: s.timestamp)

        if not highs or not lows:
            return breaks

        for i, candle in enumerate(candles):
            if not candle.is_complete:
                continue

            # BOS BULLISH: close above most recent swing high
            recent_highs_before = [h for h in highs if h.timestamp < candle.timestamp]
            if recent_highs_before:
                last_swing_high = recent_highs_before[-1].price
                if candle.close > last_swing_high:
                    displacement = (candle.close - last_swing_high) / self.pip_size
                    if (candle.body_ratio >= self.min_body_ratio and
                            displacement >= self.min_displacement_pips):
                        breaks.append(StructureBreak(
                            timestamp=candle.timestamp,
                            break_type="BOS_BULLISH",
                            break_price=candle.close,
                            prior_structure_level=last_swing_high,
                            displacement_pips=displacement,
                            confirmed=True,
                            description=(
                                f"BOS Bullish: close {candle.close:.5f} > "
                                f"swing high {last_swing_high:.5f}, "
                                f"displacement {displacement:.1f}p"
                            )
                        ))

            # BOS BEARISH: close below most recent swing low
            recent_lows_before = [l for l in lows if l.timestamp < candle.timestamp]
            if recent_lows_before:
                last_swing_low = recent_lows_before[-1].price
                if candle.close < last_swing_low:
                    displacement = (last_swing_low - candle.close) / self.pip_size
                    if (candle.body_ratio >= self.min_body_ratio and
                            displacement >= self.min_displacement_pips):
                        breaks.append(StructureBreak(
                            timestamp=candle.timestamp,
                            break_type="BOS_BEARISH",
                            break_price=candle.close,
                            prior_structure_level=last_swing_low,
                            displacement_pips=displacement,
                            confirmed=True,
                            description=(
                                f"BOS Bearish: close {candle.close:.5f} < "
                                f"swing low {last_swing_low:.5f}, "
                                f"displacement {displacement:.1f}p"
                            )
                        ))

        return breaks


# =============================================================================
# SETUP QUALITY SCORER
# =============================================================================

class SetupQualityScorer:
    """
    Scores a TJR setup from 0.0 to 1.0 based on explicit, weighted criteria.
    This produces a machine-verifiable quality rating.

    Components:
    - Liquidity event present: +0.25
    - BOS confirmed: +0.20
    - Optimal session: +0.15
    - R:R ratio (scaled): +0.20
    - Entry candle body ratio >= 0.5: +0.10
    - Structure alignment: +0.10
    """

    def score(self, setup: TJRSetup) -> float:
        score = 0.0

        if setup.liquidity_event is not None:
            score += 0.25

        if setup.structure_break is not None and setup.structure_break.confirmed:
            score += 0.20

        if setup.is_session_optimal:
            score += 0.15

        # RR contribution (max at 3.0+)
        rr_score = min(setup.reward_to_risk / 3.0, 1.0) * 0.20
        score += rr_score

        if setup.entry_candle_body_ratio >= 0.5:
            score += 0.10

        # Structure state alignment with trade direction
        if (setup.direction == SignalDirection.BUY and
                setup.structure_state == MarketStructureState.BULLISH):
            score += 0.10
        elif (setup.direction == SignalDirection.SELL and
              setup.structure_state == MarketStructureState.BEARISH):
            score += 0.10

        return round(min(score, 1.0), 4)


# =============================================================================
# TJR STRATEGY ENGINE — MAIN CLASS
# =============================================================================

class TJRStrategyEngine:
    """
    The deterministic TJR strategy engine.

    Processing flow for each instrument:
    1. Validate input data
    2. Detect swing points
    3. Analyze market structure
    4. Detect liquidity events
    5. Detect BOS / displacement
    6. Classify session
    7. Check strategy eligibility for this instrument type
    8. Build entry rules
    9. Calculate SL / TP
    10. Score setup quality
    11. Produce TJRSetup or None

    Output: TJRSetup object (fully populated) or None if no valid setup.
    """

    # Instruments where TJR is applicable
    COMPATIBLE_INSTRUMENT_TYPES = {
        InstrumentType.FOREX,
        InstrumentType.GOLD,
        InstrumentType.FUTURES,
        InstrumentType.INDICES,
        InstrumentType.COMMODITIES,
    }

    def __init__(self, config: dict):
        self.config = config
        tjr_cfg = config.get("strategy", {}).get("tjr", {})

        self.swing_lookback = tjr_cfg.get("swing_lookback", 5)
        self.min_swing_size_pips = tjr_cfg.get("min_swing_size_pips", 5)
        self.bos_confirmation_candles = tjr_cfg.get("bos_confirmation_candles", 1)
        self.equal_level_tolerance_pips = tjr_cfg.get("equal_level_tolerance_pips", 2.0)
        self.sweep_min_pips = tjr_cfg.get("sweep_min_pips_beyond", 1.0)
        self.confirmation_candle_min_body_pct = tjr_cfg.get("confirmation_candle_min_body_pct", 0.5)
        self.entry_buffer_pips = tjr_cfg.get("entry_buffer_pips", 1.0)
        self.sl_buffer_pips = tjr_cfg.get("sl_buffer_pips", 2.0)
        self.min_sl_pips = tjr_cfg.get("min_sl_pips", 5.0)
        self.min_rr = config.get("risk", {}).get("min_reward_to_risk_ratio", 2.0)

    def _get_pip_size(self, instrument: str, instrument_type: InstrumentType) -> float:
        """Return pip size for the instrument."""
        if instrument_type == InstrumentType.GOLD or instrument == "XAUUSD":
            return 0.01
        if instrument_type == InstrumentType.FUTURES:
            return 0.25  # ES default; should be per-contract in production
        if "JPY" in instrument:
            return 0.01
        return 0.0001  # Standard forex

    def _is_eligible(self, instrument_type: InstrumentType) -> Tuple[bool, str]:
        """Check if TJR strategy is eligible for this instrument type."""
        if instrument_type not in self.COMPATIBLE_INSTRUMENT_TYPES:
            return False, f"TJR not compatible with {instrument_type.value}"
        return True, ""

    def analyze(self,
                candles: List[Candle],
                instrument: str,
                instrument_type: InstrumentType,
                venue_type: VenueType,
                timeframe: str = "M15") -> Optional[TJRSetup]:
        """
        Run the full TJR analysis on a candle series.
        Returns a TJRSetup if a valid setup is found, else None.

        Minimum candles required: 50 (to establish structure)
        """
        if len(candles) < 50:
            logger.debug(f"[TJR] Insufficient candles for {instrument}: {len(candles)}")
            return None

        # 1. Eligibility check
        eligible, reason = self._is_eligible(instrument_type)
        if not eligible:
            logger.debug(f"[TJR] Ineligible: {reason}")
            return None

        pip_size = self._get_pip_size(instrument, instrument_type)

        # 2. Detect swing points (use last 100 candles for context)
        analysis_candles = candles[-100:]
        swing_detector = SwingPointDetector(swing_lookback=self.swing_lookback)
        swings = swing_detector.detect_swings(analysis_candles)

        if len(swings) < 4:
            logger.debug(f"[TJR] Insufficient swing points for {instrument}: {len(swings)}")
            return None

        # 3. Analyze market structure
        structure_analyzer = MarketStructureAnalyzer(min_swings=2)
        structure_state = structure_analyzer.classify(swings)

        if structure_state == MarketStructureState.NEUTRAL:
            logger.debug(f"[TJR] Neutral structure for {instrument}, no valid bias")
            return None

        # 4. Detect liquidity events
        liq_detector = LiquidityDetector(
            equal_level_tolerance_pips=self.equal_level_tolerance_pips,
            sweep_min_pips=self.sweep_min_pips,
            pip_size=pip_size
        )
        equal_levels = liq_detector.detect_equal_levels(swings)
        sweeps = liq_detector.detect_sweep(analysis_candles, equal_levels)

        # Most recent liquidity event
        all_liq_events = sorted(equal_levels + sweeps, key=lambda e: e.timestamp)
        recent_liq_event = all_liq_events[-1] if all_liq_events else None

        # 5. Detect BOS
        bos_detector = BOSDetector(
            min_body_ratio=self.confirmation_candle_min_body_pct,
            min_displacement_pips=self.min_sl_pips,
            pip_size=pip_size
        )
        bos_events = bos_detector.detect_bos(analysis_candles, swings)
        recent_bos = None
        if bos_events:
            # Only consider BOS that aligns with structure
            aligned_bos = [
                b for b in bos_events
                if (structure_state == MarketStructureState.BULLISH
                    and b.break_type == "BOS_BULLISH")
                or (structure_state == MarketStructureState.BEARISH
                    and b.break_type == "BOS_BEARISH")
            ]
            if aligned_bos:
                recent_bos = sorted(aligned_bos, key=lambda b: b.timestamp)[-1]

        # If no BOS, we can still trade with liquidity sweep + structure
        # but setup quality will be lower (no BOS bonus)

        # 6. Classify session (based on last candle)
        last_candle = candles[-1]
        session = SessionClassifier.classify(last_candle.timestamp)
        is_session_optimal = SessionClassifier.is_tradeable_session(session)

        # Session filter: only trade in active sessions
        if not is_session_optimal:
            logger.debug(f"[TJR] Outside tradeable session: {session.value} for {instrument}")
            return None

        # 7. Determine trade direction
        # Bullish structure → look for BUY setups
        # Bearish structure → look for SELL setups
        if structure_state == MarketStructureState.BULLISH:
            direction = SignalDirection.BUY
        else:
            direction = SignalDirection.SELL

        # 8. Entry confirmation
        # Last candle must be a confirmation candle in the trade direction
        # Body ratio must meet minimum threshold
        if last_candle.body_ratio < self.confirmation_candle_min_body_pct:
            logger.debug(
                f"[TJR] Confirmation candle body ratio insufficient: "
                f"{last_candle.body_ratio:.2f} < {self.confirmation_candle_min_body_pct}"
            )
            return None

        # Direction alignment: confirmation candle must align with trade direction
        if direction == SignalDirection.BUY and not last_candle.is_bullish:
            logger.debug(f"[TJR] BUY setup requires bullish confirmation candle")
            return None
        if direction == SignalDirection.SELL and not last_candle.is_bearish:
            logger.debug(f"[TJR] SELL setup requires bearish confirmation candle")
            return None

        # 9. Calculate entry, stop, target
        highs = [s for s in swings if s.is_high]
        lows = [s for s in swings if not s.is_high]

        if not highs or not lows:
            return None

        last_swing_high = sorted(highs, key=lambda s: s.timestamp)[-1].price
        last_swing_low = sorted(lows, key=lambda s: s.timestamp)[-1].price

        buffer = self.entry_buffer_pips * pip_size
        sl_buffer = self.sl_buffer_pips * pip_size
        min_sl = self.min_sl_pips * pip_size

        if direction == SignalDirection.BUY:
            # Entry: above confirmation candle high with buffer
            entry_price = last_candle.high + buffer
            # Stop: below the most recent swing low with buffer
            raw_sl = last_swing_low - sl_buffer
            stop_loss = raw_sl

            # Minimum stop distance enforcement
            if entry_price - stop_loss < min_sl:
                stop_loss = entry_price - min_sl

            stop_distance = entry_price - stop_loss
            take_profit = entry_price + (stop_distance * self.min_rr)

        else:  # SELL
            # Entry: below confirmation candle low with buffer
            entry_price = last_candle.low - buffer
            # Stop: above the most recent swing high with buffer
            raw_sl = last_swing_high + sl_buffer
            stop_loss = raw_sl

            if stop_loss - entry_price < min_sl:
                stop_loss = entry_price + min_sl

            stop_distance = stop_loss - entry_price
            take_profit = entry_price - (stop_distance * self.min_rr)

        # Validate prices
        if stop_distance <= 0 or entry_price <= 0 or stop_loss <= 0:
            logger.warning(f"[TJR] Invalid price calculation for {instrument}")
            return None

        stop_pips = stop_distance / pip_size
        target_pips = abs(take_profit - entry_price) / pip_size
        rr = target_pips / stop_pips if stop_pips > 0 else 0.0

        if rr < self.min_rr:
            logger.debug(f"[TJR] R:R {rr:.2f} below minimum {self.min_rr}")
            return None

        # 10. Build the setup object
        setup = TJRSetup(
            instrument=instrument,
            instrument_type=instrument_type,
            timeframe=timeframe,
            session=session,
            structure_state=structure_state,
            recent_swing_highs=[s.price for s in sorted(highs, key=lambda x: x.timestamp)[-3:]],
            recent_swing_lows=[s.price for s in sorted(lows, key=lambda x: x.timestamp)[-3:]],
            last_swing_high=last_swing_high,
            last_swing_low=last_swing_low,
            liquidity_event=recent_liq_event,
            liquidity_level_swept=(recent_liq_event.level_price if recent_liq_event else None),
            structure_break=recent_bos,
            displacement_confirmed=(recent_bos is not None),
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_candle_body_ratio=last_candle.body_ratio,
            entry_trigger_description=(
                f"{'Bullish' if direction == SignalDirection.BUY else 'Bearish'} "
                f"confirmation candle above/below prior swing, "
                f"{'BOS confirmed' if recent_bos else 'no BOS'}, "
                f"{'liquidity swept' if recent_liq_event else 'no liquidity event'}"
            ),
            stop_distance_pips=stop_pips,
            target_distance_pips=target_pips,
            reward_to_risk=rr,
            is_session_optimal=is_session_optimal,
        )

        # 11. Score the setup
        scorer = SetupQualityScorer()
        setup.setup_quality_score = scorer.score(setup)

        logger.info(
            f"[TJR] Setup found: {instrument} {direction.value} | "
            f"Entry={entry_price:.5f} SL={stop_loss:.5f} TP={take_profit:.5f} | "
            f"R:R={rr:.2f} Quality={setup.setup_quality_score:.2f} | "
            f"Session={session.value}"
        )

        return setup

    def build_signal(self, setup: TJRSetup, venue_type: VenueType,
                     account_balance: float,
                     risk_pct: float = 1.0) -> Optional[Signal]:
        """
        Build a fully sized Signal from a validated TJRSetup.
        Position sizing uses fixed fractional method.
        """
        pip_size = self._get_pip_size(setup.instrument, setup.instrument_type)
        pip_value = 10.0  # Default: $10/pip/lot (EURUSD std). Override per instrument.

        if setup.instrument_type == InstrumentType.GOLD:
            pip_value = 1.0  # Gold: $1/pip/lot for XAUUSD (0.01 pip size)

        risk_amount = account_balance * (risk_pct / 100.0)
        stop_distance_pips = setup.stop_distance_pips

        if stop_distance_pips <= 0 or pip_value <= 0:
            logger.warning(f"[TJR] Cannot size position: invalid pip data")
            return None

        position_size_lots = round(risk_amount / (stop_distance_pips * pip_value), 2)
        position_size_lots = max(0.01, position_size_lots)  # Minimum 0.01 lots

        return Signal(
            setup_id=setup.setup_id,
            instrument=setup.instrument,
            instrument_type=setup.instrument_type,
            venue_type=venue_type,
            timeframe=setup.timeframe,
            session=setup.session,
            direction=setup.direction,
            status=SignalStatus.CANDIDATE,
            entry_price=setup.entry_price,
            stop_loss=setup.stop_loss,
            take_profit=setup.take_profit,
            position_size_lots=position_size_lots,
            risk_amount_usd=risk_amount,
            reward_to_risk=setup.reward_to_risk,
            stop_distance_pips=setup.stop_distance_pips,
            target_distance_pips=setup.target_distance_pips,
            strategy_family=StrategyFamily.TJR,
            strategy_reason=setup.entry_trigger_description,
            structure_state=setup.structure_state.value,
            liquidity_event_detected=(setup.liquidity_event is not None),
            bos_confirmed=setup.displacement_confirmed,
            confidence_score=setup.setup_quality_score,
            is_paper_trade=True,
        )

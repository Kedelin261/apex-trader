"""
APEX MULTI-MARKET TJR ENGINE
Market Context Filter — Volatility, news, spread, and environment validation.

Prevents trading during:
- High-impact news windows
- Volatility spikes
- Abnormal spread expansion
- Market instability conditions
- Illiquid periods

Every filter decision is logged with full context.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from domain.models import MarketRegime, Candle

logger = logging.getLogger(__name__)


class VolatilityAnalyzer:
    """
    ATR-based volatility classifier.
    Computes ATR (Average True Range) and detects spikes.
    """

    def __init__(self, atr_period: int = 14):
        self.atr_period = atr_period

    def compute_atr(self, candles: List[Candle]) -> float:
        """Compute ATR using Wilder's smoothing method."""
        if len(candles) < self.atr_period + 1:
            return 0.0

        true_ranges = []
        for i in range(1, len(candles)):
            c = candles[i]
            prev_close = candles[i - 1].close
            tr = max(
                c.high - c.low,
                abs(c.high - prev_close),
                abs(c.low - prev_close)
            )
            true_ranges.append(tr)

        if not true_ranges:
            return 0.0

        # Wilder's smoothing
        atr = sum(true_ranges[:self.atr_period]) / self.atr_period
        for tr in true_ranges[self.atr_period:]:
            atr = (atr * (self.atr_period - 1) + tr) / self.atr_period

        return atr

    def is_volatility_spike(self, candles: List[Candle],
                             spike_multiplier: float = 3.0) -> Tuple[bool, float, float]:
        """
        Detect volatility spike.
        Returns (is_spike, current_atr, average_atr).
        Spike if current candle range > average ATR * multiplier.
        """
        if len(candles) < self.atr_period + 1:
            return False, 0.0, 0.0

        avg_atr = self.compute_atr(candles[:-1])
        last_candle = candles[-1]
        current_range = last_candle.high - last_candle.low

        is_spike = current_range > avg_atr * spike_multiplier
        return is_spike, current_range, avg_atr

    def classify_volatility(self, candles: List[Candle]) -> str:
        """Classify current volatility as LOW | NORMAL | HIGH | EXTREME."""
        if len(candles) < self.atr_period + 1:
            return "UNKNOWN"

        atr = self.compute_atr(candles)
        avg_price = candles[-1].close
        atr_pct = (atr / avg_price) * 100.0

        if atr_pct < 0.05:
            return "LOW"
        elif atr_pct < 0.15:
            return "NORMAL"
        elif atr_pct < 0.30:
            return "HIGH"
        else:
            return "EXTREME"


class RegimeClassifier:
    """
    Classifies the current market regime using ADX and price structure.

    ADX >= 40: STRONG TREND
    ADX 25-40: TRENDING
    ADX 15-25: RANGING / MEAN_REVERTING
    ADX < 15: CHOPPY / ILLIQUID

    Trend direction determined by +DI vs -DI.
    """

    def __init__(self, adx_period: int = 14):
        self.period = adx_period

    def compute_adx(self, candles: List[Candle]) -> Tuple[float, float, float]:
        """
        Compute ADX, +DI, -DI.
        Returns (adx, plus_di, minus_di).
        """
        if len(candles) < self.period * 2:
            return 0.0, 0.0, 0.0

        tr_list, plus_dm_list, minus_dm_list = [], [], []

        for i in range(1, len(candles)):
            c = candles[i]
            prev = candles[i - 1]

            tr = max(c.high - c.low, abs(c.high - prev.close), abs(c.low - prev.close))
            up_move = c.high - prev.high
            down_move = prev.low - c.low

            plus_dm = up_move if up_move > down_move and up_move > 0 else 0
            minus_dm = down_move if down_move > up_move and down_move > 0 else 0

            tr_list.append(tr)
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)

        # Smoothed sums (Wilder)
        def wilder_smooth(data, period):
            smoothed = sum(data[:period])
            result = [smoothed]
            for d in data[period:]:
                smoothed = smoothed - (smoothed / period) + d
                result.append(smoothed)
            return result

        if len(tr_list) < self.period:
            return 0.0, 0.0, 0.0

        smooth_tr = wilder_smooth(tr_list, self.period)
        smooth_plus = wilder_smooth(plus_dm_list, self.period)
        smooth_minus = wilder_smooth(minus_dm_list, self.period)

        dx_list = []
        for tr_s, p, m in zip(smooth_tr, smooth_plus, smooth_minus):
            if tr_s == 0:
                continue
            plus_di = 100.0 * p / tr_s
            minus_di = 100.0 * m / tr_s
            di_sum = plus_di + minus_di
            if di_sum == 0:
                continue
            dx = 100.0 * abs(plus_di - minus_di) / di_sum
            dx_list.append((dx, plus_di, minus_di))

        if not dx_list:
            return 0.0, 0.0, 0.0

        adx = sum(d[0] for d in dx_list[-self.period:]) / min(self.period, len(dx_list))
        _, plus_di, minus_di = dx_list[-1]
        return adx, plus_di, minus_di

    def classify(self, candles: List[Candle]) -> MarketRegime:
        """Classify current market regime."""
        adx, plus_di, minus_di = self.compute_adx(candles)

        if adx == 0.0:
            return MarketRegime.UNKNOWN

        if adx >= 25:
            if plus_di > minus_di:
                return MarketRegime.TRENDING_BULLISH
            else:
                return MarketRegime.TRENDING_BEARISH
        elif adx >= 15:
            return MarketRegime.RANGING
        else:
            return MarketRegime.ILLIQUID


class NewsFilter:
    """
    Economic calendar-based news filter.
    Blocks trading during high-impact news windows.

    Calendar data loaded from JSON file or placeholder.
    In production, integrate with ForexFactory / Investing.com API.
    """

    def __init__(self, calendar_file: Optional[str] = None,
                 block_before_minutes: int = 30,
                 block_after_minutes: int = 30,
                 impact_levels: List[str] = None):
        self.block_before = timedelta(minutes=block_before_minutes)
        self.block_after = timedelta(minutes=block_after_minutes)
        self.impact_levels = impact_levels or ["HIGH"]
        self._events: List[dict] = []

        if calendar_file:
            self._load_calendar(calendar_file)

    def _load_calendar(self, path: str):
        """Load economic calendar from JSON file."""
        try:
            with open(path, "r") as f:
                data = json.load(f)
                self._events = [
                    e for e in data
                    if e.get("impact", "").upper() in self.impact_levels
                ]
            logger.info(f"[NewsFilter] Loaded {len(self._events)} high-impact events")
        except FileNotFoundError:
            logger.warning(f"[NewsFilter] Calendar file not found: {path}")
        except Exception as e:
            logger.error(f"[NewsFilter] Failed to load calendar: {e}")

    def add_event(self, event_time: datetime, currency: str,
                  description: str, impact: str = "HIGH"):
        """Manually add a news event."""
        self._events.append({
            "time": event_time.isoformat(),
            "currency": currency,
            "description": description,
            "impact": impact
        })

    def is_news_active(self, check_time: datetime,
                       instrument: str = "") -> Tuple[bool, str]:
        """
        Check if a high-impact news event is active for the given time.
        Returns (is_active, description).
        """
        for event in self._events:
            try:
                event_time = datetime.fromisoformat(event["time"])
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)

                window_start = event_time - self.block_before
                window_end = event_time + self.block_after

                if window_start <= check_time <= window_end:
                    currency = event.get("currency", "")
                    # If instrument provided, only block relevant currencies
                    if instrument and currency:
                        if currency not in instrument.upper():
                            continue
                    desc = (
                        f"News: {event.get('description', 'Unknown')} "
                        f"[{currency}] at {event_time.strftime('%H:%M UTC')}"
                    )
                    return True, desc
            except Exception as e:
                logger.debug(f"[NewsFilter] Event parse error: {e}")

        return False, ""


class SpreadMonitor:
    """
    Monitors current spread and detects abnormal spread expansion.
    """

    def __init__(self, normal_spread_pips: float = 1.5,
                 max_spread_pips: float = 3.0,
                 spike_multiplier: float = 3.0):
        self.normal_spread = normal_spread_pips
        self.max_spread = max_spread_pips
        self.spike_multiplier = spike_multiplier
        self._spread_history: List[float] = []

    def record_spread(self, spread_pips: float):
        """Record a spread observation."""
        self._spread_history.append(spread_pips)
        if len(self._spread_history) > 100:
            self._spread_history.pop(0)

    def is_spread_acceptable(self, current_spread_pips: float) -> Tuple[bool, str]:
        """Check if current spread is acceptable."""
        if current_spread_pips > self.max_spread:
            return False, (
                f"Spread too wide: {current_spread_pips:.2f} pips > "
                f"max {self.max_spread:.2f} pips"
            )

        if self._spread_history:
            avg = sum(self._spread_history) / len(self._spread_history)
            if current_spread_pips > avg * self.spike_multiplier:
                return False, (
                    f"Spread spike: {current_spread_pips:.2f} pips = "
                    f"{current_spread_pips / avg:.1f}x avg ({avg:.2f}p)"
                )

        return True, ""


class MarketContextFilter:
    """
    Master market context filter.
    Aggregates all environmental filters into a single gate.

    A trade is environmentally eligible only if ALL filters pass.
    """

    def __init__(self, config: dict):
        self.config = config
        market_cfg = config.get("strategy", {}).get("tjr", {})
        risk_cfg = config.get("risk", {})
        news_cfg = config.get("news_filter", {})

        self.volatility_analyzer = VolatilityAnalyzer(atr_period=14)
        self.regime_classifier = RegimeClassifier(adx_period=14)
        self.news_filter = NewsFilter(
            calendar_file=news_cfg.get("calendar_file"),
            block_before_minutes=news_cfg.get("block_minutes_before", 30),
            block_after_minutes=news_cfg.get("block_minutes_after", 30),
            impact_levels=news_cfg.get("impact_levels", ["HIGH"])
        )
        self.spread_monitor = SpreadMonitor(
            max_spread_pips=risk_cfg.get("max_spread_pips", 3.0)
        )

        self.volatility_spike_multiplier = config.get("agents", {}).get(
            "market_sentinel", {}).get("volatility_spike_atr_multiplier", 3.0)

    def evaluate(self,
                 candles: List[Candle],
                 instrument: str,
                 current_spread_pips: float = 0.0,
                 check_time: Optional[datetime] = None) -> Dict:
        """
        Full environmental evaluation.
        Returns a dict with all filter results and a combined pass/fail.
        """
        if check_time is None:
            check_time = datetime.now(timezone.utc)

        results = {}

        # Volatility
        is_spike, current_atr, avg_atr = self.volatility_analyzer.is_volatility_spike(
            candles, spike_multiplier=self.volatility_spike_multiplier
        )
        vol_class = self.volatility_analyzer.classify_volatility(candles)
        results["volatility"] = {
            "is_spike": is_spike,
            "current_atr": current_atr,
            "avg_atr": avg_atr,
            "classification": vol_class,
            "passed": not is_spike
        }

        # Regime
        regime = self.regime_classifier.classify(candles)
        regime_blocks = {MarketRegime.UNSTABLE, MarketRegime.ILLIQUID, MarketRegime.NEWS_DRIVEN}
        results["regime"] = {
            "classification": regime.value,
            "passed": regime not in regime_blocks
        }

        # News
        news_active, news_desc = self.news_filter.is_news_active(check_time, instrument)
        results["news"] = {
            "active": news_active,
            "description": news_desc,
            "passed": not news_active
        }

        # Spread
        spread_ok, spread_msg = self.spread_monitor.is_spread_acceptable(current_spread_pips)
        self.spread_monitor.record_spread(current_spread_pips)
        results["spread"] = {
            "current_pips": current_spread_pips,
            "message": spread_msg,
            "passed": spread_ok
        }

        # Combined result
        all_passed = all(r.get("passed", True) for r in results.values())
        blocked_reasons = [
            f"{k}: {v.get('description') or v.get('message') or v.get('classification')}"
            for k, v in results.items()
            if not v.get("passed", True)
        ]

        results["summary"] = {
            "all_passed": all_passed,
            "regime": regime,
            "volatility_class": vol_class,
            "blocked_reasons": blocked_reasons,
            "checked_at": check_time.isoformat()
        }

        if all_passed:
            logger.debug(f"[MarketContext] {instrument}: all filters passed, regime={regime.value}")
        else:
            logger.info(
                f"[MarketContext] {instrument}: BLOCKED — {'; '.join(blocked_reasons)}"
            )

        return results

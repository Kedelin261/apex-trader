"""
APEX MULTI-MARKET TJR ENGINE
FOREX Strategy — Asian Range Breakout

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : FOREX
STRATEGY    : Asian Range Breakout

SETUP REQUIREMENTS:
  1. Define Asian session range:
       Asian session = 00:00–08:00 UTC
       Asian High = highest high of that window
       Asian Low  = lowest low of that window
       Asian Range Size = Asian High − Asian Low

  2. Range filter:
       Asian Range Size must be ≥ 10 pips AND ≤ 60 pips
       (For JPY pairs: pip = 0.01; all others: pip = 0.0001)
       If outside this band → NO TRADE

  3. Breakout trigger (London session only: 07:00–16:00 UTC):
       BUY  trigger : price closes ABOVE Asian High
       SELL trigger : price closes BELOW Asian Low
       Breakout candle body must be ≥ 50% of the candle range
         (body_size / (high − low) ≥ 0.50)

  4. ATR filter:
       ATR(14) on M15 candles
       Breakout candle range (high − low) must be ≥ 0.5 × ATR(14)
       If below → reject as false breakout

  5. STOP LOSS:
       BUY  SL = Asian Low  − 5 pips
       SELL SL = Asian High + 5 pips

  6. TAKE PROFIT:
       TP = 1.5 × (entry − SL)  [measured in price, not pips]
       Minimum R:R = 1.5

  7. Session kill:
       No new entries after 12:00 UTC (5 hours into London session)

  8. One trade per pair per session.

SIGNALS:
  BUY  when breakout is bullish above Asian High
  SELL when breakout is bearish below Asian Low
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
ASIAN_SESSION_START_UTC = 0    # 00:00
ASIAN_SESSION_END_UTC   = 8    # 08:00 (exclusive)
LONDON_SESSION_START_UTC = 7   # 07:00
LONDON_KILL_HOUR_UTC    = 12   # No new entries AT or AFTER 12:00 UTC

RANGE_MIN_PIPS = 10            # minimum asian range in pips
RANGE_MAX_PIPS = 60            # maximum asian range in pips

BODY_RATIO_MIN = 0.50          # breakout candle body ≥ 50% of range

ATR_PERIOD = 14                # ATR period (M15 candles)
ATR_BREAKOUT_MULT = 0.5        # breakout candle range ≥ 0.5 × ATR

SL_BUFFER_PIPS = 5             # stop loss buffer beyond Asian High/Low
TP_RR_RATIO    = 1.5           # TP = 1.5 × risk distance
MIN_RR         = 1.5           # minimum reward-to-risk
# ─────────────────────────────────────────────────────────────────────────────


def _pip_size(symbol: str) -> float:
    """Return pip size: 0.01 for JPY pairs, 0.0001 for all others."""
    if "JPY" in symbol.upper():
        return 0.01
    return 0.0001


class AsianRangeBreakout(BaseStrategy):
    """
    Forex: Asian Range Breakout — exact rule implementation.
    See module docstring for the complete rule set.
    """

    @property
    def name(self) -> str:
        return "asian_range_breakout"

    @property
    def asset_class(self) -> str:
        return "FOREX"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate Asian Range Breakout signal.

        candles: M15 OHLCV dicts, each must have:
                 open, high, low, close, volume, timestamp (datetime, UTC-aware)
        context: must include 'symbol'
        """
        ctx = context or {}
        symbol = ctx.get("symbol", "UNKNOWN")
        pip = _pip_size(symbol)

        if len(candles) < ATR_PERIOD + 5:
            return self._no_signal(symbol, "insufficient_candles")

        # ── Build Asian range ───────────────────────────────────────────────
        # Use the most-recent completed Asian session (today's or previous day's)
        asian_high, asian_low, asian_range_pips = self._build_asian_range(candles, pip)

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
            indicators={
                "asian_high": asian_high,
                "asian_low": asian_low,
                "asian_range_pips": asian_range_pips,
            }
        )

        if asian_high is None or asian_low is None:
            sig.rules_failed.append("no_asian_session_data")
            return sig

        # ── RULE 2: Range filter ────────────────────────────────────────────
        if asian_range_pips < RANGE_MIN_PIPS:
            sig.rules_failed.append(
                f"asian_range_too_narrow: {asian_range_pips:.1f}pips < {RANGE_MIN_PIPS}pips"
            )
            return sig
        if asian_range_pips > RANGE_MAX_PIPS:
            sig.rules_failed.append(
                f"asian_range_too_wide: {asian_range_pips:.1f}pips > {RANGE_MAX_PIPS}pips"
            )
            return sig
        sig.rules_passed.append(f"asian_range_ok: {asian_range_pips:.1f}pips")

        # ── RULE 3 + 4: London session breakout on current / last candle ─────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)

        # Must be in London session (07:00–16:00) and before kill time 12:00
        hour_utc = ts.hour
        if hour_utc < LONDON_SESSION_START_UTC or hour_utc >= LONDON_KILL_HOUR_UTC:
            sig.rules_failed.append(
                f"outside_london_window: current_hour={hour_utc}UTC "
                f"(allowed {LONDON_SESSION_START_UTC}:00–{LONDON_KILL_HOUR_UTC}:00)"
            )
            return sig
        sig.rules_passed.append(f"london_session_ok: hour={hour_utc}")

        # ATR(14) on all available candles
        atr_val = self._atr(candles, ATR_PERIOD)
        sig.indicators["atr14"] = atr_val

        candle_range = current["high"] - current["low"]
        close = current["close"]
        open_ = current["open"]
        body_size = abs(close - open_)
        body_ratio = (body_size / candle_range) if candle_range > 0 else 0.0

        sig.indicators["candle_range"] = candle_range
        sig.indicators["body_ratio"] = round(body_ratio, 4)
        sig.indicators["close"] = close

        direction = "NONE"

        # BUY trigger: close above Asian High
        if close > asian_high:
            direction = "BUY"
        # SELL trigger: close below Asian Low
        elif close < asian_low:
            direction = "SELL"
        else:
            sig.rules_failed.append("no_breakout: close inside asian range")
            return sig

        # ── RULE 3b: Body ≥ 50% ────────────────────────────────────────────
        if body_ratio < BODY_RATIO_MIN:
            sig.rules_failed.append(
                f"body_ratio_insufficient: {body_ratio:.3f} < {BODY_RATIO_MIN}"
            )
            return sig
        sig.rules_passed.append(f"body_ratio_ok: {body_ratio:.3f}")

        # ── RULE 4: ATR filter ─────────────────────────────────────────────
        atr_threshold = ATR_BREAKOUT_MULT * atr_val
        if candle_range < atr_threshold:
            sig.rules_failed.append(
                f"atr_filter_failed: candle_range={candle_range:.5f} "
                f"< {ATR_BREAKOUT_MULT}×ATR={atr_threshold:.5f}"
            )
            return sig
        sig.rules_passed.append(f"atr_filter_ok: range={candle_range:.5f}")

        # ── RULE 5 & 6: SL / TP ───────────────────────────────────────────
        sl_buf = SL_BUFFER_PIPS * pip
        if direction == "BUY":
            entry = close
            sl    = asian_low - sl_buf
            risk_dist = entry - sl
        else:
            entry = close
            sl    = asian_high + sl_buf
            risk_dist = sl - entry

        if risk_dist <= 0:
            sig.rules_failed.append(f"risk_dist_zero: entry={entry} sl={sl}")
            return sig

        tp = entry + TP_RR_RATIO * risk_dist if direction == "BUY" \
             else entry - TP_RR_RATIO * risk_dist
        rr = TP_RR_RATIO

        if rr < MIN_RR:
            sig.rules_failed.append(f"rr_insufficient: {rr:.2f} < {MIN_RR}")
            return sig
        sig.rules_passed.append(f"rr_ok: {rr:.2f}")

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction = direction
        sig.entry_price = round(entry, 5)
        sig.stop_loss   = round(sl, 5)
        sig.take_profit = round(tp, 5)
        sig.reward_to_risk = rr
        sig.timeframe = "M15"
        sig.session = "LONDON"
        sig.confidence = 0.75
        sig.is_valid = True

        logger.info(
            f"[AsianRangeBreakout] {symbol} {direction}: "
            f"entry={sig.entry_price} sl={sig.stop_loss} tp={sig.take_profit} "
            f"RR={rr:.2f} range={asian_range_pips:.1f}pips"
        )
        return sig

    # ------------------------------------------------------------------
    # INTERNAL: Build Asian range from candles
    # ------------------------------------------------------------------

    def _build_asian_range(
        self, candles: List[dict], pip: float
    ) -> tuple:
        """
        Scan candles for Asian session (00:00–08:00 UTC).
        Returns (asian_high, asian_low, range_in_pips).
        """
        asian_candles = []
        for c in candles:
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            if ASIAN_SESSION_START_UTC <= ts.hour < ASIAN_SESSION_END_UTC:
                asian_candles.append(c)

        if not asian_candles:
            return None, None, 0.0

        high = max(c["high"] for c in asian_candles)
        low  = min(c["low"]  for c in asian_candles)
        range_pips = (high - low) / pip
        return high, low, range_pips

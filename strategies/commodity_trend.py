"""
APEX MULTI-MARKET TJR ENGINE
COMMODITIES Strategy — EMA Stack Trend Following

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : COMMODITIES (USOIL, UKOIL)
STRATEGY    : EMA Stack Trend Following

SETUP REQUIREMENTS:
  1. EMA Stack definition (ALL three EMAs must be in correct order):
       EMA(8), EMA(21), EMA(55) computed on H1 candles

       For BUY  (bullish stack):
         EMA(8) > EMA(21) > EMA(55)
         Current close > EMA(8)
         ALL conditions must be true → BUY structure confirmed

       For SELL (bearish stack):
         EMA(8) < EMA(21) < EMA(55)
         Current close < EMA(8)
         ALL conditions must be true → SELL structure confirmed

       If stack is NOT in correct order → NO TRADE

  2. Pullback to EMA(21) entry:
       BUY  entry: price pulls back and closes at or near EMA(21)
                   Within 0.5 × ATR(14) of EMA(21)
       SELL entry: price pulls back and closes at or near EMA(21)
                   Within 0.5 × ATR(14) of EMA(21)

  3. ATR confirmation:
       ATR(14) on H1 candles
       Entry candle range (high − low) must be ≥ 0.3 × ATR(14)
         (Ensures candle has meaningful range, not a doji)

  4. STOP LOSS:
       BUY  SL = EMA(55) − (1.5 × ATR(14))
       SELL SL = EMA(55) + (1.5 × ATR(14))

  5. TAKE PROFIT:
       TP = entry ± 3.0 × ATR(14)
       Minimum R:R = 2.0

  6. EIA/OPEC NEWS BLOCK (HARD BLOCK — NO OVERRIDE):
       If EIA Weekly Petroleum Report is within ±2 hours → BLOCK ALL oil trades
       If OPEC meeting is scheduled today → BLOCK ALL oil trades
       These are detected via context["news_block"] flag

  7. Session restriction:
       USOIL valid: 14:30–20:00 UTC (CME oil session)
       UKOIL valid: 07:00–19:30 UTC (ICE Brent session)
       No new trades in final 30 minutes of session.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
EMA_FAST   = 8
EMA_MID    = 21
EMA_SLOW   = 55
ATR_PERIOD = 14

PULLBACK_ATR_MULT = 0.5    # entry within 0.5 × ATR of EMA(21)
MIN_RANGE_ATR_MULT = 0.3   # entry candle range ≥ 0.3 × ATR

SL_ATR_MULT = 1.5          # SL = EMA(55) ± 1.5 × ATR
TP_ATR_MULT = 3.0          # TP = entry ± 3.0 × ATR
MIN_RR      = 2.0

# Session windows (UTC): (start_h, start_m, end_h, end_m)
SESSION_WINDOWS: Dict[str, Tuple[int,int,int,int]] = {
    "USOIL": (14, 30, 20, 0),
    "UKOIL": (7,  0,  19, 30),
    "WTICO": (14, 30, 20, 0),   # Alternative symbol for WTI
    "BCO":   (7,  0,  19, 30),  # Brent
}
SESSION_CLOSE_BUFFER_MIN = 30   # no new entries in final 30 minutes
# ─────────────────────────────────────────────────────────────────────────────


class CommodityTrend(BaseStrategy):
    """
    Commodities: EMA Stack Trend Following with EIA/OPEC hard block.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "commodity_trend"

    @property
    def asset_class(self) -> str:
        return "COMMODITIES"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate commodity trend signal.

        candles: H1 OHLCV dicts (oldest first), UTC-aware timestamps.
        context: must include 'symbol'; optional 'news_block' (bool)
        """
        ctx = context or {}
        symbol = ctx.get("symbol", "UNKNOWN").upper()

        min_candles = EMA_SLOW + ATR_PERIOD + 5
        if len(candles) < min_candles:
            return self._no_signal(symbol, "insufficient_candles")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )

        # ── RULE 6: EIA/OPEC HARD BLOCK ────────────────────────────────────
        news_block = ctx.get("news_block", False)
        if news_block:
            sig.filter_blocks.append(
                "EIA_OPEC_NEWS_BLOCK: oil news event active — all trades blocked"
            )
            sig.rules_failed.append("news_block_active")
            return sig
        sig.rules_passed.append("news_block_clear")

        # ── RULE 7: Session check ──────────────────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        session_cfg = SESSION_WINDOWS.get(symbol)
        if session_cfg is None:
            sig.rules_failed.append(f"unknown_symbol: {symbol}")
            return sig

        s_h, s_m, e_h, e_m = session_cfg
        session_start = s_h * 60 + s_m
        session_end   = e_h * 60 + e_m
        kill_time     = session_end - SESSION_CLOSE_BUFFER_MIN
        current_time  = ts.hour * 60 + ts.minute

        if current_time < session_start or current_time >= session_end:
            sig.rules_failed.append(
                f"outside_session: {ts.strftime('%H:%M')}UTC not in "
                f"{s_h:02d}:{s_m:02d}–{e_h:02d}:{e_m:02d}"
            )
            return sig
        if current_time >= kill_time:
            sig.rules_failed.append(
                f"session_close_buffer: {ts.strftime('%H:%M')}UTC in final 30min"
            )
            return sig
        sig.rules_passed.append(f"session_ok: {ts.strftime('%H:%M')}UTC")

        # ── RULE 1: EMA Stack ─────────────────────────────────────────────
        closes = [c["close"] for c in candles]
        ema8_series  = self._ema(closes, EMA_FAST)
        ema21_series = self._ema(closes, EMA_MID)
        ema55_series = self._ema(closes, EMA_SLOW)
        ema8  = ema8_series[-1]
        ema21 = ema21_series[-1]
        ema55 = ema55_series[-1]
        close = current["close"]

        sig.indicators.update({
            "ema8":  ema8,
            "ema21": ema21,
            "ema55": ema55,
            "close": close,
        })

        # Determine direction from EMA stack — ALL conditions must hold
        bullish_stack = (ema8 > ema21 > ema55) and (close > ema8)
        bearish_stack = (ema8 < ema21 < ema55) and (close < ema8)

        if bullish_stack:
            direction = "BUY"
        elif bearish_stack:
            direction = "SELL"
        else:
            sig.rules_failed.append(
                f"ema_stack_not_aligned: ema8={ema8:.4f} ema21={ema21:.4f} "
                f"ema55={ema55:.4f} close={close:.4f}"
            )
            return sig
        sig.rules_passed.append(
            f"ema_stack_ok: {direction} ema8={ema8:.4f} ema21={ema21:.4f} ema55={ema55:.4f}"
        )

        # ── RULE 2: Pullback to EMA(21) ───────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sig.indicators["atr14"] = atr_val

        dist_to_ema21 = abs(close - ema21)
        pullback_threshold = PULLBACK_ATR_MULT * atr_val

        if dist_to_ema21 > pullback_threshold:
            sig.rules_failed.append(
                f"pullback_not_at_ema21: dist={dist_to_ema21:.4f} "
                f"> {PULLBACK_ATR_MULT}×ATR={pullback_threshold:.4f}"
            )
            return sig
        sig.rules_passed.append(
            f"pullback_ok: dist_to_ema21={dist_to_ema21:.4f} <= {pullback_threshold:.4f}"
        )

        # ── RULE 3: ATR range confirmation ────────────────────────────────
        candle_range = current["high"] - current["low"]
        min_range = MIN_RANGE_ATR_MULT * atr_val
        if candle_range < min_range:
            sig.rules_failed.append(
                f"candle_range_too_small: {candle_range:.4f} < {MIN_RANGE_ATR_MULT}×ATR={min_range:.4f}"
            )
            return sig
        sig.rules_passed.append(f"candle_range_ok: {candle_range:.4f}")

        # ── RULE 4 & 5: SL and TP ─────────────────────────────────────────
        entry = close
        if direction == "BUY":
            sl = ema55 - (SL_ATR_MULT * atr_val)
            tp = entry + (TP_ATR_MULT * atr_val)
        else:
            sl = ema55 + (SL_ATR_MULT * atr_val)
            tp = entry - (TP_ATR_MULT * atr_val)

        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            sig.rules_failed.append("risk_dist_zero")
            return sig

        rr = abs(tp - entry) / risk_dist
        if rr < MIN_RR:
            sig.rules_failed.append(f"rr_insufficient: {rr:.2f} < {MIN_RR}")
            return sig
        sig.rules_passed.append(f"rr_ok: {rr:.2f}")

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction   = direction
        sig.entry_price = round(entry, 4)
        sig.stop_loss   = round(sl, 4)
        sig.take_profit = round(tp, 4)
        sig.reward_to_risk = round(rr, 2)
        sig.timeframe = "H1"
        sig.session = "NEW_YORK"
        sig.confidence = 0.72
        sig.is_valid = True

        logger.info(
            f"[CommodityTrend] {symbol} {direction}: "
            f"entry={entry:.4f} sl={sl:.4f} tp={tp:.4f} "
            f"ema8/21/55={ema8:.4f}/{ema21:.4f}/{ema55:.4f} atr={atr_val:.4f}"
        )
        return sig

"""
APEX MULTI-MARKET TJR ENGINE
STOCKS Strategy — Gap and Go

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : STOCKS (e.g. AAPL, TSLA, NVDA, SPY, QQQ)
STRATEGY    : Gap and Go

SETUP REQUIREMENTS:
  1. Pre-market gap detection:
       Gap Up   : Today's open > yesterday's close
                  Gap % = (today_open − yesterday_close) / yesterday_close × 100
       Gap Down : Today's open < yesterday's close
                  Gap % = (yesterday_close − today_open) / yesterday_close × 100

       Minimum gap size: ≥ 0.5%
       Maximum gap size: ≤ 8.0%
         (Gaps > 8% likely news-driven with unpredictable behavior → reject)

  2. VWAP reclaim confirmation:
       For Gap Up   (BUY): price must reclaim VWAP
                            i.e., previous candle close < VWAP AND
                                  current candle close > VWAP
       For Gap Down (SELL): price must break below VWAP
                             i.e., previous candle close > VWAP AND
                                   current candle close < VWAP

  3. Volume confirmation:
       Current candle volume ≥ 1.5 × average volume of prior 10 candles
       (Volume surge confirms institutional participation)
       If volume < 1.5× average → REJECT

  4. Time restriction:
       Valid only 09:30–11:00 ET = 14:30–16:00 UTC
       No new entries at or after 11:00 ET = 16:00 UTC (time kill)

  5. STOP LOSS:
       BUY  SL = low of the VWAP reclaim candle − (0.5 × ATR(14))
       SELL SL = high of the VWAP break candle  + (0.5 × ATR(14))

  6. TAKE PROFIT:
       TP = entry ± 2.0 × risk distance
       Minimum R:R = 2.0

  7. One trade per stock per day.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
GAP_MIN_PCT     = 0.5     # minimum gap % to qualify
GAP_MAX_PCT     = 8.0     # maximum gap % (hard reject above this)
VOLUME_MULT     = 1.5     # current volume ≥ 1.5 × avg of prior 10 candles
VOL_LOOKBACK    = 10      # candles for volume average

# Session: 09:30–11:00 ET = 14:30–16:00 UTC
SESSION_START_UTC = (14, 30)
SESSION_END_UTC   = (16,  0)   # time kill: no new entries at/after this

ATR_PERIOD      = 14
SL_ATR_MULT     = 0.5
TP_RR_RATIO     = 2.0
MIN_RR          = 2.0
# ─────────────────────────────────────────────────────────────────────────────


class GapAndGo(BaseStrategy):
    """
    Stocks: Gap and Go with VWAP reclaim confirmation.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "gap_and_go"

    @property
    def asset_class(self) -> str:
        return "STOCKS"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate Gap and Go signal for a stock.

        candles : intraday 5-minute OHLCV dicts (oldest first), UTC-aware.
                  First candle should be the open candle (or premarket start).
        context : must include 'symbol', 'prev_close' (yesterday's closing price),
                  'today_open' (today's open price).
        """
        ctx = context or {}
        symbol    = ctx.get("symbol", "UNKNOWN").upper()
        prev_close = ctx.get("prev_close", 0.0)
        today_open = ctx.get("today_open", 0.0)

        if len(candles) < VOL_LOOKBACK + ATR_PERIOD + 2:
            return self._no_signal(symbol, "insufficient_candles")

        if prev_close <= 0 or today_open <= 0:
            return self._no_signal(symbol, "missing_gap_context: prev_close or today_open not provided")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )

        # ── RULE 4: Time restriction ───────────────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        t_min = ts.hour * 60 + ts.minute
        start = SESSION_START_UTC[0] * 60 + SESSION_START_UTC[1]
        end   = SESSION_END_UTC[0]   * 60 + SESSION_END_UTC[1]

        if t_min < start or t_min >= end:
            sig.rules_failed.append(
                f"outside_gap_and_go_window: {ts.strftime('%H:%M')}UTC not in "
                f"{SESSION_START_UTC[0]:02d}:{SESSION_START_UTC[1]:02d}–"
                f"{SESSION_END_UTC[0]:02d}:{SESSION_END_UTC[1]:02d}UTC"
            )
            return sig
        sig.rules_passed.append(f"session_time_ok: {ts.strftime('%H:%M')}UTC")

        # ── RULE 1: Gap detection ─────────────────────────────────────────
        if today_open > prev_close:
            gap_pct = (today_open - prev_close) / prev_close * 100
            gap_direction = "BUY"
        elif today_open < prev_close:
            gap_pct = (prev_close - today_open) / prev_close * 100
            gap_direction = "SELL"
        else:
            sig.rules_failed.append("no_gap: today_open == prev_close")
            return sig

        sig.indicators["gap_pct"] = round(gap_pct, 4)
        sig.indicators["gap_direction"] = gap_direction

        if gap_pct < GAP_MIN_PCT:
            sig.rules_failed.append(
                f"gap_too_small: {gap_pct:.2f}% < {GAP_MIN_PCT}%"
            )
            return sig
        if gap_pct > GAP_MAX_PCT:
            sig.filter_blocks.append(
                f"gap_too_large: {gap_pct:.2f}% > {GAP_MAX_PCT}% "
                "(likely news-driven — rejected)"
            )
            return sig
        sig.rules_passed.append(f"gap_ok: {gap_pct:.2f}% {gap_direction}")

        # ── RULE 2: VWAP reclaim / break ─────────────────────────────────
        session_candles = self._get_session_candles(candles)
        if len(session_candles) < 2:
            sig.rules_failed.append("insufficient_session_candles_for_vwap")
            return sig

        vwap = self._vwap(session_candles)
        sig.indicators["vwap"] = vwap

        if vwap == 0:
            sig.rules_failed.append("vwap_zero")
            return sig

        prev_c = candles[-2]
        cur_c  = candles[-1]

        vwap_reclaim = (prev_c["close"] < vwap) and (cur_c["close"] > vwap)
        vwap_break   = (prev_c["close"] > vwap) and (cur_c["close"] < vwap)

        if gap_direction == "BUY" and not vwap_reclaim:
            sig.rules_failed.append(
                f"vwap_reclaim_not_confirmed_for_BUY: "
                f"prev_close={prev_c['close']:.4f} cur_close={cur_c['close']:.4f} vwap={vwap:.4f}"
            )
            return sig
        if gap_direction == "SELL" and not vwap_break:
            sig.rules_failed.append(
                f"vwap_break_not_confirmed_for_SELL: "
                f"prev_close={prev_c['close']:.4f} cur_close={cur_c['close']:.4f} vwap={vwap:.4f}"
            )
            return sig
        sig.rules_passed.append(f"vwap_confirmation_ok: direction={gap_direction}")

        # ── RULE 3: Volume confirmation ────────────────────────────────────
        cur_volume  = cur_c.get("volume", 0.0)
        avg_volumes = [c.get("volume", 0.0) for c in candles[-VOL_LOOKBACK - 1: -1]]
        avg_vol     = sum(avg_volumes) / len(avg_volumes) if avg_volumes else 0.0
        vol_ratio   = cur_volume / avg_vol if avg_vol > 0 else 0.0
        sig.indicators["volume_ratio"] = round(vol_ratio, 2)
        sig.indicators["avg_volume"]   = avg_vol

        if vol_ratio < VOLUME_MULT:
            sig.rules_failed.append(
                f"volume_insufficient: ratio={vol_ratio:.2f} < {VOLUME_MULT}"
            )
            return sig
        sig.rules_passed.append(f"volume_ok: ratio={vol_ratio:.2f}")

        # ── RULE 5 & 6: SL and TP ─────────────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sl_buf  = SL_ATR_MULT * atr_val
        entry   = cur_c["close"]

        if gap_direction == "BUY":
            sl = cur_c["low"] - sl_buf
        else:
            sl = cur_c["high"] + sl_buf

        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            sig.rules_failed.append("risk_dist_zero")
            return sig

        tp = entry + TP_RR_RATIO * risk_dist if gap_direction == "BUY" \
             else entry - TP_RR_RATIO * risk_dist
        rr = TP_RR_RATIO

        if rr < MIN_RR:
            sig.rules_failed.append(f"rr_insufficient: {rr:.2f} < {MIN_RR}")
            return sig
        sig.rules_passed.append(f"rr_ok: {rr:.2f}")

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction   = gap_direction
        sig.entry_price = round(entry, 4)
        sig.stop_loss   = round(sl, 4)
        sig.take_profit = round(tp, 4)
        sig.reward_to_risk = rr
        sig.timeframe = "M5"
        sig.session = "NEW_YORK"
        sig.confidence = 0.76
        sig.is_valid = True
        sig.indicators["atr14"] = atr_val

        logger.info(
            f"[GapAndGo] {symbol} {gap_direction}: "
            f"gap={gap_pct:.2f}% entry={entry:.4f} sl={sl:.4f} tp={tp:.4f} "
            f"vwap={vwap:.4f} vol_ratio={vol_ratio:.2f}"
        )
        return sig

    # ------------------------------------------------------------------
    def _get_session_candles(self, candles: List[dict]) -> List[dict]:
        """Return only candles within the market session (14:30+ UTC)."""
        result = []
        start = SESSION_START_UTC[0] * 60 + SESSION_START_UTC[1]
        for c in candles:
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts.hour * 60 + ts.minute >= start:
                result.append(c)
        return result

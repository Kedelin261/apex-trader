"""
APEX MULTI-MARKET TJR ENGINE
INDICES Strategy — VWAP + Standard Deviation Band Reversion

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : INDICES (US30, US500, NAS100, GER40, UK100)
STRATEGY    : VWAP Standard-Deviation Band Reversion

SETUP REQUIREMENTS:
  1. VWAP and SD bands computed from session open:
       Session VWAP = volume-weighted average price from session open
       SD1 Upper = VWAP + 1×SD
       SD1 Lower = VWAP − 1×SD
       SD2 Upper = VWAP + 2×SD
       SD2 Lower = VWAP − 2×SD
       SD = volume-weighted standard deviation of typical price from VWAP

  2. Entry trigger:
       BUY  setup : price touches or crosses BELOW SD2 Lower
                    AND closes back ABOVE SD2 Lower on next candle
       SELL setup : price touches or crosses ABOVE SD2 Upper
                    AND closes back BELOW SD2 Upper on next candle

  3. EMA trend filter (MANDATORY — hard gate):
       EMA(20) and EMA(50) computed on session candles
       For BUY  : EMA(20) must be ABOVE EMA(50)  [bullish structure]
       For SELL : EMA(20) must be BELOW EMA(50)  [bearish structure]
       If EMA structure contradicts direction → REJECT

  4. RSI filter:
       RSI(14) on session closes
       For BUY  entries: RSI must be < 40  [oversold region]
       For SELL entries: RSI must be > 60  [overbought region]
       If RSI outside these bounds → REJECT

  5. STOP LOSS:
       BUY  SL = lowest low of prior 3 candles − (0.1 × ATR(14))
       SELL SL = highest high of prior 3 candles + (0.1 × ATR(14))

  6. TAKE PROFIT:
       First target  : VWAP (scale out 50% of position)
       Second target : SD1 band on opposite side (exit remaining 50%)
       Minimum R:R to first target ≥ 1.5

  7. Time restrictions:
       Valid only during official market hours for the index:
       US indices (US30, US500, NAS100): 14:30–21:00 UTC
       EU indices (GER40): 07:00–15:30 UTC
       UK indices (UK100): 08:00–16:30 UTC

  8. No new entries in final 30 minutes of session.

SCALING EXITS (EXACT):
  At TP1 (VWAP)     → close 50% of position
  At TP2 (SD1 opp.) → close remaining 50%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
EMA_FAST_PERIOD = 20
EMA_SLOW_PERIOD = 50
RSI_PERIOD      = 14
RSI_BUY_MAX     = 40      # RSI < 40 for BUY (oversold)
RSI_SELL_MIN    = 60      # RSI > 60 for SELL (overbought)
ATR_PERIOD      = 14
ATR_SL_MULT     = 0.1
SL_LOOKBACK     = 3       # candles for SL placement
SD_ENTRY_BAND   = 2       # touch SD2 to trigger
MIN_RR_TP1      = 1.5     # minimum RR to first target (VWAP)
TP1_SCALE_PCT   = 50      # % to close at TP1
TP2_SCALE_PCT   = 50      # % to close at TP2

# Session windows (UTC): start_hour, start_min, end_hour, end_min, close_buffer_min
# close_buffer_min: no new entries in final N minutes
INDEX_SESSIONS: Dict[str, Tuple[int,int,int,int,int]] = {
    "US30":   (14, 30, 21, 0,  30),
    "US500":  (14, 30, 21, 0,  30),
    "NAS100": (14, 30, 21, 0,  30),
    "GER40":  (7,  0,  15, 30, 30),
    "UK100":  (8,  0,  16, 30, 30),
}
# ─────────────────────────────────────────────────────────────────────────────


class VWAPSDReversion(BaseStrategy):
    """
    Indices: VWAP + SD Band Reversion strategy.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "vwap_sd_reversion"

    @property
    def asset_class(self) -> str:
        return "INDICES"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate VWAP SD reversion signal.

        candles: 5-minute OHLCV dicts (oldest first), UTC-aware timestamps.
        context: must include 'symbol'
        """
        ctx = context or {}
        symbol = ctx.get("symbol", "UNKNOWN").upper()

        min_candles = max(EMA_SLOW_PERIOD, RSI_PERIOD, ATR_PERIOD) + 5
        if len(candles) < min_candles:
            return self._no_signal(symbol, "insufficient_candles")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )

        # ── RULE 7: Time restriction check ────────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        session_cfg = INDEX_SESSIONS.get(symbol)
        if session_cfg is None:
            sig.rules_failed.append(f"unknown_index_symbol: {symbol}")
            return sig

        s_h, s_m, e_h, e_m, buf = session_cfg
        session_start_min = s_h * 60 + s_m
        session_end_min   = e_h * 60 + e_m
        kill_min          = session_end_min - buf
        current_min       = ts.hour * 60 + ts.minute

        if current_min < session_start_min or current_min >= session_end_min:
            sig.rules_failed.append(
                f"outside_session: {ts.strftime('%H:%M')}UTC not in "
                f"{s_h:02d}:{s_m:02d}–{e_h:02d}:{e_m:02d}"
            )
            return sig
        if current_min >= kill_min:
            sig.rules_failed.append(
                f"final_30min_kill: {ts.strftime('%H:%M')}UTC >= kill at "
                f"{kill_min//60:02d}:{kill_min%60:02d}"
            )
            return sig
        sig.rules_passed.append(f"session_time_ok: {ts.strftime('%H:%M')}UTC")

        # ── RULE 1: Compute VWAP and SD bands ─────────────────────────────
        session_candles = self._filter_session_candles(candles, s_h, s_m)
        if len(session_candles) < 3:
            sig.rules_failed.append("insufficient_session_candles")
            return sig

        vwap = self._vwap(session_candles)
        sd   = self._vwap_std(session_candles, vwap)
        sd2_upper = vwap + 2 * sd
        sd2_lower = vwap - 2 * sd
        sd1_upper = vwap + 1 * sd
        sd1_lower = vwap - 1 * sd

        sig.indicators.update({
            "vwap": vwap,
            "sd": sd,
            "sd2_upper": sd2_upper,
            "sd2_lower": sd2_lower,
            "sd1_upper": sd1_upper,
            "sd1_lower": sd1_lower,
        })

        if sd == 0:
            sig.rules_failed.append("sd_zero: no volume data")
            return sig

        # ── RULE 2: Entry trigger ─────────────────────────────────────────
        prev  = candles[-2] if len(candles) >= 2 else None
        close = current["close"]
        sig.indicators["close"] = close

        direction = "NONE"
        if prev is not None:
            prev_low  = prev["low"]
            prev_high = prev["high"]
            prev_close = prev["close"]
            # BUY: previous candle touched/crossed SD2 Lower AND current candle
            # closes back above SD2 Lower
            if prev_low <= sd2_lower and close > sd2_lower:
                direction = "BUY"
            # SELL: previous candle touched/crossed SD2 Upper AND current candle
            # closes back below SD2 Upper
            elif prev_high >= sd2_upper and close < sd2_upper:
                direction = "SELL"

        if direction == "NONE":
            sig.rules_failed.append(
                f"no_sd2_touch: close={close} bands=[{sd2_lower:.4f},{sd2_upper:.4f}]"
            )
            return sig
        sig.rules_passed.append(f"sd2_touch_ok: direction={direction}")

        # ── RULE 3: EMA trend filter (MANDATORY) ──────────────────────────
        closes = [c["close"] for c in session_candles]
        ema20_series = self._ema(closes, EMA_FAST_PERIOD)
        ema50_series = self._ema(closes, EMA_SLOW_PERIOD)
        ema20 = ema20_series[-1]
        ema50 = ema50_series[-1]
        sig.indicators["ema20"] = ema20
        sig.indicators["ema50"] = ema50

        if direction == "BUY" and ema20 <= ema50:
            sig.rules_failed.append(
                f"ema_filter_rejected_BUY: ema20={ema20:.4f} <= ema50={ema50:.4f}"
            )
            return sig
        if direction == "SELL" and ema20 >= ema50:
            sig.rules_failed.append(
                f"ema_filter_rejected_SELL: ema20={ema20:.4f} >= ema50={ema50:.4f}"
            )
            return sig
        sig.rules_passed.append(f"ema_filter_ok: ema20={ema20:.4f} ema50={ema50:.4f}")

        # ── RULE 4: RSI filter ────────────────────────────────────────────
        all_closes = [c["close"] for c in candles]
        rsi_series = self._rsi(all_closes, RSI_PERIOD)
        rsi_val = rsi_series[-1]
        sig.indicators["rsi14"] = rsi_val

        if direction == "BUY" and rsi_val >= RSI_BUY_MAX:
            sig.rules_failed.append(
                f"rsi_filter_rejected_BUY: rsi={rsi_val:.1f} >= {RSI_BUY_MAX}"
            )
            return sig
        if direction == "SELL" and rsi_val <= RSI_SELL_MIN:
            sig.rules_failed.append(
                f"rsi_filter_rejected_SELL: rsi={rsi_val:.1f} <= {RSI_SELL_MIN}"
            )
            return sig
        sig.rules_passed.append(f"rsi_filter_ok: rsi={rsi_val:.1f}")

        # ── RULE 5: SL placement ─────────────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sl_buf  = ATR_SL_MULT * atr_val
        recent  = candles[-SL_LOOKBACK:]

        if direction == "BUY":
            sl = min(c["low"] for c in recent) - sl_buf
            entry = close
        else:
            sl = max(c["high"] for c in recent) + sl_buf
            entry = close

        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            sig.rules_failed.append("risk_dist_zero")
            return sig

        # ── RULE 6: TP1 = VWAP, TP2 = SD1 opposite band ─────────────────
        tp1 = vwap          # first target: VWAP (50% exit)
        tp2 = sd1_upper if direction == "BUY" else sd1_lower   # 50% exit

        rr_tp1 = abs(tp1 - entry) / risk_dist

        if rr_tp1 < MIN_RR_TP1:
            sig.rules_failed.append(
                f"rr_tp1_insufficient: {rr_tp1:.2f} < {MIN_RR_TP1}"
            )
            return sig
        sig.rules_passed.append(f"rr_tp1_ok: {rr_tp1:.2f}")

        rr_tp2 = abs(tp2 - entry) / risk_dist

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction   = direction
        sig.entry_price = round(entry, 4)
        sig.stop_loss   = round(sl, 4)
        sig.take_profit = round(tp2, 4)    # Use TP2 as the headline TP
        sig.reward_to_risk = round(rr_tp2, 2)
        sig.timeframe = "M5"
        sig.session = "NEW_YORK"
        sig.confidence = 0.78
        sig.is_valid = True
        sig.indicators["tp1_vwap"] = round(tp1, 4)
        sig.indicators["tp2_sd1"]  = round(tp2, 4)
        sig.indicators["rr_tp1"]   = round(rr_tp1, 2)
        sig.indicators["rr_tp2"]   = round(rr_tp2, 2)
        sig.indicators["tp1_scale_pct"] = TP1_SCALE_PCT
        sig.indicators["tp2_scale_pct"] = TP2_SCALE_PCT
        sig.indicators["atr14"] = atr_val

        logger.info(
            f"[VWAPSDReversion] {symbol} {direction}: "
            f"entry={entry:.4f} sl={sl:.4f} tp1={tp1:.4f} tp2={tp2:.4f} "
            f"vwap={vwap:.4f} rsi={rsi_val:.1f} ema20/50={ema20:.4f}/{ema50:.4f}"
        )
        return sig

    # ------------------------------------------------------------------
    def _filter_session_candles(
        self, candles: List[dict], start_h: int, start_m: int
    ) -> List[dict]:
        """Filter candles to session start onward."""
        result = []
        for c in candles:
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            t_min = ts.hour * 60 + ts.minute
            if t_min >= start_h * 60 + start_m:
                result.append(c)
        return result

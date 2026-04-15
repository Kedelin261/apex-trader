"""
APEX MULTI-MARKET TJR ENGINE
FUTURES Strategy — Opening Range Breakout with VWAP Filter (ORB + VWAP)

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : FUTURES (ES, NQ, GC)
STRATEGY    : Opening Range Breakout + VWAP Filter

SETUP REQUIREMENTS:
  1. Opening Range (OR) definition:
       OR = first 30 minutes after CME session open
       CME Regular Session open = 09:30 ET = 14:30 UTC
       OR High = highest high between 14:30–15:00 UTC
       OR Low  = lowest low  between 14:30–15:00 UTC

  2. Breakout trigger (after 15:00 UTC):
       BUY  trigger : price closes ABOVE OR High
       SELL trigger : price closes BELOW OR Low

  3. VWAP filter (MANDATORY — hard gate):
       For BUY  entries: close must be > session VWAP
       For SELL entries: close must be < session VWAP
       Session VWAP computed from 14:30 UTC candles onward
       If price is on wrong side of VWAP → REJECT

  4. ATR buffer on entry:
       Entry price = breakout level + (0.3 × ATR(14))  for BUY
       Entry price = breakout level − (0.3 × ATR(14))  for SELL

  5. STOP LOSS:
       BUY  SL = OR Low  − (0.3 × ATR(14))
       SELL SL = OR High + (0.3 × ATR(14))

  6. TAKE PROFIT:
       TP = entry ± 2.0 × risk distance
       Minimum R:R = 2.0

  7. Time kill:
       No new entries AT or AFTER 19:00 UTC (4:00 PM ET)
       Any open setups after kill time are abandoned

  8. One trade per instrument per session.

  9. Re-entry:
       If stop hit and signal re-triggers → allowed once (max 2 entries/session)

INSTRUMENTS:
  ES  (S&P 500 Futures)
  NQ  (NASDAQ 100 Futures)
  GC  (Gold Futures — separate from XAUUSD spot)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
# CME regular session open in UTC
OR_START_HOUR_UTC  = 14           # 14:30 UTC = 09:30 ET
OR_START_MINUTE    = 30
OR_END_HOUR_UTC    = 15           # 15:00 UTC (30-minute OR window)
OR_END_MINUTE      = 0

KILL_HOUR_UTC      = 19           # No new entries at/after 19:00 UTC (4 PM ET)
KILL_MINUTE        = 0

ATR_PERIOD         = 14
ATR_BUFFER_MULT    = 0.3          # Entry and SL buffer = 0.3 × ATR
TP_RR_RATIO        = 2.0          # Take profit = 2.0 × risk
MIN_RR             = 2.0
MAX_ENTRIES_PER_SESSION = 2
# ─────────────────────────────────────────────────────────────────────────────

# IBKR futures contract mapping
FUTURES_BROKER_SYMBOLS: Dict[str, str] = {
    "ES":  "ES",    # S&P 500 E-mini
    "NQ":  "NQ",    # NASDAQ 100 E-mini
    "GC":  "GC",    # Gold futures (100 oz contract)
    "YM":  "YM",    # Dow Jones E-mini
    "CL":  "CL",    # WTI Crude Oil
}

# Tick size per futures instrument (for reference in downstream risk calc)
FUTURES_TICK_SIZE: Dict[str, float] = {
    "ES": 0.25,   # 1 point = $50; tick = $12.50
    "NQ": 0.25,   # 1 point = $20; tick = $5.00
    "GC": 0.10,   # 1 point = $100; tick = $10.00
    "YM": 1.0,
    "CL": 0.01,
}


class ORBWithVWAP(BaseStrategy):
    """
    Futures: Opening Range Breakout with VWAP filter.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "orb_vwap"

    @property
    def asset_class(self) -> str:
        return "FUTURES"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate ORB+VWAP signal for a futures instrument.

        candles: 5-minute OHLCV dicts (oldest first), UTC-aware timestamps.
        context: must include 'symbol'
        """
        ctx = context or {}
        symbol = ctx.get("symbol", "UNKNOWN").upper()

        if len(candles) < ATR_PERIOD + 2:
            return self._no_signal(symbol, "insufficient_candles")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
            broker_symbol=FUTURES_BROKER_SYMBOLS.get(symbol, symbol),
        )

        # ── RULE 1: Opening Range ───────────────────────────────────────────
        or_range = self._opening_range_futures(candles)
        if or_range is None:
            sig.rules_failed.append("no_opening_range_data: OR window 14:30–15:00 UTC not found")
            return sig

        or_high = or_range["high"]
        or_low  = or_range["low"]
        sig.indicators["or_high"] = or_high
        sig.indicators["or_low"]  = or_low

        # ── Time kill check on CURRENT candle ──────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)

        kill_minutes = KILL_HOUR_UTC * 60 + KILL_MINUTE
        current_minutes = ts.hour * 60 + ts.minute
        if current_minutes >= kill_minutes:
            sig.rules_failed.append(
                f"time_kill: current={ts.strftime('%H:%M')}UTC >= {KILL_HOUR_UTC:02d}:00UTC"
            )
            return sig
        sig.rules_passed.append(f"time_ok: {ts.strftime('%H:%M')}UTC")

        # Must be after OR closes (after 15:00 UTC)
        or_end_minutes = OR_END_HOUR_UTC * 60 + OR_END_MINUTE
        if current_minutes < or_end_minutes:
            sig.rules_failed.append(
                f"or_not_complete_yet: current={ts.strftime('%H:%M')}UTC"
            )
            return sig
        sig.rules_passed.append("or_complete")

        close = current["close"]
        sig.indicators["close"] = close

        # ── RULE 2: Breakout direction ──────────────────────────────────────
        direction = "NONE"
        if close > or_high:
            direction = "BUY"
        elif close < or_low:
            direction = "SELL"
        else:
            sig.rules_failed.append(
                f"no_breakout: close={close} inside OR [{or_low},{or_high}]"
            )
            return sig

        # ── RULE 3: VWAP filter (MANDATORY HARD GATE) ──────────────────────
        session_candles = self._session_candles_futures(candles)
        if not session_candles:
            sig.rules_failed.append("vwap_calculation_failed: no session candles")
            return sig

        vwap = self._vwap(session_candles)
        sig.indicators["vwap"] = vwap

        if vwap == 0.0:
            sig.rules_failed.append("vwap_zero: cannot compute VWAP")
            return sig

        if direction == "BUY" and close <= vwap:
            sig.rules_failed.append(
                f"vwap_filter_rejected_BUY: close={close} <= vwap={vwap}"
            )
            return sig
        if direction == "SELL" and close >= vwap:
            sig.rules_failed.append(
                f"vwap_filter_rejected_SELL: close={close} >= vwap={vwap}"
            )
            return sig
        sig.rules_passed.append(f"vwap_filter_ok: {direction} close={close} vwap={vwap:.4f}")

        # ── RULE 4 & 5: ATR buffer ─────────────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sig.indicators["atr14"] = atr_val
        buf = ATR_BUFFER_MULT * atr_val

        if direction == "BUY":
            entry = or_high + buf
            sl    = or_low  - buf
        else:
            entry = or_low  - buf
            sl    = or_high + buf

        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            sig.rules_failed.append("risk_dist_zero")
            return sig

        # ── RULE 6: TP ─────────────────────────────────────────────────────
        tp = entry + TP_RR_RATIO * risk_dist if direction == "BUY" \
             else entry - TP_RR_RATIO * risk_dist
        rr = TP_RR_RATIO

        if rr < MIN_RR:
            sig.rules_failed.append(f"rr_insufficient: {rr} < {MIN_RR}")
            return sig
        sig.rules_passed.append(f"rr_ok: {rr:.2f}")

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction = direction
        sig.entry_price = round(entry, 4)
        sig.stop_loss   = round(sl, 4)
        sig.take_profit = round(tp, 4)
        sig.reward_to_risk = rr
        sig.timeframe = "M5"
        sig.session = "NEW_YORK"
        sig.confidence = 0.80
        sig.is_valid = True

        logger.info(
            f"[ORBWithVWAP] {symbol} {direction}: "
            f"entry={sig.entry_price} sl={sig.stop_loss} tp={sig.take_profit} "
            f"OR=[{or_low},{or_high}] VWAP={vwap:.4f} ATR={atr_val:.4f}"
        )
        return sig

    # ------------------------------------------------------------------
    # INTERNAL
    # ------------------------------------------------------------------

    def _opening_range_futures(self, candles: List[dict]) -> Optional[dict]:
        """
        Build OR from 14:30–15:00 UTC candles.
        Returns {"high": float, "low": float} or None.
        """
        or_candles = []
        for c in candles:
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            h, m = ts.hour, ts.minute
            t_min = h * 60 + m
            start = OR_START_HOUR_UTC * 60 + OR_START_MINUTE   # 14:30
            end   = OR_END_HOUR_UTC   * 60 + OR_END_MINUTE     # 15:00
            if start <= t_min < end:
                or_candles.append(c)
        if not or_candles:
            return None
        return {
            "high": max(c["high"] for c in or_candles),
            "low":  min(c["low"]  for c in or_candles),
        }

    def _session_candles_futures(self, candles: List[dict]) -> List[dict]:
        """Return candles from 14:30 UTC onward (CME session)."""
        result = []
        for c in candles:
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            t_min = ts.hour * 60 + ts.minute
            start = OR_START_HOUR_UTC * 60 + OR_START_MINUTE
            if t_min >= start:
                result.append(c)
        return result

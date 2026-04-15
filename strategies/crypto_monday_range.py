"""
APEX MULTI-MARKET TJR ENGINE
CRYPTO Strategy 2 — Monday Range Breakout

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : CRYPTO (BTCUSD, ETHUSD, etc.)
STRATEGY    : Monday Range Breakout

SETUP REQUIREMENTS:
  1. Monday range definition:
       Monday range = entire price action from Monday 00:00 UTC to Monday 23:59 UTC
       Monday High = highest high during Monday
       Monday Low  = lowest low during Monday

  2. Breakout trigger (Tuesday onward):
       BUY  trigger : first 4-hour candle that closes ABOVE Monday High
       SELL trigger : first 4-hour candle that closes BELOW Monday Low
       Trigger is valid ONLY Tuesday through Thursday (UTC)
       If breakout does not occur by Thursday 23:59 UTC → setup expires

  3. ATR confirmation:
       ATR(14) on H4 candles
       Breakout candle range (high − low) must be ≥ 0.5 × ATR(14)

  4. RSI filter:
       RSI(14) on H4 closes
       For BUY  entries: RSI must be > 50 (momentum confirmation)
       For SELL entries: RSI must be < 50
       If RSI does not confirm → REJECT

  5. STOP LOSS:
       BUY  SL = Monday Low  − (1.0 × ATR(14))
       SELL SL = Monday High + (1.0 × ATR(14))

  6. TAKE PROFIT:
       TP = entry ± 2.0 × (entry − SL)
       Minimum R:R = 2.0

  7. Time restriction:
       Valid ONLY on Tuesday, Wednesday, Thursday (UTC weekdays 1, 2, 3)
       Monday (0) and Friday (4)–Sunday (6) → NO TRADE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
VALID_WEEKDAYS  = {1, 2, 3}    # Tuesday=1, Wednesday=2, Thursday=3
MONDAY_WEEKDAY  = 0

ATR_PERIOD      = 14
ATR_RANGE_MULT  = 0.5          # breakout candle range ≥ 0.5 × ATR

RSI_PERIOD      = 14
RSI_BUY_MIN     = 50           # RSI > 50 for BUY
RSI_SELL_MAX    = 50           # RSI < 50 for SELL

SL_ATR_MULT     = 1.0          # SL = Monday boundary ± 1.0 × ATR
TP_RR_RATIO     = 2.0
MIN_RR          = 2.0
# ─────────────────────────────────────────────────────────────────────────────


class CryptoMondayRange(BaseStrategy):
    """
    Crypto: Monday Range Breakout on H4 candles.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "crypto_monday_range"

    @property
    def asset_class(self) -> str:
        return "CRYPTO"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate Monday Range Breakout signal.

        candles : H4 OHLCV dicts (oldest first), UTC-aware timestamps.
        context : must include 'symbol'
        """
        ctx = context or {}
        symbol = ctx.get("symbol", "UNKNOWN").upper()

        min_candles = ATR_PERIOD + RSI_PERIOD + 10
        if len(candles) < min_candles:
            return self._no_signal(symbol, "insufficient_candles")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )

        # ── RULE 7: Weekday restriction ────────────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        weekday = ts.weekday()   # Monday=0, Sunday=6

        if weekday not in VALID_WEEKDAYS:
            sig.rules_failed.append(
                f"weekday_invalid: weekday={weekday} "
                f"(valid: Tue=1, Wed=2, Thu=3)"
            )
            return sig
        sig.rules_passed.append(f"weekday_ok: {weekday}")

        # ── RULE 1: Build Monday range ────────────────────────────────────
        monday_high, monday_low = self._monday_range(candles)
        if monday_high is None or monday_low is None:
            sig.rules_failed.append("no_monday_range: cannot find Monday candles")
            return sig

        sig.indicators["monday_high"] = monday_high
        sig.indicators["monday_low"]  = monday_low

        # ── RULE 2: Breakout trigger ───────────────────────────────────────
        close = current["close"]
        sig.indicators["close"] = close

        direction = "NONE"
        if close > monday_high:
            direction = "BUY"
        elif close < monday_low:
            direction = "SELL"
        else:
            sig.rules_failed.append(
                f"no_breakout: close={close} inside Monday range "
                f"[{monday_low},{monday_high}]"
            )
            return sig
        sig.rules_passed.append(f"breakout_ok: {direction}")

        # ── RULE 3: ATR range confirmation ────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sig.indicators["atr14"] = atr_val

        candle_range = current["high"] - current["low"]
        min_range    = ATR_RANGE_MULT * atr_val
        if candle_range < min_range:
            sig.rules_failed.append(
                f"atr_range_insufficient: range={candle_range:.4f} "
                f"< {ATR_RANGE_MULT}×ATR={min_range:.4f}"
            )
            return sig
        sig.rules_passed.append(f"atr_range_ok: {candle_range:.4f}")

        # ── RULE 4: RSI filter ────────────────────────────────────────────
        closes     = [c["close"] for c in candles]
        rsi_series = self._rsi(closes, RSI_PERIOD)
        rsi_val    = rsi_series[-1]
        sig.indicators["rsi14"] = round(rsi_val, 2)

        if direction == "BUY" and rsi_val <= RSI_BUY_MIN:
            sig.rules_failed.append(
                f"rsi_filter_rejected_BUY: rsi={rsi_val:.1f} <= {RSI_BUY_MIN}"
            )
            return sig
        if direction == "SELL" and rsi_val >= RSI_SELL_MAX:
            sig.rules_failed.append(
                f"rsi_filter_rejected_SELL: rsi={rsi_val:.1f} >= {RSI_SELL_MAX}"
            )
            return sig
        sig.rules_passed.append(f"rsi_filter_ok: rsi={rsi_val:.1f}")

        # ── RULE 5 & 6: SL and TP ─────────────────────────────────────────
        entry = close
        if direction == "BUY":
            sl = monday_low - (SL_ATR_MULT * atr_val)
            tp = entry + TP_RR_RATIO * (entry - sl)
        else:
            sl = monday_high + (SL_ATR_MULT * atr_val)
            tp = entry - TP_RR_RATIO * (sl - entry)

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
        sig.timeframe = "H4"
        sig.session = "CRYPTO_24_7"
        sig.confidence = 0.72
        sig.is_valid = True

        logger.info(
            f"[CryptoMondayRange] {symbol} {direction}: "
            f"monday=[{monday_low},{monday_high}] close={close:.4f} "
            f"entry={entry:.4f} sl={sl:.4f} tp={tp:.4f} rsi={rsi_val:.1f}"
        )
        return sig

    # ------------------------------------------------------------------
    def _monday_range(self, candles: List[dict]) -> tuple:
        """
        Find the most recent Monday's candles and compute H/L.
        Scans backward until Monday candles are found.
        """
        monday_candles = []
        monday_date = None
        for c in reversed(candles):
            ts: datetime = c["timestamp"]
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts.weekday() == MONDAY_WEEKDAY:
                d = ts.date()
                if monday_date is None:
                    monday_date = d
                if ts.date() == monday_date:
                    monday_candles.append(c)
                else:
                    break   # Different Monday — stop

        if not monday_candles:
            return None, None

        return (
            max(c["high"] for c in monday_candles),
            min(c["low"]  for c in monday_candles),
        )

"""
APEX MULTI-MARKET TJR ENGINE
CRYPTO Strategy 1 — Funding Rate Reversion

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : CRYPTO (BTCUSD, ETHUSD, etc.)
STRATEGY    : Funding Rate Reversion

SETUP REQUIREMENTS:
  1. Funding rate conditions (HARD GATES):
       For BUY  (funding positive extreme = longs overpaying):
         Funding rate ≥ +0.08%  → market is extremely long-biased → reversion SHORT
         (Counter-intuitive: extreme positive funding → SELL signal)
       For SELL (funding negative extreme = shorts overpaying):
         Funding rate ≤ −0.05%  → market is extremely short-biased → reversion LONG
         (Counter-intuitive: extreme negative funding → BUY signal)

       NOTE: Funding reversion is a MEAN-REVERSION strategy:
         Extreme POSITIVE funding → SHORT (SELL)
         Extreme NEGATIVE funding → LONG (BUY)

  2. RSI filter:
       RSI(14) on 1-hour candles
       For BUY  (funding extreme negative → going long):
         RSI must be < 35  (oversold; price also weak)
       For SELL (funding extreme positive → going short):
         RSI must be > 65  (overbought; price also strong)
       If RSI does not confirm → REJECT

  3. Time window:
       Funding rates update every 8 hours: 00:00, 08:00, 16:00 UTC
       Valid entry window: within 2 hours AFTER each funding rate update
       Valid windows: 00:00–02:00 UTC, 08:00–10:00 UTC, 16:00–18:00 UTC
       Outside these windows → NO TRADE

  4. STOP LOSS:
       BUY  SL = entry − (2.0 × ATR(14))
       SELL SL = entry + (2.0 × ATR(14))

  5. TAKE PROFIT:
       TP = entry ± 3.0 × ATR(14)
       Minimum R:R = 1.5

  6. Maximum position cap:
       Max 2% of account balance per trade (hard cap regardless of risk calc)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
FUNDING_POSITIVE_EXTREME = 0.08    # ≥ +0.08% → SELL signal (extreme long bias)
FUNDING_NEGATIVE_EXTREME = -0.05   # ≤ −0.05% → BUY  signal (extreme short bias)

RSI_PERIOD     = 14
RSI_BUY_MAX    = 35     # RSI < 35 for BUY
RSI_SELL_MIN   = 65     # RSI > 65 for SELL

# Valid entry hours after funding update (00, 08, 16 UTC)
FUNDING_UPDATE_HOURS = [0, 8, 16]
ENTRY_WINDOW_HOURS   = 2     # valid within 2 hours after update

ATR_PERIOD     = 14
SL_ATR_MULT    = 2.0
TP_ATR_MULT    = 3.0
MIN_RR         = 1.5
MAX_RISK_PCT   = 2.0           # hard cap: 2% of account balance per trade
# ─────────────────────────────────────────────────────────────────────────────


class CryptoFundingReversion(BaseStrategy):
    """
    Crypto: Funding Rate Reversion.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "crypto_funding_reversion"

    @property
    def asset_class(self) -> str:
        return "CRYPTO"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate crypto funding rate reversion signal.

        candles : 1-hour OHLCV dicts (oldest first), UTC-aware.
        context : must include:
                    'symbol'       (e.g. 'BTCUSD')
                    'funding_rate' (float, e.g. 0.0008 = 0.08%)
        """
        ctx = context or {}
        symbol       = ctx.get("symbol", "UNKNOWN").upper()
        funding_rate = ctx.get("funding_rate", None)

        if len(candles) < ATR_PERIOD + RSI_PERIOD + 2:
            return self._no_signal(symbol, "insufficient_candles")

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )

        # ── RULE 1: Funding rate hard gate ────────────────────────────────
        if funding_rate is None:
            sig.rules_failed.append("funding_rate_not_provided")
            return sig

        funding_pct = float(funding_rate) * 100  # convert to percent
        sig.indicators["funding_rate_pct"] = round(funding_pct, 6)

        direction = "NONE"
        if funding_pct >= FUNDING_POSITIVE_EXTREME:
            direction = "SELL"   # extreme long bias → reversion SHORT
        elif funding_pct <= FUNDING_NEGATIVE_EXTREME:
            direction = "BUY"    # extreme short bias → reversion LONG
        else:
            sig.rules_failed.append(
                f"funding_not_extreme: {funding_pct:.4f}% "
                f"(thresholds: ≥+{FUNDING_POSITIVE_EXTREME}% or ≤{FUNDING_NEGATIVE_EXTREME}%)"
            )
            return sig
        sig.rules_passed.append(f"funding_rate_extreme: {funding_pct:.4f}% → {direction}")

        # ── RULE 3: Time window ────────────────────────────────────────────
        current = candles[-1]
        ts: datetime = current["timestamp"]
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        hour_utc = ts.hour

        in_window = any(
            fh <= hour_utc < fh + ENTRY_WINDOW_HOURS
            for fh in FUNDING_UPDATE_HOURS
        )
        if not in_window:
            sig.rules_failed.append(
                f"outside_funding_window: hour={hour_utc}UTC "
                f"(valid: {FUNDING_UPDATE_HOURS} UTC +{ENTRY_WINDOW_HOURS}h)"
            )
            return sig
        sig.rules_passed.append(f"funding_window_ok: hour={hour_utc}UTC")

        # ── RULE 2: RSI filter ────────────────────────────────────────────
        closes = [c["close"] for c in candles]
        rsi_series = self._rsi(closes, RSI_PERIOD)
        rsi_val    = rsi_series[-1]
        sig.indicators["rsi14"] = round(rsi_val, 2)

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

        # ── RULE 4 & 5: SL and TP ─────────────────────────────────────────
        atr_val = self._atr(candles, ATR_PERIOD)
        sig.indicators["atr14"] = atr_val

        entry = current["close"]

        if direction == "BUY":
            sl = entry - (SL_ATR_MULT * atr_val)
            tp = entry + (TP_ATR_MULT * atr_val)
        else:
            sl = entry + (SL_ATR_MULT * atr_val)
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

        # ── RULE 6: Max risk cap embedded in indicators ────────────────────
        sig.indicators["max_risk_pct"] = MAX_RISK_PCT

        # ── Build final signal ─────────────────────────────────────────────
        sig.direction   = direction
        sig.entry_price = round(entry, 4)
        sig.stop_loss   = round(sl, 4)
        sig.take_profit = round(tp, 4)
        sig.reward_to_risk = round(rr, 2)
        sig.timeframe = "H1"
        sig.session = "CRYPTO_24_7"
        sig.confidence = 0.70
        sig.is_valid = True

        logger.info(
            f"[CryptoFundingReversion] {symbol} {direction}: "
            f"funding={funding_pct:.4f}% rsi={rsi_val:.1f} "
            f"entry={entry:.4f} sl={sl:.4f} tp={tp:.4f}"
        )
        return sig

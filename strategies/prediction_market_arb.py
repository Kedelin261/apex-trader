"""
APEX MULTI-MARKET TJR ENGINE
PREDICTION MARKETS Strategy — Kalshi / Prediction Market Arbitrage + Kelly Sizing

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : PREDICTION (Kalshi, Polymarket, etc.)
STRATEGY    : Prediction Market Arbitrage + Kelly Criterion Sizing

SETUP REQUIREMENTS:
  1. Market scanning:
       Scan available binary contracts every 60 seconds
       Only evaluate contracts with:
         - Volume > $1,000 in last 24 hours
         - Time to expiry between 1 hour and 7 days
         - Market still open (not resolved)

  2. Arbitrage detection:
       Combined market probabilities from all YES/NO legs:
         Sum of all contract prices (in $) should be ≈ $1.00
         If Sum < 0.95 → arbitrage opportunity exists (buy all legs)
         If Sum > 1.05 → arbitrage opportunity exists (sell all legs)
         Thresholds are EXACT: 0.95 and 1.05

  3. Kelly Criterion position sizing (EXACT FORMULA):
       For a binary contract where:
         p = estimated probability of winning
         b = odds (payout / stake − 1)
         Kelly fraction = (b × p − (1 − p)) / b

       Position size = Kelly fraction × account_balance
       Maximum Kelly fraction: 0.25 (hard cap — bet at most 25% of Kelly)
       Actual position = min(Kelly fraction, 0.25) × account_balance × 0.5
       (Half-Kelly rule enforced: multiply raw Kelly by 0.5)

  4. Signal conditions:
       BUY  YES leg: estimated p > current_price + 0.05
                     (We think YES more likely than market implies)
       BUY  NO  leg: estimated p < current_price − 0.05
                     (We think NO more likely than market implies by 5¢)

  5. Maximum position caps:
       Single contract: max $500 per position
       Per-event total: max $2,000 (summed across all legs of same event)
       Maximum 5 open prediction positions simultaneously

  6. Exit rules:
       Exit if price moves against by 20% of entry price
       Take profit when price reaches 90% of payout value
       Force exit 30 minutes before contract expiry

  7. Scan interval: EXACTLY 60 seconds between scans
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from strategies.base_strategy import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
SCAN_INTERVAL_SECONDS   = 60         # scan every 60 seconds exactly
MIN_VOLUME_24H          = 1_000.0    # $1,000 minimum volume in 24h
MIN_EXPIRY_HOURS        = 1          # at least 1 hour to expiry
MAX_EXPIRY_DAYS         = 7          # at most 7 days to expiry

ARB_LOW_THRESHOLD       = 0.95       # sum < 0.95 → underpriced (buy all)
ARB_HIGH_THRESHOLD      = 1.05       # sum > 1.05 → overpriced (sell all)

KELLY_MAX_FRACTION      = 0.25       # hard cap on Kelly fraction
KELLY_HALF_FRACTION     = 0.5        # half-Kelly rule

EDGE_THRESHOLD          = 0.05       # min probability edge to enter
MAX_POSITION_USD        = 500.0      # max per-contract position
MAX_EVENT_TOTAL_USD     = 2_000.0    # max total per event
MAX_OPEN_POSITIONS      = 5          # max simultaneous prediction positions

EXIT_AGAINST_PCT        = 0.20       # exit if price moves against by 20%
EXIT_PROFIT_PCT         = 0.90       # take profit at 90% of payout
EXPIRY_EXIT_MINUTES     = 30         # force exit 30 min before expiry
# ─────────────────────────────────────────────────────────────────────────────


def kelly_fraction(p: float, b: float) -> float:
    """
    EXACT Kelly Criterion formula:
      f* = (b × p − (1 − p)) / b

    Args:
        p: estimated win probability (0–1)
        b: net odds = (payout / stake) − 1
           For binary contracts paying $1 on a $c entry: b = (1/c) − 1

    Returns:
        Kelly fraction (can be negative → no bet)
    """
    if b <= 0:
        return 0.0
    return (b * p - (1 - p)) / b


def kelly_position_size(
    p: float,
    c: float,                   # contract cost (price, $0–$1)
    account_balance: float,
) -> float:
    """
    Compute Half-Kelly position size in USD.

    Payout = $1 per contract; cost = c.
    b = (1 − c) / c   (net profit on $c stake)

    Position = min(kelly_fraction, KELLY_MAX_FRACTION) × 0.5 × account_balance
    Then capped at MAX_POSITION_USD.
    """
    if c <= 0 or c >= 1:
        return 0.0
    b = (1 - c) / c
    kf = kelly_fraction(p, b)
    if kf <= 0:
        return 0.0
    effective = min(kf, KELLY_MAX_FRACTION) * KELLY_HALF_FRACTION
    return min(effective * account_balance, MAX_POSITION_USD)


class PredictionMarketArb(BaseStrategy):
    """
    Prediction Markets: Arbitrage + Kelly Criterion.
    See module docstring for complete rule set.
    """

    @property
    def name(self) -> str:
        return "prediction_market_arb"

    @property
    def asset_class(self) -> str:
        return "PREDICTION"

    # ------------------------------------------------------------------
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate a prediction market contract for trade entry.

        candles: Not used for prediction markets (no OHLCV).
                 Pass an empty list or dummy.
        context: REQUIRED fields:
          'symbol'           — contract ID/name
          'yes_price'        — current YES contract price (0–1)
          'no_price'         — current NO contract price (0–1)
          'estimated_prob'   — our estimated win probability (0–1)
          'volume_24h'       — total 24h volume in USD
          'hours_to_expiry'  — float hours until contract resolves
          'account_balance'  — float, current account balance
          'open_positions'   — int, current number of open prediction positions
          'event_total_usd'  — float, total already committed to this event
          'leg'              — 'YES' or 'NO' which leg we're evaluating
        """
        ctx = context or {}
        symbol        = ctx.get("symbol", "UNKNOWN")
        yes_price     = float(ctx.get("yes_price", 0.5))
        no_price      = float(ctx.get("no_price", 0.5))
        est_prob      = float(ctx.get("estimated_prob", 0.5))
        volume_24h    = float(ctx.get("volume_24h", 0.0))
        hours_to_exp  = float(ctx.get("hours_to_expiry", 0.0))
        account_bal   = float(ctx.get("account_balance", 10_000.0))
        open_pos      = int(ctx.get("open_positions", 0))
        event_total   = float(ctx.get("event_total_usd", 0.0))
        leg           = ctx.get("leg", "YES").upper()

        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
        )
        sig.indicators.update({
            "yes_price": yes_price,
            "no_price": no_price,
            "sum_prices": yes_price + no_price,
            "estimated_prob": est_prob,
            "volume_24h": volume_24h,
            "hours_to_expiry": hours_to_exp,
            "leg": leg,
        })

        # ── RULE 1: Volume filter ─────────────────────────────────────────
        if volume_24h < MIN_VOLUME_24H:
            sig.rules_failed.append(
                f"volume_insufficient: ${volume_24h:.0f} < ${MIN_VOLUME_24H:.0f}"
            )
            return sig
        sig.rules_passed.append(f"volume_ok: ${volume_24h:.0f}")

        # ── RULE 1: Expiry window ─────────────────────────────────────────
        days_to_exp = hours_to_exp / 24.0
        if hours_to_exp < MIN_EXPIRY_HOURS:
            sig.rules_failed.append(
                f"too_close_to_expiry: {hours_to_exp:.1f}h < {MIN_EXPIRY_HOURS}h"
            )
            return sig
        if days_to_exp > MAX_EXPIRY_DAYS:
            sig.rules_failed.append(
                f"expiry_too_far: {days_to_exp:.1f}d > {MAX_EXPIRY_DAYS}d"
            )
            return sig
        sig.rules_passed.append(f"expiry_window_ok: {hours_to_exp:.1f}h to expiry")

        # ── RULE 5: Open position cap ─────────────────────────────────────
        if open_pos >= MAX_OPEN_POSITIONS:
            sig.filter_blocks.append(
                f"max_open_positions: {open_pos} >= {MAX_OPEN_POSITIONS}"
            )
            return sig
        sig.rules_passed.append(f"open_positions_ok: {open_pos}")

        # ── RULE 5: Event total cap ────────────────────────────────────────
        if event_total >= MAX_EVENT_TOTAL_USD:
            sig.filter_blocks.append(
                f"event_total_cap: ${event_total:.0f} >= ${MAX_EVENT_TOTAL_USD:.0f}"
            )
            return sig

        # ── RULE 2: Arbitrage detection ────────────────────────────────────
        price_sum = yes_price + no_price
        arb_detected = price_sum < ARB_LOW_THRESHOLD or price_sum > ARB_HIGH_THRESHOLD
        sig.indicators["arb_detected"] = arb_detected

        # ── RULE 4: Edge-based signal ──────────────────────────────────────
        direction = "NONE"
        entry_price = 0.0

        if leg == "YES":
            edge = est_prob - yes_price
            if edge >= EDGE_THRESHOLD:
                direction = "BUY"
                entry_price = yes_price
            else:
                sig.rules_failed.append(
                    f"insufficient_edge_YES: est_prob={est_prob:.3f} "
                    f"yes_price={yes_price:.3f} edge={edge:.3f} < {EDGE_THRESHOLD}"
                )
        elif leg == "NO":
            edge = (1 - est_prob) - no_price
            if edge >= EDGE_THRESHOLD:
                direction = "BUY"
                entry_price = no_price
            else:
                sig.rules_failed.append(
                    f"insufficient_edge_NO: est_prob={est_prob:.3f} "
                    f"no_price={no_price:.3f} edge={edge:.3f} < {EDGE_THRESHOLD}"
                )
        else:
            sig.rules_failed.append(f"unknown_leg: {leg}")

        if direction == "NONE":
            return sig

        sig.rules_passed.append(
            f"edge_ok: {leg} edge={edge:.3f} >= {EDGE_THRESHOLD}"
        )

        # ── RULE 3: Kelly sizing ───────────────────────────────────────────
        p_win = est_prob if leg == "YES" else (1 - est_prob)
        pos_size = kelly_position_size(p_win, entry_price, account_bal)
        pos_size = min(pos_size, MAX_POSITION_USD - event_total)

        if pos_size <= 0:
            sig.rules_failed.append("kelly_size_zero_or_negative")
            return sig

        b = (1 - entry_price) / entry_price if entry_price > 0 else 0
        kf = kelly_fraction(p_win, b)
        sig.indicators["kelly_fraction"] = round(kf, 6)
        sig.indicators["half_kelly_size_usd"] = round(pos_size, 2)

        # ── Build final signal ─────────────────────────────────────────────
        sl = entry_price * (1 - EXIT_AGAINST_PCT)   # exit if price falls 20%
        tp = EXIT_PROFIT_PCT                          # 90¢ on $1 payout

        sig.direction      = direction
        sig.entry_price    = round(entry_price, 4)
        sig.stop_loss      = round(sl, 4)
        sig.take_profit    = tp
        sig.position_size  = round(pos_size, 2)
        sig.reward_to_risk = round((tp - entry_price) / (entry_price - sl), 2) if (entry_price - sl) > 0 else 0
        sig.timeframe      = "1MIN_SCAN"
        sig.confidence     = min(edge / (edge + 0.05), 0.95)
        sig.is_valid       = True
        sig.indicators["arb_low_threshold"]  = ARB_LOW_THRESHOLD
        sig.indicators["arb_high_threshold"] = ARB_HIGH_THRESHOLD
        sig.indicators["scan_interval_s"]    = SCAN_INTERVAL_SECONDS
        sig.indicators["expiry_exit_min"]    = EXPIRY_EXIT_MINUTES

        logger.info(
            f"[PredictionMarketArb] {symbol} {leg} {direction}: "
            f"est_prob={est_prob:.3f} price={entry_price:.3f} "
            f"edge={edge:.3f} kelly={kf:.4f} size=${pos_size:.2f}"
        )
        return sig

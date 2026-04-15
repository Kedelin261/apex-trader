"""
APEX MULTI-MARKET TJR ENGINE
Stocks Adapter — Gap Scanner

Scans stocks for pre-market gap conditions.
Provides context dicts compatible with GapAndGo strategy.

EXACT RULES:
  - Gap Up  : today_open > prev_close, gap% ≥ 0.5% and ≤ 8.0%
  - Gap Down: today_open < prev_close, gap% ≥ 0.5% and ≤ 8.0%
  - Scans at market open: 09:30 ET = 14:30 UTC

Architecture: This module produces context data ONLY.
  Strategy evaluation is done by GapAndGo.evaluate().
  No direct execution from this adapter.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ─── EXACT THRESHOLDS (match gap_and_go.py) ──────────────────────────────────
GAP_MIN_PCT = 0.5
GAP_MAX_PCT = 8.0
# ─────────────────────────────────────────────────────────────────────────────


class StockGapContext:
    """
    Holds pre-market gap context for a single stock.
    Passed to GapAndGo.evaluate() as context dict.
    """
    def __init__(
        self,
        symbol: str,
        prev_close: float,
        today_open: float,
        daily_candles: List[dict] = None,
    ):
        self.symbol     = symbol
        self.prev_close = prev_close
        self.today_open = today_open
        self.daily_candles = daily_candles or []

        if today_open > prev_close:
            self.gap_pct = (today_open - prev_close) / prev_close * 100
            self.gap_direction = "UP"
        elif today_open < prev_close:
            self.gap_pct = (prev_close - today_open) / prev_close * 100
            self.gap_direction = "DOWN"
        else:
            self.gap_pct = 0.0
            self.gap_direction = "NONE"

    @property
    def qualifies(self) -> bool:
        """True if gap meets size requirements."""
        return (
            self.gap_direction != "NONE"
            and GAP_MIN_PCT <= self.gap_pct <= GAP_MAX_PCT
        )

    def to_context(self) -> dict:
        """Produce context dict for GapAndGo.evaluate()."""
        return {
            "symbol": self.symbol,
            "prev_close": self.prev_close,
            "today_open": self.today_open,
            "gap_pct": round(self.gap_pct, 4),
            "gap_direction": self.gap_direction,
        }

    def __repr__(self) -> str:
        return (
            f"StockGapContext({self.symbol}: "
            f"{self.gap_direction} {self.gap_pct:.2f}% "
            f"prev={self.prev_close:.4f} open={self.today_open:.4f})"
        )


class GapScanner:
    """
    Scans a watchlist of stocks for qualifying pre-market gaps.

    Usage:
        scanner = GapScanner(watchlist=["AAPL", "TSLA", "NVDA"])
        gaps = scanner.scan(price_snapshot)  # returns List[StockGapContext]
    """

    def __init__(self, watchlist: List[str] = None):
        self.watchlist = [s.upper() for s in (watchlist or [])]
        logger.info(f"[GapScanner] Watchlist: {self.watchlist}")

    def scan(self, price_snapshot: Dict[str, dict]) -> List[StockGapContext]:
        """
        Scan all watchlist stocks for qualifying gaps.

        Args:
            price_snapshot: dict mapping symbol → {"prev_close": float, "today_open": float}

        Returns:
            List of StockGapContext objects that qualify (0.5% ≤ gap ≤ 8.0%)
        """
        results = []
        for symbol in self.watchlist:
            data = price_snapshot.get(symbol)
            if not data:
                logger.debug(f"[GapScanner] No price data for {symbol}")
                continue

            prev_close = float(data.get("prev_close", 0))
            today_open = float(data.get("today_open", 0))

            if prev_close <= 0 or today_open <= 0:
                logger.debug(f"[GapScanner] Invalid prices for {symbol}")
                continue

            ctx = StockGapContext(
                symbol=symbol,
                prev_close=prev_close,
                today_open=today_open,
                daily_candles=data.get("candles", []),
            )

            if ctx.qualifies:
                logger.info(
                    f"[GapScanner] QUALIFYING GAP: {ctx}"
                )
                results.append(ctx)
            else:
                logger.debug(
                    f"[GapScanner] Gap not qualifying for {symbol}: "
                    f"{ctx.gap_direction} {ctx.gap_pct:.2f}%"
                )

        return results

    def is_market_open(self) -> bool:
        """
        Check if US market is open: 14:30–21:00 UTC (09:30–16:00 ET).
        Simple check — does not account for holidays.
        """
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:   # Saturday or Sunday
            return False
        t = now.hour * 60 + now.minute
        return 14 * 60 + 30 <= t < 21 * 60

    def is_gap_and_go_window(self) -> bool:
        """
        Check if within valid Gap-and-Go window: 14:30–16:00 UTC.
        """
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:
            return False
        t = now.hour * 60 + now.minute
        return 14 * 60 + 30 <= t < 16 * 60

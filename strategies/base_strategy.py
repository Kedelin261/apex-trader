"""
APEX MULTI-MARKET TJR ENGINE
Base Strategy — Abstract interface all strategy modules must implement.

ARCHITECTURE RULE:
  Every strategy produces a deterministic StrategySignal that passes through:
    StrategyValidator → RiskGuardian → ExecutionSupervisor
  No direct execution is allowed from any strategy module.

All thresholds defined in each subclass are EXACT — never rounded, never approximated.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StrategySignal:
    """
    Deterministic output from any strategy module.

    This object travels through:
      StrategyValidator → RiskGuardian → ExecutionSupervisor

    No execution occurs without passing all three gates.
    """
    # Identity
    strategy_name: str
    asset_class: str                    # FOREX | FUTURES | INDICES | COMMODITIES | STOCKS | CRYPTO | PREDICTION
    symbol: str                         # Canonical internal symbol e.g. EURUSD
    broker_symbol: Optional[str] = None # Broker-specific e.g. EUR/USD (IBKR)

    # Direction
    direction: str = "NONE"             # BUY | SELL | NONE

    # Prices
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0

    # Sizing (raw — risk normaliser fills position_size)
    position_size: float = 0.0         # Lots / contracts / shares / oz (asset-specific)
    risk_usd: float = 0.0              # Expected dollar risk

    # Reward/Risk
    reward_to_risk: float = 0.0

    # Context
    timeframe: str = ""
    session: str = ""
    regime: str = ""
    confidence: float = 0.0

    # Rule audit — every rule that was checked
    rules_passed: List[str] = field(default_factory=list)
    rules_failed: List[str] = field(default_factory=list)
    filter_blocks: List[str] = field(default_factory=list)   # hard-block reasons

    # Raw indicators for logging / debugging
    indicators: Dict[str, Any] = field(default_factory=dict)

    # Timestamps
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Flags
    is_valid: bool = False              # True only when direction != NONE and no filter_blocks

    def to_dict(self) -> dict:
        return {
            "strategy_name": self.strategy_name,
            "asset_class": self.asset_class,
            "symbol": self.symbol,
            "broker_symbol": self.broker_symbol,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "position_size": self.position_size,
            "risk_usd": self.risk_usd,
            "reward_to_risk": self.reward_to_risk,
            "timeframe": self.timeframe,
            "session": self.session,
            "confidence": self.confidence,
            "rules_passed": self.rules_passed,
            "rules_failed": self.rules_failed,
            "filter_blocks": self.filter_blocks,
            "indicators": self.indicators,
            "is_valid": self.is_valid,
            "generated_at": self.generated_at.isoformat(),
        }


class BaseStrategy(ABC):
    """
    Abstract base for all Apex strategy modules.

    Subclasses MUST:
      1. Implement evaluate() returning a StrategySignal.
      2. Enforce their own thresholds exactly as documented.
      3. Record every rule check in signal.rules_passed / rules_failed.
      4. Never submit orders directly.
    """

    def __init__(self, config: dict = None):
        self._config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable strategy name."""

    @property
    @abstractmethod
    def asset_class(self) -> str:
        """Asset class this strategy handles."""

    @abstractmethod
    def evaluate(self, candles: List[dict], context: dict = None) -> StrategySignal:
        """
        Evaluate market conditions against strategy rules.

        Args:
            candles: List of OHLCV dicts (oldest first) with keys:
                     open, high, low, close, volume, timestamp
            context: Supplementary context (account_balance, symbol, etc.)

        Returns:
            StrategySignal — always returned, direction='NONE' if no setup.
        """

    # ------------------------------------------------------------------
    # SHARED INDICATOR UTILITIES
    # (Pure functions — no side effects, no state)
    # ------------------------------------------------------------------

    @staticmethod
    def _atr(candles: List[dict], period: int) -> float:
        """Average True Range over `period` candles. Returns 0.0 if insufficient data."""
        if len(candles) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(candles)):
            h = candles[i]["high"]
            l = candles[i]["low"]
            pc = candles[i - 1]["close"]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        # Use last `period` TRs
        recent = trs[-period:]
        return sum(recent) / len(recent) if recent else 0.0

    @staticmethod
    def _ema(values: List[float], period: int) -> List[float]:
        """Exponential Moving Average. Returns list same length as values."""
        if len(values) < period:
            return [0.0] * len(values)
        k = 2.0 / (period + 1)
        result = [0.0] * len(values)
        # Seed with SMA
        result[period - 1] = sum(values[:period]) / period
        for i in range(period, len(values)):
            result[i] = values[i] * k + result[i - 1] * (1 - k)
        return result

    @staticmethod
    def _sma(values: List[float], period: int) -> List[float]:
        """Simple Moving Average. Returns list same length as values."""
        result = [0.0] * len(values)
        for i in range(period - 1, len(values)):
            result[i] = sum(values[i - period + 1: i + 1]) / period
        return result

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> List[float]:
        """RSI. Returns list same length as closes."""
        if len(closes) < period + 1:
            return [50.0] * len(closes)
        result = [50.0] * len(closes)
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0.0))
            losses.append(max(-d, 0.0))
        # Initial averages
        avg_g = sum(gains[:period]) / period
        avg_l = sum(losses[:period]) / period
        if avg_l == 0:
            result[period] = 100.0
        else:
            rs = avg_g / avg_l
            result[period] = 100.0 - (100.0 / (1.0 + rs))
        for i in range(period + 1, len(closes)):
            avg_g = (avg_g * (period - 1) + gains[i - 1]) / period
            avg_l = (avg_l * (period - 1) + losses[i - 1]) / period
            if avg_l == 0:
                result[i] = 100.0
            else:
                rs = avg_g / avg_l
                result[i] = 100.0 - (100.0 / (1.0 + rs))
        return result

    @staticmethod
    def _vwap(candles: List[dict]) -> float:
        """Session VWAP. Returns 0.0 if no volume or insufficient data."""
        total_pv = 0.0
        total_v = 0.0
        for c in candles:
            typ = (c["high"] + c["low"] + c["close"]) / 3.0
            v = c.get("volume", 0.0)
            total_pv += typ * v
            total_v += v
        return (total_pv / total_v) if total_v > 0 else 0.0

    @staticmethod
    def _vwap_std(candles: List[dict], vwap: float) -> float:
        """Standard deviation around VWAP for SD band calculation."""
        if not candles or vwap == 0:
            return 0.0
        total_v = sum(c.get("volume", 0.0) for c in candles)
        if total_v == 0:
            return 0.0
        variance = 0.0
        for c in candles:
            typ = (c["high"] + c["low"] + c["close"]) / 3.0
            v = c.get("volume", 0.0)
            variance += v * (typ - vwap) ** 2
        return (variance / total_v) ** 0.5

    @staticmethod
    def _pivot_high(candles: List[dict], left: int, right: int, idx: int) -> bool:
        """True if candles[idx].high is the highest high in [idx-left, idx+right]."""
        if idx < left or idx + right >= len(candles):
            return False
        pivot = candles[idx]["high"]
        for j in range(idx - left, idx + right + 1):
            if j == idx:
                continue
            if candles[j]["high"] >= pivot:
                return False
        return True

    @staticmethod
    def _pivot_low(candles: List[dict], left: int, right: int, idx: int) -> bool:
        """True if candles[idx].low is the lowest low in [idx-left, idx+right]."""
        if idx < left or idx + right >= len(candles):
            return False
        pivot = candles[idx]["low"]
        for j in range(idx - left, idx + right + 1):
            if j == idx:
                continue
            if candles[j]["low"] <= pivot:
                return False
        return True

    @staticmethod
    def _opening_range(candles: List[dict], session_open_ts: datetime,
                       minutes: int) -> Optional[dict]:
        """
        Build opening range from the first `minutes` candles after session_open_ts.
        Returns {"high": float, "low": float} or None.
        """
        from datetime import timezone, timedelta
        cutoff = session_open_ts + timedelta(minutes=minutes)
        or_candles = [
            c for c in candles
            if session_open_ts <= c["timestamp"] < cutoff
        ]
        if not or_candles:
            return None
        return {
            "high": max(c["high"] for c in or_candles),
            "low": min(c["low"] for c in or_candles),
        }

    @staticmethod
    def _session_candles(candles: List[dict],
                         session_open_ts: datetime) -> List[dict]:
        """Return candles from session_open_ts onward."""
        return [c for c in candles if c.get("timestamp") >= session_open_ts]

    def _no_signal(self, symbol: str, reason: str) -> StrategySignal:
        """Return a clean no-signal result."""
        sig = StrategySignal(
            strategy_name=self.name,
            asset_class=self.asset_class,
            symbol=symbol,
            direction="NONE",
            is_valid=False,
        )
        sig.rules_failed.append(reason)
        return sig

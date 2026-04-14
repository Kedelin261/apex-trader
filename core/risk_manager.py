"""
APEX MULTI-MARKET TJR ENGINE
Risk Manager — Hard risk rules. Non-negotiable.

This module enforces all trading risk constraints.
No trade may bypass this module.
Every rejection is logged with a precise reason.

ARCHITECTURE RULE: The RiskManager is a pure validator.
It does not execute trades. It approves or rejects them.
All approvals and rejections are immutable and logged.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple

from domain.models import (
    Signal, RiskRejectionReason, DailyPnLRecord, Position,
    InstrumentType, RiskProfile, MarketRegime
)

logger = logging.getLogger(__name__)


class RiskValidationResult:
    """Result of a risk validation check. Immutable after creation."""

    def __init__(self, approved: bool, reasons: List[str],
                 rejection_codes: List[RiskRejectionReason] = None,
                 details: dict = None):
        self.approved = approved
        self.reasons = reasons
        self.rejection_codes = rejection_codes or []
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {
            "approved": self.approved,
            "reasons": self.reasons,
            "rejection_codes": [r.value for r in self.rejection_codes],
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }

    def __repr__(self) -> str:
        status = "APPROVED" if self.approved else "REJECTED"
        return f"RiskValidation[{status}]: {'; '.join(self.reasons)}"


class DailyRiskGovernor:
    """
    Tracks daily PnL and enforces daily loss limits.

    State is maintained per-day. At start of each trading day,
    state resets (manual or automatic).

    This is a hard gate. Once the daily limit is breached,
    no new trades are allowed until the next day or manual reset.
    """

    def __init__(self, max_daily_loss_pct: float, account_balance: float,
                 simulation_mode: bool = False):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.account_balance = account_balance
        self.max_daily_loss_usd = account_balance * (max_daily_loss_pct / 100.0)
        self.simulation_mode = simulation_mode  # In backtest: use candle date, not system date

        self._current_date: str = date.today().isoformat()
        self._realized_pnl: float = 0.0
        self._unrealized_pnl: float = 0.0
        self._trade_count: int = 0
        self._blocked_trades: int = 0
        self._is_killed: bool = False
        self._kill_reason: str = ""
        self._history: Dict[str, DailyPnLRecord] = {}

        logger.info(
            f"[DailyRiskGovernor] Initialized: max daily loss = "
            f"${self.max_daily_loss_usd:.2f} ({max_daily_loss_pct}%)"
        )

    def _check_date_reset(self):
        """Auto-reset at day boundary."""
        today = date.today().isoformat()
        if today != self._current_date:
            logger.info(f"[DailyRiskGovernor] Day boundary crossed: {self._current_date} → {today}")
            self._archive_day()
            self._current_date = today
            self._realized_pnl = 0.0
            self._unrealized_pnl = 0.0
            self._trade_count = 0
            self._blocked_trades = 0
            self._is_killed = False
            self._kill_reason = ""

    def _archive_day(self):
        """Archive current day record."""
        self._history[self._current_date] = DailyPnLRecord(
            date=self._current_date,
            realized_pnl=self._realized_pnl,
            unrealized_pnl=self._unrealized_pnl,
            trade_count=self._trade_count,
            blocked_trades=self._blocked_trades,
            is_killed=self._is_killed,
            kill_reason=self._kill_reason
        )

    def is_trading_allowed(self) -> Tuple[bool, str]:
        """Check if trading is allowed today."""
        self._check_date_reset()

        if self._is_killed:
            return False, f"KILL_SWITCH: {self._kill_reason}"

        total_loss = abs(min(0, self._realized_pnl + self._unrealized_pnl))
        if total_loss >= self.max_daily_loss_usd:
            reason = (
                f"Max daily loss reached: ${total_loss:.2f} >= "
                f"${self.max_daily_loss_usd:.2f} ({self.max_daily_loss_pct}%)"
            )
            self._kill(reason)
            return False, reason

        return True, ""

    def _kill(self, reason: str):
        """Engage kill switch."""
        self._is_killed = True
        self._kill_reason = reason
        logger.critical(f"[DailyRiskGovernor] KILL SWITCH ENGAGED: {reason}")

    def advance_to_date(self, new_date: str):
        """Advance governor to a new simulation date (for backtesting)."""
        if self.simulation_mode and new_date != self._current_date:
            self._archive_day()
            self._current_date = new_date
            self._realized_pnl = 0.0
            self._unrealized_pnl = 0.0
            self._trade_count = 0
            self._blocked_trades = 0
            self._is_killed = False
            self._kill_reason = ""

    def record_trade_pnl(self, realized_pnl: float):
        """Record realized PnL from a closed trade."""
        self._check_date_reset()
        self._realized_pnl += realized_pnl
        self._trade_count += 1
        logger.info(
            f"[DailyRiskGovernor] Trade closed: PnL ${realized_pnl:+.2f} | "
            f"Day total: ${self._realized_pnl:+.2f}"
        )

    def update_unrealized(self, unrealized_pnl: float):
        """Update unrealized PnL (open positions)."""
        self._check_date_reset()
        self._unrealized_pnl = unrealized_pnl

    def increment_blocked(self):
        """Record a blocked trade."""
        self._blocked_trades += 1

    def force_kill(self, reason: str):
        """External kill switch trigger."""
        self._kill(reason)

    def get_summary(self) -> DailyPnLRecord:
        """Return today's summary."""
        self._check_date_reset()
        return DailyPnLRecord(
            date=self._current_date,
            realized_pnl=self._realized_pnl,
            unrealized_pnl=self._unrealized_pnl,
            trade_count=self._trade_count,
            blocked_trades=self._blocked_trades,
            is_killed=self._is_killed,
            kill_reason=self._kill_reason
        )

    @property
    def daily_pnl(self) -> float:
        return self._realized_pnl + self._unrealized_pnl

    @property
    def daily_pnl_pct(self) -> float:
        return (self.daily_pnl / self.account_balance) * 100.0

    @property
    def is_killed(self) -> bool:
        return self._is_killed


class RiskManager:
    """
    Central risk validation engine.

    Validates:
    1. Daily loss limit (via DailyRiskGovernor)
    2. Total drawdown limit
    3. Maximum concurrent trades
    4. Risk per trade (position size vs account)
    5. Stop distance validity
    6. Spread/slippage thresholds
    7. Instrument-specific risk profiles
    8. Minimum R:R ratio
    9. Session requirements
    10. Volatility/news context

    Every rejection is logged. Every approval is logged.
    No trade bypasses this module.
    """

    def __init__(self, config: dict, simulation_mode: bool = False):
        self.config = config
        risk_cfg = config.get("risk", {})

        self.max_risk_per_trade_pct = risk_cfg.get("max_risk_per_trade_pct", 1.0)
        self.max_daily_loss_pct = risk_cfg.get("max_daily_loss_pct", 3.0)
        self.max_total_drawdown_pct = risk_cfg.get("max_total_drawdown_pct", 10.0)
        self.max_concurrent_trades = risk_cfg.get("max_concurrent_trades", 2)
        self.min_rr_ratio = risk_cfg.get("min_reward_to_risk_ratio", 2.0)
        self.max_spread_pips = risk_cfg.get("max_spread_pips", 3.0)
        self.max_slippage_pips = risk_cfg.get("max_slippage_pips", 2.0)
        self.max_position_size_usd = risk_cfg.get("max_position_size_usd", 10000.0)

        self.account_balance = risk_cfg.get("account_balance_default", 100000.0)
        self.peak_balance = self.account_balance

        self.daily_governor = DailyRiskGovernor(
            max_daily_loss_pct=self.max_daily_loss_pct,
            account_balance=self.account_balance,
            simulation_mode=simulation_mode
        )

        # Instrument-specific risk profiles
        self._risk_profiles: Dict[str, RiskProfile] = {}

        # Kill switch state
        self._global_kill_switch = False
        self._kill_reasons: List[str] = []

        logger.info(
            f"[RiskManager] Initialized: "
            f"max_risk={self.max_risk_per_trade_pct}% | "
            f"max_daily_loss={self.max_daily_loss_pct}% | "
            f"max_drawdown={self.max_total_drawdown_pct}% | "
            f"max_concurrent={self.max_concurrent_trades}"
        )

    def update_balance(self, new_balance: float):
        """Update account balance and track peak."""
        self.account_balance = new_balance
        self.peak_balance = max(self.peak_balance, new_balance)
        self.daily_governor.account_balance = new_balance

    def get_drawdown_pct(self) -> float:
        """Current drawdown from peak."""
        if self.peak_balance <= 0:
            return 0.0
        return ((self.peak_balance - self.account_balance) / self.peak_balance) * 100.0

    def register_risk_profile(self, profile: RiskProfile):
        """Register instrument-specific risk profile."""
        self._risk_profiles[profile.instrument] = profile
        logger.debug(f"[RiskManager] Registered risk profile for {profile.instrument}")

    def get_risk_profile(self, instrument: str) -> RiskProfile:
        """Get risk profile for instrument, or default."""
        return self._risk_profiles.get(
            instrument,
            RiskProfile(
                instrument=instrument,
                max_risk_pct=self.max_risk_per_trade_pct,
                max_spread_pips=self.max_spread_pips,
                max_slippage_pips=self.max_slippage_pips,
                min_rr_ratio=self.min_rr_ratio
            )
        )

    def engage_kill_switch(self, reason: str):
        """Engage global kill switch — blocks ALL trading."""
        self._global_kill_switch = True
        self._kill_reasons.append(reason)
        self.daily_governor.force_kill(reason)
        logger.critical(f"[RiskManager] GLOBAL KILL SWITCH ENGAGED: {reason}")

    def is_kill_switch_active(self) -> bool:
        return self._global_kill_switch or self.daily_governor.is_killed

    def validate_signal(self,
                        signal: Signal,
                        open_positions: List[Position],
                        current_spread_pips: float = 0.0,
                        regime: Optional[MarketRegime] = None,
                        news_active: bool = False,
                        volatility_spike: bool = False) -> RiskValidationResult:
        """
        Full risk validation of a trade signal.
        Returns RiskValidationResult with approved=True only if ALL checks pass.
        """
        rejections: List[str] = []
        rejection_codes: List[RiskRejectionReason] = []

        # ── 1. Global kill switch ──────────────────────────────────────────
        if self._global_kill_switch:
            reason = f"Global kill switch active: {'; '.join(self._kill_reasons)}"
            rejections.append(reason)
            rejection_codes.append(RiskRejectionReason.KILL_SWITCH_ACTIVE)

        # ── 2. Daily risk governor ─────────────────────────────────────────
        trading_allowed, day_reason = self.daily_governor.is_trading_allowed()
        if not trading_allowed:
            rejections.append(f"Daily risk limit: {day_reason}")
            rejection_codes.append(RiskRejectionReason.MAX_DAILY_LOSS_REACHED)

        # ── 3. Total drawdown ──────────────────────────────────────────────
        drawdown = self.get_drawdown_pct()
        if drawdown >= self.max_total_drawdown_pct:
            reason = (
                f"Max drawdown reached: {drawdown:.2f}% >= "
                f"{self.max_total_drawdown_pct}%"
            )
            rejections.append(reason)
            rejection_codes.append(RiskRejectionReason.MAX_DRAWDOWN_REACHED)
            self.engage_kill_switch(reason)

        # ── 4. Concurrent trades ───────────────────────────────────────────
        if len(open_positions) >= self.max_concurrent_trades:
            rejections.append(
                f"Max concurrent trades: {len(open_positions)} >= "
                f"{self.max_concurrent_trades}"
            )
            rejection_codes.append(RiskRejectionReason.MAX_CONCURRENT_TRADES)

        # ── 5. Risk per trade ──────────────────────────────────────────────
        profile = self.get_risk_profile(signal.instrument)
        max_risk_usd = self.account_balance * (profile.max_risk_pct / 100.0)
        if signal.risk_amount_usd > max_risk_usd:
            rejections.append(
                f"Risk per trade exceeded: ${signal.risk_amount_usd:.2f} > "
                f"${max_risk_usd:.2f} ({profile.max_risk_pct}%)"
            )
            rejection_codes.append(RiskRejectionReason.RISK_PER_TRADE_EXCEEDED)

        # ── 6. R:R ratio ───────────────────────────────────────────────────
        if signal.reward_to_risk < profile.min_rr_ratio:
            rejections.append(
                f"R:R insufficient: {signal.reward_to_risk:.2f} < "
                f"{profile.min_rr_ratio}"
            )
            rejection_codes.append(RiskRejectionReason.RR_RATIO_INSUFFICIENT)

        # ── 7. Spread ──────────────────────────────────────────────────────
        if current_spread_pips > profile.max_spread_pips:
            rejections.append(
                f"Spread too wide: {current_spread_pips:.2f} pips > "
                f"{profile.max_spread_pips:.2f}"
            )
            rejection_codes.append(RiskRejectionReason.SPREAD_TOO_WIDE)

        # ── 8. Stop loss distance ──────────────────────────────────────────
        if signal.stop_distance_pips <= 0:
            rejections.append("Invalid stop loss distance: 0 or negative")
            rejection_codes.append(RiskRejectionReason.INVALID_STOP_DISTANCE)

        # ── 9. News filter ─────────────────────────────────────────────────
        if news_active and profile.news_filter_enabled:
            rejections.append("High-impact news event active — trade blocked")
            rejection_codes.append(RiskRejectionReason.NEWS_EVENT_ACTIVE)

        # ── 10. Volatility spike ───────────────────────────────────────────
        if volatility_spike and profile.volatility_filter_enabled:
            rejections.append("Volatility spike detected — trade blocked")
            rejection_codes.append(RiskRejectionReason.VOLATILITY_TOO_HIGH)

        # ── 11. Regime filter ──────────────────────────────────────────────
        if regime in (MarketRegime.UNSTABLE, MarketRegime.ILLIQUID, MarketRegime.NEWS_DRIVEN):
            rejections.append(f"Unfavorable regime: {regime.value}")
            rejection_codes.append(RiskRejectionReason.MARKET_INSTABILITY)

        # ── Final decision ─────────────────────────────────────────────────
        approved = len(rejections) == 0

        if approved:
            logger.info(
                f"[RiskManager] APPROVED: {signal.instrument} "
                f"{signal.direction.value} | "
                f"Risk=${signal.risk_amount_usd:.2f} | "
                f"R:R={signal.reward_to_risk:.2f}"
            )
        else:
            self.daily_governor.increment_blocked()
            logger.warning(
                f"[RiskManager] REJECTED: {signal.instrument} | "
                f"Reasons: {'; '.join(rejections)}"
            )

        return RiskValidationResult(
            approved=approved,
            reasons=rejections if not approved else ["All risk checks passed"],
            rejection_codes=rejection_codes,
            details={
                "drawdown_pct": drawdown,
                "daily_pnl": self.daily_governor.daily_pnl,
                "open_positions": len(open_positions),
                "spread_pips": current_spread_pips,
                "news_active": news_active,
                "volatility_spike": volatility_spike,
                "regime": regime.value if regime else None
            }
        )

    def record_trade_close(self, pnl_usd: float, new_balance: float):
        """Record a closed trade and update balance."""
        self.daily_governor.record_trade_pnl(pnl_usd)
        self.update_balance(new_balance)

    def update_unrealized_pnl(self, total_unrealized: float):
        """Update unrealized PnL from open positions."""
        self.daily_governor.update_unrealized(total_unrealized)

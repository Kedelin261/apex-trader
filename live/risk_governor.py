"""
APEX MULTI-MARKET TJR ENGINE
Risk Governor — Hard autonomous risk caps for unattended operation.

Phase 3 Implementation: all global risk limits enforced here.

LIMITS ENFORCED:
  - max risk per trade (% of account)
  - max daily loss (% of account, auto-reset at UTC midnight)
  - max total drawdown (% of peak)
  - max concurrent positions
  - max trades per day
  - max positions per symbol
  - max positions per strategy
  - max exposure by asset class (USD notional)
  - max correlated symbol exposure
  - cooldown after N consecutive losses
  - kill switch on repeated broker rejects (configurable threshold)
  - kill switch on reconciliation mismatch
  - kill switch on stale market data

SAFETY RULE:
  When any global limit is breached:
    - new entries are BLOCKED
    - existing position management continues (stops/TPs still valid)
    - alert is emitted
    - ESM transitions to LIVE_BLOCKED if in live mode

ARCHITECTURE:
  This module is called exclusively from the autonomous scheduler's
  signal process loop — BEFORE the order reaches BrokerManager.
  No strategy, agent, or API endpoint bypasses it.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CORRELATED SYMBOL GROUPS — exposure capped across the group
# ---------------------------------------------------------------------------

CORRELATION_GROUPS: Dict[str, List[str]] = {
    "USD_INDEX":    ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY"],
    "EQUITY_US":   ["ES", "NQ", "YM", "SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA"],
    "GOLD":        ["XAUUSD", "GC", "XAGUSD"],
    "ENERGY":      ["USOIL", "UKOIL", "NATGAS", "CL"],
    "CRYPTO_BTC":  ["BTC/USDT", "BTCUSDT"],
}


def _get_correlation_group(symbol: str) -> Optional[str]:
    """Return the correlation group name for a symbol, or None."""
    s = symbol.upper()
    for group_name, members in CORRELATION_GROUPS.items():
        if s in members:
            return group_name
    return None


# ---------------------------------------------------------------------------
# GOVERNOR STATE  (persisted to disk so daily counters survive restart)
# ---------------------------------------------------------------------------

@dataclass
class GovernorDailyState:
    """Per-day risk counters. Auto-resets at UTC midnight."""
    date_utc: str = ""
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    trades_today: int = 0
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    broker_rejects_today: int = 0
    daily_loss_blocked: bool = False
    kill_reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "GovernorDailyState":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class GovernorBlockEvent:
    """Record of when and why the governor blocked trading."""
    timestamp: str
    reason: str
    block_type: str          # "DAILY_LOSS" | "DRAWDOWN" | "CONCURRENT" | "COOLDOWN" | "STALE_DATA" | etc.
    symbol: Optional[str]
    strategy: Optional[str]
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# RISK GOVERNOR
# ---------------------------------------------------------------------------

class RiskGovernor:
    """
    Autonomous hard-cap risk governor for unattended trading.

    Thread-safe. Persists daily counters to disk so they survive restarts.
    Integrates with ExecutionStateMachine to block live trading on breach.
    Emits alerts via AlertDispatcher when limits are hit.
    """

    def __init__(self, config: dict, esm=None, alert_dispatcher=None):
        self._config = config
        self._esm = esm                          # ExecutionStateMachine (optional)
        self._alert = alert_dispatcher           # AlertDispatcher (optional)
        self._lock = threading.Lock()

        risk_cfg = config.get("risk", {})
        gov_cfg = config.get("autonomy", {}).get("risk_governor", {})

        # Hard limits from config
        self._max_risk_pct        = risk_cfg.get("max_risk_per_trade_pct", 1.0)
        self._max_daily_loss_pct  = risk_cfg.get("max_daily_loss_pct", 3.0)
        self._max_drawdown_pct    = risk_cfg.get("max_total_drawdown_pct", 10.0)
        self._max_concurrent      = risk_cfg.get("max_concurrent_trades", 5)
        self._max_trades_per_day  = gov_cfg.get("max_trades_per_day", 20)
        self._max_per_symbol      = gov_cfg.get("max_positions_per_symbol", 2)
        self._max_per_strategy    = gov_cfg.get("max_positions_per_strategy", 3)
        self._max_asset_class_usd = gov_cfg.get("max_exposure_per_asset_class_usd", 50_000.0)
        self._max_correlated_usd  = gov_cfg.get("max_correlated_exposure_usd", 30_000.0)
        self._cooldown_after_losses = gov_cfg.get("cooldown_after_consecutive_losses", 3)
        self._cooldown_minutes    = gov_cfg.get("cooldown_minutes", 60)
        self._broker_reject_kill  = gov_cfg.get("kill_after_broker_rejects", 10)
        self._recon_mismatch_kill = gov_cfg.get("kill_on_reconciliation_mismatch", True)
        self._stale_data_kill     = gov_cfg.get("kill_on_stale_data_minutes", 5)
        self._account_balance     = risk_cfg.get("account_balance_default", 100_000.0)
        self._peak_balance        = self._account_balance

        # State file for daily counter persistence
        state_dir = Path(config.get("data", {}).get("state_dir", "data"))
        state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = state_dir / "risk_governor_state.json"

        # Runtime state
        self._daily = GovernorDailyState(date_utc=date.today().isoformat())
        self._cooldown_until: Optional[datetime] = None
        self._global_kill: bool = False
        self._kill_reasons: List[str] = []
        self._block_history: List[GovernorBlockEvent] = []
        self._last_data_timestamp: Optional[datetime] = None

        self._load_state()

        logger.info(
            f"[RiskGovernor] Initialised — "
            f"max_daily_loss={self._max_daily_loss_pct}% "
            f"max_drawdown={self._max_drawdown_pct}% "
            f"max_concurrent={self._max_concurrent} "
            f"max_trades_day={self._max_trades_per_day} "
            f"cooldown_after={self._cooldown_after_losses} losses"
        )

    # ------------------------------------------------------------------
    # PUBLIC: Main entry-gate — called before every order submission
    # ------------------------------------------------------------------

    def check_new_entry(
        self,
        symbol: str,
        strategy_name: str,
        asset_class: str,
        proposed_risk_usd: float,
        open_positions: List[dict],
        account_balance: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """
        Check whether a new entry is permitted.

        Returns:
            (True, "")               — entry allowed
            (False, "REASON: detail")— entry blocked

        Preserves existing positions regardless of outcome.
        """
        with self._lock:
            self._maybe_reset_daily()

            if account_balance is not None:
                self._update_balance(account_balance)

            # 1. Global kill switch
            if self._global_kill:
                return False, f"GLOBAL_KILL: {'; '.join(self._kill_reasons)}"

            # 2. ESM kill switch
            if self._esm and self._esm.is_kill_switch_active():
                return False, "ESM_KILL_SWITCH: kill switch engaged"

            # 3. Daily loss cap
            total_day_pnl = self._daily.realized_pnl_usd + self._daily.unrealized_pnl_usd
            max_day_loss_usd = self._account_balance * (self._max_daily_loss_pct / 100.0)
            if total_day_pnl <= -max_day_loss_usd:
                reason = (
                    f"DAILY_LOSS: ${abs(total_day_pnl):.2f} >= "
                    f"${max_day_loss_usd:.2f} ({self._max_daily_loss_pct}%)"
                )
                self._record_block("DAILY_LOSS", reason, symbol, strategy_name)
                self._trigger_live_block(reason)
                return False, reason

            # 4. Max drawdown
            drawdown_pct = self._get_drawdown_pct()
            if drawdown_pct >= self._max_drawdown_pct:
                reason = (
                    f"MAX_DRAWDOWN: {drawdown_pct:.2f}% >= {self._max_drawdown_pct}%"
                )
                self._record_block("DRAWDOWN", reason, symbol, strategy_name)
                self._engage_kill(reason)
                return False, reason

            # 5. Max concurrent positions
            n_open = len(open_positions)
            if n_open >= self._max_concurrent:
                reason = f"MAX_CONCURRENT: {n_open}/{self._max_concurrent} positions open"
                self._record_block("CONCURRENT", reason, symbol, strategy_name)
                return False, reason

            # 6. Max trades per day
            if self._daily.trades_today >= self._max_trades_per_day:
                reason = (
                    f"MAX_DAILY_TRADES: {self._daily.trades_today}/{self._max_trades_per_day}"
                )
                self._record_block("DAILY_TRADES", reason, symbol, strategy_name)
                return False, reason

            # 7. Max positions per symbol
            sym_count = sum(
                1 for p in open_positions
                if p.get("symbol", "").upper() == symbol.upper()
            )
            if sym_count >= self._max_per_symbol:
                reason = (
                    f"MAX_PER_SYMBOL: {sym_count}/{self._max_per_symbol} "
                    f"positions already on {symbol}"
                )
                self._record_block("SYMBOL_LIMIT", reason, symbol, strategy_name)
                return False, reason

            # 8. Max positions per strategy
            strat_count = sum(
                1 for p in open_positions
                if p.get("strategy", "") == strategy_name
            )
            if strat_count >= self._max_per_strategy:
                reason = (
                    f"MAX_PER_STRATEGY: {strat_count}/{self._max_per_strategy} "
                    f"positions from {strategy_name}"
                )
                self._record_block("STRATEGY_LIMIT", reason, symbol, strategy_name)
                return False, reason

            # 9. Asset class exposure cap
            ac_exposure = sum(
                p.get("notional_usd", 0.0) for p in open_positions
                if p.get("asset_class", "").upper() == asset_class.upper()
            )
            if ac_exposure + proposed_risk_usd > self._max_asset_class_usd:
                reason = (
                    f"ASSET_CLASS_CAP: {asset_class} exposure "
                    f"${ac_exposure + proposed_risk_usd:.2f} > "
                    f"${self._max_asset_class_usd:.2f}"
                )
                self._record_block("ASSET_CLASS_CAP", reason, symbol, strategy_name)
                return False, reason

            # 10. Correlated symbol exposure cap
            corr_group = _get_correlation_group(symbol)
            if corr_group:
                corr_exposure = sum(
                    p.get("notional_usd", 0.0) for p in open_positions
                    if _get_correlation_group(p.get("symbol", "")) == corr_group
                )
                if corr_exposure + proposed_risk_usd > self._max_correlated_usd:
                    reason = (
                        f"CORRELATED_CAP: group={corr_group} exposure "
                        f"${corr_exposure + proposed_risk_usd:.2f} > "
                        f"${self._max_correlated_usd:.2f}"
                    )
                    self._record_block("CORRELATED_CAP", reason, symbol, strategy_name)
                    return False, reason

            # 11. Consecutive loss cooldown
            if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
                remaining = (self._cooldown_until - datetime.now(timezone.utc)).seconds // 60
                reason = (
                    f"COOLDOWN: {self._daily.consecutive_losses} consecutive losses — "
                    f"cooldown active for {remaining} more minutes"
                )
                self._record_block("COOLDOWN", reason, symbol, strategy_name)
                return False, reason

            # 12. Stale market data
            if self._last_data_timestamp:
                stale_threshold = timedelta(minutes=self._stale_data_kill)
                age = datetime.now(timezone.utc) - self._last_data_timestamp
                if age > stale_threshold:
                    reason = (
                        f"STALE_DATA: last update {age.seconds//60}m ago "
                        f"(threshold={self._stale_data_kill}m)"
                    )
                    self._record_block("STALE_DATA", reason, symbol, strategy_name)
                    self._engage_kill(reason)
                    return False, reason

            return True, ""

    # ------------------------------------------------------------------
    # PUBLIC: Record outcomes (called by scheduler after fill/reject)
    # ------------------------------------------------------------------

    def record_trade_opened(self, symbol: str, strategy: str, pnl_context: float = 0.0):
        """Called when a new order fills."""
        with self._lock:
            self._daily.trades_today += 1
            self._persist_state()

    def record_trade_closed(self, realized_pnl_usd: float, new_balance: float):
        """Called when a position closes."""
        with self._lock:
            self._daily.realized_pnl_usd += realized_pnl_usd
            self._update_balance(new_balance)
            if realized_pnl_usd < 0:
                self._daily.consecutive_losses += 1
                self._daily.consecutive_wins = 0
                if self._daily.consecutive_losses >= self._cooldown_after_losses:
                    self._cooldown_until = datetime.now(timezone.utc) + timedelta(
                        minutes=self._cooldown_minutes
                    )
                    logger.warning(
                        f"[RiskGovernor] COOLDOWN engaged after "
                        f"{self._daily.consecutive_losses} consecutive losses — "
                        f"{self._cooldown_minutes}m pause"
                    )
                    if self._alert:
                        self._alert.emit(
                            "RISK_LIMIT_HIT",
                            f"Cooldown: {self._daily.consecutive_losses} consecutive losses",
                            {"cooldown_minutes": self._cooldown_minutes},
                        )
            else:
                self._daily.consecutive_losses = 0
                self._daily.consecutive_wins += 1
                self._cooldown_until = None
            self._persist_state()

    def record_unrealized_pnl(self, total_unrealized_usd: float):
        """Called by position monitor loop with current unrealised PnL."""
        with self._lock:
            self._daily.unrealized_pnl_usd = total_unrealized_usd
            # Check drawdown in real-time
            drawdown_pct = self._get_drawdown_pct()
            if drawdown_pct >= self._max_drawdown_pct * 0.8:
                logger.warning(
                    f"[RiskGovernor] Drawdown alert: {drawdown_pct:.2f}% "
                    f"(limit={self._max_drawdown_pct}%)"
                )
                if self._alert:
                    self._alert.emit(
                        "DRAWDOWN_WARNING",
                        f"Drawdown at {drawdown_pct:.2f}% of {self._max_drawdown_pct}% limit",
                        {"drawdown_pct": drawdown_pct, "limit_pct": self._max_drawdown_pct},
                    )

    def record_broker_reject(self, symbol: str, reason: str):
        """Called when a broker rejects an order."""
        with self._lock:
            self._daily.broker_rejects_today += 1
            if self._daily.broker_rejects_today >= self._broker_reject_kill:
                kill_reason = (
                    f"BROKER_REJECTS: {self._daily.broker_rejects_today} rejects today "
                    f"(threshold={self._broker_reject_kill})"
                )
                self._engage_kill(kill_reason)
            self._persist_state()

    def record_reconciliation_mismatch(self, details: str):
        """Called by reconciliation loop when state diverges from broker."""
        if self._recon_mismatch_kill:
            reason = f"RECONCILIATION_MISMATCH: {details}"
            logger.critical(f"[RiskGovernor] {reason}")
            self._engage_kill(reason)

    def mark_data_received(self):
        """Called by market data loop each time fresh data arrives."""
        with self._lock:
            self._last_data_timestamp = datetime.now(timezone.utc)

    def update_balance(self, new_balance: float):
        """Update account balance from external source."""
        with self._lock:
            self._update_balance(new_balance)

    def engage_kill_switch(self, reason: str):
        """External trigger — e.g. from API endpoint."""
        with self._lock:
            self._engage_kill(reason)

    def reset_daily_state(self, operator: str = "system"):
        """Manually reset daily counters (e.g. new trading day or operator action)."""
        with self._lock:
            self._daily = GovernorDailyState(date_utc=date.today().isoformat())
            self._cooldown_until = None
            logger.info(f"[RiskGovernor] Daily state reset by {operator}")
            self._persist_state()

    def reset_kill_switch(self, operator: str = "system"):
        """Reset global kill switch (requires explicit operator action)."""
        with self._lock:
            self._global_kill = False
            self._kill_reasons.clear()
            logger.warning(f"[RiskGovernor] Kill switch RESET by {operator}")
            self._persist_state()

    # ------------------------------------------------------------------
    # STATUS
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return full risk governor status for API / monitoring."""
        with self._lock:
            self._maybe_reset_daily()
            drawdown_pct = self._get_drawdown_pct()
            max_day_loss_usd = self._account_balance * (self._max_daily_loss_pct / 100.0)
            total_day_pnl = self._daily.realized_pnl_usd + self._daily.unrealized_pnl_usd

            cooldown_remaining_s = 0
            if self._cooldown_until:
                remaining = self._cooldown_until - datetime.now(timezone.utc)
                cooldown_remaining_s = max(0, int(remaining.total_seconds()))

            return {
                "global_kill_active": self._global_kill,
                "kill_reasons": list(self._kill_reasons),
                "daily": {
                    "date_utc": self._daily.date_utc,
                    "realized_pnl_usd": round(self._daily.realized_pnl_usd, 2),
                    "unrealized_pnl_usd": round(self._daily.unrealized_pnl_usd, 2),
                    "total_pnl_usd": round(total_day_pnl, 2),
                    "max_loss_usd": round(max_day_loss_usd, 2),
                    "loss_used_pct": round(
                        abs(min(0, total_day_pnl)) / max_day_loss_usd * 100, 2
                    ) if max_day_loss_usd > 0 else 0,
                    "trades_today": self._daily.trades_today,
                    "broker_rejects_today": self._daily.broker_rejects_today,
                    "consecutive_losses": self._daily.consecutive_losses,
                },
                "drawdown_pct": round(drawdown_pct, 3),
                "max_drawdown_pct": self._max_drawdown_pct,
                "peak_balance": round(self._peak_balance, 2),
                "current_balance": round(self._account_balance, 2),
                "cooldown_active": self._cooldown_until is not None and
                    datetime.now(timezone.utc) < self._cooldown_until,
                "cooldown_remaining_seconds": cooldown_remaining_s,
                "limits": {
                    "max_risk_per_trade_pct": self._max_risk_pct,
                    "max_daily_loss_pct": self._max_daily_loss_pct,
                    "max_drawdown_pct": self._max_drawdown_pct,
                    "max_concurrent": self._max_concurrent,
                    "max_trades_per_day": self._max_trades_per_day,
                    "max_per_symbol": self._max_per_symbol,
                    "max_per_strategy": self._max_per_strategy,
                    "max_asset_class_usd": self._max_asset_class_usd,
                    "max_correlated_usd": self._max_correlated_usd,
                    "cooldown_after_losses": self._cooldown_after_losses,
                    "broker_reject_kill_threshold": self._broker_reject_kill,
                },
                "recent_blocks": [
                    b.to_dict() for b in self._block_history[-10:]
                ],
                "last_data_received": (
                    self._last_data_timestamp.isoformat()
                    if self._last_data_timestamp else None
                ),
            }

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _maybe_reset_daily(self):
        """Reset daily counters at UTC midnight (called under lock)."""
        today = date.today().isoformat()
        if self._daily.date_utc != today:
            logger.info(
                f"[RiskGovernor] Day boundary: {self._daily.date_utc} → {today} — "
                "resetting daily counters"
            )
            self._daily = GovernorDailyState(date_utc=today)
            self._cooldown_until = None
            self._persist_state()

    def _get_drawdown_pct(self) -> float:
        if self._peak_balance <= 0:
            return 0.0
        return (
            (self._peak_balance - self._account_balance) / self._peak_balance
        ) * 100.0

    def _update_balance(self, new_balance: float):
        """Update balance and peak (must be called under lock)."""
        self._account_balance = new_balance
        if new_balance > self._peak_balance:
            self._peak_balance = new_balance

    def _engage_kill(self, reason: str):
        """Engage global kill switch and block ESM (called under lock)."""
        if not self._global_kill:
            self._global_kill = True
            self._kill_reasons.append(reason)
            logger.critical(f"[RiskGovernor] 🔴 GLOBAL KILL ENGAGED: {reason}")
            if self._esm:
                self._esm.engage_kill_switch(reason, operator="risk_governor")
            if self._alert:
                self._alert.emit("SYSTEM_BLOCKED", f"Kill switch: {reason}", {})
            self._persist_state()
        else:
            if reason not in self._kill_reasons:
                self._kill_reasons.append(reason)

    def _trigger_live_block(self, reason: str):
        """Block live trading (not full kill) via ESM."""
        if self._esm and not self._global_kill:
            try:
                self._esm.block_live(reason=reason, operator="risk_governor")
            except Exception:
                pass
        if self._alert:
            self._alert.emit("RISK_LIMIT_HIT", reason, {})

    def _record_block(
        self, block_type: str, reason: str,
        symbol: Optional[str], strategy: Optional[str],
        details: dict = None,
    ):
        event = GovernorBlockEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            reason=reason,
            block_type=block_type,
            symbol=symbol,
            strategy=strategy,
            details=details or {},
        )
        self._block_history.append(event)
        if len(self._block_history) > 500:
            self._block_history.pop(0)
        logger.warning(f"[RiskGovernor] BLOCKED [{block_type}]: {reason}")

    # ------------------------------------------------------------------
    # PERSISTENCE
    # ------------------------------------------------------------------

    def _persist_state(self):
        try:
            data = {
                "daily": self._daily.to_dict(),
                "global_kill": self._global_kill,
                "kill_reasons": self._kill_reasons,
                "peak_balance": self._peak_balance,
                "account_balance": self._account_balance,
                "cooldown_until": (
                    self._cooldown_until.isoformat()
                    if self._cooldown_until else None
                ),
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            self._state_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"[RiskGovernor] Failed to persist state: {e}")

    def _load_state(self):
        """Load persisted state on startup."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text())
            stored_daily = data.get("daily", {})
            stored_date = stored_daily.get("date_utc", "")
            today = date.today().isoformat()

            if stored_date == today:
                # Same day — restore counters
                self._daily = GovernorDailyState.from_dict(stored_daily)
                logger.info(
                    f"[RiskGovernor] Restored daily state: "
                    f"trades={self._daily.trades_today} "
                    f"realized_pnl=${self._daily.realized_pnl_usd:.2f}"
                )
            else:
                # Different day — start fresh
                self._daily = GovernorDailyState(date_utc=today)
                logger.info("[RiskGovernor] New day — daily state reset")

            # Restore global kill state
            self._global_kill = data.get("global_kill", False)
            self._kill_reasons = data.get("kill_reasons", [])
            if self._global_kill:
                logger.critical(
                    f"[RiskGovernor] Restarted with KILL SWITCH active: "
                    f"{self._kill_reasons}"
                )

            # Restore balance/peak
            self._peak_balance = data.get("peak_balance", self._peak_balance)
            stored_balance = data.get("account_balance", self._account_balance)
            if stored_balance > 0:
                self._account_balance = stored_balance

            # Restore cooldown (if still valid)
            cd = data.get("cooldown_until")
            if cd:
                cd_dt = datetime.fromisoformat(cd)
                if datetime.now(timezone.utc) < cd_dt:
                    self._cooldown_until = cd_dt
                    logger.warning(
                        f"[RiskGovernor] Cooldown still active until {cd_dt.isoformat()}"
                    )

        except Exception as e:
            logger.error(f"[RiskGovernor] Failed to load state: {e}")

"""
APEX MULTI-MARKET TJR ENGINE
Position Manager — Phase 2: Autonomous position management.

Handles all post-entry position lifecycle:
  - Stop-loss verification (price crossed SL)
  - Take-profit verification (price crossed TP)
  - Break-even move (move SL to entry when price hits BE threshold)
  - Trailing stop updates (step-trail or ATR-trail)
  - Time-based exits (max hold hours per strategy)
  - Session-close exits (close before weekend / session end)
  - Stale order cleanup (cancel unfilled limit orders older than N minutes)
  - Orphaned position detection (open in engine but not at broker)

ARCHITECTURE RULE:
  This module only manages positions — it does NOT generate signals.
  Close/modify requests go through BrokerManager.submit_order() or
  connector.close_position() — never direct broker calls.

Called from PositionMonitorLoop in autonomous_scheduler.py every 10s.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# POSITION MANAGEMENT RULES  (loaded from config)
# ---------------------------------------------------------------------------

@dataclass
class PositionManagementConfig:
    """Config-driven rules for a strategy."""
    strategy_name: str
    max_hold_hours: float = 48.0      # Time-based exit
    be_trigger_r: float = 1.0         # Move SL to BE when price is R*1.0 in profit
    trailing_enabled: bool = False
    trailing_type: str = "step"       # step | atr
    trailing_step_pips: float = 10.0  # Step-trail: lock in every N pips
    session_close_exit: bool = True   # Exit at session end (configured sessions)
    max_weekend_hold: bool = False    # Exit before weekend (Fri 21:00 UTC)
    stale_order_minutes: float = 30.0 # Cancel unfilled limit/stop orders after N min


@dataclass
class PositionAction:
    """An action the manager wants to take on a position."""
    position_id: str
    symbol: str
    action: str    # "CLOSE" | "MOVE_SL" | "MOVE_TP" | "CANCEL_ORDER"
    reason: str
    new_sl: Optional[float] = None
    new_tp: Optional[float] = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "action": self.action,
            "reason": self.reason,
            "new_sl": self.new_sl,
            "new_tp": self.new_tp,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# POSITION MANAGER
# ---------------------------------------------------------------------------

class PositionManager:
    """
    Autonomous position management engine.

    Called every 10s by PositionMonitorLoop.
    Returns a list of PositionActions to execute.
    The scheduler executes them through BrokerManager.
    """

    def __init__(self, config: dict, alert_dispatcher=None):
        self._config = config
        self._alert = alert_dispatcher
        self._lock = threading.Lock()

        # Load management rules from config
        mgmt_cfg = config.get("autonomy", {}).get("position_management", {})
        self._default_max_hold_hours = mgmt_cfg.get("max_hold_hours", 48.0)
        self._be_trigger_r = mgmt_cfg.get("be_trigger_r", 1.0)
        self._trailing_enabled = mgmt_cfg.get("trailing_enabled", False)
        self._trailing_step_pips = mgmt_cfg.get("trailing_step_pips", 10.0)
        self._session_close_exit = mgmt_cfg.get("session_close_exit", True)
        self._weekend_close = mgmt_cfg.get("close_before_weekend", True)
        self._stale_order_minutes = mgmt_cfg.get("stale_order_minutes", 30.0)

        # Per-strategy overrides
        self._strategy_configs: Dict[str, PositionManagementConfig] = {}
        self._load_strategy_configs()

        # State
        self._last_pass_at: Optional[str] = None
        self._actions_taken: List[dict] = []   # Recent actions log
        self._peak_prices: Dict[str, float] = {}   # For trailing stops: {position_id: peak}

        logger.info(
            f"[PositionManager] Initialised — "
            f"max_hold={self._default_max_hold_hours}h "
            f"be_trigger_r={self._be_trigger_r} "
            f"trailing={self._trailing_enabled}"
        )

    def _load_strategy_configs(self):
        """Build per-strategy management configs from config.autonomy.position_management.strategies."""
        strat_cfgs = (
            self._config.get("autonomy", {})
            .get("position_management", {})
            .get("strategies", {})
        )
        known = [
            "asian_range_breakout", "orb_vwap", "vwap_sd_reversion",
            "commodity_trend", "gap_and_go",
            "crypto_funding_reversion", "crypto_monday_range",
            "prediction_market_arb",
        ]
        for name in known:
            cfg = strat_cfgs.get(name, {})
            self._strategy_configs[name] = PositionManagementConfig(
                strategy_name=name,
                max_hold_hours=cfg.get("max_hold_hours", self._default_max_hold_hours),
                be_trigger_r=cfg.get("be_trigger_r", self._be_trigger_r),
                trailing_enabled=cfg.get("trailing_enabled", self._trailing_enabled),
                trailing_step_pips=cfg.get("trailing_step_pips", self._trailing_step_pips),
                session_close_exit=cfg.get("session_close_exit", self._session_close_exit),
                max_weekend_hold=cfg.get("max_weekend_hold", False),
                stale_order_minutes=cfg.get("stale_order_minutes", self._stale_order_minutes),
            )

    # ------------------------------------------------------------------
    # PUBLIC: Main evaluation pass
    # ------------------------------------------------------------------

    def evaluate_positions(
        self,
        open_positions: List[dict],
        current_prices: Dict[str, float],
        open_orders: List[dict] = None,
    ) -> List[PositionAction]:
        """
        Evaluate all open positions and pending orders.
        Returns list of PositionActions to be executed by the scheduler.

        Position dict expected fields:
          id, symbol, strategy, direction (BUY/SELL),
          entry_price, stop_loss, take_profit, entry_time (ISO str),
          units, risk_usd, r_multiple (optional)
        """
        actions: List[PositionAction] = []
        now = datetime.now(timezone.utc)
        self._last_pass_at = now.isoformat()

        for pos in open_positions:
            pos_id   = pos.get("id", "unknown")
            symbol   = pos.get("symbol", "")
            strategy = pos.get("strategy", "")
            direction = pos.get("direction", "BUY").upper()
            entry    = float(pos.get("entry_price", 0.0))
            sl       = float(pos.get("stop_loss", 0.0))
            tp       = float(pos.get("take_profit", 0.0))
            entry_ts = pos.get("entry_time")
            units    = float(pos.get("units", 0.0))

            current = current_prices.get(symbol)
            if current is None or current <= 0:
                logger.debug(f"[PositionManager] No price for {symbol} — skipping {pos_id}")
                continue

            mgmt = self._strategy_configs.get(
                strategy,
                PositionManagementConfig(strategy_name=strategy)
            )

            # ---- 1. SL hit check (should rarely trigger — broker handles this)
            if sl > 0:
                sl_hit = (direction == "BUY" and current <= sl) or \
                         (direction == "SELL" and current >= sl)
                if sl_hit:
                    actions.append(PositionAction(
                        position_id=pos_id, symbol=symbol,
                        action="CLOSE",
                        reason=f"SL_HIT: price={current} sl={sl}",
                    ))
                    continue

            # ---- 2. TP hit check
            if tp > 0:
                tp_hit = (direction == "BUY" and current >= tp) or \
                         (direction == "SELL" and current <= tp)
                if tp_hit:
                    actions.append(PositionAction(
                        position_id=pos_id, symbol=symbol,
                        action="CLOSE",
                        reason=f"TP_HIT: price={current} tp={tp}",
                    ))
                    continue

            # ---- 3. Break-even move
            if entry > 0 and sl > 0:
                be_action = self._check_break_even(
                    pos_id, symbol, direction, entry, sl, tp, current, mgmt
                )
                if be_action:
                    actions.append(be_action)

            # ---- 4. Trailing stop
            if mgmt.trailing_enabled and entry > 0 and sl > 0:
                trail_action = self._check_trailing(
                    pos_id, symbol, direction, entry, sl, current, mgmt
                )
                if trail_action:
                    actions.append(trail_action)

            # ---- 5. Time-based exit
            if entry_ts:
                time_action = self._check_time_exit(
                    pos_id, symbol, strategy, entry_ts, mgmt, now
                )
                if time_action:
                    actions.append(time_action)
                    continue

            # ---- 6. Weekend close
            if mgmt.max_weekend_hold or self._weekend_close:
                weekend_action = self._check_weekend_close(
                    pos_id, symbol, now
                )
                if weekend_action:
                    actions.append(weekend_action)
                    continue

            # ---- 7. Session close
            if mgmt.session_close_exit:
                session_action = self._check_session_close(
                    pos_id, symbol, strategy, now
                )
                if session_action:
                    actions.append(session_action)

        # ---- 8. Stale pending order cleanup
        if open_orders:
            stale = self._check_stale_orders(open_orders, now)
            actions.extend(stale)

        # ---- 9. Orphaned position detection (engine ↔ broker mismatch)
        # Note: reconciliation loop handles this — we just log here

        # Log pass summary
        if actions:
            logger.info(
                f"[PositionManager] Pass complete: "
                f"{len(open_positions)} positions, {len(actions)} actions queued"
            )
            for a in actions:
                logger.info(
                    f"[PositionManager] ACTION: {a.action} {a.symbol} [{a.position_id}]: "
                    f"{a.reason}"
                )
        else:
            logger.debug(
                f"[PositionManager] Pass complete: "
                f"{len(open_positions)} positions, no actions needed"
            )

        # Store recent actions log
        with self._lock:
            for a in actions:
                self._actions_taken.append(a.to_dict())
            if len(self._actions_taken) > 200:
                self._actions_taken = self._actions_taken[-200:]

        return actions

    # ------------------------------------------------------------------
    # INTERNAL CHECKS
    # ------------------------------------------------------------------

    def _check_break_even(
        self,
        pos_id: str, symbol: str, direction: str,
        entry: float, sl: float, tp: float,
        current: float, mgmt: PositionManagementConfig,
    ) -> Optional[PositionAction]:
        """
        Move SL to break-even when position is up by be_trigger_r * (entry - sl) pips.
        Only triggers once (SL already at or above entry means BE already moved).
        """
        if entry <= 0 or sl <= 0:
            return None

        sl_distance = abs(entry - sl)
        be_threshold = mgmt.be_trigger_r * sl_distance

        if direction == "BUY":
            profit = current - entry
            # Already at BE or better
            if sl >= entry:
                return None
            if profit >= be_threshold:
                new_sl = entry + 0.00001  # Just above entry (small buffer)
                return PositionAction(
                    position_id=pos_id, symbol=symbol,
                    action="MOVE_SL",
                    reason=(
                        f"BREAK_EVEN: profit={profit:.5f} >= threshold={be_threshold:.5f} — "
                        f"moving SL from {sl:.5f} to {new_sl:.5f}"
                    ),
                    new_sl=round(new_sl, 5),
                )
        else:  # SELL
            profit = entry - current
            if sl <= entry:
                return None
            if profit >= be_threshold:
                new_sl = entry - 0.00001
                return PositionAction(
                    position_id=pos_id, symbol=symbol,
                    action="MOVE_SL",
                    reason=(
                        f"BREAK_EVEN: profit={profit:.5f} >= threshold={be_threshold:.5f} — "
                        f"moving SL from {sl:.5f} to {new_sl:.5f}"
                    ),
                    new_sl=round(new_sl, 5),
                )
        return None

    def _check_trailing(
        self,
        pos_id: str, symbol: str, direction: str,
        entry: float, sl: float, current: float,
        mgmt: PositionManagementConfig,
    ) -> Optional[PositionAction]:
        """
        Step-trail: lock in every trailing_step_pips of move in our favour.
        Updates peak price and moves SL one step below peak.
        """
        pip_size = 0.0001  # Default; TODO: use symbol-specific pip sizes
        if "JPY" in symbol.upper():
            pip_size = 0.01

        step = mgmt.trailing_step_pips * pip_size

        peak_key = f"{pos_id}_peak"
        with self._lock:
            if direction == "BUY":
                peak = self._peak_prices.get(peak_key, entry)
                if current > peak:
                    self._peak_prices[peak_key] = current
                    peak = current
                new_trail_sl = round(peak - step, 5)
                if new_trail_sl > sl and new_trail_sl > entry:
                    return PositionAction(
                        position_id=pos_id, symbol=symbol,
                        action="MOVE_SL",
                        reason=(
                            f"TRAIL: peak={peak:.5f} step={step:.5f} → "
                            f"new_sl={new_trail_sl:.5f}"
                        ),
                        new_sl=new_trail_sl,
                    )
            else:  # SELL
                peak = self._peak_prices.get(peak_key, entry)
                if current < peak:
                    self._peak_prices[peak_key] = current
                    peak = current
                new_trail_sl = round(peak + step, 5)
                if new_trail_sl < sl and new_trail_sl < entry:
                    return PositionAction(
                        position_id=pos_id, symbol=symbol,
                        action="MOVE_SL",
                        reason=(
                            f"TRAIL: peak={peak:.5f} step={step:.5f} → "
                            f"new_sl={new_trail_sl:.5f}"
                        ),
                        new_sl=new_trail_sl,
                    )
        return None

    def _check_time_exit(
        self,
        pos_id: str, symbol: str, strategy: str,
        entry_ts: str, mgmt: PositionManagementConfig,
        now: datetime,
    ) -> Optional[PositionAction]:
        """Close position if held longer than max_hold_hours."""
        try:
            entry_dt = datetime.fromisoformat(entry_ts)
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.replace(tzinfo=timezone.utc)
            hold_hours = (now - entry_dt).total_seconds() / 3600.0
            if hold_hours >= mgmt.max_hold_hours:
                return PositionAction(
                    position_id=pos_id, symbol=symbol,
                    action="CLOSE",
                    reason=(
                        f"TIME_EXIT: held {hold_hours:.1f}h >= "
                        f"{mgmt.max_hold_hours}h limit for {strategy}"
                    ),
                )
        except Exception as e:
            logger.debug(f"[PositionManager] time exit parse error: {e}")
        return None

    def _check_weekend_close(
        self,
        pos_id: str, symbol: str, now: datetime,
    ) -> Optional[PositionAction]:
        """Close all positions before Friday 21:00 UTC to avoid weekend gap risk."""
        if now.weekday() == 4 and now.hour >= 21:   # Friday 21:00 UTC
            return PositionAction(
                position_id=pos_id, symbol=symbol,
                action="CLOSE",
                reason=f"WEEKEND_CLOSE: Friday {now.strftime('%H:%M')} UTC",
            )
        return None

    def _check_session_close(
        self,
        pos_id: str, symbol: str, strategy: str, now: datetime
    ) -> Optional[PositionAction]:
        """
        Close session-specific positions at session end.
        E.g. AsianRangeBreakout positions should close by London open (07:00 UTC).
        """
        # Asian range: close by 07:00 UTC
        if strategy == "asian_range_breakout":
            if now.hour == 7 and now.minute == 0:
                return PositionAction(
                    position_id=pos_id, symbol=symbol,
                    action="CLOSE",
                    reason=f"SESSION_CLOSE: asian_range_breakout — London open",
                )
        # ORB/VWAP: US market close 21:00 UTC
        if strategy in ("orb_vwap", "gap_and_go"):
            if now.hour == 21 and now.minute == 0:
                return PositionAction(
                    position_id=pos_id, symbol=symbol,
                    action="CLOSE",
                    reason=f"SESSION_CLOSE: {strategy} — US market close",
                )
        return None

    def _check_stale_orders(
        self,
        open_orders: List[dict],
        now: datetime,
    ) -> List[PositionAction]:
        """Cancel limit/stop orders that have been pending too long."""
        actions = []
        threshold = timedelta(minutes=self._stale_order_minutes)
        for order in open_orders:
            order_id = order.get("id", "unknown")
            symbol   = order.get("symbol", "")
            order_type = order.get("type", "MARKET").upper()
            placed_ts = order.get("placed_at") or order.get("created_at")
            if order_type == "MARKET" or not placed_ts:
                continue
            try:
                placed_dt = datetime.fromisoformat(placed_ts)
                if placed_dt.tzinfo is None:
                    placed_dt = placed_dt.replace(tzinfo=timezone.utc)
                age = now - placed_dt
                if age > threshold:
                    actions.append(PositionAction(
                        position_id=order_id, symbol=symbol,
                        action="CANCEL_ORDER",
                        reason=(
                            f"STALE_ORDER: {order_type} order pending "
                            f"{age.seconds//60}m > {self._stale_order_minutes}m"
                        ),
                    ))
            except Exception as e:
                logger.debug(f"[PositionManager] stale order parse error: {e}")
        return actions

    # ------------------------------------------------------------------
    # STATUS
    # ------------------------------------------------------------------

    def get_recent_actions(self, limit: int = 50) -> List[dict]:
        """Return recent position management actions (API-friendly)."""
        with self._lock:
            return list(self._actions_taken[-limit:])

    def status(self) -> dict:
        """Return position manager status."""
        with self._lock:
            return {
                "last_pass_at": self._last_pass_at,
                "recent_actions": list(self._actions_taken[-20:]),
                "total_actions_taken": len(self._actions_taken),
                "config": {
                    "max_hold_hours": self._default_max_hold_hours,
                    "be_trigger_r": self._be_trigger_r,
                    "trailing_enabled": self._trailing_enabled,
                    "trailing_step_pips": self._trailing_step_pips,
                    "session_close_exit": self._session_close_exit,
                    "weekend_close": self._weekend_close,
                    "stale_order_minutes": self._stale_order_minutes,
                },
            }

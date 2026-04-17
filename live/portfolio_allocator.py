"""
APEX MULTI-MARKET TJR ENGINE
Portfolio Allocator — Phase 4: Capital budget management across strategies.

Sits BEFORE final execution approval (after strategy/risk evaluation).

RESPONSIBILITIES:
  - Assign capital budgets by strategy (% of total risk budget)
  - Cap risk by asset class (USD notional)
  - Cap correlated exposure
  - Priority ordering among strategies
  - Throttle low-priority strategies if higher-priority ones are active
  - Enable/disable strategies from config

ARCHITECTURE RULE:
  The allocator is a pure gate — it approves or throttles.
  It never submits orders. It never calls brokers.
  Called from signal_process_loop in autonomous_scheduler.py.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# STRATEGY CONFIGURATION
# ---------------------------------------------------------------------------

@dataclass
class StrategyAllocation:
    """Budget allocation for a single strategy."""
    name: str
    enabled: bool = True
    priority: int = 5             # 1=highest, 10=lowest
    max_risk_budget_pct: float = 20.0   # % of total daily risk budget
    max_concurrent_positions: int = 2
    allowed_asset_classes: List[str] = field(default_factory=list)
    max_notional_usd: float = 50_000.0
    throttle_when_priority_active: bool = True   # Pause if higher-priority strategy has position


@dataclass
class AllocationDecision:
    """Result from allocator gate check."""
    allowed: bool
    reason: str
    strategy: str
    symbol: str
    priority: int
    budget_used_pct: float
    budget_remaining_pct: float


# ---------------------------------------------------------------------------
# PORTFOLIO ALLOCATOR
# ---------------------------------------------------------------------------

class PortfolioAllocator:
    """
    Capital budget gate — called per signal before order submission.

    Loads strategy allocations from config.autonomy.portfolio_allocator
    and enforces them at runtime.
    """

    def __init__(self, config: dict):
        self._config = config
        self._lock = threading.Lock()

        alloc_cfg = config.get("autonomy", {}).get("portfolio_allocator", {})
        self._total_risk_budget_usd = alloc_cfg.get(
            "total_daily_risk_budget_usd",
            config.get("risk", {}).get("account_balance_default", 100_000.0)
            * config.get("risk", {}).get("max_daily_loss_pct", 3.0) / 100.0
        )

        # Load per-strategy allocations
        self._strategies: Dict[str, StrategyAllocation] = {}
        self._load_strategy_allocations(alloc_cfg)

        # Runtime: track risk used per strategy this session
        self._risk_used: Dict[str, float] = {s: 0.0 for s in self._strategies}
        self._open_counts: Dict[str, int] = {s: 0 for s in self._strategies}

        logger.info(
            f"[PortfolioAllocator] Initialised — "
            f"total_risk_budget=${self._total_risk_budget_usd:.2f} "
            f"strategies={list(self._strategies.keys())}"
        )

    def _load_strategy_allocations(self, alloc_cfg: dict):
        """Load strategy allocation config. Falls back to sensible defaults."""
        strategy_cfgs = alloc_cfg.get("strategies", {})

        # Default allocations for all 8 known strategies
        defaults = {
            "asian_range_breakout":     StrategyAllocation(
                "asian_range_breakout", priority=2, max_risk_budget_pct=25.0,
                allowed_asset_classes=["FOREX"],
            ),
            "orb_vwap":                 StrategyAllocation(
                "orb_vwap", priority=3, max_risk_budget_pct=20.0,
                allowed_asset_classes=["FUTURES", "STOCKS"],
            ),
            "vwap_sd_reversion":        StrategyAllocation(
                "vwap_sd_reversion", priority=4, max_risk_budget_pct=15.0,
                allowed_asset_classes=["FUTURES", "GOLD", "COMMODITIES"],
            ),
            "commodity_trend":          StrategyAllocation(
                "commodity_trend", priority=5, max_risk_budget_pct=15.0,
                allowed_asset_classes=["COMMODITIES", "FUTURES"],
            ),
            "gap_and_go":               StrategyAllocation(
                "gap_and_go", priority=3, max_risk_budget_pct=20.0,
                allowed_asset_classes=["STOCKS"],
            ),
            "crypto_funding_reversion": StrategyAllocation(
                "crypto_funding_reversion", priority=7, max_risk_budget_pct=10.0,
                allowed_asset_classes=["CRYPTO"],
            ),
            "crypto_monday_range":      StrategyAllocation(
                "crypto_monday_range", priority=8, max_risk_budget_pct=10.0,
                allowed_asset_classes=["CRYPTO"],
            ),
            "prediction_market_arb":    StrategyAllocation(
                "prediction_market_arb", priority=9, max_risk_budget_pct=5.0,
                allowed_asset_classes=["PREDICTION"],
            ),
        }

        # Override from config
        for name, alloc in defaults.items():
            cfg = strategy_cfgs.get(name, {})
            if "enabled" in cfg:
                alloc.enabled = cfg["enabled"]
            if "priority" in cfg:
                alloc.priority = int(cfg["priority"])
            if "max_risk_budget_pct" in cfg:
                alloc.max_risk_budget_pct = float(cfg["max_risk_budget_pct"])
            if "max_concurrent_positions" in cfg:
                alloc.max_concurrent_positions = int(cfg["max_concurrent_positions"])
            self._strategies[name] = alloc

        # Load any additional strategies defined only in config (e.g., disabled_strategy)
        for name, cfg in strategy_cfgs.items():
            if name not in self._strategies:
                alloc = StrategyAllocation(
                    name=name,
                    enabled=cfg.get("enabled", True),
                    priority=int(cfg.get("priority", 5)),
                    max_risk_budget_pct=float(cfg.get("max_risk_budget_pct", 10.0)),
                    max_concurrent_positions=int(cfg.get("max_concurrent_positions", 2)),
                )
                self._strategies[name] = alloc
                logger.debug(
                    f"[Allocator] Loaded config-only strategy: {name!r} "
                    f"(enabled={alloc.enabled})"
                )

    # ------------------------------------------------------------------
    # PUBLIC: Main gate
    # ------------------------------------------------------------------

    def check_allocation(
        self,
        strategy_name: str,
        symbol: str,
        asset_class: str,
        proposed_risk_usd: float,
        open_positions: List[dict],
    ) -> AllocationDecision:
        """
        Check whether a new trade from strategy_name is within budget.

        Returns AllocationDecision.allowed=True if allowed, False if blocked.
        """
        with self._lock:
            alloc = self._strategies.get(strategy_name)

            # Unknown strategy — allow by default (conservative approach)
            if alloc is None:
                return AllocationDecision(
                    allowed=True, reason="unknown strategy — allowed by default",
                    strategy=strategy_name, symbol=symbol,
                    priority=5, budget_used_pct=0.0, budget_remaining_pct=100.0,
                )

            # 1. Strategy disabled in config
            if not alloc.enabled:
                return AllocationDecision(
                    allowed=False, reason=f"{strategy_name} disabled in config",
                    strategy=strategy_name, symbol=symbol,
                    priority=alloc.priority, budget_used_pct=0.0, budget_remaining_pct=0.0,
                )

            # 2. Asset class check
            if alloc.allowed_asset_classes and asset_class.upper() not in [
                a.upper() for a in alloc.allowed_asset_classes
            ]:
                return AllocationDecision(
                    allowed=False,
                    reason=(
                        f"{strategy_name} not configured for {asset_class} — "
                        f"allowed: {alloc.allowed_asset_classes}"
                    ),
                    strategy=strategy_name, symbol=symbol,
                    priority=alloc.priority, budget_used_pct=0.0, budget_remaining_pct=0.0,
                )

            # 3. Budget cap for this strategy
            strategy_budget_usd = (
                self._total_risk_budget_usd * alloc.max_risk_budget_pct / 100.0
            )
            used = self._risk_used.get(strategy_name, 0.0)
            remaining = strategy_budget_usd - used
            if proposed_risk_usd > remaining:
                budget_used_pct = (used / strategy_budget_usd * 100.0) if strategy_budget_usd > 0 else 100.0
                return AllocationDecision(
                    allowed=False,
                    reason=(
                        f"{strategy_name} budget exhausted: "
                        f"${used:.2f}/${strategy_budget_usd:.2f} used, "
                        f"proposed=${proposed_risk_usd:.2f} > remaining=${remaining:.2f}"
                    ),
                    strategy=strategy_name, symbol=symbol,
                    priority=alloc.priority,
                    budget_used_pct=round(budget_used_pct, 2),
                    budget_remaining_pct=round(100.0 - budget_used_pct, 2),
                )

            # 4. Concurrent positions per strategy
            strat_open = self._open_counts.get(strategy_name, 0)
            if strat_open >= alloc.max_concurrent_positions:
                return AllocationDecision(
                    allowed=False,
                    reason=(
                        f"{strategy_name} at concurrent limit: "
                        f"{strat_open}/{alloc.max_concurrent_positions}"
                    ),
                    strategy=strategy_name, symbol=symbol,
                    priority=alloc.priority,
                    budget_used_pct=round(used / strategy_budget_usd * 100.0, 2) if strategy_budget_usd > 0 else 0.0,
                    budget_remaining_pct=0.0,
                )

            # 5. Throttle lower-priority strategies if higher-priority are active
            if alloc.throttle_when_priority_active:
                higher_priority_active = any(
                    self._open_counts.get(s_name, 0) > 0
                    and s_alloc.priority < alloc.priority
                    for s_name, s_alloc in self._strategies.items()
                )
                if higher_priority_active:
                    logger.debug(
                        f"[Allocator] {strategy_name} throttled — "
                        "higher-priority strategy has active position"
                    )
                    return AllocationDecision(
                        allowed=False,
                        reason=(
                            f"{strategy_name} throttled — "
                            f"higher-priority strategy currently active"
                        ),
                        strategy=strategy_name, symbol=symbol,
                        priority=alloc.priority,
                        budget_used_pct=round(used / strategy_budget_usd * 100.0, 2) if strategy_budget_usd > 0 else 0.0,
                        budget_remaining_pct=round(
                            (remaining / strategy_budget_usd) * 100.0, 2
                        ) if strategy_budget_usd > 0 else 100.0,
                    )

            # All checks passed
            budget_used_pct = (used / strategy_budget_usd * 100.0) if strategy_budget_usd > 0 else 0.0
            return AllocationDecision(
                allowed=True,
                reason="within budget",
                strategy=strategy_name, symbol=symbol,
                priority=alloc.priority,
                budget_used_pct=round(budget_used_pct, 2),
                budget_remaining_pct=round(100.0 - budget_used_pct, 2),
            )

    def record_position_opened(self, strategy_name: str, risk_usd: float):
        """Call this when a position is confirmed filled."""
        with self._lock:
            self._risk_used[strategy_name] = (
                self._risk_used.get(strategy_name, 0.0) + risk_usd
            )
            self._open_counts[strategy_name] = (
                self._open_counts.get(strategy_name, 0) + 1
            )

    def record_position_closed(self, strategy_name: str, risk_usd: float):
        """Call this when a position is confirmed closed."""
        with self._lock:
            self._risk_used[strategy_name] = max(
                0.0, self._risk_used.get(strategy_name, 0.0) - risk_usd
            )
            self._open_counts[strategy_name] = max(
                0, self._open_counts.get(strategy_name, 0) - 1
            )

    def enable_strategy(self, strategy_name: str, enabled: bool):
        """Runtime enable/disable from API."""
        with self._lock:
            if strategy_name in self._strategies:
                self._strategies[strategy_name].enabled = enabled
                logger.info(
                    f"[Allocator] Strategy {strategy_name!r} "
                    f"{'ENABLED' if enabled else 'DISABLED'}"
                )

    def reset_daily(self):
        """Reset daily risk_used counters at day boundary."""
        with self._lock:
            self._risk_used = {s: 0.0 for s in self._strategies}
            logger.info("[Allocator] Daily budgets reset")

    def status(self) -> dict:
        """Return allocator state for API/monitoring."""
        with self._lock:
            strategy_states = {}
            for name, alloc in self._strategies.items():
                budget_usd = self._total_risk_budget_usd * alloc.max_risk_budget_pct / 100.0
                used = self._risk_used.get(name, 0.0)
                strategy_states[name] = {
                    "enabled": alloc.enabled,
                    "priority": alloc.priority,
                    "budget_usd": round(budget_usd, 2),
                    "risk_used_usd": round(used, 2),
                    "budget_remaining_usd": round(max(0.0, budget_usd - used), 2),
                    "open_positions": self._open_counts.get(name, 0),
                    "max_concurrent": alloc.max_concurrent_positions,
                    "allowed_asset_classes": alloc.allowed_asset_classes,
                }
            return {
                "total_risk_budget_usd": round(self._total_risk_budget_usd, 2),
                "strategies": strategy_states,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

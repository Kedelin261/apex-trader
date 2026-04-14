"""
APEX MULTI-MARKET TJR ENGINE
Reporting Agent — Performance analytics and trade summaries.

Generates:
- Daily summaries
- Per-trade rationale summaries
- Weekly/monthly analytics
- Degradation explanations
- Exportable audit reports
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from domain.models import Trade, PerformanceMetrics, TradeOutcome
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class ReportingAgent(BaseAgent):
    """Generates performance reports and trade summaries."""

    def __init__(self, ledger, state_store, config: dict):
        super().__init__(AgentName.REPORTING, ledger, state_store, config)
        reporting_cfg = config.get("agents", {}).get("reporting", {})
        self.export_formats = reporting_cfg.get("export_formats", ["json"])
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)

    def generate_trade_summary(self, trade: Trade) -> dict:
        """Generate a human-readable summary of a single trade."""
        duration = ""
        if trade.exit_time and trade.entry_time:
            delta = trade.exit_time - trade.entry_time
            hours = delta.total_seconds() / 3600
            duration = f"{hours:.1f}h"

        return {
            "trade_id": trade.trade_id,
            "instrument": trade.instrument,
            "direction": trade.direction.value,
            "strategy": trade.strategy_family.value,
            "session": trade.session.value,
            "regime": trade.regime.value if trade.regime else "UNKNOWN",
            "entry": trade.entry_price,
            "exit": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "sl": trade.stop_loss,
            "tp": trade.take_profit,
            "lots": trade.position_size_lots,
            "pnl_usd": round(trade.realized_pnl_usd, 2),
            "r_multiple": round(trade.r_multiple, 2),
            "outcome": trade.outcome.value,
            "duration": duration,
            "rationale": trade.strategy_reason,
            "volatility_context": trade.volatility_context,
            "validation_flags": trade.validation_flags,
            "agent_chain": trade.agent_decision_chain
        }

    def generate_daily_summary(self, trades: List[Trade],
                                account_balance: float,
                                date_str: str = None) -> dict:
        """Generate daily performance summary."""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        today_trades = [
            t for t in trades
            if t.entry_time and t.entry_time.strftime("%Y-%m-%d") == date_str
        ]

        closed = [t for t in today_trades if t.exit_time is not None]
        wins = [t for t in closed if t.outcome == TradeOutcome.WIN]
        losses = [t for t in closed if t.outcome == TradeOutcome.LOSS]
        open_trades = [t for t in today_trades if t.exit_time is None]

        total_pnl = sum(t.realized_pnl_usd for t in closed)
        win_rate = len(wins) / len(closed) if closed else 0.0
        avg_r = sum(t.r_multiple for t in closed) / len(closed) if closed else 0.0

        summary = {
            "date": date_str,
            "total_trades": len(closed),
            "open_trades": len(open_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 4),
            "avg_r": round(avg_r, 4),
            "total_pnl_usd": round(total_pnl, 2),
            "total_pnl_pct": round((total_pnl / account_balance) * 100, 4) if account_balance > 0 else 0,
            "account_balance": round(account_balance, 2),
            "trade_summaries": [self.generate_trade_summary(t) for t in closed]
        }

        return summary

    def export_report(self, data: dict, filename: str):
        """Export report to disk."""
        if "json" in self.export_formats:
            path = self.reports_dir / f"{filename}.json"
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"[Reporting] Exported JSON: {path}")

        if "csv" in self.export_formats and "trade_summaries" in data:
            import csv
            path = self.reports_dir / f"{filename}.csv"
            trades = data["trade_summaries"]
            if trades:
                with open(path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=trades[0].keys())
                    writer.writeheader()
                    writer.writerows(trades)
                logger.info(f"[Reporting] Exported CSV: {path}")

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """Generate reports based on current state."""
        messages = []

        trades = context.get("all_trades", [])
        account_balance = context.get("account_balance", 100000.0)

        if not trades:
            return messages

        # Daily summary
        summary = self.generate_daily_summary(trades, account_balance)
        date_str = summary["date"]
        filename = f"daily_{date_str}"
        self.export_report(summary, filename)

        # Store in state
        self.state_store.set("last_daily_report", summary)

        msg = self.emit(
            message_type=AgentMessageType.DAILY_SUMMARY,
            payload=summary,
            priority=MessagePriority.LOW,
            final_status="REPORT_GENERATED"
        )
        messages.append(msg)

        logger.info(
            f"[Reporting] Daily: {summary['total_trades']} trades, "
            f"WR={summary['win_rate']:.1%}, "
            f"PnL=${summary['total_pnl_usd']:+.2f}"
        )

        return messages

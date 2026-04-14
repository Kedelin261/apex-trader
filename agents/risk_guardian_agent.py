"""
APEX MULTI-MARKET TJR ENGINE
Risk Guardian Agent — Enforces all hard risk rules.

This agent wraps the RiskManager and communicates its decisions
through the formal agent protocol. It is the final gate before execution.

HARD RULE: If this agent rejects, execution cannot proceed.
No other agent can override a RISK_REJECTED message.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from domain.models import Signal, Position, MarketRegime
from core.risk_manager import RiskManager
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class RiskGuardianAgent(BaseAgent):
    """
    Enforces all hard risk rules through the formal protocol.
    The risk engine is non-negotiable.
    """

    def __init__(self, ledger, state_store, config: dict,
                 risk_manager: Optional[RiskManager] = None):
        super().__init__(AgentName.RISK_GUARDIAN, ledger, state_store, config)
        self.risk_manager = risk_manager or RiskManager(config)

    def evaluate_signal(self,
                        signal: Signal,
                        open_positions: List[Position],
                        current_spread_pips: float = 1.5,
                        regime: Optional[MarketRegime] = None,
                        news_active: bool = False,
                        volatility_spike: bool = False,
                        parent_msg_id: Optional[str] = None) -> AgentMessage:
        """
        Evaluate a signal through the full risk engine.
        Returns RISK_APPROVED or RISK_REJECTED message.
        """
        result = self.risk_manager.validate_signal(
            signal=signal,
            open_positions=open_positions,
            current_spread_pips=current_spread_pips,
            regime=regime,
            news_active=news_active,
            volatility_spike=volatility_spike
        )

        if result.approved:
            msg = self.emit(
                message_type=AgentMessageType.RISK_APPROVED,
                payload={
                    "signal_id": signal.signal_id,
                    "instrument": signal.instrument,
                    "risk_usd": signal.risk_amount_usd,
                    "rr": signal.reward_to_risk,
                    "drawdown_pct": result.details.get("drawdown_pct", 0),
                    "daily_pnl": result.details.get("daily_pnl", 0),
                    "open_positions": result.details.get("open_positions", 0)
                },
                target=AgentName.EXECUTION_SUPERVISOR,
                priority=MessagePriority.HIGH,
                instrument=signal.instrument,
                hard_constraints_observed=[
                    f"risk_pct={self.risk_manager.max_risk_per_trade_pct}",
                    f"max_daily_loss={self.risk_manager.max_daily_loss_pct}",
                    f"max_drawdown={self.risk_manager.max_total_drawdown_pct}",
                    f"max_concurrent={self.risk_manager.max_concurrent_trades}"
                ],
                final_status="APPROVED",
                parent_message_id=parent_msg_id
            )
            logger.info(
                f"[RiskGuardian] APPROVED: {signal.instrument} "
                f"risk=${signal.risk_amount_usd:.2f}"
            )
        else:
            msg = self.emit(
                message_type=AgentMessageType.RISK_REJECTED,
                payload={
                    "signal_id": signal.signal_id,
                    "instrument": signal.instrument,
                    "rejection_codes": [r.value for r in result.rejection_codes],
                    "details": result.details
                },
                priority=MessagePriority.HIGH,
                instrument=signal.instrument,
                hard_constraints_observed=[r.value for r in result.rejection_codes],
                final_status="REJECTED",
                rejection_reasons=result.reasons,
                parent_message_id=parent_msg_id
            )
            logger.warning(
                f"[RiskGuardian] REJECTED: {signal.instrument} — "
                f"{'; '.join(result.reasons[:2])}"
            )

            # If kill switch triggered, broadcast immediately
            if self.risk_manager.is_kill_switch_active():
                self.emit(
                    message_type=AgentMessageType.KILL_SWITCH_TRIGGERED,
                    payload={
                        "reason": "; ".join(result.reasons),
                        "drawdown_pct": result.details.get("drawdown_pct", 0),
                        "daily_pnl": result.details.get("daily_pnl", 0)
                    },
                    priority=MessagePriority.CRITICAL,
                    final_status="KILL_SWITCH_ACTIVE"
                )

        return msg

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """Process validated signals awaiting risk approval."""
        messages = []
        validated = context.get("validated_signals_for_risk", [])
        open_positions = context.get("open_positions", [])

        regime_context = self.state_store.get("regime_context", {})

        for item in validated:
            signal = item.get("signal")
            if not signal:
                continue

            regime_data = regime_context.get(signal.instrument, {})
            regime_str = regime_data.get("regime", "UNKNOWN")
            try:
                regime = MarketRegime(regime_str)
            except ValueError:
                regime = MarketRegime.UNKNOWN

            spread = context.get("current_spreads", {}).get(signal.instrument, 1.5)

            msg = self.evaluate_signal(
                signal=signal,
                open_positions=open_positions,
                current_spread_pips=spread,
                regime=regime,
                news_active=item.get("news_active", False),
                volatility_spike=item.get("volatility_spike", False),
                parent_msg_id=item.get("parent_message_id")
            )
            messages.append(msg)

        return messages

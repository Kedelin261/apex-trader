"""
APEX MULTI-MARKET TJR ENGINE
Strategy Validator Agent — Signal quality gate.

Validates raw TJR setups against all deterministic rules.
Rejects low-quality or incomplete setups BEFORE risk evaluation.
Confirms R:R thresholds and setup completeness.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from domain.models import Signal, TJRSetup, StrategyFamily, InstrumentType
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)

# Minimum quality score to proceed
MIN_QUALITY_SCORE = 0.65
MIN_RR = 2.0


class StrategyValidatorAgent(BaseAgent):
    """
    Validates TJR signal candidates before risk evaluation.
    Enforces all deterministic strategy rules.
    """

    def __init__(self, ledger, state_store, config: dict):
        super().__init__(AgentName.STRATEGY_VALIDATOR, ledger, state_store, config)
        validator_cfg = config.get("agents", {}).get("strategy_validator", {})
        self.min_quality = validator_cfg.get("min_setup_quality_score", MIN_QUALITY_SCORE)
        self.min_rr = config.get("risk", {}).get("min_reward_to_risk_ratio", MIN_RR)

    def validate_tjr_signal(self, signal: Signal,
                             setup: Optional[TJRSetup] = None,
                             parent_msg_id: Optional[str] = None) -> AgentMessage:
        """
        Validate a TJR signal candidate.
        Returns APPROVED or REJECTED message.
        """
        rejection_reasons = []

        # ── Rule 1: Strategy compatibility ──────────────────────────────────
        if signal.strategy_family != StrategyFamily.TJR:
            pass  # Non-TJR handled separately

        # ── Rule 2: Price validity ────────────────────────────────────────
        if signal.entry_price <= 0:
            rejection_reasons.append("Invalid entry price")
        if signal.stop_loss <= 0:
            rejection_reasons.append("Invalid stop loss")
        if signal.take_profit <= 0:
            rejection_reasons.append("Invalid take profit")

        # ── Rule 3: Stop distance ─────────────────────────────────────────
        if signal.stop_distance_pips < 3.0:
            rejection_reasons.append(
                f"Stop too tight: {signal.stop_distance_pips:.1f} pips < 3.0"
            )

        # ── Rule 4: R:R ───────────────────────────────────────────────────
        if signal.reward_to_risk < self.min_rr:
            rejection_reasons.append(
                f"R:R too low: {signal.reward_to_risk:.2f} < {self.min_rr}"
            )

        # ── Rule 5: Position size ─────────────────────────────────────────
        if signal.position_size_lots <= 0:
            rejection_reasons.append("Invalid position size")

        # ── Rule 6: Setup quality score ───────────────────────────────────
        if setup and setup.setup_quality_score < self.min_quality:
            rejection_reasons.append(
                f"Setup quality too low: {setup.setup_quality_score:.2f} < {self.min_quality}"
            )

        # ── Rule 7: Session validity ──────────────────────────────────────
        if setup and not setup.is_session_optimal:
            rejection_reasons.append(
                f"Non-optimal session: {setup.session.value}"
            )

        # ── Rule 8: Direction / structure alignment ───────────────────────
        if setup:
            from domain.models import SignalDirection, MarketStructureState
            if (signal.direction == SignalDirection.BUY and
                    setup.structure_state == MarketStructureState.BEARISH):
                rejection_reasons.append("BUY signal against BEARISH structure")
            elif (signal.direction == SignalDirection.SELL and
                  setup.structure_state == MarketStructureState.BULLISH):
                rejection_reasons.append("SELL signal against BULLISH structure")

        # ── Decision ──────────────────────────────────────────────────────
        approved = len(rejection_reasons) == 0

        msg_type = (AgentMessageType.SIGNAL_VALIDATION_APPROVED if approved
                    else AgentMessageType.SIGNAL_VALIDATION_REJECTED)
        priority = (MessagePriority.NORMAL if approved else MessagePriority.HIGH)

        msg = self.emit(
            message_type=msg_type,
            payload={
                "signal_id": signal.signal_id,
                "instrument": signal.instrument,
                "direction": signal.direction.value,
                "entry": signal.entry_price,
                "sl": signal.stop_loss,
                "tp": signal.take_profit,
                "rr": signal.reward_to_risk,
                "quality_score": setup.setup_quality_score if setup else None,
                "strategy": signal.strategy_family.value
            },
            target=AgentName.RISK_GUARDIAN if approved else None,
            priority=priority,
            instrument=signal.instrument,
            final_status="APPROVED" if approved else "REJECTED",
            rejection_reasons=rejection_reasons,
            confidence=setup.setup_quality_score if setup else 0.5,
            parent_message_id=parent_msg_id
        )

        if approved:
            logger.info(f"[StrategyValidator] APPROVED: {signal.instrument} {signal.direction.value}")
        else:
            logger.info(
                f"[StrategyValidator] REJECTED: {signal.instrument} — "
                f"{'; '.join(rejection_reasons)}"
            )

        return msg

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """
        Validate all pending signal candidates from context.
        """
        messages = []
        pending = context.get("pending_signals", [])

        for item in pending:
            signal = item.get("signal")
            setup = item.get("setup")
            parent_id = item.get("parent_message_id")

            if signal:
                msg = self.validate_tjr_signal(signal, setup, parent_id)
                messages.append(msg)

                # Update signal status in state
                signals_state = self.state_store.get("validated_signals", {})
                signals_state[signal.signal_id] = {
                    "approved": msg.final_status == "APPROVED",
                    "message_id": msg.message_id,
                    "reasons": msg.rejection_reasons
                }
                self.state_store.set("validated_signals", signals_state)

        return messages

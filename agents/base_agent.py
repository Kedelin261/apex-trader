"""
APEX MULTI-MARKET TJR ENGINE
Base Agent — Abstract foundation for all agents.

All agents share:
- Formal message creation and ledger recording
- Structured logging
- Health state tracking
- Cost tracking
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from protocol.agent_protocol import (
    AgentDecisionLedger, AgentStateStore, AgentMessage,
    AgentMessageType, MessagePriority, AgentName
)

logger = logging.getLogger(__name__)


class AgentHealth:
    """Tracks agent operational health."""

    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.is_healthy = True
        self.last_run: Optional[datetime] = None
        self.run_count: int = 0
        self.error_count: int = 0
        self.last_error: str = ""
        self.messages_sent: int = 0
        self.started_at: datetime = datetime.now(timezone.utc)

    def record_run(self):
        self.run_count += 1
        self.last_run = datetime.now(timezone.utc)

    def record_error(self, error: str):
        self.error_count += 1
        self.last_error = error
        logger.error(f"[{self.agent_name}] Error #{self.error_count}: {error}")

    def to_dict(self) -> dict:
        return {
            "agent": self.agent_name,
            "healthy": self.is_healthy,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "runs": self.run_count,
            "errors": self.error_count,
            "last_error": self.last_error,
            "messages_sent": self.messages_sent
        }


class BaseAgent(ABC):
    """
    Abstract base class for all agents.

    Subclasses must implement: run()
    Subclasses use: emit() to send messages, state_store for coordination.
    """

    def __init__(self,
                 agent_name: AgentName,
                 ledger: AgentDecisionLedger,
                 state_store: AgentStateStore,
                 config: dict = None):
        self.agent_name = agent_name
        self.ledger = ledger
        self.state_store = state_store
        self.config = config or {}
        self.health = AgentHealth(agent_name.value)
        self._enabled = True

        logger.info(f"[{agent_name.value}] Agent initialized")

    def emit(self,
             message_type: AgentMessageType,
             payload: dict,
             target: Optional[AgentName] = None,
             priority: MessagePriority = MessagePriority.NORMAL,
             instrument: Optional[str] = None,
             instrument_type: Optional[str] = None,
             regime_context: Optional[str] = None,
             confidence: Optional[float] = None,
             hard_constraints: List[str] = None,
             requested_action: Optional[str] = None,
             final_status: Optional[str] = None,
             rejection_reasons: List[str] = None,
             parent_message_id: Optional[str] = None) -> AgentMessage:
        """
        Create, record, and return a formal agent message.
        This is the ONLY way agents should communicate.
        """
        msg = AgentMessage(
            source_agent=self.agent_name,
            target_agent=target,
            message_type=message_type,
            priority=priority,
            instrument=instrument,
            instrument_type=instrument_type,
            regime_context=regime_context,
            payload=payload,
            confidence=confidence,
            hard_constraints_observed=hard_constraints or [],
            requested_action=requested_action,
            final_status=final_status,
            rejection_reasons=rejection_reasons or [],
            parent_message_id=parent_message_id
        )

        self.ledger.record(msg)
        self.health.messages_sent += 1

        return msg

    def is_enabled(self) -> bool:
        return self._enabled

    def disable(self):
        self._enabled = False
        logger.warning(f"[{self.agent_name.value}] Agent DISABLED")

    def enable(self):
        self._enabled = True
        logger.info(f"[{self.agent_name.value}] Agent ENABLED")

    @abstractmethod
    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """
        Execute agent logic. Returns list of messages emitted.
        context: shared dict provided by the orchestrator.
        """
        pass

    def safe_run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """Run with error handling and health tracking."""
        if not self._enabled:
            return []
        self.health.record_run()
        try:
            return self.run(context)
        except Exception as e:
            self.health.record_error(str(e))
            logger.exception(f"[{self.agent_name.value}] Unexpected error: {e}")
            return []

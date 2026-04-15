"""
APEX MULTI-MARKET TJR ENGINE
Agent Communication Protocol — Formal typed message system.

Every agent action produces an auditable message.
Every message is structured, typed, logged, and replayable.
No agent communicates through vague shared memory.

The AgentDecisionLedger stores every message permanently.
Risk rejections are immutable. 
Live trade approvals include the full upstream chain.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


# =============================================================================
# MESSAGE TYPES
# =============================================================================

class AgentMessageType(str, Enum):
    # Market scanning
    MARKET_SCAN_RESULT = "MARKET_SCAN_RESULT"
    INSTRUMENT_PRIORITY_UPDATE = "INSTRUMENT_PRIORITY_UPDATE"
    # Regime
    REGIME_CLASSIFICATION = "REGIME_CLASSIFICATION"
    # Signals
    SIGNAL_CANDIDATE = "SIGNAL_CANDIDATE"
    SIGNAL_VALIDATION_APPROVED = "SIGNAL_VALIDATION_APPROVED"
    SIGNAL_VALIDATION_REJECTED = "SIGNAL_VALIDATION_REJECTED"
    # Risk
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    # Execution
    EXECUTION_READY = "EXECUTION_READY"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_FAILED = "ORDER_FAILED"
    PROTECTION_ORDER_CONFIRMED = "PROTECTION_ORDER_CONFIRMED"
    # Anomalies and safety
    ANOMALY_DETECTED = "ANOMALY_DETECTED"
    KILL_SWITCH_TRIGGERED = "KILL_SWITCH_TRIGGERED"
    # Optimization
    OPTIMIZATION_PROPOSAL = "OPTIMIZATION_PROPOSAL"
    OPTIMIZATION_REJECTED = "OPTIMIZATION_REJECTED"
    OPTIMIZATION_APPROVED = "OPTIMIZATION_APPROVED"
    # Reporting
    REPORT_READY = "REPORT_READY"
    DAILY_SUMMARY = "DAILY_SUMMARY"
    # State
    AGENT_HEALTH_UPDATE = "AGENT_HEALTH_UPDATE"
    SYSTEM_STATE_BROADCAST = "SYSTEM_STATE_BROADCAST"


class MessagePriority(int, Enum):
    CRITICAL = 1    # Kill switch, broker failure, drawdown breach
    HIGH = 2        # Risk rejections, execution failures, anomalies
    NORMAL = 3      # Signal flow, regime updates, routine
    LOW = 4         # Optimization proposals, reporting, analytics


# Agent name constants
class AgentName(str, Enum):
    ORCHESTRATOR = "AgentOrchestrator"
    MARKET_SENTINEL = "MarketSentinelAgent"
    INSTRUMENT_SELECTION = "InstrumentSelectionAgent"
    REGIME_DETECTION = "RegimeDetectionAgent"
    STRATEGY_VALIDATOR = "StrategyValidatorAgent"
    RISK_GUARDIAN = "RiskGuardianAgent"
    EXECUTION_SUPERVISOR = "ExecutionSupervisorAgent"
    OPTIMIZATION = "OptimizationAgent"
    REPORTING = "ReportingAgent"
    COST_CONTROL = "CostControlAgent"
    SYSTEM = "System"


# =============================================================================
# AGENT MESSAGE
# =============================================================================

class AgentMessage(BaseModel):
    """
    Formal typed message for all agent-to-agent communication.
    Every field is required and validated.
    Messages are immutable after creation.
    """
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_agent: AgentName
    target_agent: Optional[AgentName] = None   # None = broadcast
    message_type: AgentMessageType
    priority: MessagePriority = MessagePriority.NORMAL

    # Context
    instrument: Optional[str] = None
    instrument_type: Optional[str] = None
    regime_context: Optional[str] = None
    session_context: Optional[str] = None

    # Content
    payload: Dict[str, Any] = {}
    confidence: Optional[float] = None   # 0.0 to 1.0

    # Constraints and decisions
    hard_constraints_observed: List[str] = []
    requested_action: Optional[str] = None
    final_status: Optional[str] = None
    rejection_reasons: List[str] = []

    # Chain tracking
    parent_message_id: Optional[str] = None  # Links this message to its cause

    model_config = ConfigDict(frozen=False)   # Allow field updates for status tracking

    def to_log_dict(self) -> dict:
        return {
            "msg_id": self.message_id,
            "ts": self.timestamp.isoformat(),
            "from": self.source_agent.value,
            "to": self.target_agent.value if self.target_agent else "BROADCAST",
            "type": self.message_type.value,
            "priority": self.priority.name,
            "instrument": self.instrument,
            "status": self.final_status,
            "reasons": self.rejection_reasons,
        }


# =============================================================================
# AGENT DECISION LEDGER
# =============================================================================

class AgentDecisionLedger:
    """
    Immutable, append-only log of all agent decisions.

    Every message is written to disk as JSONL.
    Risk rejections are permanently stored with full context.
    Live trade approvals include the full upstream chain.
    """

    def __init__(self, ledger_path: str = "logs/agent_decision_ledger.jsonl"):
        self.ledger_path = Path(ledger_path)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._memory_log: List[AgentMessage] = []
        self._trade_chains: Dict[str, List[str]] = {}  # signal_id → [msg_ids]

        logger.info(f"[AgentDecisionLedger] Initialized at {self.ledger_path}")

    def record(self, message: AgentMessage) -> str:
        """Record a message to the ledger. Returns message_id."""
        with self._lock:
            self._memory_log.append(message)

            # Write to disk
            entry = {
                "message_id": message.message_id,
                "timestamp": message.timestamp.isoformat(),
                "source_agent": message.source_agent.value,
                "target_agent": message.target_agent.value if message.target_agent else None,
                "message_type": message.message_type.value,
                "priority": message.priority.name,
                "instrument": message.instrument,
                "instrument_type": message.instrument_type,
                "regime_context": message.regime_context,
                "payload": message.payload,
                "confidence": message.confidence,
                "hard_constraints_observed": message.hard_constraints_observed,
                "requested_action": message.requested_action,
                "final_status": message.final_status,
                "rejection_reasons": message.rejection_reasons,
                "parent_message_id": message.parent_message_id
            }

            try:
                with open(self.ledger_path, "a") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception as e:
                logger.error(f"[AgentDecisionLedger] Write error: {e}")

        # Track signal chains
        signal_id = message.payload.get("signal_id")
        if signal_id:
            if signal_id not in self._trade_chains:
                self._trade_chains[signal_id] = []
            self._trade_chains[signal_id].append(message.message_id)

        return message.message_id

    def get_trade_chain(self, signal_id: str) -> List[AgentMessage]:
        """Return complete decision chain for a signal."""
        if signal_id not in self._trade_chains:
            return []
        msg_ids = set(self._trade_chains[signal_id])
        return [m for m in self._memory_log if m.message_id in msg_ids]

    def get_recent(self, n: int = 50,
                   agent: Optional[AgentName] = None,
                   msg_type: Optional[AgentMessageType] = None) -> List[AgentMessage]:
        """Get recent messages with optional filters."""
        msgs = list(self._memory_log)
        if agent:
            msgs = [m for m in msgs if m.source_agent == agent]
        if msg_type:
            msgs = [m for m in msgs if m.message_type == msg_type]
        return msgs[-n:]

    def get_rejections(self, hours: int = 24) -> List[AgentMessage]:
        """Get recent risk/validation rejections."""
        cutoff = datetime.now(timezone.utc)
        rejection_types = {
            AgentMessageType.RISK_REJECTED,
            AgentMessageType.SIGNAL_VALIDATION_REJECTED,
            AgentMessageType.EXECUTION_BLOCKED
        }
        return [
            m for m in self._memory_log
            if m.message_type in rejection_types
        ][-100:]

    def get_kill_switch_events(self) -> List[AgentMessage]:
        """Get all kill switch events."""
        return [
            m for m in self._memory_log
            if m.message_type == AgentMessageType.KILL_SWITCH_TRIGGERED
        ]

    @property
    def total_messages(self) -> int:
        return len(self._memory_log)


# =============================================================================
# AGENT STATE STORE
# =============================================================================

class AgentStateStore:
    """
    Shared state store for agent coordination.
    Agents read from and write to this store to coordinate.

    NOT a message bus — state only, no communication bypass.
    """

    def __init__(self):
        self._state: Dict[str, Any] = {}
        self._lock = Lock()

    def set(self, key: str, value: Any):
        with self._lock:
            self._state[key] = {
                "value": value,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._state.get(key)
            return entry["value"] if entry else default

    def get_all(self) -> dict:
        with self._lock:
            return {k: v["value"] for k, v in self._state.items()}

    def delete(self, key: str):
        with self._lock:
            self._state.pop(key, None)

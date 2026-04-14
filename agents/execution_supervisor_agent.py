"""
APEX MULTI-MARKET TJR ENGINE
Execution Supervisor Agent — Order execution oversight.

Confirms broker connectivity, validates order parameters,
supervises entry/SL/TP placement, handles retries and circuit breakers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from domain.models import Signal, Order, OrderType, OrderSide, SignalDirection
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class ExecutionSupervisorAgent(BaseAgent):
    """
    Supervises all execution operations.
    Detects anomalies and handles circuit breakers.
    """

    def __init__(self, ledger, state_store, config: dict,
                 broker_connector=None):
        super().__init__(AgentName.EXECUTION_SUPERVISOR, ledger, state_store, config)
        self.broker = broker_connector

        exec_cfg = config.get("agents", {}).get("execution_supervisor", {})
        self.max_retries = exec_cfg.get("max_order_retries", 3)
        self.retry_delay = exec_cfg.get("retry_delay_seconds", 2)
        self.circuit_breaker_failures = exec_cfg.get("circuit_breaker_failures", 5)

        self._consecutive_failures = 0
        self._circuit_open = False

    def check_execution_environment(self) -> tuple[bool, str]:
        """
        Verify the execution environment is ready.
        Returns (ready, reason).
        """
        # Circuit breaker check
        if self._circuit_open:
            return False, f"Circuit breaker open: {self._consecutive_failures} consecutive failures"

        # Broker connectivity check (if live)
        if self.broker is not None:
            try:
                connected = self.broker.is_connected()
                if not connected:
                    return False, "Broker not connected"
            except Exception as e:
                return False, f"Broker connectivity check failed: {e}"

        return True, ""

    def validate_order_params(self, signal: Signal) -> tuple[bool, List[str]]:
        """
        Pre-execution order parameter validation.
        Catches issues before they reach the broker.
        """
        errors = []

        if signal.entry_price <= 0:
            errors.append("Entry price invalid")
        if signal.stop_loss <= 0:
            errors.append("Stop loss invalid")
        if signal.take_profit <= 0:
            errors.append("Take profit invalid")
        if signal.position_size_lots <= 0:
            errors.append("Position size invalid")

        # Direction / price logic
        if signal.direction == SignalDirection.BUY:
            if signal.stop_loss >= signal.entry_price:
                errors.append("BUY: SL must be below entry")
            if signal.take_profit <= signal.entry_price:
                errors.append("BUY: TP must be above entry")
        else:
            if signal.stop_loss <= signal.entry_price:
                errors.append("SELL: SL must be above entry")
            if signal.take_profit >= signal.entry_price:
                errors.append("SELL: TP must be below entry")

        return len(errors) == 0, errors

    def submit_order(self, signal: Signal,
                     parent_msg_id: Optional[str] = None) -> AgentMessage:
        """
        Submit an order and supervise execution.
        Returns ORDER_SUBMITTED or ORDER_FAILED message.
        """
        # Check environment
        env_ready, env_reason = self.check_execution_environment()
        if not env_ready:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.circuit_breaker_failures:
                self._circuit_open = True
                logger.critical(
                    f"[ExecutionSupervisor] Circuit breaker OPENED after "
                    f"{self._consecutive_failures} failures"
                )

            return self.emit(
                message_type=AgentMessageType.EXECUTION_BLOCKED,
                payload={"signal_id": signal.signal_id, "reason": env_reason},
                priority=MessagePriority.HIGH,
                instrument=signal.instrument,
                final_status="BLOCKED",
                rejection_reasons=[env_reason],
                parent_message_id=parent_msg_id
            )

        # Validate order params
        params_ok, param_errors = self.validate_order_params(signal)
        if not params_ok:
            return self.emit(
                message_type=AgentMessageType.EXECUTION_BLOCKED,
                payload={
                    "signal_id": signal.signal_id,
                    "param_errors": param_errors
                },
                priority=MessagePriority.HIGH,
                instrument=signal.instrument,
                final_status="BLOCKED",
                rejection_reasons=param_errors,
                parent_message_id=parent_msg_id
            )

        # Build order object
        order = Order(
            signal_id=signal.signal_id,
            instrument=signal.instrument,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY if signal.direction == SignalDirection.BUY else OrderSide.SELL,
            quantity=signal.position_size_lots,
            is_paper=signal.is_paper_trade
        )

        # Execute via the broker interface.
        # self.broker is a BrokerManager instance when wired from the API server.
        # BrokerManager.submit_order() internally routes:
        #   - LIVE_ENABLED  → real OANDA connector
        #   - all other ESM states → PaperBroker
        # This preserves the execution state machine's gating without bypass.

        if self.broker is not None and hasattr(self.broker, "submit_order"):
            # ── Unified path: BrokerManager.submit_order() handles routing ──
            try:
                from brokers.base_connector import (
                    OrderRequest,
                    OrderSide as BrokerOrderSide,
                    OrderType as BrokerOrderType,
                )

                broker_side = (
                    BrokerOrderSide.BUY
                    if signal.direction == SignalDirection.BUY
                    else BrokerOrderSide.SELL
                )

                order_req = OrderRequest(
                    instrument=signal.instrument,
                    side=broker_side,
                    units=signal.position_size_lots,
                    order_type=BrokerOrderType.MARKET,
                    price=signal.entry_price,
                    stop_loss=signal.stop_loss,
                    take_profit=signal.take_profit,
                    client_order_id=order.order_id,
                    comment=(
                        f"apex_signal:{signal.signal_id[:8]} "
                        f"{signal.strategy_family.value}"
                    ),
                )

                result = self.broker.submit_order(order_req)

                if result.success:
                    order.broker_order_id = result.order_id
                    order.filled_price = result.filled_price or signal.entry_price
                    order.status = "FILLED"
                    self._consecutive_failures = 0

                    is_paper = getattr(result, "raw", {}).get("paper", False)
                    msg = self.emit(
                        message_type=AgentMessageType.ORDER_FILLED,
                        payload={
                            "signal_id": signal.signal_id,
                            "order_id": order.order_id,
                            "broker_order_id": order.broker_order_id,
                            "instrument": signal.instrument,
                            "direction": signal.direction.value,
                            "filled_price": order.filled_price,
                            "quantity": order.quantity,
                            "is_paper": is_paper,
                            "sl": signal.stop_loss,
                            "tp": signal.take_profit,
                        },
                        priority=MessagePriority.HIGH,
                        instrument=signal.instrument,
                        final_status="FILLED",
                        parent_message_id=parent_msg_id,
                    )
                    logger.info(
                        f"[ExecutionSupervisor] ORDER FILLED: "
                        f"{signal.instrument} {signal.direction.value} "
                        f"@ {order.filled_price:.5f} "
                        f"({'PAPER' if is_paper else 'LIVE'} "
                        f"{signal.position_size_lots:.2f} lots)"
                    )
                else:
                    # Broker rejected the order (e.g. insufficient funds,
                    # kill switch, ESM not in LIVE_ENABLED, etc.)
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= self.circuit_breaker_failures:
                        self._circuit_open = True
                        logger.critical(
                            f"[ExecutionSupervisor] Circuit breaker OPENED after "
                            f"{self._consecutive_failures} failures"
                        )

                    reject_reason = result.reject_reason or "Order rejected by broker/ESM"
                    msg = self.emit(
                        message_type=AgentMessageType.ORDER_FAILED,
                        payload={
                            "signal_id": signal.signal_id,
                            "instrument": signal.instrument,
                            "reject_reason": reject_reason,
                            "insufficient_funds": getattr(result, "insufficient_funds", False),
                            "consecutive_failures": self._consecutive_failures,
                        },
                        priority=MessagePriority.CRITICAL,
                        instrument=signal.instrument,
                        final_status="FAILED",
                        rejection_reasons=[reject_reason],
                        parent_message_id=parent_msg_id,
                    )
                    logger.error(
                        f"[ExecutionSupervisor] ORDER REJECTED by broker: "
                        f"{reject_reason}"
                    )

            except Exception as e:
                self._consecutive_failures += 1
                msg = self.emit(
                    message_type=AgentMessageType.ORDER_FAILED,
                    payload={
                        "signal_id": signal.signal_id,
                        "error": str(e),
                        "consecutive_failures": self._consecutive_failures,
                    },
                    priority=MessagePriority.CRITICAL,
                    instrument=signal.instrument,
                    final_status="FAILED",
                    rejection_reasons=[str(e)],
                    parent_message_id=parent_msg_id,
                )
                logger.error(f"[ExecutionSupervisor] ORDER EXCEPTION: {e}")

        else:
            # ── No broker wired at all: pure paper fill ──────────────────────
            order.status = "FILLED"
            order.filled_price = signal.entry_price
            order.filled_quantity = signal.position_size_lots
            order.filled_at = datetime.now(timezone.utc)
            order.broker_order_id = f"PAPER_{order.order_id[:8]}"
            self._consecutive_failures = 0

            msg = self.emit(
                message_type=AgentMessageType.ORDER_FILLED,
                payload={
                    "signal_id": signal.signal_id,
                    "order_id": order.order_id,
                    "instrument": signal.instrument,
                    "direction": signal.direction.value,
                    "filled_price": order.filled_price,
                    "quantity": order.quantity,
                    "is_paper": True,
                    "sl": signal.stop_loss,
                    "tp": signal.take_profit,
                },
                priority=MessagePriority.HIGH,
                instrument=signal.instrument,
                final_status="FILLED",
                parent_message_id=parent_msg_id,
            )
            logger.info(
                f"[ExecutionSupervisor] PAPER FILL (no broker): "
                f"{signal.instrument} {signal.direction.value} "
                f"@ {order.filled_price:.5f} ({signal.position_size_lots:.2f} lots)"
            )

        return msg

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """Process execution-ready signals."""
        messages = []
        execution_queue = context.get("execution_queue", [])

        for item in execution_queue:
            signal = item.get("signal")
            if signal:
                msg = self.submit_order(signal, item.get("parent_message_id"))
                messages.append(msg)

        return messages

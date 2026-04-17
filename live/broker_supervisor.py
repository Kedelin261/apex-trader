"""
APEX MULTI-MARKET TJR ENGINE
Broker Supervisor — Phase 5: Broker heartbeat, reconnect with backoff, health tracking.

RESPONSIBILITIES:
  - Periodic heartbeat checks (configurable interval)
  - Reconnect with exponential backoff on disconnect
  - Connector health tracking (latency, consecutive failures, circuit state)
  - Auto downgrade to LIVE_CONNECTED_SAFE → PAPER_MODE if broker unavailable
  - Resume-safe reconnect (no duplicate order submission)
  - OANDA is the primary autonomous path; IBKR is secondary/optional

OANDA PRIORITY RULE:
  OANDA must be healthy for autonomous operation.
  IBKR unavailability is non-fatal — system continues with OANDA + paper for IBKR symbols.

ARCHITECTURE:
  Runs as the HealthMonitorLoop in autonomous_scheduler.py.
  Calls BrokerManager.connect() for reconnect attempts.
  Communicates state change via ExecutionStateMachine.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class BrokerHealthStatus:
    """Tracks health metrics for a single broker connector."""

    def __init__(self, broker_name: str):
        self.broker_name = broker_name
        self.connected: bool = False
        self.last_heartbeat_at: Optional[datetime] = None
        self.latency_ms: Optional[float] = None
        self.consecutive_failures: int = 0
        self.total_reconnects: int = 0
        self.last_reconnect_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.circuit_open: bool = False

    def record_success(self, latency_ms: Optional[float] = None):
        self.connected = True
        self.last_heartbeat_at = datetime.now(timezone.utc)
        self.latency_ms = latency_ms
        self.consecutive_failures = 0
        self.circuit_open = False
        self.last_error = None

    def record_failure(self, error: str):
        self.connected = False
        self.consecutive_failures += 1
        self.last_error = error
        self.last_heartbeat_at = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {
            "broker": self.broker_name,
            "connected": self.connected,
            "last_heartbeat_at": (
                self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None
            ),
            "latency_ms": self.latency_ms,
            "consecutive_failures": self.consecutive_failures,
            "total_reconnects": self.total_reconnects,
            "last_reconnect_at": (
                self.last_reconnect_at.isoformat() if self.last_reconnect_at else None
            ),
            "last_error": self.last_error,
            "circuit_open": self.circuit_open,
        }


class BrokerSupervisor:
    """
    Autonomous broker heartbeat monitor and reconnection manager.

    Runs as HealthMonitorLoop — called every heartbeat_interval_s seconds.
    Uses exponential backoff for reconnect attempts.
    OANDA failures trigger safe downgrade; IBKR failures are logged but non-fatal.
    """

    def __init__(self, broker_manager, config: dict, alert_dispatcher=None):
        self._broker_manager = broker_manager
        self._config = config
        self._alert = alert_dispatcher
        self._lock = threading.Lock()

        sup_cfg = config.get("autonomy", {}).get("broker_supervisor", {})
        self._heartbeat_interval_s = sup_cfg.get("heartbeat_interval_seconds", 30)
        self._max_reconnect_attempts = sup_cfg.get("max_reconnect_attempts", 5)
        self._backoff_base_s = sup_cfg.get("backoff_base_seconds", 10)
        self._backoff_max_s = sup_cfg.get("backoff_max_seconds", 300)
        self._failure_downgrade_threshold = sup_cfg.get("failure_downgrade_threshold", 3)

        # Health tracking per broker
        self._health: Dict[str, BrokerHealthStatus] = {
            "oanda": BrokerHealthStatus("oanda"),
            "ibkr": BrokerHealthStatus("ibkr"),
        }

        # Reconnect state
        self._reconnect_attempts: Dict[str, int] = {"oanda": 0, "ibkr": 0}
        self._next_reconnect_at: Dict[str, Optional[datetime]] = {
            "oanda": None, "ibkr": None
        }

        # Track which orders were in-flight when disconnect happened
        # to avoid re-submitting on reconnect
        self._inflight_order_ids: List[str] = []

        logger.info(
            f"[BrokerSupervisor] Initialised — "
            f"heartbeat={self._heartbeat_interval_s}s "
            f"max_reconnect={self._max_reconnect_attempts} "
            f"backoff={self._backoff_base_s}→{self._backoff_max_s}s"
        )

    # ------------------------------------------------------------------
    # PUBLIC: Called by HealthMonitorLoop
    # ------------------------------------------------------------------

    def run_heartbeat(self):
        """
        Check broker health and attempt reconnect if disconnected.
        Called by autonomous_scheduler HealthMonitorLoop.
        """
        now = datetime.now(timezone.utc)

        # Check OANDA (primary path)
        oanda_ok = self._check_oanda(now)

        # Check IBKR (secondary, non-fatal)
        ibkr_ok = self._check_ibkr(now)

        # Log summary
        logger.debug(
            f"[BrokerSupervisor] Heartbeat: oanda={oanda_ok} ibkr={ibkr_ok}"
        )

        return {"oanda": oanda_ok, "ibkr": ibkr_ok}

    def register_inflight_order(self, order_id: str):
        """Register an order as in-flight (prevent re-submission on reconnect)."""
        with self._lock:
            if order_id not in self._inflight_order_ids:
                self._inflight_order_ids.append(order_id)
            # Keep last 1000
            if len(self._inflight_order_ids) > 1000:
                self._inflight_order_ids.pop(0)

    def is_order_inflight(self, order_id: str) -> bool:
        """Check if this order was already submitted (dedup guard)."""
        with self._lock:
            return order_id in self._inflight_order_ids

    def get_health(self) -> dict:
        """Return health status of all brokers."""
        with self._lock:
            return {
                "oanda": self._health["oanda"].to_dict(),
                "ibkr": self._health["ibkr"].to_dict(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    # ------------------------------------------------------------------
    # INTERNAL: OANDA check (primary path)
    # ------------------------------------------------------------------

    def _check_oanda(self, now: datetime) -> bool:
        """Check OANDA connectivity. Attempt reconnect if disconnected."""
        bm = self._broker_manager
        health = self._health["oanda"]

        # Is the connector present and connected?
        oanda_conn = getattr(bm, "_connector", None)
        if oanda_conn is not None:
            try:
                # Lightweight health probe via connection_status()
                status = oanda_conn.connection_status()
                connected = status.get("connected", False) or status.get("authenticated", False)
                latency = status.get("latency_ms")
                if connected:
                    health.record_success(latency)
                    self._reconnect_attempts["oanda"] = 0
                    self._next_reconnect_at["oanda"] = None
                    return True
                else:
                    health.record_failure(status.get("error", "not connected"))
            except Exception as e:
                health.record_failure(str(e))
        else:
            # No connector at all
            health.record_failure("connector not initialised")

        # Connector missing or unhealthy — attempt reconnect
        return self._attempt_reconnect_oanda(now, health)

    def _attempt_reconnect_oanda(self, now: datetime, health: BrokerHealthStatus) -> bool:
        """
        Attempt OANDA reconnect with exponential backoff.
        Failure above threshold triggers ESM downgrade.
        """
        attempts = self._reconnect_attempts["oanda"]

        # Check if we should wait before next attempt
        next_attempt = self._next_reconnect_at["oanda"]
        if next_attempt and now < next_attempt:
            logger.debug(
                f"[BrokerSupervisor] OANDA reconnect backoff — "
                f"next attempt in {(next_attempt - now).seconds}s"
            )
            return False

        # Too many consecutive failures → downgrade ESM
        if health.consecutive_failures >= self._failure_downgrade_threshold:
            logger.error(
                f"[BrokerSupervisor] OANDA down for "
                f"{health.consecutive_failures} consecutive checks — "
                "blocking live trading"
            )
            esm = getattr(self._broker_manager, "_esm", None)
            if esm and not esm.is_kill_switch_active():
                try:
                    esm.block_live(
                        reason=f"OANDA disconnected after {health.consecutive_failures} heartbeat failures",
                        operator="broker_supervisor",
                    )
                except Exception:
                    pass
            if self._alert:
                self._alert.emit(
                    "BROKER_DISCONNECTED",
                    f"OANDA disconnected (failures={health.consecutive_failures})",
                    health.to_dict(),
                )

        # Attempt reconnect (only up to max_reconnect_attempts)
        if attempts < self._max_reconnect_attempts:
            logger.info(
                f"[BrokerSupervisor] Attempting OANDA reconnect "
                f"(attempt {attempts + 1}/{self._max_reconnect_attempts})"
            )
            try:
                ok, msg = self._broker_manager.connect(
                    broker="oanda", operator="broker_supervisor"
                )
                if ok:
                    health.record_success()
                    health.total_reconnects += 1
                    health.last_reconnect_at = datetime.now(timezone.utc)
                    self._reconnect_attempts["oanda"] = 0
                    self._next_reconnect_at["oanda"] = None
                    logger.info("[BrokerSupervisor] OANDA RECONNECTED ✓")
                    if self._alert:
                        self._alert.emit(
                            "BROKER_CONNECTED",
                            "OANDA reconnected successfully",
                            {},
                        )
                    return True
                else:
                    self._reconnect_attempts["oanda"] += 1
                    self._schedule_backoff("oanda")
                    logger.warning(
                        f"[BrokerSupervisor] OANDA reconnect failed: {msg}"
                    )
            except Exception as e:
                self._reconnect_attempts["oanda"] += 1
                self._schedule_backoff("oanda")
                logger.error(f"[BrokerSupervisor] OANDA reconnect exception: {e}")
        else:
            logger.error(
                f"[BrokerSupervisor] OANDA max reconnect attempts "
                f"({self._max_reconnect_attempts}) exhausted"
            )

        return False

    # ------------------------------------------------------------------
    # INTERNAL: IBKR check (secondary, non-fatal)
    # ------------------------------------------------------------------

    def _check_ibkr(self, now: datetime) -> bool:
        """Check IBKR connectivity. Non-fatal — OANDA continues regardless."""
        bm = self._broker_manager
        health = self._health["ibkr"]

        ibkr_conn = getattr(bm, "_ibkr_connector", None)
        if ibkr_conn is None:
            # IBKR not configured — this is expected in OANDA-only mode
            return True

        try:
            connected = False
            if hasattr(ibkr_conn, "is_connected"):
                connected = ibkr_conn.is_connected()
            elif hasattr(ibkr_conn, "connection_status"):
                status = ibkr_conn.connection_status()
                connected = status.get("connected", False)

            if connected:
                health.record_success()
                self._reconnect_attempts["ibkr"] = 0
                return True
            else:
                health.record_failure("IBKR not connected")
        except Exception as e:
            health.record_failure(str(e))

        # IBKR failure — attempt reconnect if not too many attempts
        return self._attempt_reconnect_ibkr(now, health)

    def _attempt_reconnect_ibkr(self, now: datetime, health: BrokerHealthStatus) -> bool:
        """Non-fatal IBKR reconnect attempt."""
        attempts = self._reconnect_attempts["ibkr"]
        next_attempt = self._next_reconnect_at["ibkr"]

        if next_attempt and now < next_attempt:
            return False

        if attempts < self._max_reconnect_attempts:
            logger.info(
                f"[BrokerSupervisor] Attempting IBKR reconnect "
                f"(attempt {attempts + 1}/{self._max_reconnect_attempts})"
            )
            try:
                ok, msg = self._broker_manager.connect(
                    broker="ibkr", operator="broker_supervisor"
                )
                if ok:
                    health.record_success()
                    health.total_reconnects += 1
                    health.last_reconnect_at = datetime.now(timezone.utc)
                    self._reconnect_attempts["ibkr"] = 0
                    logger.info("[BrokerSupervisor] IBKR RECONNECTED ✓")
                    return True
                else:
                    self._reconnect_attempts["ibkr"] += 1
                    self._schedule_backoff("ibkr")
            except Exception as e:
                self._reconnect_attempts["ibkr"] += 1
                self._schedule_backoff("ibkr")
                logger.warning(f"[BrokerSupervisor] IBKR reconnect failed: {e}")
        return False

    def _schedule_backoff(self, broker: str):
        """Schedule next reconnect attempt with exponential backoff."""
        attempts = self._reconnect_attempts.get(broker, 0)
        delay = min(
            self._backoff_base_s * (2 ** attempts),
            self._backoff_max_s,
        )
        self._next_reconnect_at[broker] = (
            datetime.now(timezone.utc) + timedelta(seconds=delay)
        )
        logger.info(
            f"[BrokerSupervisor] {broker.upper()} backoff: next attempt in {delay}s"
        )

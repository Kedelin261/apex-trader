"""
APEX MULTI-MARKET TJR ENGINE
Agent Orchestrator — Central coordination of the multi-agent workflow.

Enforces the mandatory decision chain:
1. Market Sentinel → environment acceptable
2. Instrument Selection → instrument eligible
3. Regime Detection → regime classified
4. Strategy Engine → signal candidate generated
5. Strategy Validator → signal approved
6. Risk Guardian → risk approved
7. Execution Supervisor → execution ready
8. Trade Executor → order submitted/filled
9. Reporting Agent → lifecycle summary

No step may be skipped for real-money or paper trades.
The orchestrator enforces this chain through protocol messages.

External signal entry point (TradingView webhooks):
  process_external_signal(signal) — runs an externally-supplied Signal object
  through steps 5–8 of the same decision chain without bypassing any gate.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from domain.models import (
    Signal, TJRSetup, Position, Trade, InstrumentType, VenueType,
    EnvironmentMode, StrategyHealthState, TradeOutcome
)
from domain.symbol_utils import normalize_symbol
from core.tjr_strategy_engine import TJRStrategyEngine
from data.market_data_service import MarketDataService
from protocol.agent_protocol import (
    AgentDecisionLedger, AgentStateStore, AgentMessage,
    AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent
from agents.market_sentinel_agent import MarketSentinelAgent
from agents.regime_detection_agent import RegimeDetectionAgent
from agents.strategy_validator_agent import StrategyValidatorAgent
from agents.risk_guardian_agent import RiskGuardianAgent
from agents.execution_supervisor_agent import ExecutionSupervisorAgent
from agents.reporting_agent import ReportingAgent

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """
    Coordinates all agents through the mandatory decision chain.
    Enforces protocol compliance. Logs every decision.
    """

    def __init__(self, config: dict, broker_connector=None):
        self.config = config
        self.mode = EnvironmentMode(
            config.get("system", {}).get("environment", "paper").upper()
        )

        # Core infrastructure
        self.ledger = AgentDecisionLedger(
            config.get("agents", {}).get(
                "decision_ledger_path", "logs/agent_decision_ledger.jsonl"
            )
        )
        self.state_store = AgentStateStore()

        # Strategy and data
        self.strategy_engine = TJRStrategyEngine(config)
        self.data_service = MarketDataService(config)

        # Initialize all agents
        self.market_sentinel = MarketSentinelAgent(self.ledger, self.state_store, config)
        self.regime_agent = RegimeDetectionAgent(self.ledger, self.state_store, config)
        self.strategy_validator = StrategyValidatorAgent(self.ledger, self.state_store, config)
        self.risk_guardian = RiskGuardianAgent(
            self.ledger, self.state_store, config
        )
        self.execution_supervisor = ExecutionSupervisorAgent(
            self.ledger, self.state_store, config, broker_connector
        )
        self.reporting_agent = ReportingAgent(self.ledger, self.state_store, config)

        # State
        self._open_positions: List[Position] = []
        self._all_trades: List[Trade] = []
        self._account_balance = config.get("risk", {}).get("account_balance_default", 100000.0)
        self._kill_switch = False
        self._cycle_count = 0

        logger.info(
            f"[Orchestrator] Initialized: mode={self.mode.value} | "
            f"balance=${self._account_balance:,.0f}"
        )

    def _build_context(self, active_instruments: List[dict],
                       candle_feeds: Dict[str, list],
                       current_spreads: Dict[str, float] = None) -> dict:
        """Build shared context dict for all agents."""
        return {
            "active_instruments": active_instruments,
            "candle_feeds": candle_feeds,
            "current_spreads": current_spreads or {},
            "current_time": datetime.now(timezone.utc),
            "open_positions": self._open_positions,
            "all_trades": self._all_trades,
            "account_balance": self._account_balance,
            "mode": self.mode,
            "pending_signals": [],
            "validated_signals_for_risk": [],
            "execution_queue": []
        }

    def run_cycle(self,
                  active_instruments: List[dict],
                  candle_feeds: Dict[str, list],
                  current_spreads: Dict[str, float] = None) -> dict:
        """
        Execute one complete orchestration cycle.
        Returns summary of the cycle.
        """
        if self._kill_switch:
            logger.warning("[Orchestrator] Kill switch active — skipping cycle")
            return {"status": "KILL_SWITCH_ACTIVE", "cycle": self._cycle_count}

        self._cycle_count += 1
        cycle_messages: List[AgentMessage] = []
        cycle_signals_generated = 0
        cycle_trades_attempted = 0
        cycle_trades_executed = 0

        context = self._build_context(active_instruments, candle_feeds, current_spreads)

        # ── STEP 1: Market Sentinel ─────────────────────────────────────────
        sentinel_messages = self.market_sentinel.safe_run(context)
        cycle_messages.extend(sentinel_messages)

        tradeable_symbols = self.state_store.get("tradeable_instruments", [])
        if not tradeable_symbols:
            logger.info("[Orchestrator] No tradeable instruments this cycle")
            return {"status": "NO_TRADEABLE_INSTRUMENTS", "cycle": self._cycle_count}

        # Filter to tradeable instruments only
        tradeable_instruments = [
            i for i in active_instruments
            if i.get("symbol") in tradeable_symbols
        ]

        context["active_instruments"] = tradeable_instruments

        # ── STEP 2: Regime Detection ────────────────────────────────────────
        regime_messages = self.regime_agent.safe_run(context)
        cycle_messages.extend(regime_messages)

        regime_context = self.state_store.get("regime_context", {})

        # ── STEP 3: Signal Generation (TJR Engine) ──────────────────────────
        pending_signals = []

        for inst_info in tradeable_instruments:
            symbol = inst_info.get("symbol", "")
            inst_type_str = inst_info.get("type", "FOREX")
            venue_str = inst_info.get("venue", "CFD_BROKER")

            try:
                inst_type = InstrumentType(inst_type_str)
                venue_type = VenueType(venue_str)
            except ValueError:
                continue

            candles = candle_feeds.get(symbol, [])
            if len(candles) < 50:
                continue

            # Check regime compatibility
            regime_data = regime_context.get(symbol, {})
            eligible_strategies = regime_data.get("eligible_strategies", ["TJR"])
            if "TJR" not in eligible_strategies:
                logger.debug(f"[Orchestrator] TJR not eligible for {symbol} in current regime")
                continue

            # Run TJR analysis (no lookahead: strategy sees all candles up to now)
            setup: Optional[TJRSetup] = self.strategy_engine.analyze(
                candles=candles,
                instrument=symbol,
                instrument_type=inst_type,
                venue_type=venue_type,
                timeframe=inst_info.get("timeframe", "M15")
            )

            if setup is None:
                continue

            # Build signal
            signal = self.strategy_engine.build_signal(
                setup=setup,
                venue_type=venue_type,
                account_balance=self._account_balance,
                risk_pct=self.config.get("risk", {}).get("max_risk_per_trade_pct", 1.0)
            )

            if signal is None:
                continue

            # Emit signal candidate
            candidate_msg = AgentMessage(
                source_agent=AgentName.ORCHESTRATOR,
                message_type=AgentMessageType.SIGNAL_CANDIDATE,
                payload={
                    "signal_id": signal.signal_id,
                    "instrument": symbol,
                    "direction": signal.direction.value,
                    "setup_quality": setup.setup_quality_score
                },
                instrument=symbol,
                confidence=setup.setup_quality_score,
                final_status="CANDIDATE"
            )
            self.ledger.record(candidate_msg)
            cycle_messages.append(candidate_msg)
            cycle_signals_generated += 1

            pending_signals.append({
                "signal": signal,
                "setup": setup,
                "parent_message_id": candidate_msg.message_id
            })

        if not pending_signals:
            return {
                "status": "NO_SIGNALS",
                "cycle": self._cycle_count,
                "messages": len(cycle_messages)
            }

        context["pending_signals"] = pending_signals

        # ── STEP 4: Strategy Validator ──────────────────────────────────────
        validator_messages = self.strategy_validator.safe_run(context)
        cycle_messages.extend(validator_messages)

        validated_signals_state = self.state_store.get("validated_signals", {})

        # Build risk evaluation queue (only validated signals)
        risk_queue = []
        for item in pending_signals:
            signal = item["signal"]
            val_state = validated_signals_state.get(signal.signal_id, {})
            if val_state.get("approved", False):
                risk_queue.append({
                    "signal": signal,
                    "parent_message_id": val_state.get("message_id"),
                    "news_active": False,
                    "volatility_spike": False
                })

        if not risk_queue:
            return {
                "status": "ALL_SIGNALS_REJECTED_BY_VALIDATOR",
                "cycle": self._cycle_count,
                "messages": len(cycle_messages)
            }

        context["validated_signals_for_risk"] = risk_queue

        # ── STEP 5: Risk Guardian ───────────────────────────────────────────
        risk_messages = self.risk_guardian.safe_run(context)
        cycle_messages.extend(risk_messages)

        # Check if kill switch was triggered
        kill_events = self.ledger.get_kill_switch_events()
        if kill_events:
            self._kill_switch = True
            logger.critical("[Orchestrator] Kill switch triggered — halting")
            return {"status": "KILL_SWITCH_TRIGGERED", "cycle": self._cycle_count}

        # Build execution queue (only risk-approved)
        exec_queue = []
        for msg in risk_messages:
            if msg.message_type == AgentMessageType.RISK_APPROVED:
                signal_id = msg.payload.get("signal_id")
                # Find the signal
                for item in pending_signals:
                    if item["signal"].signal_id == signal_id:
                        exec_queue.append({
                            "signal": item["signal"],
                            "parent_message_id": msg.message_id
                        })
                        cycle_trades_attempted += 1
                        break

        if not exec_queue:
            return {
                "status": "ALL_SIGNALS_REJECTED_BY_RISK",
                "cycle": self._cycle_count,
                "messages": len(cycle_messages)
            }

        context["execution_queue"] = exec_queue

        # ── STEP 6: Execution Supervisor ────────────────────────────────────
        exec_messages = self.execution_supervisor.safe_run(context)
        cycle_messages.extend(exec_messages)

        # Track executed trades
        for msg in exec_messages:
            if msg.message_type == AgentMessageType.ORDER_FILLED:
                cycle_trades_executed += 1
                # Create Trade record and add to state
                signal_id = msg.payload.get("signal_id")
                for item in pending_signals:
                    if item["signal"].signal_id == signal_id:
                        sig = item["signal"]
                        trade = Trade(
                            signal_id=sig.signal_id,
                            order_id=str(uuid.uuid4()),
                            instrument=sig.instrument,
                            instrument_type=sig.instrument_type,
                            direction=sig.direction,
                            strategy_family=sig.strategy_family,
                            session=sig.session,
                            entry_price=msg.payload.get("filled_price", sig.entry_price),
                            entry_time=datetime.now(timezone.utc),
                            stop_loss=sig.stop_loss,
                            take_profit=sig.take_profit,
                            position_size_lots=sig.position_size_lots,
                            risk_amount_usd=sig.risk_amount_usd,
                            reward_to_risk=sig.reward_to_risk,
                            strategy_reason=sig.strategy_reason,
                            agent_decision_chain=[
                                m.message_id for m in cycle_messages
                                if m.instrument == sig.instrument
                            ],
                            outcome=TradeOutcome.OPEN,
                            is_paper=(self.mode == EnvironmentMode.PAPER)
                        )
                        self._all_trades.append(trade)
                        break

        # ── STEP 7: Reporting (periodic) ────────────────────────────────────
        if self._cycle_count % 100 == 0:
            report_context = {**context, "all_trades": self._all_trades}
            report_messages = self.reporting_agent.safe_run(report_context)
            cycle_messages.extend(report_messages)

        summary = {
            "status": "CYCLE_COMPLETE",
            "cycle": self._cycle_count,
            "signals_generated": cycle_signals_generated,
            "trades_attempted": cycle_trades_attempted,
            "trades_executed": cycle_trades_executed,
            "messages_emitted": len(cycle_messages),
            "total_trades": len(self._all_trades),
            "ledger_size": self.ledger.total_messages
        }

        logger.info(
            f"[Orchestrator] Cycle {self._cycle_count}: "
            f"signals={cycle_signals_generated} | "
            f"executed={cycle_trades_executed} | "
            f"ledger={self.ledger.total_messages}"
        )

        return summary

    # =========================================================================
    # EXTERNAL SIGNAL PROCESSING  (TradingView webhook → agent pipeline)
    # =========================================================================

    def process_external_signal(self, signal: Signal) -> dict:
        """
        Process an externally-supplied Signal (e.g. from a TradingView webhook)
        through the mandatory agent decision chain (steps 5-8):

          5. Strategy Validator  →  validates price/SL/TP/RR rules
          6. Risk Guardian       →  checks drawdown / daily-loss / concurrent positions
          7. Execution Supervisor →  submits to paper broker or live OANDA depending on ESM state
          8. Trade record creation on fill

        This method is the only authorised entry point for external signals.
        It communicates exclusively through the Intent Layer (ledger + state store).

        Returns:
            dict with keys:
              status         – KILL_SWITCH_ACTIVE | SIGNAL_REJECTED | RISK_REJECTED |
                               EXECUTION_BLOCKED | FILLED | ORDER_FAILED
              cycle          – current cycle number
              signal_id      – UUID of the processed signal
              messages_emitted – count of ledger messages added in this call
              ledger_size    – total ledger size after processing
              trades_attempted – 0 or 1
              trades_executed  – 0 or 1
              rejection_reasons – list[str] explaining any block
        """
        if self._kill_switch:
            logger.warning(
                f"[Orchestrator] External signal BLOCKED — kill switch active "
                f"(signal_id={signal.signal_id})"
            )
            return {
                "status": "KILL_SWITCH_ACTIVE",
                "cycle": self._cycle_count,
                "signal_id": signal.signal_id,
                "messages_emitted": 0,
                "ledger_size": self.ledger.total_messages,
                "trades_attempted": 0,
                "trades_executed": 0,
                "rejection_reasons": ["Kill switch is active — all trading halted"],
            }

        # ── Normalise symbol to canonical format ───────────────────────────
        # Ensures XAU_USD, xau/usd, GOLD etc. all become XAUUSD inside the pipeline.
        canonical = normalize_symbol(signal.instrument)
        if canonical != signal.instrument:
            logger.info(
                f"[Orchestrator] Symbol normalised: {signal.instrument!r} → {canonical!r} "
                f"(signal_id={signal.signal_id})"
            )
            # Use Pydantic model_copy so the immutable model is not mutated directly
            signal = signal.model_copy(update={"instrument": canonical})

        # ── Increment cycle counter ─────────────────────────────────────────
        self._cycle_count += 1
        cycle_messages: List[AgentMessage] = []
        ledger_before = self.ledger.total_messages

        # ── Record SIGNAL_CANDIDATE in ledger ──────────────────────────────
        candidate_msg = AgentMessage(
            source_agent=AgentName.ORCHESTRATOR,
            message_type=AgentMessageType.SIGNAL_CANDIDATE,
            payload={
                "signal_id": signal.signal_id,
                "instrument": signal.instrument,
                "direction": signal.direction.value,
                "entry": signal.entry_price,
                "sl": signal.stop_loss,
                "tp": signal.take_profit,
                "rr": signal.reward_to_risk,
                "strategy": signal.strategy_family.value,
                "source": "external_webhook",
            },
            instrument=signal.instrument,
            confidence=signal.confidence_score,
            final_status="CANDIDATE",
        )
        self.ledger.record(candidate_msg)
        cycle_messages.append(candidate_msg)

        logger.info(
            f"[Orchestrator] External signal received: cycle={self._cycle_count} "
            f"signal_id={signal.signal_id} "
            f"{signal.instrument} {signal.direction.value} "
            f"entry={signal.entry_price} sl={signal.stop_loss} tp={signal.take_profit}"
        )

        # ── STEP 5: Strategy Validator ──────────────────────────────────────
        # Pass through validate_tjr_signal directly (no TJRSetup for external
        # signals — only the rules that operate on the Signal object itself apply).
        val_msg = self.strategy_validator.validate_tjr_signal(
            signal=signal,
            setup=None,           # No TJRSetup for external signals
            parent_msg_id=candidate_msg.message_id,
        )
        cycle_messages.append(val_msg)

        if val_msg.final_status != "APPROVED":
            logger.info(
                f"[Orchestrator] External signal VALIDATOR REJECTED: "
                f"{'; '.join(val_msg.rejection_reasons)}"
            )
            return {
                "status": "SIGNAL_REJECTED",
                "cycle": self._cycle_count,
                "signal_id": signal.signal_id,
                "messages_emitted": len(cycle_messages),
                "ledger_size": self.ledger.total_messages,
                "trades_attempted": 0,
                "trades_executed": 0,
                "rejection_reasons": val_msg.rejection_reasons,
            }

        # Update state store so risk_guardian.run() could also see it
        validated_signals_state = self.state_store.get("validated_signals", {})
        validated_signals_state[signal.signal_id] = {
            "approved": True,
            "message_id": val_msg.message_id,
            "reasons": [],
        }
        self.state_store.set("validated_signals", validated_signals_state)

        # ── STEP 6: Risk Guardian ───────────────────────────────────────────
        risk_msg = self.risk_guardian.evaluate_signal(
            signal=signal,
            open_positions=self._open_positions,
            current_spread_pips=1.5,    # Conservative default for external signals
            regime=None,
            news_active=False,
            volatility_spike=False,
            parent_msg_id=val_msg.message_id,
        )
        cycle_messages.append(risk_msg)

        # Check if risk triggered a kill switch
        kill_events = self.ledger.get_kill_switch_events()
        if kill_events and not self._kill_switch:
            self._kill_switch = True
            logger.critical("[Orchestrator] Kill switch triggered by risk guardian")

        if risk_msg.message_type != AgentMessageType.RISK_APPROVED:
            logger.info(
                f"[Orchestrator] External signal RISK REJECTED: "
                f"{'; '.join(risk_msg.rejection_reasons)}"
            )
            return {
                "status": "RISK_REJECTED",
                "cycle": self._cycle_count,
                "signal_id": signal.signal_id,
                "messages_emitted": len(cycle_messages),
                "ledger_size": self.ledger.total_messages,
                "trades_attempted": 1,
                "trades_executed": 0,
                "rejection_reasons": risk_msg.rejection_reasons,
            }

        # ── STEP 7: Execution Supervisor ────────────────────────────────────
        exec_msg = self.execution_supervisor.submit_order(
            signal=signal,
            parent_msg_id=risk_msg.message_id,
        )
        cycle_messages.append(exec_msg)

        trades_executed = 0
        final_status = exec_msg.final_status or "UNKNOWN"
        rejection_reasons: List[str] = exec_msg.rejection_reasons or []

        # ── STEP 8: Trade record on fill ───────────────────────────────────
        if exec_msg.message_type == AgentMessageType.ORDER_FILLED:
            trades_executed = 1
            trade = Trade(
                signal_id=signal.signal_id,
                order_id=str(uuid.uuid4()),
                instrument=signal.instrument,
                instrument_type=signal.instrument_type,
                direction=signal.direction,
                strategy_family=signal.strategy_family,
                session=signal.session,
                entry_price=exec_msg.payload.get("filled_price", signal.entry_price),
                entry_time=datetime.now(timezone.utc),
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                position_size_lots=signal.position_size_lots,
                risk_amount_usd=signal.risk_amount_usd,
                reward_to_risk=signal.reward_to_risk,
                strategy_reason=signal.strategy_reason,
                agent_decision_chain=[m.message_id for m in cycle_messages],
                outcome=TradeOutcome.OPEN,
                is_paper=signal.is_paper_trade,
                notes=f"External webhook signal | cycle={self._cycle_count}",
            )
            self._all_trades.append(trade)
            logger.info(
                f"[Orchestrator] External signal FILLED: "
                f"{signal.instrument} {signal.direction.value} "
                f"@ {trade.entry_price:.5f} | trade_id={trade.trade_id[:8]} "
                f"{'PAPER' if signal.is_paper_trade else 'LIVE'}"
            )
            final_status = "FILLED"

        elif exec_msg.message_type == AgentMessageType.EXECUTION_BLOCKED:
            final_status = "EXECUTION_BLOCKED"
        elif exec_msg.message_type == AgentMessageType.ORDER_FAILED:
            final_status = "ORDER_FAILED"

        messages_emitted = self.ledger.total_messages - ledger_before

        summary = {
            "status": final_status,
            "cycle": self._cycle_count,
            "signal_id": signal.signal_id,
            "messages_emitted": messages_emitted,
            "ledger_size": self.ledger.total_messages,
            "trades_attempted": 1,
            "trades_executed": trades_executed,
            "rejection_reasons": rejection_reasons,
        }

        logger.info(
            f"[Orchestrator] External signal complete: "
            f"cycle={self._cycle_count} status={final_status} "
            f"ledger_size={self.ledger.total_messages}"
        )
        return summary

    def get_agent_health(self) -> Dict[str, dict]:
        """Return health status of all agents."""
        return {
            "market_sentinel": self.market_sentinel.health.to_dict(),
            "regime_detection": self.regime_agent.health.to_dict(),
            "strategy_validator": self.strategy_validator.health.to_dict(),
            "risk_guardian": self.risk_guardian.health.to_dict(),
            "execution_supervisor": self.execution_supervisor.health.to_dict(),
            "reporting": self.reporting_agent.health.to_dict()
        }

    def get_system_state(self) -> dict:
        """Return complete system state snapshot."""
        return {
            "mode": self.mode.value,
            "kill_switch": self._kill_switch,
            "cycle_count": self._cycle_count,
            "account_balance": self._account_balance,
            "open_positions": len(self._open_positions),
            "total_trades": len(self._all_trades),
            "ledger_messages": self.ledger.total_messages,
            "agent_health": self.get_agent_health(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

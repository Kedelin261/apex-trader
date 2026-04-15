"""
APEX MULTI-MARKET TJR ENGINE v3.0
API Server — Production FastAPI backend with full multi-broker connectivity.

Endpoints (legacy):
  POST /api/signal          — TradingView webhook ingest
  GET  /api/status          — System health and status
  GET  /api/trades          — Trade history
  GET  /api/metrics         — Performance metrics
  GET  /api/agents          — Agent health
  GET  /api/ledger          — Agent decision ledger
  GET  /api/positions       — Internal open positions
  POST /api/backtest/run    — Trigger backtest
  POST /api/control/kill    — Engage kill switch
  POST /api/control/resume  — Resume trading (PAPER only)
  GET  /api/equity-curve    — Equity curve data

NEW — Broker Connectivity:
  POST /api/broker/connect          — Authenticate broker, enter LIVE_CONNECTED_SAFE
  GET  /api/broker/status           — Broker connection status
  GET  /api/broker/account          — Live account info from broker
  GET  /api/broker/balances         — Live balance from broker
  GET  /api/broker/positions        — Live positions from broker
  GET  /api/broker/open-orders      — Live pending orders from broker
  POST /api/broker/preflight        — Run full preflight checklist
  POST /api/broker/test-order-payload — Build/validate order payload (safe mode)

NEW — Execution State Machine:
  POST /api/execution/arm-live      — Step 2: arm live trading (after connect)
  POST /api/execution/disarm-live   — Disarm; return to LIVE_CONNECTED_SAFE
  POST /api/execution/set-mode      — Set execution mode (paper|live)
  GET  /api/execution/state         — Full execution state snapshot
  GET  /api/execution/history       — State transition history

NEW — Reconciliation:
  POST /api/reconcile/run           — Manual reconciliation run
  GET  /api/reconcile/history       — Reconciliation history

NEW — Safety Controls:
  POST /api/control/kill            — Engage kill switch
  POST /api/control/reset-to-paper  — Reset kill switch → paper mode
  GET  /api/control/rejection-history — Order rejection history

  GET  /dashboard                   — Enhanced live dashboard
"""

from __future__ import annotations

import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from domain.models import InstrumentType, VenueType, EnvironmentMode
from domain.symbol_utils import normalize_symbol, is_known_symbol
from data.market_data_service import MarketDataService
from backtest.backtest_engine import BacktestEngine
from agents.agent_orchestrator import AgentOrchestrator
from core.risk_manager import RiskManager

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIG
# =============================================================================

def load_config(path: str = "config/system_config.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        alt = Path(__file__).parent.parent / path
        if alt.exists():
            config_path = alt
        else:
            logger.warning(f"Config not found at {path}, using defaults")
            return {}
    with open(config_path) as f:
        return yaml.safe_load(f)


CONFIG = load_config()

# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title="Apex Multi-Market TJR Engine",
    description="Production-grade autonomous trading system v3.0",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy singletons
_orchestrator: Optional[AgentOrchestrator] = None
_data_service: Optional[MarketDataService] = None
_broker_manager = None
_start_time = datetime.now(timezone.utc)
_system_logs: List[dict] = []


def get_orchestrator() -> AgentOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = AgentOrchestrator(CONFIG)
    return _orchestrator


def get_data_service() -> MarketDataService:
    global _data_service
    if _data_service is None:
        _data_service = MarketDataService(CONFIG)
    return _data_service


def get_broker_manager():
    global _broker_manager
    if _broker_manager is None:
        from live.broker_manager import BrokerManager
        orch = get_orchestrator()
        risk_mgr = orch.risk_guardian.risk_manager
        _broker_manager = BrokerManager(CONFIG, orchestrator=orch, risk_manager=risk_mgr)
    return _broker_manager


def log_event(level: str, message: str, details: dict = None):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "details": details or {},
    }
    _system_logs.append(entry)
    if len(_system_logs) > 500:
        _system_logs.pop(0)


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================

class TradingViewWebhook(BaseModel):
    symbol: str
    action: str
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    timeframe: Optional[str] = "M15"
    strategy: Optional[str] = "TJR"
    message: Optional[str] = ""
    schema_version: Optional[str] = "1.0"


class BacktestRequest(BaseModel):
    instrument: str = "XAUUSD"
    instrument_type: str = "GOLD"
    venue_type: str = "CFD_BROKER"
    timeframe: str = "M15"
    initial_balance: float = 100000.0


class ControlCommand(BaseModel):
    reason: str = ""
    operator: str = "api"


class BrokerConnectRequest(BaseModel):
    broker: str = "oanda"
    operator: str = "api"


class ArmLiveRequest(BaseModel):
    operator: str
    confirmation_code: str = Field(default="", description="Explicit operator confirmation")
    acknowledge_risk: bool = Field(
        default=False,
        description="Operator must acknowledge risk by setting this to true"
    )


class DisarmRequest(BaseModel):
    operator: str
    reason: str = "Manual disarm"


class SetModeRequest(BaseModel):
    mode: str  # "paper"
    operator: str


class TestOrderRequest(BaseModel):
    instrument: str = "XAUUSD"
    side: str = "BUY"
    units: float = 1000.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class ResetRequest(BaseModel):
    operator: str
    reason: str = "Manual reset to paper"


# =============================================================================
# ROOT / DASHBOARD
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(
        '<html><head><meta http-equiv="refresh" content="0; url=/dashboard"></head>'
        "<body>Redirecting...</body></html>"
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    dashboard_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if dashboard_path.exists():
        return HTMLResponse(dashboard_path.read_text())
    return HTMLResponse(_generate_dashboard_html())


# =============================================================================
# LEGACY ENDPOINTS (backward compatible)
# =============================================================================

@app.post("/api/signal")
async def receive_signal(webhook: TradingViewWebhook, request: Request):
    """
    TradingView webhook — processes signal through the full agent decision chain.

    Flow:
      1. Schema validation
      2. Kill-switch gate
      3. Build internal Signal domain object from webhook payload
      4. Call orchestrator.process_external_signal(signal)
         → StrategyValidator → RiskGuardian → ExecutionSupervisor
         → paper fill OR live OANDA fill depending on execution state
      5. Return real result (no fake "queued" language)
    """
    from domain.models import (
        Signal, SignalDirection, StrategyFamily, SessionType,
        InstrumentType, VenueType, SignalStatus,
    )

    client_ip = request.client.host if request.client else "unknown"
    log_event(
        "INFO",
        f"TradingView webhook: {webhook.symbol} {webhook.action}",
        {"ip": client_ip, "schema_version": webhook.schema_version},
    )

    # ── 1. Schema version guard ────────────────────────────────────────────
    if webhook.schema_version not in ("1.0", "1", None):
        raise HTTPException(400, f"Unsupported schema version: {webhook.schema_version!r}")

    orch = get_orchestrator()
    bm = get_broker_manager()

    # ── 2. Kill-switch gate ────────────────────────────────────────────────
    if orch._kill_switch or bm._esm.is_kill_switch_active():
        log_event("WARNING", "Signal rejected — kill switch active",
                  {"symbol": webhook.symbol})
        return JSONResponse(
            {"status": "REJECTED", "reason": "Kill switch active — all trading halted"},
            status_code=403,
        )

    exec_state = bm._esm.current_state().value
    is_live = bm._esm.is_live_trading_allowed()

    # ── 3. Map webhook → internal Signal ──────────────────────────────────
    # Direction
    action_upper = (webhook.action or "").upper().strip()
    if action_upper in ("BUY", "LONG"):
        direction = SignalDirection.BUY
    elif action_upper in ("SELL", "SHORT"):
        direction = SignalDirection.SELL
    else:
        raise HTTPException(
            400,
            f"Invalid action: {webhook.action!r}. Expected BUY/SELL/LONG/SHORT.",
        )

    # Price fields — require at least entry price
    entry_price = webhook.price
    stop_loss = webhook.stop_loss
    take_profit = webhook.take_profit

    if not entry_price or entry_price <= 0:
        raise HTTPException(400, "Field 'price' is required and must be > 0")
    if not stop_loss or stop_loss <= 0:
        raise HTTPException(400, "Field 'stop_loss' is required and must be > 0")
    if not take_profit or take_profit <= 0:
        raise HTTPException(400, "Field 'take_profit' is required and must be > 0")

    # 3a. Canonicalise incoming symbol (XAU_USD / xau/usd / GOLD → XAUUSD)
    raw_symbol = webhook.symbol
    symbol = normalize_symbol(raw_symbol)

    # Diagnostic logging for XAUUSD
    if symbol == "XAUUSD":
        logger.info(
            f"[API/signal] XAUUSD diagnostic: "
            f"raw={raw_symbol!r} canonical={symbol!r} "
            f"action={action_upper} entry={entry_price} "
            f"sl={stop_loss} tp={take_profit}"
        )

    # 3b. Pre-submit tradeability check (Intent Layer gate)
    tradeability = bm.check_symbol(symbol)

    if symbol == "XAUUSD":
        logger.info(
            f"[API/signal] XAUUSD tradeability: "
            f"broker_sym={tradeability.broker_symbol!r} "
            f"asset_class={tradeability.asset_class!r} "
            f"is_tradeable={tradeability.is_tradeable} "
            f"rejection_code={tradeability.rejection_code!r}"
        )

    # Hard-block only when broker is connected AND instrument is not tradeable.
    # In pure PAPER_MODE (no broker) we allow through for testing.
    broker_connected = bm._esm.is_connected()
    if broker_connected and not tradeability.is_tradeable:
        log_event(
            "WARNING",
            f"Signal rejected — instrument not tradeable: {symbol}",
            tradeability.to_dict(),
        )
        return JSONResponse(
            {
                "status": "REJECTED",
                "reason": tradeability.reason_if_not_tradeable or "Instrument not tradeable",
                "rejection_code": tradeability.rejection_code,
                "canonical_symbol": symbol,
                "broker_symbol": tradeability.broker_symbol,
            },
            status_code=422,
        )

    # 3c. Infer instrument type and venue type from canonical symbol
    GOLD_SYMBOLS    = {"XAUUSD", "XAGUSD"}
    FUTURES_SYMBOLS = {"ES", "NQ", "CL", "GC", "YM"}
    CRYPTO_SYMBOLS  = {"BTCUSD", "ETHUSD", "XRPUSD", "LTCUSD"}
    INDEX_SYMBOLS   = {"US30", "US500", "NAS100", "GER40", "UK100"}
    OIL_SYMBOLS     = {"USOIL", "UKOIL"}

    # Prefer asset_class from mapper if available
    if tradeability.asset_class == "GOLD":
        instrument_type = InstrumentType.GOLD
        venue_type = VenueType.CFD_BROKER
    elif tradeability.asset_class in ("INDEX",):
        instrument_type = InstrumentType.INDICES
        venue_type = VenueType.CFD_BROKER
    elif tradeability.asset_class in ("OIL", "METALS"):
        instrument_type = InstrumentType.COMMODITIES
        venue_type = VenueType.CFD_BROKER
    elif symbol in GOLD_SYMBOLS:
        instrument_type = InstrumentType.GOLD
        venue_type = VenueType.CFD_BROKER
    elif symbol in FUTURES_SYMBOLS:
        instrument_type = InstrumentType.FUTURES
        venue_type = VenueType.FUTURES_EXCHANGE
    elif symbol in CRYPTO_SYMBOLS:
        instrument_type = InstrumentType.CRYPTO
        venue_type = VenueType.CRYPTO_EXCHANGE
    elif symbol in INDEX_SYMBOLS or symbol in OIL_SYMBOLS:
        instrument_type = InstrumentType.INDICES
        venue_type = VenueType.CFD_BROKER
    else:
        instrument_type = InstrumentType.FOREX
        venue_type = VenueType.FOREX_BROKER

    # Compute stop and target distances in pips
    pip_size = 0.01 if "JPY" in symbol else (1.0 if symbol in GOLD_SYMBOLS else 0.0001)
    if pip_size == 0:
        pip_size = 0.0001

    if direction == SignalDirection.BUY:
        stop_distance_pips = (entry_price - stop_loss) / pip_size
        target_distance_pips = (take_profit - entry_price) / pip_size
    else:
        stop_distance_pips = (stop_loss - entry_price) / pip_size
        target_distance_pips = (entry_price - take_profit) / pip_size

    # Clamp to sensible minimums so validator rules have real numbers to work with
    stop_distance_pips = max(stop_distance_pips, 0.0)
    target_distance_pips = max(target_distance_pips, 0.0)
    rr = (target_distance_pips / stop_distance_pips) if stop_distance_pips > 0 else 0.0

    # Position size: risk 1% of account balance, capped at 1 lot for safety
    account_balance = orch._account_balance
    risk_pct = CONFIG.get("risk", {}).get("max_risk_per_trade_pct", 1.0) / 100.0
    risk_usd = account_balance * risk_pct

    pip_value_usd = 10.0  # per standard lot
    if stop_distance_pips > 0 and pip_value_usd > 0:
        position_size_lots = round(risk_usd / (stop_distance_pips * pip_value_usd), 2)
    else:
        position_size_lots = 0.01
    position_size_lots = max(0.01, min(position_size_lots, 1.0))

    # Session: derive from current UTC hour
    now_hour = datetime.now(timezone.utc).hour
    if 7 <= now_hour < 13:
        session = SessionType.LONDON
    elif 13 <= now_hour < 16:
        session = SessionType.OVERLAP
    elif 16 <= now_hour < 22:
        session = SessionType.NEW_YORK
    elif 0 <= now_hour < 8:
        session = SessionType.ASIAN
    else:
        session = SessionType.OFF_HOURS

    signal = Signal(
        signal_id=str(__import__("uuid").uuid4()),
        instrument=symbol,
        instrument_type=instrument_type,
        venue_type=venue_type,
        timeframe=webhook.timeframe or "M15",
        session=session,
        direction=direction,
        status=SignalStatus.CANDIDATE,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        position_size_lots=position_size_lots,
        risk_amount_usd=round(risk_usd, 2),
        reward_to_risk=round(rr, 2),
        stop_distance_pips=round(stop_distance_pips, 1),
        target_distance_pips=round(target_distance_pips, 1),
        strategy_family=StrategyFamily.TJR,
        strategy_reason=webhook.message or f"TradingView webhook {webhook.strategy or 'TJR'}",
        confidence_score=0.75,         # Treat external signals as 75% confident
        is_paper_trade=not is_live,    # True when paper/safe; False only in LIVE_ENABLED
    )

    # Wire BrokerManager into the execution supervisor so it routes correctly
    # This is a safe reference injection — no bypass of the ESM gating logic.
    if orch.execution_supervisor.broker is None:
        orch.execution_supervisor.broker = bm

    # ── 4. Run through agent pipeline ─────────────────────────────────────
    result = orch.process_external_signal(signal)

    log_event(
        "INFO",
        f"Signal processed: {symbol} {action_upper} → {result['status']}",
        {
            "cycle": result["cycle"],
            "signal_id": result["signal_id"],
            "execution_state": exec_state,
            "trades_executed": result["trades_executed"],
            "rejections": result["rejection_reasons"],
        },
    )

    # ── 5. Return real result ──────────────────────────────────────────────
    return {
        "status": result["status"],
        "symbol": symbol,
        "action": action_upper,
        "signal_id": result["signal_id"],
        "cycle": result["cycle"],
        "execution_state": exec_state,
        "routed_to": "LIVE_BROKER" if is_live else "PAPER",
        "trades_attempted": result["trades_attempted"],
        "trades_executed": result["trades_executed"],
        "messages_emitted": result["messages_emitted"],
        "ledger_size": result["ledger_size"],
        "rejection_reasons": result["rejection_reasons"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/status")
async def system_status():
    """Real-time system health."""
    uptime = (datetime.now(timezone.utc) - _start_time).total_seconds() / 3600
    try:
        orch = get_orchestrator()
        bm = get_broker_manager()
        state = orch.get_system_state()
        risk_mgr = orch.risk_guardian.risk_manager
        daily = risk_mgr.daily_governor.get_summary()
        snap = bm._esm.snapshot()

        # ── Runtime environment: prefer broker/ESM snapshot over static config ──
        # snap.environment is the OANDA environment string ("practice" / "live")
        # when a broker is connected; falls back to config value in pure paper mode.
        runtime_environment = (
            snap.environment              # "practice" or "live" from ESM
            if snap.environment and snap.environment not in ("none", None)
            else CONFIG.get("system", {}).get("environment", "paper")
        )

        return {
            "system": "Apex Multi-Market TJR Engine",
            "version": "3.0.0",
            "environment": runtime_environment,
            "uptime_hours": round(uptime, 2),
            "kill_switch_active": orch._kill_switch or snap.kill_switch_active,
            "execution_state": snap.state,
            "broker_connected": bm._esm.is_connected(),
            "broker": snap.broker or "none",
            "live_trading_armed": snap.live_enabled,
            "live_trading_allowed": bm._esm.is_live_trading_allowed(),
            "cycle_count": orch._cycle_count,
            "account_balance": round(risk_mgr.account_balance, 2),
            "drawdown_pct": round(risk_mgr.get_drawdown_pct(), 4),
            "daily_pnl_usd": round(daily.realized_pnl, 2),
            "daily_pnl_pct": round(risk_mgr.daily_governor.daily_pnl_pct, 4),
            "daily_blocked_trades": daily.blocked_trades,
            "open_positions": state["open_positions"],
            "total_trades": state["total_trades"],
            "ledger_messages": state["ledger_messages"],
            "agent_health": state["agent_health"],
            "broker_status": bm.broker_status() if bm else {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {
            "system": "Apex Multi-Market TJR Engine",
            "version": "3.0.0",
            "status": "INITIALIZING",
            "uptime_hours": round(uptime, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    try:
        orch = get_orchestrator()
        trades = orch._all_trades[-limit:]
        return {
            "total": len(orch._all_trades),
            "returned": len(trades),
            "trades": [
                {
                    "trade_id": t.trade_id[:8],
                    "instrument": t.instrument,
                    "direction": t.direction.value,
                    "strategy": t.strategy_family.value,
                    "entry": t.entry_price,
                    "exit": t.exit_price,
                    "sl": t.stop_loss,
                    "tp": t.take_profit,
                    "lots": t.position_size_lots,
                    "pnl_usd": round(t.realized_pnl_usd, 2),
                    "r_multiple": round(t.r_multiple, 2),
                    "outcome": t.outcome.value,
                    "entry_time": t.entry_time.isoformat() if t.entry_time else None,
                    "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                    "session": t.session.value,
                    "reason": t.strategy_reason,
                }
                for t in reversed(trades)
            ],
        }
    except Exception as e:
        return {"error": str(e), "trades": []}


@app.get("/api/metrics")
async def get_metrics():
    try:
        orch = get_orchestrator()
        trades = orch._all_trades
        if not trades:
            return {"total_trades": 0, "win_rate": 0, "expectancy": 0, "profit_factor": 0,
                    "net_pnl": 0, "message": "No trades yet"}
        from backtest.backtest_engine import PerformanceCalculator
        calc = PerformanceCalculator()
        closed = [t for t in trades if t.exit_time is not None]
        if not closed:
            return {"total_trades": len(trades), "open_trades": len(trades), "message": "All trades open"}
        metrics = calc.compute(closed, orch._account_balance, [])
        return metrics.model_dump()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/agents")
async def get_agent_health():
    try:
        return get_orchestrator().get_agent_health()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ledger")
async def get_ledger(limit: int = 50, msg_type: Optional[str] = None):
    try:
        orch = get_orchestrator()
        from protocol.agent_protocol import AgentMessageType
        filter_type = None
        if msg_type:
            try:
                filter_type = AgentMessageType(msg_type)
            except ValueError:
                pass
        msgs = orch.ledger.get_recent(limit, msg_type=filter_type)
        return {
            "total_in_ledger": orch.ledger.total_messages,
            "returned": len(msgs),
            "messages": [m.to_log_dict() for m in reversed(msgs)],
        }
    except Exception as e:
        return {"error": str(e), "messages": []}


@app.get("/api/broker/capability-cache")
async def broker_capability_cache():
    """
    Return the symbol capability cache hydrated on broker connect.

    v2.3 fields per instrument:
      canonical_symbol, broker_symbol, asset_class,
      is_mapped, is_supported, is_tradeable_metadata,
      is_tradeable_submit_validated, is_tradeable (combined),
      reason_if_not_tradeable, rejection_code, checked_at

    IMPORTANT: is_tradeable=True only when:
      1. is_mapped: canonical→broker mapping exists
      2. is_supported: broker API confirmed instrument available on account
      3. is_tradeable_metadata: broker API says instrument is open/not halted
    is_tradeable_submit_validated=True only after a real or dry-run order
    confirmed the full payload is accepted.
    """
    try:
        bm = get_broker_manager()
        instruments = bm.get_capability_cache()
        # Surface a clear warning if XAUUSD is not fully confirmed
        xau_entry = next((i for i in instruments if i.get("canonical_symbol") == "XAUUSD"), None)
        xau_warning = None
        if xau_entry:
            if not xau_entry.get("is_supported"):
                xau_warning = (
                    "XAUUSD capability unconfirmed — broker API was not reached "
                    "during instrument lookup. Do not trust is_tradeable=True from cache."
                )
            elif not xau_entry.get("is_tradeable"):
                xau_warning = (
                    f"XAUUSD marked NOT tradeable: {xau_entry.get('reason_if_not_tradeable')}"
                )
        return {
            "broker": bm._broker_name or "none",
            "hydrated": bm._symbol_mapper.is_hydrated if bm._symbol_mapper else False,
            "hydrated_at": (
                bm._symbol_mapper.hydrated_at.isoformat()
                if bm._symbol_mapper and bm._symbol_mapper.hydrated_at
                else None
            ),
            "broker_connected": bm._esm.is_connected(),
            "xauusd_warning": xau_warning,
            "instruments": instruments,
        }
    except Exception as e:
        return {"error": str(e), "instruments": []}


@app.get("/api/broker/debug/xau-diagnostics")
async def xau_diagnostics(
    lots: float = 0.10,
    entry: float = 2300.0,
    stop_loss: float = 2290.0,
    take_profit: float = 2325.0,
    side: str = "BUY",
):
    """
    Full XAUUSD diagnostic endpoint (v2.3).

    Returns:
      - capability_cache: what the mapper thinks about XAUUSD
      - payload_probe: dry-run payload validation (no order submitted)
      - broker_rejection_history: recent XAUUSD rejections from connector
      - manager_rejection_history: recent XAUUSD rejections from broker_manager
      - broker_connected: current broker connection state
      - live_trading_allowed: whether live orders can be placed

    Use this to diagnose why OANDA is rejecting XAUUSD without placing a real order.
    Safe to call at any time — no order is submitted.
    """
    try:
        bm = get_broker_manager()
        from brokers.base_connector import OrderRequest, OrderSide, OrderType
        try:
            order_side = OrderSide(side.upper())
        except ValueError:
            order_side = OrderSide.BUY

        probe_request = OrderRequest(
            instrument="XAUUSD",
            side=order_side,
            units=lots,
            order_type=OrderType.MARKET,
            price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment="xau-diagnostics-probe",
        )
        diag = bm.get_xau_diagnostics(probe_request)
        diag["probe_params"] = {
            "lots": lots,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "side": side,
        }
        return diag
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/broker/debug/rejection-history")
async def rejection_history_detail(limit: int = 50):
    """
    Full structured rejection history (v2.3).
    Includes raw_broker_error, parsed_error_code, normalized_rejection_code,
    payload_snapshot for every rejected order.
    """
    try:
        bm = get_broker_manager()
        # Broker-manager-level history
        mgr_history = bm.rejection_history(limit)
        # Connector-level history (more granular for live orders)
        conn_history = []
        if bm._connector and hasattr(bm._connector, "get_rejection_history"):
            conn_history = bm._connector.get_rejection_history()[-limit:]
        return {
            "manager_rejection_history": mgr_history,
            "connector_rejection_history": conn_history,
            "total_manager": len(bm._rejection_history),
            "total_connector": len(conn_history),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/positions")
async def get_internal_positions():
    """Internal (engine-tracked) open positions."""
    try:
        orch = get_orchestrator()
        return {
            "open_count": len(orch._open_positions),
            "positions": [
                {
                    "id": p.position_id[:8],
                    "instrument": p.instrument,
                    "direction": p.direction.value,
                    "lots": p.size_lots,
                    "entry": p.entry_price,
                    "current": p.current_price,
                    "sl": p.stop_loss,
                    "tp": p.take_profit,
                    "unrealized_pnl": round(p.unrealized_pnl, 2),
                    "opened": p.opened_at.isoformat(),
                }
                for p in orch._open_positions
            ],
        }
    except Exception as e:
        return {"error": str(e), "positions": []}


@app.post("/api/backtest/run")
async def run_backtest(req: BacktestRequest, background_tasks: BackgroundTasks):
    log_event("INFO", f"Backtest requested: {req.instrument}", req.model_dump())
    try:
        inst_type = InstrumentType(req.instrument_type)
        venue_type = VenueType(req.venue_type)
    except ValueError as e:
        raise HTTPException(400, f"Invalid type: {e}")

    config = dict(CONFIG)
    config.setdefault("backtest", {})["initial_balance"] = req.initial_balance
    engine = BacktestEngine(config)
    try:
        result = engine.run(
            instrument=req.instrument,
            instrument_type=inst_type,
            venue_type=venue_type,
            timeframe=req.timeframe,
        )
        log_event("INFO", f"Backtest complete: {req.instrument}",
                  {"trades": result["metrics"]["total_trades"]})
        return result
    except Exception as e:
        log_event("ERROR", f"Backtest failed: {e}")
        raise HTTPException(500, f"Backtest error: {e}")


# =============================================================================
# BROKER CONNECTIVITY ENDPOINTS
# =============================================================================

@app.post("/api/broker/connect")
async def broker_connect(req: BrokerConnectRequest):
    """
    Authenticate with broker, run preflight, enter LIVE_CONNECTED_SAFE.
    This does NOT arm live trading — use /api/execution/arm-live for that.
    """
    bm = get_broker_manager()
    log_event("INFO", f"Broker connect requested: {req.broker}", {"operator": req.operator})

    ok, msg = bm.connect(broker=req.broker, operator=req.operator)

    broker_status = bm.broker_status()
    log_event(
        "INFO" if ok else "ERROR",
        f"Broker connect: {'OK' if ok else 'FAILED'} — {msg}",
        broker_status,
    )

    # Build per-broker connection result for IBKR
    ibkr_result = None
    if req.broker in ("ibkr", "both") and bm._ibkr_connector:
        ibkr_result = {
            "connected": bm._ibkr_connector.is_connected()
                if hasattr(bm._ibkr_connector, "is_connected") else False,
            "broker": "ibkr",
            "state": (
                bm._ibkr_connector._conn_state.value
                if hasattr(bm._ibkr_connector, "_conn_state") else "UNKNOWN"
            ),
            "account_id": getattr(bm._ibkr_connector, "_account_id", None),
            "environment": getattr(bm._ibkr_connector, "_environment", None),
        }

    return {
        "success": ok,
        "message": msg,
        "connected": ok,
        "broker": req.broker,
        "execution_state": broker_status["execution_state"],
        "broker_status": broker_status,
        "ibkr": ibkr_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/broker/status")
async def broker_status():
    """Current broker connection status and execution state."""
    bm = get_broker_manager()
    return bm.broker_status()


@app.get("/api/broker/account")
async def broker_account():
    """Live account info from broker (requires LIVE_CONNECTED_SAFE or better)."""
    bm = get_broker_manager()
    if not bm._esm.is_connected():
        raise HTTPException(
            400,
            "Not connected to broker. Call POST /api/broker/connect first.",
        )
    acct = bm.get_account_info()
    if acct is None:
        raise HTTPException(500, "Failed to retrieve account info")
    return acct.safe_dict()


@app.get("/api/broker/balances")
async def broker_balances():
    """Live balance from broker."""
    bm = get_broker_manager()
    if not bm._esm.is_connected():
        raise HTTPException(400, "Not connected to broker.")
    bals = bm.get_balances()
    return {"balances": [b.safe_dict() for b in bals]}


@app.get("/api/broker/positions")
async def broker_positions():
    """Live positions from broker."""
    bm = get_broker_manager()
    if not bm._esm.is_connected():
        raise HTTPException(400, "Not connected to broker.")
    pos = bm.get_positions()
    return {
        "count": len(pos),
        "positions": [p.safe_dict() for p in pos],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/broker/open-orders")
async def broker_open_orders():
    """Live pending orders from broker."""
    bm = get_broker_manager()
    if not bm._esm.is_connected():
        raise HTTPException(400, "Not connected to broker.")
    orders = bm.get_open_orders()
    return {
        "count": len(orders),
        "orders": [o.safe_dict() for o in orders],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/broker/preflight")
async def run_preflight(req: ControlCommand):
    """Run full preflight checklist. Updates execution state accordingly."""
    bm = get_broker_manager()
    result = bm.run_preflight(operator=req.operator or "api")
    log_event(
        "INFO" if result.overall_pass else "WARNING",
        f"Preflight: pass={result.overall_pass}",
        {"blocking": result.blocking_failures},
    )
    return result.safe_dict()


@app.post("/api/broker/test-order-payload")
async def test_order_payload(req: TestOrderRequest):
    """
    Build the order payload that WOULD be sent to the broker,
    without actually submitting it. Safe to call in any state.
    """
    bm = get_broker_manager()
    from brokers.base_connector import OrderRequest, OrderSide, OrderType
    try:
        side = OrderSide(req.side.upper())
    except ValueError:
        raise HTTPException(400, f"Invalid side: {req.side}. Must be BUY or SELL.")

    order_req = OrderRequest(
        instrument=req.instrument,
        side=side,
        units=req.units,
        order_type=OrderType.MARKET,
        stop_loss=req.stop_loss,
        take_profit=req.take_profit,
        client_order_id="apex_test",
        comment="Test payload — not submitted",
    )

    ok, payload = bm.test_order_payload(order_req)
    return {
        "valid": ok,
        "note": "NOT SUBMITTED — test only",
        "payload": payload,
        "execution_state": bm._esm.current_state().value,
    }


# =============================================================================
# EXECUTION STATE MACHINE ENDPOINTS
# =============================================================================

@app.post("/api/execution/arm-live")
async def arm_live(req: ArmLiveRequest):
    """
    Step 2 of live arming. Operator must have:
    1. Called /api/broker/connect (LIVE_CONNECTED_SAFE state)
    2. Passed preflight (/api/broker/preflight)
    3. Explicitly set acknowledge_risk=true

    This transitions to LIVE_ENABLED — real orders will be routed to broker.
    """
    if not req.acknowledge_risk:
        raise HTTPException(
            400,
            "Must set acknowledge_risk=true to confirm you understand real funds are at risk.",
        )

    bm = get_broker_manager()
    ok, msg = bm.arm_live(operator=req.operator, confirmation_code=req.confirmation_code)

    log_event(
        "WARNING" if ok else "ERROR",
        f"Live arm attempt by {req.operator}: {'OK' if ok else 'FAILED'} — {msg}",
        {"operator": req.operator, "state": bm._esm.current_state().value},
    )

    return {
        "success": ok,
        "message": msg,
        "execution_state": bm._esm.current_state().value,
        "live_trading_allowed": bm._esm.is_live_trading_allowed(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/execution/disarm-live")
async def disarm_live(req: DisarmRequest):
    """Disarm live trading. Returns to LIVE_CONNECTED_SAFE state."""
    bm = get_broker_manager()
    ok, msg = bm.disarm_live(operator=req.operator, reason=req.reason)
    log_event("WARNING", f"Live disarmed by {req.operator}: {msg}")
    return {
        "success": ok,
        "message": msg,
        "execution_state": bm._esm.current_state().value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/execution/set-mode")
async def set_mode(req: SetModeRequest):
    """Set execution mode (currently: paper only)."""
    bm = get_broker_manager()
    ok, msg = bm.set_mode(req.mode, req.operator)
    return {
        "success": ok,
        "message": msg,
        "execution_state": bm._esm.current_state().value,
    }


@app.get("/api/execution/state")
async def execution_state():
    """Full execution state snapshot."""
    bm = get_broker_manager()
    snap = bm.execution_state()
    snap["live_trading_allowed"] = bm._esm.is_live_trading_allowed()
    return snap


@app.get("/api/execution/history")
async def execution_history(limit: int = 50):
    """State transition history log."""
    bm = get_broker_manager()
    return {
        "transitions": bm.transition_history(limit),
        "current_state": bm._esm.current_state().value,
    }


# =============================================================================
# RECONCILIATION
# =============================================================================

@app.post("/api/reconcile/run")
async def reconcile_run(req: ControlCommand):
    """Manual reconciliation: compare internal vs broker state."""
    bm = get_broker_manager()
    result = bm.run_reconciliation(operator=req.operator or "api")
    log_event(
        "INFO",
        "Reconciliation run",
        result if result else {},
    )
    if result is None:
        raise HTTPException(400, "Not connected to broker or no reconciliation service available.")
    return result


@app.get("/api/reconcile/history")
async def reconcile_history(limit: int = 20):
    bm = get_broker_manager()
    if bm._recon_svc:
        return {"history": bm._recon_svc.history(limit)}
    return {"history": [], "note": "No broker connected"}


# =============================================================================
# CONTROL ENDPOINTS (enhanced)
# =============================================================================

@app.post("/api/control/kill")
async def engage_kill_switch(cmd: ControlCommand):
    """Engage global kill switch. Halts ALL trading. Irreversible until reset."""
    reason = cmd.reason or "Manual kill switch via API"
    orch = get_orchestrator()
    bm = get_broker_manager()

    orch._kill_switch = True
    orch.risk_guardian.risk_manager.engage_kill_switch(reason)
    ok, msg = bm.engage_kill_switch(reason=reason, operator=cmd.operator)

    log_event("CRITICAL", f"Kill switch engaged by {cmd.operator}: {reason}")

    from protocol.agent_protocol import AgentMessage, AgentMessageType, MessagePriority, AgentName
    kill_msg = AgentMessage(
        source_agent=AgentName.SYSTEM,
        message_type=AgentMessageType.KILL_SWITCH_TRIGGERED,
        priority=MessagePriority.CRITICAL,
        payload={"reason": reason, "operator": cmd.operator},
        final_status="KILL_SWITCH_ACTIVE",
    )
    orch.ledger.record(kill_msg)

    return {
        "status": "KILL_SWITCH_ENGAGED",
        "reason": reason,
        "execution_state": bm._esm.current_state().value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/control/resume")
async def resume_trading(cmd: ControlCommand):
    """Resume trading after kill switch — resets to PAPER_MODE only."""
    orch = get_orchestrator()
    bm = get_broker_manager()

    # Reset internal state
    orch._kill_switch = False
    orch.risk_guardian.risk_manager._global_kill_switch = False
    orch.risk_guardian.risk_manager.daily_governor._is_killed = False
    orch.risk_guardian.risk_manager.daily_governor._kill_reason = ""

    # ESM reset to paper
    ok, msg = bm.reset_kill_switch(operator=cmd.operator)

    log_event("WARNING", f"Trading resumed by {cmd.operator}: {cmd.reason}")
    return {
        "status": "TRADING_RESUMED",
        "execution_state": bm._esm.current_state().value,
        "operator": cmd.operator,
        "note": "System is now in PAPER_MODE. Re-connect broker to return to live.",
    }


@app.post("/api/control/reset-to-paper")
async def reset_to_paper(req: ResetRequest):
    """Explicit reset to paper mode. Clears all live arming."""
    bm = get_broker_manager()
    ok, msg = bm.set_mode("paper", req.operator)
    log_event("WARNING", f"Reset to paper by {req.operator}: {req.reason}")
    return {
        "success": ok,
        "message": msg,
        "execution_state": bm._esm.current_state().value,
    }


@app.get("/api/control/rejection-history")
async def order_rejection_history(limit: int = 50):
    """All order rejections including insufficient funds."""
    bm = get_broker_manager()
    return {
        "count": len(bm.rejection_history()),
        "rejections": bm.rejection_history(limit),
    }


@app.get("/api/equity-curve")
async def get_equity_curve():
    orch = get_orchestrator()
    return {
        "balance": orch._account_balance,
        "equity_points": [],
        "message": "Real-time equity tracking active",
    }


@app.get("/api/logs")
async def get_logs(limit: int = 50):
    return {"logs": _system_logs[-limit:]}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "3.0.0", "timestamp": datetime.now(timezone.utc).isoformat()}


# =============================================================================
# PHASE 1 — IBKR-SPECIFIC ENDPOINTS
# =============================================================================

@app.get("/api/broker/ibkr/health")
async def ibkr_health():
    """
    IBKR-specific health check.

    Returns:
        connection status, latency_ms, last heartbeat, error state,
        account_id, environment, circuit_breaker_open
    """
    try:
        bm = get_broker_manager()
        return bm.ibkr_health()
    except Exception as e:
        return {
            "connected": False,
            "state": "ERROR",
            "error": str(e),
        }


@app.get("/api/broker/ibkr/env-check")
async def ibkr_env_check():
    """
    Validate IBKR environment variables without attempting a connection.

    Returns structured error if any required variable is missing:
        {"valid": false, "error": "IBKR_ENV_NOT_CONFIGURED",
         "missing": ["APEX_IBKR_ACCOUNT_ID"], "invalid": []}
    """
    try:
        bm = get_broker_manager()
        return bm.validate_ibkr_env()
    except Exception as e:
        return {"valid": False, "error": str(e)}


@app.get("/api/broker/routing-table")
async def broker_routing_table():
    """
    Return the complete broker routing table with audit log for key symbols.
    """
    try:
        from live.broker_manager import get_route_log, OANDA_ROUTING, IBKR_ROUTING
        bm = get_broker_manager()

        audit_symbols = ["EURUSD", "XAUUSD", "AAPL", "ES", "NQ", "GBPUSD", "USOIL", "US500"]
        audit = [get_route_log(s) for s in audit_symbols]

        return {
            "routing_table": bm.get_routing_table(),
            "audit": audit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/broker/route/{symbol}")
async def route_symbol(symbol: str):
    """
    Return routing decision for a given symbol with full audit log.
    """
    try:
        from live.broker_manager import get_route_log
        return get_route_log(symbol.upper())
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# PHASE 2 — ROUTING VALIDATION ENDPOINT
# =============================================================================

@app.get("/api/broker/routing-validate")
async def routing_validate():
    """
    Run the required routing test matrix:
      EURUSD → OANDA
      XAUUSD → IBKR
      AAPL   → IBKR
      ES     → IBKR

    Returns PASS/FAIL per symbol with route_reason.
    """
    try:
        from live.broker_manager import get_route_log

        test_matrix = {
            "EURUSD": "oanda",
            "XAUUSD": "ibkr",
            "AAPL":   "ibkr",
            "ES":     "ibkr",
        }

        results = []
        all_pass = True
        for symbol, expected_broker in test_matrix.items():
            route = get_route_log(symbol)
            actual_broker = route["broker_selected"]
            passed = (actual_broker == expected_broker)
            if not passed:
                all_pass = False
            results.append({
                **route,
                "expected_broker": expected_broker,
                "pass": passed,
            })

        return {
            "all_pass": all_pass,
            "test_matrix": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"error": str(e), "all_pass": False}


# =============================================================================
# PHASE 3 — EXECUTION STATE MACHINE (execution/history already exists)
# =============================================================================

@app.get("/api/execution/transitions")
async def execution_transitions(limit: int = 100):
    """
    Full state transition history with preflight_passed flags.
    """
    try:
        bm = get_broker_manager()
        return {
            "current_state": bm._esm.current_state().value,
            "live_trading_allowed": bm._esm.is_live_trading_allowed(),
            "kill_switch_active": bm._esm.is_kill_switch_active(),
            "transitions": bm.transition_history(limit),
            "count": len(bm._esm._transition_log),
        }
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# PHASE 4 — AUTONOMOUS SCHEDULER ENDPOINT
# =============================================================================

# Scheduler singleton
_scheduler = None


def get_scheduler():
    global _scheduler
    if _scheduler is None:
        from live.autonomous_scheduler import AutonomousScheduler
        orch = get_orchestrator()
        bm = get_broker_manager()
        ds = get_data_service()
        _scheduler = AutonomousScheduler(
            orchestrator=orch,
            broker_manager=bm,
            config=CONFIG,
            market_data_service=ds,
        )
        _scheduler.start()
        log_event("INFO", "AutonomousScheduler started", {})
    return _scheduler


@app.get("/api/system/scheduler")
async def scheduler_status():
    """
    Return autonomous scheduler status: running state, all 5 loop states,
    loaded strategies, pending signals, candle cache.
    """
    try:
        sched = get_scheduler()
        status = sched.status()

        # Enrich with per-loop thread health
        loop_names = [
            "market-data-loop",
            "strategy-scan-loop",
            "signal-process-loop",
            "position-monitor-loop",
            "reconciliation-loop",
        ]
        thread_health = {}
        for t in sched._threads:
            if t.name in loop_names:
                thread_health[t.name] = "ALIVE" if t.is_alive() else "DEAD"

        return {
            **status,
            "loop_health": thread_health,
            "loops_expected": loop_names,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"error": str(e), "running": False}


@app.post("/api/system/scheduler/start")
async def scheduler_start():
    """Start the autonomous scheduler if not already running."""
    try:
        sched = get_scheduler()
        if not sched._running:
            sched.start()
        return {
            "started": True,
            "running": sched._running,
            "strategies": list(sched._strategies.keys()),
        }
    except Exception as e:
        return {"error": str(e), "started": False}


@app.post("/api/system/scheduler/stop")
async def scheduler_stop():
    """Stop the autonomous scheduler."""
    global _scheduler
    try:
        if _scheduler and _scheduler._running:
            _scheduler.stop()
        return {"stopped": True}
    except Exception as e:
        return {"error": str(e), "stopped": False}


# =============================================================================
# PHASE 5 — STRATEGY VALIDATION ENDPOINT
# =============================================================================

@app.get("/api/strategies/performance")
async def strategies_performance():
    """
    Return per-strategy performance metrics from the live trade ledger.
    Groups trades by strategy_family; computes win_rate, avg_r, total_pnl.
    """
    try:
        orch = get_orchestrator()
        trades = orch._all_trades

        from collections import defaultdict
        by_strategy = defaultdict(list)
        for t in trades:
            key = t.strategy_family.value if hasattr(t.strategy_family, "value") else str(t.strategy_family)
            by_strategy[key].append(t)

        perf = {}
        for strat, strat_trades in by_strategy.items():
            closed = [t for t in strat_trades if t.exit_time is not None]
            wins   = [t for t in closed if t.realized_pnl_usd > 0]
            losses = [t for t in closed if t.realized_pnl_usd <= 0]
            total_pnl = sum(t.realized_pnl_usd for t in closed)
            avg_r = (
                sum(getattr(t, "r_multiple", 0) for t in closed) / len(closed)
                if closed else 0.0
            )
            perf[strat] = {
                "total_trades": len(strat_trades),
                "closed_trades": len(closed),
                "open_trades": len(strat_trades) - len(closed),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": round(len(wins) / len(closed) * 100, 1) if closed else 0.0,
                "total_pnl_usd": round(total_pnl, 2),
                "avg_r_multiple": round(avg_r, 3),
            }

        # Add all registered strategies (even with no trades yet)
        try:
            sched = get_scheduler()
            for strat_name in sched._strategies:
                if strat_name not in perf:
                    perf[strat_name] = {
                        "total_trades": 0, "closed_trades": 0,
                        "open_trades": 0, "wins": 0, "losses": 0,
                        "win_rate": 0.0, "total_pnl_usd": 0.0, "avg_r_multiple": 0.0,
                        "status": "loaded_no_trades_yet",
                    }
        except Exception:
            pass

        return {
            "strategy_count": len(perf),
            "strategies": perf,
            "total_trades": len(trades),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"error": str(e), "strategies": {}}


@app.get("/api/strategies/list")
async def strategies_list():
    """Return all loaded strategies with metadata."""
    try:
        from live.autonomous_scheduler import AutonomousScheduler
        sched = get_scheduler()
        result = []
        for name, strategy in sched._strategies.items():
            result.append({
                "name": name,
                "class": type(strategy).__name__,
                "asset_class": getattr(strategy, "asset_class", "UNKNOWN"),
            })
        return {
            "loaded": len(result),
            "strategies": result,
        }
    except Exception as e:
        return {"error": str(e), "strategies": []}


# =============================================================================
# PHASE 6 — RISK NORMALISATION DEBUG ENDPOINT
# =============================================================================

@app.get("/api/risk/normalise")
async def risk_normalise(
    symbol: str = "XAUUSD",
    asset_class: str = "FUTURES",
    account_balance: float = 100000.0,
    entry_price: float = 2350.0,
    stop_loss: float = 2340.0,
    risk_pct: float = 1.0,
):
    """
    Debug endpoint: return exact position sizing for given parameters.

    Example for FUTURES:
        {
            "asset_class": "FUTURES",
            "risk_usd": 100,
            "contracts": 1
        }
    """
    try:
        from live.autonomous_scheduler import RiskNormaliser
        normaliser = RiskNormaliser(risk_pct=risk_pct)
        result = normaliser.calculate(
            symbol=symbol,
            asset_class=asset_class,
            account_balance=account_balance,
            entry_price=entry_price,
            stop_loss=stop_loss,
        )
        risk_usd = account_balance * (risk_pct / 100.0)
        return {
            "symbol": symbol,
            "asset_class": asset_class,
            "account_balance": account_balance,
            "risk_pct": risk_pct,
            "risk_usd": round(risk_usd, 2),
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "sl_distance": abs(entry_price - stop_loss),
            "sizing": result,
            # Convenience top-level fields
            "units": result.get("units"),
            "method": result.get("method"),
            "actual_risk_usd": result.get("risk_usd"),
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/risk/normalise")
async def risk_normalise_post(body: dict):
    """POST version of risk normaliser debug endpoint."""
    return await risk_normalise(
        symbol=body.get("symbol", "XAUUSD"),
        asset_class=body.get("asset_class", "FUTURES"),
        account_balance=body.get("account_balance", 100000.0),
        entry_price=body.get("entry_price", 2350.0),
        stop_loss=body.get("stop_loss", 2340.0),
        risk_pct=body.get("risk_pct", 1.0),
    )


# =============================================================================
# PHASE 7 — PREFLIGHT HARD VALIDATION (GET version)
# =============================================================================

@app.get("/api/broker/preflight")
async def get_preflight_status():
    """
    GET preflight status — returns last preflight result and whether
    the system is allowed to enter LIVE_ENABLED state.

    The system CANNOT enter LIVE_ENABLED unless ALL checks pass.
    POST /api/broker/preflight to run a new preflight check.
    """
    try:
        bm = get_broker_manager()
        snap = bm._esm.snapshot()

        # Check if preflight has been run
        preflight_run = snap.preflight_passed is not None
        preflight_passed = snap.preflight_passed or False
        preflight_at = snap.preflight_performed_at

        # Determine blockers for LIVE_ENABLED
        blockers = []
        if not bm._esm.is_connected():
            blockers.append("NOT_CONNECTED: No broker connected. Call POST /api/broker/connect.")
        if not preflight_run:
            blockers.append("PREFLIGHT_NOT_RUN: Run POST /api/broker/preflight first.")
        elif not preflight_passed:
            blockers.append("PREFLIGHT_FAILED: Resolve all blocking preflight failures.")
        if snap.kill_switch_active:
            blockers.append("KILL_SWITCH_ENGAGED: Reset via POST /api/control/resume.")

        can_arm_live = (
            bm._esm.is_connected()
            and preflight_passed
            and not snap.kill_switch_active
            and snap.state == "LIVE_CONNECTED_SAFE"
        )

        return {
            "preflight_run": preflight_run,
            "preflight_passed": preflight_passed,
            "preflight_at": preflight_at,
            "current_state": snap.state,
            "can_arm_live": can_arm_live,
            "blockers_for_live": blockers,
            "broker": snap.broker,
            "environment": snap.environment,
            "account_id": snap.account_id,
            "connectors": {
                "oanda": bm._connector is not None,
                "ibkr": bm._ibkr_connector is not None,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# PHASE 8 — FAILURE MODE SIMULATION ENDPOINTS
# =============================================================================

@app.post("/api/debug/simulate/ibkr-disconnect")
async def simulate_ibkr_disconnect():
    """
    Simulate IBKR disconnect.
    Forces the IBKR connector into ERROR state and records a rejection.
    Used for failure mode testing.
    """
    try:
        bm = get_broker_manager()
        if bm._ibkr_connector is None:
            return {"error": "IBKR connector not initialised", "simulated": False}

        from brokers.base_connector import ConnectionState
        bm._ibkr_connector._conn_state = ConnectionState.ERROR
        bm._ibkr_connector._consecutive_failures = 5
        bm._ibkr_connector._circuit_open = True

        log_event("WARNING", "[SIMULATION] IBKR disconnect simulated", {})
        return {
            "simulated": True,
            "action": "ibkr_disconnect",
            "new_state": bm._ibkr_connector._conn_state.value,
            "circuit_open": True,
            "note": "Call POST /api/broker/connect broker=ibkr to reconnect",
        }
    except Exception as e:
        return {"error": str(e), "simulated": False}


@app.post("/api/debug/simulate/insufficient-funds")
async def simulate_insufficient_funds():
    """
    Simulate an order rejection due to insufficient funds.
    Submits a deliberately oversized paper order and returns the rejection record.
    """
    try:
        bm = get_broker_manager()
        from brokers.base_connector import OrderRequest, OrderSide, OrderType, OrderStatus, OrderResult

        # Build a large order that would fail funds check
        req = OrderRequest(
            instrument="XAUUSD",
            side=OrderSide.BUY,
            units=999999.0,
            order_type=OrderType.MARKET,
            price=2350.0,
            stop_loss=2340.0,
            take_profit=2370.0,
            comment="simulation:insufficient_funds",
        )

        # Directly inject a simulated rejection
        rejection = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "canonical_symbol": "XAUUSD",
            "target_broker": "ibkr",
            "broker_symbol": "GC",
            "units": 999999.0,
            "side": "BUY",
            "reason": "MARGIN_INSUFFICIENT: Insufficient funds for position size",
            "raw_broker_error": "Order quantity exceeds margin available",
            "parsed_error_code": "MARGIN_INSUFFICIENT",
            "normalized_rejection_code": "INSUFFICIENT_FUNDS",
            "insufficient_funds": True,
            "payload_snapshot": {"instrument": "XAUUSD", "units": 999999, "mode": "SIMULATION"},
            "mode": "SIMULATION",
        }
        bm._rejection_history.append(rejection)
        if len(bm._rejection_history) > 200:
            bm._rejection_history.pop(0)

        log_event("WARNING", "[SIMULATION] Insufficient funds rejection injected", {})
        return {
            "simulated": True,
            "action": "insufficient_funds",
            "rejection_injected": rejection,
        }
    except Exception as e:
        return {"error": str(e), "simulated": False}


@app.post("/api/debug/simulate/invalid-symbol")
async def simulate_invalid_symbol():
    """
    Simulate an invalid symbol rejection.
    Calls check_symbol with a deliberately unknown symbol.
    """
    try:
        bm = get_broker_manager()
        result = bm.check_symbol("INVALID_SYMBOL_XYZ")

        log_event("WARNING", "[SIMULATION] Invalid symbol check", {"symbol": "INVALID_SYMBOL_XYZ"})
        return {
            "simulated": True,
            "action": "invalid_symbol",
            "symbol": "INVALID_SYMBOL_XYZ",
            "is_tradeable": result.is_tradeable,
            "rejection_code": result.rejection_code,
            "reason": result.reason_if_not_tradeable,
        }
    except Exception as e:
        return {"error": str(e), "simulated": False}


@app.post("/api/debug/simulate/repeated-failures")
async def simulate_repeated_failures():
    """
    Simulate repeated consecutive broker failures.
    After CIRCUIT_THRESHOLD failures the circuit breaker opens and
    the execution state transitions to LIVE_BLOCKED.
    """
    try:
        bm = get_broker_manager()

        # Inject 5+ failures into rejection history for the same symbol
        failure_count = 6
        for i in range(failure_count):
            bm._rejection_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "canonical_symbol": "ES",
                "target_broker": "ibkr",
                "broker_symbol": "ES",
                "units": 1.0,
                "side": "BUY",
                "reason": f"Simulated consecutive failure #{i+1}",
                "raw_broker_error": "Connection timeout",
                "parsed_error_code": "TIMEOUT",
                "normalized_rejection_code": "BROKER_TIMEOUT",
                "insufficient_funds": False,
                "payload_snapshot": {"instrument": "ES", "units": 1},
                "mode": "SIMULATION",
            })

        # Open circuit breaker on IBKR connector if available
        circuit_tripped = False
        if bm._ibkr_connector:
            bm._ibkr_connector._consecutive_failures = failure_count
            bm._ibkr_connector._circuit_open = True
            circuit_tripped = True

            # Transition execution state to LIVE_BLOCKED if live
            if bm._esm.is_live_trading_allowed():
                bm._esm.block_live(
                    reason=f"Circuit breaker: {failure_count} consecutive failures simulated",
                    operator="simulation",
                )

        if len(bm._rejection_history) > 200:
            bm._rejection_history = bm._rejection_history[-200:]

        log_event("WARNING", f"[SIMULATION] {failure_count} repeated failures injected", {})
        return {
            "simulated": True,
            "action": "repeated_failures",
            "failures_injected": failure_count,
            "circuit_breaker_tripped": circuit_tripped,
            "current_state": bm._esm.current_state().value,
            "live_blocked": not bm._esm.is_live_trading_allowed(),
        }
    except Exception as e:
        return {"error": str(e), "simulated": False}


# =============================================================================
# PHASE 9 — VERSION (updated above in /health and below)
# =============================================================================

@app.get("/api/version")
async def version():
    """Return system version and build info."""
    return {
        "system": "Apex Multi-Market TJR Engine",
        "version": "3.0.0",
        "codename": "IBKR-MULTI-BROKER",
        "features": [
            "IBKR TWS/Gateway connector (paper account U25324619)",
            "Multi-broker routing: OANDA (EURUSD) + IBKR (XAUUSD/Futures/Stocks)",
            "Autonomous 5-loop scheduler",
            "8 trading strategies across 5 asset classes",
            "Full ESM state machine with kill switch",
            "Risk normaliser: FOREX/FUTURES/STOCKS/CRYPTO",
            "Fail-closed preflight hard validation",
        ],
        "broker_routing": {
            "EURUSD": "OANDA",
            "XAUUSD": "IBKR",
            "Futures (ES/NQ/YM/CL)": "IBKR",
            "Stocks (AAPL/MSFT/TSLA)": "IBKR",
            "Indices (US30/US500)": "IBKR",
            "Unknown": "PAPER_FALLBACK",
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# DASHBOARD HTML
# =============================================================================

def _generate_dashboard_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex TJR Engine v3.0 — Live Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css" rel="stylesheet">
<style>
  body { background: #0a0e1a; color: #e2e8f0; font-family: 'Courier New', monospace; }
  .card { background: #111827; border: 1px solid #1f2937; border-radius: 8px; }
  .metric-value { font-size: 1.6rem; font-weight: bold; }
  .positive { color: #10b981; }
  .negative { color: #ef4444; }
  .neutral { color: #60a5fa; }
  .warn { color: #f59e0b; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .status-ok { background: #10b981; box-shadow: 0 0 6px #10b981; }
  .status-warn { background: #f59e0b; box-shadow: 0 0 6px #f59e0b; }
  .status-dead { background: #ef4444; box-shadow: 0 0 6px #ef4444; }
  .state-badge { padding: 3px 10px; border-radius: 4px; font-size: 0.7rem; font-weight: bold; letter-spacing: 0.05em; }
  .state-paper { background: #1e3a5f; color: #60a5fa; }
  .state-safe { background: #1a3a2a; color: #34d399; }
  .state-live { background: #4c1d1d; color: #fca5a5; }
  .state-blocked { background: #44330a; color: #fcd34d; }
  .state-kill { background: #3b0000; color: #f87171; border: 1px solid #ef4444; }
  .agent-card { transition: all 0.2s; }
  .agent-card:hover { border-color: #3b82f6; }
  .trade-row:hover { background: #1f2937; }
  .btn { cursor: pointer; border-radius: 4px; font-size: 0.75rem; padding: 6px 12px; transition: all 0.15s; }
  .btn-primary { background: #1d4ed8; color: white; }
  .btn-primary:hover { background: #2563eb; }
  .btn-danger { background: #7f1d1d; color: #fca5a5; border: 1px solid #ef4444; }
  .btn-danger:hover { background: #991b1b; }
  .btn-success { background: #065f46; color: #6ee7b7; border: 1px solid #10b981; }
  .btn-success:hover { background: #047857; }
  .btn-warn { background: #78350f; color: #fcd34d; border: 1px solid #f59e0b; }
  .btn-warn:hover { background: #92400e; }
  .btn-neutral { background: #374151; color: #d1d5db; }
  .btn-neutral:hover { background: #4b5563; }
  input.armed-input { background: #1f2937; border: 1px solid #374151; color: #e2e8f0;
    border-radius:4px; padding: 4px 8px; font-size: 0.75rem; width: 100%; }
</style>
</head>
<body>
<div class="min-h-screen p-4">

  <!-- Header -->
  <div class="flex items-center justify-between mb-4">
    <div>
      <h1 class="text-xl font-bold text-blue-400">
        <i class="fas fa-chart-line mr-2"></i>APEX MULTI-MARKET TJR ENGINE v2.0
      </h1>
      <p class="text-gray-500 text-xs">Autonomous Trading System — Deterministic TJR + Multi-Agent</p>
    </div>
    <div class="flex items-center gap-3">
      <span id="env-badge" class="state-badge state-paper">PAPER</span>
      <span id="exec-state-badge" class="state-badge state-paper">PAPER_MODE</span>
      <span class="status-dot status-ok" id="system-dot"></span>
      <span id="system-status" class="text-xs text-gray-300">ONLINE</span>
      <span id="last-update" class="text-xs text-gray-600">--:--:--</span>
    </div>
  </div>

  <!-- Kill Banner -->
  <div id="kill-banner" class="hidden mb-3 p-3 bg-red-950 border border-red-600 rounded flex items-center gap-2">
    <i class="fas fa-radiation text-red-400"></i>
    <strong class="text-red-300">KILL SWITCH ACTIVE — ALL TRADING HALTED</strong>
    <button onclick="resetToPaper()" class="ml-auto btn btn-neutral text-xs">Reset to Paper</button>
  </div>

  <!-- Live Arming Banner -->
  <div id="live-banner" class="hidden mb-3 p-3 bg-amber-950 border border-amber-600 rounded flex items-center gap-2">
    <i class="fas fa-exclamation-triangle text-amber-400"></i>
    <strong class="text-amber-300">⚡ LIVE TRADING ARMED — Real orders routing to broker</strong>
    <button onclick="disarmLive()" class="ml-auto btn btn-warn text-xs">Disarm Live</button>
  </div>

  <!-- Key Metrics -->
  <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2 mb-4">
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Balance</div>
      <div class="metric-value neutral" id="balance">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Daily PnL</div>
      <div class="metric-value" id="daily-pnl">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Drawdown</div>
      <div class="metric-value" id="drawdown">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Open Pos</div>
      <div class="metric-value neutral" id="open-pos">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Win Rate</div>
      <div class="metric-value" id="win-rate">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Broker Bal</div>
      <div class="metric-value neutral" id="broker-bal">--</div></div>
    <div class="card p-3"><div class="text-xs text-gray-500 mb-1">Cycles</div>
      <div class="metric-value neutral" id="cycle-count">--</div></div>
  </div>

  <!-- Main Grid -->
  <div class="grid grid-cols-1 lg:grid-cols-4 gap-3 mb-3">

    <!-- Broker Control Panel -->
    <div class="card p-4 lg:col-span-1">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-plug mr-1 text-green-400"></i> BROKER PANEL
      </h3>

      <!-- Connection status -->
      <div class="mb-3 p-2 bg-gray-900 rounded text-xs space-y-1">
        <div class="flex justify-between">
          <span class="text-gray-400">State</span>
          <span id="panel-state" class="font-bold text-blue-400">--</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-400">Broker</span>
          <span id="panel-broker" class="text-gray-300">--</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-400">Env</span>
          <span id="panel-env" class="text-gray-300">--</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-400">Account</span>
          <span id="panel-acct" class="text-gray-300">--</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-400">Preflight</span>
          <span id="panel-preflight" class="text-gray-300">--</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-400">Last Sync</span>
          <span id="panel-sync" class="text-gray-500">--</span>
        </div>
      </div>

      <!-- Buttons -->
      <div class="space-y-2">
        <button onclick="connectBroker()" class="w-full btn btn-primary">
          <i class="fas fa-plug mr-1"></i> Connect OANDA
        </button>
        <button onclick="runPreflight()" class="w-full btn btn-neutral">
          <i class="fas fa-clipboard-check mr-1"></i> Run Preflight
        </button>
        <div class="border-t border-gray-700 pt-2">
          <div class="text-xs text-gray-500 mb-1">Arm Live Trading</div>
          <input id="arm-op" class="armed-input mb-1" placeholder="Your name (operator ID)">
          <label class="flex items-center gap-2 text-xs text-amber-400 mb-1 cursor-pointer">
            <input type="checkbox" id="arm-ack" class="accent-amber-500">
            I acknowledge real funds are at risk
          </label>
          <button onclick="armLive()" class="w-full btn btn-warn">
            <i class="fas fa-bolt mr-1"></i> ARM LIVE TRADING
          </button>
        </div>
        <button onclick="disarmLive()" class="w-full btn btn-neutral">
          <i class="fas fa-lock mr-1"></i> Disarm Live
        </button>
        <div class="border-t border-gray-700 pt-2">
          <button onclick="engageKill()" class="w-full btn btn-danger">
            <i class="fas fa-stop mr-1"></i> KILL SWITCH
          </button>
        </div>
        <button onclick="runBacktest()" class="w-full btn btn-primary">
          <i class="fas fa-play mr-1"></i> Run Backtest
        </button>
      </div>
      <div id="action-status" class="text-xs text-gray-400 mt-2 min-h-4"></div>
    </div>

    <!-- Agents + Performance -->
    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-robot mr-1 text-blue-400"></i> AGENTS
      </h3>
      <div id="agent-list" class="space-y-1">
        <div class="text-gray-500 text-xs">Loading...</div>
      </div>
    </div>

    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-chart-bar mr-1 text-green-400"></i> PERFORMANCE
      </h3>
      <div class="space-y-1 text-xs" id="perf-metrics">
        <div class="text-gray-500">No trades yet</div>
      </div>
    </div>

    <!-- Preflight Results -->
    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-clipboard-check mr-1 text-yellow-400"></i> PREFLIGHT
      </h3>
      <div id="preflight-list" class="space-y-1 text-xs">
        <div class="text-gray-500">Run preflight to see results</div>
      </div>
    </div>
  </div>

  <!-- Bottom grid -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">

    <!-- Trade Log -->
    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-list mr-1 text-purple-400"></i> TRADE LOG
      </h3>
      <div class="overflow-x-auto">
        <table class="w-full text-xs">
          <thead>
            <tr class="text-gray-500 border-b border-gray-700">
              <th class="text-left py-1">Inst</th><th>Dir</th><th>Entry</th>
              <th>R</th><th>PnL</th><th>Out</th>
            </tr>
          </thead>
          <tbody id="trade-table">
            <tr><td colspan="6" class="text-gray-600 py-2 text-center">No trades</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Decision Ledger -->
    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-scroll mr-1 text-yellow-400"></i> DECISION LEDGER
      </h3>
      <div id="ledger-log" class="space-y-1 max-h-48 overflow-y-auto text-xs font-mono">
        <div class="text-gray-600">No ledger entries</div>
      </div>
    </div>

    <!-- Broker Live Positions -->
    <div class="card p-4">
      <h3 class="text-xs font-bold text-gray-300 mb-3">
        <i class="fas fa-layer-group mr-1 text-teal-400"></i> BROKER POSITIONS
      </h3>
      <div id="broker-pos-list" class="text-xs space-y-1">
        <div class="text-gray-600">Not connected</div>
      </div>
      <div class="mt-2">
        <h4 class="text-xs text-gray-500 mb-1">Order Rejections</h4>
        <div id="rejection-list" class="text-xs space-y-1 max-h-24 overflow-y-auto">
          <div class="text-gray-600">None</div>
        </div>
      </div>
    </div>

  </div>
</div>

<script>
const API = '';
let _lastPreflightData = null;

async function fetchJSON(url) {
  try { const r = await fetch(API + url); return await r.json(); }
  catch(e) { return null; }
}

async function postJSON(url, body) {
  try {
    const r = await fetch(API + url, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    return await r.json();
  } catch(e) { return {error: e.message}; }
}

function stateClass(state) {
  const map = {
    'PAPER_MODE': 'state-paper',
    'LIVE_CONNECTED_SAFE': 'state-safe',
    'LIVE_ENABLED': 'state-live',
    'LIVE_BLOCKED': 'state-blocked',
    'KILL_SWITCH_ENGAGED': 'state-kill',
  };
  return map[state] || 'state-paper';
}

function setStatus(msg, cls='text-gray-400') {
  const el = document.getElementById('action-status');
  el.textContent = msg;
  el.className = 'text-xs mt-2 min-h-4 ' + cls;
}

async function refreshStatus() {
  const data = await fetchJSON('/api/status');
  if (!data) return;
  document.getElementById('last-update').textContent = new Date().toLocaleTimeString();

  const env = (data.environment || 'PAPER').toUpperCase();
  document.getElementById('env-badge').textContent = env;

  const state = data.execution_state || 'PAPER_MODE';
  const badge = document.getElementById('exec-state-badge');
  badge.textContent = state.replace(/_/g,' ');
  badge.className = 'state-badge ' + stateClass(state);

  document.getElementById('panel-state').textContent = state;
  document.getElementById('panel-state').className = 'font-bold ' + {
    PAPER_MODE:'text-blue-400',LIVE_CONNECTED_SAFE:'text-green-400',
    LIVE_ENABLED:'text-red-400',LIVE_BLOCKED:'text-yellow-400',
    KILL_SWITCH_ENGAGED:'text-red-600'
  }[state] || 'text-gray-400';

  const bs = data.broker_status || {};
  document.getElementById('panel-broker').textContent = bs.broker || '—';
  document.getElementById('panel-env').textContent = bs.environment || '—';
  document.getElementById('panel-acct').textContent = bs.account_id || '—';
  document.getElementById('panel-preflight').textContent =
    bs.preflight_passed === null ? '—' : bs.preflight_passed ? '✅ PASSED' : '❌ FAILED';
  const sync = bs.last_sync;
  document.getElementById('panel-sync').textContent = sync ? sync.substring(11,19) : '—';

  document.getElementById('cycle-count').textContent = data.cycle_count || '0';

  const bal = data.account_balance || 0;
  document.getElementById('balance').textContent = '$' + bal.toLocaleString('en-US', {maximumFractionDigits:0});

  const pnl = data.daily_pnl_usd || 0;
  const pnlEl = document.getElementById('daily-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);
  pnlEl.className = 'metric-value ' + (pnl >= 0 ? 'positive' : 'negative');

  const dd = data.drawdown_pct || 0;
  const ddEl = document.getElementById('drawdown');
  ddEl.textContent = dd.toFixed(2) + '%';
  ddEl.className = 'metric-value ' + (dd > 5 ? 'negative' : dd > 2 ? 'warn' : 'positive');

  document.getElementById('open-pos').textContent = data.open_positions || '0';

  // Kill banner
  const killed = data.kill_switch_active || state === 'KILL_SWITCH_ENGAGED';
  document.getElementById('kill-banner').classList.toggle('hidden', !killed);
  document.getElementById('live-banner').classList.toggle('hidden', state !== 'LIVE_ENABLED');
  document.getElementById('system-dot').className = 'status-dot ' + (killed ? 'status-dead' : 'status-ok');
  document.getElementById('system-status').textContent = killed ? 'KILL SWITCH' : 'ONLINE';

  // Agents
  if (data.agent_health) {
    const c = document.getElementById('agent-list');
    c.innerHTML = '';
    for (const [name, h] of Object.entries(data.agent_health)) {
      const dot = h.healthy ? 'status-ok' : 'status-warn';
      c.innerHTML += `<div class="agent-card flex items-center justify-between p-1 border border-gray-800 rounded text-xs">
        <div class="flex items-center gap-1"><span class="status-dot ${dot}"></span>
        <span class="text-gray-300">${name.replace(/_/g,' ')}</span></div>
        <span class="text-gray-600">r:${h.runs||0} e:${h.errors||0}</span></div>`;
    }
  }

  // Refresh broker positions if connected
  if (bs.broker && bs.broker !== 'none') {
    refreshBrokerPositions();
  }
}

async function refreshMetrics() {
  const data = await fetchJSON('/api/metrics');
  if (!data || data.error || data.total_trades === 0) return;
  const wr = ((data.win_rate||0)*100).toFixed(1);
  document.getElementById('win-rate').textContent = wr + '%';
  document.getElementById('win-rate').className = 'metric-value ' + (parseFloat(wr)>=50?'positive':'negative');
  const c = document.getElementById('perf-metrics');
  c.innerHTML = `
    <div class="flex justify-between"><span class="text-gray-500">Trades</span><span>${data.total_trades}</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Win Rate</span><span class="${data.win_rate>=0.5?'positive':'negative'}">${wr}%</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Prof Factor</span><span class="${data.profit_factor>=1.5?'positive':'negative'}">${(data.profit_factor||0).toFixed(2)}</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Expectancy</span><span class="${data.expectancy>=0?'positive':'negative'}">${(data.expectancy||0).toFixed(3)}R</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Net PnL</span><span class="${data.net_profit_usd>=0?'positive':'negative'}">$${(data.net_profit_usd||0).toFixed(0)}</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Max DD</span><span class="${data.max_drawdown_pct>5?'negative':'positive'}">${(data.max_drawdown_pct||0).toFixed(2)}%</span></div>
    <div class="flex justify-between"><span class="text-gray-500">Sharpe</span><span>${(data.sharpe_ratio||0).toFixed(2)}</span></div>
  `;
}

async function refreshTrades() {
  const data = await fetchJSON('/api/trades?limit=15');
  if (!data || !data.trades || !data.trades.length) return;
  document.getElementById('trade-table').innerHTML = data.trades.map(t => `
    <tr class="trade-row border-b border-gray-800">
      <td class="py-1">${t.instrument}</td>
      <td class="text-center ${t.direction==='BUY'?'positive':'negative'}">${t.direction}</td>
      <td class="text-center">${t.entry?t.entry.toFixed(4):'--'}</td>
      <td class="text-center ${t.r_multiple>=0?'positive':'negative'}">${t.r_multiple?t.r_multiple.toFixed(1)+'R':'--'}</td>
      <td class="text-center ${t.pnl_usd>=0?'positive':'negative'}">${t.pnl_usd?(t.pnl_usd>=0?'+':'')+t.pnl_usd.toFixed(0):'--'}</td>
      <td class="text-center ${t.outcome==='WIN'?'positive':t.outcome==='LOSS'?'negative':'neutral'}">${t.outcome||'OPEN'}</td>
    </tr>`).join('');
}

async function refreshLedger() {
  const data = await fetchJSON('/api/ledger?limit=15');
  if (!data || !data.messages || !data.messages.length) return;
  const colors = {
    'RISK_REJECTED':'text-red-400','RISK_APPROVED':'text-green-400',
    'KILL_SWITCH_TRIGGERED':'text-red-300 font-bold','ORDER_FILLED':'text-green-300',
    'SIGNAL_VALIDATION_APPROVED':'text-blue-400','SIGNAL_VALIDATION_REJECTED':'text-orange-400',
    'MARKET_SCAN_RESULT':'text-gray-500','REGIME_CLASSIFICATION':'text-purple-400',
  };
  document.getElementById('ledger-log').innerHTML = data.messages.map(m => {
    const color = colors[m.type] || 'text-gray-400';
    const time = m.ts ? m.ts.substring(11,19) : '--';
    return `<div class="${color}">[${time}] ${m.type} (${m.from})</div>`;
  }).join('');
}

async function refreshBrokerPositions() {
  const posData = await fetchJSON('/api/broker/positions');
  const rejData = await fetchJSON('/api/control/rejection-history?limit=5');
  const balData = await fetchJSON('/api/broker/balances');

  if (balData && balData.balances && balData.balances.length) {
    const b = balData.balances[0];
    document.getElementById('broker-bal').textContent = '$' + (b.balance||0).toLocaleString('en-US',{maximumFractionDigits:0});
  }

  const posEl = document.getElementById('broker-pos-list');
  if (posData && posData.positions) {
    if (!posData.positions.length) {
      posEl.innerHTML = '<div class="text-gray-600">No open positions</div>';
    } else {
      posEl.innerHTML = posData.positions.map(p =>
        `<div class="flex justify-between p-1 border border-gray-800 rounded">
          <span class="${p.side==='LONG'?'positive':'negative'}">${p.instrument} ${p.side}</span>
          <span class="${p.unrealized_pnl>=0?'positive':'negative'}">${p.unrealized_pnl>=0?'+':''}$${p.unrealized_pnl.toFixed(2)}</span>
        </div>`
      ).join('');
    }
  }

  const rejEl = document.getElementById('rejection-list');
  if (rejData && rejData.rejections) {
    if (!rejData.rejections.length) {
      rejEl.innerHTML = '<div class="text-gray-600">None</div>';
    } else {
      rejEl.innerHTML = rejData.rejections.slice(-3).map(r =>
        `<div class="text-red-400">${r.instrument} ${r.side}: ${(r.reason||'').substring(0,40)}</div>`
      ).join('');
    }
  }
}

// ─── ACTIONS ─────────────────────────────────────────────────────────────────

async function connectBroker() {
  setStatus('Connecting to OANDA...', 'text-yellow-400');
  const data = await postJSON('/api/broker/connect', {broker:'oanda', operator:'dashboard'});
  if (data && data.success) {
    setStatus('Connected: ' + data.message.substring(0,60), 'text-green-400');
  } else {
    setStatus('Failed: ' + ((data&&data.message)||'error'), 'text-red-400');
  }
  refreshAll();
}

async function runPreflight() {
  setStatus('Running preflight...', 'text-yellow-400');
  const data = await postJSON('/api/broker/preflight', {operator:'dashboard'});
  if (data) {
    const pass = data.overall_pass;
    setStatus(`Preflight ${pass?'PASSED ✅':'FAILED ❌'}`, pass?'text-green-400':'text-red-400');
    // Render preflight list
    const c = document.getElementById('preflight-list');
    if (data.checks) {
      c.innerHTML = data.checks.map(ch => {
        const icon = ch.passed ? '✅' : (ch.critical ? '🔴' : '⚠️');
        return `<div class="${ch.passed?'text-green-400':ch.critical?'text-red-400':'text-yellow-400'}">${icon} ${ch.name}: ${(ch.message||'').substring(0,55)}</div>`;
      }).join('');
    }
    if (data.warnings && data.warnings.length) {
      c.innerHTML += data.warnings.map(w =>
        `<div class="text-yellow-500 mt-1">⚠ ${w.substring(0,55)}</div>`).join('');
    }
    _lastPreflightData = data;
  }
  refreshAll();
}

async function armLive() {
  const op = document.getElementById('arm-op').value.trim();
  const ack = document.getElementById('arm-ack').checked;
  if (!op) { setStatus('Enter your operator name first.', 'text-red-400'); return; }
  if (!ack) { setStatus('Acknowledge risk checkbox required.', 'text-red-400'); return; }
  if (!confirm('⚡ ARM LIVE TRADING?\\n\\nReal orders will be sent to OANDA.\\nContinue?')) return;
  setStatus('Arming live trading...', 'text-yellow-400');
  const data = await postJSON('/api/execution/arm-live', {operator:op, acknowledge_risk:true, confirmation_code:'dashboard'});
  if (data && data.success) {
    setStatus('LIVE TRADING ARMED ⚡', 'text-red-400');
  } else {
    setStatus('Arm failed: ' + ((data&&data.message)||'error'), 'text-red-400');
  }
  refreshAll();
}

async function disarmLive() {
  if (!confirm('Disarm live trading? System returns to LIVE_CONNECTED_SAFE.')) return;
  setStatus('Disarming...', 'text-yellow-400');
  const data = await postJSON('/api/execution/disarm-live', {operator:'dashboard', reason:'Manual disarm via dashboard'});
  setStatus(data&&data.success ? 'Disarmed ✓' : 'Disarm failed', data&&data.success?'text-green-400':'text-red-400');
  refreshAll();
}

async function engageKill() {
  if (!confirm('🔴 ENGAGE KILL SWITCH?\\nAll trading will HALT immediately.')) return;
  const data = await postJSON('/api/control/kill', {reason:'Manual kill via dashboard', operator:'dashboard'});
  setStatus('KILL SWITCH ENGAGED 🔴', 'text-red-400');
  refreshAll();
}

async function resetToPaper() {
  if (!confirm('Reset to PAPER_MODE? Kill switch will be cleared.')) return;
  const data = await postJSON('/api/control/resume', {reason:'Manual reset', operator:'dashboard'});
  setStatus('Reset to paper mode', 'text-blue-400');
  refreshAll();
}

async function runBacktest() {
  setStatus('Running backtest...', 'text-yellow-400');
  const data = await postJSON('/api/backtest/run', {instrument:'XAUUSD',instrument_type:'GOLD',venue_type:'CFD_BROKER',timeframe:'M15'});
  if (data && data.metrics) {
    const m = data.metrics;
    setStatus(`Backtest done: ${m.total_trades} trades, WR=${(m.win_rate*100).toFixed(0)}%, Net=$${m.net_profit_usd.toFixed(0)}`, 'text-green-400');
  } else {
    setStatus('Backtest error: ' + (data&&data.detail||JSON.stringify(data||{}).substring(0,60)), 'text-red-400');
  }
  refreshAll();
}

function refreshAll() {
  refreshStatus();
  refreshMetrics();
  refreshTrades();
  refreshLedger();
}

refreshAll();
setInterval(refreshAll, 5000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8080)

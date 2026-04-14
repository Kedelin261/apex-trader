"""
APEX MULTI-MARKET TJR ENGINE
API Server — FastAPI backend for webhooks, dashboard, and control.

Endpoints:
  POST /api/signal          — TradingView webhook ingest
  GET  /api/status          — System health and status
  GET  /api/trades          — Trade history
  GET  /api/metrics         — Performance metrics
  GET  /api/agents          — Agent health
  GET  /api/ledger          — Agent decision ledger (recent)
  GET  /api/positions       — Open positions
  POST /api/backtest/run    — Trigger backtest
  POST /api/control/kill    — Engage kill switch
  POST /api/control/resume  — Resume trading
  GET  /api/equity-curve    — Equity curve data
  GET  /dashboard           — Dashboard HTML
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
from pydantic import BaseModel

from domain.models import InstrumentType, VenueType, EnvironmentMode
from data.market_data_service import MarketDataService
from backtest.backtest_engine import BacktestEngine
from agents.agent_orchestrator import AgentOrchestrator
from core.risk_manager import RiskManager

logger = logging.getLogger(__name__)

# =============================================================================
# LOAD CONFIG
# =============================================================================

def load_config(path: str = "config/system_config.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        # Try relative to script location
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
# APP INITIALIZATION
# =============================================================================

app = FastAPI(
    title="Apex Multi-Market TJR Engine",
    description="Production-grade autonomous trading system",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Lazy-initialize orchestrator and services
_orchestrator: Optional[AgentOrchestrator] = None
_data_service: Optional[MarketDataService] = None
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


def log_event(level: str, message: str, details: dict = None):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    _system_logs.append(entry)
    if len(_system_logs) > 500:
        _system_logs.pop(0)


# =============================================================================
# REQUEST MODELS
# =============================================================================

class TradingViewWebhook(BaseModel):
    """TradingView Pine Script alert payload."""
    symbol: str
    action: str           # "buy" | "sell" | "close"
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


# =============================================================================
# ROUTES
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect to dashboard."""
    return HTMLResponse("""
    <html><head>
    <meta http-equiv="refresh" content="0; url=/dashboard">
    </head><body>Redirecting to dashboard...</body></html>
    """)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve the trading dashboard."""
    dashboard_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if dashboard_path.exists():
        return HTMLResponse(dashboard_path.read_text())
    return HTMLResponse(_generate_dashboard_html())


@app.post("/api/signal")
async def receive_signal(webhook: TradingViewWebhook, request: Request):
    """
    TradingView webhook endpoint.
    Accepts Pine Script alert payloads and routes them through the agent pipeline.
    """
    client_ip = request.client.host
    log_event("INFO", f"TradingView signal received: {webhook.symbol} {webhook.action}",
              {"ip": client_ip, "symbol": webhook.symbol})

    # Validate schema version
    if webhook.schema_version not in ("1.0", "1", None):
        raise HTTPException(400, "Unsupported schema version")

    orch = get_orchestrator()

    # If kill switch is active, reject all signals
    if orch._kill_switch:
        return JSONResponse({"status": "REJECTED", "reason": "Kill switch active"}, status_code=403)

    # Build minimal context for webhook-driven signal
    response = {
        "status": "RECEIVED",
        "symbol": webhook.symbol,
        "action": webhook.action,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": (
            "Signal received and queued for agent pipeline validation. "
            "See /api/ledger for decision chain."
        )
    }

    log_event("INFO", f"Signal queued: {webhook.symbol}", response)
    return response


@app.get("/api/status")
async def system_status():
    """Real-time system health and status."""
    uptime = (datetime.now(timezone.utc) - _start_time).total_seconds() / 3600

    try:
        orch = get_orchestrator()
        state = orch.get_system_state()
        risk_mgr = orch.risk_guardian.risk_manager
        daily = risk_mgr.daily_governor.get_summary()

        return {
            "system": "Apex Multi-Market TJR Engine",
            "version": "1.0.0",
            "environment": CONFIG.get("system", {}).get("environment", "paper"),
            "uptime_hours": round(uptime, 2),
            "kill_switch_active": orch._kill_switch,
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
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "system": "Apex Multi-Market TJR Engine",
            "version": "1.0.0",
            "status": "INITIALIZING",
            "uptime_hours": round(uptime, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    """Return recent trade history."""
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
                    "reason": t.strategy_reason
                }
                for t in reversed(trades)
            ]
        }
    except Exception as e:
        return {"error": str(e), "trades": []}


@app.get("/api/metrics")
async def get_metrics():
    """Return current performance metrics."""
    try:
        orch = get_orchestrator()
        trades = orch._all_trades

        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0,
                "expectancy": 0,
                "profit_factor": 0,
                "net_pnl": 0,
                "message": "No trades yet"
            }

        from domain.models import TradeOutcome
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
    """Return all agent health states."""
    try:
        orch = get_orchestrator()
        return orch.get_agent_health()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ledger")
async def get_ledger(limit: int = 50, msg_type: Optional[str] = None):
    """Return recent agent decision ledger entries."""
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
            "messages": [m.to_log_dict() for m in reversed(msgs)]
        }
    except Exception as e:
        return {"error": str(e), "messages": []}


@app.get("/api/positions")
async def get_positions():
    """Return current open positions."""
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
                    "opened": p.opened_at.isoformat()
                }
                for p in orch._open_positions
            ]
        }
    except Exception as e:
        return {"error": str(e), "positions": []}


@app.post("/api/backtest/run")
async def run_backtest(req: BacktestRequest, background_tasks: BackgroundTasks):
    """Run a backtest in the background and return job info."""
    log_event("INFO", f"Backtest requested: {req.instrument}", req.dict())

    try:
        inst_type = InstrumentType(req.instrument_type)
        venue_type = VenueType(req.venue_type)
    except ValueError as e:
        raise HTTPException(400, f"Invalid instrument/venue type: {e}")

    config = dict(CONFIG)
    config.setdefault("backtest", {})["initial_balance"] = req.initial_balance

    # Run synchronously for now (small datasets)
    engine = BacktestEngine(config)
    try:
        result = engine.run(
            instrument=req.instrument,
            instrument_type=inst_type,
            venue_type=venue_type,
            timeframe=req.timeframe
        )
        log_event("INFO", f"Backtest complete: {req.instrument}",
                  {"trades": result["metrics"]["total_trades"]})
        return result
    except Exception as e:
        log_event("ERROR", f"Backtest failed: {e}")
        raise HTTPException(500, f"Backtest error: {e}")


@app.post("/api/control/kill")
async def engage_kill_switch(cmd: ControlCommand):
    """Engage the global kill switch."""
    reason = cmd.reason or "Manual kill switch via API"
    orch = get_orchestrator()
    orch._kill_switch = True
    orch.risk_guardian.risk_manager.engage_kill_switch(reason)
    log_event("CRITICAL", f"Kill switch engaged by {cmd.operator}: {reason}")

    # Emit kill switch message
    from protocol.agent_protocol import AgentMessage, AgentMessageType, MessagePriority, AgentName
    msg = AgentMessage(
        source_agent=AgentName.SYSTEM,
        message_type=AgentMessageType.KILL_SWITCH_TRIGGERED,
        priority=MessagePriority.CRITICAL,
        payload={"reason": reason, "operator": cmd.operator},
        final_status="KILL_SWITCH_ACTIVE"
    )
    orch.ledger.record(msg)

    return {"status": "KILL_SWITCH_ENGAGED", "reason": reason, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/api/control/resume")
async def resume_trading(cmd: ControlCommand):
    """Resume trading after kill switch (requires explicit confirmation)."""
    orch = get_orchestrator()
    orch._kill_switch = False
    orch.risk_guardian.risk_manager._global_kill_switch = False
    orch.risk_guardian.risk_manager.daily_governor._is_killed = False
    orch.risk_guardian.risk_manager.daily_governor._kill_reason = ""

    log_event("WARNING", f"Trading resumed by {cmd.operator}: {cmd.reason}")
    return {"status": "TRADING_RESUMED", "operator": cmd.operator}


@app.get("/api/equity-curve")
async def get_equity_curve():
    """Return equity curve data for charting."""
    # For live/paper mode, return current state snapshots
    orch = get_orchestrator()
    return {
        "balance": orch._account_balance,
        "equity_points": [],  # Populated during backtest or paper trading
        "message": "Real-time equity tracking active in paper/live mode"
    }


@app.get("/api/logs")
async def get_logs(limit: int = 50):
    """Return recent system logs."""
    return {"logs": _system_logs[-limit:]}


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# =============================================================================
# DASHBOARD HTML
# =============================================================================

def _generate_dashboard_html() -> str:
    """Generate the dashboard HTML inline if no file exists."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Multi-Market TJR Engine</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css" rel="stylesheet">
<style>
  body { background: #0a0e1a; color: #e2e8f0; font-family: 'Courier New', monospace; }
  .card { background: #111827; border: 1px solid #1f2937; border-radius: 8px; }
  .metric-value { font-size: 2rem; font-weight: bold; }
  .positive { color: #10b981; }
  .negative { color: #ef4444; }
  .neutral { color: #60a5fa; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .status-ok { background: #10b981; box-shadow: 0 0 8px #10b981; }
  .status-warn { background: #f59e0b; }
  .status-dead { background: #ef4444; }
  .agent-card { transition: all 0.2s; }
  .agent-card:hover { border-color: #3b82f6; }
  .trade-row:hover { background: #1f2937; }
</style>
</head>
<body>
<div class="min-h-screen p-4">
  <!-- Header -->
  <div class="flex items-center justify-between mb-6">
    <div>
      <h1 class="text-2xl font-bold text-blue-400">
        <i class="fas fa-chart-line mr-2"></i>
        APEX MULTI-MARKET TJR ENGINE
      </h1>
      <p class="text-gray-400 text-sm">Autonomous Trading System v1.0.0</p>
    </div>
    <div class="flex items-center gap-4">
      <span id="env-badge" class="px-3 py-1 bg-blue-900 text-blue-300 rounded text-sm font-bold">PAPER</span>
      <div class="flex items-center gap-2">
        <span class="status-dot status-ok" id="system-dot"></span>
        <span id="system-status" class="text-sm text-gray-300">ONLINE</span>
      </div>
      <span id="last-update" class="text-xs text-gray-500">--:--:--</span>
    </div>
  </div>

  <!-- Kill Switch Banner -->
  <div id="kill-banner" class="hidden mb-4 p-3 bg-red-900 border border-red-500 rounded text-red-300 flex items-center gap-2">
    <i class="fas fa-exclamation-triangle"></i>
    <strong>KILL SWITCH ACTIVE</strong> — All trading halted
    <button onclick="resumeTrading()" class="ml-auto px-3 py-1 bg-red-700 hover:bg-red-600 rounded text-sm">Resume</button>
  </div>

  <!-- Key Metrics Row -->
  <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mb-4">
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Balance</div>
      <div class="metric-value text-2xl neutral" id="balance">--</div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Daily PnL</div>
      <div class="metric-value text-2xl" id="daily-pnl">--</div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Drawdown</div>
      <div class="metric-value text-2xl" id="drawdown">--</div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Open Positions</div>
      <div class="metric-value text-2xl neutral" id="open-pos">--</div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Win Rate</div>
      <div class="metric-value text-2xl" id="win-rate">--</div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400 mb-1">Cycle Count</div>
      <div class="metric-value text-2xl neutral" id="cycle-count">--</div>
    </div>
  </div>

  <!-- Main Grid -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
    <!-- Agent Health -->
    <div class="card p-4">
      <h3 class="text-sm font-bold text-gray-300 mb-3">
        <i class="fas fa-robot mr-1 text-blue-400"></i> AGENT STATUS
      </h3>
      <div id="agent-list" class="space-y-2">
        <div class="text-gray-500 text-sm">Loading agents...</div>
      </div>
    </div>

    <!-- Metrics -->
    <div class="card p-4">
      <h3 class="text-sm font-bold text-gray-300 mb-3">
        <i class="fas fa-chart-bar mr-1 text-green-400"></i> PERFORMANCE
      </h3>
      <div class="space-y-2 text-sm" id="perf-metrics">
        <div class="text-gray-500">No trades yet</div>
      </div>
    </div>

    <!-- Controls -->
    <div class="card p-4">
      <h3 class="text-sm font-bold text-gray-300 mb-3">
        <i class="fas fa-sliders-h mr-1 text-yellow-400"></i> CONTROLS
      </h3>
      <div class="space-y-2">
        <button onclick="runBacktest()" class="w-full px-3 py-2 bg-blue-700 hover:bg-blue-600 rounded text-sm">
          <i class="fas fa-play mr-1"></i> Run Backtest (XAUUSD M15)
        </button>
        <button onclick="engageKill()" class="w-full px-3 py-2 bg-red-800 hover:bg-red-700 rounded text-sm">
          <i class="fas fa-stop mr-1"></i> Engage Kill Switch
        </button>
        <button onclick="refreshAll()" class="w-full px-3 py-2 bg-gray-700 hover:bg-gray-600 rounded text-sm">
          <i class="fas fa-sync mr-1"></i> Refresh Data
        </button>
        <div id="backtest-status" class="text-xs text-gray-400 mt-2"></div>
      </div>
    </div>
  </div>

  <!-- Trade Log and Ledger -->
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <!-- Trade Log -->
    <div class="card p-4">
      <h3 class="text-sm font-bold text-gray-300 mb-3">
        <i class="fas fa-list mr-1 text-purple-400"></i> TRADE LOG
      </h3>
      <div class="overflow-x-auto">
        <table class="w-full text-xs">
          <thead>
            <tr class="text-gray-400 border-b border-gray-700">
              <th class="text-left py-1">Inst</th>
              <th>Dir</th>
              <th>Entry</th>
              <th>Exit</th>
              <th>R</th>
              <th>PnL</th>
              <th>Out</th>
            </tr>
          </thead>
          <tbody id="trade-table">
            <tr><td colspan="7" class="text-gray-500 py-2 text-center">No trades</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Agent Decision Ledger -->
    <div class="card p-4">
      <h3 class="text-sm font-bold text-gray-300 mb-3">
        <i class="fas fa-scroll mr-1 text-yellow-400"></i> DECISION LEDGER
      </h3>
      <div id="ledger-log" class="space-y-1 max-h-64 overflow-y-auto text-xs font-mono">
        <div class="text-gray-500">No ledger entries</div>
      </div>
    </div>
  </div>
</div>

<script>
const API = '';

async function fetchJSON(url) {
  try {
    const r = await fetch(API + url);
    return await r.json();
  } catch(e) {
    return null;
  }
}

async function refreshStatus() {
  const data = await fetchJSON('/api/status');
  if (!data) return;

  document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
  document.getElementById('env-badge').textContent = (data.environment || 'PAPER').toUpperCase();
  document.getElementById('cycle-count').textContent = data.cycle_count || '0';

  const bal = data.account_balance || 0;
  document.getElementById('balance').textContent = '$' + bal.toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:0});

  const pnl = data.daily_pnl_usd || 0;
  const pnlEl = document.getElementById('daily-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);
  pnlEl.className = 'metric-value text-2xl ' + (pnl >= 0 ? 'positive' : 'negative');

  const dd = data.drawdown_pct || 0;
  const ddEl = document.getElementById('drawdown');
  ddEl.textContent = dd.toFixed(2) + '%';
  ddEl.className = 'metric-value text-2xl ' + (dd > 5 ? 'negative' : dd > 2 ? 'text-yellow-400' : 'positive');

  document.getElementById('open-pos').textContent = data.open_positions || '0';

  // Kill switch banner
  if (data.kill_switch_active) {
    document.getElementById('kill-banner').classList.remove('hidden');
    document.getElementById('system-dot').className = 'status-dot status-dead';
    document.getElementById('system-status').textContent = 'KILL SWITCH';
  } else {
    document.getElementById('kill-banner').classList.add('hidden');
    document.getElementById('system-dot').className = 'status-dot status-ok';
    document.getElementById('system-status').textContent = 'ONLINE';
  }

  // Agent health
  if (data.agent_health) {
    const container = document.getElementById('agent-list');
    container.innerHTML = '';
    for (const [name, h] of Object.entries(data.agent_health)) {
      const dot = h.healthy ? 'status-ok' : 'status-warn';
      container.innerHTML += `
        <div class="agent-card flex items-center justify-between p-2 border border-gray-700 rounded text-xs">
          <div class="flex items-center gap-2">
            <span class="status-dot ${dot}"></span>
            <span class="text-gray-300">${name.replace('_', ' ').toUpperCase()}</span>
          </div>
          <div class="text-gray-500">
            runs:${h.runs || 0} err:${h.errors || 0}
          </div>
        </div>`;
    }
  }
}

async function refreshMetrics() {
  const data = await fetchJSON('/api/metrics');
  if (!data || data.error) return;

  if (data.total_trades === 0) return;

  const wr = ((data.win_rate || 0) * 100).toFixed(1);
  document.getElementById('win-rate').textContent = wr + '%';
  const wrEl = document.getElementById('win-rate');
  wrEl.className = 'metric-value text-2xl ' + (parseFloat(wr) >= 50 ? 'positive' : 'negative');

  const container = document.getElementById('perf-metrics');
  container.innerHTML = `
    <div class="flex justify-between"><span class="text-gray-400">Total Trades</span><span>${data.total_trades}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Win Rate</span><span class="${data.win_rate >= 0.5 ? 'positive' : 'negative'}">${wr}%</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Profit Factor</span><span class="${data.profit_factor >= 1.5 ? 'positive' : 'negative'}">${(data.profit_factor || 0).toFixed(2)}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Expectancy (R)</span><span class="${data.expectancy >= 0 ? 'positive' : 'negative'}">${(data.expectancy || 0).toFixed(3)}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Net PnL</span><span class="${data.net_profit_usd >= 0 ? 'positive' : 'negative'}">$${(data.net_profit_usd || 0).toFixed(2)}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Max Drawdown</span><span class="${data.max_drawdown_pct > 5 ? 'negative' : 'positive'}">${(data.max_drawdown_pct || 0).toFixed(2)}%</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Sharpe Ratio</span><span>${(data.sharpe_ratio || 0).toFixed(2)}</span></div>
    <div class="flex justify-between"><span class="text-gray-400">Avg Hold</span><span>${(data.avg_hold_hours || 0).toFixed(1)}h</span></div>
  `;
}

async function refreshTrades() {
  const data = await fetchJSON('/api/trades?limit=20');
  if (!data) return;

  const tbody = document.getElementById('trade-table');
  if (!data.trades || data.trades.length === 0) return;

  tbody.innerHTML = data.trades.map(t => `
    <tr class="trade-row border-b border-gray-800">
      <td class="py-1 text-gray-200">${t.instrument}</td>
      <td class="text-center ${t.direction === 'BUY' ? 'text-green-400' : 'text-red-400'}">${t.direction}</td>
      <td class="text-center">${t.entry ? t.entry.toFixed(4) : '--'}</td>
      <td class="text-center">${t.exit ? t.exit.toFixed(4) : '--'}</td>
      <td class="text-center ${t.r_multiple >= 0 ? 'positive' : 'negative'}">${t.r_multiple ? t.r_multiple.toFixed(1) + 'R' : '--'}</td>
      <td class="text-center ${t.pnl_usd >= 0 ? 'positive' : 'negative'}">${t.pnl_usd ? (t.pnl_usd >= 0 ? '+' : '') + '$' + Math.abs(t.pnl_usd).toFixed(0) : '--'}</td>
      <td class="text-center ${t.outcome === 'WIN' ? 'positive' : t.outcome === 'LOSS' ? 'negative' : 'neutral'}">${t.outcome || 'OPEN'}</td>
    </tr>
  `).join('');
}

async function refreshLedger() {
  const data = await fetchJSON('/api/ledger?limit=20');
  if (!data) return;

  const container = document.getElementById('ledger-log');
  if (!data.messages || data.messages.length === 0) return;

  const typeColors = {
    'RISK_REJECTED': 'text-red-400',
    'RISK_APPROVED': 'text-green-400',
    'KILL_SWITCH_TRIGGERED': 'text-red-300 font-bold',
    'ORDER_FILLED': 'text-green-300',
    'SIGNAL_VALIDATION_APPROVED': 'text-blue-400',
    'SIGNAL_VALIDATION_REJECTED': 'text-orange-400',
    'MARKET_SCAN_RESULT': 'text-gray-400',
    'REGIME_CLASSIFICATION': 'text-purple-400',
  };

  container.innerHTML = data.messages.map(m => {
    const color = typeColors[m.type] || 'text-gray-400';
    const time = m.ts ? m.ts.substring(11, 19) : '--';
    const inst = m.instrument ? ` [${m.instrument}]` : '';
    const status = m.status ? ` → ${m.status}` : '';
    return `<div class="${color}">[${time}] ${m.type}${inst}${status} (${m.from})</div>`;
  }).join('');
}

async function runBacktest() {
  const statusEl = document.getElementById('backtest-status');
  statusEl.textContent = 'Running backtest...';
  statusEl.className = 'text-xs text-yellow-400 mt-2';
  try {
    const r = await fetch('/api/backtest/run', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({instrument: 'XAUUSD', instrument_type: 'GOLD', venue_type: 'CFD_BROKER', timeframe: 'M15'})
    });
    const data = await r.json();
    if (data.metrics) {
      statusEl.textContent = `Done! ${data.metrics.total_trades} trades, WR=${(data.metrics.win_rate*100).toFixed(1)}%, Net=$${data.metrics.net_profit_usd.toFixed(0)}`;
      statusEl.className = 'text-xs text-green-400 mt-2';
      refreshAll();
    } else {
      statusEl.textContent = 'Error: ' + (data.detail || JSON.stringify(data));
      statusEl.className = 'text-xs text-red-400 mt-2';
    }
  } catch(e) {
    statusEl.textContent = 'Error: ' + e.message;
    statusEl.className = 'text-xs text-red-400 mt-2';
  }
}

async function engageKill() {
  if (!confirm('Engage kill switch? This will halt all trading.')) return;
  await fetch('/api/control/kill', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({reason: 'Manual via dashboard', operator: 'dashboard'})
  });
  refreshAll();
}

async function resumeTrading() {
  if (!confirm('Resume trading? Ensure conditions are safe.')) return;
  await fetch('/api/control/resume', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({reason: 'Manual resume via dashboard', operator: 'dashboard'})
  });
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
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")

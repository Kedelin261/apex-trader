# Apex Multi-Market TJR Engine

**Version:** 1.0.0  
**Status:** Production-Grade Architecture, Paper Trading Ready  
**Classification:** Autonomous Institutional Trading System

---

## System Overview

The Apex Multi-Market TJR Engine is a production-grade, fully autonomous trading system combining:

- **Deterministic TJR strategy execution** (rule-based, auditable, no discretion)
- **Multi-agent AI orchestration** (scanning, regime detection, risk governance, execution supervision)
- **Universal market support** (Forex, Gold, Futures, Indices, Stocks, Crypto, Prediction Markets)
- **Institutional risk management** (non-bypassable hard rules)
- **Paper trading and live execution pathways**
- **Full audit trail and decision ledger**

---

## Quick Start

```bash
# 1. Install dependencies
pip3 install fastapi uvicorn pyyaml pydantic structlog apscheduler sqlalchemy ccxt

# 2. Start the system (paper mode)
cd /home/user/apex-trader
python3 live/api_server.py

# 3. Access dashboard
open http://localhost:8080/dashboard

# 4. Run backtest via API
curl -X POST http://localhost:8080/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"instrument":"XAUUSD","instrument_type":"GOLD","venue_type":"CFD_BROKER","timeframe":"M15"}'
```

---

## Architecture

```
Apex Multi-Market TJR Engine
│
├── TJR Strategy Engine (Python/deterministic)
│   ├── Swing Point Detector
│   ├── Market Structure Analyzer
│   ├── Liquidity Detector
│   ├── BOS Detector
│   ├── Session Classifier
│   └── Setup Quality Scorer
│
├── Multi-Agent Orchestration Layer
│   ├── Market Sentinel Agent
│   ├── Regime Detection Agent
│   ├── Strategy Validator Agent
│   ├── Risk Guardian Agent (non-bypassable)
│   ├── Execution Supervisor Agent
│   └── Reporting Agent
│
├── Agent Communication Protocol
│   ├── AgentMessage (typed, validated)
│   ├── AgentDecisionLedger (immutable JSONL)
│   └── AgentStateStore (coordination)
│
├── Risk Engine
│   ├── RiskManager (hard rules)
│   ├── DailyRiskGovernor (daily loss tracking)
│   └── MarketContextFilter (volatility/news/spread)
│
├── Data Infrastructure
│   ├── MarketDataService
│   ├── CsvLoader (historical)
│   └── HistoricalDataValidator
│
├── Backtest Engine
│   ├── Candle-by-candle simulation (no lookahead)
│   ├── RealisticFillModel (slippage/spread/commission)
│   └── PerformanceCalculator
│
└── API Server (FastAPI)
    ├── Dashboard (real-time)
    ├── TradingView Webhook (/api/signal)
    ├── Backtest trigger (/api/backtest/run)
    └── Kill switch controls
```

---

## Mandatory Trade Decision Chain

Every live or paper trade MUST pass this complete chain:

```
1. Market Sentinel → environment acceptable?
2. Regime Detection → regime classified, TJR eligible?
3. TJR Engine → valid setup found?
4. Strategy Validator → all rules met, quality >= 0.65?
5. Risk Guardian → risk approved (hard rules pass)?
6. Execution Supervisor → broker ready, order params valid?
7. Order Execution → fill confirmed?
8. Reporting Agent → trade lifecycle logged?
```

No step may be skipped. All decisions are logged to the AgentDecisionLedger.

---

## TJR Strategy Rules (Deterministic)

The TJR engine applies these exact rules:

| Rule | Implementation |
|------|---------------|
| Market Structure | SwingPointDetector with N-candle confirmation |
| Bullish Bias | Higher Highs AND Higher Lows (both required) |
| Bearish Bias | Lower Highs AND Lower Lows (both required) |
| BOS Confirmation | Close beyond prior swing + body ratio >= 50% |
| Liquidity Event | Equal level cluster within tolerance + sweep detection |
| Entry Trigger | Confirmation candle with body >= 50% of range |
| Stop Loss | Beyond most recent opposing swing + buffer |
| Take Profit | Entry ± (stop distance × min_rr) |
| Session Filter | London, New York, or Overlap only |
| Min R:R | 2.0:1 (configurable, minimum enforced) |
| Min Quality Score | 0.65 (all criteria weighted) |

---

## Risk Rules (Non-Negotiable)

| Rule | Default | Configurable |
|------|---------|-------------|
| Max risk per trade | 1% | Yes |
| Max daily loss | 3% | Yes |
| Max total drawdown | 10% | Yes |
| Max concurrent trades | 2 | Yes |
| Min R:R ratio | 2.0 | Yes |
| Max spread | 3 pips | Yes |
| Kill switch | Auto on limit breach | N/A |

---

## Market Support Matrix

| Market | TJR Support | Alternative Strategies | Notes |
|--------|-------------|----------------------|-------|
| Forex | ✅ Full | Trend following | London/NY sessions |
| Gold (XAUUSD) | ✅ Full | Trend following | High volatility filter |
| Futures | ✅ Full | Trend following | Contract-aware |
| Indices | ✅ Full | Trend following | Session-aware |
| Commodities | ✅ Full | Trend following | — |
| Stocks | ⚠️ Config needed | Event-driven | Market hours |
| Crypto | ❌ TJR incompatible | Volatility breakout | 24/7 aware |
| Prediction | ❌ TJR incompatible | Event-driven | Binary logic |

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/dashboard` | GET | Real-time trading dashboard |
| `/api/signal` | POST | TradingView webhook ingest |
| `/api/status` | GET | System health and status |
| `/api/trades` | GET | Trade history |
| `/api/metrics` | GET | Performance metrics |
| `/api/agents` | GET | Agent health states |
| `/api/ledger` | GET | Decision ledger entries |
| `/api/positions` | GET | Open positions |
| `/api/backtest/run` | POST | Trigger backtest |
| `/api/control/kill` | POST | Engage kill switch |
| `/api/control/resume` | POST | Resume trading |
| `/api/equity-curve` | GET | Equity curve data |

---

## Configuration

Main config: `config/system_config.yaml`

Key sections:
- `system.environment`: `backtest` | `paper` | `live`
- `risk.*`: All hard risk rules
- `strategy.tjr.*`: TJR engine parameters
- `markets.*`: Per-market configuration
- `agents.*`: Agent configuration
- `brokers.*`: Broker credentials (use env vars, not YAML)

---

## Deployment

See `setup.md` for full deployment instructions.

```bash
# Docker
docker-compose up -d

# Manual
python3 live/api_server.py
```

---

## Testing

```bash
cd /home/user/apex-trader
python3 -m pytest tests/ -v
```

---

## Project Structure

```
apex-trader/
├── core/           # TJR engine, risk, market context
├── domain/         # Data models and enumerations
├── data/           # Market data service, CSV loader
├── adapters/       # Per-market adapters (forex, futures, crypto, etc.)
├── backtest/       # Realistic backtesting engine
├── live/           # API server, paper/live trading
├── agents/         # Multi-agent orchestration
├── protocol/       # Agent communication protocol
├── config/         # YAML configuration
├── scripts/        # Pine Script, deployment scripts
├── docs/           # Documentation
├── tests/          # Unit, integration, replay tests
├── logs/           # Agent decision ledger, trade logs
└── reports/        # Generated performance reports
```

---

## Compliance

See `COMPLIANCE.md` for regulatory and operational compliance documentation.

## Risk Documentation

See `RISK.md` for complete risk management documentation.

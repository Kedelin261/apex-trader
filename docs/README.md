# Apex Multi-Market TJR Engine

**Version:** 2.0.0  
**Status:** Live-Ready — Full Broker Connectivity Implemented  
**Classification:** Autonomous Institutional Trading System  
**Broker:** OANDA v20 REST API (Practice: ✅ Authenticated | Balance: $100,000 USD)

---

## System Overview

The Apex Multi-Market TJR Engine is a production-grade, fully autonomous trading system:

- **Deterministic TJR strategy** (rule-based, auditable, zero discretion)
- **Multi-agent AI orchestration** (9 agents: Sentinel, Regime, Validator, Risk, Execution, Reporting…)
- **Universal market support** (Forex, Gold, Futures, Indices, Stocks, Crypto, Prediction)
- **Institutional risk management** (non-bypassable hard rules, kill switch, circuit breaker)
- **Full broker connectivity** (OANDA v20 REST, live order submission, positions, balances)
- **Execution state machine** (PAPER → LIVE_CONNECTED_SAFE → LIVE_ENABLED → KILL_SWITCH)
- **Full audit trail** (decision ledger, reconciliation log, rejection history)

---

## What's New in v2.0

| Feature | Status |
|---------|--------|
| OANDA v20 REST connector (full) | ✅ |
| Credential manager (env-only, masked) | ✅ |
| Execution state machine (5 states) | ✅ |
| Two-step live arming flow | ✅ |
| Preflight checklist (14 checks) | ✅ |
| Reconciliation service (60s auto) | ✅ |
| Order rejection parsing + history | ✅ |
| Circuit breaker (5 failures) | ✅ |
| Broker API endpoints (12 new) | ✅ |
| Enhanced dashboard with broker panel | ✅ |
| 86 tests passing (all green) | ✅ |
| Security: credentials never logged | ✅ |

---

## Quick Start

```bash
# 1. Configure credentials
cp /home/user/apex-trader/.env.example /home/user/apex-trader/.env
# Edit .env with your OANDA token and account ID

# 2. Start the system
cd /home/user/apex-trader
pm2 start ecosystem.config.cjs

# 3. Verify
curl http://localhost:8080/health

# 4. Connect broker (safe mode)
curl -X POST http://localhost:8080/api/broker/connect \
  -H "Content-Type: application/json" \
  -d '{"broker": "oanda", "operator": "YourName"}'
```

---

## Operator Workflow

```
START (PAPER_MODE)
      │
      │  POST /api/broker/connect
      ▼
LIVE_CONNECTED_SAFE  ← reads account, positions, balances (no orders)
      │
      │  POST /api/broker/preflight (verify 14 checks)
      │  POST /api/execution/arm-live (acknowledge_risk=true)
      ▼
LIVE_ENABLED  ← real TJR orders routing to OANDA
      │
      │  POST /api/execution/disarm-live
      ▼
LIVE_CONNECTED_SAFE (monitoring, no trading)
```

---

## Preflight Results (Live Test)

```
✅ credentials_valid       — Token format valid
✅ authentication_success  — OANDA Practice authenticated
✅ account_info_readable   — $100,000 USD balance
✅ environment_correct     — practice configured
✅ permissions_check       — can_trade=True
✅ instrument_mapping      — XAUUSD, EURUSD, GBPUSD mapped
✅ positions_readable      — 0 open positions
✅ orders_readable         — 0 pending orders
✅ kill_switch_available   — Active
✅ sufficient_balance      — $100,000 USD
✅ risk_kill_switch_off    — Clear
✅ execution_state_valid   — LIVE_CONNECTED_SAFE
✅ daily_loss_limit_ok     — Not hit
✅ operator_arming_required — Two-step enforced
```

---

## API Reference

### Legacy (v1.0)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/signal` | POST | TradingView webhook |
| `/api/status` | GET | System health |
| `/api/trades` | GET | Trade history |
| `/api/metrics` | GET | Performance metrics |
| `/api/agents` | GET | Agent health |
| `/api/ledger` | GET | Decision ledger |
| `/api/backtest/run` | POST | Run backtest |

### Broker (v2.0 New)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/broker/connect` | POST | Authenticate broker |
| `/api/broker/status` | GET | Connection status |
| `/api/broker/account` | GET | Live account info |
| `/api/broker/balances` | GET | Live balances |
| `/api/broker/positions` | GET | Live positions |
| `/api/broker/open-orders` | GET | Pending orders |
| `/api/broker/preflight` | POST | Run preflight (14 checks) |
| `/api/broker/test-order-payload` | POST | Build order JSON (safe) |
| `/api/execution/arm-live` | POST | Arm live trading |
| `/api/execution/disarm-live` | POST | Disarm |
| `/api/execution/state` | GET | State snapshot |
| `/api/execution/history` | GET | Transition history |
| `/api/control/kill` | POST | Kill switch |
| `/api/control/resume` | POST | Reset to paper |
| `/api/control/rejection-history` | GET | Order rejections |
| `/api/reconcile/run` | POST | Reconcile positions |
| `/api/reconcile/history` | GET | Reconciliation history |

---

## Risk Rules (Non-Negotiable)

| Rule | Value |
|------|-------|
| Max risk per trade | 1% of balance |
| Max daily loss | 3% of balance |
| Max total drawdown | 10% |
| Max concurrent trades | 2 |
| Min reward:risk ratio | 2.0 |
| Max spread | 3.0 pips |
| Kill switch | Irreversible until reset |
| Live arming | Two-step (connect → arm) |

---

## Test Results

```
86 tests passed (0 failed)
  - 53 broker/state-machine/API integration tests
  - 33 TJR engine / risk management unit tests
```

---

## Project Structure

```
apex-trader/
├── brokers/
│   ├── base_connector.py      # Abstract connector interface
│   ├── credential_manager.py  # Secure env-var credential loading
│   └── oanda_connector.py     # Full OANDA v20 implementation
├── live/
│   ├── api_server.py          # FastAPI server (v2.0, 12 new endpoints)
│   ├── broker_manager.py      # Central broker lifecycle manager
│   ├── execution_state_machine.py  # 5-state machine, persisted
│   ├── preflight_service.py   # 14-check preflight orchestration
│   └── reconciliation_service.py  # Auto + manual reconciliation
├── core/
│   ├── tjr_strategy_engine.py # Deterministic TJR (unchanged)
│   └── risk_manager.py        # Hard risk rules (unchanged)
├── agents/                    # 9 AI agents (unchanged)
├── tests/
│   ├── unit/test_tjr_engine.py          # 33 tests
│   └── integration/test_broker_integration.py  # 53 tests
└── docs/
    ├── LIVE_TRADING_SETUP.md
    ├── BROKER_CONNECTION.md
    ├── EXECUTION_STATES.md
    ├── PRE_FLIGHT_CHECKLIST.md
    └── SAFETY_CONTROLS.md
```

---

## Deployment

```bash
# Development
pm2 start ecosystem.config.cjs

# Tests
python3 -m pytest tests/ -v

# Docker (coming)
docker-compose up

# Cloudflare (API logs)
# R2 bucket configured via env vars for audit log archival
```

---

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/LIVE_TRADING_SETUP.md` | Step-by-step live trading setup |
| `docs/BROKER_CONNECTION.md` | OANDA connector architecture |
| `docs/EXECUTION_STATES.md` | State machine reference |
| `docs/PRE_FLIGHT_CHECKLIST.md` | All 14 preflight checks |
| `docs/SAFETY_CONTROLS.md` | Kill switch, circuit breaker, fail-closed |
| `docs/ARCHITECTURE.md` | System architecture |
| `docs/RISK.md` | Risk management rules |
| `docs/AGENT_PROTOCOL.md` | Agent communication protocol |
| `docs/COMPLIANCE.md` | Compliance and audit requirements |

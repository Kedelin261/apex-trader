# Broker Connection Guide
## OANDA v20 REST API Integration

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    BrokerManager                             │
│  ┌─────────────────┐   ┌─────────────────────────────────┐  │
│  │ CredentialMgr   │   │  ExecutionStateMachine          │  │
│  │ (env-only load) │   │  (5 states, audited)            │  │
│  └────────┬────────┘   └──────────────┬──────────────────┘  │
│           │                           │                      │
│  ┌────────▼────────┐   ┌──────────────▼──────────────────┐  │
│  │ OandaConnector  │   │  PreflightService (14 checks)    │  │
│  │ (full v20 REST) │   └──────────────┬──────────────────┘  │
│  └────────┬────────┘                  │                      │
│           │             ┌─────────────▼───────────────────┐  │
│           └────────────►│  ReconciliationService          │  │
│                         │  (internal vs broker, 60s)      │  │
│                         └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## OANDA Environments

| Environment | Base URL | Use Case |
|------------|----------|----------|
| `practice` | `api-fxpractice.oanda.com` | Demo account, paper trading |
| `live` | `api-fxtrade.oanda.com` | Real money, production |

Set via `APEX_OANDA_ENVIRONMENT=practice` (default: `practice`).

---

## Supported Instruments

| Internal Symbol | OANDA Symbol | Type |
|----------------|-------------|------|
| EURUSD | EUR_USD | Forex |
| GBPUSD | GBP_USD | Forex |
| USDJPY | USD_JPY | Forex |
| XAUUSD | XAU_USD | Gold |
| XAGUSD | XAG_USD | Silver |
| USOIL | WTICO_USD | Oil |
| US30 | US30_USD | Index |
| US500 | SPX500_USD | Index |
| NAS100 | NAS100_USD | Index |

---

## Connector Methods

All methods are fully implemented (no stubs):

```python
connector.authenticate()              → (bool, str)
connector.is_connected()              → bool
connector.get_account_info()          → AccountInfo
connector.get_balances()              → List[BrokerBalance]
connector.get_permissions()           → PermissionsCheck
connector.get_positions()             → List[BrokerPosition]
connector.get_open_orders()           → List[BrokerOrder]
connector.validate_instrument_mapping(symbol) → Optional[InstrumentMapping]
connector.submit_order(request)       → OrderResult
connector.cancel_order(order_id)      → (bool, str)
connector.get_order_status(order_id)  → BrokerOrder
connector.reconcile_state(internal)   → ReconciliationResult
connector.run_preflight_checks()      → PreflightResult
connector.build_test_order_payload(r) → (bool, dict)  # Safe mode test
```

---

## Order Flow

```
TJR Signal Generated
       │
       ▼
ExecutionStateMachine.is_live_trading_allowed()?
       │
   YES │          NO
       │          │
       ▼          ▼
OandaConnector   PaperBroker
.submit_order()  .submit_order()
       │
       ▼
   Success? → Record fill
   Reject?  → Record rejection + check circuit breaker
   Insuf.$? → Flag insufficient_funds=True
   CB Open? → ESM.block_live()
```

---

## Circuit Breaker

After **5 consecutive order failures**, the circuit breaker opens:
- All new order submissions blocked
- ESM transitions to `LIVE_BLOCKED`
- Manual reset required: `connector.reset_circuit_breaker()`

---

## Reconciliation

Runs every 60 seconds (background) and on demand:

```bash
curl -X POST http://localhost:8080/api/reconcile/run \
  -H "Content-Type: application/json" \
  -d '{"operator": "YourName"}'
```

Compares:
- Internal positions (engine-tracked) vs OANDA live positions
- PnL discrepancy per position (> $100 → auto-block)
- Orphan detection (positions at broker not in engine, and vice versa)

---

## Security Rules

1. **Credentials never logged** — only masked hints (`8572****09`)
2. **Credentials from environment only** — not config YAML
3. **`repr()` never shows token** — dataclass field `repr=False`
4. **`.env` in `.gitignore`** — never committed
5. **Dashboard shows only account ID** — not token
6. **All API responses masked** — no credential fields in JSON

# Live Trading Setup Guide
## Apex Multi-Market TJR Engine v2.0

> **CRITICAL**: Never arm live trading with real money until all preflight checks pass and you have verified correct system behaviour in paper mode and LIVE_CONNECTED_SAFE mode.

---

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| OANDA Account | Practice or live forex/CFD account |
| API Token | OANDA v20 REST API token |
| Account ID | Your OANDA account ID (auto-detected if blank) |
| Python 3.10+ | For the trading engine |
| Internet | For broker API connectivity |

---

## Step 1 — Configure Credentials

Create `/home/user/apex-trader/.env`:

```bash
# OANDA
APEX_OANDA_API_TOKEN=<your-32hex-dash-32hex-token>
APEX_OANDA_ACCOUNT_ID=<your-account-id>      # e.g. 101-001-12345678-001
APEX_OANDA_ENVIRONMENT=practice              # practice | live

# JWT (internal)
APEX_JWT_SECRET=<random-64-char-secret>
```

**Finding your OANDA token**: Log in to OANDA → My Account → API Access → Generate New Token

**Finding your account ID**: After generating token, call:
```bash
curl https://api-fxpractice.oanda.com/v3/accounts \
  -H "Authorization: Bearer <your_token>"
```

---

## Step 2 — Start the System

```bash
cd /home/user/apex-trader
pm2 start ecosystem.config.cjs
# Verify
curl http://localhost:8080/health
```

---

## Step 3 — Connect to Broker (LIVE_CONNECTED_SAFE)

```bash
curl -X POST http://localhost:8080/api/broker/connect \
  -H "Content-Type: application/json" \
  -d '{"broker": "oanda", "operator": "YourName"}'
```

**On success**: System enters `LIVE_CONNECTED_SAFE` state.
- ✅ Real authentication with broker
- ✅ Account info readable
- ✅ Positions readable
- ✅ Balances readable
- 🔒 **No orders submitted** (safe lock active)

---

## Step 4 — Run Preflight Checklist

```bash
curl -X POST http://localhost:8080/api/broker/preflight \
  -H "Content-Type: application/json" \
  -d '{"operator": "YourName"}'
```

All 14 checks must pass:
1. `credentials_valid` — Token format valid
2. `authentication_success` — OANDA accepted token
3. `account_info_readable` — Account summary loaded
4. `environment_correct` — practice/live as configured
5. `permissions_check` — Trading permitted
6. `instrument_mapping` — XAUUSD, EURUSD, GBPUSD mapped
7. `positions_readable` — Position endpoint works
8. `orders_readable` — Order endpoint works
9. `kill_switch_available` — Kill switch active
10. `sufficient_balance` — Balance > 0
11. `risk_kill_switch_off` — Risk engine clear
12. `execution_state_valid` — ESM in valid state
13. `daily_loss_limit_ok` — Daily limit not hit
14. `operator_arming_required` — Two-step arming enforced

---

## Step 5 — Observe in LIVE_CONNECTED_SAFE Mode

While in LIVE_CONNECTED_SAFE, you can:
- View real account balance: `GET /api/broker/account`
- View real positions: `GET /api/broker/positions`
- View real balances: `GET /api/broker/balances`
- Build order payloads (not submitted): `POST /api/broker/test-order-payload`
- Run reconciliation: `POST /api/reconcile/run`

**You cannot**: Submit real orders (state machine blocks them)

---

## Step 6 — Fund Account (if needed)

If balance is $0, orders will be rejected by OANDA. Fund your account through OANDA's portal before proceeding to step 7.

---

## Step 7 — Arm Live Trading (LIVE_ENABLED) ⚡

**This is the point of no return for real money.**

```bash
curl -X POST http://localhost:8080/api/execution/arm-live \
  -H "Content-Type: application/json" \
  -d '{
    "operator": "YourName",
    "acknowledge_risk": true,
    "confirmation_code": "APEX_LIVE_ARM"
  }'
```

Requirements to arm:
- Current state must be `LIVE_CONNECTED_SAFE`
- Preflight must have passed
- `acknowledge_risk` must be `true`
- Kill switch must be inactive

**After arming**: System state → `LIVE_ENABLED`. TJR signals now route to real broker.

---

## Disarming

```bash
curl -X POST http://localhost:8080/api/execution/disarm-live \
  -H "Content-Type: application/json" \
  -d '{"operator": "YourName", "reason": "End of session"}'
```

Returns to `LIVE_CONNECTED_SAFE`. No orders lost; open positions remain at broker.

---

## Emergency Kill Switch

```bash
curl -X POST http://localhost:8080/api/control/kill \
  -H "Content-Type: application/json" \
  -d '{"reason": "Risk event", "operator": "YourName"}'
```

Transitions to `KILL_SWITCH_ENGAGED`. **Irreversible** until explicit reset:

```bash
curl -X POST http://localhost:8080/api/control/resume \
  -H "Content-Type: application/json" \
  -d '{"operator": "YourName", "reason": "Conditions normalised"}'
```

Note: Reset only goes to `PAPER_MODE`. Must reconnect broker to resume live.

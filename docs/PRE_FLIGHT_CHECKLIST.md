# Pre-Flight Checklist
## Apex Multi-Market TJR Engine v2.0

---

## Automated Preflight (14 Checks)

Run via API:
```bash
curl -X POST http://localhost:8080/api/broker/preflight \
  -H "Content-Type: application/json" \
  -d '{"operator": "YourName"}'
```

### Checks Summary

| # | Check Name | Critical | Description |
|---|-----------|---------|-------------|
| 1 | `credentials_valid` | YES | Token format valid (32hex-32hex) |
| 2 | `authentication_success` | YES | OANDA accepted token |
| 3 | `account_info_readable` | YES | Account summary loaded successfully |
| 4 | `environment_correct` | NO | practice/live matches config |
| 5 | `permissions_check` | NO | Trading permitted, no restrictions |
| 6 | `instrument_mapping` | NO | XAUUSD, EURUSD, GBPUSD tradeable |
| 7 | `positions_readable` | YES | Open positions endpoint works |
| 8 | `orders_readable` | NO | Pending orders endpoint works |
| 9 | `kill_switch_available` | YES | Kill switch API active |
| 10 | `sufficient_balance` | NO | Balance > $0 (warning if zero) |
| 11 | `risk_kill_switch_off` | YES | Risk engine not in kill state |
| 12 | `execution_state_valid` | YES | ESM in PAPER/CONNECTED_SAFE/BLOCKED |
| 13 | `daily_loss_limit_ok` | YES | Daily loss limit not hit |
| 14 | `operator_arming_required` | NO | Two-step arming confirmed enforced |

**All critical (YES) checks must pass to arm live trading.**

---

## Manual Pre-Go-Live Checklist

Before pressing the ARM LIVE TRADING button, verify:

### System
- [ ] API server running: `curl http://localhost:8080/health`
- [ ] All 86 tests pass: `python3 -m pytest tests/ -q`
- [ ] No errors in PM2 logs: `pm2 logs apex-trader --nostream`

### Broker
- [ ] OANDA credentials valid (token not expired)
- [ ] Account ID correct and accessible
- [ ] Environment confirmed (practice vs live)
- [ ] Balance > $0 in target account
- [ ] No existing positions from other systems that could conflict

### Risk Settings
- [ ] `max_risk_per_trade_pct: 1.0` in config
- [ ] `max_daily_loss_pct: 3.0` in config
- [ ] `max_total_drawdown_pct: 10.0` in config
- [ ] `max_concurrent_trades: 2` in config
- [ ] Spread filter active: `max_spread_pips: 3.0`

### Strategy
- [ ] Backtest run and results reviewed
- [ ] Paper trading observed for ≥ 1 session
- [ ] No unusual signals or blocked trades
- [ ] Economic calendar checked for high-impact events today

### Safety
- [ ] Kill switch endpoint tested
- [ ] Disarm flow tested
- [ ] Operator knows the arming/disarming procedure
- [ ] Reconciliation runs clean (0 mismatches)

---

## Expected Response (All Pass)

```json
{
  "overall_pass": true,
  "blocking_failures": [],
  "warnings": [],
  "checks": [
    {"name": "credentials_valid", "passed": true, "critical": true},
    {"name": "authentication_success", "passed": true, "critical": true},
    ...14 total...
  ]
}
```

## Common Failures and Fixes

| Failure | Cause | Fix |
|---------|-------|-----|
| `credentials_valid` | Token format wrong | Regenerate OANDA API token |
| `authentication_success` | Token rejected | Token expired; regenerate |
| `account_info_readable` | Account ID wrong | Auto-detect or check OANDA portal |
| `sufficient_balance` | Zero balance | Fund the account |
| `risk_kill_switch_off` | Kill switch active | `POST /api/control/resume` |
| `daily_loss_limit_ok` | Daily limit hit | Wait until next trading day |
| `execution_state_valid` | Wrong state | `POST /api/broker/connect` first |

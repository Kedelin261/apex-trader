# Safety Controls
## Apex Multi-Market TJR Engine v2.0

---

## Architecture: Defense-in-Depth

```
Signal Generated
      │
      ▼
[1] Kill Switch Check        ← CRITICAL (blocks all)
      │ pass
      ▼
[2] Execution State Check    ← CRITICAL (must be LIVE_ENABLED)
      │ pass
      ▼
[3] Circuit Breaker Check    ← CRITICAL (broker failures)
      │ pass
      ▼
[4] Risk Manager Gates       ← per-signal checks
      ├── Daily Loss Limit
      ├── Max Drawdown
      ├── Max Concurrent Trades
      ├── Min R:R Ratio
      ├── Max Spread Filter
      └── Regime/Session Filter
      │ pass
      ▼
[5] Strategy Validator Agent ← quality score ≥ 0.65
      │ pass
      ▼
[6] Broker Submit            ← connector.submit_order()
      │
      ├── INSUFFICIENT_MARGIN → rejection logged, no retry
      └── SUCCESS → record fill
```

---

## Hard Rules (Cannot be overridden by AI)

These rules are enforced in code, not configuration. AI agents can only pass/fail signals through the Intent Layer:

| Rule | Value | Code Location |
|------|-------|--------------|
| Max risk per trade | 1% of balance | `risk_manager.py:check_signal()` |
| Max daily loss | 3% of balance | `risk_manager.py:DailyRiskGovernor` |
| Max total drawdown | 10% | `risk_manager.py:get_drawdown_pct()` |
| Max concurrent trades | 2 | `risk_manager.py:check_signal()` |
| Min reward:risk | 2.0 | `risk_manager.py:check_signal()` |
| Max spread | 3.0 pips | `risk_manager.py:check_signal()` |
| Kill switch irreversible | until reset | `execution_state_machine.py` |
| Live arming 2-step | connect then arm | `broker_manager.py` |

---

## Kill Switch

### Engage
```bash
POST /api/control/kill
{"reason": "Your reason", "operator": "YourName"}
```

**Effects**:
- `ExecutionStateMachine` → `KILL_SWITCH_ENGAGED`
- `RiskManager._global_kill_switch = True`
- `AgentOrchestrator._kill_switch = True`
- Decision ledger entry created
- All subsequent order submissions blocked

### Reset (PAPER_MODE only)
```bash
POST /api/control/resume
{"operator": "YourName", "reason": "Conditions normalised"}
```

**Effects**:
- System returns to `PAPER_MODE`
- Kill switch cleared at all layers
- Broker connection cleared
- Must reconnect to resume live

---

## Execution State Guards

Every order submission checks:

```python
if esm.is_kill_switch_active():         # → REJECTED
if not esm.is_live_trading_allowed():   # → PAPER route
if connector.check_circuit_breaker():   # → REJECTED
```

`is_live_trading_allowed()` is `True` ONLY when:
- State == `LIVE_ENABLED`
- `_live_enabled == True`
- `_kill_switch == False`

---

## Circuit Breaker

| Event | Action |
|-------|--------|
| Order rejected | `_consecutive_failures += 1` |
| 5th failure | `_circuit_breaker_open = True` |
| Circuit open | All orders blocked |
| Block detected | ESM → `LIVE_BLOCKED` |
| Manual reset | `connector.reset_circuit_breaker()` |

---

## Reconciliation Auto-Block

If internal vs broker position PnL discrepancy > $100:
- Reconciliation service logs mismatch
- ESM transitions to `LIVE_BLOCKED`
- Alert logged
- Requires operator investigation before re-arming

---

## Credential Security

| Measure | Implementation |
|---------|---------------|
| Never in logs | Token field is `repr=False` in dataclass |
| Never in config YAML | Only loaded from environment variables |
| Never in dashboard | API returns masked hint only |
| Never in git | `.env` in `.gitignore` |
| Startup validation | Fails-closed if credentials invalid |
| .env fallback | Only parsed if not already in `os.environ` |

---

## Fail-Closed Behaviour

All error paths default to rejection, not approval:

| Error | Behaviour |
|-------|-----------|
| Credential missing | `is_usable() = False` → connect fails |
| Auth fails (401) | Connector stays DISCONNECTED |
| OANDA API timeout | `ConnectionError` → ESM stays in current state |
| Preflight partial fail | `overall_pass = False` → arming blocked |
| Unknown order status | `OrderStatus.UNKNOWN` → logged as failure |
| Reconciliation error | `is_clean = False` → auto-block if over threshold |
| Circuit breaker | Orders blocked until manual reset |
| State file corrupted | Defaults to PAPER_MODE |
| LIVE_ENABLED on restart | Downgrades to LIVE_CONNECTED_SAFE |

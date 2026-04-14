# Risk Management Documentation — Apex Multi-Market TJR Engine

## Risk Philosophy

Risk management is not optional. Every rule in this document is enforced in code.
No trade can bypass the risk engine. No agent can override a risk rejection.

## Hard Risk Rules

### Per-Trade Risk (RiskManager.validate_signal)

| Rule | Default | Class |
|------|---------|-------|
| Max risk per trade | 1.0% of account | Hard |
| Min R:R ratio | 2.0:1 | Hard |
| Max spread at entry | 3.0 pips | Hard |
| Min stop distance | 5.0 pips | Hard |
| Max position size | $10,000 | Hard |

### Daily Risk (DailyRiskGovernor)

| Rule | Default | Behavior on Breach |
|------|---------|-------------------|
| Max daily loss | 3.0% | Kill switch engaged |
| Max drawdown | 10.0% | Kill switch engaged |

Once the kill switch is engaged, it remains active until manually reset via `/api/control/resume`.
This reset requires explicit confirmation.

### Session Rules (TJR Engine)

- Trading only during London, New York, or Overlap sessions
- Asian session trades require explicit market-specific override
- Off-hours trading is disabled for TJR

### Concurrent Trade Limits

- Maximum 2 simultaneous open trades (configurable)
- Each new signal is checked against current open position count
- No override possible through any agent

### Volatility and News Filters (MarketContextFilter)

| Filter | Trigger | Action |
|--------|---------|--------|
| Volatility spike | Current ATR > 3x average | Block trade |
| Spread expansion | Spread > 3x normal | Block trade |
| High-impact news | Within ±30 minutes | Block trade |
| Regime: UNSTABLE | ADX < 15 + fragility | Block trade |
| Regime: ILLIQUID | Low volume/liquidity | Block trade |

### Market-Specific Risk Profiles

Different instruments carry different risk characteristics:
- **XAUUSD (Gold)**: Tighter pip value, higher ATR volatility filter
- **Futures (ES/NQ)**: Contract-specific pip value, rollover awareness
- **Crypto**: 24/7 regime, no session filter, higher volatility threshold

## Kill Switch System

The kill switch is a three-level protection system:

1. **Instrument-level**: Specific instrument blocked by context filter
2. **Daily-level**: Day's trading blocked by DailyRiskGovernor
3. **Global**: All trading halted via `engage_kill_switch(reason)`

Kill switch triggers:
- Max daily loss reached
- Max drawdown reached
- 5+ consecutive execution failures (circuit breaker)
- Manual trigger via API or dashboard

## Position Sizing

Position size formula (fixed fractional):
```
risk_amount = account_balance × (risk_pct / 100)
position_lots = risk_amount / (stop_distance_pips × pip_value_per_lot)
```

Pip values:
- EURUSD/standard forex: $10.00 per pip per standard lot
- XAUUSD: $1.00 per pip (0.01 pip size)
- Futures (ES): $12.50 per tick per contract

## Risk Ledger

All risk decisions are logged to:
- `logs/agent_decision_ledger.jsonl` — every RISK_APPROVED/RISK_REJECTED message
- Immutable: entries are never deleted or modified
- Each entry includes: timestamp, signal_id, instrument, rejection reasons, account state

## Operational Risk Checklist

Before going live:
- [ ] Backtest shows positive expectancy on 1+ year data
- [ ] Max drawdown in backtest < defined threshold
- [ ] Paper trading runs without crashes for 2+ weeks
- [ ] All agent decisions are logged correctly
- [ ] Kill switch tested and working
- [ ] Daily risk governor tested with simulated losses
- [ ] Broker connectivity verified
- [ ] Risk parameters reviewed by operator

## Forbidden Actions

The following are architecturally prevented:
- Trades without risk approval (risk chain is mandatory)
- Modifying hard risk parameters at runtime without restart
- Optimization proposals overriding live logic directly
- Silencing risk rejection messages
- Bypassing the AgentDecisionLedger

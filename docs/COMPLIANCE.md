# Compliance Document — Apex Multi-Market TJR Engine

## Disclaimer

This system is a tool for research, simulation, and operator-controlled trading.
It does not constitute financial advice. All trading involves risk. Past performance
in backtests does not guarantee future results.

## Operational Compliance

### Audit Trail
- Every trade decision is logged to AgentDecisionLedger (JSONL)
- All risk approvals and rejections are permanently stored
- Trade lifecycle is fully traceable from signal to close
- Agent decision chains are attached to every trade record

### System Boundaries
- The system operates within configured risk limits at all times
- No autonomous increase of risk parameters
- All kill switch events are logged with reason and timestamp
- All operator commands (via API or dashboard) are logged

### Data Integrity
- Historical data validated before use (no corrupt/duplicate candles)
- Live data feed anomalies trigger circuit breakers
- All prices are validated before order submission

### Broker Compliance
- Duplicate order prevention: each signal generates one order
- Idempotency safeguards on all order submissions
- Position reconciliation with broker state on reconnect

## Forbidden System Behaviors

The following are architecturally prevented:
1. Fabricating trading results
2. Backdating trades
3. Silencing risk rejections
4. Overriding stop losses without logged reason
5. Operating in live mode without prior paper trading validation
6. Deploying unvalidated strategy changes to live capital

## Environment Controls

| Environment | Risk | Execution | Capital |
|-------------|------|-----------|---------|
| BACKTEST | Simulated | Simulated | Virtual |
| PAPER | Real-time simulated | Simulated | Virtual |
| LIVE | Real-time enforced | Real broker | Real |

Transition from PAPER to LIVE requires:
1. Minimum 100 paper trades logged
2. Positive expectancy demonstrated
3. Max drawdown within threshold
4. Manual configuration change (not automatic)
5. Operator confirmation

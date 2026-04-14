# Architecture Document — Apex Multi-Market TJR Engine

## Architectural Principles

1. **Deterministic core, AI-assisted periphery**: The TJR strategy engine produces fully rule-based signals. AI agents assist with classification, validation, and supervision — not with overriding rules.

2. **Protocol-enforced communication**: All agent communication is typed, logged, and replayable. No ad hoc shared memory.

3. **Non-bypassable risk law**: The RiskManager and DailyRiskGovernor are positioned as mandatory gates. No signal reaches execution without passing both.

4. **Market-aware abstraction**: Universal market support uses proper abstractions. TJR is not blindly applied to incompatible markets.

5. **Fail-safe by design**: Every failure mode has a defined response. No silent failures.

## System Layers

### Layer 1: Data Infrastructure
- `MarketDataService`: Loads and normalizes historical and live candle data
- `CsvLoader`: CSV ingest with format auto-detection
- `HistoricalDataValidator`: Rejects corrupt, duplicate, invalid data
- All data validated before reaching strategy logic

### Layer 2: Domain Models
- All data structures use Pydantic for validation
- Immutable where possible
- Explicit enumerations for all categorical values
- No implicit conversions

### Layer 3: Strategy Engine (TJR)
- Fully deterministic, no discretionary conditions
- All rules are numeric and auditable
- Outputs: TJRSetup (candidate) → Signal (execution-ready)
- Eligibility checked per instrument type before analysis

### Layer 4: Risk Engine
- RiskManager: Per-signal hard validation
- DailyRiskGovernor: Daily loss tracking and kill switch
- MarketContextFilter: Volatility/news/spread/regime gates
- Kill switch is irreversible within session

### Layer 5: Agent Orchestration
- AgentOrchestrator enforces the 8-step decision chain
- Each agent has defined scope and cannot bypass other agents' decisions
- AgentDecisionLedger provides immutable audit trail

### Layer 6: API Server
- FastAPI for webhooks and dashboard
- All endpoints connect to real backend state
- No placeholder data
- Kill switch and control endpoints included

## Decision Chain (Mandatory)

```
MarketSentinelAgent
    → Scan environment, detect volatility/spread issues
    → Emit MARKET_SCAN_RESULT per instrument
    → Update tradeable_instruments in StateStore

RegimeDetectionAgent
    → Classify regime per instrument (ADX-based)
    → Determine eligible strategy families
    → Emit REGIME_CLASSIFICATION
    → Update regime_context in StateStore

TJRStrategyEngine.analyze()
    → Process candle series (no lookahead)
    → Detect swings, structure, liquidity, BOS
    → Apply session filter
    → Produce TJRSetup or None
    → Orchestrator emits SIGNAL_CANDIDATE

StrategyValidatorAgent
    → Validate all rules deterministically
    → Check quality score >= threshold
    → Emit SIGNAL_VALIDATION_APPROVED or REJECTED

RiskGuardianAgent
    → Check all hard risk rules
    → Check daily limits, drawdown, concurrent trades
    → Emit RISK_APPROVED or RISK_REJECTED
    → On breaches: engage kill switch + emit KILL_SWITCH_TRIGGERED

ExecutionSupervisorAgent
    → Validate order parameters
    → Check execution environment
    → Submit order (paper or live)
    → Emit ORDER_FILLED or ORDER_FAILED
    → Manage circuit breaker

ReportingAgent
    → Generate trade lifecycle summary
    → Export daily/weekly reports
    → Emit DAILY_SUMMARY
```

## Agent Priority Rules

- CRITICAL messages are processed immediately (kill switch, broker failure, drawdown breach)
- Risk messages override optimization messages
- Execution block messages override signal approval messages
- A signal cannot execute unless: regime + validation + risk + execution are all approved
- Optimization proposals CANNOT alter live strategy logic directly
- Reporting messages CANNOT alter execution state

## Data Flow Diagram

```
Market Data (CSV/Live) 
    → MarketDataService 
    → HistoricalDataValidator 
    → Candle[] (validated)
    
Candle[] 
    → TJRStrategyEngine.analyze() 
    → TJRSetup (if setup found)
    → Signal (with position sizing)
    
Signal 
    → StrategyValidatorAgent.validate_tjr_signal() 
    → [APPROVED/REJECTED] 
    → RiskGuardianAgent.evaluate_signal()
    → [APPROVED/REJECTED]
    → ExecutionSupervisorAgent.submit_order()
    → [FILLED/FAILED]
    → Trade (record created)
    → ReportingAgent.generate_trade_summary()

All agents → AgentDecisionLedger (immutable JSONL)
All state → AgentStateStore (coordination only)
All metrics → MetricsService → Dashboard
```

## Security Boundaries

AI agents MAY:
- Classify regimes
- Rank instruments
- Score setup quality
- Detect anomalies
- Generate reports
- Propose optimizations (flagged as proposals only)

AI agents MAY NOT:
- Place trades outside the approval chain
- Override hard risk rules
- Modify live strategy parameters without validation
- Bypass the AgentDecisionLedger
- Execute orders without risk approval

## Broker Integration Architecture

Each broker/exchange has an isolated connector:
- `ForexBrokerConnector`: REST/FIX protocol forex brokers
- `TradovateConnector`: Tradovate API for futures
- `CryptoExchangeConnector`: CCXT-compatible exchanges
- `PaperBrokerConnector`: In-memory simulation (default)

All connectors implement the same interface:
- `is_connected() → bool`
- `place_order(instrument, direction, lots, sl, tp) → dict`
- `get_positions() → List[Position]`
- `get_account_balance() → float`

The ExecutionSupervisor never knows which connector it's using.

## Performance Characteristics

- Backtest: ~1000 candles/second on CPU
- Agent cycle: ~30-50ms per instrument
- Memory: ~200MB baseline
- Storage: JSONL ledger grows at ~1KB per trade decision chain

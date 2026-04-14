# Agent Communication Protocol — Apex Multi-Market TJR Engine

## Protocol Principles

1. Every agent action produces an auditable message
2. Messages are typed, validated, and logged before processing
3. No agent communicates through vague shared memory
4. Risk rejections are immutable
5. Live trade approvals include the complete upstream chain
6. Agents may recommend but not execute (unless designated executor)

## Message Schema

```json
{
  "message_id": "uuid",
  "timestamp": "ISO-8601",
  "source_agent": "AgentName",
  "target_agent": "AgentName | null (broadcast)",
  "message_type": "AgentMessageType",
  "priority": "CRITICAL|HIGH|NORMAL|LOW",
  "instrument": "symbol or null",
  "instrument_type": "InstrumentType or null",
  "regime_context": "MarketRegime or null",
  "payload": {},
  "confidence": "0.0-1.0 or null",
  "hard_constraints_observed": ["list of constraints"],
  "requested_action": "string or null",
  "final_status": "string",
  "rejection_reasons": ["list"],
  "parent_message_id": "uuid or null"
}
```

## Message Types

| Type | Direction | Priority |
|------|-----------|----------|
| MARKET_SCAN_RESULT | Sentinel → All | NORMAL |
| INSTRUMENT_PRIORITY_UPDATE | Selection → All | NORMAL |
| REGIME_CLASSIFICATION | Regime → All | NORMAL |
| SIGNAL_CANDIDATE | Engine → Validator | NORMAL |
| SIGNAL_VALIDATION_APPROVED | Validator → Risk | NORMAL |
| SIGNAL_VALIDATION_REJECTED | Validator → Ledger | HIGH |
| RISK_APPROVED | Risk → Executor | HIGH |
| RISK_REJECTED | Risk → Ledger | HIGH |
| EXECUTION_READY | Executor → Engine | HIGH |
| EXECUTION_BLOCKED | Executor → Ledger | HIGH |
| ORDER_SUBMITTED | Executor → All | HIGH |
| ORDER_FILLED | Executor → Reporting | HIGH |
| ORDER_FAILED | Executor → Risk | CRITICAL |
| PROTECTION_ORDER_CONFIRMED | Executor → Reporting | HIGH |
| ANOMALY_DETECTED | Any → Risk | HIGH |
| KILL_SWITCH_TRIGGERED | Risk/System → All | CRITICAL |
| OPTIMIZATION_PROPOSAL | Optimizer → Review | LOW |
| OPTIMIZATION_REJECTED | Review → Optimizer | NORMAL |
| REPORT_READY | Reporting → All | LOW |
| DAILY_SUMMARY | Reporting → All | LOW |
| AGENT_HEALTH_UPDATE | Any → Orchestrator | LOW |
| SYSTEM_STATE_BROADCAST | Orchestrator → All | NORMAL |

## Priority Rules

1. CRITICAL messages are processed before all others
2. Risk messages (RISK_REJECTED) override optimization messages
3. EXECUTION_BLOCKED overrides SIGNAL_VALIDATION_APPROVED
4. Broadcast messages (target=null) are for state sync only, not approval bypass

## Kill Switch Protocol

When triggered:
1. RiskGuardian emits KILL_SWITCH_TRIGGERED (CRITICAL)
2. Orchestrator immediately halts cycle
3. All pending signals are cancelled
4. Message logged to ledger permanently
5. Dashboard shows kill switch banner
6. Telegram alert sent (if configured)
7. Only manual `/api/control/resume` can re-enable

## Trade Decision Chain Requirements

For a trade to be executed, the following messages MUST exist in the ledger:

```
MARKET_SCAN_RESULT (status=TRADEABLE) for instrument
REGIME_CLASSIFICATION (eligible_strategies includes TJR)
SIGNAL_CANDIDATE (setup found)
SIGNAL_VALIDATION_APPROVED
RISK_APPROVED
ORDER_FILLED (execution confirmed)
```

Missing any link → trade cannot be explained → system flags as anomaly.

## AgentDecisionLedger

- Location: `logs/agent_decision_ledger.jsonl`
- Format: One JSON object per line (JSONL)
- Append-only: entries never deleted or modified
- Thread-safe: concurrent writes protected by mutex
- Searchable by: signal_id, message_type, agent_name, instrument

## Cost Control

- AI model calls tracked per agent per day
- Local models preferred where possible
- Optimization agent has daily call budget
- Reporting agent uses local computation only

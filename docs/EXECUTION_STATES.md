# Execution State Machine
## Apex Multi-Market TJR Engine v2.0

---

## States

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXECUTION STATE MACHINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐     broker.connect()     ┌──────────────────┐  │
│  │ PAPER_MODE  │ ─────────────────────→   │ LIVE_CONNECTED_  │  │
│  │  (default)  │ ←─────────────────────   │    SAFE          │  │
│  └─────────────┘     reset_to_paper()     └──────────────────┘  │
│         ↑                                        │    ↑          │
│         │                                        │    │          │
│         │                         arm_live()     │    │ disarm() │
│         │                                        ↓    │          │
│         │                                ┌──────────────────┐   │
│         │                                │  LIVE_ENABLED    │   │
│         │                                │  (real orders)   │   │
│         │                                └──────────────────┘   │
│         │                                        │               │
│         │                                preflight fails         │
│         │                                        ↓               │
│         │              resolve issue     ┌──────────────────┐   │
│         │ ←─────────────────────────     │  LIVE_BLOCKED    │   │
│         │                                └──────────────────┘   │
│         │                                                        │
│         │         ANY STATE   ──kill_switch()──→                 │
│         │                                                        │
│         │                              ┌──────────────────────┐ │
│         └──────── reset_to_paper() ──  │ KILL_SWITCH_ENGAGED  │ │
│                                        │   (irreversible)     │ │
│                                        └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## State Descriptions

### `PAPER_MODE` (default)
- **Orders**: All routed to paper broker (in-memory fill simulation)
- **Broker**: Not connected
- **Risk**: All rules enforced
- **Entry**: Default state on startup / after reset

### `LIVE_CONNECTED_SAFE`
- **Orders**: Blocked at broker level (safe lock)
- **Broker**: Authenticated, reading data only
- **Account/positions/balances**: Live from broker
- **Preflight**: Must pass before arming
- **Entry**: After successful `POST /api/broker/connect`

### `LIVE_ENABLED` ⚡
- **Orders**: Real orders submitted to live broker
- **Broker**: Fully connected and armed
- **Requirement**: Two-step: connect → arm (with `acknowledge_risk=true`)
- **Entry**: After explicit `POST /api/execution/arm-live`
- **Exit**: Via disarm, risk event, or kill switch

### `LIVE_BLOCKED`
- **Orders**: Blocked (neither paper nor live)
- **Reason**: Preflight failure or automatic risk block
- **Recovery**: Fix underlying issue → reconnect → re-preflight

### `KILL_SWITCH_ENGAGED` 🔴
- **Orders**: All blocked, immediately halted
- **Recovery**: Only via `POST /api/control/resume` → resets to PAPER_MODE
- **Note**: Irreversible until explicit operator action

---

## Allowed Transitions

| From | To | Trigger |
|------|----|---------| 
| PAPER_MODE | LIVE_CONNECTED_SAFE | broker.connect() + auth success |
| PAPER_MODE | KILL_SWITCH_ENGAGED | kill switch |
| LIVE_CONNECTED_SAFE | LIVE_ENABLED | arm-live (with preflight + ack) |
| LIVE_CONNECTED_SAFE | LIVE_BLOCKED | preflight failure |
| LIVE_CONNECTED_SAFE | PAPER_MODE | reset-to-paper |
| LIVE_CONNECTED_SAFE | KILL_SWITCH_ENGAGED | kill switch |
| LIVE_ENABLED | LIVE_CONNECTED_SAFE | disarm |
| LIVE_ENABLED | LIVE_BLOCKED | auto-block (risk/recon) |
| LIVE_ENABLED | KILL_SWITCH_ENGAGED | kill switch |
| LIVE_BLOCKED | PAPER_MODE | reset |
| LIVE_BLOCKED | LIVE_CONNECTED_SAFE | re-preflight |
| LIVE_BLOCKED | KILL_SWITCH_ENGAGED | kill switch |
| KILL_SWITCH_ENGAGED | PAPER_MODE | explicit reset |

---

## API Endpoints

| Endpoint | Method | Action |
|----------|--------|--------|
| `/api/broker/connect` | POST | Connect broker → LIVE_CONNECTED_SAFE |
| `/api/broker/status` | GET | Current connection status |
| `/api/broker/account` | GET | Live account info |
| `/api/broker/balances` | GET | Live balances |
| `/api/broker/positions` | GET | Live positions |
| `/api/broker/open-orders` | GET | Live pending orders |
| `/api/broker/preflight` | POST | Run all preflight checks |
| `/api/broker/test-order-payload` | POST | Build order JSON (no submit) |
| `/api/execution/arm-live` | POST | Step 2: arm live trading |
| `/api/execution/disarm-live` | POST | Disarm → LIVE_CONNECTED_SAFE |
| `/api/execution/set-mode` | POST | Force paper mode |
| `/api/execution/state` | GET | Full state snapshot |
| `/api/execution/history` | GET | Transition history |
| `/api/control/kill` | POST | Engage kill switch |
| `/api/control/resume` | POST | Reset to PAPER_MODE |
| `/api/control/reset-to-paper` | POST | Alias for resume |
| `/api/control/rejection-history` | GET | Order rejection log |
| `/api/reconcile/run` | POST | Manual reconciliation |
| `/api/reconcile/history` | GET | Reconciliation history |

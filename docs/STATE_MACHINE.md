# Stream State Machine

This document defines the authoritative state machine for StreamPay streams. The stop, pause, settle, and cancel routes use the transition table in `app/lib/state-machine.ts` so invalid and idempotent actions behave consistently.

## Transition Matrix

| Current State | Start | Pause | Stop | Settle | Withdraw | Cancel |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Draft** | Active | ❌ | Ended | ❌ | ❌ | ❌* |
| **Active** | (Idem) | Paused | Ended | Ended | ❌ | Cancelled |
| **Paused** | Active | (Idem) | Ended | Ended | ❌ | Cancelled |
| **Ended** | ❌ | ❌ | (Idem) | (Idem) | Withdrawn | ❌ |
| **Withdrawn** | ❌ | ❌ | ❌ | ❌ | (Idem) | ❌ |
| **Cancelled** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

*Legend:*
- **State Name**: Resulting state if the transition is legal.
- **(Idem)**: Idempotent transition (success with no state change).
- **❌**: Illegal transition (rejected with `409 Conflict`).
- `*` Draft streams retain the `STREAM_NOT_CANCELLABLE` response because no funds are escrowed.
- Cancelling an already cancelled stream is illegal to prevent a second refund.
- Idempotent stop, settle, and pause requests return the unchanged stream and do not repeat side effects.

## Dashboard Copy Mapping

These mappings ensure that the internal technical states align with the user-visible copy in the StreamPay dashboard.

| Internal State | Dashboard Badge | Description | Primary Action |
| :--- | :--- | :--- | :--- |
| `draft` | **Draft** | Ready to be launched. | **Start** |
| `active` | **Active** | Funds are flowing. | **Pause** |
| `paused` | **Paused** | Streaming is suspended. | **Resume** (Start) |
| `ended` | **Ended** | Stream finished; funds awaiting claim. | **Withdraw** |
| `withdrawn` | **Withdrawn** | Funds have been claimed by recipient. | None |
| `cancelled` | **Cancelled** | Escrow has been refunded and paid out. | None |

## Technical Implementation

The state machine is implemented in [`app/lib/state-machine.ts`](../app/lib/state-machine.ts). Its `allowedActions(status)` helper exposes the table's actions in table order, and `nextActionFor(status)` supplies the preferred client action from those allowed actions. Stop, pause, settle, and cancel routes call `transition()` directly. Start and batch routes remain outside this change.

### Error Handling
Illegal transitions return a standardized error model:
```json
{
  "error": {
    "code": "ILLEGAL_TRANSITION",
    "message": "Action 'pause' is illegal for a stream in 'draft' state.",
    "legacy_code": "INVALID_STREAM_STATE",
    "request_id": "..."
  }
}
```
The `legacy_code` field is included during the compatibility release for existing clients that match `INVALID_STREAM_STATE`.

# Architecture

## Boundaries

`time-strike` does one thing: converts deadlines into deterministic temporal control signals. It has no network client, shell access, repository access, LLM, database or web UI.

```text
MCP stdio (rmcp)
  -> five thin tool adapters
  -> TaskManager (RwLock<HashMap>)
  -> pure evaluate_time_policy()
  -> monotonic Clock (Instant)
  -> optional SnapshotStore (atomic JSON)

Trusted host/harness
  -> ActionLeaseLedger (Mutex-protected ephemeral authority)
```

## Time sources

- Runtime: `Instant` through `MonotonicClock`.
- Tests: deterministic `ManualClock`.
- Persistence: wall-clock milliseconds only to charge process downtime.
- Recovery: `base_elapsed + new Instant`; a restart cannot reset elapsed time.

## Policy

`evaluate_time_policy` is pure, deterministic and O(1). Inputs are total, elapsed, optional progress/ETA and optional reserve overrides. Outputs are mode, schedule, usable work, reserves, next check, maximum new action and stop/convergence flags.

The required temporal mode/schedule is automatic. The older core `Mode` and `SchedulePhase` types describe internal work-chunk profiles used by embedding tests; they are not the MCP temporal contract and are intentionally not client-configurable.

Finalization has two concepts:

- accounting reserve: reported validation + finalization seconds;
- preventive trigger: may enter `finalize` earlier (up to 10%/5 minutes) to protect delivery.

## Children

A child budget is clamped to the parent's currently available work budget. The parent's adaptive reserve is calculated from its total remaining time before child reservations are subtracted, so successive children cannot consume validation or finalization time. Active child reservations are tracked on the parent and released when a child finishes. Parent completion rejects active children. The core retains an explicit force override for trusted host integrations, but the MCP transport rejects agent-requested force so an agent cannot detach unfinished children or discard their reservations.

## Action leases

The MCP adapter converts an action proposal already carried by `tick` into a bounded admission lease without adding another tool call. Its ceiling is the smaller of the policy's maximum new action and the next mandatory check. The response binds the lease to the normalized action, exact ETA and task; `one_shot=true` requires atomic consumption. The `task:tick` identifier is only a correlation id, never a capability that the host may trust by shape or value.

Before sending `tick`, the trusted host records a monotonic `tick_request_started`. It registers only a lease returned by that request on the current MCP connection, verifies the response task/action/ETA binding against the request, rejects every `lease_id` already seen without changing existing or active state, atomically supersedes any older unconsumed lease for the task and stores `expires_at = min(tick_request_started + expires_in_seconds, host_deadline)`. At consumption it compares the execution task as well as the action and ETA inside the same atomic operation, rejecting unknown, superseded, expired, previously consumed, concurrently consumed or rebound leases, and starts work only when the complete ETA still fits before `expires_at`. Duplicate delivery, retry or concurrent registration therefore fails closed and can never restore consumed authority. Reconnect invalidates the ledger. The expiry never restarts when the agent presents or uses a lease, and wall clock never enters execution. The server does not execute external work; lease calculation remains O(1) with no additional server lock or persistence operation.

The public Rust `ActionLeaseLedger` implements this host boundary without joining
it to `TaskManager` or the MCP adapter. Its mutex covers validation of active
authority, supersession and one-shot consumption. It stores only connection-local
ephemeral records; hosts still own dispatch, cancellation, privilege checks and
the absolute deadline.

## Persistence

`FileStore` holds an exclusive advisory lock for its lifetime, fails closed for a second writer, writes unique `0600` temporary files, flushes, atomically renames and syncs the parent directory where supported. A failed save rolls the in-memory mutation back. Writes occur on lifecycle operations, not through a polling watchdog. Snapshot v2 stores elapsed state and wall-clock save time.

## Concurrency and idle behavior

State uses short `RwLock` critical sections. No busy loop or polling thread exists; idle CPU is effectively zero. Deadlines are evaluated lazily on calls and during recovery.

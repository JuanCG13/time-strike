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

The MCP adapter converts an action proposal already carried by `tick` into a bounded admission lease without adding another tool call. Its ceiling is the smaller of the policy's maximum new action and the next mandatory check. The lease identifier combines the task id and monotonic tick sequence; it is a correlation id, not a secret capability. Expiry is relative, so no wall clock enters execution. The server does not execute or police external work: the trusted host/harness must accept only leases returned on its current MCP connection, invalidate them on reconnect, and stop or re-check when they expire. Lease calculation is O(1) and adds no lock or persistence operation.

## Persistence

`FileStore` holds an exclusive advisory lock for its lifetime, fails closed for a second writer, writes unique `0600` temporary files, flushes, atomically renames and syncs the parent directory where supported. A failed save rolls the in-memory mutation back. Writes occur on lifecycle operations, not through a polling watchdog. Snapshot v2 stores elapsed state and wall-clock save time.

## Concurrency and idle behavior

State uses short `RwLock` critical sections. No busy loop or polling thread exists; idle CPU is effectively zero. Deadlines are evaluated lazily on calls and during recovery.

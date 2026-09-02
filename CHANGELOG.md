# Changelog

All notable improvements to Time Strike are recorded here.

## [Unreleased]

### Added

- 2026-08-30: initial checkpoints can now submit two to eight bounded structured plan steps with per-step duration and observable completion criteria; Time Strike validates the shape, derives the authoritative ETA from their sum, and reports the accepted count while retaining legacy note plans.
- 2026-08-30: parent tasks now calculate their adaptive reserve before subtracting child reservations, preventing successive children from eroding validation and finalization time while preserving the existing API and O(1) allocation path.
- 2026-08-29: MCP `finish_task` now rejects agent-requested `force=true`, preventing an agent from detaching unfinished children or discarding their reservations; the core override remains available to trusted host integrations and the input field remains compatible.
- 2026-08-29: hosts can set `TIME_STRIKE_DEADLINE_UNIX_MS` before launching the MCP server; the wall deadline is converted once to an immutable monotonic limit that is enforced under the task lock during both creation and adjustment, rejects starts delayed past the limit, prevents authorized budget increases from bypassing it, and reports the active deadline authority.
- 2026-08-28: `tick` now reports actual elapsed time, budget-accounted elapsed time, overrun, and deadline compliance separately; actual elapsed time also survives persistence without changing the existing `TaskView` layout or its live/finish elapsed semantics, and legacy v2 snapshots remain recoverable.

## [0.2.2] - 2026-09-02

### Added

- Added a reusable Rust `ActionLeaseLedger` for host/harness enforcement. It validates the exact task, normalized action, ETA and request anchor, atomically rejects duplicate registration and concurrent consumption, supersedes stale authority, prevents replay, and clamps execution to the host's monotonic hard deadline. This moves enforcement from a smoke-test reference into a production API without adding an MCP call, network dependency or persistence write.

## [0.2.1] - 2026-09-01

### Added

- `tick` can now issue a bounded, task-and-action-bound, one-shot lease without adding another MCP call. The host anchors expiry to the monotonic tick-request start, rejects task/action/ETA rebinding, replay, duplicate or concurrent registration and concurrent use without restoring consumed authority, and clamps consumption to its hard deadline. Invalid proposals fail before mutation and legacy clients retain the advisory response.

## [0.2.0] - 2026-08-27

### Added

- Mandatory compact execution plan through the first `checkpoint`.
- A single `directive` field with explicit actions: plan, execute, split, converge, validate, finalize, or stop.
- Automatic planning budget and planning-time reporting.
- Persistent checkpoint ETA and `plan_submitted` state.
- Explicit `replan` support for legitimate progress or ETA resets.
- Real overrun metrics in `finish_task`.

### Changed

- Oversized actions now return `split_action` using `current_action_estimated_seconds`.
- Budget increases are disabled by default unless `TIME_STRIKE_ALLOW_BUDGET_INCREASE=1` is set by the host.
- Validation and finalization reserves now have minimums of 5% and 3%.
- Progress cannot decrease unless `replan=true`.
- `tick` no longer persists the complete task snapshot on every call.
- `next_check_seconds` never exceeds the remaining budget.
- MCP server metadata and instructions now identify Time Strike `0.2.0`.

### Compatibility

- The original five MCP tools remain unchanged: `start_task`, `tick`, `checkpoint`, `adjust_task`, and `finish_task`.
- New protocol fields are additive, and persisted state uses Serde defaults for recovery compatibility.

## [0.1.0] - 2026-08-22

- Initial deterministic MCP time-budget controller.
- Monotonic task timing, checkpoints, adjustable budgets, parent-child reservations, persistence, and deadline policy.

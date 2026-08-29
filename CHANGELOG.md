# Changelog

All notable improvements to Time Strike are recorded here.

## [Unreleased]

### Added

- 2026-08-29: MCP `finish_task` now rejects agent-requested `force=true`, preventing an agent from detaching unfinished children or discarding their reservations; the core override remains available to trusted host integrations and the input field remains compatible.
- 2026-08-29: hosts can set `TIME_STRIKE_DEADLINE_UNIX_MS` before launching the MCP server; the wall deadline is converted once to an immutable monotonic limit that is enforced under the task lock during both creation and adjustment, rejects starts delayed past the limit, prevents authorized budget increases from bypassing it, and reports the active deadline authority.
- 2026-08-28: `tick` now reports actual elapsed time, budget-accounted elapsed time, overrun, and deadline compliance separately; actual elapsed time also survives persistence without changing the existing `TaskView` layout or its live/finish elapsed semantics, and legacy v2 snapshots remain recoverable.

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

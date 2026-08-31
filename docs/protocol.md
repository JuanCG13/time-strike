# MCP protocol

Transport: stdio, newline-delimited UTF-8 JSON-RPC. stdout contains MCP messages only; diagnostics go to stderr.

## Tools

### start_task

Inputs: `budget_seconds`; optional `objective`, `task_id`, `parent_task_id`, reserve percentages and `verbose`.

Returns: task id, remaining seconds, initial mode, next check and child clamp state.

### tick

Inputs are all optional: `task_id`, progress, ETA, current action and current-action ETA. Omitting `task_id` uses the most recently started active task.

Returns current elapsed/remaining, mode, schedule, reserves, usable work, `max_new_action_seconds`, `next_check_seconds`, action fit and `must_*` flags. When both current action and a positive finite ETA are supplied, the ETA must fit within `action_lease_ceiling_seconds`, the smaller of the policy action limit and the next mandatory check. A fitting proposal receives an action-bound, task-bound, one-shot `action_lease`; `expires_in_seconds` is anchored to monotonic time captured by the host before sending the tick request and is clamped again by the host deadline. The host must keep a connection-local issuance ledger and atomically reject unknown ids, mismatched action/ETA, replay, concurrent use, superseded or expired leases. Rejected or incomplete proposals receive no lease. Invalid ETAs and blank or oversized action labels fail before the tick mutates state. Clients that omit action details retain the legacy advisory response. `actual_elapsed_seconds` preserves monotonic runtime, while `accounted_elapsed_seconds` is capped at the budget; `overrun_seconds` and `deadline_met` make deadline compliance explicit. The legacy `elapsed_seconds` remains an alias of accounted elapsed time. In the Rust API, the original `TaskView` layout and semantics remain unchanged; callers that need the split use `tick_with_timing` or `task_timing`. History/objective/checkpoint text is never returned.

### checkpoint

Stores bounded progress and a compact note (or up to four truncated completed items). The initial plan can be supplied as `plan_steps`, an array of two to eight objects with `action`, `estimated_seconds`, and `done_when`. Structured steps are validated without truncation, persisted as a compact deterministic summary, and their summed estimates become the authoritative plan ETA. The legacy `note` plan remains accepted for compatibility. Returns current pressure and the accepted `plan_step_count` without checkpoint history.

### adjust_task

Requires exactly one of `add_seconds`, `remove_seconds`, `set_total_seconds`. Elapsed time is preserved and child budgets remain parent-clamped.

### finish_task

Returns elapsed, total, unused, used percentage, checkpoint count and deadline result.
`force=true` is retained in the input schema for compatibility but is rejected over MCP; forced parent completion is a host-only core privilege.

## Errors

Invalid budgets, missing tasks, parent violations, active-child completion, agent-requested force, invalid progress and ambiguous adjustments return MCP tool errors. Externally visible remaining time is never negative.

## Compact/verbose

Compact is the default. `verbose=true` (or `[output] compact=false`) adds short deterministic reason codes; it never emits history or prose reasoning.

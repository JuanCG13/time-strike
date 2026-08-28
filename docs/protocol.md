# MCP protocol

Transport: stdio, newline-delimited UTF-8 JSON-RPC. stdout contains MCP messages only; diagnostics go to stderr.

## Tools

### start_task

Inputs: `budget_seconds`; optional `objective`, `task_id`, `parent_task_id`, reserve percentages and `verbose`.

Returns: task id, remaining seconds, initial mode, next check and child clamp state.

### tick

Inputs are all optional: `task_id`, progress, ETA, current action and current-action ETA. Omitting `task_id` uses the most recently started active task.

Returns current elapsed/remaining, mode, schedule, reserves, usable work, `max_new_action_seconds`, `next_check_seconds`, action fit and `must_*` flags. `actual_elapsed_seconds` preserves monotonic runtime, while `accounted_elapsed_seconds` is capped at the budget; `overrun_seconds` and `deadline_met` make deadline compliance explicit. The legacy `elapsed_seconds` remains an alias of accounted elapsed time. History/objective/checkpoint text is never returned.

### checkpoint

Stores bounded progress and a compact note (or up to four truncated completed items). Returns current pressure without checkpoint history.

### adjust_task

Requires exactly one of `add_seconds`, `remove_seconds`, `set_total_seconds`. Elapsed time is preserved and child budgets remain parent-clamped.

### finish_task

Returns elapsed, total, unused, used percentage, checkpoint count and deadline result.

## Errors

Invalid budgets, missing tasks, parent violations, active-child completion, invalid progress and ambiguous adjustments return MCP tool errors. Externally visible remaining time is never negative.

## Compact/verbose

Compact is the default. `verbose=true` (or `[output] compact=false`) adds short deterministic reason codes; it never emits history or prose reasoning.

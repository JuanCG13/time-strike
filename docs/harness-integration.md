# Harness integration policy

Time Strike is an MCP stdio server. Register the absolute `time-strike` binary path in Claude, Codex, Hermes, or any generic MCP client as shown in the README.

## Minimal agent policy

```text
At task start, call start_task with the hard budget.
Call tick before and after expensive work, when next_check_seconds elapses,
before delegation, before validation, and before delivery.
Respect mode, schedule, max_new_action_seconds, must_converge,
must_validate, must_finalize, and must_stop.
Never start work materially longer than max_new_action_seconds.
Submit the first checkpoint with plan_complete=true and two to eight plan_steps.
Each step should contain one action, estimated_seconds, and an observable done_when.
Use later checkpoints only for meaningful progress or ETA changes.
Call finish_task before returning the final result.
```

## Host-owned absolute deadline

For strict deadline work, the harness should set `TIME_STRIKE_DEADLINE_UNIX_MS`
before launching Time Strike. The value is an unsigned Unix timestamp in
milliseconds established before the first model inference. `start_task` then
uses the smaller of the agent-requested budget and the remaining host time. A
late tool call therefore cannot reset the clock or manufacture a fresh budget.

Wall time is consulted only once at server initialization to convert the host's
absolute deadline into an immutable monotonic instant. The limit is applied
under the task lock when creation fixes `started_at` and whenever a budget is
adjusted, including when `TIME_STRIKE_ALLOW_BUDGET_INCREASE=1`. Lock contention,
wall-clock rollback, and later adjustments therefore cannot move the effective
end past the host deadline. An elapsed deadline rejects task creation without
persisting it; an invalid value prevents server startup.

```bash
TIME_STRIKE_DEADLINE_UNIX_MS=1787994000000 /absolute/path/to/time-strike
```

The `start_task` result reports `deadline_authority` as `host_absolute` when
this guard is active, or `agent_relative` in compatibility mode. The harness
must still enforce inference and tool timeouts externally; the MCP server does
not acquire those privileges.

## Subagents

Call `tick` before delegation. Start each child with `parent_task_id`; Time Strike clamps the child request to parent availability. Preserve parent time for integration, validation, and finalization. Concurrent agents must share one long-lived MCP server process to share task state.

See the README for verified Claude Code, Codex CLI, Hermes Agent, Claude Desktop, and generic JSON-RPC examples.

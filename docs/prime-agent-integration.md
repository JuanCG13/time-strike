# Prime Agent integration policy

Add the server:

```bash
prime-agent mcp add prime-time --cwd /ABS/prime-time-mcp -- /ABS/prime-time-mcp/target/release/prime-time-mcp
```

Minimal agent policy:

```text
At task start, call start_task with the user's hard budget.
Before and after expensive work, when next_check_seconds elapses,
before delegation, before validation, and before delivery, call tick.

Respect mode, schedule, max_new_action_seconds, must_converge,
must_validate, must_finalize, and must_stop.

Never start work whose estimate materially exceeds max_new_action_seconds.
If must_finalize=true, stop exploration and prepare delivery.
If must_stop=true, do not start additional work.
Call checkpoint only for meaningful progress/ETA changes.
Call finish_task before returning the final result.
```

## Subagents

Call `tick` before delegation. Start each child with `parent_task_id`; the server clamps the request to parent availability. Preserve parent time for integration, validation and finalization. Concurrent agents must share the same long-lived MCP server process to share state.

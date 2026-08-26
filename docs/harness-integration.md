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
Use checkpoint only for meaningful progress or ETA changes.
Call finish_task before returning the final result.
```

## Subagents

Call `tick` before delegation. Start each child with `parent_task_id`; Time Strike clamps the child request to parent availability. Preserve parent time for integration, validation, and finalization. Concurrent agents must share one long-lived MCP server process to share task state.

See the README for verified Claude Code, Codex CLI, Hermes Agent, Claude Desktop, and generic JSON-RPC examples.

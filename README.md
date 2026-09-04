# Time Strike

Time Strike is a small, native Rust [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that turns a human deadline into compact, deterministic control signals for an autonomous agent. It helps an agent decide how much work to start, when to validate, when to converge, and when to stop.

Time Strike is deliberately local-first: it does not run an LLM, make network requests, inspect repositories, execute shell commands, schedule calendar events, or send messages. It exposes five MCP tools over stdio and keeps the policy engine independent from MCP so it can be tested and embedded predictably.

## Latest changes

**v0.2.3:** Rust hosts can atomically revoke all action-lease authority for one task. Revocation is idempotent, blocks pending leases and prevents delayed responses from restoring authority without disrupting other tasks on the same connection. See the [changelog](CHANGELOG.md#023---2026-09-04).

## Features

- Monotonic runtime accounting using `std::time::Instant`.
- Deterministic O(1) `tick` policy decisions.
- Adaptive validation and finalization reserves.
- Automatic modes: `explore`, `execute`, `converge`, `validate`, `finalize`, `emergency`, and `expired`.
- Automatic schedule pressure: `ahead`, `on_track`, `behind`, and `critical`.
- Action caps, next-check intervals, and `must_*` control flags.
- Reusable host-side enforcement for one-shot action leases.
- Parent/child budget clamping with thread-safe task state.
- Optional atomic JSON persistence; downtime is charged during recovery.
- Compatible with Claude Desktop, Claude Code, Codex, Hermes Agent, and generic MCP stdio clients.

## See it in action: a five-minute chat task

Imagine asking an AI coding agent for a useful result under a hard deadline:

> **You:** Review this pull request and give me the three highest-risk findings. You have five minutes.
>
> **Agent:** I’ll time-box the review and reserve time to validate the findings.
>
> `Time Strike · start_task(budget_seconds: 300)`
>
> **Agent:** I inspected the changed authentication and database paths. Time Strike says I’m on schedule, with 168 seconds available for new work.
>
> `Time Strike · tick() → mode: converge, must_converge: true`
>
> **Agent:** I’ll stop exploring unrelated files, confirm the three strongest findings, and prepare the answer.
>
> `Time Strike · tick() → mode: validate, must_validate: true`
>
> **Agent:** Validation complete. Here are the three risks, each with file evidence and a recommended fix.
>
> `Time Strike · finish_task() → deadline_met: true`

Without Time Strike, the agent only knows that you mentioned “five minutes.” With Time Strike, the harness receives concrete signals about remaining time, action size, convergence, validation, and when to stop. Time Strike does not perform the review itself—it keeps the agent’s workflow aligned with your deadline.

## Architecture

```text
MCP stdio (rmcp)
  -> five thin tool adapters
  -> TaskManager (thread-safe registry)
  -> pure evaluate_time_policy() function
  -> monotonic Clock (Instant)
  -> optional SnapshotStore (atomic JSON)

Host/harness
  -> ActionLeaseLedger (atomic admission enforcement)
```

The core has no async or MCP-specific state. Production uses `MonotonicClock`; tests use `ManualClock`. Persistence uses wall-clock timestamps only to charge process downtime after recovery. See [`docs/architecture.md`](docs/architecture.md) and [`docs/protocol.md`](docs/protocol.md).

## Prerequisites

- Rust 1.97 or newer (with Cargo).
- An MCP-capable client or a generic stdio harness.
- No API key, database, network service, or environment secret is required.

## Build and install

Clone the repository, then build the release binary:

```bash
git clone https://github.com/JuanCG13/time-strike.git
cd time-strike
cargo build --release
```

The binary is:

```text
target/release/time-strike
```

For a local install, copy that binary to a directory on the client machine's `PATH`, or use its absolute path in the client configuration. MCP clients launch Time Strike as a child process over stdin/stdout; do not run it as an HTTP server. Logs go to stderr and stdout is reserved for MCP JSON-RPC.

### Optional configuration

No configuration file is required. `config.example.toml` documents the optional TOML settings:

```bash
TIME_STRIKE_CONFIG=/path/to/config.toml target/release/time-strike
TIME_STRIKE_STATE=/path/to/state.json target/release/time-strike
```

`TIME_STRIKE_STATE` enables persistence directly. Do not put secrets in the config file.

## MCP client configuration

Replace `/absolute/path/time-strike` below with the directory containing this checkout. Client configuration normally requires an absolute executable path; the placeholder is intentionally machine-neutral.

### Claude Desktop

Add the server to Claude Desktop's MCP configuration JSON (the exact file location depends on the operating system):

```json
{
  "mcpServers": {
    "time-strike": {
      "command": "/absolute/path/time-strike/target/release/time-strike",
      "args": []
    }
  }
}
```

Restart Claude Desktop after saving the file. The server should appear as `time-strike` with tools named `start_task`, `tick`, `checkpoint`, `adjust_task`, and `finish_task`.

### Claude Code

The current Claude Code CLI form is:

```bash
claude mcp add --scope user time-strike -- /absolute/path/time-strike/target/release/time-strike
claude mcp list
claude mcp get time-strike
```

The `--scope user` flag stores the server for the user. Omit it or choose another scope only when that is intentional. Reference: [Claude Code MCP documentation](https://docs.anthropic.com/en/docs/claude-code/mcp).

### Codex CLI

Add the stdio server with the verified CLI syntax:

```bash
codex mcp add time-strike -- /absolute/path/time-strike/target/release/time-strike
codex mcp list
```

Codex configuration can also be represented in TOML:

```toml
[mcp_servers.time-strike]
command = "/absolute/path/time-strike/target/release/time-strike"
args = []
```

The equivalent entry belongs in the Codex configuration file used by the local installation. Reference: [OpenAI Codex MCP documentation](https://developers.openai.com/codex/mcp). Codex receives the server's MCP `instructions`; the first 512 characters are self-contained so clients can act on the basic lifecycle without external context.

### Hermes Agent

Use Hermes Agent's stdio MCP registration:

```bash
hermes mcp add time_strike --command /absolute/path/time-strike/target/release/time-strike
hermes mcp list
hermes mcp test time_strike
```

Start a fresh Hermes session after changing MCP registrations. Hermes' [MCP Integration documentation](https://hermes-agent.nousresearch.com/docs) is the source of truth for the installed version. `hermes verify` HTTP readiness checks are not applicable because Time Strike is stdio-only.

### Generic JSON-RPC / MCP stdio harness

A generic harness should start:

```text
command: /absolute/path/time-strike/target/release/time-strike
args:    []
```

MCP stdio transports exchange one JSON-RPC message per newline. A minimal initialize request is:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"my-stdio-client","version":"1.0.0"}}}
```

After reading the initialize response, send the MCP notification and list tools:

```json
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
```

A harness may then call a tool with `tools/call`, for example:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"start_task","arguments":{"budget_seconds":900,"objective":"Finish the requested change"}}}
```

Read stdout as protocol data and stderr as diagnostics. Never merge the two streams.

## Tools and workflow

| Tool | Purpose |
|---|---|
| `start_task` | Start a total budget, optionally clamped under a parent task. |
| `tick` | Get the central compact policy decision before or after work. |
| `checkpoint` | Record progress/ETA and return updated temporal pressure. |
| `adjust_task` | Add, remove, or set total budget seconds. |
| `finish_task` | Finish the task and return deadline metrics. |

The normal lifecycle is:

```text
start_task → tick → checkpoint → finish_task
```

1. Call `start_task` once with `budget_seconds`.
2. Call `tick` before and after costly work, and whenever `next_check_seconds` elapses.
3. Before costly work, capture host monotonic time and call `tick` with `current_action` and `current_action_estimated_seconds`; the host may consume the returned `action_lease` exactly once only for that normalized action and ETA, before the request-anchored expiry and hard deadline.
4. Submit the initial `checkpoint` with two to eight structured `plan_steps` (`action`, `estimated_seconds`, and `done_when`), then checkpoint only after meaningful progress or ETA changes. The legacy compact `note` plan remains supported.
5. Obey `must_converge`, `must_validate`, `must_finalize`, and `must_stop`. These are control signals for the calling agent; Time Strike cannot force an LLM to call a tool.
6. Call `finish_task` before delivering the result. `adjust_task` is optional when scope changes.

Example compact `tick` result:

```json
{"remaining_seconds":417,"mode":"converge","schedule":"behind","max_new_action_seconds":95,"next_check_seconds":30,"action_lease_ceiling_seconds":30,"action_lease":{"lease_id":"review:7","task_id":"review","action":"Inspect one file","duration_seconds":20,"expires_in_seconds":30,"expiry_anchor":"tick_request_started","one_shot":true},"must_finalize":false,"must_stop":false}
```

## Troubleshooting

- **No tools appear:** verify the executable path, run `cargo build --release`, restart the client, and use the client's MCP list/get command.
- **The process exits immediately:** run the binary from a terminal and inspect stderr. Check that the executable has permission to run and that any `TIME_STRIKE_CONFIG` path is readable.
- **Protocol parse errors:** keep stdout untouched; do not pipe logs or banners into stdout. Use one newline-delimited JSON-RPC message per line.
- **No active task:** call `start_task` first, or pass an explicit `task_id` to subsequent tools.
- **Persistence lock errors:** only one long-lived Time Strike process should open a given state file. Use a different `TIME_STRIKE_STATE` path for an independent session.


## Security and privacy

Time Strike is local and intentionally narrow. It does not make network calls, load credentials, execute arbitrary commands, or read repositories. Objective, notes, and completed-item text can be stored in the optional local JSON snapshot, so avoid putting secrets, personal data, or access tokens in tool arguments. Protect a persistent state file with normal filesystem permissions and use a separate path per trust boundary. Client configuration files may contain executable paths and environment values; review them before sharing.

This is a control-signal server, not a security boundary. An MCP client or model can ignore `must_stop`; the harness remains responsible for enforcing its own policy and permissions. Report vulnerabilities privately according to [`SECURITY.md`](SECURITY.md).

## Benchmarks and tests

The core policy is designed to be O(1); the stdio path includes JSON-RPC and process I/O overhead. Run the reproducible checks locally:

```bash
cargo fmt --all -- --check
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

Optional smoke and measurement scripts require the release binary:

```bash
python tests/smoke_mcp.py
python tests/persistence_contention.py
cargo run --quiet --release --example core_stats
python tests/benchmark_tools.py
python tests/benchmark_transport.py
```

A prior local 10,000-operation baseline measured the core policy at approximately 1.48 µs mean and local MCP stdio `tick` at approximately 75.87 µs mean. Treat these as development-host reference numbers, not a performance guarantee; hardware, Rust, OS scheduling, and client framing affect results. Criterion benchmarks are in [`benches/tick.rs`](benches/tick.rs).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development workflow and quality gates.

## License

Time Strike is released under the [MIT License](LICENSE).

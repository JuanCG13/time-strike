# Contributing to Time Strike

Thanks for helping improve Time Strike.

## Development setup

1. Install Rust 1.97 or newer.
2. Fork and clone the repository.
3. Create a focused branch.
4. Keep changes deterministic, local-first, and compatible with MCP stdio.

## Required checks

Run before opening a pull request:

```bash
cargo fmt --all -- --check
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
python tests/smoke_mcp.py
```

Do not write logs to stdout: stdout is reserved for MCP JSON-RPC. Add regression tests for policy or protocol changes. Avoid dependencies or features that add network access, shell execution, credential handling, or repository inspection without an explicit design discussion.

## Pull requests

Explain the problem, the smallest solution, compatibility impact, and commands used for verification. Do not commit `target/`, `.evidence/`, state snapshots, logs, credentials, or personal data.

#!/usr/bin/env python3
import json
import os
import stat
import subprocess
import tempfile
from pathlib import Path

binary = Path(__file__).resolve().parents[1] / "target/release/time-strike"


def send(proc, message):
    assert proc.stdin is not None
    proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    proc.stdin.flush()


def request(proc, request_id, method, params):
    send(proc, {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
    assert proc.stdout is not None
    response = json.loads(proc.stdout.readline())
    if "error" in response:
        raise RuntimeError(response["error"])
    return response["result"]


with tempfile.TemporaryDirectory(prefix="time-strike-contention-") as directory:
    state = Path(directory) / "state.json"
    env = os.environ | {"TIME_STRIKE_STATE": str(state)}
    first = subprocess.Popen(
        [str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, env=env,
    )
    try:
        request(first, 1, "initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "contention", "version": "1"},
        })
        send(first, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        request(first, 2, "tools/call", {
            "name": "start_task", "arguments": {"task_id": "locked", "budget_seconds": 60},
        })
        if stat.S_IMODE(state.stat().st_mode) != 0o600:
            raise AssertionError(oct(stat.S_IMODE(state.stat().st_mode)))

        second = subprocess.run(
            [str(binary)], input="", capture_output=True, text=True, env=env, timeout=3,
        )
        if second.returncode == 0 or "state already has a writer" not in second.stderr:
            raise AssertionError({"code": second.returncode, "stderr": second.stderr})
        print(json.dumps({"second_writer_blocked": True, "state_mode": "0600"}))
    finally:
        first.terminate()
        first.wait(timeout=3)

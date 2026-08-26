#!/usr/bin/env python3
import json
import subprocess
import sys
from pathlib import Path

binary = Path(__file__).resolve().parents[1] / "target/release/time-strike"
proc = subprocess.Popen(
    [str(binary)],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
)


def send(message):
    proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    proc.stdin.flush()


def request(request_id, method, params=None):
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    send(message)
    while True:
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError(f"server closed stdout; stderr={proc.stderr.read()!r}")
        response = json.loads(line)
        if response.get("id") == request_id:
            if "error" in response:
                raise RuntimeError(f"{method}: {response['error']}")
            return response["result"]


try:
    initialized = request(
        1,
        "initialize",
        {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "time-strike-smoke", "version": "1.0"},
        },
    )
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    tools = request(2, "tools/list", {})["tools"]
    names = [tool["name"] for tool in tools]
    expected = ["adjust_task", "checkpoint", "finish_task", "start_task", "tick"]
    if sorted(names) != expected:
        raise AssertionError(f"unexpected tools: {names}")

    started = request(
        3,
        "tools/call",
        {
            "name": "start_task",
            "arguments": {
                "objective": "stdio smoke",
                "task_id": "smoke-task",
                "budget_seconds": 60,
            },
        },
    )
    if started.get("isError"):
        raise AssertionError(started)

    ticked = request(4, "tools/call", {"name": "tick", "arguments": {}})
    if ticked.get("isError"):
        raise AssertionError(ticked)

    finished = request(5, "tools/call", {"name": "finish_task", "arguments": {}})
    if finished.get("isError"):
        raise AssertionError(finished)

    print(
        json.dumps(
            {
                "protocol": initialized["protocolVersion"],
                "server": initialized["serverInfo"],
                "tools": names,
                "start": started.get("structuredContent"),
                "tick": ticked.get("structuredContent"),
                "finish": finished.get("structuredContent"),
            },
            separators=(",", ":"),
        )
    )
finally:
    if proc.stdin:
        proc.stdin.close()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=3)
    stderr = proc.stderr.read() if proc.stderr else ""
    if stderr:
        print(stderr, file=sys.stderr, end="")

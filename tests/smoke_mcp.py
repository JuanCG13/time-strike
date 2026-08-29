#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time
from pathlib import Path

binary = Path(__file__).resolve().parents[1] / "target/release/time-strike"
env = os.environ.copy()
env["TIME_STRIKE_DEADLINE_UNIX_MS"] = str(int(time.time() * 1000) + 30_000)
proc = subprocess.Popen(
    [str(binary)],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
    env=env,
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
    start_content = started.get("structuredContent") or {}
    assert start_content["directive"] == "submit_plan"
    assert start_content["mode"] == "plan"
    assert start_content["max_new_action_seconds"] == 0
    assert start_content["deadline_authority"] == "host_absolute"
    assert start_content["clamped"] is True
    assert 0 < start_content["remaining_seconds"] <= 30

    planned = request(
        4,
        "tools/call",
        {
            "name": "checkpoint",
            "arguments": {
                "plan_complete": True,
                "note": "Inspect protocol; apply minimal change; run targeted smoke; deliver result.",
                "estimated_remaining_work_seconds": 45,
                "progress_percent": 0,
            },
        },
    )
    if planned.get("isError"):
        raise AssertionError(planned)

    ticked = request(
        5,
        "tools/call",
        {
            "name": "tick",
            "arguments": {
                "current_action": "Run a deliberately oversized action",
                "current_action_estimated_seconds": 240,
            },
        },
    )
    if ticked.get("isError"):
        raise AssertionError(ticked)
    tick_content = ticked.get("structuredContent") or {}
    assert tick_content["directive"] == "split_action"
    assert tick_content["action_fits"] is False
    assert tick_content["must_plan"] is False
    assert tick_content["elapsed_seconds"] == tick_content["accounted_elapsed_seconds"]
    assert tick_content["actual_elapsed_seconds"] >= tick_content["accounted_elapsed_seconds"]
    assert tick_content["overrun_seconds"] == 0
    assert tick_content["deadline_met"] is True

    increase = request(
        6,
        "tools/call",
        {"name": "adjust_task", "arguments": {"add_seconds": 10}},
    )
    if not increase.get("isError"):
        raise AssertionError("budget increase unexpectedly succeeded")

    finished = request(7, "tools/call", {"name": "finish_task", "arguments": {}})
    if finished.get("isError"):
        raise AssertionError(finished)

    print(
        json.dumps(
            {
                "protocol": initialized["protocolVersion"],
                "server": initialized["serverInfo"],
                "tools": names,
                "start": start_content,
                "checkpoint": planned.get("structuredContent"),
                "tick": tick_content,
                "budget_increase_blocked": increase.get("isError"),
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

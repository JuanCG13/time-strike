#!/usr/bin/env python3
import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

binary = Path(__file__).resolve().parents[1] / "target/release/time-strike"


class ActionLeaseGuard:
    """Minimal host-side ledger for the documented one-shot lease contract."""

    def __init__(self, hard_deadline_monotonic):
        self.hard_deadline_monotonic = hard_deadline_monotonic
        self.lock = threading.Lock()
        self.leases = {}
        self.active_by_task = {}

    def register(self, request_started_monotonic, action, eta, lease):
        assert lease["expiry_anchor"] == "tick_request_started"
        assert lease["one_shot"] is True
        assert lease["action"] == action.strip()
        assert lease["duration_seconds"] == eta
        record = {
            "task_id": lease["task_id"],
            "action": action.strip(),
            "duration_seconds": eta,
            "expires_at": min(
                request_started_monotonic + lease["expires_in_seconds"],
                self.hard_deadline_monotonic,
            ),
            "consumed": False,
            "superseded": False,
        }
        with self.lock:
            previous = self.active_by_task.get(lease["task_id"])
            if previous in self.leases:
                self.leases[previous]["superseded"] = True
            self.leases[lease["lease_id"]] = record
            self.active_by_task[lease["task_id"]] = lease["lease_id"]

    def consume(self, lease_id, action, eta, now_monotonic):
        with self.lock:
            record = self.leases.get(lease_id)
            if record is None or record["consumed"] or record["superseded"]:
                return False
            if record["action"] != action.strip() or record["duration_seconds"] != eta:
                return False
            if now_monotonic + eta > record["expires_at"]:
                return False
            record["consumed"] = True
            return True


def verify_action_lease_enforcement(lease):
    request_started = 100.0

    mismatch = ActionLeaseGuard(200.0)
    mismatch.register(request_started, "Inspect one file", 1.0, lease)
    assert not mismatch.consume(lease["lease_id"], "Delete one file", 1.0, 100.0)
    assert not mismatch.consume(lease["lease_id"], "Inspect one file", 0.5, 100.0)
    assert not mismatch.consume("host-task:999", "Inspect one file", 1.0, 100.0)

    one_shot = ActionLeaseGuard(200.0)
    one_shot.register(request_started, "Inspect one file", 1.0, lease)
    barrier = threading.Barrier(3)

    def consume_concurrently():
        barrier.wait()
        return one_shot.consume(lease["lease_id"], "Inspect one file", 1.0, 100.0)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(consume_concurrently) for _ in range(2)]
        barrier.wait()
        assert sorted(future.result() for future in futures) == [False, True]
    assert not one_shot.consume(lease["lease_id"], "Inspect one file", 1.0, 100.0)

    superseded = ActionLeaseGuard(200.0)
    superseded.register(request_started, "Inspect one file", 1.0, lease)
    replacement = dict(lease, lease_id="host-task:3", action="Inspect two files")
    superseded.register(request_started, "Inspect two files", 1.0, replacement)
    assert not superseded.consume(
        lease["lease_id"], "Inspect one file", 1.0, request_started
    )
    assert superseded.consume(
        replacement["lease_id"], "Inspect two files", 1.0, request_started
    )

    delayed = ActionLeaseGuard(200.0)
    delayed.register(request_started, "Inspect one file", 1.0, lease)
    expires_at = request_started + lease["expires_in_seconds"]
    assert not delayed.consume(
        lease["lease_id"], "Inspect one file", 1.0, expires_at - 0.5
    )

    deadline_clamped = ActionLeaseGuard(100.5)
    deadline_clamped.register(request_started, "Inspect one file", 1.0, lease)
    assert not deadline_clamped.consume(
        lease["lease_id"], "Inspect one file", 1.0, request_started
    )


def clean_env():
    env = os.environ.copy()
    for key in (
        "TIME_STRIKE_DEADLINE_UNIX_MS",
        "TIME_STRIKE_ALLOW_BUDGET_INCREASE",
        "TIME_STRIKE_CONFIG",
        "TIME_STRIKE_STATE",
    ):
        env.pop(key, None)
    return env


class McpClient:
    def __init__(self, env):
        self.proc = subprocess.Popen(
            [str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, env=env,
        )

    def send(self, message):
        self.proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()

    def request(self, request_id, method, params=None):
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self.send(message)
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"server closed stdout; stderr={self.proc.stderr.read()!r}")
            response = json.loads(line)
            if response.get("id") == request_id:
                if "error" in response:
                    raise RuntimeError(f"{method}: {response['error']}")
                return response["result"]

    def initialize(self):
        initialized = self.request(1, "initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "time-strike-smoke", "version": "1.0"},
        })
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        names = sorted(tool["name"] for tool in self.request(2, "tools/list", {})["tools"])
        assert names == ["adjust_task", "checkpoint", "finish_task", "start_task", "tick"]
        return initialized

    def call(self, request_id, name, arguments):
        return self.request(request_id, "tools/call", {"name": name, "arguments": arguments})

    def close(self):
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            self.proc.wait(timeout=3)
        stderr = self.proc.stderr.read() if self.proc.stderr else ""
        if stderr:
            raise AssertionError(f"unexpected server stderr: {stderr}")


def start(client, request_id, task_id, budget=60):
    return client.call(request_id, "start_task", {
        "objective": "stdio smoke", "task_id": task_id, "budget_seconds": budget,
    })


def smoke_unset_deadline():
    client = McpClient(clean_env())
    try:
        initialized = client.initialize()
        started = start(client, 3, "relative-task")
        assert not started.get("isError"), started
        content = started.get("structuredContent") or {}
        assert content["deadline_authority"] == "agent_relative"
        assert content["clamped"] is False
        assert 59 < content["remaining_seconds"] <= 60
        client.call(4, "finish_task", {"task_id": "relative-task"})
        return initialized["protocolVersion"]
    finally:
        client.close()


def smoke_future_deadline_and_adjustments():
    env = clean_env()
    env["TIME_STRIKE_DEADLINE_UNIX_MS"] = str(int(time.time() * 1000) + 30_000)
    env["TIME_STRIKE_ALLOW_BUDGET_INCREASE"] = "1"
    client = McpClient(env)
    try:
        client.initialize()
        started = start(client, 3, "host-task")
        assert not started.get("isError"), started
        start_content = started.get("structuredContent") or {}
        assert start_content["directive"] == "submit_plan"
        assert start_content["mode"] == "plan"
        assert start_content["max_new_action_seconds"] == 0
        assert start_content["deadline_authority"] == "host_absolute"
        assert start_content["clamped"] is True
        assert 0 < start_content["remaining_seconds"] <= 30

        rejected = client.call(4, "checkpoint", {
            "task_id": "host-task", "plan_complete": True,
            "plan_steps": [
                {"action": "Do everything", "estimated_seconds": 45,
                 "done_when": "everything is done"},
            ],
            "progress_percent": 0,
        })
        assert rejected.get("isError") is True, rejected
        still_unplanned = client.call(5, "tick", {"task_id": "host-task"})
        assert (still_unplanned.get("structuredContent") or {})["must_plan"] is True

        planned = client.call(6, "checkpoint", {
            "task_id": "host-task", "plan_complete": True,
            "plan_steps": [
                {"action": "Inspect protocol", "estimated_seconds": 5,
                 "done_when": "the affected invariant is identified"},
                {"action": "Apply the minimal change", "estimated_seconds": 15,
                 "done_when": "the regression is fixed"},
                {"action": "Run targeted smoke", "estimated_seconds": 25,
                 "done_when": "all required checks pass"},
            ],
            "progress_percent": 0,
        })
        assert not planned.get("isError"), planned
        assert (planned.get("structuredContent") or {})["plan_step_count"] == 3
        invalid_action = client.call(7, "tick", {
            "task_id": "host-task", "current_action": "Invalid action",
            "current_action_estimated_seconds": -1,
        })
        assert invalid_action.get("isError") is True, invalid_action

        leased = client.call(8, "tick", {
            "task_id": "host-task", "current_action": "Inspect one file",
            "current_action_estimated_seconds": 1,
        })
        assert not leased.get("isError"), leased
        leased_content = leased.get("structuredContent") or {}
        assert leased_content["action_fits"] is True
        action_lease = leased_content["action_lease"]
        assert action_lease == {
            "lease_id": "host-task:2",
            "task_id": "host-task",
            "action": "Inspect one file",
            "duration_seconds": 1.0,
            "expires_in_seconds": action_lease["expires_in_seconds"],
            "expiry_anchor": "tick_request_started",
            "one_shot": True,
        }
        assert action_lease["expires_in_seconds"] >= action_lease["duration_seconds"]
        assert action_lease["expires_in_seconds"] <= leased_content[
            "action_lease_ceiling_seconds"
        ]
        assert leased_content["action_lease_ceiling_seconds"] - action_lease[
            "expires_in_seconds"
        ] <= 0.001
        verify_action_lease_enforcement(action_lease)

        ticked = client.call(9, "tick", {
            "task_id": "host-task", "current_action": "Run an oversized action",
            "current_action_estimated_seconds": 240,
        })
        assert not ticked.get("isError"), ticked
        tick_content = ticked.get("structuredContent") or {}
        assert tick_content["directive"] == "split_action"
        assert tick_content["action_fits"] is False
        assert "action_lease" not in tick_content
        assert tick_content["must_plan"] is False
        assert tick_content["elapsed_seconds"] == tick_content["accounted_elapsed_seconds"]
        assert tick_content["actual_elapsed_seconds"] >= tick_content["accounted_elapsed_seconds"]
        assert tick_content["overrun_seconds"] == 0
        assert tick_content["deadline_met"] is True

        added = client.call(10, "adjust_task", {"task_id": "host-task", "add_seconds": 60})
        assert not added.get("isError"), added
        added_content = added.get("structuredContent") or {}
        assert added_content["clamped"] is True
        assert added_content["total_budget_seconds"] <= 30
        set_total = client.call(11, "adjust_task", {
            "task_id": "host-task", "set_total_seconds": 120,
        })
        assert not set_total.get("isError"), set_total
        set_content = set_total.get("structuredContent") or {}
        assert set_content["clamped"] is True
        assert set_content["total_budget_seconds"] <= 30

        finished = client.call(12, "finish_task", {"task_id": "host-task"})
        assert not finished.get("isError"), finished
        return start_content["remaining_seconds"]
    finally:
        client.close()


def smoke_elapsed_deadline():
    env = clean_env()
    env["TIME_STRIKE_DEADLINE_UNIX_MS"] = str(int(time.time() * 1000) - 1_000)
    client = McpClient(env)
    try:
        client.initialize()
        started = start(client, 3, "late-task")
        assert started.get("isError") is True, started
        missing = client.call(4, "tick", {"task_id": "late-task"})
        assert missing.get("isError") is True, missing
    finally:
        client.close()


def smoke_agent_cannot_force_finish_parent():
    client = McpClient(clean_env())
    try:
        client.initialize()
        parent = start(client, 3, "force-parent")
        assert not parent.get("isError"), parent
        child = client.call(4, "start_task", {
            "objective": "active child", "task_id": "force-child",
            "parent_task_id": "force-parent", "budget_seconds": 10,
        })
        assert not child.get("isError"), child

        forced = client.call(5, "finish_task", {
            "task_id": "force-parent", "force": True,
        })
        assert forced.get("isError") is True, forced
        forced_text = json.dumps(forced)
        assert "host-only core privilege" in forced_text, forced

        normal_parent = client.call(6, "finish_task", {"task_id": "force-parent"})
        assert normal_parent.get("isError") is True, normal_parent
        assert "active children" in json.dumps(normal_parent), normal_parent

        child_finish = client.call(7, "finish_task", {"task_id": "force-child"})
        assert not child_finish.get("isError"), child_finish
        parent_finish = client.call(8, "finish_task", {"task_id": "force-parent"})
        assert not parent_finish.get("isError"), parent_finish
    finally:
        client.close()


def smoke_malformed_deadline():
    env = clean_env()
    env["TIME_STRIKE_DEADLINE_UNIX_MS"] = "not-a-timestamp"
    result = subprocess.run(
        [str(binary)], input="", text=True, capture_output=True,
        env=env, timeout=3, check=False,
    )
    assert result.returncode != 0
    assert "must be an unsigned Unix timestamp" in result.stderr


protocol = smoke_unset_deadline()
remaining = smoke_future_deadline_and_adjustments()
smoke_elapsed_deadline()
smoke_agent_cannot_force_finish_parent()
smoke_malformed_deadline()
print(json.dumps({
    "protocol": protocol,
    "cases": ["unset", "future", "elapsed", "host_only_force", "malformed"],
    "future_remaining_seconds": remaining,
    "budget_increases_capped": True,
    "agent_force_rejected": True,
}, separators=(",", ":")))

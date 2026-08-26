#!/usr/bin/env python3
import json
import statistics
import subprocess
import time
from pathlib import Path

OPERATIONS = 10_000
binary = Path(__file__).resolve().parents[1] / "target/release/time-strike"
proc = subprocess.Popen(
    [str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    stderr=subprocess.PIPE, text=True, bufsize=1,
)
assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
stdin, stdout, stderr = proc.stdin, proc.stdout, proc.stderr


def send(message):
    stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    stdin.flush()


def request(request_id, method, params=None):
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    send(message)
    while True:
        response = json.loads(stdout.readline())
        if response.get("id") == request_id:
            if "error" in response:
                raise RuntimeError(response["error"])
            return response["result"]


def percentile(sorted_values, value):
    index = round((len(sorted_values) - 1) * value)
    return sorted_values[index]


try:
    request(1, "initialize", {
        "protocolVersion": "2025-11-25", "capabilities": {},
        "clientInfo": {"name": "transport-benchmark", "version": "1"},
    })
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    request(2, "tools/call", {"name": "start_task", "arguments": {
        "task_id": "benchmark", "budget_seconds": 1_000_000,
    }})
    samples = []
    for index in range(OPERATIONS):
        started = time.perf_counter_ns()
        result = request(index + 3, "tools/call", {"name": "tick", "arguments": {}})
        samples.append(time.perf_counter_ns() - started)
        if result.get("isError"):
            raise RuntimeError(result)
    samples.sort()
    output = {
        "operations": OPERATIONS, "unit": "ns",
        "mean": round(statistics.fmean(samples), 2),
        "median": statistics.median(samples),
        "p95": percentile(samples, 0.95),
        "p99": percentile(samples, 0.99),
    }
    print(json.dumps(output, separators=(",", ":")))
finally:
    stdin.close()
    proc.wait(timeout=5)
    error_output = stderr.read()
    if error_output:
        raise RuntimeError(f"unexpected stderr: {error_output}")

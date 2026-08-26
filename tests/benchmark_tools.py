#!/usr/bin/env python3
import json, statistics, subprocess, time
from pathlib import Path
BINARY=Path(__file__).resolve().parents[1]/"target/release/time-strike"
p=subprocess.Popen([str(BINARY)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
assert p.stdin and p.stdout and p.stderr

def send(m): p.stdin.write(json.dumps(m,separators=(",",":"))+"\n"); p.stdin.flush()
def req(i,method,params):
    send({"jsonrpc":"2.0","id":i,"method":method,"params":params})
    while True:
        r=json.loads(p.stdout.readline())
        if r.get("id")==i:
            if "error" in r: raise RuntimeError(r["error"])
            if r["result"].get("isError"): raise RuntimeError(r["result"])
            return r["result"]
def stats(v):
    v.sort(); n=len(v)
    return {"operations":n,"unit":"ns","mean":round(statistics.fmean(v),2),"median":statistics.median(v),"p95":v[round((n-1)*.95)],"p99":v[round((n-1)*.99)]}
def timed(name,args,count,start_id):
    out=[]
    for j in range(count):
        before=time.perf_counter_ns(); req(start_id+j,"tools/call",{"name":name,"arguments":args(j)}); out.append(time.perf_counter_ns()-before)
    return stats(out),start_id+count
try:
    req(1,"initialize",{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"all-tools-bench","version":"1"}})
    send({"jsonrpc":"2.0","method":"notifications/initialized"}); i=2; report={}
    report["start_task"],i=timed("start_task",lambda j:{"task_id":f"s{j}","budget_seconds":1000},1000,i)
    req(i,"tools/call",{"name":"start_task","arguments":{"task_id":"main","budget_seconds":1000000}}); i+=1
    report["tick"],i=timed("tick",lambda j:{"task_id":"main"},10000,i)
    report["checkpoint"],i=timed("checkpoint",lambda j:{"task_id":"main","progress_percent":j%101},1000,i)
    report["adjust_task"],i=timed("adjust_task",lambda j:{"task_id":"main","add_seconds":0.001},1000,i)
    report["finish_task"],i=timed("finish_task",lambda j:{"task_id":f"s{j}"},1000,i)
    print(json.dumps(report,separators=(",",":")))
finally:
    p.stdin.close(); p.wait(timeout=5)
    err=p.stderr.read()
    if err: raise RuntimeError(err)

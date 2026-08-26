#!/usr/bin/env python3
import json, os, subprocess, time
from pathlib import Path
binary=Path(__file__).resolve().parents[1]/"target/release/time-strike"
p=subprocess.Popen([str(binary)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
try:
    time.sleep(.2)
    def sample():
        status=Path(f"/proc/{p.pid}/status").read_text()
        rss=int(next(x.split()[1] for x in status.splitlines() if x.startswith("VmRSS:")))
        stat=Path(f"/proc/{p.pid}/stat").read_text().split()
        return rss,int(stat[13])+int(stat[14])
    rss1,cpu1=sample(); started=time.monotonic(); time.sleep(1.0); rss2,cpu2=sample(); elapsed=time.monotonic()-started
    hz=os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    print(json.dumps({"idle_seconds":round(elapsed,3),"rss_kib":max(rss1,rss2),"cpu_seconds":(cpu2-cpu1)/hz},separators=(",",":")))
finally:
    p.terminate(); p.wait(timeout=3)

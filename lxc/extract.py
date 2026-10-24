#!/usr/bin/env python3
"""Pull the last N days of selected Prometheus metrics, one raw JSON file per day per metric."""
import json, os, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

BASE = Path(__file__).parent
PROM_URL = os.environ["PROM_URL"].rstrip("/")
DAYS, STEP = 15, 300
MAX_SKEW_S = 120

# name -> PromQL (aggregated at the source to keep series counts small)
QUERIES = {
    "cpu_busy_ratio": '1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))',
    "mem_available_bytes": "node_memory_MemAvailable_bytes",
    "mem_total_bytes": "node_memory_MemTotal_bytes",
    "load1": "node_load1",
    "temp_celsius": "node_thermal_zone_temp",
    "disk_avail_bytes": 'node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs"}',
    "disk_size_bytes": 'node_filesystem_size_bytes{fstype!~"tmpfs|overlay|squashfs"}',
    "net_rx_bytes_per_s": 'rate(node_network_receive_bytes_total{device!~"lo|veth.*|br-.*|docker0"}[5m])',
    "net_tx_bytes_per_s": 'rate(node_network_transmit_bytes_total{device!~"lo|veth.*|br-.*|docker0"}[5m])',
    "container_cpu_cores": 'rate(container_cpu_usage_seconds_total{name!=""}[5m])',
    "container_mem_bytes": 'container_memory_working_set_bytes{name!=""}',
}

def prom(path, **params):
    r = requests.get(f"{PROM_URL}{path}", params=params, timeout=60)
    r.raise_for_status()
    body = r.json()
    if body.get("status") != "success":
        raise RuntimeError(f"{path} failed: {body}")
    return body

def check_clock():
    server = datetime.fromisoformat(
        prom("/api/v1/status/runtimeinfo")["data"]["serverTime"].replace("Z", "+00:00")
    )
    skew = abs((datetime.now(timezone.utc) - server).total_seconds())
    if skew > MAX_SKEW_S:
        sys.exit(f"clock skew {skew:.0f}s vs Prometheus; refusing to run")

def day_chunks(now):
    first = (now - timedelta(days=DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    d = first
    while d < now:
        yield d, min(d + timedelta(days=1), now)
        d += timedelta(days=1)

def main():
    check_clock()
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%S")
    run_dir = BASE / "out" / f"run={run_id}"
    n_files = 0
    for start, end in day_chunks(now):
        day_dir = run_dir / f"day={start:%Y-%m-%d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        for name, expr in QUERIES.items():
            resp = prom("/api/v1/query_range", query=expr,
                        start=start.timestamp(), end=end.timestamp(), step=STEP)
            doc = {"run_id": run_id, "metric_name": name, "query": expr,
                   "chunk_start": start.isoformat(), "chunk_end": end.isoformat(),
                   "step_s": STEP, "response": resp}
            (day_dir / f"{name}.json").write_text(json.dumps(doc))
            n_files += 1
    # marker file written last: upload step only ships runs that have it
    (run_dir / "_COMPLETE").write_text(now.isoformat())
    print(f"run {run_id}: wrote {n_files} files to {run_dir}")

if __name__ == "__main__":
    main()

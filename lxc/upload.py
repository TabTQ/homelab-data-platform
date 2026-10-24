#!/usr/bin/env python3
"""Upload every COMPLETE run in out/ to the UC Volume, move it to sent/, then write the _COMPLETE marker."""
import io, shutil, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from databricks.sdk import WorkspaceClient  # reads DATABRICKS_HOST / DATABRICKS_TOKEN

BASE = Path(__file__).parent
VOLUME = "/Volumes/homelab/landing/prom_raw"

w = WorkspaceClient()

def upload_one(args):
    local, remote = args
    with open(local, "rb") as f:
        w.files.upload(remote, f, overwrite=True)

def main():
    runs = sorted(p for p in (BASE / "out").glob("run=*") if (p / "_COMPLETE").exists())
    if not runs:
        print("nothing to upload")
        return
    for run in runs:
        jobs = [(p, f"{VOLUME}/{run.name}/{p.relative_to(run)}") for p in run.rglob("*.json")]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(upload_one, jobs))   # raises on the first failure
        shutil.move(str(run), BASE / "sent" / run.name)
        # marker last: tells the Pi orchestrator the run is fully uploaded and safe to stop the LXC
        w.files.upload(f"{VOLUME}/{run.name}/_COMPLETE", io.BytesIO(b"ok"), overwrite=True)
        print(f"{run.name}: uploaded {len(jobs)} files")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.exit(f"upload failed, run stays in out/ for retry: {e}")

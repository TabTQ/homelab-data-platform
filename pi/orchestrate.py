#!/usr/bin/env python3
"""Cron every 4h. If Databricks data is stale: wake Proxmox, start the ETL LXC via the Hub,
wait until the new run is fully uploaded, then stop the LXC and (except on Sundays IST) the host."""
import fcntl, json, os, re, socket, subprocess, sys, time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

E = os.environ
HUB = E.get("HUB_URL", "http://localhost:3010").rstrip("/")
PVE_HOST, PVE_NODE, PVE_MAC = E["PVE_HOST"], E["PVE_NODE"], E["PVE_MAC"]
LXC_HOST, LXC_ID = E["LXC_HOST"], E.get("LXC_ID", "130")
DBX, TOKEN = E["DATABRICKS_HOST"].rstrip("/"), E["DATABRICKS_TOKEN"]
VOLUME_DIR = "/Volumes/homelab/landing/prom_raw"
STALE_S = int(E.get("STALE_DAYS", "10")) * 86400
COOLDOWN_S, HOST_WAIT_S, LXC_WAIT_S, DATA_WAIT_S, STOP_GRACE_S = 24 * 3600, 600, 300, 2 * 3600, 20
SHUTDOWN_HOST = E.get("SHUTDOWN_HOST", "1") == "1"
DRY_RUN = E.get("DRY_RUN", "1") == "1"
BACKUP_START, BACKUP_END = E.get("BACKUP_START", "04:25"), E.get("BACKUP_END", "05:15")  # IST, vzdump runs 04:30 (~3 min)
HERE = Path(__file__).parent
STATE = HERE / "state.json"


def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def sunday_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).weekday() == 6


def backup_window_wait_s(now=None):
    """Seconds to wait if `now` falls inside the daily PBS backup window (IST), else 0."""
    now = now or datetime.now(ZoneInfo("Asia/Kolkata"))
    def at(hhmm):
        h, m = map(int, hhmm.split(":"))
        return now.replace(hour=h, minute=m, second=0, microsecond=0)
    start, end = at(BACKUP_START), at(BACKUP_END)
    return (end - now).total_seconds() if start <= now < end else 0


def get(url, token=None):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"{}")


def list_names(path):
    """Names inside a Volume directory (Files API). Raises if the API fails."""
    names, page = [], None
    while True:
        data = get(f"{DBX}/api/2.0/fs/directories{path}" + (f"?page_token={page}" if page else ""), TOKEN)
        names += [c.get("name", "").rstrip("/") for c in data.get("contents", [])]
        page = data.get("next_page_token")
        if not page:
            return names


def runs():
    """{run_folder_name: epoch_seconds} for every run=YYYYMMDDTHHMMSS folder."""
    out = {}
    for n in list_names(VOLUME_DIR):
        m = re.fullmatch(r"run=(\d{8}T\d{6})", n)
        if m:
            out[n] = datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).timestamp()
    return out


def new_run_complete(since):
    """True once a run created after `since` has its _COMPLETE marker (upload fully finished)."""
    for name, t in runs().items():
        if t > since and "_COMPLETE" in list_names(f"{VOLUME_DIR}/{name}"):
            return True
    return False


def ping(ip):
    return subprocess.run(["ping", "-c", "1", "-W", "2", ip], capture_output=True).returncode == 0


def api_up():
    try:
        socket.create_connection((PVE_HOST, 8006), 3).close()
        return True
    except OSError:
        return False


def wait_for(pred, timeout, every=15):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception as e:
            log(f"transient error while waiting: {e}")
        time.sleep(every)
    return False


def hub(path):
    log(f"hub {path}: {get(HUB + path)}")


def main():
    lock = open(HERE / "lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return                                    # another cycle is still running

    try:
        known = runs()                            # preflight: Files API must work before touching anything
    except Exception as e:
        log(f"preflight failed, nothing touched: {e}")
        sys.exit(1)
    now = time.time()
    age = now - max(known.values()) if known else float("inf")
    if age < STALE_S:
        return                                    # fresh: stay silent
    st = json.loads(STATE.read_text()) if STATE.exists() else {}
    if now - st.get("last_attempt", 0) < COOLDOWN_S:
        return

    log(f"data is {age / 86400:.1f} days old")
    host_was_up = ping(PVE_HOST)
    if host_was_up and ping(LXC_HOST):
        log("LXC already running: leaving everything alone")
        return
    if DRY_RUN:
        log(f"DRY_RUN plan: {'host already up' if host_was_up else 'wake host and wait for ping'}, "
            f"toggle LXC {LXC_ID} on, wait for a new run with _COMPLETE, toggle LXC off"
            + (", shut host down unless Sunday IST" if SHUTDOWN_HOST and not host_was_up else ""))
        return

    STATE.write_text(json.dumps({"last_attempt": now}))
    if not host_was_up:
        hub(f"/wake/mac/{PVE_MAC}?target=proxmox")
        if not wait_for(lambda: ping(PVE_HOST), HOST_WAIT_S) or not wait_for(api_up, 180):
            log("ALERT: host did not come up in time; nothing else done")
            return
        time.sleep(30)                            # let the Proxmox API settle

    started = time.time()
    hub(f"/toggle/{PVE_NODE}/lxc/{LXC_ID}")       # one toggle only, never retried
    if not wait_for(lambda: ping(LXC_HOST), LXC_WAIT_S):
        log("ALERT: LXC did not come up; not toggling again, leaving host as is")
        return

    if not wait_for(lambda: new_run_complete(started), DATA_WAIT_S, every=30):
        log("ALERT: no complete upload within timeout; leaving everything up")
        return

    log("new run fully uploaded")
    time.sleep(STOP_GRACE_S)                      # the toggle is a hard stop: let the LXC settle first
    wait = backup_window_wait_s()
    if wait:                                      # never stop the LXC or host while the nightly backup may run
        log(f"inside backup window: waiting {wait / 60:.0f} min before stopping anything")
        time.sleep(wait)
    if ping(LXC_HOST):                            # only stop it if it is still up (toggle flips state)
        hub(f"/toggle/{PVE_NODE}/lxc/{LXC_ID}")
        wait_for(lambda: not ping(LXC_HOST), 120)
    if SHUTDOWN_HOST and not host_was_up:
        if sunday_ist():
            log("Sunday IST: leaving host up for backups")
        else:
            hub(f"/node/{PVE_NODE}/shutdown")


if __name__ == "__main__":
    main()

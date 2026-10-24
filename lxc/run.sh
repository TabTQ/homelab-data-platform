#!/usr/bin/env bash
set -euo pipefail
cd /opt/hl-etl
set -a; . ./.env; set +a
for i in $(seq 1 30); do curl -fs "$PROM_URL/-/ready" >/dev/null && break; sleep 10; done
./venv/bin/python extract.py
./venv/bin/python upload.py
find sent -maxdepth 1 -name 'run=*' -mtime +14 -exec rm -rf {} +

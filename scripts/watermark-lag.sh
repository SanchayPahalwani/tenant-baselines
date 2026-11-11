#!/usr/bin/env bash
# Print how far the running job's live watermark is behind the wall clock:
#
#   scripts/watermark-lag.sh [samples] [interval_s]     (defaults 60 and 5)
#
# Each sample reads the currentInputWatermark of every subtask of the minute aggregator from the
# Flink REST API and takes the lowest. The script prints the min, median and max of
# now - watermark in seconds over the samples, and the UTC weekday and time of the first sample.
# Flink serves metrics from a cache that a request refreshes at most every 10 s, so a sample can
# read a watermark up to 10 s old. Exits 1 with a message on stderr when there is no running job or the metric
# is missing.
set -euo pipefail

FLINK="${FLINK_URL:-http://localhost:8081}"
SAMPLES="${1:-60}"
INTERVAL_S="${2:-5}"
if ! [[ "$SAMPLES" =~ ^[1-9][0-9]*$ && "$INTERVAL_S" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 [samples] [interval_s]" >&2
  exit 2
fi

exec python3 - "$FLINK" "$SAMPLES" "$INTERVAL_S" << 'PY'
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

base = sys.argv[1]
samples = int(sys.argv[2])
interval_s = float(sys.argv[3])
# A request more than 10 s after the cache's last refresh starts a new one but is answered from
# the old cache, which can be minutes old. The script starts a refresh and waits this long before
# the first sample.
REFRESH_WAIT_S = 5


def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as r:
        return json.load(r)


def die(msg):
    print(f"watermark-lag: {msg}", file=sys.stderr)
    sys.exit(1)


try:
    jobs = [j for j in get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
    if not jobs:
        die("no running job")
    jid = jobs[0]["jid"]
    vertices = get(f"/jobs/{jid}")["vertices"]
    get(f"/jobs/{jid}/vertices/{vertices[0]['id']}/metrics")
    time.sleep(REFRESH_WAIT_S)
    ids = []
    for v in vertices:
        found = [m["id"] for m in get(f"/jobs/{jid}/vertices/{v['id']}/metrics")
                 if m["id"].endswith(".minute-aggregator.currentInputWatermark")]
        if found:
            vid, ids = v["id"], found
            break
    if not ids:
        die("no minute-aggregator.currentInputWatermark metric on any vertex")
    query = urllib.parse.quote(",".join(ids), safe=",")
    first = datetime.now(timezone.utc)
    lags = []
    for i in range(samples):
        if i > 0:
            time.sleep(interval_s)
        values = get(f"/jobs/{jid}/vertices/{vid}/metrics?get={query}")
        now_ms = time.time() * 1000
        got = {m["id"]: m["value"] for m in values}
        missing = [m for m in ids if m not in got]
        if missing:
            die(f"no value for {', '.join(missing)}")
        watermark_ms = min(int(float(got[m])) for m in ids)
        lags.append((now_ms - watermark_ms) / 1000)
    print(f"watermark lag over {samples} samples {interval_s:g} s apart:"
          f" min {min(lags):.1f} s, median {statistics.median(lags):.1f} s,"
          f" max {max(lags):.1f} s; from {first:%A %H:%M} UTC")
except SystemExit:
    raise
except Exception as e:
    die(f"cannot read the Flink REST API at {base}: {e}")
PY

#!/usr/bin/env bash
# Print how far the running job's live watermark is behind the wall clock:
#
#   scripts/watermark-lag.sh [samples] [interval_s]     (defaults 60 and 11)
#
# Each sample reads the currentInputWatermark of every subtask of the minute aggregator from the
# Flink REST API and takes the lowest. The script prints the min, median and max of
# now - watermark in seconds over the samples, and the UTC weekday and time of the first sample.
# Each sample is fresh: it starts a refresh of Flink's metric cache, waits for it and reads, and
# "now" is when it started the refresh (see flink_metrics.py). A refresh starts only when the
# cache is more than 10 s old, so samples are at least 11 s apart. Exits 1 with a message on
# stderr when there is no running job, the metric is missing or there is no watermark yet.
set -euo pipefail

FLINK="${FLINK_URL:-http://localhost:8081}"
SAMPLES="${1:-60}"
INTERVAL_S="${2:-11}"
if ! [[ "$SAMPLES" =~ ^[1-9][0-9]*$ && "$INTERVAL_S" =~ ^[0-9]+$ ]] || [ "$INTERVAL_S" -lt 11 ]; then
  echo "usage: $0 [samples] [interval_s], with interval_s at least 11" >&2
  exit 2
fi

exec python3 -B - "$(dirname "$0")" "$FLINK" "$SAMPLES" "$INTERVAL_S" << 'PY'
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, sys.argv[1])
from flink_metrics import run  # noqa: E402

samples = int(sys.argv[3])
interval_s = int(sys.argv[4])
# A lag this large means a subtask has no watermark yet (Long.MIN_VALUE).
NO_WATERMARK_S = 86_400


def body(fm):
    fm.find_job()
    sent = fm.refresh()
    vid, ids = fm.find_metric(".minute-aggregator.currentInputWatermark")
    first = datetime.fromtimestamp(sent, timezone.utc)
    lags = []
    for i in range(samples):
        if i > 0:
            time.sleep(max(0.0, sent + interval_s - time.time()))
            sent = fm.refresh()
        watermark_s = min(fm.values(vid, ids).values()) / 1000
        lag = sent - watermark_s
        if lag > NO_WATERMARK_S:
            fm.die("no watermark yet")
        lags.append(lag)
    print(f"watermark lag over {samples} samples {interval_s} s apart:"
          f" min {min(lags):.1f} s, median {statistics.median(lags):.1f} s,"
          f" max {max(lags):.1f} s; from {first:%A %H:%M} UTC")


run("watermark-lag", sys.argv[2], body)
PY

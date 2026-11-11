#!/usr/bin/env bash
# Print the running job's total lateEvents, summed over every subtask of the late-events
# operator, from the Flink REST API. Exits 1 with a message on stderr when there is no running
# job or the metric is missing. `smoke.sh` and `demo.sh` use it to check that the history was
# not dropped as late. The read is fresh: see flink_metrics.py.
set -euo pipefail

FLINK="${FLINK_URL:-http://localhost:8081}"

exec python3 -B - "$(dirname "$0")" "$FLINK" << 'PY'
import sys

sys.path.insert(0, sys.argv[1])
from flink_metrics import run  # noqa: E402


def body(fm):
    fm.find_job()
    fm.refresh()
    vid, ids = fm.find_metric(".late-events.lateEvents")
    print(sum(int(v) for v in fm.values(vid, ids).values()))


run("late-events", sys.argv[2], body)
PY

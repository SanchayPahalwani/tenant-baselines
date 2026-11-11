#!/usr/bin/env bash
# Print the running job's total lateEvents, summed over every subtask of the late-events
# operator, from the Flink REST API. Exits 1 with a message on stderr when there is no running
# job or the metric is missing. `smoke.sh` and `demo.sh` use it to check that the history was
# not dropped as late.
set -euo pipefail

FLINK="${FLINK_URL:-http://localhost:8081}"

exec python3 - "$FLINK" << 'PY'
import json
import sys
import time
import urllib.parse
import urllib.request

base = sys.argv[1]
# The REST API answers from a metric cache. A request more than 10 s after the last refresh
# starts a new one but is answered from the old cache, which can be minutes old or older than
# the job. The script starts a refresh and waits this long before it reads.
REFRESH_WAIT_S = 5


def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as r:
        return json.load(r)


def die(msg):
    print(f"late-events: {msg}", file=sys.stderr)
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
                 if m["id"].endswith(".late-events.lateEvents")]
        if found:
            vid, ids = v["id"], found
            break
    if not ids:
        die("no late-events.lateEvents metric on any vertex")
    query = urllib.parse.quote(",".join(ids), safe=",")
    values = get(f"/jobs/{jid}/vertices/{vid}/metrics?get={query}")
    got = {m["id"]: m["value"] for m in values}
    missing = [i for i in ids if i not in got]
    if missing:
        die(f"no value for {', '.join(missing)}")
    print(sum(int(float(got[i])) for i in ids))
except SystemExit:
    raise
except Exception as e:
    die(f"cannot read the Flink REST API at {base}: {e}")
PY

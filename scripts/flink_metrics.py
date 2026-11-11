"""Fresh reads of the running job's metrics from the Flink REST API.

`late-events.sh` and `watermark-lag.sh` use this with the host's python3.

The REST API answers from a metric cache. A request made more than 10 s after the cache's last
refresh (`metrics.fetcher.update-interval`) starts a new refresh, but it is answered from the old
cache, which can be minutes old or older than the job. `refresh` sends such a request and waits
for the refresh to land, so the read that follows is fresh. The refresh starts only if the cache
is more than 10 s old, so two fresh reads must be at least 11 s apart.
"""

import json
import sys
import time
import urllib.parse
import urllib.request

REFRESH_WAIT_S = 1
MIN_FRESH_INTERVAL_S = 11


class FlinkMetrics:
    def __init__(self, prog: str, base: str):
        self.prog = prog
        self.base = base
        self.jid = ""
        self.vertices: list[str] = []

    def die(self, msg: str):
        print(f"{self.prog}: {msg}", file=sys.stderr)
        sys.exit(1)

    def get(self, path: str):
        with urllib.request.urlopen(self.base + path, timeout=10) as r:
            return json.load(r)

    def find_job(self) -> None:
        """The running job and its vertex ids; exits 1 if there is none."""
        jobs = [j for j in self.get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        if not jobs:
            self.die("no running job")
        self.jid = jobs[0]["jid"]
        self.vertices = [v["id"] for v in self.get(f"/jobs/{self.jid}")["vertices"]]
        if not self.vertices:
            self.die(f"job {self.jid} has no vertices")

    def refresh(self) -> float:
        """Starts a cache refresh, if the cache is more than 10 s old, and waits for it.

        Returns the wall-clock time of the request, which is when the fresh values were read.
        """
        sent = time.time()
        self.get(f"/jobs/{self.jid}/vertices/{self.vertices[0]}/metrics")
        time.sleep(REFRESH_WAIT_S)
        return sent

    def find_metric(self, suffix: str) -> tuple[str, list[str]]:
        """(vertex id, metric ids) of the first vertex with metrics ending in suffix."""
        for vid in self.vertices:
            ids = [
                m["id"]
                for m in self.get(f"/jobs/{self.jid}/vertices/{vid}/metrics")
                if m["id"].endswith(suffix)
            ]
            if ids:
                return vid, ids
        self.die(f"no {suffix.lstrip('.')} metric on any vertex")

    def values(self, vid: str, ids: list[str]) -> dict[str, float]:
        query = urllib.parse.quote(",".join(ids), safe=",")
        got = {
            m["id"]: m["value"]
            for m in self.get(f"/jobs/{self.jid}/vertices/{vid}/metrics?get={query}")
        }
        missing = [i for i in ids if i not in got]
        if missing:
            self.die(f"no value for {', '.join(missing)}")
        return {i: float(got[i]) for i in ids}


def run(prog: str, base: str, body) -> None:
    """Runs body(FlinkMetrics); any error reading the API exits 1 with a message."""
    fm = FlinkMetrics(prog, base)
    try:
        body(fm)
    except SystemExit:
        raise
    except Exception as e:
        fm.die(f"cannot read the Flink REST API at {base}: {e}")

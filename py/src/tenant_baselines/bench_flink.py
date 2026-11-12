"""The benchmark's reads of the running job: Flink's REST metrics and checkpoint statistics.

Synthetic data only. Every function that calls REST takes `get` (and `patch`) callables, which
default to httpx. Metrics are read the way `scripts/flink_metrics.py` does: the REST API answers
from a cache that a request refreshes only when it finds the cache more than 10 s old, so a
sample is one burst (a request that starts the refresh, a 1 s wait, then the reads), and a
driver makes no other REST call between two samples.
"""

import re
import subprocess
import time
import urllib.parse
from collections.abc import Callable, Iterable

import httpx

from tenant_baselines.eval_run import REST, RunFailed
from tenant_baselines.freeze import REPO

REFRESH_WAIT_S = 1.0
CANCEL_WAIT_S = 60
_WATERMARK_FLOOR = 0  # a lowest watermark below this means no watermark yet (Long.MIN_VALUE)
_SUBMIT = (
    *("docker", "compose", "exec", "-T", "jobmanager"),
    *("bash", "/opt/tb-scripts/submit-bench.sh"),
)
_JOB_ID = re.compile(r"[0-9a-f]{32}")
_TERMINAL = ("FINISHED", "CANCELED", "FAILED")
_UNITS = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
_SIZE = re.compile(r"([0-9.]+)\s*([A-Za-z]+)")


def _sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)


def _json(get: Callable, path: str):
    return get(f"{REST}{path}", timeout=10).raise_for_status().json()


def submit(
    run_id: str, topic: str, extra: Iterable[str] = (), run: Callable = subprocess.run
) -> str:
    """Submit the bench job inside the jobmanager; returns its job id."""
    done = run([*_SUBMIT, run_id, topic, *extra], cwd=REPO, stdout=subprocess.PIPE, text=True)
    lines = done.stdout.strip().splitlines()
    job_id = lines[-1] if lines else ""
    if done.returncode != 0 or not _JOB_ID.fullmatch(job_id):
        raise RunFailed(f"submit-bench.sh exited {done.returncode}: {done.stdout.strip()!r}")
    return job_id


def vertex(job: dict, prefix: str) -> dict:
    """The one vertex of a `GET /jobs/<id>` body whose name starts with `prefix`. A vertex chained
    with its sinks is named `scorer -> (...)`, so a name is matched by its start."""
    found = [v for v in job["vertices"] if v["name"].startswith(prefix)]
    if len(found) != 1:
        names = [v["name"] for v in job["vertices"]]
        raise KeyError(f"{len(found)} vertices start with {prefix!r}; the vertices are {names}")
    return found[0]


def _refresh(job_id: str, vertex_id: str, get: Callable, sleep: Callable) -> None:
    """Send the request that starts a metric-cache refresh, and wait for the refresh to land."""
    _json(get, f"/jobs/{job_id}/vertices/{vertex_id}/metrics")
    sleep(REFRESH_WAIT_S)


def _values(get: Callable, job_id: str, vertex_id: str, ids: list[str]) -> dict[str, float]:
    query = urllib.parse.quote(",".join(ids), safe=",")
    got = {
        m["id"]: m["value"]
        for m in _json(get, f"/jobs/{job_id}/vertices/{vertex_id}/metrics?get={query}")
    }
    missing = [i for i in ids if i not in got]
    if missing:
        raise KeyError(f"no value for {', '.join(missing)} on vertex {vertex_id}")
    return {i: float(got[i]) for i in ids}


def _find_metric(get: Callable, job_id: str, suffix: str) -> tuple[str, list[str]]:
    """(vertex id, metric ids) of the first vertex with metrics ending in `suffix`."""
    for v in _json(get, f"/jobs/{job_id}")["vertices"]:
        listed = _json(get, f"/jobs/{job_id}/vertices/{v['id']}/metrics")
        ids = [m["id"] for m in listed if m["id"].endswith(suffix)]
        if ids:
            return v["id"], ids
    raise KeyError(f"no {suffix.lstrip('.')} metric on any vertex of job {job_id}")


def sample(
    job_id: str,
    source_vertex_id: str,
    parallelism: int,
    get: Callable = httpx.get,
    now: Callable = time.time,
    monotonic: Callable = time.monotonic,
    sleep: Callable = time.sleep,
) -> dict:
    """One burst: the source's output count and every vertex's busy, backpressured and idle time.

    `count` sums the task-level `<i>.numRecordsOut` of the source subtasks, not the operator-level
    ids, which would count the same records again.
    """
    t = now()
    mono = monotonic()
    _json(get, f"/jobs/{job_id}/vertices/{source_vertex_id}/metrics")
    sleep(REFRESH_WAIT_S)
    ids = [f"{i}.numRecordsOut" for i in range(parallelism)]
    counts = _values(get, job_id, source_vertex_id, ids)
    job = _json(get, f"/jobs/{job_id}")
    return {
        "t": t,
        "mono": mono,
        "count": int(sum(counts.values())),
        "vertices": {
            v["name"]: {
                "busy_ms": v["metrics"]["accumulated-busy-time"],
                "backpressured_ms": v["metrics"]["accumulated-backpressured-time"],
                "idle_ms": v["metrics"]["accumulated-idle-time"],
                "parallelism": v["parallelism"],
            }
            for v in job["vertices"]
        },
    }


def watermark_lag_s(
    job_id: str,
    offset_s: float,
    get: Callable = httpx.get,
    now: Callable = time.time,
    sleep: Callable = time.sleep,
) -> float | None:
    """How far the minute aggregator's lowest input watermark is behind the job's clock, in
    seconds. `offset_s` is the VM clock minus the host's. The request time is read before the
    burst's first request. None until every subtask has a watermark.
    """
    sent = now()  # before the first request of the burst, so the value is never newer than it
    first = _json(get, f"/jobs/{job_id}")["vertices"][0]["id"]
    _refresh(job_id, first, get, sleep)
    vid, ids = _find_metric(get, job_id, ".minute-aggregator.currentInputWatermark")
    lowest = min(_values(get, job_id, vid, ids).values())
    if lowest < _WATERMARK_FLOOR:
        return None
    return sent + offset_s - lowest / 1000


def late_events(job_id: str, get: Callable = httpx.get, sleep: Callable = time.sleep) -> int:
    """The job's total `lateEvents`, summed over the late-events operator's subtasks."""
    first = _json(get, f"/jobs/{job_id}")["vertices"][0]["id"]
    _refresh(job_id, first, get, sleep)
    vid, ids = _find_metric(get, job_id, ".late-events.lateEvents")
    return int(sum(_values(get, job_id, vid, ids).values()))


def restarts(job_id: str, get: Callable = httpx.get) -> int:
    return len(_json(get, f"/jobs/{job_id}/exceptions")["exceptionHistory"]["entries"])


def job_times(job_id: str, get: Callable = httpx.get) -> dict:
    """Start and end of the job and the end of its source vertex, in epoch milliseconds."""
    job = _json(get, f"/jobs/{job_id}")
    return {
        "start_ms": job["start-time"],
        "end_ms": job["end-time"],
        "source_end_ms": vertex(job, "Source:")["end-time"],
    }


def checkpoint_config(job_id: str, get: Callable = httpx.get) -> dict:
    body = _json(get, f"/jobs/{job_id}/checkpoints/config")
    return {"interval_ms": body["interval"], "state_backend": body["state_backend"]}


def checkpoint_history(job_id: str, get: Callable = httpx.get) -> list[dict]:
    """The completed checkpoints; failed and in-progress ones are left out."""
    history = _json(get, f"/jobs/{job_id}/checkpoints")["history"]
    return [
        {
            "id": c["id"],
            "trigger_timestamp": c["trigger_timestamp"],
            "state_size": c["state_size"],
            "checkpointed_size": c["checkpointed_size"],
        }
        for c in history
        if c["status"] == "COMPLETED"
    ]


def latest_before(history: list[dict], before_ms: int | None) -> dict:
    """The checkpoint with the highest id triggered before `before_ms` (any, when None)."""
    pool = [c for c in history if before_ms is None or c["trigger_timestamp"] < before_ms]
    if not pool:
        raise ValueError(f"no completed checkpoint was triggered before {before_ms}")
    return max(pool, key=lambda c: c["id"])


def vertex_state(job_id: str, checkpoint_id: int, get: Callable = httpx.get) -> list[dict]:
    """The state of one checkpoint, per vertex, by the vertex's name."""
    names = {v["id"]: v["name"] for v in _json(get, f"/jobs/{job_id}")["vertices"]}
    tasks = _json(get, f"/jobs/{job_id}/checkpoints/details/{checkpoint_id}")["tasks"]
    return [
        {
            "name": names[vid],
            "state_size": t["state_size"],
            "checkpointed_size": t["checkpointed_size"],
        }
        for vid, t in tasks.items()
    ]


def cancel_and_clean(
    job_id: str,
    run_id: str,
    get: Callable = httpx.get,
    patch: Callable = httpx.patch,
    run: Callable = _sh,
    sleep: Callable = time.sleep,
    monotonic: Callable = time.monotonic,
) -> None:
    """Cancel the job, wait for it to stop, and delete the run's checkpoint directory. A job that
    is already over is only cleaned."""
    if _json(get, f"/jobs/{job_id}")["state"] not in _TERMINAL:
        r = patch(f"{REST}/jobs/{job_id}", params={"mode": "cancel"}, timeout=10)
        # 409: the job finished on its own meanwhile; the wait below sees its state
        if r.status_code not in (202, 409):
            raise RunFailed(f"cancelling job {job_id} returned HTTP {r.status_code}, not 202")
        deadline = monotonic() + CANCEL_WAIT_S
        while (state := _json(get, f"/jobs/{job_id}")["state"]) not in _TERMINAL:
            if monotonic() >= deadline:
                raise RunFailed(f"job {job_id} is still {state} {CANCEL_WAIT_S} s after cancel")
            sleep(1.0)
    rm = run(
        ["docker", "compose", "exec", "-T", "jobmanager"]
        + ["rm", "-rf", f"/flink-data/checkpoints/bench/{run_id}"]
    )
    if rm.returncode != 0:
        raise RunFailed(f"could not remove the checkpoints of {run_id}: {rm.stderr.strip()}")


def taskmanager_memory(run: Callable = _sh) -> dict:
    """The task manager container's memory use and limit, in bytes, from `docker stats`."""
    cid = run(["docker", "compose", "ps", "-q", "taskmanager"]).stdout.strip()
    if not cid:
        raise RunFailed("no task manager container; is the stack up?")
    out = run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", cid]).stdout
    used, _, limit = out.partition("/")
    return {"used_bytes": _bytes(used), "limit_bytes": _bytes(limit)}


def _bytes(text: str) -> int:
    m = _SIZE.fullmatch(text.strip())
    if not m or m[2].upper() not in _UNITS:
        raise ValueError(f"cannot read a size from {text!r}")
    return round(float(m[1]) * _UNITS[m[2].upper()])

"""One eval run end to end: simulate, fit, replay as aggregates through the job, score.

Synthetic data only. `prepare` holds the seed guard of the seed protocol (spec 5.3): a run of
each kind may only simulate the seeds its kind allows.
"""

import argparse
import json
import logging
import re
import subprocess
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson

from tenant_baselines.evalmodel import EPOCH_MS, REPLAY_END_MS, TrafficParams, rates
from tenant_baselines.kafka_io import create_topic, end_offsets, produce_all, read_range
from tenant_baselines.scenarios import Scenario, apply, place, write_labels
from tenant_baselines.scoring import normalise, summarise
from tenant_baselines.simulate import Aggregates, check_rows, checked, encode_row, rows, simulate
from tenant_baselines.thresholds import eval_thresholds
from tenant_baselines.world import build_world

log = logging.getLogger(__name__)

DEV_SEEDS = (7, 11, 13)
AMPLITUDES = (1.5, 6.0, 12.0)
PATHS_SEED = 7  # the equivalence check's seed (Task 14)

REST = "http://127.0.0.1:8081"
POLL_S = 2.0
POLL_ATTEMPTS = 5  # tries of one poll before the run is given up
ALERTS_TOPIC = "eval.alerts"
REPO = Path(__file__).resolve().parents[3]
DATA_DIR = REPO / "data" / "eval"
_CONTAINER_DATA = "/opt/tb-data"  # the jobmanager's read-only mount of the repo's data/
_SUBMIT = (
    *("docker", "compose", "exec", "-T", "jobmanager"),
    *("bash", "/opt/tb-scripts/submit-eval.sh"),
)
_FAILED = ("FAILED", "FAILING", "RESTARTING", "CANCELLING", "CANCELED", "SUSPENDED")
_JOB_ID = re.compile(r"[0-9a-f]{32}")


class SeedNotAllowed(Exception):
    pass


class RunFailed(Exception):
    pass


def run_id(kind: str, seed: int, amplitude: float, now: datetime) -> str:
    """`<kind>-s<seed>-a<amplitude>-<UTC yyyymmddThhmmss>`; `now` must carry its time zone."""
    if now.tzinfo is None:
        raise ValueError("run_id needs an aware datetime")
    return f"{kind}-s{seed}-a{amplitude:g}-{now.astimezone(UTC):%Y%m%dT%H%M%S}"


def _check_seed(kind: str, world_name: str, seed: int, freeze: dict | None) -> None:
    if kind == "dev":
        if seed not in DEV_SEEDS:
            raise SeedNotAllowed(f"a dev run uses one of the dev seeds {DEV_SEEDS}, not {seed}")
    elif kind == "off":
        if freeze is None:
            raise SeedNotAllowed("an official run needs the freeze file")
        if seed not in freeze["official_seeds"]:
            raise SeedNotAllowed(
                f"an official run uses one of the frozen seeds {freeze['official_seeds']},"
                f" not {seed}"
            )
    elif kind == "paths":
        if world_name != "paths" or seed != PATHS_SEED:
            raise SeedNotAllowed(
                f"a paths run uses world 'paths' and seed {PATHS_SEED},"
                f" not {world_name!r} and {seed}"
            )
    else:
        raise SeedNotAllowed(f"unknown run kind {kind!r}")


def _meta(kind: str, world_name: str, seed: int, amplitude: float) -> dict:
    return {
        "kind": kind,
        "world": world_name,
        "seed": seed,
        "amplitude": amplitude,
        "epoch_ms": EPOCH_MS,
        "replay_end_ms": REPLAY_END_MS,
        "data": "synthetic",
    }


def prepare(
    kind: str,
    world_name: str,
    seed: int,
    amplitude: float,
    out_dir: str | Path,
    freeze: dict | None = None,
) -> tuple[Aggregates, list[Scenario]]:
    """Simulate a run's five weeks and fit its static thresholds, after the seed guard.

    Writes `labels.json`, `aggregates.npz` and `thresholds.json` into `out_dir`, which must not
    exist yet: a run id is never reused.
    """
    _check_seed(kind, world_name, seed, freeze)
    out = Path(out_dir)
    if out.exists():
        raise FileExistsError(f"{out} exists; a run id is never reused")
    world = build_world(world_name, seed)
    r = rates(world, TrafficParams(amplitude=amplitude), seed)
    scenarios = place(world, r, seed)
    a = simulate(world, r, apply(scenarios, world, r.base.shape[1]), seed)
    check_rows(a)
    thresholds = eval_thresholds(a, scenarios)

    out.mkdir(parents=True)
    write_labels(out / "labels.json", scenarios, _meta(kind, world_name, seed, amplitude))
    a.save(out / "aggregates.npz")
    text = json.dumps(thresholds, indent=2, sort_keys=True, allow_nan=False)
    (out / "thresholds.json").write_text(text + "\n")
    return a, scenarios


def submit(
    run_id: str,
    mode: str,
    topic: str,
    thresholds_path: str,
    extra: Iterable[str] = (),
    replay_end_ms: int = REPLAY_END_MS,
) -> str:
    """Submit the eval job inside the jobmanager; returns its job id. `thresholds_path` is the
    path inside the container."""
    cmd = [*_SUBMIT, run_id, mode, topic, str(replay_end_ms), thresholds_path, *extra]
    done = subprocess.run(cmd, cwd=REPO, stdout=subprocess.PIPE, text=True)
    lines = done.stdout.strip().splitlines()
    job_id = lines[-1] if lines else ""
    if done.returncode != 0 or not _JOB_ID.fullmatch(job_id):
        raise RunFailed(f"submit-eval.sh exited {done.returncode}: {done.stdout.strip()!r}")
    return job_id


def _cancel(job_id: str, patch: Callable) -> None:
    """Ask Flink to cancel the job. A failure is logged, not raised: the caller raises its own."""
    try:
        r = patch(f"{REST}/jobs/{job_id}", params={"mode": "cancel"}, timeout=10)
    except httpx.HTTPError as e:
        log.warning("could not cancel job %s: %s", job_id, e)
        return
    if r.status_code != 202:
        log.warning("cancelling job %s returned HTTP %s, not 202", job_id, r.status_code)


def _get_json(get: Callable, url: str) -> dict:
    """GET `url` as JSON, trying up to POLL_ATTEMPTS times on a connection, timeout or HTTP
    status error; the last error is raised."""
    for attempt in range(1, POLL_ATTEMPTS + 1):
        try:
            return get(url, timeout=10).raise_for_status().json()
        except httpx.HTTPError as e:
            if attempt == POLL_ATTEMPTS:
                raise
            log.warning("polling %s failed (%s), try %d of %d", url, e, attempt, POLL_ATTEMPTS)
            time.sleep(POLL_S)
    raise AssertionError("unreachable")


def _check_no_restarts(job_id: str, get: Callable) -> None:
    """Raise RunFailed if the finished job failed and restarted on the way: its at-least-once
    sinks then wrote some output twice. Every failure is an entry of the exception history."""
    url = f"{REST}/jobs/{job_id}/exceptions"
    try:
        body = _get_json(get, url)
    except httpx.HTTPError as e:
        raise RunFailed(f"could not read {url}: {e}") from e
    entries = (body.get("exceptionHistory") or {}).get("entries")
    if entries is None:
        raise RunFailed(f"{url} has no exception history; cannot tell whether the job restarted")
    if entries:
        first = entries[0].get("exceptionName")
        raise RunFailed(
            f"job {job_id} finished after {len(entries)} failure(s) and restart(s), first {first};"
            " its output after a restart is not trusted"
        )


def wait_finished(
    job_id: str, timeout_s: float = 1800, get: Callable = httpx.get, patch: Callable = httpx.patch
) -> dict:
    """Poll the job every POLL_S seconds until it is FINISHED: `{"job_seconds", "vertices"}`.

    Raises RunFailed, after cancelling the job, when it is failed, failing, restarting,
    cancelled or suspended, when it runs past the timeout, or when a poll keeps failing. A
    FINISHED job raises RunFailed too if it restarted on the way. A bounded eval must not run on
    after its input went wrong, and must not hold the slots of the next run.
    """
    deadline = time.monotonic() + timeout_s
    url = f"{REST}/jobs/{job_id}"
    while True:
        try:
            job = _get_json(get, url)
        except httpx.HTTPError as e:
            _cancel(job_id, patch)
            raise RunFailed(f"could not poll job {job_id}: {e}") from e
        state = job["state"]
        if state == "FINISHED":
            _check_no_restarts(job_id, get)
            return {"job_seconds": job["duration"] / 1000, "vertices": job["vertices"]}
        if state in _FAILED:
            _cancel(job_id, patch)
            raise RunFailed(f"job {job_id} is {state}; see {url}/exceptions")
        if time.monotonic() >= deadline:
            _cancel(job_id, patch)
            raise RunFailed(f"job {job_id} timed out after {timeout_s} s in state {state}")
        time.sleep(POLL_S)


def _container_path(path: Path) -> str:
    """The jobmanager's path of a file under the repo's data/ directory."""
    try:
        rel = path.resolve().relative_to(REPO / "data")
    except ValueError:
        raise ValueError(f"{path} is not under {REPO / 'data'}, the jobmanager's mount") from None
    return f"{_CONTAINER_DATA}/{rel.as_posix()}"


def _checkpoint(job_id: str) -> dict:
    body = httpx.get(f"{REST}/jobs/{job_id}/checkpoints", timeout=10).raise_for_status().json()
    latest = (body.get("latest") or {}).get("completed")
    if latest is None:
        return {"state_size": None, "external_path": None}
    return {"state_size": latest.get("state_size"), "external_path": latest.get("external_path")}


def _vertex_times(vertices: list[dict]) -> list[dict]:
    """Per vertex: accumulated busy, back-pressured and idle ms, where the job detail has them."""
    out = []
    for v in vertices:
        m = v.get("metrics") or {}
        out.append(
            {
                "name": v["name"],
                "parallelism": v.get("parallelism"),
                "busy_ms": m.get("accumulated-busy-time"),
                "backpressured_ms": m.get("accumulated-backpressured-time"),
                "idle_ms": m.get("accumulated-idle-time"),
            }
        )
    return out


def _commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.strip()


def _dirty() -> bool:
    """Whether the working tree differs from HEAD (data/ is ignored, so a run's own files are not
    counted). Dev runs record it; official runs refuse a dirty tree (Task 17)."""
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout
    return bool(status.strip())


def _write_failed(out: Path, rid: str, stage: str, job_id: str | None, e: BaseException) -> None:
    """`failed.json` in the run's directory: a failed run is then told apart from one running."""
    doc = {"run_id": rid, "stage": stage, "job_id": job_id, "error": f"{type(e).__name__}: {e}"}
    try:
        (out / "failed.json").write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    except OSError as w:
        log.warning("could not write %s: %s", out / "failed.json", w)


def run(
    kind: str,
    seed: int,
    amplitude: float,
    world_name: str = "eval",
    data_dir: str | Path = DATA_DIR,
) -> dict:
    """Generate, produce, replay through the job in aggregates mode, read back, score.

    Writes `alerts.jsonl` (the run's alert log in normal form) and `summary.json` beside the
    files of `prepare`, in `<data_dir>/<run_id>/`, and returns the summary. A run that fails
    once its directory exists leaves `failed.json` there instead (stage, error, job id).
    """
    t0 = time.monotonic()
    rid = run_id(kind, seed, amplitude, datetime.now(UTC))
    out = Path(data_dir) / rid
    thresholds_path = _container_path(out / "thresholds.json")
    commit, dirty = _commit(), _dirty()
    existed = out.exists()  # prepare refuses it; failed.json must not land in another run's files
    stage, job_id = "generate", None
    try:
        log.info("run %s: generating", rid)
        a, scenarios = prepare(kind, world_name, seed, amplitude, out)
        t_generate = time.monotonic() - t0

        stage = "produce"
        topic = f"{rid}.aggregates-in"
        log.info("run %s: producing to %s", rid, topic)
        t1 = time.monotonic()
        create_topic(topic, 1)
        records = ((0, r["key"].encode(), encode_row(r)) for r in checked(rows(a)))
        n_rows = produce_all(topic, records)
        if end_offsets(topic) != {0: n_rows}:
            raise RunFailed(f"{topic} does not hold exactly the {n_rows} rows produced")
        t_produce = time.monotonic() - t1

        stage = "submit"
        start = end_offsets(ALERTS_TOPIC)  # before the submit: the job's first row is after it
        job_id = submit(rid, "aggregates", topic, thresholds_path)
        log.info("run %s: %d rows produced, job %s submitted", rid, n_rows, job_id)

        stage = "job"
        job = wait_finished(job_id)

        stage = "read"
        end = end_offsets(ALERTS_TOPIC)
        # The whole range is read and checked complete first; eval.alerts is shared by every run.
        alerts = [r for r in read_range(ALERTS_TOPIC, start, end) if r["run_id"] == rid]
        log.info(
            "run %s: job finished in %.0f s, %d alert rows", rid, job["job_seconds"], len(alerts)
        )

        stage = "score"
        log_rows, _ = normalise(alerts, REPLAY_END_MS)
        with open(out / "alerts.jsonl", "wb") as f:
            for row in log_rows:
                f.write(orjson.dumps(row, option=orjson.OPT_SORT_KEYS) + b"\n")
        summary = summarise(alerts, scenarios, _meta(kind, world_name, seed, amplitude))
        checkpoint = _checkpoint(job_id)
        summary |= {
            "run_id": rid,
            "job_id": job_id,
            "commit": commit,
            "dirty": dirty,
            "aggregate_rows": n_rows,
            "alert_rows": len(alerts),
            "checkpoint_state_size": checkpoint["state_size"],
            "checkpoint_path": checkpoint["external_path"],
            "vertices": _vertex_times(job["vertices"]),
            "timings": {
                "generate": round(t_generate, 1),
                "produce": round(t_produce, 1),
                "job": round(job["job_seconds"], 1),
                "total": round(time.monotonic() - t0, 1),
            },
        }
        text = json.dumps(summary, indent=2, sort_keys=True, allow_nan=False)
        (out / "summary.json").write_text(text + "\n")
        return summary
    except BaseException as e:
        if not existed and out.is_dir():
            _write_failed(out, rid, stage, job_id, e)
        raise


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one dev seed of the eval end to end")
    ap.add_argument("--kind", choices=["dev"], required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--amplitude", type=float, choices=AMPLITUDES, required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # else one line per poll of the job
    s = run(args.kind, args.seed, args.amplitude)
    t = s["timings"]
    print(
        f"eval-dev: {s['run_id']} done; {s['aggregate_rows']} rows, {s['alert_rows']} alert rows;"
        f" generate {t['generate']} s, produce {t['produce']} s, job {t['job']} s,"
        f" total {t['total']} s; {(DATA_DIR / s['run_id']).relative_to(REPO)}/summary.json"
    )


if __name__ == "__main__":
    main()

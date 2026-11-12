"""The state-size measurement: a dev seed's five weeks through the job, on two state backends.

Synthetic data only. The job runs in aggregates mode with the eval's settings and a 5 s checkpoint
interval, once on the hashmap backend (a full snapshot of serialized state: the per-key headline)
and once on RocksDB with incremental checkpoints (the stack's backend). The checkpoint reported is
the latest completed one triggered before the source finished; it must lie in the last 15% of the
time from the job's start to the source's end. Run with `make bench-state`.

Flink keeps only `web.checkpoints.history` completed checkpoints in its statistics (10 by default;
`docker-compose.yml` is sealed and does not set it). The history is read right after the job
finishes, before any clean-up.
"""

import argparse
import json
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx

from tenant_baselines import bench_env, bench_flink, bench_throughput, eval_run, kafka_io
from tenant_baselines.bench_delay_run import Polls, make_run_dir, power_reasons
from tenant_baselines.eval_run import RunFailed
from tenant_baselines.freeze import REPO, container_path, load_freeze
from tenant_baselines.simulate import checked, encode_row, rows
from tenant_baselines.world import build_world

__all__ = [
    "EXTRAPOLATE_KEYS",
    "LAST_SHARE",
    "SPEC_ESTIMATE_BYTES_PER_KEY",
    "backend_of",
    "build_result",
    "count_settings",
    "extrapolate",
    "in_last_share",
    "main",
    "measure_run",
    "per_key",
    "run_backend",
    "size_range",
    "world_counts",
]

# Section 4.3's estimate: 3 baseline slots x 168 hours x 5 weeks x 60 minutes x 4 bytes.
SPEC_ESTIMATE_BYTES_PER_KEY = 3 * 168 * 5 * 60 * 4
EXTRAPOLATE_KEYS = (2_000, 20_000, 100_000)
LAST_SHARE = 0.15
BACKENDS = ("hashmap", "rocksdb")
INTERVAL_MS = 5000
WORLD, SEED, AMPLITUDE = "eval", 7, 6.0
JOB_TIMEOUT_S = 900
DATA_BENCH = REPO / "data" / "bench"
EVAL_CHECKPOINTS = "/flink-data/checkpoints/eval"
_BACKEND_NAMES = {"HashMapStateBackend": "hashmap", "EmbeddedRocksDBStateBackend": "rocksdb"}


def in_last_share(trigger_ms: int, start_ms: int, source_end_ms: int, share: float = LAST_SHARE):
    """Whether a checkpoint was triggered before the source ended, in the last `share` of the time
    from the job's start to the source's end."""
    cutoff = source_end_ms - share * (source_end_ms - start_ms)
    return cutoff <= trigger_ms < source_end_ms


def per_key(vertices: list[dict], keys: int) -> dict:
    """The scorer vertex's state divided by the keys; the other vertices as they are."""
    scorers = [v for v in vertices if v["name"].startswith("scorer")]
    if len(scorers) != 1:
        raise KeyError(
            f"{len(scorers)} vertices start with 'scorer'; the vertices are"
            f" {[v['name'] for v in vertices]}"
        )
    scorer = scorers[0]
    return {
        "scorer_bytes": scorer["state_size"],
        "per_key_bytes": scorer["state_size"] / keys,
        "other_vertices": [
            {"name": v["name"], "state_size": v["state_size"]} for v in vertices if v is not scorer
        ],
    }


def size_range(history: list[dict], start_ms: int, source_end_ms: int) -> dict:
    """The count, lowest and highest `state_size` of the completed checkpoints in the last share."""
    sizes = [
        c["state_size"]
        for c in history
        if in_last_share(c["trigger_timestamp"], start_ms, source_end_ms)
    ]
    return {
        "checkpoints": len(sizes),
        "min": min(sizes) if sizes else None,
        "max": max(sizes) if sizes else None,
    }


def count_settings(thresholds: dict, seasonal_k: list) -> int:
    """The seasonal ks plus the distinct (detector, setting) pairs of the thresholds file."""
    pairs = {(s["detector"], s["setting"]) for s in thresholds["sets"]}
    return len(seasonal_k) + len(pairs)


def extrapolate(per_key_bytes: float, keys: Iterable[int]) -> list[dict]:
    return [{"keys": k, "bytes": round(per_key_bytes * k)} for k in keys]


def backend_of(state_backend: str) -> str | None:
    return _BACKEND_NAMES.get(state_backend)


def world_counts() -> tuple[int, int]:
    """(keys, keys that run all five weeks) of the eval world, counted from the world."""
    world = build_world(WORLD, SEED)
    start = {t.name: t.start_week for t in world.tenants}
    return len(world.keys), sum(1 for k in world.keys if start[k.tenant] == 1)


def measure_run(job_id: str, backend: str, keys: int, flink=bench_flink) -> dict:
    """Read a finished job's checkpoints and state: the backend and interval it ran with, the
    latest completed checkpoint before the source ended (in the last share, or RunFailed), the
    range over the last share and the per-key figure. Call it before any clean-up."""
    times = flink.job_times(job_id)
    cfg = flink.checkpoint_config(job_id)
    if backend_of(cfg["state_backend"]) != backend:
        raise RunFailed(f"job {job_id} ran on {cfg['state_backend']}, not {backend}")
    if cfg["interval_ms"] != INTERVAL_MS:
        raise RunFailed(
            f"job {job_id} checkpointed every {cfg['interval_ms']} ms, not {INTERVAL_MS}"
        )
    history = flink.checkpoint_history(job_id)
    start, end = times["start_ms"], times["source_end_ms"]
    try:
        last = flink.latest_before(history, end)
    except ValueError as e:
        raise RunFailed(f"{e}; the history holds {len(history)} completed checkpoints") from e
    if not in_last_share(last["trigger_timestamp"], start, end):
        raise RunFailed(
            f"no completed checkpoint lies in the last {LAST_SHARE:.0%} before the source ended:"
            f" the latest, {last['id']}, was triggered at {last['trigger_timestamp']} (job start"
            f" {start}, source end {end}); the history holds {len(history)} checkpoints"
        )
    vertices = flink.vertex_state(job_id, last["id"])
    return {
        "backend": backend,
        "state_backend": cfg["state_backend"],
        "interval_ms": cfg["interval_ms"],
        "job_seconds": (times["end_ms"] - start) / 1000,
        "history_checkpoints": len(history),
        "checkpoint": {
            "id": last["id"],
            "trigger_share": (last["trigger_timestamp"] - start) / (end - start),
            "state_size": last["state_size"],
            "checkpointed_size": last["checkpointed_size"],
            "vertices": vertices,
        },
        "range": size_range(history, start, end),
        "per_key": per_key(vertices, keys),
    }


def _sh(cmd: list[str]):
    return bench_env._sh(cmd)


def _clean(job_id: str, run_id: str) -> None:
    """Cancel the job if it still runs and delete its checkpoint directory."""
    bench_flink.cancel_and_clean(job_id, run_id)
    rm = _sh(
        ["docker", "compose", "exec", "-T", "jobmanager"]
        + ["rm", "-rf", f"{EVAL_CHECKPOINTS}/{run_id}"]
    )
    if rm.returncode != 0:
        raise RunFailed(f"could not remove the checkpoints of {run_id}: {rm.stderr.strip()}")


def _produce(topic: str, a) -> int:
    records = ((0, r["key"].encode(), encode_row(r)) for r in checked(rows(a)))
    return kafka_io.produce_all(topic, records)


REAL_OPS = SimpleNamespace(
    prepare=eval_run.prepare,
    create_topic=kafka_io.create_topic,
    produce=_produce,
    end_offsets=kafka_io.end_offsets,
    submit=eval_run.submit,
    container_path=container_path,
    wait_finished=eval_run.wait_finished,
    measure=measure_run,
    restarts=bench_flink.restarts,
    clean=_clean,
)


def _write_failed(out: Path, run_id: str, stage: str, job_id: str | None, e: BaseException):
    doc = {"run_id": run_id, "stage": stage, "job_id": job_id, "error": f"{type(e).__name__}: {e}"}
    try:
        (out / "failed.json").write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    except OSError as w:
        print(f"bench: could not write {out / 'failed.json'}: {w}")


def run_backend(backend: str, base: Path, polls: Polls, ops=REAL_OPS) -> dict:
    """One bounded job on `backend`: produce the five weeks, run it, read its state. A failure
    cancels and cleans the job, leaves `failed.json` in the run's directory and is raised."""
    run_id = f"state-{backend}-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    out = Path(base) / run_id
    topic = f"{run_id}.aggregates-in"
    stage, job_id = "generate", None
    load_start = bench_throughput._load()
    polls.slept.append(bench_env.slept_s())
    try:
        a, _ = ops.prepare("dev", WORLD, SEED, AMPLITUDE, out)  # makes `out`; never reuses it
        polls.slept.append(bench_env.slept_s())
        stage = "produce"
        ops.create_topic(topic, 1)
        n_rows = ops.produce(topic, a)
        if ops.end_offsets(topic) != {0: n_rows}:
            raise RunFailed(f"{topic} does not hold exactly the {n_rows} rows produced")
        polls.slept.append(bench_env.slept_s())
        stage = "submit"
        extra = [f"-Dexecution.checkpointing.interval={INTERVAL_MS // 1000}s"]
        if backend == "hashmap":
            extra.append("-Dstate.backend.type=hashmap")
        thresholds_path = ops.container_path(out / "thresholds.json")
        job_id = ops.submit(run_id, "aggregates", topic, thresholds_path, extra=extra)
        stage = "job"

        def get(*args, **kwargs):  # every poll of the wait ticks the poll clock
            polls.tick(time)
            return httpx.get(*args, **kwargs)

        ops.wait_finished(job_id, get=get, timeout_s=JOB_TIMEOUT_S)
        polls.tick(time)
        stage = "measure"
        restarts = ops.restarts(job_id)
        measured = ops.measure(job_id, backend, len(a.keys))  # before any clean-up
        thresholds = json.loads((out / "thresholds.json").read_text())
        run = {
            **measured,
            "run_id": run_id,
            "job_id": job_id,
            "keys": len(a.keys),
            "aggregate_rows": n_rows,
            "settings": count_settings(thresholds, load_freeze()["seasonal_k"]),
            "restarts": restarts,
            "stall_gaps_s": [round(g, 1) for g in bench_env.gaps(list(polls))],
            "host_sleep_s": round(bench_env.host_sleep_s(polls.slept), 1),
            "load_start": load_start,
            "load_end": bench_throughput._load(),
        }
        (out / "run.json").write_text(json.dumps(run, indent=2, sort_keys=True, allow_nan=False))
    except BaseException as e:
        if out.is_dir():
            _write_failed(out, run_id, stage, job_id, e)
        try:
            ops.clean(job_id, run_id) if job_id else print(f"bench: no job to clean for {run_id}")
        except Exception as c:
            print(f"bench: could not clean {run_id}: {c}")
        raise
    ops.clean(job_id, run_id)
    return run


def build_result(env: dict, runs: list[dict], power_end: str | None, on_battery: bool) -> dict:
    """The result file's content from the finished runs (the first is the headline hashmap run)."""
    first = runs[0]
    keys, five_weeks = world_counts()
    extra = power_reasons(power_end, on_battery)
    reasons: list[str] = []
    for r in runs:
        _, why = bench_env.validity(
            r["restarts"], 0, r["stall_gaps_s"], host_sleep_s=r["host_sleep_s"]
        )
        reasons += [f"{r['backend']}: {w}" for w in why]
    reasons += extra
    headline = next(r for r in runs if r["backend"] == "hashmap")
    return {
        "data": "synthetic",
        "env": env,
        "valid": not reasons,
        "invalid_reasons": reasons,
        "world": {
            "name": WORLD,
            "seed": SEED,
            "amplitude": AMPLITUDE,
            "keys": keys,
            "keys_with_five_weeks": five_weeks,
            "weeks": 5,
            "aggregate_rows": first["aggregate_rows"],
        },
        "settings": first["settings"],
        "spec_estimate_bytes_per_key": SPEC_ESTIMATE_BYTES_PER_KEY,
        "runs": runs,
        "headline_backend": "hashmap",
        "extrapolation": {
            "label": "extrapolation, not measured",
            **{
                r["backend"]: extrapolate(r["per_key"]["per_key_bytes"], EXTRAPOLATE_KEYS)
                for r in runs
            },
        },
        "power_end": power_end,
        "headline_per_key_bytes": headline["per_key"]["per_key_bytes"],
    }


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="State-size measurement (synthetic data)")
    ap.add_argument("--on-battery", action="store_true")
    args = ap.parse_args(argv)

    bench_env.preflight(args.on_battery)
    env = bench_env.collect(args.on_battery)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    result_dir = make_run_dir(DATA_BENCH, f"state-{stamp}")
    runs = []
    for backend in BACKENDS:
        run = run_backend(backend, DATA_BENCH, Polls())
        runs.append(run)
        c = run["checkpoint"]
        print(
            f"bench: {backend}: checkpoint {c['id']} at {c['trigger_share']:.0%} of the run,"
            f" {run['per_key']['per_key_bytes']:.0f} bytes per key ({run['run_id']})"
        )
    power_end = bench_env._power_line(bench_env._sh)
    result = build_result(env, runs, power_end, args.on_battery)
    path = bench_env.write_result("state", result, result_dir, pilot=False)
    why = "" if result["valid"] else " (INVALID: " + "; ".join(result["invalid_reasons"]) + ")"
    print(f"bench: wrote {path}{why}")


if __name__ == "__main__":
    main()

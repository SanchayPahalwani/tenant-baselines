"""The event-path equivalence check (spec 5.7): one world through both input paths of the job.

Synthetic data only. The `paths` world's five weeks run through the job twice: as events in
replay mode, and as pre-built minute aggregates in aggregates mode, both from the same draws.
The two alert logs must be equal in normal form, and the events job's minute aggregates must
equal the Python ones.
"""

import argparse
import json
import logging
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import orjson

from tenant_baselines.eval_run import (
    ALERTS_TOPIC,
    PATHS_SEED,
    RunFailed,
    prepare,
    run_id,
    submit,
    wait_finished,
)
from tenant_baselines.evalmodel import EPOCH_MS, MINUTE_MS, REPLAY_END_MS, TrafficParams, rates
from tenant_baselines.freeze import REPO, container_path, head_commit, sha256_of, tree_dirty
from tenant_baselines.kafka_io import create_topic, end_offsets, produce_all, read_range
from tenant_baselines.replay_events import partition_events, partition_of
from tenant_baselines.scenarios import Scenario, apply
from tenant_baselines.scoring import normalise
from tenant_baselines.simulate import Aggregates, KeyDraws, checked, draw_key, encode_row, rows
from tenant_baselines.world import World, build_world

log = logging.getLogger(__name__)

AMPLITUDE = 6.0
TIMEOUT_S = 7200  # for each job; wait_finished's default of 1,800 s is for an eval run
EVENT_PARTITIONS = 8
AGGREGATES_TOPIC = "eval.aggregates"
DIFF_LINES = 20
DATA_DIR = REPO / "data" / "eval"
RESULTS = REPO / "docs" / "results" / "check-paths.json"
JAR = REPO / "flink-job" / "target" / "tenant-baselines-job.jar"  # the jobmanager's default jar
_DAY_MINUTES = 1440


def _ident(row: dict) -> tuple:
    return tuple(sorted(row.items()))


def _line(row: dict) -> str:
    return orjson.dumps(row, option=orjson.OPT_SORT_KEYS).decode()


def diff_logs(a: list[dict], b: list[dict], replay_end_ms: int = REPLAY_END_MS) -> list[str]:
    """The first 20 rows that one alert log has and the other has not, in normal-form order.

    Both logs are brought to normal form first, so neither the order of the rows nor
    `emitted_at_ms` and `run_id` count, and values are compared parsed. Each line is a row as
    JSON, after `- ` when only `a` has it and `+ ` when only `b` has it. Empty when equal.
    """
    norm_a, _ = normalise(a, replay_end_ms)
    norm_b, _ = normalise(b, replay_end_ms)
    in_a, in_b = {_ident(r) for r in norm_a}, {_ident(r) for r in norm_b}
    if in_a == in_b:
        return []
    out = []
    for row in normalise(norm_a + norm_b, replay_end_ms)[0]:  # the union, in normal-form order
        ident = _ident(row)
        if (ident in in_a) != (ident in in_b):
            out.append(("- " if ident in in_a else "+ ") + _line(row))
            if len(out) == DIFF_LINES:
                break
    return out


def _cell_text(cell: tuple | None) -> str:
    if cell is None:
        return "no row"
    count, errors, p95 = cell
    return f"count {count}, errors {errors}, p95 {p95}"


def diff_aggregates(job_rows: Iterable[dict], a: Aggregates) -> list[str]:
    """The first 20 differences between the events job's minute aggregates and the Python ones.

    Walks `(key, minute_ms)` in order. The job must have exactly one row for each minute of each
    key from its first request to the replay end, as `rows(a)` does, with equal `count` and
    `errors` and an equal p95: exact float equality, and null on a zero row.
    """
    n_min = a.count.shape[1]
    count, errors, p95, first = a.count.tolist(), a.errors.tolist(), a.p95.tolist(), a.first
    want = {}
    for k, key in enumerate(a.keys):
        for m in range(int(first[k]), n_min):
            c = count[k][m]
            p = p95[k][m] if c else None  # the job writes null where the array has NaN
            want[(key.key, a.epoch_ms + m * MINUTE_MS)] = (c, errors[k][m], p)
    got: dict[tuple, list[tuple]] = {}
    for r in job_rows:
        cell = (r["key"], r["minute_ms"])
        got.setdefault(cell, []).append((r["count"], r["errors"], r["p95_latency_ms"]))

    out = []
    for cell in sorted(want.keys() | got.keys()):
        job = got.get(cell, [])
        if len(job) == 1 and job[0] == want.get(cell):
            continue
        key, minute_ms = cell
        if len(job) > 1:
            out.append(f"{key} minute {minute_ms}: the job has {len(job)} rows")
        else:
            job_text = _cell_text(job[0] if job else None)
            out.append(
                f"{key} minute {minute_ms}: job {job_text}; python {_cell_text(want.get(cell))}"
            )
        if len(out) == DIFF_LINES:
            break
    return out


def verdict(
    log_ev: list[dict],
    log_ag: list[dict],
    n_job_rows: int,
    n_py_rows: int,
    agg_diffs: list[str],
    log_diffs: list[str],
) -> list[str]:
    """Why the check fails, one reason per failed condition; empty when it passes.
    `log_diffs` is `diff_logs(log_ev, log_ag)`, computed once by the caller."""
    reasons = []
    if not log_ev:
        reasons.append("the events path's alert log is empty")
    if not log_ag:
        reasons.append("the aggregates path's alert log is empty")
    if not any(_regional_pages(log_) for log_ in (log_ev, log_ag)):
        reasons.append("neither alert log has a firing row of scope region")
    if log_diffs:
        reasons.append(f"the alert logs differ, first at: {log_diffs[0]}")
    if n_job_rows != n_py_rows:
        reasons.append(f"the events job wrote {n_job_rows} aggregate rows; Python has {n_py_rows}")
    if agg_diffs:
        reasons.append(f"the minute aggregates differ, first at: {agg_diffs[0]}")
    return reasons


def _draws(world: World, a: Aggregates, scenarios: list[Scenario]) -> list[KeyDraws]:
    """Every key's draws, made again as `prepare` made them: same world, rates, effects, seed.

    `prepare` keeps only the aggregates, so the draws are made a second time here, and they must
    give the same counts and errors.
    """
    r = rates(world, TrafficParams(amplitude=AMPLITUDE), PATHS_SEED)
    effects = apply(scenarios, world, r.base.shape[1])
    draws = [draw_key(world, r, effects, PATHS_SEED, k) for k in range(len(world.keys))]
    for k, d in enumerate(draws):
        if not (np.array_equal(d.counts, a.count[k]) and np.array_equal(d.errors, a.errors[k])):
            raise RunFailed(f"the draws of key {world.keys[k].key} differ from prepare's")
    return draws


def _first_days(draws: list[KeyDraws], days: int) -> list[KeyDraws]:
    n = days * _DAY_MINUTES
    return [
        KeyDraws(d.counts[:n], d.errors[:n], d.latencies[: int(d.counts[:n].sum())]) for d in draws
    ]


def _produce_events(topic: str, world: World, draws: list[KeyDraws]) -> int:
    """Every request as an event, in time order within each partition; returns the count."""
    create_topic(topic, EVENT_PARTITIONS)
    want = dict.fromkeys(range(EVENT_PARTITIONS), 0)
    for k, d in enumerate(draws):
        want[partition_of(k, EVENT_PARTITIONS)] += int(d.counts.sum())
    events = partition_events(world, draws, EPOCH_MS, PATHS_SEED, EVENT_PARTITIONS)
    n = produce_all(topic, ((p, key.encode(), value) for p, key, value in events))
    if end_offsets(topic) != want:
        raise RunFailed(f"{topic} does not hold exactly the {n} events produced, {want}")
    return n


def _produce_rows(topic: str, a: Aggregates) -> int:
    create_topic(topic, 1)
    n = produce_all(topic, ((0, r["key"].encode(), encode_row(r)) for r in checked(rows(a))))
    if end_offsets(topic) != {0: n}:
        raise RunFailed(f"{topic} does not hold exactly the {n} rows produced")
    return n


def _run_job(rid: str, mode: str, topic: str, thresholds: str, **kw) -> tuple[dict, dict, dict]:
    """Submit and wait; the job detail, and the alerts' and aggregates' offsets before it."""
    alerts_start, aggregates_start = end_offsets(ALERTS_TOPIC), end_offsets(AGGREGATES_TOPIC)
    job_id = submit(rid, mode, topic, thresholds, **kw)
    log.info("job %s (%s, %s) submitted as %s", rid, mode, topic, job_id)
    job = wait_finished(job_id, TIMEOUT_S)
    log.info("job %s finished in %.0f s", rid, job["job_seconds"])
    return job, alerts_start, aggregates_start


def _read_run(topic: str, start: dict[int, int], rid: str) -> list[dict]:
    return [r for r in read_range(topic, start, end_offsets(topic)) if r["run_id"] == rid]


def _write_log(path: Path, log_: list[dict]) -> None:
    path.write_text("".join(_line(row) + "\n" for row in log_))


def _regional_pages(log_: list[dict]) -> int:
    return sum(1 for r in log_ if r["state"] == "firing" and r["scope"] == "region")


def check(data_dir: Path = DATA_DIR, results: Path = RESULTS, jar: Path = JAR) -> int:
    """Both paths end to end; writes the results file and returns the exit code.

    The commit, the tree's state and the jar's hash are read before anything runs: a commit made
    while the jobs run must not be recorded as the one they ran on."""
    t0 = time.monotonic()
    commit, dirty = head_commit(), tree_dirty("paths")
    jar_sha256 = sha256_of(jar) if Path(jar).is_file() else None
    rid = run_id("paths", PATHS_SEED, AMPLITUDE, datetime.now(UTC))
    out = Path(data_dir) / rid
    log.info("run %s: generating", rid)
    a, scenarios = prepare("paths", "paths", PATHS_SEED, AMPLITUDE, out)
    world = build_world("paths", PATHS_SEED)
    draws = _draws(world, a, scenarios)
    thresholds = container_path(out / "thresholds.json")
    t_generate = time.monotonic() - t0

    t1 = time.monotonic()
    events_topic, rows_topic = f"{rid}.events", f"{rid}.aggregates-in"
    n_events = _produce_events(events_topic, world, draws)
    del draws
    n_rows = _produce_rows(rows_topic, a)
    t_produce = time.monotonic() - t1
    log.info("run %s: %d events and %d rows produced in %.0f s", rid, n_events, n_rows, t_produce)

    ev, ag = f"{rid}-ev", f"{rid}-ag"
    job_ev, alerts_start, aggregates_start = _run_job(ev, "replay", events_topic, thresholds)
    alerts_ev = _read_run(ALERTS_TOPIC, alerts_start, ev)
    aggregates_ev = _read_run(AGGREGATES_TOPIC, aggregates_start, ev)
    job_ag, alerts_start, _ = _run_job(ag, "aggregates", rows_topic, thresholds)
    alerts_ag = _read_run(ALERTS_TOPIC, alerts_start, ag)

    log_ev, _ = normalise(alerts_ev, REPLAY_END_MS)
    log_ag, _ = normalise(alerts_ag, REPLAY_END_MS)
    _write_log(out / "alerts-ev.jsonl", log_ev)
    _write_log(out / "alerts-ag.jsonl", log_ag)
    log_diffs = diff_logs(log_ev, log_ag)
    agg_diffs = diff_aggregates(aggregates_ev, a)
    reasons = verdict(log_ev, log_ag, len(aggregates_ev), n_rows, agg_diffs, log_diffs)

    result = {
        "commit": commit,
        "dirty": dirty,
        "run_id": rid,
        "jar_sha256": jar_sha256,
        "passed": not reasons,
        "date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "world": "paths (synthetic)",
        "events": n_events,
        "aggregate_rows": n_rows,
        "alert_rows": len(log_ev),
        "region_firing_rows": _regional_pages(log_ev),
        "alerts_identical": not log_diffs,
        "aggregates_identical": not agg_diffs and len(aggregates_ev) == n_rows,
        "seconds": {
            "generate": round(t_generate, 1),
            "produce": round(t_produce, 1),
            "events_job": round(job_ev["job_seconds"], 1),
            "aggregates_job": round(job_ag["job_seconds"], 1),
            "total": round(time.monotonic() - t0, 1),
        },
    }
    Path(results).parent.mkdir(parents=True, exist_ok=True)
    Path(results).write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")

    if not reasons:
        print(f"check-paths: identical ({len(log_ev)} alert rows, {n_rows} aggregate rows)")
        return 0
    print(f"check-paths: FAILED ({rid})")
    for reason in reasons:
        print(f"  {reason}")
    if log_diffs:
        print("first differing alert rows (- events path, + aggregates path):")
        for line in log_diffs:
            print(f"  {line}")
    if agg_diffs:
        print("first differing aggregate rows:")
        for line in agg_diffs:
            print(f"  {line}")
    return 1


def time_slice(days: int, data_dir: Path = DATA_DIR) -> int:
    """The first `days` days of events alone through the events job, timed; compares nothing."""
    rid = run_id("paths", PATHS_SEED, AMPLITUDE, datetime.now(UTC))
    out = Path(data_dir) / rid
    a, scenarios = prepare("paths", "paths", PATHS_SEED, AMPLITUDE, out)
    world = build_world("paths", PATHS_SEED)
    draws = _first_days(_draws(world, a, scenarios), days)
    topic = f"{rid}.events-{days}d"
    n = _produce_events(topic, world, draws)
    end_ms = EPOCH_MS + days * _DAY_MINUTES * MINUTE_MS
    thresholds = container_path(out / "thresholds.json")
    job, _, _ = _run_job(f"{rid}-slice", "replay", topic, thresholds, replay_end_ms=end_ms)
    s = job["job_seconds"]
    print(f"check-paths: {days}-day slice, {n} events: job {s:.1f} s, {n / s:,.0f} events/s")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Check that both input paths give one alert log")
    ap.add_argument("--slice-days", type=int, help="time the events path on the first N days only")
    args = ap.parse_args()
    days = (REPLAY_END_MS - EPOCH_MS) // (_DAY_MINUTES * MINUTE_MS)
    if args.slice_days is not None and not 1 <= args.slice_days <= days:
        ap.error(f"--slice-days must be 1 to {days}")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # else one line per poll of the job
    sys.exit(check() if args.slice_days is None else time_slice(args.slice_days))


if __name__ == "__main__":
    main()

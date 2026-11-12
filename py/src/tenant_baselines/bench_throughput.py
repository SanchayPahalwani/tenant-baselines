"""The throughput benchmark: pre-fill a topic, then read it with the live job, twice.

Synthetic data only. The metric is the task-level `numRecordsOut` of the `Source: events` vertex,
sampled every 12 s; the sustained rate is the counter's rise over a window that leaves out the
job's start and the last tenth of the fill. Run with `make bench-throughput`.
"""

import argparse
import json
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines import bench_env, bench_flink, bench_world, kafka_io
from tenant_baselines.bench_fill import fill_events
from tenant_baselines.eval_run import RunFailed
from tenant_baselines.freeze import REPO

__all__ = [
    "WindowTooShort",
    "alert_counts",
    "check_final_count",
    "main",
    "sustained_rate",
    "window",
    "window_shares",
]

BYTES_PER_EVENT = 20  # measured 15.7 in the pilot, rounded up to the next 10
EVENTS_PER_MINUTE = 240_000  # 2,000 keys at 120 requests a minute
PARTITIONS = 8
SAMPLE_INTERVAL_S = 12
RUN_TIMEOUT_S = 30 * 60
RUNNING_TIMEOUT_S = 120
METRIC_CACHE_WAIT_S = 11
MAX_MINUTES = 300
DATA_DIR = "/var/lib/kafka/data"
THRESHOLDS = REPO / "infra" / "flink" / "thresholds-demo.json"
DATA_BENCH = REPO / "data" / "bench"
ALERTS_TOPIC = "bench.alerts"


class WindowTooShort(Exception):
    pass


def window(
    samples: list[dict], total: int, warmup_s: float = 60, end_share: float = 0.9
) -> tuple[int, int]:
    """Indexes of the window's first and last sample.

    The first is the first sample at least `warmup_s` after the first one with a count above 0;
    the last is the last sample with a count below `end_share * total`.
    """
    for a, b in zip(samples, samples[1:], strict=False):
        if b["count"] < a["count"]:
            raise ValueError(
                f"the source count fell from {a['count']} to {b['count']}: the job restarted"
            )
    active = next((i for i, s in enumerate(samples) if s["count"] > 0), None)
    if active is None:
        raise WindowTooShort("the source never emitted; raise BENCH_MINUTES or check the job")
    t0 = samples[active]["t"] + warmup_s
    first = next((i for i, s in enumerate(samples) if s["t"] >= t0), None)
    last = max((i for i, s in enumerate(samples) if s["count"] < end_share * total), default=None)
    if first is None or last is None or last <= first:
        raise WindowTooShort(
            "no sample lies between the warm-up and the last tenth of the fill; raise BENCH_MINUTES"
        )
    return first, last


def sustained_rate(
    samples: list[dict],
    total: int,
    warmup_s: float = 60,
    end_share: float = 0.9,
    min_window_s: float = 120,
    min_intervals: int = 8,
) -> dict:
    first, last = window(samples, total, warmup_s, end_share)
    win = samples[first : last + 1]
    span = win[-1]["t"] - win[0]["t"]
    intervals = len(win) - 1
    if span < min_window_s or intervals < min_intervals:
        raise WindowTooShort(
            f"the window is {span:.0f} s and {intervals} intervals; it needs {min_window_s} s and"
            f" {min_intervals}. Raise BENCH_MINUTES"
        )
    rates = [
        (b["count"] - a["count"]) / (b["t"] - a["t"]) for a, b in zip(win, win[1:], strict=False)
    ]
    return {
        "events_per_s": round((win[-1]["count"] - win[0]["count"]) / span, 1),
        "window_start_s": win[0]["t"],
        "window_end_s": win[-1]["t"],
        "window_s": span,
        "intervals": intervals,
        "min_interval_rate": round(min(rates), 1),
        "max_interval_rate": round(max(rates), 1),
        "timing_error_pct": round(100 * 2 * bench_flink.REFRESH_WAIT_S / span, 1),
    }


def window_shares(samples: list[dict], first: int, last: int) -> dict:
    """Per vertex: the busy, back-pressured and idle share of `parallelism x` the window."""
    a, b = samples[first], samples[last]
    window_ms = (b["t"] - a["t"]) * 1000
    out = {}
    for name, v in b["vertices"].items():
        denom = v["parallelism"] * window_ms
        out[name] = {
            share: round((v[f"{key}_ms"] - a["vertices"][name][f"{key}_ms"]) / denom, 4)
            for share, key in (
                ("busy", "busy"),
                ("backpressured", "backpressured"),
                ("idle", "idle"),
            )
        }
    return out


def check_final_count(count: int, total: int) -> None:
    if count != total:
        raise ValueError(
            f"the source count ended at {count} but {total} events were filled: the metric is"
            " not the task-level record count"
        )


def alert_counts(rows: list[dict]) -> dict:
    """The `firing` rows, counted by `<detector>/<setting>/<scope>`."""
    c = Counter(
        f"{r['detector']}/{r['setting']}/{r['scope']}" for r in rows if r["state"] == "firing"
    )
    return dict(sorted(c.items()))


def parse_df_available_kib(text: str) -> int:
    """The fourth field of the second line of `df -Pk`."""
    return int(text.strip().splitlines()[1].split()[3])


def parse_du_kib(text: str) -> int:
    return int(text.split()[0])


def _kafka_exec(run: Callable, *cmd: str) -> str:
    done = run(["docker", "compose", "exec", "-T", "kafka", *cmd])
    if done.returncode != 0:
        raise RunFailed(f"`{' '.join(cmd)}` in the broker exited {done.returncode}: {done.stderr}")
    return done.stdout


def _share_of(shares: dict, prefix: str) -> dict:
    found = [n for n in shares if n.startswith(prefix)]
    if len(found) != 1:
        raise KeyError(f"{len(found)} vertices start with {prefix!r}: {sorted(shares)}")
    return shares[found[0]]


def _settings() -> int:
    sets = json.loads(THRESHOLDS.read_text())["sets"]
    return 1 + len({(s["detector"], s["setting"]) for s in sets})


def _wait_running(job_id: str, get: Callable = bench_flink.httpx.get) -> None:
    deadline = time.monotonic() + RUNNING_TIMEOUT_S
    while True:
        state = bench_flink._json(get, f"/jobs/{job_id}")["state"]
        if state == "RUNNING":
            return
        if state in ("FAILED", "CANCELED", "FINISHED") or time.monotonic() > deadline:
            raise RunFailed(f"job {job_id} is {state}, not RUNNING")
        time.sleep(1.0)


def _fill(args, fill_id: str, run: Callable) -> dict:
    topic = f"{fill_id}.events"
    world = bench_world.build("bench", args.seed)
    n_keys = len(world.keys)
    need = 2 * args.minutes * EVENTS_PER_MINUTE * BYTES_PER_EVENT
    free = parse_df_available_kib(_kafka_exec(run, "df", "-Pk", DATA_DIR)) * 1024
    if free < need:
        raise bench_env.PreflightFailed(
            f"the broker has {free / 1e9:.1f} GB free and the fill needs twice"
            f" {need / 2e9:.1f} GB; lower BENCH_MINUTES"
        )
    before = parse_du_kib(_kafka_exec(run, "du", "-sk", DATA_DIR)) * 1024
    kafka_io.create_topic(topic, PARTITIONS)
    start_ms = int(time.time() // 60 * 60_000) - (args.minutes + 1) * 60_000
    t0 = time.monotonic()
    produced = kafka_io.produce_all(
        topic, fill_events(world, start_ms, args.minutes, args.rpm, args.seed, PARTITIONS)
    )
    seconds = time.monotonic() - t0
    ends = kafka_io.end_offsets(topic)
    if sum(ends.values()) != produced:
        raise RunFailed(f"{topic} holds {sum(ends.values())} records, {produced} were produced")
    after = parse_du_kib(_kafka_exec(run, "du", "-sk", DATA_DIR)) * 1024
    grown = after - before
    return {
        "topic": topic,
        "keys": n_keys,
        "partitions": PARTITIONS,
        "event_minutes": args.minutes,
        "rpm_per_key": args.rpm,
        "events": produced,
        "seconds": round(seconds, 1),
        "events_per_s": round(produced / seconds, 1),
        "kafka_bytes": grown,
        "bytes_per_event": round(grown / produced, 1),
    }


def _one_run(n: int, fill_id: str, total: int, args, run_dir: Path) -> dict:
    run_id = f"{fill_id}-r{n}"
    topic = f"{fill_id}.events"
    alerts_from = kafka_io.end_offsets(ALERTS_TOPIC)
    job_id = bench_flink.submit(run_id, topic)
    samples: list[dict] = []
    try:
        _wait_running(job_id)
        job = bench_flink._json(bench_flink.httpx.get, f"/jobs/{job_id}")
        src = bench_flink.vertex(job, "Source: events")
        deadline = time.monotonic() + RUN_TIMEOUT_S
        due = time.monotonic()
        while True:
            try:
                s = bench_flink.sample(job_id, src["id"], src["parallelism"])
            except KeyError:
                if samples:
                    raise  # the metrics existed and went; not a start-up gap
                s = None  # tasks not up yet
            if s is not None:
                samples.append(s)
                if s["count"] >= total:
                    break
            if time.monotonic() > deadline:
                raise RunFailed(f"{run_id}: the count did not reach {total} in 30 minutes")
            due += SAMPLE_INTERVAL_S
            time.sleep(max(0.0, due - time.monotonic()))
        check_final_count(samples[-1]["count"], total)
        time.sleep(METRIC_CACHE_WAIT_S)
        late = bench_flink.late_events(job_id)
        restarts = bench_flink.restarts(job_id)
        history = bench_flink.checkpoint_history(job_id)
        ck = bench_flink.latest_before(history, None)
        vertices = bench_flink.vertex_state(job_id, ck["id"])
        memory = bench_flink.taskmanager_memory()
        alerts_to = kafka_io.end_offsets(ALERTS_TOPIC)
        rows = [
            r
            for r in kafka_io.read_range(ALERTS_TOPIC, alerts_from, alerts_to)
            if r.get("run_id") == run_id
        ]
    except BaseException:
        try:
            bench_flink.cancel_and_clean(job_id, run_id)
        except Exception as e:  # keep the first error
            print(f"bench: could not clean {run_id}: {e}")
        raise
    bench_flink.cancel_and_clean(job_id, run_id)
    (run_dir / f"samples-r{n}.json").write_text(json.dumps(samples, allow_nan=False))
    return _summarise(run_id, job_id, total, samples, args.pilot) | {
        "checkpoint": {
            "id": ck["id"],
            "state_size": ck["state_size"],
            "checkpointed_size": ck["checkpointed_size"],
            "vertices": vertices,
        },
        "taskmanager_memory": memory,
        "alert_rows": alert_counts(rows),
        "late_events": late,
        "restarts": restarts,
    }


def _summarise(run_id: str, job_id: str, total: int, samples: list[dict], pilot: bool) -> dict:
    t0 = samples[0]["t"]
    rebased = [{**s, "t": round(s["t"] - t0, 1)} for s in samples]
    out: dict = {"run_id": run_id, "job_id": job_id, "events": total}
    try:
        rate = sustained_rate(rebased, total)
        first, last = window(rebased, total)
    except WindowTooShort as e:
        if not pilot:
            raise
        out["window_error"] = str(e)
        rate, first, last = {}, 0, len(samples) - 1
    shares = window_shares(rebased, first, last)
    out |= rate
    out["source"] = _share_of(shares, "Source: events")
    out["drop_ticks_busy"] = _share_of(shares, "drop-ticks")["busy"]
    out["shares"] = shares
    out["busiest_vertex"] = max(shares, key=lambda n: shares[n]["busy"])
    out["host_sleep_gaps_s"] = [round(g, 1) for g in bench_env.gaps([s["mono"] for s in samples])]
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Throughput benchmark (synthetic data)")
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--rpm", type=float, default=120)
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--on-battery", action="store_true")
    ap.add_argument("--pilot", action="store_true")
    args = ap.parse_args(argv)
    if not 1 <= args.minutes <= MAX_MINUTES:
        ap.error(f"--minutes must be between 1 and {MAX_MINUTES}")

    bench_env.preflight(args.on_battery)
    env = bench_env.collect(args.on_battery)
    fill_id = "tp-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    run_dir = DATA_BENCH / fill_id
    run_dir.mkdir(parents=True)
    run = bench_env._sh
    fill = _fill(args, fill_id, run)
    print(f"bench: filled {fill['events']} events in {fill['seconds']} s ({fill_id})")
    total = fill["events"]
    runs = []
    for n in range(1, args.runs + 1):
        r = _one_run(n, fill_id, total, args, run_dir)
        runs.append(r)
        print(
            f"bench: run {n}: {r.get('events_per_s', 'no window')} events/s, source idle"
            f" {r['source']['idle']}, busiest {r['busiest_vertex']}"
        )
    extra = [f"run {r['run_id']}: {r['window_error']}" for r in runs if "window_error" in r]
    sleeps = [g for r in runs for g in r["host_sleep_gaps_s"]]
    valid, reasons = bench_env.validity(
        sum(r["restarts"] for r in runs), sum(r["late_events"] for r in runs), sleeps, extra
    )
    rates = [r["events_per_s"] for r in runs if "events_per_s" in r]
    spread = (
        round((max(rates) - min(rates)) / (sum(rates) / len(rates)) * 100, 1) if rates else None
    )
    result = {
        "data": "synthetic",
        "env": env,
        "valid": valid,
        "invalid_reasons": reasons,
        "fill": fill,
        "warmup_s": 60,
        "end_share": 0.9,
        "sample_interval_s": SAMPLE_INTERVAL_S,
        "settings": _settings(),
        "runs": runs,
        "spread_pct": spread,
    }
    path = bench_env.write_result("throughput", result, run_dir, args.pilot)
    print(f"bench: wrote {path}{'' if valid else ' (INVALID: ' + '; '.join(reasons) + ')'}")


if __name__ == "__main__":
    main()

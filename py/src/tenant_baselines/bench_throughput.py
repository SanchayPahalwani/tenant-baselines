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

from confluent_kafka.admin import AdminClient, ConfigResource

from tenant_baselines import bench_env, bench_flink, bench_world, kafka_io
from tenant_baselines.bench_fill import fill_events
from tenant_baselines.eval_run import RunFailed
from tenant_baselines.freeze import REPO

__all__ = [
    "WindowTooShort",
    "alert_counts",
    "check_final_count",
    "next_due",
    "main",
    "sustained_rate",
    "window",
    "window_shares",
]

BYTES_PER_EVENT = 20  # measured 15.7 in the pilot, rounded up to the next 10
EVENTS_PER_MINUTE = 240_000  # 2,000 keys at 120 requests a minute
PARTITIONS = 8
SAMPLE_INTERVAL_S = 12
MIN_RUN_TIMEOUT_S = 30 * 60
MIN_EVENTS_PER_S = 8000  # a run fails only below this rate
MIN_BURST_GAP_S = 11  # the metric cache refreshes at most every 10 s
WARMUP_S = 60
END_SHARE = 0.9
RUNNING_TIMEOUT_S = 120
METRIC_CACHE_WAIT_S = 11
MAX_MINUTES = 300
DATA_DIR = "/var/lib/kafka/data"
THRESHOLDS = REPO / "infra" / "flink" / "thresholds-demo.json"
DATA_BENCH = REPO / "data" / "bench"
ALERTS_TOPIC = "bench.alerts"


class WindowTooShort(Exception):
    pass


def _fresh(samples: list[dict]) -> list[int]:
    """Indexes of the samples that were not read from a stale cache."""
    return [i for i, s in enumerate(samples) if not s.get("stale")]


def _endpoint_ok(samples: list[dict], i: int) -> bool:
    """A window endpoint is fresh and does not directly follow a stale sample, which can carry an
    older cache generation inside the freshness tolerance."""
    return not samples[i].get("stale") and not (i > 0 and samples[i - 1].get("stale"))


def window(
    samples: list[dict], total: int, warmup_s: float = WARMUP_S, end_share: float = END_SHARE
) -> tuple[int, int]:
    """Indexes of the window's first and last sample, both fresh.

    The first is the first fresh sample at least `warmup_s` after the first one with a count above
    0; the last is the last fresh sample with a count below `end_share * total` (a count equal to
    it is out).
    """
    fresh = _fresh(samples)
    for a, b in zip(fresh, fresh[1:], strict=False):
        if samples[b]["count"] < samples[a]["count"]:
            raise ValueError(
                f"the source count fell from {samples[a]['count']} to {samples[b]['count']}:"
                " the job restarted"
            )
    active = next((i for i in fresh if samples[i]["count"] > 0), None)
    if active is None:
        raise WindowTooShort("the source never emitted; raise BENCH_MINUTES or check the job")
    t0 = samples[active]["t"] + warmup_s
    ends = [i for i in fresh if _endpoint_ok(samples, i)]
    first = next((i for i in ends if samples[i]["t"] >= t0), None)
    last = max((i for i in ends if samples[i]["count"] < end_share * total), default=None)
    if first is None or last is None or last <= first:
        raise WindowTooShort(
            "no sample lies between the warm-up and the last tenth of the fill; raise BENCH_MINUTES"
        )
    return first, last


def sustained_rate(
    samples: list[dict],
    total: int,
    warmup_s: float = WARMUP_S,
    end_share: float = END_SHARE,
    min_window_s: float = 120,
    min_intervals: int = 8,
) -> dict:
    """The rate over the window, from the fresh samples only; rounded only in the result."""
    first, last = window(samples, total, warmup_s, end_share)
    win = [samples[i] for i in _fresh(samples) if first <= i <= last]
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
    # The slower endpoint's read, measured: each endpoint's counter was read `read_s` after its
    # `t`, so the span is off by at most that, whatever the waits and re-reads were.
    read_s = max(x.get("read_s", bench_flink.REFRESH_WAIT_S) for x in (win[0], win[-1]))
    return {
        "events_per_s": round((win[-1]["count"] - win[0]["count"]) / span, 1),
        "window_start_s": round(win[0]["t"], 1),
        "window_end_s": round(win[-1]["t"], 1),
        "window_s": round(span, 1),
        "intervals": intervals,
        "min_interval_rate": round(min(rates), 1),
        "max_interval_rate": round(max(rates), 1),
        "timing_error_pct": round(100 * read_s / span, 1),
    }


def next_due(due: float, burst_start: float) -> float:
    """When the next burst starts: one interval after the last due time, but never closer than
    MIN_BURST_GAP_S to the last burst's start, so a late burst is not followed by a catch-up."""
    return max(due + SAMPLE_INTERVAL_S, burst_start + MIN_BURST_GAP_S)


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
    """The sum of the first fields of `du -sk` lines: one line per partition directory."""
    return sum(int(line.split()[0]) for line in text.splitlines() if line.strip())


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


def _topic_compression(topic: str) -> str:
    """The topic's `compression.type`, read from the broker."""
    admin = AdminClient({"bootstrap.servers": kafka_io.BOOTSTRAP, **kafka_io._CLIENT})
    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    return admin.describe_configs([res])[res].result()["compression.type"].value


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
    # This topic's partition directories only: other topics' growth is not in the figure.
    du = _kafka_exec(run, "sh", "-c", f"du -sk {DATA_DIR}/{topic}-*")
    on_disk = parse_du_kib(du) * 1024
    return {
        "topic": topic,
        "keys": n_keys,
        "partitions": PARTITIONS,
        "event_minutes": args.minutes,
        "rpm_per_key": args.rpm,
        "events": produced,
        "seconds": round(seconds, 1),
        "events_per_s": round(produced / seconds, 1),
        "kafka_bytes": on_disk,
        "bytes_per_event": round(on_disk / produced, 1),
        "compression": _topic_compression(topic),
    }


def _poll_source(
    job_id: str,
    src: dict,
    samples: list[dict],
    total: int,
    deadline: float,
    sample_fn: Callable | None = None,
    monotonic: Callable | None = None,
    sleep: Callable | None = None,
) -> tuple[str, str]:
    """Take a sample every SAMPLE_INTERVAL_S until the count reaches `total`.

    Returns `("", "")` when it did, else `(kind, reason)`: "restarted" (the count fell),
    "vanished" (the metrics went) or "deadline". Each sample is compared with the last fresh one.
    A restart is a count or clock below that sample's on two samples in a row.
    """
    sample_fn = sample_fn or bench_flink.sample
    monotonic = monotonic or time.monotonic
    sleep = sleep or time.sleep
    due = monotonic()
    regressed_in_a_row = 0
    while True:
        burst_start = monotonic()
        fresh = [x for x in samples if not x["stale"]]
        last_fresh = fresh[-1] if fresh else None
        try:
            s = sample_fn(job_id, src["id"], src["parallelism"], previous=last_fresh)
        except KeyError as e:
            if samples:
                return "vanished", f"the source's metrics vanished: {e}"
            s = None  # tasks not up yet
        if s is not None:
            samples.append(s)
            # A restart resets the count and the accumulated times together, and every later
            # sample is then "stale" against the last fresh one; so a restart is a count or a
            # clock below the last fresh sample's, stale or not, twice in a row.
            regressed = last_fresh is not None and (
                s["count"] < last_fresh["count"] or s["clock_ms"] < last_fresh["clock_ms"]
            )
            regressed_in_a_row = regressed_in_a_row + 1 if regressed else 0
            if regressed_in_a_row >= 2:
                return "restarted", f"the source count fell to {s['count']}: the job restarted"
            if s["count"] >= total:
                return "", ""
        if monotonic() > deadline:
            return "deadline", "the count did not reach the fill before the deadline"
        due = next_due(due, burst_start)
        sleep(max(0.0, due - monotonic()))


def power_reasons(runs: list[dict], on_battery: bool) -> list[str]:
    """Why the power at the end of a run makes it invalid: battery without `--on-battery`."""
    if on_battery:
        return []
    return [
        f"the laptop went to battery during the run {r['run_id']}"
        for r in runs
        if r.get("power_end") and "AC Power" not in r["power_end"]
    ]


def _load() -> dict:
    """The machine's load; a failed read is recorded as the error, never fails the run."""
    try:
        return bench_env.load_record()
    except Exception as e:
        return {
            "host_load_1m": None,
            "project_containers_cpu_pct": None,
            "other_containers_cpu_pct": None,
            "error": str(e),
        }


def _aborted(run_id, job_id, total, reason, restarts, samples, load_start, load_end) -> dict:
    return {
        "run_id": run_id,
        "job_id": job_id,
        "events": total,
        "aborted": reason,
        "restarts": restarts,
        "late_events": None,  # not read: the job did not finish
        "stale_samples": sum(x["stale"] for x in samples),
        "stall_gaps_s": _gaps(samples),
        "host_sleep_s": _host_sleep(samples),
        "load_start": load_start,
        "load_end": load_end,
    }


def _one_run(n: int, fill_id: str, total: int, args, run_dir: Path) -> dict:
    run_id = f"{fill_id}-r{n}"
    topic = f"{fill_id}.events"
    alerts_from = kafka_io.end_offsets(ALERTS_TOPIC)
    load_start = _load()
    job_id = bench_flink.submit(run_id, topic)
    samples: list[dict] = []
    timeout_s = max(MIN_RUN_TIMEOUT_S, total / MIN_EVENTS_PER_S)
    print(
        f"bench: {run_id} fails below {MIN_EVENTS_PER_S} events/s ({timeout_s / 60:.0f} min limit)"
    )
    try:
        _wait_running(job_id)
        job = bench_flink._json(bench_flink.httpx.get, f"/jobs/{job_id}")
        src = bench_flink.vertex(job, "Source: events")
        kind, stop = _poll_source(job_id, src, samples, total, time.monotonic() + timeout_s)
        if kind == "deadline":
            raise RunFailed(f"{run_id}: {stop}")
        load_end = _load()
        if kind:
            restarts = bench_flink.restarts(job_id)
            if kind == "restarted" and restarts == 0:
                restarts = 1  # the count fell, so the job did restart; the history lags
            bench_flink.cancel_and_clean(job_id, run_id)
            return _aborted(run_id, job_id, total, stop, restarts, samples, load_start, load_end)
        check_final_count(samples[-1]["count"], total)
        time.sleep(METRIC_CACHE_WAIT_S)
        fresh = [x for x in samples if not x["stale"]]
        if not fresh:
            raise RunFailed(f"{run_id}: no sample was fresh; cannot check the late events")
        fresh_after = (src["id"], src["parallelism"], fresh[-1])
        try:
            late = bench_flink.late_events(job_id, fresh_after=fresh_after)
        except RunFailed:
            restarts = bench_flink.restarts(job_id)
            if not restarts:
                raise
            # a restart reset the clock, so the late-events read looked stale
            bench_flink.cancel_and_clean(job_id, run_id)
            reason = f"the job restarted {restarts} time(s); the late-events read was stale"
            return _aborted(run_id, job_id, total, reason, restarts, samples, load_start, load_end)
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
    finally:
        (run_dir / f"samples-r{n}.json").write_text(json.dumps(samples, allow_nan=False))
    bench_flink.cancel_and_clean(job_id, run_id)
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
        "load_start": load_start,
        "load_end": load_end,
    }


def _gaps(samples: list[dict]) -> list[float]:
    """Stalls of the driver between polls. A host sleep is `_host_sleep`'s."""
    return [round(g, 1) for g in bench_env.gaps([s["mono"] for s in samples])]


def _host_sleep(samples: list[dict]) -> float:
    """The OS sleep counter's rise over the run's samples (the monotonic clock stops in a sleep)."""
    missing = [i for i, s in enumerate(samples) if "slept" not in s]
    if missing:
        raise ValueError(f"samples {missing} carry no sleep-counter reading")
    return round(bench_env.host_sleep_s([s["slept"] for s in samples]), 1)


def _source_clock_rate(a: dict, b: dict, name: str = "Source: events") -> float:
    """Events per second over the source's own clock: the count delta over the clock's rise per
    subtask. A cross-check beside the host rate; it does not depend on the host's timing."""
    par = b["vertices"][name]["parallelism"]
    seconds = (b["clock_ms"] - a["clock_ms"]) / par / 1000
    return round((b["count"] - a["count"]) / seconds, 1)


def _summarise(run_id: str, job_id: str, total: int, samples: list[dict], pilot: bool) -> dict:
    t0 = samples[0]["t"]
    rebased = [{**s, "t": s["t"] - t0} for s in samples]  # unrounded: round only what is reported
    out: dict = {
        "run_id": run_id,
        "job_id": job_id,
        "events": total,
        "stale_samples": sum(s["stale"] for s in samples),
        "reread_samples": sum(s["reread"] for s in samples),
        "max_read_s": round(max(s["read_s"] for s in samples), 1),
    }
    try:
        rate = sustained_rate(rebased, total)
        first, last = window(rebased, total)
    except WindowTooShort as e:
        if not pilot:
            raise
        out["window_error"] = str(e)
        fresh = _fresh(samples)
        rate = {}
        first, last = (fresh[0], fresh[-1]) if fresh else (0, 0)
    out |= rate
    if last > first:
        shares = window_shares(rebased, first, last)
        out["events_per_s_source_clock"] = _source_clock_rate(samples[first], samples[last])
    else:  # fewer than two fresh samples: there is no window to take shares over
        shares = {}
        out["window_error"] = out.get("window_error", "") + "; fewer than two fresh samples"
    out["source"] = _share_of(shares, "Source: events") if shares else None
    out["drop_ticks_busy"] = _share_of(shares, "drop-ticks")["busy"] if shares else None
    out["shares"] = shares
    out["busiest_vertex"] = max(shares, key=lambda n: shares[n]["busy"]) if shares else None
    out["stall_gaps_s"] = _gaps(samples)
    out["host_sleep_s"] = _host_sleep(samples)
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Throughput benchmark (synthetic data)")
    ap.add_argument("--minutes", type=int, default=240)
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
        r["power_end"] = bench_env._power_line(run)
        runs.append(r)
        if "aborted" in r:
            print(f"bench: run {n} stopped: {r['aborted']}")
            break
        print(
            f"bench: run {n}: {r.get('events_per_s', 'no window')} events/s, source idle"
            f" {(r['source'] or {}).get('idle')}, busiest {r['busiest_vertex']}, stale samples"
            f" {r['stale_samples']}, min interval rate {r.get('min_interval_rate')}"
        )
    extra = [f"run {r['run_id']}: {r['window_error']}" for r in runs if "window_error" in r]
    extra += [f"run {r['run_id']}: {r['aborted']}" for r in runs if "aborted" in r]
    extra += power_reasons(runs, args.on_battery)
    stalls = [g for r in runs for g in r["stall_gaps_s"]]
    valid, reasons = bench_env.validity(
        sum(r["restarts"] for r in runs),
        sum(r["late_events"] or 0 for r in runs),
        stalls,
        extra,
        host_sleep_s=sum(r["host_sleep_s"] for r in runs),
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
        "warmup_s": WARMUP_S,
        "end_share": END_SHARE,
        "sample_interval_s": SAMPLE_INTERVAL_S,
        "settings": _settings(),
        "runs": runs,
        "spread_pct": spread,
    }
    path = bench_env.write_result("throughput", result, run_dir, args.pilot)
    print(f"bench: wrote {path}{'' if valid else ' (INVALID: ' + '; '.join(reasons) + ')'}")


if __name__ == "__main__":
    main()

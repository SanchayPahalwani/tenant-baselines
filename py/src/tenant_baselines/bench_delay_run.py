"""The delay benchmark: three weeks of history, then live traffic with injected incidents.

Synthetic data only. A generator container writes the events and the incident labels; the live job
reads them; the driver reads the job's alerts from `bench.alerts` and times them against the
broker's append time. Run with `make bench-delay`.
"""

import argparse
import json
import statistics
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines import (
    bench_delay,
    bench_env,
    bench_flink,
    bench_throughput,
    bench_world,
    kafka_io,
)
from tenant_baselines.eval_run import RunFailed
from tenant_baselines.freeze import REPO

__all__ = [
    "LagReader",
    "generator_command",
    "lag_summary",
    "main",
    "make_run_dir",
    "offset_moved",
    "plan",
    "power_reasons",
    "rows_of_run",
    "wait_caught_up",
    "watch",
]

PARTITIONS = 8
HISTORY_DAYS = 21
POLL_S = 10  # the waits before the watch
WATCH_POLL_S = 30
PARTITIONS_TIMEOUT_S = 120
LABELS_TIMEOUT_S = 15 * 60
CAUGHT_UP_LAG_S = 60
CATCH_UP_MARGIN_MS = 60_000  # the job must be caught up this long before the first incident
METRIC_CACHE_WAIT_S = 11
OFFSET_LIMIT_S = 1.0
STALE_RUN_LIMIT = 2  # a clock below the last fresh one this many times in a row: a restart
DATA_BENCH = REPO / "data" / "bench"
ALERTS_TOPIC = "bench.alerts"
HEADLINE = ("seasonal", "k4")


def plan(labels: dict) -> dict:
    first = min(i["start_ms"] for i in labels["incidents"])
    return {"first_start_ms": first, "read_at_ms": bench_delay.run_end_ms(labels)}


def lag_summary(samples_s: list[float]) -> dict:
    if not samples_s:
        return {"samples": 0, "min": None, "median": None, "max": None}
    return {
        "samples": len(samples_s),
        "min": round(min(samples_s), 1),
        "median": round(statistics.median(samples_s), 1),
        "max": round(max(samples_s), 1),
    }


def offset_moved(before: dict, after: dict, limit_s: float = OFFSET_LIMIT_S) -> bool:
    return abs(after["offset_s"] - before["offset_s"]) > limit_s


def container_name(run_id: str) -> str:
    return f"tb-{run_id}"


def generator_command(run_id: str, incidents: int, out_dir: Path) -> list[str]:
    """Run the compose `generator` image detached, with the labels directory mounted at /out."""
    return [
        *("docker", "compose", "--profile", "demo", "run", "-d", "--no-deps"),
        *("--name", container_name(run_id), "-v", f"{out_dir}:/out", "generator"),
        *("python", "-m", "tenant_baselines.gen_live", "--bootstrap", "kafka:19092"),
        *("--topic", f"{run_id}.events", "--world", "delay", "--seed", "1"),
        *("--script", "bench", "--bench-incidents", str(incidents)),
        *("--labels-out", "/out/labels.json", "--history-days", str(HISTORY_DAYS)),
        *("--partitions", str(PARTITIONS)),
    ]


def rows_of_run(records: list[tuple[int, dict]], run_id: str) -> list[tuple[int, dict]]:
    """`bench.alerts` is shared by every bench run, and alert ids hash no run id."""
    return [(at, r) for at, r in records if r.get("run_id") == run_id]


def make_run_dir(base: Path, run_id: str) -> Path:
    """The run's directory. A run id is never reused: an existing one is an error."""
    path = Path(base) / run_id
    path.mkdir(parents=True)
    return path


def power_reasons(power_end: str | None, on_battery: bool) -> list[str]:
    """Battery at the end of the run makes it invalid, unless `--on-battery` was given."""
    if on_battery or not power_end or "AC Power" in power_end:
        return []
    return ["the laptop went to battery during the run"]


def lag_reasons(block: dict) -> list[str]:
    reasons = []
    if block["summary"]["samples"] == 0:
        reasons.append("no watermark-lag sample from the first incident on was kept")
    elif block["stale_lag_samples"] > block["samples_from_first_incident"] / 2:
        reasons.append("more than half of the lag samples from the first incident on were stale")
    at = block["at_first_incident_s"]
    if at is not None and at >= CAUGHT_UP_LAG_S:
        reasons.append(
            f"the watermark lag at the first incident's start was {at:.1f} s, not under 60 s"
        )
    return reasons


class LagReader:
    """Watermark-lag samples, each tested for freshness against the last fresh one.

    A stale sample (the source's clock did not advance) is kept in `samples` but never used for a
    lag. A restart resets the clock, so every later sample is stale; a clock below the last fresh
    one twice in a row is a restart, and raises.
    """

    def __init__(self, job_id: str, offset_s: float, source: tuple[str, int], sample_fn=None):
        self.job_id, self.offset_s, self.source = job_id, offset_s, source
        self._sample = sample_fn or bench_flink.watermark_lag_sample
        self.samples: list[dict] = []
        self._regressed = 0

    def _last_fresh(self) -> dict | None:
        return next((s for s in reversed(self.samples) if not s["stale"]), None)

    def read(self) -> dict:
        last = self._last_fresh()
        s = self._sample(self.job_id, self.offset_s, self.source, previous=last)
        self.samples.append(s)
        regressed = last is not None and s["clock_ms"] < last["clock_ms"]
        self._regressed = self._regressed + 1 if regressed else 0
        if self._regressed >= STALE_RUN_LIMIT:
            raise RunFailed("the source's clock fell: the job restarted")
        return s

    def block(self, first_start_ms: int, caught_up_lag: float) -> dict:
        """The summary of the lag samples from the first incident's start, and how many reads
        were dropped for being stale or having no watermark yet."""
        kept = [
            s
            for s in self.samples
            if (s["t"] + self.offset_s) * 1000 >= first_start_ms
            and not s["stale"]
            and s["lag_s"] is not None
        ]
        late = [s for s in self.samples if (s["t"] + self.offset_s) * 1000 >= first_start_ms]
        return {
            "summary": lag_summary([s["lag_s"] for s in kept]),
            "samples_from_first_incident": len(late),
            "stale_lag_samples": sum(s["stale"] for s in late),
            "no_watermark_samples": sum(not s["stale"] and s["lag_s"] is None for s in late),
            "caught_up_lag_s": round(caught_up_lag, 1),
            "at_first_incident_s": round(kept[0]["lag_s"], 1) if kept else None,
        }


def _vm_now_ms(offset_s: float, time_) -> float:
    return (time_.time() + offset_s) * 1000


def wait_caught_up(
    reader: LagReader,
    first_start_ms: int,
    offset_s: float,
    *,
    polls: list[float],
    check: Callable[[], None],
    time_=None,
) -> float:
    """Poll the lag every POLL_S until a fresh sample is below 60 s; returns that lag.

    The generator "catches up" when it has produced the history, not when the job has read it. If
    the lag is still too large one minute before the first incident starts, the run fails.
    """
    time_ = time_ or time
    while True:
        polls.append(time_.monotonic())
        check()
        s = reader.read()
        if not s["stale"] and s["lag_s"] is not None and s["lag_s"] < CAUGHT_UP_LAG_S:
            return s["lag_s"]
        if _vm_now_ms(offset_s, time_) >= first_start_ms - CATCH_UP_MARGIN_MS:
            seen = (
                "no fresh lag read"
                if s["stale"] or s["lag_s"] is None
                else f"the watermark lag was still {s['lag_s']:.1f} s"
            )
            raise RunFailed(
                f"the job did not catch up in time: {seen} one minute before the first incident"
            )
        time_.sleep(POLL_S)


def watch(
    reader: LagReader,
    read_at_ms: int,
    offset_s: float,
    *,
    polls: list[float],
    check: Callable[[], None],
    time_=None,
) -> None:
    """Until `read_at_ms` on the VM's clock: a poll every WATCH_POLL_S, a lag sample on every
    second poll, and a check that the generator still runs. No other REST call."""
    time_ = time_ or time
    due = time_.monotonic()
    n = 1  # the first poll only waits: a sample then would be a stale read, 4 s after catch-up's
    while _vm_now_ms(offset_s, time_) < read_at_ms:
        polls.append(time_.monotonic())
        check()
        if n % 2 == 0:
            reader.read()
        n += 1
        due = max(due + WATCH_POLL_S, time_.monotonic() + 1)
        time_.sleep(max(0.0, due - time_.monotonic()))


def _generator_running(name: str, run: Callable) -> bool:
    return run(["docker", "inspect", "-f", "{{.State.Running}}", name]).stdout.strip() == "true"


def _save_generator_log(name: str, run: Callable, run_dir: Path) -> int:
    """Write the container's log to `generator.log`; returns the count of failed deliveries."""
    done = run(["docker", "logs", name])
    text = done.stdout + done.stderr
    (run_dir / "generator.log").write_text(text)
    return sum("delivery failed" in line for line in text.splitlines())


def _remove_generator(name: str, run: Callable) -> None:
    run(["docker", "rm", "-f", name])


def _wait_partitions(topic: str, polls: list[float], check: Callable[[], None]) -> None:
    """Until every partition has data: a partition that is idle at the job's start would take
    its history as late events (see scripts/demo.sh)."""
    deadline = time.monotonic() + PARTITIONS_TIMEOUT_S
    while True:
        polls.append(time.monotonic())
        check()
        ends = kafka_io.end_offsets(topic)
        if ends and all(o > 0 for o in ends.values()):
            return
        if time.monotonic() > deadline:
            raise RunFailed(
                f"{topic} had no data in every partition after {PARTITIONS_TIMEOUT_S} s"
            )
        time.sleep(POLL_S)


def _wait_labels(path: Path, polls: list[float], check: Callable[[], None]) -> dict:
    deadline = time.monotonic() + LABELS_TIMEOUT_S
    while True:
        polls.append(time.monotonic())
        check()
        if path.is_file():
            return json.loads(path.read_text())
        if time.monotonic() > deadline:
            raise RunFailed(f"no {path.name} after {LABELS_TIMEOUT_S // 60} minutes")
        time.sleep(POLL_S)


def _find_job(run_id: str) -> str | None:
    """The id of a running job whose name or configuration carries `run_id`, else None."""
    get = bench_flink.httpx.get
    for job in bench_flink._json(get, "/jobs/overview")["jobs"]:
        if job["state"] in ("FINISHED", "CANCELED", "FAILED"):
            continue
        config = json.dumps(bench_flink._json(get, f"/jobs/{job['jid']}/config"))
        if run_id in job["name"] or run_id in config:
            return job["jid"]
    return None


def _cleanup(job_id: str | None, run_id: str, name: str, run: Callable, run_dir: Path) -> None:
    """The one rule for every failure after the generator started: cancel the job and clean its
    checkpoints, save the generator's log, then remove the generator. Each step is tried. With no
    job id (an interrupt during the submit) the job is looked up by the run id."""
    try:
        job_id = job_id or _find_job(run_id)
        if job_id:
            bench_flink.cancel_and_clean(job_id, run_id)
        else:
            print(f"bench: no running job carries {run_id}; none to cancel")
    except Exception as e:
        print(f"bench: could not clean {run_id}: {e}")
    try:
        _save_generator_log(name, run, run_dir)
    except Exception as e:
        print(f"bench: could not save the log of {name}: {e}")
    try:
        _remove_generator(name, run)
    except Exception as e:
        print(f"bench: could not remove {name}: {e}")


def _world_block(world, partitions: int, history_days: int) -> dict:
    return {
        "keys": len(world.keys),
        "rpm_per_key": world.tenants[0].peak_rpm,
        "partitions": partitions,
        "history_days": history_days,
    }


def _schedule_block() -> dict:
    return {
        "settle_s": bench_delay.SETTLE_MS // 1000,
        "spacing_s": bench_delay.SPACING_MS // 1000,
        "duration_s": bench_delay.DURATION_MS // 1000,
        "magnitude": bench_delay.MAGNITUDE,
        "tail_s": bench_delay.TAIL_MS // 1000,
    }


def _execute(args, run_id: str, run_dir: Path, env: dict, before: dict, polls: list) -> dict:
    run = bench_env._sh
    topic = f"{run_id}.events"
    name = container_name(run_id)
    load_start = bench_throughput._load()
    alerts_from = kafka_io.end_offsets(ALERTS_TOPIC)
    holder: dict = {"job_id": None, "reader": None}

    def check() -> None:
        if not _generator_running(name, run):
            raise RunFailed(f"the generator container {name} exited")

    try:
        done = run(generator_command(run_id, args.incidents, run_dir))
        if done.returncode != 0:
            raise RunFailed(f"could not start the generator: {done.stderr.strip()}")
        _wait_partitions(topic, polls, check)
        job_id = holder["job_id"] = bench_flink.submit(run_id, topic)
        polls.append(time.monotonic())
        bench_throughput._wait_running(job_id)
        polls.append(time.monotonic())
        job = bench_flink._json(bench_flink.httpx.get, f"/jobs/{job_id}")
        src = bench_flink.vertex(job, "Source: events")
        offset = before["offset_s"]
        reader = holder["reader"] = LagReader(job_id, offset, (src["id"], src["parallelism"]))
        labels = _wait_labels(run_dir / "labels.json", polls, check)
        when = plan(labels)
        caught_up_lag = wait_caught_up(
            reader, when["first_start_ms"], offset, polls=polls, check=check
        )
        watch(reader, when["read_at_ms"], offset, polls=polls, check=check)

        alerts_to = kafka_io.end_offsets(ALERTS_TOPIC)
        records = rows_of_run(bench_env.read_stamped(ALERTS_TOPIC, alerts_from, alerts_to), run_id)
        time.sleep(METRIC_CACHE_WAIT_S)
        late, restarts = _late_and_restarts(job_id, src, reader)
        stopped = run(["docker", "stop", name])
        failed = _save_generator_log(name, run, run_dir)
        _remove_generator(name, run)
        if stopped.returncode != 0:
            raise RunFailed(f"could not stop the generator: {stopped.stderr.strip()}")
        bench_flink.cancel_and_clean(job_id, run_id)
        after = bench_env.vm_clock_offset()
        load_end = bench_throughput._load()
        power_end = bench_env._power_line(run)
    except BaseException:
        _cleanup(holder["job_id"], run_id, name, run, run_dir)
        raise
    finally:
        reader = holder["reader"]
        (run_dir / "samples-lag.json").write_text(
            json.dumps({"polls": polls, "lag": reader.samples if reader else []}, allow_nan=False)
        )

    lag = reader.block(when["first_start_ms"], caught_up_lag)
    headline = {
        "detector": HEADLINE[0],
        "setting": HEADLINE[1],
        **bench_delay.measure(labels, records, *HEADLINE),
    }
    by_detector = {
        f"{d}/{s}": bench_delay.measure(labels, records, d, s)
        for d, s in bench_delay.pairs(records)
    }
    extra = []
    if not bench_delay.enough_matched(headline):
        extra.append("fewer than 90% of the incidents were matched")
    if offset_moved(before, after):
        extra.append("the VM clock moved against the host's by more than 1 s")
    if failed:
        extra.append("the generator reported failed deliveries")
    extra += power_reasons(power_end, args.on_battery) + lag_reasons(lag)
    sleeps = [round(g, 1) for g in bench_env.gaps(polls)]
    valid, reasons = bench_env.validity(restarts, late or 0, sleeps, extra)
    world = bench_world.build("delay", 1)
    return {
        "data": "synthetic",
        "env": env,
        "valid": valid,
        "invalid_reasons": reasons,
        "run_id": run_id,
        "job_id": job_id,
        "world": _world_block(world, PARTITIONS, HISTORY_DAYS),
        "incidents": args.incidents,
        "schedule": _schedule_block(),
        "design": bench_delay.DESIGN,
        "settings": bench_throughput._settings(),
        "headline": headline,
        "by_detector": by_detector,
        "watermark_lag_s": lag["summary"],
        "lag_check": {k: v for k, v in lag.items() if k != "summary"},
        "clock_offset": {"before": before, "after": after},
        "generator": {"caught_up_ms": labels["t0_ms"], "failed_deliveries": failed},
        "late_events": late,
        "restarts": restarts,
        "host_sleep_gaps_s": sleeps,
        "power_end": power_end,
        "load_start": load_start,
        "load_end": load_end,
    }


def _late_and_restarts(job_id: str, src: dict, reader: LagReader) -> tuple[int | None, int]:
    """The late events, read fresh against the last fresh lag sample; None when a restart left no
    fresh read."""
    fresh = [s for s in reader.samples if not s["stale"]]
    if not fresh:
        restarts = bench_flink.restarts(job_id)
        if restarts:
            return None, restarts
        raise RunFailed("no lag sample was fresh; cannot check the late events")
    after = (src["id"], src["parallelism"], fresh[-1])
    for attempt in (0, 1):
        try:
            late = bench_flink.late_events(job_id, fresh_after=after)
            break
        except RunFailed:
            restarts = bench_flink.restarts(job_id)
            if restarts:
                return None, restarts  # a restart reset the clock, so the read looked stale
            if attempt == 1:
                raise
            time.sleep(METRIC_CACHE_WAIT_S)  # the cache may refresh; try once more
    return late, bench_flink.restarts(job_id)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Delay benchmark (synthetic data)")
    ap.add_argument("--incidents", type=int, default=60)
    ap.add_argument("--on-battery", action="store_true")
    ap.add_argument("--pilot", action="store_true")
    args = ap.parse_args(argv)
    if args.incidents <= 0:
        ap.error("--incidents must be above 0")

    bench_env.preflight(args.on_battery)
    env = bench_env.collect(args.on_battery)
    before = bench_env.vm_clock_offset()
    run_id = "delay-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    run_dir = make_run_dir(DATA_BENCH, run_id)
    kafka_io.create_topic(f"{run_id}.events", PARTITIONS)
    polls: list[float] = []
    result = _execute(args, run_id, run_dir, env, before, polls)
    path = bench_env.write_result("delay", result, run_dir, args.pilot)
    h = result["headline"]
    print(f"bench: {h['matched']} of {h['incidents']} incidents matched ({run_id})")
    print(f"bench: pipeline delay {h['pipeline_delay_s']}")
    print(f"bench: time to page {h['time_to_page_s']}")
    why = "" if result["valid"] else " (INVALID: " + "; ".join(result["invalid_reasons"]) + ")"
    print(f"bench: wrote {path}{why}")


if __name__ == "__main__":
    main()

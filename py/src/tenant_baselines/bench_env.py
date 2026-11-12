"""The benchmark's preflight, environment record, clock offset, stamped reads and result files.

Synthetic data only. Shared by the three bench drivers. Nothing here changes the job or the sealed
eval modules: `kafka_io` is only imported from.
"""

import json
import re
import subprocess
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, TopicPartition

from tenant_baselines import kafka_io
from tenant_baselines.eval_official import PreflightFailed, check_machine
from tenant_baselines.eval_run import REST
from tenant_baselines.freeze import REPO, dirty_paths, head_commit

__all__ = [
    "ClockReadFailed",
    "NotAppendTime",
    "PreflightFailed",
    "collect",
    "gaps",
    "preflight",
    "read_stamped",
    "validity",
    "vm_clock_offset",
    "write_result",
]

JAR = REPO / "flink-job" / "target" / "tenant-baselines-job.jar"
PERF_DIR = REPO / "docs" / "performance"
_OVER = ("FINISHED", "CANCELED", "FAILED")
_CLOCK = re.compile(r"\d+\.\d{9}")
_GB = 1024**3


class NotAppendTime(ValueError):
    pass


class ClockReadFailed(PreflightFailed):
    pass


def _sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)


def _json(get: Callable, path: str):
    return get(f"{REST}{path}", timeout=10).raise_for_status().json()


def _power_line(run: Callable) -> str | None:
    """The first line of `pmset -g ps`; None when there is no pmset."""
    try:
        out = run(["pmset", "-g", "ps"]).stdout
    except FileNotFoundError:
        return None
    return out.splitlines()[0] if out.strip() else ""


def preflight(on_battery: bool = False, run: Callable = _sh, get: Callable = httpx.get) -> None:
    """Raise PreflightFailed, naming the fix, unless a bench run may start."""
    if not on_battery:
        try:
            ps = run(["pmset", "-g", "ps"])
        except FileNotFoundError:
            print("bench: pmset not found; skipping the power check")
        else:
            if "AC Power" not in ps.stdout:
                raise PreflightFailed("the laptop is on battery; plug it in, or pass --on-battery")
    check_machine(True, run)  # the power check is done above; this is the VM clock
    try:
        jobs = _json(get, "/jobs/overview")["jobs"]
    except httpx.HTTPError as e:
        raise PreflightFailed(f"could not read the Flink REST API at {REST}: {e}") from e
    for job in jobs:
        if job["state"] not in _OVER:
            raise PreflightFailed(
                f"job {job['name']} is {job['state']}; cancel it, or `make down` and `make up`"
            )
    if not JAR.is_file():
        raise PreflightFailed(f"no job jar at {JAR}; run `make jar`")


def _out(run: Callable, cmd: list[str], strip: bool = True) -> str:
    """The stdout of a command; raises on a non-zero exit, so a failed command records nothing."""
    done = run(cmd)
    if done.returncode != 0:
        raise RuntimeError(f"`{' '.join(cmd)}` exited {done.returncode}: {done.stderr.strip()}")
    return done.stdout.strip() if strip else done.stdout


def _result_file(path: str) -> bool:
    """The files a bench run writes: they do not make the tree dirty."""
    return path == "docs/performance.md" or path.startswith("docs/performance/")


def collect(on_battery: bool, run: Callable = _sh, get: Callable = httpx.get) -> dict:
    """The environment record of a bench run. Other containers are counted, never named."""
    status = _out(run, ["git", "status", "--porcelain", "-z"], strip=False)
    dirty = [p for p in dirty_paths("bench", status) if not _result_file(p)]
    info = json.loads(_out(run, ["docker", "info", "--format", "{{json .}}"]))
    running = set(_out(run, ["docker", "ps", "-q"]).split())
    ours = set(_out(run, ["docker", "compose", "ps", "-q"]).split())
    cfg = {e["key"]: e["value"] for e in _json(get, "/jobmanager/config")}
    wanted = (
        "parallelism.default",
        "taskmanager.numberOfTaskSlots",
        "taskmanager.memory.process.size",
    )
    images = _out(run, ["docker", "compose", "config", "--images"]).splitlines()
    return {
        "data": "synthetic",
        "date_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "commit": head_commit(),
        "dirty": bool(dirty),
        "on_battery": on_battery,
        "host": {
            "cpu": _out(run, ["sysctl", "-n", "machdep.cpu.brand_string"]),
            "cores": int(_out(run, ["sysctl", "-n", "hw.ncpu"])),
            "memory_gb": round(int(_out(run, ["sysctl", "-n", "hw.memsize"])) / _GB, 1),
            "os": _out(run, ["sw_vers", "-productVersion"]),
            "arch": _out(run, ["uname", "-m"]),
        },
        "power": _power_line(run),
        "vm": {
            "cpus": info["NCPU"],
            "memory_gb": round(info["MemTotal"] / _GB, 1),
            "server_version": info["ServerVersion"],
        },
        "other_containers": len(running - ours),
        "images": sorted(x for x in images if x),
        "flink": {
            "version": _json(get, "/config")["flink-version"],
            **{k: cfg.get(k) for k in wanted},
        },
    }


def vm_clock_offset(run: Callable = _sh, now: Callable = time.time, tries: int = 5) -> dict:
    """The jobmanager's clock minus the host's: `{"offset_s", "bracket_s"}` of the tightest try.

    Each try brackets the VM reading between two host readings; the offset assumes the VM read at
    the midpoint, so it is good to within half the bracket.
    """
    best: dict | None = None
    for _ in range(tries):
        before = now()
        out = run(["docker", "compose", "exec", "-T", "jobmanager", "date", "+%s.%N"])
        after = now()
        if not _CLOCK.fullmatch(out.stdout.strip()):
            raise ClockReadFailed(
                f"could not read the jobmanager's clock (exit {out.returncode}): got"
                f" {out.stdout!r}, wanted seconds with nine decimals; is the stack up?"
            )
        vm = float(out.stdout.strip())
        try_ = {"offset_s": vm - (before + after) / 2, "bracket_s": after - before}
        if best is None or try_["bracket_s"] < best["bracket_s"]:
            best = try_
    if best is None:
        raise ValueError("tries must be at least 1")
    return best


def gaps(monotonic_s: list[float], max_gap_s: float = 60) -> list[float]:
    """Every interval between neighbouring readings longer than `max_gap_s`: a host sleep."""
    return [b - a for a, b in zip(monotonic_s, monotonic_s[1:], strict=False) if b - a > max_gap_s]


def validity(
    restarts: int, late_events: int, sleep_gaps: list[float], extra: Iterable[str] = ()
) -> tuple[bool, list[str]]:
    """Why a run is invalid, in plain words; valid when there is no reason."""
    reasons = []
    if restarts:
        reasons.append(f"the job restarted {restarts} time(s)")
    if late_events:
        reasons.append(f"{late_events} event(s) were dropped as late")
    reasons += [f"the host stalled for {g:.0f} s between two polls" for g in sleep_gaps]
    reasons += list(extra)
    return not reasons, reasons


def read_stamped(
    topic: str,
    start: dict[int, int],
    end: dict[int, int],
    consumer=None,
    bootstrap: str = kafka_io.BOOTSTRAP,
) -> list[tuple[int, dict]]:
    """Every record of `[start, end)` in each partition as `(broker timestamp ms, parsed JSON)`.

    Complete or it raises kafka_io.IncompleteRead. Raises NotAppendTime unless the broker stamped
    each record with its append time: a create-time stamp would be the producer's, not the broker's.
    """
    own = consumer is None
    c = kafka_io._consumer(bootstrap) if own else consumer
    try:
        return _read_stamped(c, topic, start, end)
    finally:
        if own:
            c.close()


def _read_stamped(c, topic: str, start: dict[int, int], end: dict[int, int]) -> list:
    if set(start) != set(end):
        raise ValueError(f"start and end name different partitions: {sorted(start)}, {sorted(end)}")
    for p in sorted(start):
        if end[p] < start[p]:
            raise kafka_io.IncompleteRead(f"{topic}[{p}]: end {end[p]} is before start {start[p]}")
        low, _ = c.get_watermark_offsets(TopicPartition(topic, p), timeout=kafka_io._TIMEOUT_S)
        if start[p] < low:
            raise kafka_io.IncompleteRead(
                f"{topic}[{p}]: start {start[p]} is below the low watermark {low}"
            )
    want = {p: start[p] for p in start if end[p] > start[p]}
    rows: dict[int, list[tuple[int, dict]]] = {p: [] for p in start}
    if want:
        c.assign([TopicPartition(topic, p, o) for p, o in sorted(want.items())])
    idle_since = time.monotonic()
    while want:
        batch = c.consume(num_messages=kafka_io._BATCH, timeout=1.0)
        if not batch:
            if time.monotonic() - idle_since >= kafka_io.READ_IDLE_S:
                break
            continue
        idle_since = time.monotonic()
        for msg in batch:
            if msg.error() is not None:
                raise kafka_io.IncompleteRead(f"{topic}: {msg.error()}")
            p, offset = msg.partition(), msg.offset()
            if p not in want:
                continue
            if offset != want[p]:
                raise kafka_io.IncompleteRead(
                    f"{topic}[{p}]: offset {offset} where {want[p]} was next"
                )
            kind, ms = msg.timestamp()
            if kind != TIMESTAMP_LOG_APPEND_TIME:
                raise NotAppendTime(
                    f"{topic}[{p}] offset {offset} is not stamped with the broker's append time;"
                    " the topic was not made by this project's tools. Run `make reset`."
                )
            rows[p].append((ms, orjson.loads(msg.value())))
            want[p] += 1
            if want[p] == end[p]:
                del want[p]
    expected = sum(end[p] - start[p] for p in start)
    got = sum(len(r) for r in rows.values())
    if got != expected:
        raise kafka_io.IncompleteRead(f"{topic}: read {got} records of {expected} in the range")
    return [r for p in sorted(rows) for r in rows[p]]


def write_result(name: str, result: dict, run_dir: Path, pilot: bool) -> Path:
    """Write `<run_dir>/result.json`, and `docs/performance/<name>.json` too unless a pilot."""
    text = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "result.json"
    path.write_text(text)
    if not pilot:
        PERF_DIR.mkdir(parents=True, exist_ok=True)
        (PERF_DIR / f"{name}.json").write_text(text)
    return path

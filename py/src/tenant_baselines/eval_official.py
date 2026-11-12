"""The official driver: the seed protocol of spec 5.3 as code.

Synthetic data only. Until the freeze commit only the dev seeds run (`eval_run`). After it, each
official (seed, amplitude) pair runs once and is logged in `docs/results/official-runs.jsonl`
before, and after, its run. Guards: a clean tree, frozen code, the frozen jar, AC power, and a
VM clock that agrees with the host's. A rerun needs a reason: `bug:` or `infrastructure:`.
"""

import argparse
import hashlib
import json
import logging
import re
import shutil
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines import eval_run
from tenant_baselines.eval_run import AMPLITUDES, DATA_DIR, DEV_SEEDS, REPO, RunFailed
from tenant_baselines.evalmodel import TrafficParams, rates
from tenant_baselines.kafka_io import IncompleteRead, topics
from tenant_baselines.scenarios import PlacementError, place
from tenant_baselines.world import build_world

log = logging.getLogger(__name__)

FREEZE_PATH = REPO / "eval" / "freeze.json"
REGISTRY_PATH = REPO / "docs" / "results" / "official-runs.jsonl"
RUN_LOG_PATH = REPO / "docs" / "results" / "run-log.jsonl"
SUMMARIES_DIR = REPO / "docs" / "results" / "summaries"
JAR_PATH = REPO / "flink-job" / "target" / "tenant-baselines-job.jar"
FROZEN_PATHS = ("flink-job", "py", "infra", "scripts", "eval", "Makefile", "docker-compose.yml")
# Paths the driver writes between runs. results.md is rendered by `make eval` after the loop.
WRITABLE = ("docs/results/", "docs/results.md")
BENCH_SEEDS = range(900_000, 900_020)  # the bench's seeds; never official
CLOCK_TOLERANCE_S = 60
_RUN_ID = re.compile(r"off-s(\d+)-a([0-9.]+)-")


class NumbersMayExist(Exception):
    pass


class NeedsReason(Exception):
    pass


class PreflightFailed(Exception):
    pass


# ---------------------------------------------------------------- the freeze file


def load_freeze(path: str | Path = FREEZE_PATH) -> dict:
    """The freeze file, checked: five distinct official seeds, none a dev or bench seed."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"{p} does not exist; the settings are not frozen yet")
    freeze = json.loads(p.read_text())
    seeds = freeze.get("official_seeds")
    if not isinstance(seeds, list) or len(seeds) != 5 or len(set(seeds)) != 5:
        raise ValueError(f"official_seeds must be five distinct integers, got {seeds!r}")
    if not all(isinstance(s, int) and not isinstance(s, bool) for s in seeds):
        raise ValueError(f"official_seeds must be integers, got {seeds!r}")
    bad = [s for s in seeds if s in DEV_SEEDS or s in BENCH_SEEDS]
    if bad:
        raise ValueError(f"official seeds {bad} are dev seeds {DEV_SEEDS} or in 900000-900019")
    if tuple(freeze.get("amplitudes", ())) != AMPLITUDES:
        raise ValueError(f"amplitudes must be {list(AMPLITUDES)}, got {freeze.get('amplitudes')!r}")
    return freeze


def planned(freeze: dict) -> list[tuple[int, float]]:
    """(seed, amplitude) pairs: the main amplitude first, then the others, seeds in file order."""
    main = freeze["main_amplitude"]
    order = [main] + [a for a in freeze["amplitudes"] if a != main]
    return [(seed, amp) for amp in order for seed in freeze["official_seeds"]]


# ---------------------------------------------------------------- the registry


class Registry:
    """`official-runs.jsonl`: one JSON line per event, appended and never rewritten."""

    def __init__(self, path: str | Path = REGISTRY_PATH):
        self.path = Path(path)

    def entries(self) -> list[dict]:
        if not self.path.is_file():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x.strip()]

    def _append(self, entry: dict) -> None:
        entry["at"] = datetime.now(UTC).isoformat(timespec="seconds")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, sort_keys=True, allow_nan=False) + "\n")

    def started(self, seed: int, amplitude: float, run_id: str, commit: str) -> None:
        self._append(
            {
                "event": "started",
                "seed": seed,
                "amplitude": amplitude,
                "run_id": run_id,
                "commit": commit,
            }
        )

    def finished(self, run_id: str, timings: dict) -> None:
        seed, amplitude = _pair_of(run_id)
        self._append(
            {
                "event": "finished",
                "seed": seed,
                "amplitude": amplitude,
                "run_id": run_id,
                "timings": timings,
            }
        )

    def failed(self, run_id: str, why: str) -> None:
        seed, amplitude = _pair_of(run_id)
        self._append(
            {"event": "failed", "seed": seed, "amplitude": amplitude, "run_id": run_id, "why": why}
        )


def _pair_of(run_id: str) -> tuple[int, float]:
    m = _RUN_ID.match(run_id)
    if not m:
        raise ValueError(f"{run_id!r} is not an official run id")
    return int(m.group(1)), float(m.group(2))


def may_run(
    entries: list[dict],
    seed: int,
    amplitude: float,
    rerun_reason: str | None,
    summary_exists: bool,
    topic_exists: bool,
) -> bool:
    """Whether the pair may run now; False means it already ran (the caller goes on).

    Raises NumbersMayExist when a summary or an input topic exists for a pair with no registry
    line and no `bug:` reason, and NeedsReason when a failed or unfinished pair has no
    `infrastructure:` (or `bug:`) reason.
    """
    reason = (rerun_reason or "").strip()
    bug = reason.startswith("bug:")
    last = next(
        (e for e in reversed(entries) if e["seed"] == seed and e["amplitude"] == amplitude), None
    )
    if last is not None and last["event"] == "finished":
        return bug
    if bug:
        return True
    if last is None:
        if summary_exists or topic_exists:
            raise NumbersMayExist(
                f"seed {seed} amplitude {amplitude:g} has no registry line but a summary or an"
                " input topic exists, so numbers may exist; rerun with --rerun-reason 'bug: ...'"
            )
        return True
    # failed, or started with no outcome
    if summary_exists:
        raise NumbersMayExist(
            f"seed {seed} amplitude {amplitude:g} has no finished entry but a summary exists;"
            " rerun with --rerun-reason 'bug: ...'"
        )
    if reason.startswith("infrastructure:"):
        return True
    raise NeedsReason(
        f"seed {seed} amplitude {amplitude:g} failed or did not finish; rerun with"
        " --rerun-reason 'infrastructure: ...' (or 'bug: ...')"
    )


# ---------------------------------------------------------------- preflight


def _sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)


def _changed_paths(porcelain: str) -> list[str]:
    paths = []
    for line in porcelain.splitlines():
        if line.strip():
            paths.append(line[3:].split(" -> ")[-1].strip().strip('"'))
    return paths


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def preflight(
    freeze: dict,
    rerun_reason: str | None,
    on_battery: bool = False,
    run: Callable[[list[str]], subprocess.CompletedProcess] = _sh,
    jar_path: Path = JAR_PATH,
    host_time: Callable[[], float] = time.time,
) -> None:
    """Raise PreflightFailed, naming the fix, unless the official runs may start."""
    bug = (rerun_reason or "").strip().startswith("bug:")

    status = run(["git", "status", "--porcelain"])
    if status.returncode != 0:
        raise PreflightFailed(f"git status failed: {status.stderr.strip()}")
    dirty = [p for p in _changed_paths(status.stdout) if not p.startswith(WRITABLE)]
    if dirty:
        raise PreflightFailed(
            f"the working tree has changes outside docs/results/: {dirty[:5]}; commit or"
            " discard them"
        )

    if not bug:
        found = run(["git", "log", "-1", "--format=%H", "--", "eval/freeze.json"])
        f_commit = found.stdout.strip()
        if found.returncode != 0 or not f_commit:
            raise PreflightFailed("eval/freeze.json is not committed; commit the freeze first")
        diff = run(["git", "diff", "--quiet", f_commit, "HEAD", "--", *FROZEN_PATHS])
        if diff.returncode != 0:
            raise PreflightFailed(
                f"frozen code changed since the freeze commit {f_commit[:7]}; revert it, or"
                " rerun with --rerun-reason 'bug: ...' and log the change in run-log.jsonl"
            )
        if not jar_path.is_file():
            raise PreflightFailed(f"{jar_path} does not exist; run `make jar` at the freeze commit")
        if _sha256(jar_path) != freeze["jar_sha256"]:
            raise PreflightFailed(
                f"{jar_path.name} differs from the jar in eval/freeze.json; rebuild it at the"
                " freeze commit with `make jar`, or rerun with --rerun-reason 'bug: ...'"
            )

    if not on_battery:
        try:
            batt = run(["pmset", "-g", "batt"])
        except FileNotFoundError:
            print("eval-official: pmset not found; skipping the power check")
        else:
            if "AC Power" not in batt.stdout:
                raise PreflightFailed("the laptop is on battery; plug it in, or pass --on-battery")

    date = run(["docker", "compose", "exec", "-T", "jobmanager", "date", "+%s"])
    try:
        vm = int(date.stdout.strip())
    except ValueError:
        raise PreflightFailed(
            f"could not read the jobmanager's clock (exit {date.returncode}); is the stack up?"
        ) from None
    skew = vm - host_time()
    if abs(skew) > CLOCK_TOLERANCE_S:
        raise PreflightFailed(
            f"the Docker VM's clock is {skew:+.0f} s off the host's; restart Docker Desktop's VM"
            " or sync its clock (a host sleep leaves it behind)"
        )


# ---------------------------------------------------------------- main


def place_all(pairs: list[tuple[int, float]], world_name: str = "eval") -> None:
    """Place every run's scenarios (no simulation, no detector); raises PlacementError."""
    for seed, amp in pairs:
        world = build_world(world_name, seed)
        r = rates(world, TrafficParams(amplitude=amp), seed)
        try:
            place(world, r, seed)
        except PlacementError as e:
            raise PlacementError(f"seed {seed} amplitude {amp:g}: {e}") from e


def _copy_summary(rid: str, data_dir: Path, summaries_dir: Path) -> None:
    dest = summaries_dir / rid
    dest.mkdir(parents=True)
    for name in ("summary.json", "labels.json"):
        shutil.copy2(data_dir / rid / name, dest / name)


def _commit_hash() -> str:
    return eval_run._commit()


def _append_run_log_stub(path: Path, rid: str, reason: str) -> None:
    entry = {
        "commit": _commit_hash(),
        "date": datetime.now(UTC).date().isoformat(),
        "what": f"rerun of an official run, new id {rid}",
        "why": reason,
        "old": "to be completed",
        "new": "to be completed",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Run the official (seed, amplitude) pairs once each")
    ap.add_argument("--only-seed", type=int)
    ap.add_argument("--only-amplitude", type=float, choices=AMPLITUDES)
    ap.add_argument("--on-battery", action="store_true")
    ap.add_argument("--rerun-reason", help="'bug: ...' or 'infrastructure: ...'")
    args = ap.parse_args(argv)
    if args.rerun_reason and (args.only_seed is None or args.only_amplitude is None):
        ap.error("--rerun-reason names one run: pass --only-seed and --only-amplitude with it")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    freeze = load_freeze(FREEZE_PATH)
    preflight(freeze, args.rerun_reason, on_battery=args.on_battery)
    pairs = planned(freeze)
    place_all(pairs)  # before any run starts: a seed that cannot be placed stops everything
    selected = [
        (s, a)
        for s, a in pairs
        if (args.only_seed is None or s == args.only_seed)
        and (args.only_amplitude is None or a == args.only_amplitude)
    ]
    reg = Registry()
    t_start = time.monotonic()
    left = len(selected)
    for seed, amp in selected:
        prefix = f"off-s{seed}-a{amp:g}-"
        entries = reg.entries()
        summary_exists = any(DATA_DIR.glob(f"{prefix}*/summary.json"))
        topic_exists = any(t.startswith(prefix) for t in topics())
        if not may_run(entries, seed, amp, args.rerun_reason, summary_exists, topic_exists):
            print(f"eval-official: seed {seed} amplitude {amp:g}: already run")
            left -= 1
            continue
        rid = eval_run.run_id("off", seed, amp, datetime.now(UTC))
        is_rerun = bool(
            summary_exists
            or topic_exists
            or any(e["seed"] == seed and e["amplitude"] == amp for e in entries)
        )
        if args.rerun_reason and is_rerun:
            _append_run_log_stub(RUN_LOG_PATH, rid, args.rerun_reason)
        reg.started(seed, amp, rid, _commit_hash())
        try:
            s = eval_run.run("off", seed, amp, rid=rid)
            _copy_summary(rid, DATA_DIR, SUMMARIES_DIR)
        except (RunFailed, IncompleteRead) as e:
            reg.failed(rid, f"{type(e).__name__}: {e}")
            print(f"eval-official: {rid} FAILED: {e}")
            left -= 1
            continue
        reg.finished(rid, s["timings"])
        left -= 1
        print(
            f"eval-official: {rid} done; elapsed {(time.monotonic() - t_start) / 60:.1f} min,"
            f" {left} run(s) left"
        )


if __name__ == "__main__":
    main()

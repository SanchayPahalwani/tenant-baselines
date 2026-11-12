"""The official driver: the seed protocol of spec 5.3 as code.

Synthetic data only. Until the freeze commit only the dev seeds run (`eval_run`). After it, each
official (seed, amplitude) pair runs once and is logged in `docs/results/official-runs.jsonl`
before, and after, its run. Guards, checked again before every run: a clean tree, code equal to
the freeze commit, the frozen jar, the registry's history, AC power, and a VM clock that agrees
with the host's. A rerun needs a reason: `bug:` or `infrastructure:`, with text after it.
"""

import argparse
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines import eval_run
from tenant_baselines.eval_run import AMPLITUDES, DATA_DIR, RunFailed
from tenant_baselines.evalmodel import TrafficParams, rates
from tenant_baselines.freeze import (
    FREEZE_PATH,
    REPO,
    FrozenJarError,
    changed_paths,
    driver_writes,
    head_commit,
    load_freeze,
    planned,
    sha256_of,
    verify_frozen_jar,
)
from tenant_baselines.kafka_io import IncompleteRead, topics
from tenant_baselines.scenarios import PlacementError, place
from tenant_baselines.world import build_world

log = logging.getLogger(__name__)

REGISTRY_PATH = REPO / "docs" / "results" / "official-runs.jsonl"
RUN_LOG_PATH = REPO / "docs" / "results" / "run-log.jsonl"
SUMMARIES_DIR = REPO / "docs" / "results" / "summaries"
SUPERSEDED_DIR = REPO / "docs" / "results" / "superseded"
# The driver writes docs/results/ between runs (`freeze.driver_writes`); results.md is rendered
# by `make eval` after the loop.
# The whole tree is frozen except what the driver writes, and the freeze file (its `commit` key
# is set after the freeze commit; the rest is compared separately).
EXCLUDED = (":(exclude)docs/results", ":(exclude)docs/results.md", ":(exclude)eval/freeze.json")
CLOCK_TOLERANCE_S = 60


class NumbersMayExist(Exception):
    pass


class NeedsReason(Exception):
    pass


class BadReason(ValueError):
    pass


class PreflightFailed(Exception):
    pass


def reason_kind(reason: str | None) -> str | None:
    """`bug`, `infrastructure`, or None for no reason. Any other reason is rejected: one with
    another prefix, and a bare prefix."""
    if not reason:
        return None
    r = reason.strip()
    for kind in ("bug", "infrastructure"):
        if r.startswith(f"{kind}:"):
            if not r[len(kind) + 1 :].strip():
                raise BadReason(f"--rerun-reason '{kind}:' needs a few words after the colon")
            return kind
    raise BadReason(f"--rerun-reason must start with 'bug:' or 'infrastructure:', not {reason!r}")


# ---------------------------------------------------------------- the registry


class Registry:
    """`official-runs.jsonl`: one JSON line per event, appended and never rewritten."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or REGISTRY_PATH)

    def entries(self) -> list[dict]:
        if not self.path.is_file():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x.strip()]

    def _append(self, entry: dict) -> None:
        entry["at"] = datetime.now(UTC).isoformat(timespec="seconds")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, sort_keys=True, allow_nan=False) + "\n")

    def started(
        self,
        seed: int,
        amplitude: float,
        run_id: str,
        commit: str,
        reason: str | None = None,
        jar_sha256: str | None = None,
    ) -> None:
        self._append(
            {
                "event": "started",
                "seed": seed,
                "amplitude": amplitude,
                "run_id": run_id,
                "commit": commit,
                "reason": reason,
                "jar_sha256": jar_sha256,
            }
        )

    def finished(self, run_id: str, timings: dict, summary_sha256: str | None = None) -> None:
        seed, amplitude = _pair_of(run_id)
        entry = {
            "event": "finished",
            "seed": seed,
            "amplitude": amplitude,
            "run_id": run_id,
            "timings": timings,
        }
        if summary_sha256 is not None:
            entry["summary_sha256"] = summary_sha256
        self._append(entry)

    def failed(self, run_id: str, why: str) -> None:
        seed, amplitude = _pair_of(run_id)
        self._append(
            {"event": "failed", "seed": seed, "amplitude": amplitude, "run_id": run_id, "why": why}
        )


def _pair_of(run_id: str) -> tuple[int, float]:
    parts = run_id.split("-")
    if len(parts) < 4 or parts[0] != "off" or parts[1][:1] != "s" or parts[2][:1] != "a":
        raise ValueError(f"{run_id!r} is not an official run id")
    return int(parts[1][1:]), float(parts[2][1:])


def last_entry(entries: list[dict], seed: int, amplitude: float) -> dict | None:
    return next(
        (e for e in reversed(entries) if e["seed"] == seed and e["amplitude"] == amplitude), None
    )


def may_run(
    entries: list[dict],
    seed: int,
    amplitude: float,
    rerun_reason: str | None,
    summary_exists: bool,
    topic_exists: bool,
) -> bool:
    """Whether the pair may run now; False means it already ran (the caller goes on).

    `summary_exists`: a summary, an alert log or a copied summary of the pair is on disk.
    `topic_exists`: an input topic or a run directory of the pair exists.
    Raises NumbersMayExist when either holds for a pair with no registry line and no `bug:`
    reason (or when a summary exists for a failed or unfinished pair), and NeedsReason when a
    failed or unfinished pair has no `infrastructure:` (or `bug:`) reason.
    """
    kind = reason_kind(rerun_reason)
    last = last_entry(entries, seed, amplitude)
    if last is not None and last["event"] == "finished":
        return kind == "bug"
    if kind == "bug":
        return True
    if last is None:
        if summary_exists or topic_exists:
            raise NumbersMayExist(
                f"seed {seed} amplitude {amplitude:g} has no registry line but a summary, a run"
                " directory or an input topic exists, so numbers may exist; rerun with"
                " --rerun-reason 'bug: ...'"
            )
        return True
    # failed, or started with no outcome
    if summary_exists:
        raise NumbersMayExist(
            f"seed {seed} amplitude {amplitude:g} has no finished entry but a summary exists;"
            " rerun with --rerun-reason 'bug: ...'"
        )
    if kind == "infrastructure":
        return True
    raise NeedsReason(
        f"seed {seed} amplitude {amplitude:g} failed or did not finish; rerun with"
        " --rerun-reason 'infrastructure: ...' (or 'bug: ...')"
    )


# ---------------------------------------------------------------- preflight


def _sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)


def _extends_committed(
    git: Callable[..., subprocess.CompletedProcess], rel: str, path: Path, what: str
) -> str:
    """The committed text of an append-only file; raise unless the working copy extends it."""
    committed = git("show", f"HEAD:{rel}")
    if committed.returncode != 0:
        raise PreflightFailed(f"{rel} is not committed at HEAD")
    current = path.read_text() if path.is_file() else ""
    if not current.startswith(committed.stdout):
        raise PreflightFailed(
            f"{rel} does not extend the committed file; {what} is append-only. Restore it from"
            " git and log what happened in run-log.jsonl"
        )
    return committed.stdout


def _official_start_commits(committed_registry: str) -> list[str]:
    """The commits of the committed `started` lines that ran under no reason."""
    try:
        lines = [json.loads(x) for x in committed_registry.splitlines() if x.strip()]
    except json.JSONDecodeError as e:
        raise PreflightFailed(f"the committed registry is not JSON lines: {e}") from None
    return sorted(
        {
            e["commit"]
            for e in lines
            if e.get("event") == "started" and e.get("reason") is None and e.get("commit")
        }
    )


def check_machine(
    on_battery: bool,
    run: Callable[[list[str]], subprocess.CompletedProcess] = _sh,
    host_time: Callable[[], float] = time.time,
) -> None:
    """Power and the jobmanager's clock; checked before every run."""
    if not on_battery:
        try:
            batt = run(["pmset", "-g", "batt"])
        except FileNotFoundError:
            print("eval-official: pmset not found; skipping the power check")
        else:
            if "AC Power" not in batt.stdout:
                raise PreflightFailed("the laptop is on battery; plug it in, or pass --on-battery")
    try:
        date = run(["docker", "compose", "exec", "-T", "jobmanager", "date", "+%s"])
    except FileNotFoundError:
        raise PreflightFailed("docker is not installed or not on the PATH") from None
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


def preflight(
    freeze: dict,
    rerun_reason: str | None,
    on_battery: bool = False,
    run: Callable[[list[str]], subprocess.CompletedProcess] = _sh,
    repo: Path | None = None,
    registry_path: Path | None = None,
    host_time: Callable[[], float] = time.time,
    run_log_path: Path | None = None,
) -> str:
    """Raise PreflightFailed, naming the fix, unless the next run may start. Returns the SHA-256
    of the frozen jar."""
    repo = Path(repo or REPO)
    registry_path = Path(registry_path or REGISTRY_PATH)
    run_log_path = Path(run_log_path or RUN_LOG_PATH)
    kind = reason_kind(rerun_reason)
    bug = kind == "bug"

    def git(*args: str) -> subprocess.CompletedProcess:
        try:
            return run(["git", *args])
        except FileNotFoundError:
            raise PreflightFailed("git is not installed or not on the PATH") from None

    status = git("status", "--porcelain", "-z")
    if status.returncode != 0:
        raise PreflightFailed(f"git status failed: {status.stderr.strip()}")
    dirty = [p for p in changed_paths(status.stdout) if not driver_writes(p)]
    if dirty:
        raise PreflightFailed(
            f"the working tree has changes outside docs/results/: {dirty[:5]}; commit or"
            " discard them"
        )

    committed = _extends_committed(
        git, "docs/results/official-runs.jsonl", registry_path, "the registry"
    )
    _extends_committed(git, "docs/results/run-log.jsonl", run_log_path, "the run log")

    if not bug:
        c = freeze.get("commit")
        if not c:
            raise PreflightFailed("eval/freeze.json has no `commit`; record the freeze commit")
        if not re.fullmatch(r"[0-9a-f]{40}", str(c)):
            raise PreflightFailed(
                f"eval/freeze.json's `commit` must be a full 40-character commit hash, not {c!r}"
            )
        diff = git("diff", "--quiet", c, "HEAD", "--", ".", *EXCLUDED)
        if diff.returncode != 0:
            raise PreflightFailed(
                f"the code or settings differ from the freeze commit {c[:7]} (git diff exit"
                f" {diff.returncode}); revert the change, or rerun with --rerun-reason"
                " 'bug: ...' and log it in run-log.jsonl"
            )
        then = git("show", f"{c}:eval/freeze.json")
        if then.returncode != 0:
            raise PreflightFailed(f"eval/freeze.json cannot be read at the freeze commit {c[:7]}")
        before = {k: v for k, v in json.loads(then.stdout).items() if k != "commit"}
        now = {k: v for k, v in freeze.items() if k != "commit"}
        if before != now:
            raise PreflightFailed(
                f"eval/freeze.json differs from the file at the freeze commit {c[:7]} by more than"
                " its `commit` key; restore it, or rerun with --rerun-reason 'bug: ...'"
            )
        # The registry is committed and append-only: the anchor must equal what already ran.
        for ran in _official_start_commits(committed):
            same = git("diff", "--quiet", ran, c, "--", ".", *EXCLUDED)
            if same.returncode != 0:
                raise PreflightFailed(
                    f"the freeze commit {c[:7]} in eval/freeze.json does not match the commit the"
                    f" official runs started on, {ran[:7]} (git diff exit {same.returncode});"
                    " restore its `commit` key"
                )

    try:
        sha = verify_frozen_jar(freeze, repo, kind)
    except FrozenJarError as e:
        raise PreflightFailed(str(e)) from None

    check_machine(on_battery, run, host_time)
    return sha


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


def _old_summaries(summaries_dir: Path, prefix: str) -> list[Path]:
    return sorted(d for d in summaries_dir.glob(f"{prefix}*") if d.is_dir())


def _copy_summary(rid: str, data_dir: Path, summaries_dir: Path) -> None:
    dest = summaries_dir / rid
    dest.mkdir(parents=True)
    for name in ("summary.json", "labels.json"):
        shutil.copy2(data_dir / rid / name, dest / name)


def _supersede(old: list[Path], superseded_dir: Path) -> None:
    """Move earlier summaries of a rerun pair out of the set `eval_report` reads."""
    for d in old:
        superseded_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(d), str(superseded_dir / d.name))


def _commit_hash() -> str:
    return head_commit()


def _append_run_log(path: Path, rid: str, reason: str, superseded: list[str]) -> None:
    """The run log line of a run under a reason, written before the run. `supersedes` names the
    summaries the run replaces once it finishes; the report derives the headlines, so `old` and
    `new` stay null."""
    entry = {
        "commit": _commit_hash(),
        "date": datetime.now(UTC).date().isoformat(),
        "what": f"run under a reason: official run {rid}",
        "why": reason,
        "old": None,
        "new": None,
        "supersedes": superseded,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry, sort_keys=True) + "\n")


def _history(seed: int, amp: float) -> tuple[bool, bool]:
    """(numbers on disk, other trace) for a pair: summaries and alert logs in data/eval and in
    docs/results/summaries; run directories of data/eval; input topics."""
    prefix = f"off-s{seed}-a{amp:g}-"
    runs = [d for d in DATA_DIR.glob(f"{prefix}*") if d.is_dir()]
    numbers = any(
        (d / n).is_file() for d in runs for n in ("summary.json", "alerts.jsonl")
    ) or bool(_old_summaries(SUMMARIES_DIR, prefix))
    trace = bool(runs) or any(t.startswith(prefix) for t in topics())
    return numbers, trace


def _all_finished(entries: list[dict], pairs: list[tuple[int, float]]) -> bool:
    return all((e := last_entry(entries, s, a)) and e["event"] == "finished" for s, a in pairs)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Run the official (seed, amplitude) pairs once each")
    ap.add_argument("--only-seed", type=int)
    ap.add_argument("--only-amplitude", type=float, choices=AMPLITUDES)
    ap.add_argument("--on-battery", action="store_true")
    ap.add_argument("--rerun-reason", help="'bug: ...' or 'infrastructure: ...'")
    args = ap.parse_args(argv)
    try:
        reason_kind(args.rerun_reason)
    except BadReason as e:
        ap.error(str(e))
    if args.rerun_reason and (args.only_seed is None or args.only_amplitude is None):
        ap.error("--rerun-reason names one run: pass --only-seed and --only-amplitude with it")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        _drive(args)
    except (PreflightFailed, NeedsReason, NumbersMayExist) as e:
        print(f"eval-official: {e}", file=sys.stderr)
        sys.exit(2)


def _drive(args: argparse.Namespace) -> None:
    freeze = load_freeze(FREEZE_PATH)
    pairs = planned(freeze)
    selected = [
        (s, a)
        for s, a in pairs
        if (args.only_seed is None or s == args.only_seed)
        and (args.only_amplitude is None or a == args.only_amplitude)
    ]
    reg = Registry()
    if not args.rerun_reason and _all_finished(reg.entries(), selected):
        print(
            f"eval-official: all {len(selected)} official runs are finished; nothing to run"
            " (`make results` renders; a re-run needs --rerun-reason 'bug: ...')"
        )
        return
    preflight(freeze, args.rerun_reason, on_battery=args.on_battery)
    place_all(pairs)  # before any run starts: a seed that cannot be placed stops everything
    t_start = time.monotonic()
    left = len(selected)
    for seed, amp in selected:
        entries = reg.entries()
        numbers, trace = _history(seed, amp)
        if not may_run(entries, seed, amp, args.rerun_reason, numbers, trace):
            print(f"eval-official: seed {seed} amplitude {amp:g}: already run")
            left -= 1
            continue
        jar_sha = preflight(freeze, args.rerun_reason, on_battery=args.on_battery)
        rid = eval_run.run_id("off", seed, amp, datetime.now(UTC))
        prefix = f"off-s{seed}-a{amp:g}-"
        old = _old_summaries(SUMMARIES_DIR, prefix)
        if args.rerun_reason:
            _append_run_log(RUN_LOG_PATH, rid, args.rerun_reason, [d.name for d in old])
        reg.started(seed, amp, rid, _commit_hash(), args.rerun_reason, jar_sha)
        try:
            s = eval_run.run("off", seed, amp, rid=rid)
            _copy_summary(rid, DATA_DIR, SUMMARIES_DIR)
            _supersede(old, SUPERSEDED_DIR)
        except (RunFailed, IncompleteRead) as e:
            reg.failed(rid, f"{type(e).__name__}: {e}")
            print(f"eval-official: {rid} FAILED: {e}")
            status = "FAILED"
        else:
            reg.finished(rid, s["timings"], sha256_of(SUMMARIES_DIR / rid / "summary.json"))
            status = "done"
        left -= 1
        print(
            f"eval-official: {rid} {status}; elapsed {(time.monotonic() - t_start) / 60:.1f} min,"
            f" {left} run(s) left"
        )

    entries = reg.entries()
    open_pairs = [
        (s, a)
        for s, a in selected
        if (e := last_entry(entries, s, a)) is None or e["event"] != "finished"
    ]
    if open_pairs:
        print(f"eval-official: failed or unfinished: {open_pairs}")
        sys.exit(1)


if __name__ == "__main__":
    main()

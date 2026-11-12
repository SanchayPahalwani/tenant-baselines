"""The freeze file (`eval/freeze.json`), the frozen jar it names, and the facts of the checkout
that a run records.

Synthetic data only. Shared by `eval_run` (which loads the freeze for an official run),
`eval_official` (the driver), `check_paths` and `eval_report`.
"""

import hashlib
import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
FREEZE_PATH = REPO / "eval" / "freeze.json"
DEV_SEEDS = (7, 11, 13)
AMPLITUDES = (1.5, 6.0, 12.0)
PLACEMENT_TEST_SEEDS = range(900_000, 900_020)  # the placement test's seeds; never official
FROZEN_DIR = "data/frozen"
_CONTAINER_DATA = "/opt/tb-data"  # the jobmanager's read-only mount of the repo's data/
# The code the official runs measured. The report checks that it is unchanged since the freeze.
MEASURED_PATHS = (
    "flink-job/src/main",
    "shared",
    "scripts/submit-eval.sh",
    "docker-compose.yml",
    "infra",
    *(
        f"py/src/tenant_baselines/{name}.py"
        for name in (
            "world",
            "evalmodel",
            "traffic",
            "latency",
            "scenarios",
            "simulate",
            "thresholds",
            "scoring",
            "kafka_io",
            "replay_events",
        )
    ),
)
# Added 2026-11-12 (UTC), after the official runs. A file under these may differ from the freeze
# commit: the eval reads the alerts topic, and no run reads a dashboard. The report names each
# such file.
DISPLAY_ONLY_PATHS = ("infra/grafana/dashboards",)
_REFREEZE = (
    "after a fix to the Java code, build the jar (`make jar`), copy it to"
    " data/frozen/<sha256>.jar and set `jar_sha256` in eval/freeze.json (the steps are in the"
    " Makefile comment above `eval`)"
)


class FrozenJarError(Exception):
    pass


def load_freeze(path: str | Path = FREEZE_PATH) -> dict:
    """The freeze file, checked: five distinct official seeds, none a dev or placement-test seed."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"{p} does not exist; the settings are not frozen yet")
    freeze = json.loads(p.read_text())
    seeds = freeze.get("official_seeds")
    if not isinstance(seeds, list) or len(seeds) != 5 or len(set(seeds)) != 5:
        raise ValueError(f"official_seeds must be five distinct integers, got {seeds!r}")
    if not all(isinstance(s, int) and not isinstance(s, bool) for s in seeds):
        raise ValueError(f"official_seeds must be integers, got {seeds!r}")
    bad = [s for s in seeds if s in DEV_SEEDS or s in PLACEMENT_TEST_SEEDS]
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


def frozen_jar(freeze: dict, repo: Path = REPO) -> Path:
    """The host path of the frozen jar copy, `data/frozen/<sha256>.jar`."""
    return Path(repo) / FROZEN_DIR / f"{freeze['jar_sha256']}.jar"


def frozen_jar_container_path(freeze: dict) -> str:
    return f"{_CONTAINER_DATA}/frozen/{freeze['jar_sha256']}.jar"


def sha256_of(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def verify_frozen_jar(freeze: dict, repo: Path = REPO, reason_kind: str | None = None) -> str:
    """Hash the frozen jar copy and return the SHA-256; raise FrozenJarError unless it is there
    and equals the freeze file's. The jar is not rebuilt: a rebuild is not reproducible.

    `reason_kind` is the kind of the run's rerun reason: under `bug` the message names the steps
    after a Java fix instead of restoring the copy."""
    path = frozen_jar(freeze, repo)
    if not path.is_file():
        fix = (
            _REFREEZE
            if reason_kind == "bug"
            else "restore the copy made at the freeze (the jar is not rebuilt, a rebuild gives"
            " other bytes)"
        )
        raise FrozenJarError(f"the frozen jar {path} is missing; {fix}")
    digest = sha256_of(path)
    if digest != freeze["jar_sha256"]:
        fix = _REFREEZE if reason_kind == "bug" else "restore the copy made at the freeze"
        raise FrozenJarError(
            f"the frozen jar {path} does not hash to the SHA-256 in eval/freeze.json; {fix}"
        )
    return digest


# ---------------------------------------------------------------- the checkout


def head_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.strip()


def container_path(path: Path) -> str:
    """The jobmanager's path of a file under the repo's data/ directory."""
    try:
        rel = path.resolve().relative_to(REPO / "data")
    except ValueError:
        raise ValueError(f"{path} is not under {REPO / 'data'}, the jobmanager's mount") from None
    return f"{_CONTAINER_DATA}/{rel.as_posix()}"


def driver_writes(path: str) -> bool:
    """Whether the official driver writes `path` between runs: docs/results/ and results.md."""
    return path == "docs/results.md" or path.startswith("docs/results/")


def changed_paths(porcelain_z: str) -> list[str]:
    """Every path in `git status --porcelain -z` output; a rename or copy lists both sides."""
    tokens = porcelain_z.split("\0")
    paths, i = [], 0
    while i < len(tokens):
        t = tokens[i]
        i += 1
        if len(t) < 4:
            continue
        paths.append(t[3:])
        if "R" in t[:2] or "C" in t[:2]:
            if i < len(tokens) and tokens[i]:
                paths.append(tokens[i])
            i += 1
    return paths


def dirty_paths(kind: str, porcelain_z: str) -> list[str]:
    """The changed paths that make a run of `kind` dirty. An official run does not count what the
    driver itself writes before the run (its registry line), so that is left out for `off`."""
    paths = changed_paths(porcelain_z)
    return [p for p in paths if not driver_writes(p)] if kind == "off" else paths


def tree_dirty(kind: str) -> bool:
    """Whether the working tree differs from HEAD for a run of `kind` (data/ is ignored, so a
    run's own files are not counted)."""
    status = subprocess.run(
        ["git", "status", "--porcelain", "-z"], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout
    return bool(dirty_paths(kind, status))

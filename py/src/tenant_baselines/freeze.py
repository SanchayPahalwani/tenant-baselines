"""The freeze file (`eval/freeze.json`) and the frozen jar it names.

Synthetic data only. Shared by `eval_run` (which loads the freeze for an official run) and
`eval_official` (the driver).
"""

import hashlib
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
FREEZE_PATH = REPO / "eval" / "freeze.json"
DEV_SEEDS = (7, 11, 13)
AMPLITUDES = (1.5, 6.0, 12.0)
PLACEMENT_TEST_SEEDS = range(900_000, 900_020)  # the placement test's seeds; never official
FROZEN_DIR = "data/frozen"
_CONTAINER_DATA = "/opt/tb-data"  # the jobmanager's read-only mount of the repo's data/


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


def frozen_jar(freeze: dict, repo: Path = REPO) -> Path:
    """The host path of the frozen jar copy, `data/frozen/<sha256>.jar`."""
    return Path(repo) / FROZEN_DIR / f"{freeze['jar_sha256']}.jar"


def frozen_jar_container_path(freeze: dict) -> str:
    return f"{_CONTAINER_DATA}/frozen/{freeze['jar_sha256']}.jar"


def verify_frozen_jar(freeze: dict, repo: Path = REPO) -> str:
    """Hash the frozen jar copy and return the SHA-256; raise FrozenJarError unless it is there
    and equals the freeze file's. The jar is not rebuilt: a rebuild is not reproducible."""
    path = frozen_jar(freeze, repo)
    if not path.is_file():
        raise FrozenJarError(
            f"the frozen jar {path} is missing; restore the copy made at the freeze"
            " (the jar is not rebuilt, a rebuild gives other bytes)"
        )
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    if h.hexdigest() != freeze["jar_sha256"]:
        raise FrozenJarError(
            f"the frozen jar {path} does not hash to the SHA-256 in eval/freeze.json;"
            " restore the copy made at the freeze"
        )
    return h.hexdigest()

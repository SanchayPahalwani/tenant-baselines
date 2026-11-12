"""Repo-wide rules, checked over the tracked tree.

Synthetic data only. The banned words may appear only on a line that states the rule, and every
document and result file says its data is synthetic.
"""

import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

BANNED = ("spearheaded", "leveraged", "synergy", "cutting-edge", "results-driven", "passionate")
SYNTHETIC_DOCS = ["README.md", "docs/results.md", "docs/performance.md"]
ALLOWED_FILES = {
    "py/tests/test_bench_report.py",
    "py/tests/test_eval_report.py",
    "py/tests/test_repo_rules.py",
}
SKIPPED_DIRS = {
    ".git",
    "target",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "data",
    ".idea",
    ".superpowers",
}


def _readable(paths) -> list[Path]:
    kept = []
    for p in paths:
        try:
            p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        kept.append(p)
    return kept


def tracked_files(repo: Path = REPO, run=subprocess.run) -> list[Path]:
    try:
        done = run(["git", "ls-files"], cwd=repo, capture_output=True, text=True)
        listed = done.stdout.splitlines() if done.returncode == 0 else None
    except FileNotFoundError:
        listed = None
    if listed is not None:
        paths = [repo / name for name in listed]
    else:
        paths = [
            p
            for p in repo.rglob("*")
            if p.is_file() and not SKIPPED_DIRS & set(p.relative_to(repo).parts)
        ]
    return sorted(_readable(p for p in paths if p.is_file()))


def banned_lines(repo: Path, files: list[Path]) -> list[str]:
    hits = []
    for path in files:
        rel = path.relative_to(repo).as_posix()
        if rel in ALLOWED_FILES:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            lowered = line.lower()
            found = sum(1 for word in BANNED if word in lowered)
            if 0 < found < 3:
                hits.append(f"{rel}:{number}")
    return hits


def test_no_banned_words_outside_statements_of_the_rule():
    assert banned_lines(REPO, tracked_files()) == []


def test_the_rule_allows_only_lines_that_state_it(tmp_path):
    (tmp_path / "two.md").write_text(f"We use {BANNED[1]} and {BANNED[2]} here.\n")
    (tmp_path / "one.md").write_text(f"Very {BANNED[0]}.\n")
    (tmp_path / "all.md").write_text("Banned: " + ", ".join(BANNED) + ".\n")
    files = [tmp_path / "all.md", tmp_path / "one.md", tmp_path / "two.md"]
    assert banned_lines(tmp_path, files) == ["one.md:1", "two.md:1"]


def _fake_tree(tmp_path):
    (tmp_path / "a.md").write_text("a\n")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "x.class").write_text("x\n")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "y.py").write_text("y\n")


def test_tracked_files_falls_back_when_git_is_missing(tmp_path):
    _fake_tree(tmp_path)

    def run(*args, **kwargs):
        raise FileNotFoundError("git")

    assert tracked_files(tmp_path, run=run) == [tmp_path / "a.md"]


def test_tracked_files_falls_back_when_git_fails(tmp_path):
    _fake_tree(tmp_path)

    def run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 128, stdout="", stderr="fatal")

    assert tracked_files(tmp_path, run=run) == [tmp_path / "a.md"]


def test_documents_say_synthetic_at_the_top():
    for name in SYNTHETIC_DOCS:
        top = "\n".join((REPO / name).read_text(encoding="utf-8").splitlines()[:12])
        assert "synthetic" in top.lower(), name


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_result_files_are_labelled_synthetic():
    summaries = sorted((REPO / "docs/results/summaries").glob("*/summary.json"))
    assert summaries
    for path in summaries:
        data = _load(path)
        assert data["synthetic"] is True, path
        assert data["meta"]["data"] == "synthetic", path
    labels = sorted((REPO / "docs/results/summaries").glob("*/labels.json"))
    assert labels
    for path in labels:
        assert _load(path)["meta"]["data"] == "synthetic", path
    # power.json is the OS power log (times and power sources), not data; it has no label.
    performance = sorted(
        p for p in (REPO / "docs/performance").glob("*.json") if p.name != "power.json"
    )
    assert len(performance) == 3
    for path in performance:
        assert _load(path)["data"] == "synthetic", path
    assert "synthetic" in _load(REPO / "docs/results/check-paths.json")["world"]
    assert "synthetic" in _load(REPO / "eval/freeze.json")["note"]

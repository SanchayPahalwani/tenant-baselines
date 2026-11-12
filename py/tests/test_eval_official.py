import json
import subprocess
from pathlib import Path

import pytest

from tenant_baselines import eval_official, eval_run
from tenant_baselines.eval_official import (
    NeedsReason,
    NumbersMayExist,
    PreflightFailed,
    Registry,
    load_freeze,
    may_run,
    planned,
    preflight,
)
from tenant_baselines.eval_run import SeedNotAllowed, prepare
from tenant_baselines.scenarios import PlacementError

SEEDS = [4101, 4102, 4103, 4104, 4105]
FREEZE = {
    "official_seeds": SEEDS,
    "amplitudes": [1.5, 6.0, 12.0],
    "main_amplitude": 6.0,
    "jar_sha256": "",
}


def write_freeze(path: Path, **over) -> Path:
    path.write_text(json.dumps({**FREEZE, **over}))
    return path


def test_freeze_needs_five_distinct_official_seeds_apart_from_dev(tmp_path):
    p = tmp_path / "freeze.json"
    assert load_freeze(write_freeze(p))["official_seeds"] == SEEDS
    for bad in (
        SEEDS[:4],
        [*SEEDS[:4], 4101],
        [*SEEDS[:4], 7],
        [*SEEDS[:4], 13],
        [*SEEDS[:4], 900_000],
        [*SEEDS[:4], 900_019],
    ):
        with pytest.raises(ValueError):
            load_freeze(write_freeze(p, official_seeds=bad))
    assert load_freeze(write_freeze(p, official_seeds=[*SEEDS[:4], 900_020]))
    with pytest.raises(FileNotFoundError):
        load_freeze(tmp_path / "none.json")


def test_planned_order_is_main_amplitude_first():
    pairs = planned(FREEZE)
    assert len(pairs) == 15 and len(set(pairs)) == 15
    assert pairs[:5] == [(s, 6.0) for s in SEEDS]
    assert pairs[5:10] == [(s, 1.5) for s in SEEDS]
    assert pairs[10:] == [(s, 12.0) for s in SEEDS]


def entry(event, seed=4101, amp=6.0):
    return {"event": event, "seed": seed, "amplitude": amp}


def test_finished_pair_is_skipped_so_the_loop_resumes():
    done = [entry("started"), entry("finished")]
    assert may_run(done, 4101, 6.0, None, True, True) is False
    assert may_run(done, 4102, 6.0, None, False, False) is True  # another pair


def test_a_finished_run_repeats_only_with_a_bug_reason():
    done = [entry("started"), entry("finished")]
    assert may_run(done, 4101, 6.0, "infrastructure: x", True, True) is False
    assert may_run(done, 4101, 6.0, "bug: the scorer", True, True) is True


def test_a_failed_run_may_repeat_as_infrastructure():
    failed = [entry("started"), entry("failed")]
    assert may_run(failed, 4101, 6.0, "infrastructure: broker restarted", False, True) is True
    with pytest.raises(NeedsReason):
        may_run(failed, 4101, 6.0, None, False, False)
    with pytest.raises(NeedsReason):
        may_run(failed, 4101, 6.0, "because", False, False)


def test_a_started_run_without_an_outcome_counts_as_failed():
    started = [entry("started")]
    with pytest.raises(NeedsReason):
        may_run(started, 4101, 6.0, None, False, False)
    assert may_run(started, 4101, 6.0, "infrastructure: laptop slept", False, False) is True
    with pytest.raises(NumbersMayExist):
        may_run(started, 4101, 6.0, "infrastructure: laptop slept", True, False)


def test_a_summary_or_topic_without_a_registry_line_refuses():
    assert may_run([], 4101, 6.0, None, False, False) is True
    for summary, topic in ((True, False), (False, True)):
        with pytest.raises(NumbersMayExist):
            may_run([], 4101, 6.0, None, summary, topic)
        with pytest.raises(NumbersMayExist):
            may_run([], 4101, 6.0, "infrastructure: x", summary, topic)
        assert may_run([], 4101, 6.0, "bug: x", summary, topic) is True


def test_registry_appends_lines_and_reads_them_back(tmp_path):
    reg = Registry(tmp_path / "sub" / "runs.jsonl")
    assert reg.entries() == []
    reg.started(4101, 6.0, "off-s4101-a6-20261113T100000", "abc")
    reg.finished("off-s4101-a6-20261113T100000", {"total": 5.0})
    reg.started(4102, 1.5, "off-s4102-a1.5-20261113T101000", "abc")
    reg.failed("off-s4102-a1.5-20261113T101000", "RunFailed: x")
    got = reg.entries()
    assert [(e["event"], e["seed"], e["amplitude"]) for e in got] == [
        ("started", 4101, 6.0),
        ("finished", 4101, 6.0),
        ("started", 4102, 1.5),
        ("failed", 4102, 1.5),
    ]
    assert may_run(got, 4101, 6.0, None, True, True) is False
    assert may_run(got, 4102, 1.5, "infrastructure: x", False, False) is True


# ---------------------------------------------------------------- preflight


class Fake:
    """Answers the commands preflight runs; `answers` maps a command's first words."""

    def __init__(self, **over):
        self.status = ""
        self.diff_rc = 0
        self.freeze_commit = "f" * 40
        self.batt = "Now drawing from 'AC Power'\n"
        self.vm = 1_000_000
        self.__dict__.update(over)
        self.calls = []

    def __call__(self, cmd):
        self.calls.append(cmd)
        out, rc = "", 0
        if cmd[:2] == ["git", "status"]:
            out = self.status
        elif cmd[:2] == ["git", "log"]:
            out = self.freeze_commit + "\n"
        elif cmd[:2] == ["git", "diff"]:
            rc = self.diff_rc
        elif cmd[0] == "pmset":
            if self.batt is None:
                raise FileNotFoundError("pmset")
            out = self.batt
        elif cmd[0] == "docker":
            out = f"{self.vm}\n"
        return subprocess.CompletedProcess(cmd, rc, out, "")


@pytest.fixture
def jar(tmp_path):
    import hashlib

    p = tmp_path / "job.jar"
    p.write_bytes(b"jar bytes")
    return p, {**FREEZE, "jar_sha256": hashlib.sha256(b"jar bytes").hexdigest()}


def go(freeze, fake, jar_path, reason=None, **kw):
    preflight(freeze, reason, run=fake, jar_path=jar_path, host_time=lambda: 1_000_000, **kw)


def test_everything_in_order_passes(jar):
    path, freeze = jar
    go(freeze, Fake(), path)


def test_tree_guard_allows_only_docs_results(jar):
    path, freeze = jar
    ok = " M docs/results/official-runs.jsonl\n?? docs/results/summaries/\n"
    go(freeze, Fake(status=ok), path)
    with pytest.raises(PreflightFailed, match="py/src"):
        go(freeze, Fake(status=ok + " M py/src/tenant_baselines/scoring.py\n"), path)


def test_frozen_code_guard(jar):
    path, freeze = jar
    with pytest.raises(PreflightFailed, match="frozen code"):
        go(freeze, Fake(diff_rc=1), path)
    go(freeze, Fake(diff_rc=1), path, reason="bug: the scorer")
    fake = Fake()
    go(freeze, fake, path)
    diff = next(c for c in fake.calls if c[:2] == ["git", "diff"])
    assert diff[2:5] == ["--quiet", "f" * 40, "HEAD"] and "py" in diff and "eval" in diff


def test_jar_must_match_the_freeze(jar, tmp_path):
    path, freeze = jar
    with pytest.raises(PreflightFailed, match="jar"):
        go({**freeze, "jar_sha256": "0" * 64}, Fake(), path)
    with pytest.raises(PreflightFailed, match="does not exist"):
        go(freeze, Fake(), tmp_path / "missing.jar")
    go({**freeze, "jar_sha256": "0" * 64}, Fake(), path, reason="bug: rebuilt the jar")


def test_refuses_on_battery_without_the_flag(jar):
    path, freeze = jar
    battery = Fake(batt="Now drawing from 'Battery Power'\n")
    with pytest.raises(PreflightFailed, match="battery"):
        go(freeze, battery, path)
    go(freeze, battery, path, on_battery=True)


def test_no_pmset_skips_the_power_check(jar, capsys):
    path, freeze = jar
    go(freeze, Fake(batt=None), path)
    assert "skipping the power check" in capsys.readouterr().out


def test_refuses_when_the_vm_clock_is_off(jar):
    path, freeze = jar
    with pytest.raises(PreflightFailed, match="clock"):
        go(freeze, Fake(vm=1_000_000 - 300), path)
    go(freeze, Fake(vm=1_000_000 - 30), path)


# ---------------------------------------------------------------- main and the run


@pytest.fixture
def driver(tmp_path, monkeypatch):
    """main() with everything outside it faked; `calls` records what happened in order."""
    freeze = write_freeze(tmp_path / "freeze.json", jar_sha256="x")
    calls = []
    monkeypatch.setattr(eval_official, "FREEZE_PATH", freeze)
    monkeypatch.setattr(eval_official, "REGISTRY_PATH", tmp_path / "runs.jsonl")
    monkeypatch.setattr(eval_official, "RUN_LOG_PATH", tmp_path / "run-log.jsonl")
    monkeypatch.setattr(eval_official, "SUMMARIES_DIR", tmp_path / "summaries")
    monkeypatch.setattr(eval_official, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(eval_official, "preflight", lambda *a, **k: calls.append("preflight"))
    monkeypatch.setattr(eval_official, "topics", lambda: set())
    monkeypatch.setattr(eval_official, "_commit_hash", lambda: "abc")
    monkeypatch.setattr(eval_official, "Registry", lambda: Registry(tmp_path / "runs.jsonl"))
    monkeypatch.setattr(eval_official, "build_world", lambda name, seed: (name, seed))
    monkeypatch.setattr(eval_official, "rates", lambda world, params, seed: params.amplitude)
    return tmp_path, calls


def test_placement_runs_for_all_before_any_run_starts(driver, monkeypatch):
    tmp, calls = driver
    n = []

    def place(world, r, seed):
        n.append((seed, r))
        if len(n) == 9:
            raise PlacementError("no slot")

    monkeypatch.setattr(eval_official, "place", place)
    monkeypatch.setattr(eval_run, "run", lambda *a, **k: calls.append("run"))
    with pytest.raises(PlacementError, match="seed 4104 amplitude 1.5"):
        eval_official.main([])
    assert len(n) == 9 and "run" not in calls
    assert not (tmp / "runs.jsonl").exists()


def fake_run(tmp, outcomes):
    def run(kind, seed, amp, rid=None, **kw):
        assert kind == "off"
        out = tmp / "data" / rid
        out.mkdir(parents=True)
        (out / "labels.json").write_text("{}")
        o = outcomes.get((seed, amp))
        if o:
            raise o
        (out / "summary.json").write_text("{}")
        return {"timings": {"total": 1.0}}

    return run


def test_loop_logs_each_run_copies_the_summary_and_resumes(driver, monkeypatch, capsys):
    tmp, calls = driver
    monkeypatch.setattr(eval_official, "place", lambda *a: [])
    monkeypatch.setattr(eval_run, "run", fake_run(tmp, {(4102, 6.0): eval_run.RunFailed("boom")}))
    eval_official.main(["--only-amplitude", "6"])
    reg = Registry(tmp / "runs.jsonl").entries()
    assert [(e["event"], e["seed"]) for e in reg if e["event"] != "started"] == [
        ("finished", 4101),
        ("failed", 4102),
        ("finished", 4103),
        ("finished", 4104),
        ("finished", 4105),
    ]
    rid = next(e["run_id"] for e in reg if e["seed"] == 4101)
    assert (tmp / "summaries" / rid / "summary.json").is_file()
    assert (tmp / "summaries" / rid / "labels.json").is_file()
    # a second call skips the finished pairs and refuses the failed one without a reason
    capsys.readouterr()
    with pytest.raises(NeedsReason):
        eval_official.main(["--only-amplitude", "6"])
    assert "already run" in capsys.readouterr().out


def test_a_rerun_reason_needs_one_named_run(driver):
    with pytest.raises(SystemExit):
        eval_official.main(["--rerun-reason", "bug: x"])


# ---------------------------------------------------------------- the freeze on the run path


def test_run_loads_the_freeze_from_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_run, "REPO", tmp_path)
    monkeypatch.setattr(eval_run, "_commit", lambda: "c0ffee")
    monkeypatch.setattr(eval_run, "_dirty", lambda: False)
    data = tmp_path / "data" / "eval"
    monkeypatch.setattr(eval_official, "FREEZE_PATH", tmp_path / "missing.json")
    with pytest.raises(FileNotFoundError):
        eval_run.run("off", 4101, 6.0, data_dir=data)
    p = write_freeze(tmp_path / "f.json", official_seeds=[4201, 4202, 4203, 4204, 4205])
    monkeypatch.setattr(eval_official, "FREEZE_PATH", p)
    with pytest.raises(SeedNotAllowed, match="frozen seeds"):
        eval_run.run("off", 4101, 6.0, data_dir=data)
    assert not data.exists()


def test_prepare_refuses_a_dev_seed_even_if_a_freeze_lists_it(tmp_path):
    freeze = {"official_seeds": [7, 4102, 4103, 4104, 4105]}
    with pytest.raises(SeedNotAllowed, match="dev seed"):
        prepare("off", "eval", 7, 6.0, tmp_path / "out", freeze=freeze)
    assert not (tmp_path / "out").exists()

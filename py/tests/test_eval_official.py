import json
import subprocess
from pathlib import Path

import pytest

from tenant_baselines import eval_official, eval_report, eval_run
from tenant_baselines import freeze as freeze_mod
from tenant_baselines.eval_official import (
    BadReason,
    NeedsReason,
    NumbersMayExist,
    PreflightFailed,
    Registry,
    may_run,
    planned,
    preflight,
)
from tenant_baselines.eval_run import SeedNotAllowed, prepare
from tenant_baselines.freeze import load_freeze
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


def test_the_amplitudes_must_be_the_three_of_the_protocol(tmp_path):
    p = tmp_path / "freeze.json"
    with pytest.raises(ValueError, match="amplitudes"):
        load_freeze(write_freeze(p, amplitudes=[1.5, 6.0]))
    with pytest.raises(ValueError, match="amplitudes"):
        load_freeze(write_freeze(p, amplitudes=[6.0, 1.5, 12.0]))


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


def test_empty_reasons_are_rejected():
    for bare in ("bug:", "bug:  ", "infrastructure:"):
        with pytest.raises(BadReason):
            may_run([], 4101, 6.0, bare, False, False)
    assert may_run([], 4101, 6.0, "no prefix at all", False, False) is True


def test_a_run_directory_or_alert_log_counts_as_history():
    # the caller turns run directories into topic_exists and alert logs into summary_exists
    with pytest.raises(NumbersMayExist):
        may_run([], 4101, 6.0, "infrastructure: x", True, False)


# ---------------------------------------------------------------- preflight

FZ_COMMIT = "f" * 40


class Fake:
    """Answers the commands preflight runs."""

    def __init__(self, freeze_doc=None, **over):
        self.status = ""
        self.status_rc = 0
        self.diff_rc = 0
        self.committed_registry = ""
        self.batt = "Now drawing from 'AC Power'\n"
        self.vm = 1_000_000
        self.no_docker = False
        self.freeze_at_commit = freeze_doc
        self.git_show_rc = 0
        self.freeze_show_rc = 0
        self.__dict__.update(over)
        self.calls = []

    def __call__(self, cmd):
        self.calls.append(cmd)
        out, rc = "", 0
        if cmd[:2] == ["git", "status"]:
            out, rc = self.status, self.status_rc
        elif cmd[:2] == ["git", "show"]:
            if cmd[2].startswith("HEAD:"):
                rc, out = self.git_show_rc, self.committed_registry
            else:
                rc, out = self.freeze_show_rc, json.dumps(self.freeze_at_commit)
        elif cmd[:2] == ["git", "diff"]:
            rc = self.diff_rc
        elif cmd[0] == "pmset":
            if self.batt is None:
                raise FileNotFoundError("pmset")
            out = self.batt
        elif cmd[0] == "docker":
            if self.no_docker:
                raise FileNotFoundError("docker")
            out = f"{self.vm}\n"
        return subprocess.CompletedProcess(cmd, rc, out, "")


@pytest.fixture
def setup(tmp_path):
    """A repo dir with the frozen jar, a registry file, and the freeze dict."""
    import hashlib

    data = b"jar bytes"
    sha = hashlib.sha256(data).hexdigest()
    (tmp_path / "data" / "frozen").mkdir(parents=True)
    (tmp_path / "data" / "frozen" / f"{sha}.jar").write_bytes(data)
    reg = tmp_path / "runs.jsonl"
    reg.write_text("")
    fz = {**FREEZE, "jar_sha256": sha, "commit": FZ_COMMIT}
    return tmp_path, reg, fz


def go(setup, fake=None, reason=None, **kw):
    repo, reg, fz = setup
    fake = fake or Fake({k: v for k, v in fz.items() if k != "commit"})
    return preflight(
        fz,
        reason,
        run=fake,
        repo=repo,
        registry_path=reg,
        host_time=lambda: 1_000_000,
        **kw,
    )


def test_everything_in_order_passes_and_returns_the_jar_hash(setup):
    assert go(setup) == setup[2]["jar_sha256"]


def test_tree_guard_allows_only_docs_results(setup):
    ok = " M docs/results/official-runs.jsonl\0?? docs/results/summaries/\0"
    go(setup, Fake(status=ok, freeze_doc=None, freeze_at_commit=setup[2]))
    bad = ok + " M py/src/tenant_baselines/scoring.py\0"
    with pytest.raises(PreflightFailed, match="py/src"):
        go(setup, Fake(status=bad, freeze_at_commit=setup[2]))


def test_tree_guard_allows_the_rendered_results_document(setup):
    go(setup, Fake(status=" M docs/results.md\0", freeze_at_commit=setup[2]))


def test_tree_guard_checks_both_sides_of_a_rename(setup):
    moved_in = "R  docs/results/x.json\0py/src/tenant_baselines/scoring.py\0"
    with pytest.raises(PreflightFailed, match="scoring.py"):
        go(setup, Fake(status=moved_in, freeze_at_commit=setup[2]))
    moved_out = "R  py/src/tenant_baselines/scoring.py\0docs/results/scoring.py\0"
    with pytest.raises(PreflightFailed, match="scoring.py"):
        go(setup, Fake(status=moved_out, freeze_at_commit=setup[2]))


def test_git_status_failure_fails_closed(setup):
    with pytest.raises(PreflightFailed, match="git status"):
        go(setup, Fake(status_rc=128, freeze_at_commit=setup[2]))


def test_frozen_code_guard_diffs_the_whole_tree_against_the_freeze_commit(setup):
    fz = setup[2]
    with pytest.raises(PreflightFailed, match="freeze commit"):
        go(setup, Fake(diff_rc=1, freeze_at_commit=fz))
    with pytest.raises(PreflightFailed, match="freeze commit"):
        go(setup, Fake(diff_rc=128, freeze_at_commit=fz))  # a failing git diff fails closed
    go(setup, Fake(diff_rc=1, freeze_at_commit=fz), reason="bug: the scorer")
    fake = Fake(freeze_at_commit=fz)
    go(setup, fake)
    diff = next(c for c in fake.calls if c[:2] == ["git", "diff"])
    assert diff[2:] == [
        "--quiet",
        FZ_COMMIT,
        "HEAD",
        "--",
        ".",
        ":(exclude)docs/results",
        ":(exclude)docs/results.md",
        ":(exclude)eval/freeze.json",
    ]


def test_the_anchor_is_the_commit_key_not_the_last_touch(setup):
    repo, reg, fz = setup
    with pytest.raises(PreflightFailed, match="no `commit`"):
        preflight(
            {k: v for k, v in fz.items() if k != "commit"},
            None,
            run=Fake(freeze_at_commit=fz),
            repo=repo,
            registry_path=reg,
            host_time=lambda: 1_000_000,
        )
    fake = Fake(freeze_at_commit=fz)
    go(setup, fake)
    assert not any(c[:2] == ["git", "log"] for c in fake.calls)
    assert ["git", "show", f"{FZ_COMMIT}:eval/freeze.json"] in fake.calls


def test_freeze_file_must_equal_the_one_at_the_freeze_commit_but_for_commit(setup):
    fz = setup[2]
    older = {k: v for k, v in fz.items() if k != "commit"}
    go(setup, Fake(freeze_at_commit=older))
    with pytest.raises(PreflightFailed, match="more than"):
        go(setup, Fake(freeze_at_commit={**older, "official_seeds": [1, 2, 3, 4, 5]}))
    with pytest.raises(PreflightFailed, match="cannot be read"):
        go(setup, Fake(freeze_at_commit=older, freeze_show_rc=1))


def test_the_frozen_jar_must_be_there_and_match(setup):
    repo, reg, fz = setup
    jar = repo / "data" / "frozen" / f"{fz['jar_sha256']}.jar"
    jar.write_bytes(b"other bytes")
    with pytest.raises(PreflightFailed, match="does not hash") as e:
        go(setup, Fake(freeze_at_commit=fz))
    assert "make jar" not in str(e.value)
    jar.unlink()
    with pytest.raises(PreflightFailed, match="missing") as e:
        go(setup, Fake(freeze_at_commit=fz))
    assert "make jar" not in str(e.value) and "frozen" in str(e.value)
    # a bug reason does not excuse the jar
    with pytest.raises(PreflightFailed, match="missing"):
        go(setup, Fake(freeze_at_commit=fz), reason="bug: x")


def test_the_registry_must_extend_the_committed_file(setup):
    repo, reg, fz = setup
    line = '{"event": "started"}\n'
    reg.write_text(line + '{"event": "finished"}\n')
    go(setup, Fake(freeze_at_commit=fz, committed_registry=line))
    reg.write_text('{"event": "other"}\n')
    with pytest.raises(PreflightFailed, match="append-only"):
        go(setup, Fake(freeze_at_commit=fz, committed_registry=line))
    reg.write_text("")
    with pytest.raises(PreflightFailed, match="append-only"):
        go(setup, Fake(freeze_at_commit=fz, committed_registry=line))
    with pytest.raises(PreflightFailed, match="not committed"):
        go(setup, Fake(freeze_at_commit=fz, git_show_rc=128))


def test_refuses_on_battery_without_the_flag(setup):
    battery = Fake(batt="Now drawing from 'Battery Power'\n", freeze_at_commit=setup[2])
    with pytest.raises(PreflightFailed, match="battery"):
        go(setup, battery)
    go(setup, battery, on_battery=True)


def test_no_pmset_skips_the_power_check(setup, capsys):
    go(setup, Fake(batt=None, freeze_at_commit=setup[2]))
    assert "skipping the power check" in capsys.readouterr().out


def test_refuses_when_the_vm_clock_is_off(setup):
    with pytest.raises(PreflightFailed, match="clock"):
        go(setup, Fake(vm=1_000_000 - 300, freeze_at_commit=setup[2]))
    go(setup, Fake(vm=1_000_000 - 30, freeze_at_commit=setup[2]))


def test_a_missing_docker_binary_is_a_preflight_failure(setup):
    with pytest.raises(PreflightFailed, match="docker"):
        go(setup, Fake(no_docker=True, freeze_at_commit=setup[2]))


# ---------------------------------------------------------------- main


class Env:
    """main() with everything outside it faked."""

    def __init__(self, tmp_path, monkeypatch):
        self.tmp = tmp_path
        self.calls = []
        self.preflight_reasons = []
        self.outcomes = {}
        self.freeze = write_freeze(tmp_path / "freeze.json", jar_sha256="x", commit="c" * 40)
        m = monkeypatch
        m.setattr(eval_official, "FREEZE_PATH", self.freeze)
        m.setattr(eval_official, "REGISTRY_PATH", tmp_path / "runs.jsonl")
        m.setattr(eval_official, "RUN_LOG_PATH", tmp_path / "run-log.jsonl")
        m.setattr(eval_official, "SUMMARIES_DIR", tmp_path / "summaries")
        m.setattr(eval_official, "SUPERSEDED_DIR", tmp_path / "superseded")
        m.setattr(eval_official, "DATA_DIR", tmp_path / "data")
        m.setattr(eval_official, "preflight", self.preflight)
        m.setattr(eval_official, "topics", lambda: set())
        m.setattr(eval_official, "_commit_hash", lambda: "abc")
        m.setattr(eval_official, "build_world", lambda name, seed: (name, seed))
        m.setattr(eval_official, "rates", lambda world, params, seed: params.amplitude)
        m.setattr(eval_official, "place", lambda *a: [])
        m.setattr(eval_run, "run", self.run)
        self.n = 0

    def preflight(self, freeze, reason, **kw):
        self.calls.append("preflight")
        self.preflight_reasons.append(reason)
        return "jarsha"

    def run(self, kind, seed, amp, rid=None, **kw):
        assert kind == "off"
        self.calls.append(("run", seed, amp))
        out = self.tmp / "data" / rid
        out.mkdir(parents=True)
        (out / "labels.json").write_text('{"scenarios": []}')
        o = self.outcomes.get((seed, amp))
        if o:
            raise o
        self.n += 1
        (out / "summary.json").write_text(json.dumps(self.summary(rid, seed, amp)))
        return {"timings": {"total": 1.0}}

    @staticmethod
    def summary(rid, seed, amp):
        return {"run_id": rid, "meta": {"kind": "off", "seed": seed, "amplitude": amp}}

    def log(self):
        p = self.tmp / "run-log.jsonl"
        return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


def test_placement_runs_for_all_before_any_run_starts(env, monkeypatch):
    n = []

    def place(world, r, seed):
        n.append((seed, r))
        if len(n) == 9:
            raise PlacementError("no slot")

    monkeypatch.setattr(eval_official, "place", place)
    with pytest.raises(PlacementError, match="seed 4104 amplitude 1.5"):
        eval_official.main([])
    assert len(n) == 9 and not any(c[0] == "run" for c in env.calls if isinstance(c, tuple))
    assert not (env.tmp / "runs.jsonl").exists()


def test_loop_logs_each_run_copies_the_summary_and_resumes(env, capsys):
    env.outcomes[(4102, 6.0)] = eval_run.RunFailed("boom")
    with pytest.raises(SystemExit) as e:
        eval_official.main(["--only-amplitude", "6"])
    assert e.value.code == 1  # a failed pair makes make eval stop before it renders
    reg = Registry(env.tmp / "runs.jsonl").entries()
    assert [(e["event"], e["seed"]) for e in reg if e["event"] != "started"] == [
        ("finished", 4101),
        ("failed", 4102),
        ("finished", 4103),
        ("finished", 4104),
        ("finished", 4105),
    ]
    started = [e for e in reg if e["event"] == "started"]
    assert all(e["jar_sha256"] == "jarsha" and e["reason"] is None for e in started)
    out = capsys.readouterr().out
    assert out.count("run(s) left") == 5  # failed runs print their elapsed time too
    rid = next(e["run_id"] for e in reg if e["seed"] == 4101)
    assert (env.tmp / "summaries" / rid / "summary.json").is_file()
    assert (env.tmp / "summaries" / rid / "labels.json").is_file()
    # a second call skips the finished pairs and refuses the failed one without a reason
    with pytest.raises(NeedsReason):
        eval_official.main(["--only-amplitude", "6"])
    assert "already run" in capsys.readouterr().out


def test_resuming_with_a_summary_on_disk_for_an_unfinished_pair_refuses(env):
    # the crash-after-summary path: a started line, a summary, no outcome
    rid = "off-s4101-a6-20261113T100000"
    Registry(env.tmp / "runs.jsonl").started(4101, 6.0, rid, "abc")
    (env.tmp / "data" / rid).mkdir(parents=True)
    (env.tmp / "data" / rid / "summary.json").write_text("{}")
    with pytest.raises(NumbersMayExist):
        eval_official.main(["--only-seed", "4101", "--only-amplitude", "6"])
    with pytest.raises(NumbersMayExist):
        eval_official.main(
            ["--only-seed", "4101", "--only-amplitude", "6", "--rerun-reason", "infrastructure: x"]
        )
    assert not any(isinstance(c, tuple) for c in env.calls)


def test_copied_summaries_and_run_directories_count_as_history(env):
    (env.tmp / "summaries" / "off-s4101-a6-20261113T100000").mkdir(parents=True)
    with pytest.raises(NumbersMayExist):
        eval_official.main(["--only-seed", "4101", "--only-amplitude", "6"])
    (env.tmp / "data" / "off-s4102-a6-20261113T100000").mkdir(parents=True)  # a bare directory
    with pytest.raises(NumbersMayExist):
        eval_official.main(["--only-seed", "4102", "--only-amplitude", "6"])


def test_a_rerun_reason_needs_one_named_run(env):
    with pytest.raises(SystemExit):
        eval_official.main(["--rerun-reason", "bug: x"])
    with pytest.raises(SystemExit):
        eval_official.main(
            ["--only-seed", "4101", "--only-amplitude", "6", "--rerun-reason", "bug:"]
        )


def test_a_bug_run_of_a_fresh_pair_is_logged_with_its_reason_and_jar(env):
    eval_official.main(
        ["--only-seed", "4101", "--only-amplitude", "6", "--rerun-reason", "bug: scorer fix"]
    )
    (started,) = [e for e in Registry(env.tmp / "runs.jsonl").entries() if e["event"] == "started"]
    assert started["reason"] == "bug: scorer fix" and started["jar_sha256"] == "jarsha"
    (line,) = env.log()
    assert line["why"] == "bug: scorer fix" and line["commit"] == "abc"
    assert set(line) == {"commit", "date", "what", "why", "old", "new"}
    assert env.preflight_reasons[-1] == "bug: scorer fix"  # the guards saw the reason


def test_a_bug_rerun_moves_the_old_summary_aside_so_the_report_still_renders(env, monkeypatch):
    only = ["--only-seed", "4101", "--only-amplitude", "6"]
    eval_official.main(only)
    first = next(e["run_id"] for e in Registry(env.tmp / "runs.jsonl").entries())
    monkeypatch.setattr(eval_official.eval_run, "run_id", lambda k, s, a, now: first[:-1] + "9")
    eval_official.main([*only, "--rerun-reason", "bug: the scorer miscounted"])
    summaries = env.tmp / "summaries"
    assert [d.name for d in summaries.iterdir()] == [first[:-1] + "9"]
    assert (env.tmp / "superseded" / first / "summary.json").is_file()
    (line,) = env.log()
    assert first in line["old"] and "superseded" in line["old"]
    # eval_report.main reads that directory: one summary per pair, so no "appears twice"
    monkeypatch.setattr(eval_report, "REPO", env.tmp)
    monkeypatch.setattr(eval_report, "render", lambda *a, **k: "rendered\n")
    monkeypatch.setattr(eval_report, "measure_machine", lambda: {})
    out = env.tmp / "results.md"
    eval_report.main(["--summaries", str(summaries), "--out", str(out)])
    assert out.read_text() == "rendered\n"
    # without the move, the same directory is refused
    (summaries / first).mkdir()
    (summaries / first / "summary.json").write_text(json.dumps(Env.summary(first, 4101, 6.0)))
    with pytest.raises(ValueError, match="appears twice"):
        eval_report.main(["--summaries", str(summaries), "--out", str(out)])


# ---------------------------------------------------------------- the freeze on the run path


def frozen_repo(tmp_path, monkeypatch, **over):
    """A repo dir holding the freeze file and the frozen jar; eval_run is pointed at it."""
    import hashlib

    sha = hashlib.sha256(b"jar").hexdigest()
    (tmp_path / "data" / "frozen").mkdir(parents=True)
    (tmp_path / "data" / "frozen" / f"{sha}.jar").write_bytes(b"jar")
    p = write_freeze(tmp_path / "f.json", jar_sha256=sha, **over)
    monkeypatch.setattr(eval_run, "REPO", tmp_path)
    monkeypatch.setattr(eval_run, "FREEZE_PATH", p)
    return sha


def test_run_loads_the_freeze_from_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_run, "REPO", tmp_path)
    monkeypatch.setattr(eval_run, "_commit", lambda: "c0ffee")
    monkeypatch.setattr(eval_run, "_dirty", lambda: False)
    data = tmp_path / "data" / "eval"
    monkeypatch.setattr(eval_run, "FREEZE_PATH", tmp_path / "missing.json")
    with pytest.raises(FileNotFoundError):
        eval_run.run("off", 4101, 6.0, data_dir=data)
    frozen_repo(tmp_path, monkeypatch, official_seeds=[4201, 4202, 4203, 4204, 4205])
    with pytest.raises(SeedNotAllowed, match="frozen seeds"):
        eval_run.run("off", 4101, 6.0, data_dir=data)
    assert not data.exists()


def test_run_refuses_a_missing_or_changed_frozen_jar(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_run, "_commit", lambda: "c0ffee")
    monkeypatch.setattr(eval_run, "_dirty", lambda: False)
    sha = frozen_repo(tmp_path, monkeypatch)
    jar = tmp_path / "data" / "frozen" / f"{sha}.jar"
    jar.write_bytes(b"changed")
    with pytest.raises(freeze_mod.FrozenJarError, match="does not hash"):
        eval_run.run("off", 4101, 6.0, data_dir=tmp_path / "data" / "eval")
    jar.unlink()
    with pytest.raises(freeze_mod.FrozenJarError, match="missing"):
        eval_run.run("off", 4101, 6.0, data_dir=tmp_path / "data" / "eval")


def test_an_official_run_id_must_belong_to_its_pair(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_run, "_commit", lambda: "c0ffee")
    monkeypatch.setattr(eval_run, "_dirty", lambda: False)
    frozen_repo(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="does not belong"):
        eval_run.run(
            "off", 4101, 6.0, data_dir=tmp_path / "data" / "eval", rid="off-s4102-a6-20261113T1000"
        )


def test_submit_passes_the_jar_only_when_given(monkeypatch):
    cmds = []
    monkeypatch.setattr(
        eval_run.subprocess,
        "run",
        lambda cmd, **k: cmds.append(cmd) or subprocess.CompletedProcess(cmd, 0, "a" * 32, ""),
    )
    eval_run.submit("rid", "aggregates", "t", "/p", jar="/opt/tb-data/frozen/x.jar")
    eval_run.submit("rid", "aggregates", "t", "/p")
    n = cmds[0].index("--jar")
    assert cmds[0][n : n + 3] == ["--jar", "/opt/tb-data/frozen/x.jar", "rid"]
    assert cmds[0].index("/opt/tb-scripts/submit-eval.sh") < n
    assert "--jar" not in cmds[1]


def test_prepare_refuses_a_dev_seed_even_if_a_freeze_lists_it(tmp_path):
    freeze_doc = {"official_seeds": [7, 4102, 4103, 4104, 4105]}
    with pytest.raises(SeedNotAllowed, match="dev seed"):
        prepare("off", "eval", 7, 6.0, tmp_path / "out", freeze=freeze_doc)
    assert not (tmp_path / "out").exists()

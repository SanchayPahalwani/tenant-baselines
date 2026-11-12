import hashlib
import json
import random

import numpy as np
import pytest

from tenant_baselines import check_paths
from tenant_baselines import latency as lat
from tenant_baselines.check_paths import diff_aggregates, diff_logs, verdict
from tenant_baselines.evalmodel import EPOCH_MS, MINUTE_MS, REPLAY_END_MS
from tenant_baselines.simulate import Aggregates, rows
from tenant_baselines.world import Key

M = MINUTE_MS
KEY = "p1|eu-central|eu-central-s1"


def alert(**kw):
    base = {
        "alert_id": "a1",
        "run_id": "r-ev",
        "detector": "seasonal",
        "setting": "k4",
        "scope": "tenant",
        "key": KEY,
        "tenant": "p1",
        "region": "eu-central",
        "unit": "eu-central-s1",
        "signal": "error_rate",
        "state": "firing",
        "starts_at_ms": EPOCH_MS,
        "event_time_ms": EPOCH_MS,
        "emitted_at_ms": 5,
        "value": 0.25,
        "reference": 0.01,
        "reason": None,
    }
    base.update(kw)
    return base


def regional(**kw):
    return alert(
        alert_id="g1",
        scope="region",
        key="eu-central",
        tenant="",
        unit="",
        **kw,
    )


def log(n=40):
    """n alert rows a minute apart, the fifth a regional page."""
    out = [alert(alert_id=f"a{i}", event_time_ms=EPOCH_MS + i * M) for i in range(n)]
    out[4] = regional(event_time_ms=EPOCH_MS + 4 * M)
    return out


def other_run(rows_):
    """The same rows as another run writes them: shuffled, with its own run id and emit times."""
    out = [r | {"run_id": "r-ag", "emitted_at_ms": 1_000 + i} for i, r in enumerate(rows_)]
    random.Random(3).shuffle(out)
    return out


def test_equal_logs_have_no_diff():
    a = log()
    b = other_run(a) + [a[7] | {"emitted_at_ms": 99}]  # a duplicate, emitted again
    assert diff_logs(a, b) == []
    assert diff_logs([], []) == []


def test_diff_logs_reports_the_first_difference():
    a = log()
    b = other_run(a)
    changed = next(i for i, r in enumerate(b) if r["alert_id"] == "a9")
    b[changed] = b[changed] | {"value": 0.5}
    lines = diff_logs(a, b)
    assert len(lines) == 2
    assert lines[0].startswith("- ") and lines[1].startswith("+ ")
    assert '"alert_id":"a9"' in lines[0] and '"value":0.25' in lines[0]
    assert '"alert_id":"a9"' in lines[1] and '"value":0.5' in lines[1]
    assert "run_id" not in lines[0] and "emitted_at_ms" not in lines[1]

    # A row only in one log, and a row past the replay end that the normal form drops.
    b = other_run(a[:12] + a[13:]) + [alert(alert_id="late", event_time_ms=REPLAY_END_MS + 1)]
    lines = diff_logs(a, b)
    assert len(lines) == 1 and lines[0].startswith("- ") and '"alert_id":"a12"' in lines[0]

    # The first 20 differences, in normal-form order (event time first).
    b = other_run([r | {"value": 9.0} for r in a])
    lines = diff_logs(a, b)
    assert len(lines) == 20
    assert all('"alert_id":"a0"' in line for line in lines[:2])
    assert all('"alert_id":"a9"' in line for line in lines[18:])


def tiny_aggregates():
    """Two keys, six minutes; the second key's first request is in minute 2, minute 3 is silent."""
    keys = [Key("p1", "eu-central", "eu-central-s1"), Key("p2", "eu-central", "eu-central-s2")]
    count = np.array([[3, 1, 2, 4, 2, 1], [0, 0, 5, 0, 2, 7]], dtype=np.int64)
    errors = np.array([[1, 0, 0, 2, 0, 0], [0, 0, 1, 0, 0, 3]], dtype=np.int64)
    p95 = np.where(count > 0, lat.representative(np.full(count.shape, 80)), np.nan)
    return Aggregates(keys, np.array([0, 2]), count, errors, p95, EPOCH_MS)


def job_rows(a):
    """The rows as the events job writes them: with a run id, in no particular order."""
    out = [r | {"run_id": "r-ev"} for r in rows(a)]
    random.Random(5).shuffle(out)
    return out


def test_diff_aggregates_catches_a_missing_zero_row_and_a_p95_off_by_a_bucket():
    a = tiny_aggregates()
    good = job_rows(a)
    assert len(good) == 6 + 4
    assert diff_aggregates(good, a) == []

    silent = next(
        r for r in good if r["key"].startswith("p2|") and r["minute_ms"] == EPOCH_MS + 3 * M
    )
    assert silent["count"] == 0 and silent["p95_latency_ms"] is None
    missing = [r for r in good if r is not silent]
    (line,) = diff_aggregates(missing, a)
    assert "p2|" in line and str(EPOCH_MS + 3 * M) in line

    off = [dict(r) for r in good]
    target = next(r for r in off if r["key"].startswith("p1|") and r["minute_ms"] == EPOCH_MS + M)
    target["p95_latency_ms"] = float(lat.representative(81))
    (line,) = diff_aggregates(off, a)
    assert "p1|" in line and str(EPOCH_MS + M) in line

    # A row twice, a row before the key's first request, and a p95 on a zero row also differ.
    assert len(diff_aggregates(good + [good[0]], a)) == 1
    early = silent | {"minute_ms": EPOCH_MS}
    assert len(diff_aggregates(good + [early], a)) == 1
    zero_p95 = [r | {"p95_latency_ms": 1.0} if r is silent else r for r in good]
    assert len(diff_aggregates(zero_p95, a)) == 1


@pytest.mark.parametrize("field,job,python", [("count", 3, 2), ("errors", 1, 0)])
def test_diff_aggregates_reports_a_count_or_an_error_count_alone(field, job, python):
    a = tiny_aggregates()
    rows_ = [dict(r) for r in job_rows(a)]
    minute = EPOCH_MS + 2 * M
    target = next(r for r in rows_ if r["key"].startswith("p1|") and r["minute_ms"] == minute)
    target[field] += 1
    (line,) = diff_aggregates(rows_, a)
    p95 = float(lat.representative(80))
    other = {"count": 2, "errors": 0} | {field: job}
    want = {"count": 2, "errors": 0} | {field: python}
    assert line == (
        f"{KEY} minute {minute}: job count {other['count']}, errors {other['errors']}, p95 {p95};"
        f" python count {want['count']}, errors {want['errors']}, p95 {p95}"
    )


@pytest.fixture
def logs():
    a = log()
    return a, other_run(a)


def judge(ev, ag, n_job, n_py, agg_diffs):
    return verdict(ev, ag, n_job, n_py, agg_diffs, diff_logs(ev, ag))


def test_verdict_fails_on_empty_logs_no_regional_row_or_a_row_count_mismatch(logs):
    ev, ag = logs
    assert judge(ev, ag, 10, 10, []) == []

    (empty,) = [r for r in judge([], ag, 10, 10, []) if "empty" in r]
    assert "events" in empty
    (empty,) = [r for r in judge(ev, [], 10, 10, []) if "empty" in r]
    assert "aggregates" in empty

    local = [r for r in ev if r["scope"] != "region"]
    (reason,) = judge(local, other_run(local), 10, 10, [])
    assert "region" in reason
    # A regional row that is not a page does not count.
    resolved = local + [regional(state="resolved", event_time_ms=EPOCH_MS + 50 * M)]
    assert len(judge(resolved, other_run(resolved), 10, 10, [])) == 1

    (reason,) = judge(ev, ag, 9, 10, [])
    assert "9" in reason and "10" in reason

    (reason,) = judge(ev, ag, 10, 10, ["p1|… minute 0: job …, python …"])
    assert "aggregates" in reason

    differs = [r | {"value": 2.0} if r["alert_id"] == "a3" else r for r in ag]
    (reason,) = judge(ev, differs, 10, 10, [])
    assert "differ" in reason
    # the reason quotes the diff it is given; the log diff is computed once, by the caller
    (reason,) = verdict(ev, ag, 10, 10, [], ["- a given line"])
    assert reason == "the alert logs differ, first at: - a given line"


class FakeJobs:
    """`check` with the stack faked. A commit lands, and the tree changes, while the jobs run."""

    def __init__(self, monkeypatch, alerts_ag=None):
        self.a = tiny_aggregates()
        self.head, self.dirty = "before", False
        self.alerts_ag = alerts_ag
        m = monkeypatch
        m.setattr(check_paths, "prepare", self.prepare)
        m.setattr(check_paths, "build_world", lambda name, seed: None)
        m.setattr(check_paths, "_draws", lambda world, a, scenarios: [])
        m.setattr(check_paths, "_produce_events", lambda topic, world, draws: 123)
        m.setattr(check_paths, "_produce_rows", lambda topic, a: len(job_rows(a)))
        m.setattr(check_paths, "_run_job", self.run_job)
        m.setattr(check_paths, "_read_run", self.read_run)
        m.setattr(check_paths, "container_path", lambda path: "/opt/tb-data/x")
        m.setattr(check_paths, "head_commit", lambda: self.head)
        m.setattr(check_paths, "tree_dirty", lambda kind: self.dirty)

    def prepare(self, kind, world, seed, amplitude, out):
        out.mkdir(parents=True)
        return self.a, []

    def run_job(self, rid, mode, topic, thresholds, **kw):
        self.head, self.dirty = "after", True
        return {"job_seconds": 1.0}, {0: 0}, {0: 0}

    def read_run(self, topic, start, rid):
        if topic == check_paths.AGGREGATES_TOPIC:
            return job_rows(self.a)
        rows_ = log() if rid.endswith("-ev") or self.alerts_ag is None else self.alerts_ag
        return [r | {"run_id": rid} for r in rows_]


def test_check_records_commit_tree_and_jar_from_before_the_jobs(tmp_path, monkeypatch):
    FakeJobs(monkeypatch)
    jar = tmp_path / "job.jar"
    jar.write_bytes(b"jar bytes")
    out = tmp_path / "check-paths.json"
    assert check_paths.check(tmp_path / "data", out, jar=jar) == 0
    r = json.loads(out.read_text())
    assert r["commit"] == "before" and r["dirty"] is False
    assert r["passed"] is True and r["alerts_identical"] and r["aggregates_identical"]
    assert r["run_id"].startswith("paths-s7-a6-")
    assert r["jar_sha256"] == hashlib.sha256(b"jar bytes").hexdigest()


def test_check_writes_passed_false_when_the_logs_differ(tmp_path, monkeypatch):
    FakeJobs(monkeypatch, alerts_ag=log()[:-1])
    out = tmp_path / "check-paths.json"
    assert check_paths.check(tmp_path / "data", out, jar=tmp_path / "none.jar") == 1
    r = json.loads(out.read_text())
    assert r["passed"] is False and r["alerts_identical"] is False
    assert r["jar_sha256"] is None

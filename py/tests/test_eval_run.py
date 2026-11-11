import json
from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pytest

from tenant_baselines import eval_run
from tenant_baselines.eval_run import (
    DEV_SEEDS,
    PATHS_SEED,
    RunFailed,
    SeedNotAllowed,
    prepare,
    run_id,
    wait_finished,
)
from tenant_baselines.kafka_io import IncompleteRead, read_range
from tenant_baselines.scenarios import read_labels
from tenant_baselines.simulate import Aggregates

JOB = "0123456789abcdef0123456789abcdef"


def test_run_id_format():
    now = datetime(2026, 11, 12, 10, 15, 0, tzinfo=UTC)
    assert run_id("dev", 7, 6.0, now) == "dev-s7-a6-20261112T101500"
    assert run_id("dev", 11, 1.5, now) == "dev-s11-a1.5-20261112T101500"
    assert run_id("off", 4101, 12.0, now) == "off-s4101-a12-20261112T101500"
    # The stamp is UTC whatever zone the clock is given in.
    plus_one = now.astimezone(timezone(timedelta(hours=1)))
    assert run_id("dev", 7, 6.0, plus_one) == "dev-s7-a6-20261112T101500"
    with pytest.raises(ValueError):
        run_id("dev", 7, 6.0, datetime(2026, 11, 12, 10, 15))


def test_prepare_refuses_seeds_outside_its_kind(tmp_path, monkeypatch):
    def no_world(*args):
        raise AssertionError("the guard must run before anything is simulated")

    monkeypatch.setattr(eval_run, "build_world", no_world)
    out = tmp_path / "run"
    freeze = {"official_seeds": [4101, 4102, 4103, 4104, 4105]}
    with pytest.raises(SeedNotAllowed, match=r"\(7, 11, 13\)"):
        prepare("dev", "eval", 4101, 6.0, out)
    with pytest.raises(SeedNotAllowed, match="4101"):
        prepare("off", "eval", 7, 6.0, out, freeze=freeze)
    with pytest.raises(SeedNotAllowed, match="freeze"):
        prepare("off", "eval", 4101, 6.0, out)
    with pytest.raises(SeedNotAllowed):
        prepare("paths", "eval", PATHS_SEED, 6.0, out)
    with pytest.raises(SeedNotAllowed):
        prepare("paths", "paths", 11, 6.0, out)
    with pytest.raises(SeedNotAllowed):
        prepare("bench", "eval", 7, 6.0, out)
    assert not out.exists()
    assert set(DEV_SEEDS) == {7, 11, 13}


def test_prepare_writes_labels_aggregates_and_thresholds(tmp_path):
    out = tmp_path / "run"
    a, scenarios = prepare("paths", "paths", PATHS_SEED, 6.0, out)
    assert sorted(p.name for p in out.iterdir()) == [
        "aggregates.npz",
        "labels.json",
        "thresholds.json",
    ]
    thresholds = json.loads((out / "thresholds.json").read_text())
    assert len(thresholds["sets"]) == 16
    labels, meta = read_labels(out / "labels.json")
    assert labels == scenarios
    assert meta["seed"] == PATHS_SEED and meta["world"] == "paths" and meta["data"] == "synthetic"
    assert meta["replay_end_ms"] == eval_run.REPLAY_END_MS
    saved = Aggregates.load(out / "aggregates.npz")
    assert [k.key for k in saved.keys] == [k.key for k in a.keys]
    assert np.array_equal(saved.count, a.count)
    assert np.array_equal(saved.p95, a.p95, equal_nan=True)


class FakeMessage:
    def __init__(self, partition, offset, value):
        self._partition, self._offset, self._value = partition, offset, value

    def error(self):
        return None

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value


class FakeConsumer:
    """One partition of `topic`, holding the given offsets, with a low watermark of `low`."""

    def __init__(self, offsets, low):
        self.offsets, self.low = list(offsets), low
        self.assigned = None
        self.sent = False

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return self.low, (self.offsets[-1] + 1 if self.offsets else self.low)

    def assign(self, partitions):
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in partitions]

    def consume(self, num_messages=1, timeout=-1):
        if self.sent:
            return []
        self.sent = True
        return [FakeMessage(0, o, json.dumps({"offset": o}).encode()) for o in self.offsets]

    def close(self):
        pass


def test_read_range_rejects_a_gap():
    with pytest.raises(IncompleteRead, match="8"):
        read_range("eval.alerts", {0: 5}, {0: 9}, consumer=FakeConsumer([5, 6, 8], low=5))
    with pytest.raises(IncompleteRead, match="low watermark"):
        read_range("eval.alerts", {0: 3}, {0: 9}, consumer=FakeConsumer([5, 6, 7, 8], low=5))
    c = FakeConsumer([5, 6, 7, 8], low=5)
    got = read_range("eval.alerts", {0: 5}, {0: 9}, consumer=c)
    assert c.assigned == [("eval.alerts", 0, 5)]
    assert [r["offset"] for r in got] == [5, 6, 7, 8]


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        return self

    def json(self):
        return self.body


class FakeRest:
    """Answers GET /jobs/<id> with the given states in turn; a poll past the last one fails."""

    def __init__(self, *states, vertices=()):
        self.states = list(states)
        self.vertices = list(vertices)
        self.gets = []
        self.patches = []

    def get(self, url, **kw):
        self.gets.append(url)
        assert self.states, "polled again after the last scripted state"
        state = self.states.pop(0)
        body = {"jid": JOB, "state": state, "duration": 61_500, "vertices": self.vertices}
        return FakeResponse(body)

    def patch(self, url, **kw):
        self.patches.append((url, kw.get("params")))
        return FakeResponse({})


@pytest.fixture
def fast_poll(monkeypatch):
    monkeypatch.setattr(eval_run, "POLL_S", 0.0)


def test_wait_returns_on_finished(fast_poll):
    vertices = [{"name": "scorer", "metrics": {"accumulated-busy-time": 10.0}}]
    rest = FakeRest("RUNNING", "RUNNING", "FINISHED", vertices=vertices)
    got = wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert got == {"job_seconds": 61.5, "vertices": vertices}
    assert rest.gets == [f"http://127.0.0.1:8081/jobs/{JOB}"] * 3
    assert rest.patches == []


CANCEL = (f"http://127.0.0.1:8081/jobs/{JOB}", {"mode": "cancel"})


def test_wait_fails_on_restart(fast_poll):
    rest = FakeRest("RUNNING", "RESTARTING")
    with pytest.raises(RunFailed, match="is RESTARTING"):
        wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]

    rest = FakeRest("RUNNING", "FAILED")
    with pytest.raises(RunFailed, match="is FAILED"):
        wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]

    rest = FakeRest("RUNNING")
    with pytest.raises(RunFailed, match="timed out"):
        wait_finished(JOB, timeout_s=0, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]

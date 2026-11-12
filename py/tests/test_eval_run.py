import inspect
import json
from datetime import UTC, datetime, timedelta, timezone

import httpx
import numpy as np
import pytest

from tenant_baselines import eval_run, kafka_io
from tenant_baselines.eval_run import (
    ALERTS_TOPIC,
    DEV_SEEDS,
    PATHS_SEED,
    POLL_ATTEMPTS,
    RunFailed,
    SeedNotAllowed,
    prepare,
    run_id,
    wait_finished,
)
from tenant_baselines.evalmodel import MINUTE_MS, week_range
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
    """Holds the given offsets per partition and hands them all out in one batch, partition by
    partition; `low` is every partition's low watermark."""

    def __init__(self, offsets: dict[int, list[int]], low: int):
        self.offsets, self.low = offsets, low
        self.assigned = None
        self.sent = False

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        held = self.offsets.get(tp.partition, [])
        return self.low, (held[-1] + 1 if held else self.low)

    def assign(self, partitions):
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in partitions]

    def consume(self, num_messages=1, timeout=-1):
        if self.sent:
            return []
        self.sent = True
        return [
            FakeMessage(p, o, json.dumps({"partition": p, "offset": o}).encode())
            for p, offsets in self.offsets.items()
            for o in offsets
        ]

    def close(self):
        pass


def test_read_range_rejects_a_gap():
    with pytest.raises(IncompleteRead, match="8"):
        read_range("eval.alerts", {0: 5}, {0: 9}, consumer=FakeConsumer({0: [5, 6, 8]}, low=5))
    with pytest.raises(IncompleteRead, match="low watermark"):
        read_range("eval.alerts", {0: 3}, {0: 9}, consumer=FakeConsumer({0: [5, 6, 7, 8]}, low=5))
    c = FakeConsumer({0: [5, 6, 7, 8]}, low=5)
    got = read_range("eval.alerts", {0: 5}, {0: 9}, consumer=c)
    assert c.assigned == [("eval.alerts", 0, 5)]
    assert [r["offset"] for r in got] == [5, 6, 7, 8]


def test_read_range_rejects_a_short_read(monkeypatch):
    monkeypatch.setattr(kafka_io, "READ_IDLE_S", 0)
    with pytest.raises(IncompleteRead, match="3 records of 4"):
        read_range("eval.alerts", {0: 5}, {0: 9}, consumer=FakeConsumer({0: [5, 6, 7]}, low=5))


def test_read_range_skips_records_past_the_end():
    c = FakeConsumer({0: [0, 1, 2], 1: [3, 4]}, low=0)
    got = read_range("eval.alerts", {0: 0, 1: 3}, {0: 2, 1: 5}, consumer=c)
    assert c.assigned == [("eval.alerts", 0, 0), ("eval.alerts", 1, 3)]
    assert [(r["partition"], r["offset"]) for r in got] == [(0, 0), (0, 1), (1, 3), (1, 4)]


class FakeResponse:
    def __init__(self, body, status_code=200):
        self.body, self.status_code = body, status_code

    def raise_for_status(self):
        return self

    def json(self):
        return self.body


URL = f"http://127.0.0.1:8081/jobs/{JOB}"
CANCEL = (URL, {"mode": "cancel"})


class FakeRest:
    """Answers GET /jobs/<id> with the given states in turn, raising any answer that is an
    exception; a poll past the last answer fails. GET /jobs/<id>/exceptions returns
    `exceptions` as the exception history."""

    def __init__(self, *answers, vertices=(), exceptions=(), cancel_status=202):
        self.answers = list(answers)
        self.vertices = list(vertices)
        self.exceptions = list(exceptions)
        self.cancel_status = cancel_status
        self.gets = []
        self.patches = []

    def get(self, url, **kw):
        self.gets.append(url)
        if url == f"{URL}/exceptions":
            return FakeResponse({"exceptionHistory": {"entries": self.exceptions}})
        assert self.answers, "polled again after the last scripted answer"
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        body = {"jid": JOB, "state": answer, "duration": 61_500, "vertices": self.vertices}
        return FakeResponse(body)

    def patch(self, url, **kw):
        self.patches.append((url, kw.get("params")))
        return FakeResponse({}, status_code=self.cancel_status)


@pytest.fixture
def fast_poll(monkeypatch):
    monkeypatch.setattr(eval_run, "POLL_S", 0.0)


def test_wait_returns_on_finished(fast_poll):
    vertices = [{"name": "scorer", "metrics": {"accumulated-busy-time": 10.0}}]
    rest = FakeRest("RUNNING", "RUNNING", "FINISHED", vertices=vertices)
    got = wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert got == {"job_seconds": 61.5, "vertices": vertices}
    assert rest.gets == [URL] * 3 + [f"{URL}/exceptions"]
    assert rest.patches == []


def test_wait_fails_on_restart(fast_poll):
    for state in ("RESTARTING", "FAILED", "SUSPENDED"):
        rest = FakeRest("RUNNING", state)
        with pytest.raises(RunFailed, match=f"is {state}"):
            wait_finished(JOB, get=rest.get, patch=rest.patch)
        assert rest.patches == [CANCEL]

    rest = FakeRest("RUNNING")
    with pytest.raises(RunFailed, match="timed out"):
        wait_finished(JOB, timeout_s=0, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]


def _status_error(code):
    request = httpx.Request("GET", URL)
    return httpx.HTTPStatusError(str(code), request=request, response=httpx.Response(code))


def test_wait_retries_a_failing_poll(fast_poll):
    errors = (httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), _status_error(503))
    rest = FakeRest(*errors, "RUNNING", *errors, "FINISHED")
    assert wait_finished(JOB, get=rest.get, patch=rest.patch)["job_seconds"] == 61.5
    assert rest.patches == []


def test_wait_cancels_when_polls_keep_failing(fast_poll):
    rest = FakeRest("RUNNING", *[httpx.ConnectError("refused")] * POLL_ATTEMPTS)
    with pytest.raises(RunFailed, match="could not poll"):
        wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]
    assert rest.gets == [URL] * (1 + POLL_ATTEMPTS)


def test_wait_rejects_a_finished_job_that_restarted(fast_poll):
    failure = {"exceptionName": "java.lang.IllegalArgumentException", "timestamp": 1}
    rest = FakeRest("RUNNING", "FINISHED", exceptions=[failure])
    with pytest.raises(RunFailed, match=r"1 failure\(s\) and restart"):
        wait_finished(JOB, get=rest.get, patch=rest.patch)


def test_cancel_logs_a_status_other_than_202(fast_poll, caplog):
    rest = FakeRest("FAILED", cancel_status=409)
    with caplog.at_level("WARNING", logger="tenant_baselines.eval_run"):
        with pytest.raises(RunFailed, match="is FAILED"):
            wait_finished(JOB, get=rest.get, patch=rest.patch)
    assert rest.patches == [CANCEL]
    assert "HTTP 409" in caplog.text


T = week_range(4)[0] + 100 * MINUTE_MS
OTHER_RUN = "dev-s11-a6-20261112T000000"


def alert(rid, alert_id, **kw):
    row = {
        "alert_id": alert_id,
        "run_id": rid,
        "detector": "seasonal",
        "setting": "k4",
        "scope": "tenant",
        "key": "p1|eu-central|eu-central-s1",
        "tenant": "p1",
        "region": "eu-central",
        "unit": "eu-central-s1",
        "signal": "error_rate",
        "state": "firing",
        "starts_at_ms": T,
        "event_time_ms": T,
        "emitted_at_ms": 5,
        "value": 1.0,
        "reference": 0.1,
        "reason": None,
    }
    return row | kw


class FakeStack:
    """Kafka, the submit and Flink REST for `run`, recording each call in order. eval.alerts' end
    moves from 5 to 9 across the job, and the range holds a row of another run."""

    N_ROWS = 123

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.calls = []
        self.alerts_ends = [{0: 5}, {0: 9}]
        self.wait_error = None
        self.rid = None

    def create_topic(self, name, partitions):
        self.calls.append(("create_topic", name, partitions))

    def produce_all(self, topic, records):
        self.calls.append(("produce_all", topic))
        return self.N_ROWS

    def end_offsets(self, topic):
        self.calls.append(("end_offsets", topic))
        if topic == ALERTS_TOPIC:
            return self.alerts_ends.pop(0)
        return {0: self.N_ROWS}

    def submit(self, rid, mode, topic, thresholds_path, extra=(), jar=None):
        self.calls.append(("submit", rid, mode, topic, thresholds_path))
        self.jar = jar
        self.rid = rid
        return JOB

    def wait_finished(self, job_id, timeout_s=1800):
        self.calls.append(("wait_finished", job_id))
        if self.wait_error is not None:
            raise self.wait_error
        return {"job_seconds": 1.5, "vertices": []}

    def read_range(self, topic, start, end):
        self.calls.append(("read_range", topic, start, end))
        return [
            alert(self.rid, "a1"),
            alert(OTHER_RUN, "x1"),
            alert(self.rid, "a1", state="resolved", event_time_ms=T + MINUTE_MS),
        ]


@pytest.fixture
def stack(tmp_path, monkeypatch):
    fake = FakeStack(tmp_path / "data" / "eval")
    monkeypatch.setattr(eval_run, "REPO", tmp_path)
    for name in ("create_topic", "produce_all", "end_offsets", "submit", "wait_finished"):
        monkeypatch.setattr(eval_run, name, getattr(fake, name))
    monkeypatch.setattr(eval_run, "read_range", fake.read_range)
    monkeypatch.setattr(
        eval_run, "_checkpoint", lambda job_id: {"state_size": 7, "external_path": "p"}
    )
    monkeypatch.setattr(eval_run, "_commit", lambda: "c0ffee")
    monkeypatch.setattr(eval_run, "_dirty", lambda: True)
    return fake


def test_run_reads_from_the_offsets_before_submit_and_filters_after_the_read(stack):
    s = eval_run.run("paths", PATHS_SEED, 6.0, world_name="paths", data_dir=stack.data_dir)
    rid = stack.rid
    topic = f"{rid}.aggregates-in"
    assert stack.calls == [
        ("create_topic", topic, 1),
        ("produce_all", topic),
        ("end_offsets", topic),
        ("end_offsets", ALERTS_TOPIC),
        ("submit", rid, "aggregates", topic, f"/opt/tb-data/eval/{rid}/thresholds.json"),
        ("wait_finished", JOB),
        ("end_offsets", ALERTS_TOPIC),
        ("read_range", ALERTS_TOPIC, {0: 5}, {0: 9}),
    ]
    out = stack.data_dir / rid
    log = [json.loads(line) for line in (out / "alerts.jsonl").read_text().splitlines()]
    assert [(r["alert_id"], r["state"]) for r in log] == [("a1", "firing"), ("a1", "resolved")]
    assert s["alert_rows"] == 2 and s["aggregate_rows"] == FakeStack.N_ROWS
    assert s["run_id"] == rid and s["job_id"] == JOB
    assert s["commit"] == "c0ffee" and s["dirty"] is True
    assert json.loads((out / "summary.json").read_text()) == s
    assert not (out / "failed.json").exists()


def test_a_failed_run_leaves_failed_json(stack):
    stack.wait_error = RunFailed(f"job {JOB} is FAILED")
    with pytest.raises(RunFailed):
        eval_run.run("paths", PATHS_SEED, 6.0, world_name="paths", data_dir=stack.data_dir)
    out = stack.data_dir / stack.rid
    assert json.loads((out / "failed.json").read_text()) == {
        "run_id": stack.rid,
        "stage": "job",
        "job_id": JOB,
        "error": f"RunFailed: job {JOB} is FAILED",
    }
    assert not (out / "summary.json").exists()


def test_failed_json_never_lands_in_another_runs_directory(stack, monkeypatch):
    monkeypatch.setattr(eval_run, "run_id", lambda *args: "dev-s7-a6-20261112T101500")
    taken = stack.data_dir / "dev-s7-a6-20261112T101500"
    taken.mkdir(parents=True)
    (taken / "labels.json").write_text("{}")
    with pytest.raises(FileExistsError):
        eval_run.run("dev", 7, 6.0, data_dir=stack.data_dir)
    assert [p.name for p in taken.iterdir()] == ["labels.json"]
    assert stack.calls == []


def test_run_defaults_to_the_repos_data_dir():
    default = inspect.signature(eval_run.run).parameters["data_dir"].default
    assert default == eval_run.REPO / "data" / "eval"
    assert (eval_run.REPO / "docker-compose.yml").is_file()


def test_an_official_run_submits_the_frozen_jar(stack, monkeypatch, tmp_path):
    import hashlib

    sha = hashlib.sha256(b"jar").hexdigest()
    frozen = tmp_path / "data" / "frozen"
    frozen.mkdir(parents=True)
    (frozen / f"{sha}.jar").write_bytes(b"jar")
    fz = tmp_path / "freeze.json"
    fz.write_text(
        json.dumps(
            {
                "official_seeds": [4101, 4102, 4103, 4104, 4105],
                "amplitudes": [1.5, 6.0, 12.0],
                "jar_sha256": sha,
            }
        )
    )
    monkeypatch.setattr(eval_run, "FREEZE_PATH", fz)
    rid = "off-s4101-a6-20261113T100000"
    s = eval_run.run("off", 4101, 6.0, world_name="paths", data_dir=stack.data_dir, rid=rid)
    assert s["run_id"] == rid and stack.jar == f"/opt/tb-data/frozen/{sha}.jar"

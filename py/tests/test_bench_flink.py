import subprocess

import pytest

from tenant_baselines import bench_flink
from tenant_baselines.eval_run import RunFailed

NO_WATERMARK = -9223372036854775808


class Resp:
    def __init__(self, body, status=202):
        self.body, self.status_code = body, status

    def raise_for_status(self):
        return self

    def json(self):
        return self.body


def getter(routes: dict, calls: list | None = None):
    def get(url, timeout=None):
        path = url.removeprefix("http://127.0.0.1:8081")
        if calls is not None:
            calls.append(path)
        return Resp(routes[path])

    return get


def vtx(vid, name, **metrics):
    return {"id": vid, "name": name, "parallelism": 2, "metrics": metrics}


def test_vertex_matches_a_chained_name_by_prefix():
    job = {"vertices": [vtx("a", "Source: events"), vtx("b", "scorer -> (scores-sink: Writer)")]}
    assert bench_flink.vertex(job, "scorer")["id"] == "b"
    job["vertices"].append(vtx("c", "scorer2"))
    with pytest.raises(KeyError, match="2 vertices"):
        bench_flink.vertex(job, "scorer")
    with pytest.raises(KeyError, match="0 vertices"):
        bench_flink.vertex(job, "nothing")


class Seq:
    """A route that answers with its items in turn; the last one repeats."""

    def __init__(self, *items):
        self.items = list(items)

    def next(self):
        return self.items.pop(0) if len(self.items) > 1 else self.items[0]


def seq_getter(routes: dict, calls: list | None = None):
    def get(url, timeout=None):
        path = url.removeprefix("http://127.0.0.1:8081")
        if calls is not None:
            calls.append(path)
        body = routes[path]
        return Resp(body.next() if isinstance(body, Seq) else body)

    return get


BASE = "/jobs/j/vertices/s/metrics"
QUERY = BASE + "?get=" + ",".join(bench_flink.source_ids(2))


def reads(c0: int, c1: int, busy: float = 0.0, bp: int = 0, idle: int = 0) -> list[dict]:
    """The source's metrics for two subtasks; the clock is the sum of the three accumulates."""
    body = [
        {"id": "0.numRecordsOut", "value": str(c0)},
        {"id": "1.numRecordsOut", "value": str(c1)},
        {"id": "0.Source__events.numRecordsOut", "value": "99"},
    ]
    for i in (0, 1):
        body += [
            {"id": f"{i}.accumulateBusyTimeMs", "value": str(busy)},
            {"id": f"{i}.accumulateBackPressuredTimeMs", "value": str(bp)},
            {"id": f"{i}.accumulateIdleTimeMs", "value": str(idle)},
        ]
    return body


def job_body(busy=100):
    m = {
        "accumulated-busy-time": busy,
        "accumulated-backpressured-time": 5,
        "accumulated-idle-time": 900,
    }
    return {"vertices": [vtx("s", "Source: events", **m)]}


def take(routes, previous=None, calls=None, slept=None):
    slept = [] if slept is None else slept
    return bench_flink.sample(
        "j", "s", 2, previous=previous, get=seq_getter(routes, calls),
        now=lambda: 5.0, monotonic=lambda: 7.0, sleep=slept.append,
    )  # fmt: skip


def test_sample_sums_task_level_ids_only():
    routes = {"/jobs/j": job_body(), BASE: [], QUERY: reads(10, 20, busy=50.0, bp=2, idle=8)}
    slept, calls = [], []
    got = take(routes, calls=calls, slept=slept)
    assert calls == [BASE, QUERY, "/jobs/j", QUERY]  # one trigger; the count read brackets the job
    assert got["count"] == 30 and got["t"] == 5.0 and got["mono"] == 7.0
    assert got["clock_ms"] == 120.0 and got["stale"] is False
    assert slept == [bench_flink.REFRESH_WAIT_S] == [3.0]
    assert got["vertices"] == {
        "Source: events": {
            "busy_ms": 100,
            "backpressured_ms": 5,
            "idle_ms": 900,
            "parallelism": 2,
        }  # fmt: skip
    }


def test_a_stale_read_is_read_again_in_the_same_burst():
    previous = {"clock_ms": 120.0, "count": 30}
    routes = {
        "/jobs/j": job_body(),
        BASE: [],
        QUERY: Seq(reads(10, 20, busy=50.0, bp=2, idle=8), reads(10, 20, busy=50.0, bp=2, idle=8),
                   reads(70, 80, busy=90.0, bp=2, idle=8)),
    }  # fmt: skip
    slept, calls = [], []
    got = take(routes, previous=previous, calls=calls, slept=slept)
    assert got["stale"] is False and got["count"] == 150 and got["clock_ms"] == 200.0
    assert calls.count(BASE) == 1  # the refresh was triggered once; the second look is a re-read
    assert slept == [bench_flink.REFRESH_WAIT_S, bench_flink.STALE_RETRY_WAIT_S]


def test_a_read_that_stays_stale_is_marked():
    previous = {"clock_ms": 120.0, "count": 30}
    routes = {"/jobs/j": job_body(), BASE: [], QUERY: reads(10, 20, busy=50.0, bp=2, idle=8)}
    got = take(routes, previous=previous)
    assert got["stale"] is True


def test_a_refresh_landing_between_the_reads_is_a_mixed_sample_and_is_read_again():
    previous = {"clock_ms": 100.0, "count": 0}
    old, new = reads(10, 20, busy=50.0), reads(40, 50, busy=75.0)
    routes = {"/jobs/j": job_body(), BASE: [], QUERY: Seq(old, new, new, new)}
    slept = []
    got = take(routes, previous=previous, slept=slept)
    assert got["stale"] is False and got["count"] == 90  # the second look, not the mixed one
    assert len(slept) == 2


def test_a_mixed_sample_that_stays_mixed_is_marked_stale():
    previous = {"clock_ms": 100.0, "count": 0}
    a, b = reads(10, 20, busy=50.0), reads(40, 50, busy=75.0)
    routes = {"/jobs/j": job_body(), BASE: [], QUERY: Seq(a, b, a, b)}
    assert take(routes, previous=previous)["stale"] is True


def test_sample_raises_when_a_counter_has_no_value():
    base = "/jobs/j/vertices/s/metrics"
    routes = {
        base: [],
        QUERY: [{"id": "0.numRecordsOut", "value": "1"}],
        "/jobs/j": {"vertices": []},
    }
    with pytest.raises(KeyError, match="1.numRecordsOut"):
        bench_flink.sample("j", "s", 2, get=getter(routes), sleep=lambda s: None)


def watermark_routes(values: list):
    ids = [f"{i}.minute-aggregator.currentInputWatermark" for i in range(len(values))]
    return {
        "/jobs/j": {"vertices": [{"id": "s", "name": "src"}, {"id": "m", "name": "agg"}]},
        "/jobs/j/vertices/s/metrics": [{"id": "0.numRecordsOut"}],
        "/jobs/j/vertices/m/metrics": [{"id": i} for i in [*ids, "0.other"]],
        "/jobs/j/vertices/m/metrics?get=" + ",".join(ids): [
            {"id": i, "value": str(v)} for i, v in zip(ids, values, strict=True)
        ],
    }


def test_watermark_lag_none_before_first_watermark():
    routes = watermark_routes([NO_WATERMARK, 978_000])
    got = bench_flink.watermark_lag_s("j", 0.0, get=getter(routes), sleep=lambda s: None)
    assert got is None


def test_watermark_lag_adds_the_offset():
    routes = watermark_routes([980_000, 978_000])
    clock = iter([1000.0, 1005.0, 1010.0, 1015.0])  # only the first reading is the request time
    seq: list = []

    def now():
        seq.append("now")
        return next(clock)

    got = bench_flink.watermark_lag_s(
        "j", -0.7, get=getter(routes, seq), now=now, sleep=lambda s: None
    )
    assert got == pytest.approx(21.3)
    assert seq[0] == "now" and seq[1] == "/jobs/j"  # read before the burst's first request


def test_late_events_sums_the_subtasks():
    ids = ["0.late-events.lateEvents", "1.late-events.lateEvents"]
    routes = {
        "/jobs/j": {"vertices": [{"id": "m", "name": "x"}]},
        "/jobs/j/vertices/m/metrics": [{"id": i} for i in ids],
        "/jobs/j/vertices/m/metrics?get=" + ",".join(ids): [
            {"id": ids[0], "value": "3"},
            {"id": ids[1], "value": "4"},
        ],
    }
    assert bench_flink.late_events("j", get=getter(routes), sleep=lambda s: None) == 7


def late_routes(late_bodies, clock_bodies):
    ids = ["0.late-events.lateEvents", "1.late-events.lateEvents"]
    return {
        "/jobs/j": {"vertices": [{"id": "m", "name": "x"}]},
        "/jobs/j/vertices/m/metrics": [{"id": i} for i in ids],
        "/jobs/j/vertices/m/metrics?get=" + ",".join(ids): late_bodies,
        "/jobs/j/vertices/s/metrics": [],
        QUERY: clock_bodies,
    }


def late(value):
    return [{"id": "0.late-events.lateEvents", "value": "3"},
            {"id": "1.late-events.lateEvents", "value": str(value)}]  # fmt: skip


def test_late_events_rereads_when_the_clock_has_not_moved():
    stale, fresh = reads(0, 0, busy=50.0), reads(0, 0, busy=90.0)
    routes = late_routes(Seq(late(0), late(4)), Seq(stale, stale, fresh, fresh))
    slept = []
    got = bench_flink.late_events(
        "j", fresh_after=("s", 2, 100.0), get=seq_getter(routes), sleep=slept.append
    )
    assert got == 7 and len(slept) == 2


def test_late_events_raises_when_the_clock_never_moves():
    stale = reads(0, 0, busy=50.0)
    routes = late_routes(late(0), stale)
    with pytest.raises(bench_flink.RunFailed, match="stale"):
        bench_flink.late_events(
            "j", fresh_after=("s", 2, 100.0), get=seq_getter(routes), sleep=lambda s: None
        )


def test_restarts_and_job_times_and_config():
    routes = {
        "/jobs/j/exceptions": {"exceptionHistory": {"entries": [{}, {}]}},
        "/jobs/j": {
            "start-time": 10,
            "end-time": 90,
            "vertices": [
                {"name": "Source: events", "end-time": 80},
                {"name": "scorer", "end-time": 90},
            ],
        },
        "/jobs/j/checkpoints/config": {"interval": 30000, "state_backend": "RocksDBStateBackend"},
    }
    get = getter(routes)
    assert bench_flink.restarts("j", get) == 2
    assert bench_flink.job_times("j", get) == {"start_ms": 10, "end_ms": 90, "source_end_ms": 80}
    assert bench_flink.checkpoint_config("j", get) == {
        "interval_ms": 30000,
        "state_backend": "RocksDBStateBackend",
    }


def entry(i, ts, status="COMPLETED"):
    return {
        "id": i,
        "status": status,
        "trigger_timestamp": ts,
        "state_size": i * 100,
        "checkpointed_size": i * 10,
        "other": "dropped",
    }


def test_checkpoint_history_drops_failed_and_in_progress():
    history = [entry(1, 10), entry(2, 20, "FAILED"), entry(3, 30, "IN_PROGRESS"), entry(4, 40)]
    got = bench_flink.checkpoint_history("j", getter({"/jobs/j/checkpoints": {"history": history}}))
    assert got == [
        {"id": 1, "trigger_timestamp": 10, "state_size": 100, "checkpointed_size": 10},
        {"id": 4, "trigger_timestamp": 40, "state_size": 400, "checkpointed_size": 40},
    ]


def test_latest_before():
    history = [{"id": i, "trigger_timestamp": t} for i, t in {1: 10, 2: 20, 3: 30}.items()]
    assert bench_flink.latest_before(history, 25)["id"] == 2
    assert bench_flink.latest_before(history, None)["id"] == 3
    with pytest.raises(ValueError):
        bench_flink.latest_before(history, 5)


def test_vertex_state_maps_ids_to_names():
    routes = {
        "/jobs/j": {
            "vertices": [{"id": "a", "name": "Source: events"}, {"id": "b", "name": "agg"}]
        },
        "/jobs/j/checkpoints/details/7": {
            "tasks": {
                "a": {"state_size": 0, "checkpointed_size": 0, "x": 1},
                "b": {"state_size": 500, "checkpointed_size": 50},
            }
        },
    }
    assert bench_flink.vertex_state("j", 7, getter(routes)) == [
        {"name": "Source: events", "state_size": 0, "checkpointed_size": 0},
        {"name": "agg", "state_size": 500, "checkpointed_size": 50},
    ]


def cancel_world(states: list[str]):
    """A job that reports each state of `states` in turn, then the last one for good."""
    it = iter(states)
    last = [states[0]]

    def get(url, timeout=None):
        last[0] = next(it, last[0])
        return Resp({"state": last[0]})

    return get


def ok(cmds: list):
    def run(cmd):
        cmds.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return run


def test_cancel_and_clean_waits_then_removes_the_directory():
    cmds, patched = [], []

    def patch(url, params, timeout):
        patched.append((url, params))
        return Resp({})

    bench_flink.cancel_and_clean(
        "j", "r1", get=cancel_world(["RUNNING", "CANCELLING", "CANCELED"]), patch=patch,
        run=ok(cmds), sleep=lambda s: None,
    )  # fmt: skip
    assert patched == [("http://127.0.0.1:8081/jobs/j", {"mode": "cancel"})]
    assert cmds[0][-3:] == ["rm", "-rf", "/flink-data/checkpoints/bench/r1"]


def test_cancel_and_clean_treats_a_409_as_a_job_that_is_over():
    cmds = []
    bench_flink.cancel_and_clean(
        "j", "r1", get=cancel_world(["RUNNING", "FINISHED"]),
        patch=lambda *a, **k: Resp({}, 409), run=ok(cmds), sleep=lambda s: None,
    )  # fmt: skip
    assert len(cmds) == 1


def test_cancel_and_clean_raises_when_the_cancel_is_refused():
    cmds = []
    with pytest.raises(RunFailed, match="HTTP 500"):
        bench_flink.cancel_and_clean(
            "j", "r1", get=cancel_world(["RUNNING"]), patch=lambda *a, **k: Resp({}, 500),
            run=cmds.append, sleep=lambda s: None,
        )  # fmt: skip
    assert cmds == []


def test_cancel_and_clean_raises_when_the_removal_fails():
    failed = subprocess.CompletedProcess([], 1, stdout="", stderr="permission denied")
    with pytest.raises(RunFailed, match="permission denied"):
        bench_flink.cancel_and_clean(
            "j", "r1", get=cancel_world(["FINISHED"]), patch=None, run=lambda cmd: failed
        )


def test_cancel_and_clean_only_cleans_a_finished_job():
    cmds = []
    bench_flink.cancel_and_clean(
        "j", "r1", get=cancel_world(["FINISHED"]), patch=None, run=ok(cmds)
    )
    assert len(cmds) == 1


def test_cancel_and_clean_gives_up_after_the_wait():
    clock = iter(range(0, 1000, 20))
    cmds = []
    with pytest.raises(RunFailed, match="CANCELLING"):
        bench_flink.cancel_and_clean(
            "j", "r1", get=cancel_world(["RUNNING", "CANCELLING"]),
            patch=lambda *a, **k: Resp({}), run=ok(cmds),
            sleep=lambda s: None, monotonic=lambda: next(clock),
        )  # fmt: skip
    assert cmds == []  # the directory of a job that may still run is left alone


def test_taskmanager_memory_reads_docker_stats():
    def run(cmd):
        out = "abc123\n" if cmd[:3] == ["docker", "compose", "ps"] else "1.5GiB / 3GiB\n"
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    assert bench_flink.taskmanager_memory(run) == {
        "used_bytes": int(1.5 * 1024**3),
        "limit_bytes": 3 * 1024**3,
    }


def test_submit_returns_the_last_line_and_passes_the_arguments():
    seen = []

    def run(cmd, **kw):
        seen.append(cmd)
        return subprocess.CompletedProcess(
            cmd, 0, stdout="Job has been submitted\n" + "a" * 32 + "\n"
        )

    assert bench_flink.submit("r1", "bench.in", ["-Dx=1"], run=run) == "a" * 32
    assert seen[0][-3:] == ["r1", "bench.in", "-Dx=1"]
    assert "submit-bench.sh" in " ".join(seen[0])


def test_submit_raises_on_a_failed_script_or_a_bad_id():
    for rc, out in [(1, "a" * 32), (0, "not a job id"), (0, "")]:
        done = subprocess.CompletedProcess([], rc, stdout=out)
        with pytest.raises(RunFailed, match="submit-bench.sh"):
            bench_flink.submit("r1", "t", run=lambda cmd, done=done, **kw: done)

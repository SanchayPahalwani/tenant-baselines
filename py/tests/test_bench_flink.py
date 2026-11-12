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


def test_sample_sums_task_level_ids_only():
    busy = {
        "accumulated-busy-time": 100,
        "accumulated-backpressured-time": 5,
        "accumulated-idle-time": 900,
    }
    base = "/jobs/j/vertices/s/metrics"
    routes = {
        "/jobs/j": {"vertices": [vtx("s", "Source: events", **busy)]},
        base: [],
        base + "?get=0.numRecordsOut,1.numRecordsOut": [
            {"id": "0.numRecordsOut", "value": "10"},
            {"id": "1.numRecordsOut", "value": "20"},
            {"id": "0.Source__events.numRecordsOut", "value": "10"},
        ],
    }
    slept = []
    got = bench_flink.sample(
        "j", "s", 2, get=getter(routes), now=lambda: 5.0, monotonic=lambda: 7.0, sleep=slept.append
    )
    assert got["count"] == 30 and got["t"] == 5.0 and got["mono"] == 7.0
    assert slept == [1.0]
    assert got["vertices"] == {
        "Source: events": {
            "busy_ms": 100,
            "backpressured_ms": 5,
            "idle_ms": 900,
            "parallelism": 2,
        }  # fmt: skip
    }


def test_sample_raises_when_a_counter_has_no_value():
    base = "/jobs/j/vertices/s/metrics"
    routes = {
        base: [],
        base + "?get=0.numRecordsOut,1.numRecordsOut": [{"id": "0.numRecordsOut", "value": "1"}],
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
    got = bench_flink.watermark_lag_s(
        "j", -0.7, get=getter(routes), now=lambda: 1000.0, sleep=lambda s: None
    )
    assert got == pytest.approx(21.3)


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


def test_cancel_and_clean_waits_then_removes_the_directory():
    cmds, patched = [], []

    def patch(url, params, timeout):
        patched.append((url, params))
        return Resp({})

    bench_flink.cancel_and_clean(
        "j", "r1", get=cancel_world(["RUNNING", "CANCELLING", "CANCELED"]), patch=patch,
        run=lambda cmd: cmds.append(cmd), sleep=lambda s: None,
    )  # fmt: skip
    assert patched == [("http://127.0.0.1:8081/jobs/j", {"mode": "cancel"})]
    assert cmds[0][-3:] == ["rm", "-rf", "/flink-data/checkpoints/bench/r1"]


def test_cancel_and_clean_only_cleans_a_finished_job():
    cmds = []
    bench_flink.cancel_and_clean(
        "j", "r1", get=cancel_world(["FINISHED"]), patch=None, run=cmds.append
    )
    assert len(cmds) == 1


def test_cancel_and_clean_gives_up_after_the_wait():
    clock = iter(range(0, 1000, 20))
    with pytest.raises(RunFailed, match="CANCELLING"):
        bench_flink.cancel_and_clean(
            "j", "r1", get=cancel_world(["RUNNING", "CANCELLING"]),
            patch=lambda *a, **k: Resp({}), run=lambda cmd: None,
            sleep=lambda s: None, monotonic=lambda: next(clock),
        )  # fmt: skip


def test_taskmanager_memory_reads_docker_stats():
    def run(cmd):
        out = "abc123\n" if cmd[:3] == ["docker", "compose", "ps"] else "1.5GiB / 3GiB\n"
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    assert bench_flink.taskmanager_memory(run) == {
        "used_bytes": int(1.5 * 1024**3),
        "limit_bytes": 3 * 1024**3,
    }

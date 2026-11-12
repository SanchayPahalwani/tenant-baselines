import json
from types import SimpleNamespace

import pytest

from tenant_baselines import bench_state as bs
from tenant_baselines.eval_run import RunFailed


def ckpt(i, trigger, size):
    return {"id": i, "trigger_timestamp": trigger, "state_size": size, "checkpointed_size": size}


def test_in_last_share():
    assert bs.in_last_share(90_000, 0, 100_000)
    assert not bs.in_last_share(80_000, 0, 100_000)
    assert not bs.in_last_share(100_500, 0, 100_000)


def test_per_key_divides_the_scorer_only():
    vertices = [
        {"name": "scorer", "state_size": 48_000_000},
        {"name": "population-baselines", "state_size": 2_000_000},
        {"name": "region-grouper -> (alerts-sink: Writer)", "state_size": 500_000},
        {"name": "Source: aggregates", "state_size": 100},
    ]
    got = bs.per_key(vertices, 48)
    assert got["scorer_bytes"] == 48_000_000
    assert got["per_key_bytes"] == 1_000_000
    assert got["other_vertices"] == [
        {"name": "population-baselines", "state_size": 2_000_000},
        {"name": "region-grouper -> (alerts-sink: Writer)", "state_size": 500_000},
        {"name": "Source: aggregates", "state_size": 100},
    ]


def test_per_key_needs_a_scorer_vertex():
    with pytest.raises(KeyError):
        bs.per_key([{"name": "Source: aggregates", "state_size": 1}], 48)


def test_size_range():
    history = [
        ckpt(1, 50_000, 3),
        ckpt(2, 86_000, 5),
        ckpt(3, 91_000, 9),
        ckpt(4, 96_000, 7),
        ckpt(5, 101_000, 1),  # after the source ended
    ]
    assert bs.size_range(history, 0, 100_000) == {"checkpoints": 3, "min": 5, "max": 9}


def test_size_range_of_nothing():
    assert bs.size_range([], 0, 100_000) == {"checkpoints": 0, "min": None, "max": None}


def test_count_settings():
    sets = [
        {"detector": "global_static", "setting": "p99"},
        {"detector": "global_static", "setting": "p99"},
        {"detector": "per_key_static", "setting": "p99"},
    ]
    assert bs.count_settings({"sets": sets}, [3, 4, 5, 6]) == 6


def test_extrapolate():
    assert bs.extrapolate(1_000_000, (2_000,)) == [{"keys": 2000, "bytes": 2_000_000_000}]
    assert bs.extrapolate(0.4, (3,)) == [{"keys": 3, "bytes": 1}]


def test_spec_estimate():
    assert bs.SPEC_ESTIMATE_BYTES_PER_KEY == 604_800


def test_backend_names():
    assert bs.backend_of("HashMapStateBackend") == "hashmap"
    assert bs.backend_of("EmbeddedRocksDBStateBackend") == "rocksdb"
    assert bs.backend_of("Something") is None


def test_world_counts():
    keys, five = bs.world_counts()
    assert keys == 48
    assert five == 46


# ---- measure_run ----


def flink_fake(history, config=None, times=None):
    return SimpleNamespace(
        job_times=lambda job_id: (
            times or {"start_ms": 0, "end_ms": 110_000, "source_end_ms": 100_000}
        ),
        checkpoint_config=lambda job_id: (
            config or {"interval_ms": 5000, "state_backend": "HashMapStateBackend"}
        ),
        checkpoint_history=lambda job_id: history,
        vertex_state=lambda job_id, cid: [
            {"name": "scorer", "state_size": SCORER_BY_CHECKPOINT[cid], "checkpointed_size": 1},
            {"name": "Source: aggregates", "state_size": 2, "checkpointed_size": 2},
        ],
        checkpoint_history_length=lambda job_id: len(history) + 2,
        checkpoints_after_tasks_finish=lambda job_id: True,
        latest_before=bs.bench_flink.latest_before,
    )


SCORER_BY_CHECKPOINT = {7: 4_000, 8: 480, 9: 5_000}
HISTORY = [ckpt(7, 90_000, 400), ckpt(8, 95_000, 480), ckpt(9, 104_000, 500)]


def test_measure_run_reports_the_last_checkpoint_before_the_source_ended():
    got = bs.measure_run("j", "hashmap", 48, flink_fake(HISTORY))
    assert got["backend"] == "hashmap"
    assert got["interval_ms"] == 5000
    assert got["checkpoint"]["id"] == 8
    assert got["checkpoint"]["trigger_share"] == 0.95
    assert got["checkpoint"]["state_size"] == 480
    assert got["checkpoint"]["vertices"][0]["state_size"] == 480  # checkpoint 8's, not 7's or 9's
    assert got["history_checkpoints"] == 3
    assert got["history_entries"] == 5
    assert got["checkpoints_after_tasks_finish"] is True
    assert got["range"] == {"checkpoints": 2, "min": 400, "max": 480}
    assert got["per_key"]["per_key_bytes"] == 10
    assert got["job_seconds"] == 110


def test_measure_run_fails_when_no_checkpoint_is_in_the_last_share():
    old = [ckpt(1, 20_000, 10), ckpt(2, 60_000, 20)]
    with pytest.raises(RunFailed, match="was triggered before"):
        bs.measure_run("j", "hashmap", 48, flink_fake(old))


def test_measure_run_fails_with_no_checkpoint_before_the_source_ended():
    with pytest.raises(RunFailed, match="no completed checkpoint"):
        bs.measure_run("j", "hashmap", 48, flink_fake([ckpt(1, 120_000, 10)]))


def test_measure_run_fails_on_the_wrong_backend():
    cfg = {"interval_ms": 5000, "state_backend": "EmbeddedRocksDBStateBackend"}
    with pytest.raises(RunFailed, match="hashmap"):
        bs.measure_run("j", "hashmap", 48, flink_fake(HISTORY, config=cfg))


def test_measure_run_fails_on_the_wrong_interval():
    cfg = {"interval_ms": 30_000, "state_backend": "HashMapStateBackend"}
    with pytest.raises(RunFailed, match="5000"):
        bs.measure_run("j", "hashmap", 48, flink_fake(HISTORY, config=cfg))


# ---- run_backend ----


def clock_at(*seconds):
    """A clock whose monotonic() returns the given readings in turn, then the last one."""
    it = iter(seconds)
    last = [0.0]

    def monotonic():
        last[0] = next(it, last[0])
        return last[0]

    return SimpleNamespace(monotonic=monotonic)


def ops_fake(tmp_path, calls, fail_at=None, clock=None, clean_error=None):
    def step(name, value=None):
        def f(*a, **k):
            calls.append(name)
            if fail_at == name:
                raise RunFailed(f"{name} broke")
            return value

        return f

    def prepare(kind, world, seed, amp, out):
        calls.append("prepare")
        out.mkdir(parents=True)
        (out / "thresholds.json").write_text(
            json.dumps({"sets": [{"detector": "d", "setting": "s"}]})
        )
        return SimpleNamespace(keys=list(range(48))), []

    def submit(run_id, mode, topic, thresholds, extra=()):
        calls.append(("submit", list(extra)))
        return "a" * 32

    def wait(job_id, get=None, timeout_s=0):
        calls.append("wait")
        if fail_at == "wait":
            raise RunFailed("wait broke")
        for _ in range(3):  # three polls of the job
            get("http://flink/jobs/j")

    def clean(job_id, run_id):
        calls.append("clean")
        if clean_error:
            raise RunFailed(clean_error)

    return SimpleNamespace(
        prepare=prepare,
        create_topic=step("create_topic"),
        produce=step("produce", 1234),
        end_offsets=lambda t: {0: 1234},
        submit=submit,
        container_path=lambda p: f"/opt/tb-data/{p.name}",
        wait_finished=wait,
        measure=step("measure", {"backend": "hashmap", "interval_ms": 5000}),
        restarts=step("restarts", 0),
        clean=clean,
        http_get=lambda *a, **k: None,
        clock=clock or clock_at(*range(1, 100)),
    )


def test_run_backend_submits_with_the_backend_and_interval(tmp_path):
    calls = []
    polls = bs.Polls()
    run = bs.run_backend("hashmap", tmp_path, polls, ops=ops_fake(tmp_path, calls))
    submit = [c for c in calls if isinstance(c, tuple)][0]
    assert "-Dexecution.checkpointing.interval=5s" in submit[1]
    assert "-Dstate.backend.type=hashmap" in submit[1]
    assert run["backend"] == "hashmap"
    assert run["restarts"] == 0
    assert run["aggregate_rows"] == 1234
    assert run["keys"] == 48
    assert run["settings"] == 5
    assert calls.index("measure") < calls.index("clean")
    assert run["run_id"].startswith("state-hashmap-")
    assert len(polls) == 3 + 1  # one tick per poll of the wait, one after it
    assert run["stall_gaps_s"] == []


def test_a_stall_inside_the_wait_is_recorded(tmp_path):
    clock = clock_at(0, 2, 100, 102)  # a 98 s gap between the second and third poll
    run = bs.run_backend("hashmap", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, [], clock=clock))
    assert run["stall_gaps_s"] == [98.0]


def test_a_failed_cleanup_keeps_the_measurement(tmp_path):
    run = bs.run_backend(
        "hashmap", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, [], clean_error="rm failed")
    )
    assert "rm failed" in run["cleanup_error"]
    saved = json.loads(next(tmp_path.glob("state-hashmap-*/run.json")).read_text())
    assert saved["cleanup_error"] == run["cleanup_error"]
    assert run["backend"] == "hashmap"


def test_run_backend_rocksdb_keeps_the_cluster_backend(tmp_path):
    calls = []
    bs.run_backend("rocksdb", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, calls))
    submit = [c for c in calls if isinstance(c, tuple)][0]
    assert submit[1] == ["-Dexecution.checkpointing.interval=5s"]


def test_a_failed_run_keeps_its_records_and_cleans(tmp_path):
    calls = []
    with pytest.raises(RunFailed, match="wait broke"):
        bs.run_backend(
            "hashmap", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, calls, fail_at="wait")
        )
    assert "clean" in calls
    failed = list(tmp_path.glob("state-hashmap-*/failed.json"))
    assert len(failed) == 1
    doc = json.loads(failed[0].read_text())
    assert doc["stage"] == "job"
    assert doc["job_id"] == "a" * 32
    assert "wait broke" in doc["error"]


def test_a_failed_measure_cleans_after_the_failure(tmp_path):
    calls = []
    with pytest.raises(RunFailed, match="measure broke"):
        bs.run_backend(
            "hashmap", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, calls, fail_at="measure")
        )
    assert calls[-1] == "clean"


# ---- the result ----


def good_run(backend):
    return {
        "backend": backend,
        "restarts": 0,
        "keys": 48,
        "aggregate_rows": 10,
        "settings": 5,
        "host_sleep_s": 0.0,
        "stall_gaps_s": [],
        "per_key": {"per_key_bytes": 1000.0},
    }


def test_result_is_valid_with_no_reasons():
    runs = [good_run("hashmap"), good_run("rocksdb")]
    r = bs.build_result({"data": "synthetic"}, runs, "AC Power", False)
    assert r["valid"] and r["invalid_reasons"] == []
    assert r["headline_backend"] == "hashmap"
    assert r["extrapolation"]["label"] == "extrapolation, not measured"
    assert r["extrapolation"]["hashmap"] == [
        {"keys": 2000, "bytes": 2_000_000},
        {"keys": 20000, "bytes": 20_000_000},
        {"keys": 100000, "bytes": 100_000_000},
    ]
    assert r["spec_estimate_bytes_per_key"] == 604_800
    assert r["world"]["keys"] == 48 and r["world"]["keys_with_five_weeks"] == 46
    assert r["world"]["aggregate_rows"] == 10
    assert r["settings"] == 5
    json.dumps(r, allow_nan=False)


def test_result_is_invalid_after_a_sleep_a_stall_or_battery():
    run = good_run("hashmap") | {"host_sleep_s": 30.0, "stall_gaps_s": [75.0]}
    r = bs.build_result({}, [run], "Battery Power", False)
    text = " ".join(r["invalid_reasons"])
    assert not r["valid"]
    assert "slept" in text and "stalled" in text and "battery" in text


def test_on_battery_flag_waives_the_battery_reason():
    assert bs.build_result({}, [good_run("hashmap")], "Battery Power", True)["valid"]


def test_result_is_invalid_after_a_restart():
    run = good_run("hashmap") | {"restarts": 1}
    assert not bs.build_result({}, [run], "AC Power", False)["valid"]


def test_result_refuses_runs_that_disagree():
    for field, value in (("keys", 47), ("aggregate_rows", 11), ("settings", 6)):
        runs = [good_run("hashmap"), good_run("rocksdb") | {field: value}]
        with pytest.raises(RunFailed):
            bs.build_result({}, runs, "AC Power", False)


def test_run_backend_records_the_rise_of_the_sleep_counter(tmp_path, monkeypatch):
    readings = iter([100.0, 100.0, 100.0, 400.0])
    monkeypatch.setattr(bs.bench_env, "slept_s", lambda: next(readings, 400.0))
    monkeypatch.setattr(bs.bench_env, "load_record", lambda: {"host_load_1m": 1.0})
    run = bs.run_backend("hashmap", tmp_path, bs.Polls(), ops=ops_fake(tmp_path, []))
    assert run["host_sleep_s"] == 300.0


def test_run_backend_refuses_a_topic_that_does_not_hold_every_row(tmp_path, monkeypatch):
    monkeypatch.setattr(bs.bench_env, "load_record", lambda: {"host_load_1m": 1.0})
    calls = []
    ops = ops_fake(tmp_path, calls)
    ops.end_offsets = lambda topic: {0: 1233}
    with pytest.raises(RunFailed, match="does not hold exactly the 1234 rows produced"):
        bs.run_backend("hashmap", tmp_path, bs.Polls(), ops=ops)
    assert "submit" not in [c[0] if isinstance(c, tuple) else c for c in calls]

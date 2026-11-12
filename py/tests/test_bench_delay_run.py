import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tenant_baselines import bench_delay, bench_world
from tenant_baselines import bench_delay_run as bdr
from tenant_baselines.eval_run import RunFailed

T0 = 1_800_000_000_000
AC = "Now drawing from 'AC Power'"
BATTERY = "Now drawing from 'Battery Power'"


def labels_for(n=3):
    inc = bench_delay.schedule(bench_world.build("delay", 1), T0, n, 1)
    return bench_delay.labels(inc, bench_world.build("delay", 1), T0)


# ---- pure parts ----


def test_plan_reads_three_minutes_after_the_last_incident():
    lab = labels_for(6)
    last_end = max(i["end_ms"] for i in lab["incidents"])
    assert bdr.plan(lab) == {
        "first_start_ms": min(i["start_ms"] for i in lab["incidents"]),
        "read_at_ms": last_end + 180_000,
    }


def test_lag_summary():
    assert bdr.lag_summary([21.1, 22.5, 25.7]) == {
        "samples": 3, "min": 21.1, "median": 22.5, "max": 25.7,
    }  # fmt: skip
    assert bdr.lag_summary([]) == {"samples": 0, "min": None, "median": None, "max": None}


def test_offset_moved():
    assert bdr.offset_moved({"offset_s": -0.70}, {"offset_s": -0.20}) is False
    assert bdr.offset_moved({"offset_s": -0.70}, {"offset_s": 0.50}) is True
    assert bdr.offset_moved({"offset_s": 0.0}, {"offset_s": -1.5}) is True


def test_generator_command_runs_in_the_compose_image(tmp_path):
    cmd = bdr.generator_command("delay-x", 60, tmp_path)
    assert cmd[:7] == ["docker", "compose", "--profile", "demo", "run", "-d", "--no-deps"]
    assert cmd[cmd.index("--name") + 1] == "tb-delay-x"
    assert cmd[cmd.index("-v") + 1] == f"{tmp_path}:/out"
    assert cmd.index("generator") > cmd.index("-v")
    tail = cmd[cmd.index("generator") + 1 :]
    assert tail == [
        *("python", "-m", "tenant_baselines.gen_live"),
        *("--bootstrap", "kafka:19092", "--topic", "delay-x.events"),
        *("--world", "delay", "--seed", "1", "--script", "bench"),
        *("--bench-incidents", "60", "--labels-out", "/out/labels.json"),
        *("--history-days", "21", "--partitions", "8"),
    ]


def test_rows_of_this_run_only():
    rows = [(1, {"run_id": "a", "x": 1}), (2, {"run_id": "b"}), (3, {"x": 2})]
    assert bdr.rows_of_run(rows, "a") == [(1, {"run_id": "a", "x": 1})]


def test_the_run_dir_is_never_reused(tmp_path):
    bdr.make_run_dir(tmp_path, "delay-x")
    with pytest.raises(FileExistsError):
        bdr.make_run_dir(tmp_path, "delay-x")


def test_power_at_the_end_is_a_reason_without_on_battery():
    assert bdr.power_reasons(BATTERY, False) == ["the laptop went to battery during the run"]
    assert bdr.power_reasons(BATTERY, True) == []
    assert bdr.power_reasons(AC, False) == []
    assert bdr.power_reasons(None, False) == []


# ---- the lag reader, the catch-up wait and the watch loop ----


class Clock:
    def __init__(self):
        self.host = T0 / 1000
        self.mono = 5000.0
        self.slept = []

    def time(self):
        return self.host

    def monotonic(self):
        return self.mono

    def sleep(self, s):
        self.slept.append(s)
        self.host += s
        self.mono += s


def fake_sample_fn(clock, lags, par=2):
    """A lag sample per call: `lags` gives the lag (or 'stale'/None) at the host time."""

    def fn(job_id, offset_s, source, previous=None, **kw):
        v = lags(clock.host) if callable(lags) else lags.pop(0)
        clock.host += 3.0
        clock.mono += 3.0
        return {
            "t": clock.host - 3.0,
            "lag_s": None if v in ("stale", None) else v,
            "clock_ms": (clock.host - T0 / 1000) * 1000 * par,
            "stale": v == "stale",
            "read_s": 3.0,
            "reread": False,
        }

    return fn


def reader_for(clock, lags, offset=0.0):
    return bdr.LagReader("j", offset, ("s", 2), sample_fn=fake_sample_fn(clock, lags))


def test_reader_checks_each_sample_against_the_last_fresh_one():
    seen = []

    def fn(job_id, offset_s, source, previous=None, **kw):
        seen.append(previous and previous["stale"])
        return {"t": 1.0, "lag_s": 21.0, "clock_ms": 1.0, "stale": len(seen) == 2, "read_s": 3.0}

    r = bdr.LagReader("j", -0.7, ("s", 2), sample_fn=fn)
    for _ in range(3):
        r.read()
    assert seen == [None, False, False]  # the stale second sample is never the "previous"


def test_a_clock_that_runs_backwards_twice_is_a_restart():
    reads = iter([(100_000.0, False), (50.0, True), (60.0, True)])

    def fn(job_id, offset_s, source, previous=None, **kw):
        clock_ms, stale = next(reads)
        return {"t": 1.0, "lag_s": 2.0, "clock_ms": clock_ms, "stale": stale, "read_s": 3.0}

    r = bdr.LagReader("j", 0.0, ("s", 2), sample_fn=fn)
    r.read()
    r.read()  # one regression: not yet a restart
    with pytest.raises(RunFailed, match="restarted"):
        r.read()


def test_catch_up_returns_the_first_fresh_lag_below_60():
    clock = Clock()
    r = reader_for(clock, [900.0, "stale", None, 75.0, 40.0, 5.0])
    polls = []
    got = bdr.wait_caught_up(
        r, T0 + 600_000, 0.0, polls=polls, check=lambda: None,
        time_=clock, 
    )  # fmt: skip
    assert got == 40.0
    assert len(polls) == 5 and len(r.samples) == 5
    assert all(s == 10 for s in clock.slept)


def test_catch_up_fails_one_minute_before_the_first_incident():
    clock = Clock()
    r = reader_for(clock, lambda t: 500.0)
    with pytest.raises(RunFailed, match="did not catch up"):
        bdr.wait_caught_up(r, T0 + 120_000, 0.0, polls=[], check=lambda: None, time_=clock)
    assert clock.host * 1000 >= T0 + 60_000  # it waited up to one minute before the start
    assert clock.host * 1000 < T0 + 60_000 + 30_000


def test_catch_up_stops_when_the_generator_is_gone():
    clock = Clock()
    r = reader_for(clock, lambda t: 500.0)

    def gone():
        raise RunFailed("the generator exited")

    with pytest.raises(RunFailed, match="generator exited"):
        bdr.wait_caught_up(r, T0 + 600_000, 0.0, polls=[], check=gone, time_=clock)


def test_watch_polls_every_30_s_samples_every_second_poll_and_checks_the_generator():
    clock = Clock()
    r = reader_for(clock, lambda t: 21.0)
    polls, checks = [], []
    bdr.watch(r, T0 + 300_000, 0.0, polls=polls, check=lambda: checks.append(1), time_=clock)
    assert len(checks) == len(polls) >= 9
    assert len(r.samples) == (len(polls) + 1) // 2
    gaps = [b - a for a, b in zip(polls, polls[1:], strict=False)]
    assert all(29.9 < g < 34 for g in gaps)  # a sampling poll runs a few seconds long
    assert clock.host * 1000 >= T0 + 300_000


def test_watch_fails_when_the_generator_exits():
    clock = Clock()
    r = reader_for(clock, lambda t: 21.0)
    n = []

    def check():
        n.append(1)
        if len(n) == 3:
            raise RunFailed("the generator container exited")

    with pytest.raises(RunFailed, match="exited"):
        bdr.watch(r, T0 + 900_000, 0.0, polls=[], check=check, time_=clock)
    assert len(r.samples) == 1


def test_lag_block_keeps_samples_from_the_first_incident_and_counts_the_dropped():
    clock = Clock()
    r = reader_for(clock, [900.0, 30.0, 25.0, "stale", None, 22.0, 24.0])
    for _ in range(7):
        r.read()
    first_start = T0 + 3_000 + 3_000  # the third sample's t is T0 + 6 s
    block = r.block(first_start, caught_up_lag=30.0)
    # samples 3, 6 and 7 are fresh and from the first start on; 4 is stale, 5 has no watermark
    assert block["summary"] == {"samples": 3, "min": 22.0, "median": 24.0, "max": 25.0}
    assert block["stale_lag_samples"] == 1
    assert block["no_watermark_samples"] == 1
    assert block["caught_up_lag_s"] == 30.0
    assert block["at_first_incident_s"] == 25.0


def test_lag_block_has_no_first_incident_lag_without_a_kept_sample():
    clock = Clock()
    r = reader_for(clock, [900.0])
    r.read()
    block = r.block(T0 + 10**9, caught_up_lag=5.0)
    assert block["summary"]["samples"] == 0 and block["at_first_incident_s"] is None


def test_the_lag_at_the_first_incident_is_a_reason_when_not_near_the_bound():
    assert bdr.lag_reasons({"at_first_incident_s": 30.0}) == []
    assert bdr.lag_reasons({"at_first_incident_s": None}) == []
    assert len(bdr.lag_reasons({"at_first_incident_s": 61.0})) == 1


# ---- the whole driver, on fakes ----


def build_harness(monkeypatch, tmp_path, *, gen_exits_at=None, lag=lambda t: 22.0, logs="",
                  offsets=None, matched=3, power_end=AC, restarts=0):  # fmt: skip
    clock = Clock()
    h = SimpleNamespace(clock=clock, cmds=[], cancelled=[], result_dir=None, running=True)
    monkeypatch.setattr(bdr, "time", clock)
    monkeypatch.setattr(bdr, "DATA_BENCH", tmp_path / "bench")
    (tmp_path / "bench").mkdir()
    monkeypatch.setattr(bdr.bench_env, "preflight", lambda on_battery: None)
    monkeypatch.setattr(bdr.bench_env, "collect", lambda on_battery: {"data": "synthetic"})
    monkeypatch.setattr(bdr.bench_env, "_power_line", lambda run: power_end)
    offs = iter(offsets or [{"offset_s": -0.7, "bracket_s": 0.1}] * 2)
    monkeypatch.setattr(bdr.bench_env, "vm_clock_offset", lambda: next(offs))
    monkeypatch.setattr(bdr.bench_throughput, "_load", lambda: {"host_load_1m": 1.0})
    monkeypatch.setattr(bdr.bench_throughput, "_wait_running", lambda job_id: None)
    monkeypatch.setattr(bdr.kafka_io, "create_topic", lambda name, parts: None)
    monkeypatch.setattr(bdr.kafka_io, "end_offsets", lambda topic: {p: 5 for p in range(8)})
    monkeypatch.setattr(bdr.bench_flink, "submit", lambda run_id, topic: "j" * 32)
    monkeypatch.setattr(
        bdr.bench_flink, "_json",
        lambda get, path: {"vertices": [{"id": "s", "name": "Source: events", "parallelism": 2}]},
    )  # fmt: skip
    monkeypatch.setattr(bdr.bench_flink, "restarts", lambda job_id: restarts)
    monkeypatch.setattr(bdr.bench_flink, "late_events", lambda job_id, fresh_after=None: 0)
    monkeypatch.setattr(
        bdr.bench_flink, "cancel_and_clean", lambda j, r: h.cancelled.append((j, r))
    )
    monkeypatch.setattr(bdr.bench_flink, "watermark_lag_sample", fake_sample_fn(clock, lag))

    state = {"inspects": 0, "labels": None}

    def sh(cmd):
        h.cmds.append(cmd)
        out = ""
        if cmd[:3] == ["docker", "compose", "--profile"]:
            out_dir = Path(cmd[cmd.index("-v") + 1].split(":")[0])
            lab = bench_delay.labels(
                bench_delay.schedule(bench_world.build("delay", 1), int(clock.host * 1000), 3, 1),
                bench_world.build("delay", 1),
                int(clock.host * 1000),
            )
            (out_dir / "labels.json").write_text(json.dumps(lab))
            state["labels"] = lab
        elif cmd[:2] == ["docker", "inspect"]:
            state["inspects"] += 1
            running = gen_exits_at is None or state["inspects"] < gen_exits_at
            out = "true\n" if running else "false\n"
        elif cmd[:2] == ["docker", "logs"]:
            out = logs
        return SimpleNamespace(returncode=0, stdout=out, stderr="")

    monkeypatch.setattr(bdr.bench_env, "_sh", sh)

    def read_stamped(topic, start, end):
        rows = []
        for inc in state["labels"]["incidents"][:matched]:
            ev = inc["start_ms"] + 240_000
            row = {
                "alert_id": f"a{inc['id']}", "state": "firing", "detector": "seasonal",
                "setting": "k4", "scope": "tenant", "key": inc["key"], "signal": inc["signal"],
                "event_time_ms": ev, "emitted_at_ms": ev + 1000, "run_id": h.run_id(),
            }  # fmt: skip
            rows.append((ev + 25_000, row))
            rows.append((ev + 26_000, {**row, "run_id": "someone-else"}))
        return rows

    monkeypatch.setattr(bdr.bench_env, "read_stamped", read_stamped)
    h.run_id = lambda: next((tmp_path / "bench").iterdir()).name
    return h


def run_main(h, tmp_path, *extra):
    bdr.main(["--incidents", "3", "--pilot", *extra])
    d = next((tmp_path / "bench").iterdir())
    return json.loads((d / "result.json").read_text()), d


def test_a_clean_run_writes_a_valid_result_from_this_runs_rows_only(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path)
    r, d = run_main(h, tmp_path)
    assert r["valid"] is True and r["invalid_reasons"] == []
    assert r["data"] == "synthetic" and r["run_id"].startswith("delay-") and r["job_id"] == "j" * 32
    assert r["world"] == {"keys": 36, "rpm_per_key": 40.0, "partitions": 8, "history_days": 21}
    assert r["incidents"] == 3 and r["design"] == bench_delay.DESIGN
    assert r["schedule"] == {
        "settle_s": 300, "spacing_s": 180, "duration_s": 360, "magnitude": 0.2, "tail_s": 180,
    }  # fmt: skip
    assert r["headline"]["detector"] == "seasonal" and r["headline"]["setting"] == "k4"
    assert r["headline"]["matched"] == 3
    assert r["headline"]["time_to_page_s"]["n"] == 3
    assert list(r["by_detector"]) == ["seasonal/k4"]
    assert r["by_detector"]["seasonal/k4"]["matched"] == 3
    assert r["settings"] >= 2
    assert r["watermark_lag_s"]["samples"] >= 5 and r["watermark_lag_s"]["median"] == 22.0
    assert r["lag_check"]["at_first_incident_s"] == 22.0
    assert r["clock_offset"] == {
        "before": {"offset_s": -0.7, "bracket_s": 0.1},
        "after": {"offset_s": -0.7, "bracket_s": 0.1},
    }
    assert r["generator"]["failed_deliveries"] == 0
    assert r["generator"]["caught_up_ms"] > 0
    assert r["late_events"] == 0 and r["restarts"] == 0 and r["host_sleep_gaps_s"] == []
    assert r["power_end"] == AC and r["load_start"] and r["load_end"]
    assert (d / "samples-lag.json").is_file() and (d / "labels.json").is_file()
    assert h.cancelled == [("j" * 32, r["run_id"])]
    names = [c[1] for c in h.cmds if c[0] == "docker" and len(c) > 1]
    assert names.index("stop") < names.index("logs") < names.index("rm")


def test_the_rows_of_other_runs_are_not_measured(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, matched=2)
    r, _ = run_main(h, tmp_path)
    assert r["headline"]["matched"] == 2 and r["headline"]["incidents"] == 3
    assert r["valid"] is False
    assert "fewer than 90% of the incidents were matched" in r["invalid_reasons"]


def test_a_clock_that_moved_makes_the_run_invalid(monkeypatch, tmp_path):
    offs = [{"offset_s": -0.7, "bracket_s": 0.1}, {"offset_s": 0.6, "bracket_s": 0.1}]
    h = build_harness(monkeypatch, tmp_path, offsets=offs)
    r, _ = run_main(h, tmp_path)
    assert r["valid"] is False
    assert "the VM clock moved against the host's by more than 1 s" in r["invalid_reasons"]


def test_failed_deliveries_make_the_run_invalid(monkeypatch, tmp_path):
    log = "x delivery failed: boom\nok\ny delivery failed: boom\n"
    h = build_harness(monkeypatch, tmp_path, logs=log)
    r, _ = run_main(h, tmp_path)
    assert r["generator"]["failed_deliveries"] == 2
    assert "the generator reported failed deliveries" in r["invalid_reasons"]


def test_a_run_that_ends_on_battery_is_invalid(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, power_end=BATTERY)
    r, _ = run_main(h, tmp_path)
    assert "the laptop went to battery during the run" in r["invalid_reasons"]


def test_a_run_on_battery_is_valid_when_allowed(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, power_end=BATTERY)
    r, _ = run_main(h, tmp_path, "--on-battery")
    assert r["valid"] is True


def test_a_restart_makes_the_run_invalid(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, restarts=1)
    r, _ = run_main(h, tmp_path)
    assert r["valid"] is False and any("restarted" in x for x in r["invalid_reasons"])


def test_a_generator_that_exits_fails_the_run_after_cleaning_up(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, gen_exits_at=10)
    with pytest.raises(RunFailed, match="generator"):
        bdr.main(["--incidents", "3", "--pilot"])
    assert h.cancelled and h.cancelled[0][0] == "j" * 32
    docker = [c for c in h.cmds if c[:2] in (["docker", "rm"], ["docker", "stop"])]
    assert any(c[:2] == ["docker", "rm"] for c in docker)
    d = next((tmp_path / "bench").iterdir())
    assert (d / "samples-lag.json").is_file()  # the failure path keeps its samples


def test_a_job_that_never_catches_up_fails_after_cleaning_up(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path, lag=lambda t: 900.0)
    with pytest.raises(RunFailed, match="did not catch up"):
        bdr.main(["--incidents", "3", "--pilot"])
    assert h.cancelled
    assert any(c[:2] == ["docker", "rm"] for c in h.cmds)


def test_a_failure_before_the_job_cancels_only_the_generator(monkeypatch, tmp_path):
    h = build_harness(monkeypatch, tmp_path)

    def boom(run_id, topic):
        raise RunFailed("submit failed")

    monkeypatch.setattr(bdr.bench_flink, "submit", boom)
    with pytest.raises(RunFailed, match="submit failed"):
        bdr.main(["--incidents", "3", "--pilot"])
    assert h.cancelled == []
    assert any(c[:2] == ["docker", "rm"] for c in h.cmds)

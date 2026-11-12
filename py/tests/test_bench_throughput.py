import json
from types import SimpleNamespace

import pytest

from tenant_baselines import bench_env
from tenant_baselines import bench_throughput as bt


def ramp(total_s=264, step=12, first=600_000, per=1_200_000):
    """(0, 0), (12, 0), (24, first), then +per every step."""
    out = [{"t": 0, "count": 0}, {"t": 12, "count": 0}]
    out += [
        {"t": 24 + i * step, "count": first + i * per} for i in range((total_s - 24) // step + 1)
    ]
    return out


def test_sustained_rate_excludes_warmup_and_the_last_tenth():
    r = bt.sustained_rate(ramp(), 25_000_000)
    assert r == {
        "events_per_s": 100_000.0,
        "window_start_s": 84,
        "window_end_s": 240,
        "window_s": 156,
        "intervals": 13,
        "min_interval_rate": 100_000.0,
        "max_interval_rate": 100_000.0,
        "timing_error_pct": 1.9,  # 100 x 3 s / 156 s
    }


def test_window_too_short_raises():
    with pytest.raises(bt.WindowTooShort, match="BENCH_MINUTES"):
        bt.sustained_rate(ramp(), 10_000_000)


def test_counter_going_backwards_raises():
    s = [{"t": 0, "count": 2_000_000}, {"t": 12, "count": 100}]
    with pytest.raises(ValueError, match="restarted"):
        bt.window(s, 10_000_000)


def test_final_count_must_equal_the_fill():
    for bad in (999, 1001):
        with pytest.raises(ValueError, match="not the task-level record count"):
            bt.check_final_count(bad, 1000)
    bt.check_final_count(1000, 1000)


def test_window_shares():
    def at(t, busy):
        return {
            "t": t,
            "count": 0,
            "vertices": {
                "scorer": {
                    "busy_ms": busy,
                    "backpressured_ms": 0,
                    "idle_ms": (t - 100) * 1000,
                    "parallelism": 2,
                }
            },
        }

    s = [at(100, 10_000), at(220, 130_000)]
    sh = bt.window_shares(s, 0, 1)
    assert sh["scorer"]["busy"] == 0.5
    assert sh["scorer"]["backpressured"] == 0.0
    assert sh["scorer"]["idle"] == 0.5


def test_alert_counts():
    rows = [
        {"detector": "global_static", "setting": "p99.9", "scope": "tenant", "state": "firing"},
        {"detector": "global_static", "setting": "p99.9", "scope": "tenant", "state": "firing"},
        {"detector": "global_static", "setting": "p99.9", "scope": "tenant", "state": "resolved"},
        {"detector": "seasonal", "setting": "k4", "scope": "region", "state": "firing"},
    ]
    assert bt.alert_counts(rows) == {"global_static/p99.9/tenant": 2, "seasonal/k4/region": 1}


def test_free_space_and_size_parsing():
    df = "Filesystem 1024-blocks Used Available Capacity Mounted\n/dev/vda1 100 40 60 40% /data\n"
    assert bt.parse_df_available_kib(df) == 60
    assert bt.parse_du_kib("1234\t/var/lib/kafka/data/t-0\n66\t/var/lib/kafka/data/t-1\n") == 1300


def fresh_ramp(n=40, per=1_000_000):
    return [{"t": 12 * k, "count": k * per, "stale": False} for k in range(n)]


def test_the_sample_at_the_end_share_is_not_in_the_window():
    s = fresh_ramp()
    total = 30_000_000
    assert s[27]["count"] == bt.END_SHARE * total  # the boundary: 27 M of 30 M
    first, last = bt.window(s, total)
    assert s[last]["count"] == 26_000_000 and first == 6  # 60 s after the first count


def test_stale_samples_are_never_endpoints_and_do_not_change_the_rate():
    clean = fresh_ramp()
    rate = bt.sustained_rate(clean, 30_000_000)
    s = fresh_ramp()
    for k in (6, 12, 26):  # the start, a middle one, the end: each a repeat of its neighbour
        s[k] = {**s[k - 1], "t": s[k]["t"], "stale": True}
    first, last = bt.window(s, 30_000_000)
    assert not s[first]["stale"] and not s[last]["stale"]
    assert (first, last) == (8, 25)  # 7 follows the stale 6, so it is not an endpoint either
    got = bt.sustained_rate(s, 30_000_000)
    assert got["events_per_s"] == rate["events_per_s"] == pytest.approx(83_333.3, abs=0.1)
    assert got["min_interval_rate"] > 0
    assert got["intervals"] == 16  # samples 8 to 25 are 18, one of them stale: 17 fresh


def test_a_sample_right_after_a_stale_one_is_never_an_endpoint():
    s = fresh_ramp()
    s[25] = {**s[24], "t": s[25]["t"], "stale": True}
    first, last = bt.window(s, 30_000_000)
    assert (first, last) == (6, 24)  # 26 follows the stale 25; the last endpoint is 24, not 26


def test_next_due_catches_up_without_a_burst():
    assert bt.next_due(100.0, burst_start=95.0) == 112.0  # on schedule
    assert bt.next_due(100.0, burst_start=130.0) == 141.0  # late: 11 s after the last burst


def test_timing_error_is_the_slower_endpoint_read_over_the_window():
    s = fresh_ramp()
    s[6]["read_s"], s[26]["read_s"] = 2.0, 5.5  # the window is samples 6 to 26: 240 s
    s[10]["read_s"] = 9.0  # inside the window: not an endpoint
    assert bt.sustained_rate(s, 30_000_000)["timing_error_pct"] == 2.3  # 100 x 5.5 / 240


def sampled(k, count, stale=False, read_s=1.0, par=2, **extra):
    """A sample 12 k seconds in, with the source's clock running at wall time."""
    v = {"busy_ms": 1000 * k, "backpressured_ms": 0, "idle_ms": 0, "parallelism": par}
    return {
        "t": 1000.0 + 12 * k, "mono": 5.0 + 12 * k, "count": count, "stale": stale,
        "clock_ms": 12_000.0 * k * par, "read_s": read_s, "reread": False, "slept": 0.0,
        "vertices": {"Source: events": v, "drop-ticks": dict(v)}, **extra,
    }  # fmt: skip


def run_samples(n=30, per=1_000_000):
    return [sampled(k, k * per) for k in range(n)]


def test_summarise_reports_the_source_clock_rate_beside_the_host_rate():
    s = run_samples()
    out = bt._summarise("r1", "j1", 30_000_000, s, pilot=False)
    assert out["stale_samples"] == 0
    assert out["events_per_s"] == pytest.approx(83_333.3, abs=0.1)
    assert out["events_per_s_source_clock"] == pytest.approx(83_333.3, abs=0.1)
    assert out["timing_error_pct"] == round(100 * 1.0 / out["window_s"], 1)
    assert out["source"]["busy"] == pytest.approx(1000 / 24_000, abs=0.001)
    assert out["drop_ticks_busy"] == out["source"]["busy"]


def test_summarise_reports_a_host_sleep_apart_from_a_stall():
    s = run_samples()
    for k, x in enumerate(s):
        x["slept"] = 0.0 if k < 10 else 300.0
    out = bt._summarise("r1", "j1", 30_000_000, s, pilot=False)
    assert out["host_sleep_s"] == 300.0 and out["stall_gaps_s"] == []
    assert "host_sleep_gaps_s" not in out
    s[20]["mono"] += 61  # a 61 s stall, and every later sample moves with it
    for x in s[21:]:
        x["mono"] += 61
    out = bt._summarise("r1", "j1", 30_000_000, s, pilot=False)
    assert out["stall_gaps_s"] == [73.0]


def test_summarise_pilot_fallback_walks_both_ends_to_fresh_samples():
    s = [sampled(k, k * 1000, stale=k in (0, 1, 9)) for k in range(10)]
    out = bt._summarise("r1", "j1", 1_000_000_000, s, pilot=True)
    assert "window_error" in out and out["stale_samples"] == 3
    # shares are over samples 2 to 8, the first and last fresh ones: 1 s busy per 12 s, 2 subtasks
    assert out["source"]["busy"] == pytest.approx(1000 / 24_000, abs=0.001)
    assert out["events_per_s_source_clock"] == pytest.approx(1000 / 12, abs=0.1)
    with pytest.raises(bt.WindowTooShort):
        bt._summarise("r1", "j1", 1_000_000_000, s, pilot=False)


def test_summarise_pilot_with_one_fresh_sample_or_none_does_not_divide_by_zero():
    one = [sampled(k, k * 1000, stale=k != 3) for k in range(6)]
    out = bt._summarise("r1", "j1", 1_000_000_000, one, pilot=True)
    assert "window_error" in out and out["shares"] == {} and out["source"] is None
    assert "events_per_s_source_clock" not in out
    none = [sampled(k, k * 1000, stale=True) for k in range(6)]
    out = bt._summarise("r1", "j1", 1_000_000_000, none, pilot=True)
    assert "window_error" in out and out["busiest_vertex"] is None
    with pytest.raises(bt.WindowTooShort):
        bt._summarise("r1", "j1", 1_000_000_000, none, pilot=False)


AC = "Now drawing from 'AC Power'"
BATTERY = "Now drawing from 'Battery Power'"


def done_run(run_id="tp-x-r1", **edit):
    """A finished run's record, as `_one_run` returns it and `main` adds `power_end` to."""
    r = {
        "run_id": run_id,
        "restarts": 0,
        "late_events": 0,
        "stall_gaps_s": [],
        "host_sleep_s": 0.0,
        "events_per_s": 100_000.0,
        "power_end": AC,
        "source": {"idle": 0.001},
        "busiest_vertex": "scorer",
        "stale_samples": 0,
    }
    return r | edit


def test_build_result_of_clean_runs_is_valid_with_the_spread():
    runs = [done_run(), done_run("tp-x-r2", events_per_s=110_000.0)]
    r = bt.build_result({"data": "synthetic"}, {"events": 1000}, runs, on_battery=False)
    assert r["valid"] is True and r["invalid_reasons"] == []
    assert r["data"] == "synthetic" and r["env"] == {"data": "synthetic"}
    assert r["fill"] == {"events": 1000} and r["runs"] == runs
    assert r["spread_pct"] == 9.5  # 10,000 of the mean 105,000
    assert (r["warmup_s"], r["end_share"], r["sample_interval_s"]) == (60, 0.9, 12)
    assert r["settings"] == bt._settings()


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        ({"host_sleep_s": 3.0}, "the host slept 3 s during the run"),
        ({"stall_gaps_s": [75.0]}, "the driver stalled for 75 s between two polls"),
        ({"restarts": 1}, "the job restarted 1 time(s)"),
        ({"late_events": 3}, "3 event(s) were dropped as late"),
        ({"aborted": "the source count fell", "late_events": None},
         "run tp-x-r2: the source count fell"),
        ({"power_end": BATTERY}, "the laptop went to battery during the run tp-x-r2"),
        ({"window_error": "the window is 60 s"}, "run tp-x-r2: the window is 60 s"),
    ],
)  # fmt: skip
def test_build_result_names_each_reason_a_run_is_invalid(edit, reason):
    runs = [done_run(), done_run("tp-x-r2", **edit)]
    r = bt.build_result({}, {"events": 1000}, runs, on_battery=False)
    assert r["valid"] is False and r["invalid_reasons"] == [reason]


def test_build_result_sums_the_sleep_over_the_runs_and_allows_one_second():
    runs = [done_run(host_sleep_s=0.5), done_run("tp-x-r2", host_sleep_s=0.5)]
    assert bt.build_result({}, {}, runs, on_battery=False)["valid"] is True
    runs[1]["host_sleep_s"] = 0.6
    assert bt.build_result({}, {}, runs, on_battery=False)["invalid_reasons"] == [
        "the host slept 1 s during the run"
    ]


def test_build_result_allows_battery_with_on_battery():
    runs = [done_run(power_end=BATTERY)]
    assert bt.build_result({}, {}, runs, on_battery=True)["valid"] is True


def test_main_writes_the_verdict_of_its_runs(monkeypatch, tmp_path):
    written = {}
    monkeypatch.setattr(bt.bench_env, "preflight", lambda on_battery: None)
    monkeypatch.setattr(bt.bench_env, "collect", lambda on_battery: {"data": "synthetic"})
    monkeypatch.setattr(bt, "DATA_BENCH", tmp_path)
    monkeypatch.setattr(bt, "_fill", lambda args, fill_id, run: {"events": 1000, "seconds": 1.0})
    runs = iter([done_run("tp-x-r1"), done_run("tp-x-r2", stall_gaps_s=[75.0])])
    monkeypatch.setattr(bt, "_one_run", lambda n, fill_id, total, args, run_dir: next(runs))
    power = iter([AC, BATTERY])
    monkeypatch.setattr(bt.bench_env, "_power_line", lambda run: next(power))

    def write(name, result, run_dir, pilot):
        written.update(name=name, result=result)
        return run_dir / "result.json"

    monkeypatch.setattr(bt.bench_env, "write_result", write)
    bt.main(["--minutes", "10"])
    r = written["result"]
    assert written["name"] == "throughput" and r["valid"] is False
    assert r["invalid_reasons"] == [
        "the driver stalled for 75 s between two polls",
        "the laptop went to battery during the run tp-x-r2",
    ]
    assert [x["power_end"] for x in r["runs"]] == [AC, BATTERY]


def scripted(items, previous_seen):
    it = iter(items)

    def fn(job_id, sid, par, previous=None):
        previous_seen.append(previous)
        item = next(it)
        if isinstance(item, Exception):
            raise item
        return item

    return fn


def poll(items, total=3_000_000, samples=None, deadline=1e9):
    seen: list = []
    samples = [] if samples is None else samples
    ticks = iter(range(10_000))
    kind = bt._poll_source(
        "j", {"id": "s", "parallelism": 2}, samples, total, deadline,
        sample_fn=scripted(items, seen), monotonic=lambda: float(next(ticks)),
        sleep=lambda s: None,
    )  # fmt: skip
    return kind, samples, seen


def test_poll_stops_at_the_total_and_compares_with_the_last_fresh_sample():
    a, b, c = sampled(0, 1_000_000), sampled(1, 1_000_000, stale=True), sampled(2, 3_000_000)
    kind, samples, seen = poll([a, b, c])
    assert kind == ("", "") and samples == [a, b, c]
    assert seen == [None, a, a]  # the stale sample b is not the baseline


def after_restart(k):
    """What the real `sample` gives once the job has restarted: the count and the accumulated
    times start again, so the clock is below the last fresh sample's and the sample is stale."""
    return {**sampled(k, 100 * k), "stale": True, "clock_ms": 500.0 * k}


def test_poll_detects_a_restart_whose_samples_are_all_stale():
    a, b, c = sampled(5, 2_000_000), after_restart(1), after_restart(2)
    kind, samples, _ = poll([a, b, c])
    assert kind[0] == "restarted" and samples == [a, b, c]


def test_one_regressed_sample_is_not_yet_a_restart():
    a, b, c = sampled(5, 2_000_000), after_restart(1), sampled(6, 3_000_000)
    assert poll([a, b, c])[0] == ("", "")


def test_a_clock_reset_alone_counts_as_a_regression():
    a = sampled(5, 2_000_000)
    b = {**sampled(6, 2_100_000), "clock_ms": 10.0, "stale": True}
    c = {**sampled(7, 2_200_000), "clock_ms": 20.0, "stale": True}
    assert poll([a, b, c])[0][0] == "restarted"


def test_poll_keeps_the_samples_when_the_metrics_vanish():
    a = sampled(0, 1_000_000)
    kind, samples, _ = poll([a, KeyError("no value for 0.numRecordsOut")])
    assert kind[0] == "vanished" and "0.numRecordsOut" in kind[1] and samples == [a]


def test_poll_waits_through_a_start_up_without_metrics():
    a = sampled(0, 3_000_000)
    kind, samples, _ = poll([KeyError("not yet"), a])
    assert kind == ("", "") and samples == [a]


def test_poll_gives_up_at_the_deadline():
    a = sampled(0, 1_000_000)
    kind, _, _ = poll([a, a, a], deadline=0.5)
    assert kind[0] == "deadline"


LOAD = {
    "host_load_1m": 1.5,
    "project_containers_cpu_pct": 10.0,
    "other_containers_cpu_pct": 20.0,
    "vm_cpus": 8,
    "cpu_pct_unit": "percent of one CPU",
}


def one_run(monkeypatch, tmp_path, poll_result, restarts=0, script=None, load=None):
    calls = []
    monkeypatch.setattr(bt.bench_env, "load_record", load or (lambda: LOAD))
    monkeypatch.setattr(bt.kafka_io, "end_offsets", lambda topic: {0: 0})
    monkeypatch.setattr(bt.bench_flink, "submit", lambda run_id, topic: "j" * 32)
    monkeypatch.setattr(bt, "_wait_running", lambda job_id: None)
    monkeypatch.setattr(
        bt.bench_flink, "_json",
        lambda get, path: {"vertices": [{"id": "s", "name": "Source: events", "parallelism": 2}]},
    )  # fmt: skip
    monkeypatch.setattr(bt.bench_flink, "restarts", lambda job_id: restarts)
    monkeypatch.setattr(bt.bench_flink, "cancel_and_clean", lambda j, r: calls.append((j, r)))

    def fake_poll(job_id, src, samples, total, deadline, **kw):
        samples += [sampled(0, 1_000_000), sampled(1, 100)]
        return poll_result

    if script is None:
        monkeypatch.setattr(bt, "_poll_source", fake_poll)
    else:  # the real loop, fed what the real `sample` would give
        it = iter(script)
        monkeypatch.setattr(bt.bench_flink, "sample", lambda *a, **k: next(it))
        monkeypatch.setattr(bt.time, "sleep", lambda s: None)
    args = SimpleNamespace(pilot=False)
    return bt._one_run(1, "tp-x", 3_000_000, args, tmp_path), calls


def test_a_restarted_run_is_recorded_cancelled_and_keeps_its_samples(monkeypatch, tmp_path):
    r, calls = one_run(monkeypatch, tmp_path, ("restarted", "the source count fell"))
    assert r["restarts"] == 1 and r["late_events"] is None and "fell" in r["aborted"]
    assert r["run_id"] == "tp-x-r1" and r["stale_samples"] == 0
    assert calls == [("j" * 32, "tp-x-r1")]
    assert len(json.loads((tmp_path / "samples-r1.json").read_text())) == 2


def test_a_real_restart_ends_the_run_as_restarted_with_its_samples(monkeypatch, tmp_path):
    script = [sampled(5, 2_000_000), after_restart(1), after_restart(2)]
    r, calls = one_run(monkeypatch, tmp_path, None, script=script)
    assert "restarted" in r["aborted"] and r["restarts"] == 1 and r["late_events"] is None
    assert calls and len(json.loads((tmp_path / "samples-r1.json").read_text())) == 3
    ok, reasons = bench_env.validity(r["restarts"], 0, [], [f"run {r['run_id']}: {r['aborted']}"])
    assert not ok and any("restarted" in x for x in reasons)


def test_a_failed_load_record_does_not_kill_the_run(monkeypatch, tmp_path):
    def broken():
        raise RuntimeError("`sysctl -n vm.loadavg` exited 1: boom")

    r, _ = one_run(monkeypatch, tmp_path, ("restarted", "fell"), load=broken)
    assert r["load_start"]["host_load_1m"] is None and "boom" in r["load_start"]["error"]
    assert "boom" in r["load_end"]["error"] and r["restarts"] == 1


def test_a_stale_late_events_read_after_a_restart_ends_the_run_as_restarted(monkeypatch, tmp_path):
    def stale(*a, **k):
        raise bt.bench_flink.RunFailed("the late-events read is stale")

    monkeypatch.setattr(bt.bench_flink, "late_events", stale)
    final = [sampled(5, 3_000_000)]
    r, calls = one_run(monkeypatch, tmp_path, None, restarts=2, script=final)
    assert r["restarts"] == 2 and "restarted" in r["aborted"] and r["late_events"] is None
    assert calls and (tmp_path / "samples-r1.json").exists()


def test_a_stale_late_events_read_without_a_restart_still_fails(monkeypatch, tmp_path):
    def stale(*a, **k):
        raise bt.bench_flink.RunFailed("the late-events read is stale")

    monkeypatch.setattr(bt.bench_flink, "late_events", stale)
    with pytest.raises(bt.RunFailed, match="stale"):
        one_run(monkeypatch, tmp_path, None, restarts=0, script=[sampled(5, 3_000_000)])


def test_a_run_whose_metrics_vanish_is_invalid_with_the_reason(monkeypatch, tmp_path):
    r, calls = one_run(monkeypatch, tmp_path, ("vanished", "no value for 0.numRecordsOut"))
    assert "no value for 0.numRecordsOut" in r["aborted"] and r["restarts"] == 0
    assert calls and (tmp_path / "samples-r1.json").exists()


def test_a_missed_deadline_raises_after_cleaning(monkeypatch, tmp_path):
    with pytest.raises(bt.RunFailed, match="did not reach"):
        one_run(monkeypatch, tmp_path, ("deadline", "the count did not reach the fill"))
    assert (tmp_path / "samples-r1.json").exists()


def test_power_at_the_end_of_a_run_without_on_battery():
    ac, battery = "Now drawing from 'AC Power'", "Now drawing from 'Battery Power'"
    runs = [{"run_id": "a", "power_end": ac}, {"run_id": "b", "power_end": battery}]
    assert bt.power_reasons(runs, on_battery=False) == [
        "the laptop went to battery during the run b"
    ]
    assert bt.power_reasons(runs, on_battery=True) == []
    assert bt.power_reasons([{"run_id": "a", "power_end": None}], on_battery=False) == []


def test_a_sample_without_a_sleep_reading_is_refused():
    s = [sampled(k, k * 1_000_000) for k in range(3)]
    del s[1]["slept"]
    with pytest.raises(ValueError, match=r"samples \[1\] carry no sleep-counter reading"):
        bt._host_sleep(s)


def complete_run(monkeypatch, tmp_path, final_count, rows=()):
    """`_one_run` to its end on fakes: a 30 M fill read in 12 s samples of 1 M events."""
    script = [sampled(k, k * 1_000_000) for k in range(30)] + [sampled(30, final_count)]
    it = iter(script)
    monkeypatch.setattr(bt.bench_env, "load_record", lambda: LOAD)
    monkeypatch.setattr(bt.kafka_io, "end_offsets", lambda topic: {0: 0})
    monkeypatch.setattr(bt.kafka_io, "read_range", lambda topic, a, b: list(rows))
    monkeypatch.setattr(bt.bench_flink, "submit", lambda run_id, topic: "j" * 32)
    monkeypatch.setattr(bt, "_wait_running", lambda job_id: None)
    monkeypatch.setattr(
        bt.bench_flink, "_json",
        lambda get, path: {"vertices": [{"id": "s", "name": "Source: events", "parallelism": 2}]},
    )  # fmt: skip
    monkeypatch.setattr(bt.bench_flink, "sample", lambda *a, **k: next(it))
    monkeypatch.setattr(bt.time, "sleep", lambda s: None)
    monkeypatch.setattr(bt.bench_flink, "late_events", lambda job_id, fresh_after=None: 0)
    monkeypatch.setattr(bt.bench_flink, "restarts", lambda job_id: 0)
    ck = {"id": 4, "trigger_timestamp": 1, "state_size": 10, "checkpointed_size": 5}
    monkeypatch.setattr(bt.bench_flink, "checkpoint_history", lambda job_id: [ck])
    monkeypatch.setattr(bt.bench_flink, "vertex_state", lambda job_id, cid: [])
    monkeypatch.setattr(bt.bench_flink, "taskmanager_memory", lambda: {"used_bytes": 1})
    cleaned = []
    monkeypatch.setattr(bt.bench_flink, "cancel_and_clean", lambda j, r: cleaned.append(r))
    run = bt._one_run(1, "tp-x", 30_000_000, SimpleNamespace(pilot=False), tmp_path)
    return run, cleaned


def alert(run_id, setting="k4"):
    return {"detector": "seasonal", "setting": setting, "scope": "tenant", "state": "firing",
            "run_id": run_id}  # fmt: skip


def test_a_complete_run_counts_only_its_own_alert_rows(monkeypatch, tmp_path):
    rows = [alert("tp-x-r1"), alert("tp-x-r1"), alert("tp-x-r2"), alert("tp-y-r1", "p99.9")]
    run, cleaned = complete_run(monkeypatch, tmp_path, 30_000_000, rows)
    assert run["alert_rows"] == {"seasonal/k4/tenant": 2}
    assert run["events_per_s"] == pytest.approx(83_333.3, abs=0.1) and cleaned == ["tp-x-r1"]
    assert run["checkpoint"]["id"] == 4 and run["late_events"] == 0


def test_a_run_whose_count_ends_off_the_fill_fails_and_cleans(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="not the task-level record count"):
        complete_run(monkeypatch, tmp_path, 30_000_001)
    assert (tmp_path / "samples-r1.json").exists()


def broker(cmd):
    """`df -Pk` and `du -sk` in the broker: plenty of space, 10 KiB of topic."""
    df = "Filesystem 1024-blocks Used Available Capacity Mounted\n/dev/vda1 9 1 999999999 1% /d\n"
    return SimpleNamespace(returncode=0, stdout=df if "df" in cmd else "10\t/x\n", stderr="")


def test_the_fill_is_refused_unless_the_topic_holds_every_event(monkeypatch):
    monkeypatch.setattr(bt.kafka_io, "create_topic", lambda name, parts: None)
    monkeypatch.setattr(bt.kafka_io, "produce_all", lambda topic, records: 1000)
    monkeypatch.setattr(bt, "_topic_compression", lambda topic: "zstd")
    args = SimpleNamespace(seed=1, minutes=1, rpm=120.0)
    monkeypatch.setattr(bt.kafka_io, "end_offsets", lambda topic: {0: 500, 1: 499})
    with pytest.raises(bt.RunFailed, match="holds 999 records, 1000 were produced"):
        bt._fill(args, "tp-x", broker)
    monkeypatch.setattr(bt.kafka_io, "end_offsets", lambda topic: {0: 500, 1: 500})
    fill = bt._fill(args, "tp-x", broker)
    assert fill["events"] == 1000 and fill["topic"] == "tp-x.events" and fill["keys"] == 2000

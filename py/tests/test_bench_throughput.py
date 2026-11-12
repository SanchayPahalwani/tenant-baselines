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
    assert (first, last) == (7, 25)
    got = bt.sustained_rate(s, 30_000_000)
    assert got["events_per_s"] == rate["events_per_s"] == pytest.approx(83_333.3, abs=0.1)
    assert got["min_interval_rate"] > 0
    assert got["intervals"] == 18 - 1 - 0  # 7..25 are 19 samples, one stale in the middle


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
        "clock_ms": 12_000.0 * k * par, "read_s": read_s, "reread": False,
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


def test_summarise_pilot_fallback_walks_both_ends_to_fresh_samples():
    s = [sampled(k, k * 1000, stale=k in (0, 1, 9)) for k in range(10)]
    out = bt._summarise("r1", "j1", 1_000_000_000, s, pilot=True)
    assert "window_error" in out and out["stale_samples"] == 3
    # shares are over samples 2 to 8, the first and last fresh ones: 1 s busy per 12 s, 2 subtasks
    assert out["source"]["busy"] == pytest.approx(1000 / 24_000, abs=0.001)
    assert out["events_per_s_source_clock"] == pytest.approx(1000 / 12, abs=0.1)
    with pytest.raises(bt.WindowTooShort):
        bt._summarise("r1", "j1", 1_000_000_000, s, pilot=False)


def test_validity_skips_a_late_events_that_was_not_read():
    assert bench_env.validity(0, None, [], []) == (True, [])


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


def test_poll_stops_when_the_count_falls():
    a, b = sampled(0, 2_000_000), sampled(1, 100)
    kind, samples, _ = poll([a, b])
    assert kind[0] == "restarted" and samples == [a, b]


def test_a_stale_sample_with_a_lower_count_is_not_a_restart():
    a, b, c = sampled(0, 2_000_000), sampled(1, 1_999_000, stale=True), sampled(2, 3_000_000)
    assert poll([a, b, c])[0] == ("", "")


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


def one_run(monkeypatch, tmp_path, poll_result, restarts=0):
    calls = []
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

    monkeypatch.setattr(bt, "_poll_source", fake_poll)
    args = SimpleNamespace(pilot=False)
    return bt._one_run(1, "tp-x", 3_000_000, args, tmp_path), calls


def test_a_restarted_run_is_recorded_cancelled_and_keeps_its_samples(monkeypatch, tmp_path):
    r, calls = one_run(monkeypatch, tmp_path, ("restarted", "the source count fell"))
    assert r["restarts"] == 1 and r["late_events"] is None and "fell" in r["aborted"]
    assert r["run_id"] == "tp-x-r1" and r["stale_samples"] == 0
    assert calls == [("j" * 32, "tp-x-r1")]
    assert len(json.loads((tmp_path / "samples-r1.json").read_text())) == 2


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

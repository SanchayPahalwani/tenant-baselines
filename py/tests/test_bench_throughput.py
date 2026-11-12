import pytest

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

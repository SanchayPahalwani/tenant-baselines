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
        "timing_error_pct": 1.3,
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
                    "idle_ms": 0 + (t - 100) * 1000 * 2 // 2,
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
    assert bt.parse_du_kib("1234\t/var/lib/kafka/data\n") == 1234

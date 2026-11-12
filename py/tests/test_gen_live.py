import sys

import orjson
import pytest

from tenant_baselines import bench_world
from tenant_baselines.gen_live import (
    DeliveryStats,
    LiveGenerator,
    check_failed,
    main,
    next_step,
    start_bench,
    take_step,
)
from tenant_baselines.incidents import Incident
from tenant_baselines.traffic import shape
from tenant_baselines.world import build_world

T = 1_767_182_400_000  # a fixed second


def test_same_seed_same_output():
    w = build_world("demo", 1)
    a, b = LiveGenerator(w, 5, []), LiveGenerator(w, 5, [])
    for i in range(5):
        assert a.tick(T + i * 1000) == b.tick(T + i * 1000)


def test_sorted_and_inside_second():
    gen = LiveGenerator(build_world("demo", 1), 5, [])
    for i in range(20):
        sec = T + i * 1000
        ts = [orjson.loads(p)["ts"] for _, p in gen.tick(sec)]
        assert ts == sorted(ts)
        assert all(sec <= x < sec + 1000 for x in ts)


def test_count_matches_rate():
    w = build_world("demo", 1)
    gen = LiveGenerator(w, 5, [])
    total = sum(len(gen.tick(T)) for _ in range(600))
    expected = sum(gen.key_rate_rpm(k, T) for k in w.keys) * 10
    assert total == pytest.approx(expected, rel=0.10)


def test_error_spike_raises_error_rate():
    w = build_world("demo", 1)
    inc = Incident("error_spike", T, T + 1000, 0.5, region="eu-central")
    gen = LiveGenerator(w, 5, [inc])
    msgs = [orjson.loads(p) for _ in range(300) for _, p in gen.tick(T)]
    eu = [m for m in msgs if m["rg"] == "eu-central"]
    other = [m for m in msgs if m["rg"] != "eu-central"]
    assert sum(not m["ok"] for m in eu) / len(eu) > 0.4
    assert sum(not m["ok"] for m in other) / len(other) < 0.1


def test_delivery_stats_counts_failures(caplog):
    stats = DeliveryStats()
    stats.on_delivery(None, object())
    with caplog.at_level("WARNING", logger="gen_live"):
        stats.on_delivery("broker down", object())
    assert (stats.delivered, stats.failed) == (1, 1)
    assert "broker down" in caplog.text


def test_minute_same_seed_same_output():
    w = build_world("demo", 1)
    a, b = LiveGenerator(w, 5, []), LiveGenerator(w, 5, [])
    for i in range(3):
        assert a.minute(T + i * 60_000) == b.minute(T + i * 60_000)


def test_minute_sorted_and_inside_minute():
    gen = LiveGenerator(build_world("demo", 1), 5, [])
    for i in range(5):
        m = T + i * 60_000
        ts = [orjson.loads(p)["ts"] for _, p in gen.minute(m)]
        assert ts == sorted(ts)
        assert all(m <= x < m + 60_000 for x in ts)


def test_minute_count_matches_rate():
    w = build_world("demo", 1)
    gen = LiveGenerator(w, 5, [])
    start = 1_767_182_400_000 + 3_600_000 * 2  # Wednesday 14:00 UTC
    total = sum(len(gen.minute(start + i * 60_000)) for i in range(120))
    expected = sum(gen.key_rate_rpm(k, start + i * 60_000) for k in w.keys for i in range(120))
    assert total == pytest.approx(expected, rel=0.05)


def test_minute_applies_volume_mult():
    w = build_world("demo", 1)
    start = 1_767_182_400_000 + 3_600_000 * 2
    k = max(w.keys, key=lambda x: LiveGenerator(w, 5, []).key_rate_rpm(x, start))
    inc = Incident("volume_mult", start, start + 60 * 60_000, 0.1, key=k.key)
    gen = LiveGenerator(w, 5, [])
    gen.set_incidents([inc])
    counts: dict[str, int] = {}
    for i in range(60):
        for key, _ in gen.minute(start + i * 60_000):
            counts[key] = counts.get(key, 0) + 1
    expected = {
        x.key: sum(gen.key_rate_rpm(x, start + i * 60_000) for i in range(60)) for x in w.keys
    }
    assert counts[k.key] == pytest.approx(expected[k.key] * 0.1, rel=0.30)
    for x in w.keys:
        if x != k and expected[x.key] >= 200:
            assert counts.get(x.key, 0) == pytest.approx(expected[x.key], rel=0.10)


def test_partition_of_covers_all_partitions():
    w = build_world("demo", 1)
    gen = LiveGenerator(w, 5, [])
    assert {gen.partition_of(k.key, 8) for k in w.keys} == set(range(8))
    k = w.keys[3]
    assert gen.partition_of(k.key, 8) == gen.partition_of(k.key, 8) == 3 % 8


def test_next_step():
    assert next_step(120_000, 180_000) == "minute"
    assert next_step(120_000, 179_999) == "second"
    assert next_step(121_000, 400_000) == "second"
    assert next_step(120_000, 120_999) == "wait"


def test_tick_uses_the_key_rate():
    """Two keys share a tenant's rate, so each key gets about half of it."""
    world = build_world("demo", 1)
    keys = [k for k in world.keys if k.tenant == "t02"]
    assert len(keys) == 2
    t02 = next(t for t in world.tenants if t.name == "t02")
    gen = LiveGenerator(world, 3, [])
    second = 1_767_182_400_000 + 3_600_000 * 2  # Wednesday 14:00 UTC, 15:00 in t02's zone
    # From the tenant, not from the generator: 15:00 on a weekday is at the weekday peak.
    assert shape(15.0, 2) == 1.0
    expected = t02.peak_rpm * shape(15.0, 2) / 2 / 60.0
    assert gen.key_rate_rpm(keys[0], second) / 60.0 == pytest.approx(expected)
    counts = [0, 0]
    for _ in range(2000):
        for key, _ in gen.tick(second):
            for i, k in enumerate(keys):
                counts[i] += key == k.key
    # about 2.41 events per tick and key: the mean of 2,000 ticks is within 1.4% at one sigma
    for c in counts:
        assert c / 2000 == pytest.approx(expected, rel=0.05)


@pytest.mark.parametrize("world", ["eval", "paths"])
def test_live_generator_rejects_non_demo_worlds(monkeypatch, world):
    monkeypatch.setattr(sys, "argv", ["gen_live", "--world", world])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2


def test_steps_cross_the_seam_without_gap_or_overlap():
    clock = [1_800_000_000_000 + 150_000]
    next_ms = 1_800_000_000_000
    spans = []
    while True:
        step, start, nxt = take_step(next_ms, clock[0])
        if step == "wait":
            assert start == nxt == next_ms
            break
        if step == "minute":
            assert start % 60_000 == 0
            clock[0] += 7000
        spans.append((start, nxt))
        next_ms = nxt
    minutes = [a for a in spans if a[1] - a[0] == 60_000]
    seconds = [a for a in spans if a[1] - a[0] == 1000]
    assert minutes and seconds
    assert len(minutes) + len(seconds) == len(spans)
    assert spans[0][0] == 1_800_000_000_000
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:], strict=False))
    assert abs(clock[0] - spans[-1][1]) <= 1000


def test_bench_script_needs_labels_out(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["gen_live", "--world", "delay", "--script", "bench"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert "--script bench needs --labels-out" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--history-days", "0"], "--script bench needs --history-days"),
        (["--bench-incidents", "0"], "--bench-incidents must be"),
        (["--script", "demo"], "needs --script bench or none"),
    ],
)
def test_bad_argument_combinations_are_refused_up_front(monkeypatch, capsys, extra, message):
    argv = ["gen_live", "--world", "delay", "--script", "bench", "--labels-out", "x.json", *extra]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert message in capsys.readouterr().err


def test_start_bench_writes_labels_and_sets_incidents(tmp_path):
    w = bench_world.build("delay", 1)
    gen = LiveGenerator(w, 1, [])
    out = tmp_path / "labels.json"
    t0 = 1_800_000_000_000
    incidents = start_bench(w, gen, out, t0, 6, 1)
    assert len(incidents) == 6 and gen._incidents == incidents
    assert [p.name for p in tmp_path.iterdir()] == ["labels.json"]
    lab = orjson.loads(out.read_bytes())
    assert lab["t0_ms"] == t0 and len(lab["incidents"]) == 6
    assert lab["incidents"][0]["start_ms"] == incidents[0].start_ms


def test_failed_deliveries_end_a_bench_run_only(caplog):
    stats = DeliveryStats()
    stats.failed = 1
    check_failed("demo", stats)
    check_failed("none", stats)
    with caplog.at_level("ERROR", logger="gen_live"), pytest.raises(SystemExit) as exc:
        check_failed("bench", stats)
    assert exc.value.code == 1 and "deliveries failed" in caplog.text
    stats.failed = 0
    check_failed("bench", stats)


def test_delay_world_tick_rate():
    w = bench_world.build("delay", 1)
    gen = LiveGenerator(w, 5, [])
    n = sum(len(gen.tick(T + i * 1000)) for i in range(2000))
    assert n / 2000 / len(w.keys) == pytest.approx(40 / 60, rel=0.10)

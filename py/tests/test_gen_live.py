import orjson
import pytest

from tenant_baselines.gen_live import DeliveryStats, LiveGenerator
from tenant_baselines.incidents import Incident
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

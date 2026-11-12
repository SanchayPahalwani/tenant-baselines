import orjson
import pytest

from tenant_baselines import bench_world
from tenant_baselines.bench_fill import fill_events

START = 1_800_000_000_000 // 60_000 * 60_000


def _fill(seed=1, minutes=3, rpm=120, start=START):
    return fill_events(bench_world.build("bench", 1), start, minutes, rpm, seed, 8)


def test_time_order_within_each_partition():
    per: dict[int, list[int]] = {}
    for part, key, payload in _fill():
        assert isinstance(key, bytes)
        per.setdefault(part, []).append(orjson.loads(payload)["ts"])
    assert sorted(per) == list(range(8))
    for ts in per.values():
        assert ts == sorted(ts)
        assert START <= ts[0] and ts[-1] < START + 180_000


def test_count_is_near_the_rate():
    n = sum(1 for _ in _fill())
    assert abs(n - 720_000) < 7_200


def test_partition_is_the_key_index_modulo():
    w = bench_world.build("bench", 1)
    index = {k.key.encode(): i for i, k in enumerate(w.keys)}
    for part, key, _ in _fill(minutes=1, rpm=2):
        assert part == index[key] % 8


def test_same_seed_same_events():
    a = list(_fill(minutes=1, rpm=5))
    assert a == list(_fill(minutes=1, rpm=5))
    assert a != list(_fill(seed=2, minutes=1, rpm=5))


def test_start_must_be_on_a_minute():
    with pytest.raises(ValueError, match="multiple"):
        next(_fill(start=1))

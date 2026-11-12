import bisect
from collections import defaultdict

import numpy as np
import orjson
import pytest

from tenant_baselines import latency as lat
from tenant_baselines.evalmodel import EPOCH_MS, Rates, TrafficParams, no_effects, rates
from tenant_baselines.replay_events import key_events, partition_events, partition_of
from tenant_baselines.simulate import aggregate_key, draw_key
from tenant_baselines.world import build_world

N = 1440
SEED = 5
KEYS = (0, 3)


@pytest.fixture(scope="module")
def setup():
    world = build_world("paths", 7)
    r = rates(world, TrafficParams(weeks=1), 7)
    r = Rates(r.base[:, :N].copy(), r.peak, r.start)
    e = no_effects(len(world.keys), N)
    return world, {k: draw_key(world, r, e, SEED, k) for k in range(len(world.keys))}


def reference(ts, latency, ok):
    """Group by ts // 60_000; count, failed, and p95 by bisect and the rank rule."""
    minutes = defaultdict(list)
    for t, x, o in zip(ts.tolist(), latency.tolist(), ok.tolist(), strict=True):
        minutes[t // 60_000].append((x, o))
    bounds = lat.bounds()
    out = {}
    for m, items in minutes.items():
        hist = np.zeros(lat.BUCKETS, dtype=np.int64)
        for x, _ in items:
            hist[max(bisect.bisect_right(bounds, x) - 1, 0)] += 1
        rank = int(np.ceil(0.95 * len(items)))
        idx = int(np.argmax(np.cumsum(hist) >= rank))
        out[m] = (len(items), sum(not o for _, o in items), float(lat.representative(idx)))
    return out


def test_events_and_aggregates_from_the_same_draws_agree(setup):
    _, draws = setup
    for k in KEYS:
        d = draws[k]
        counts, errors, p95 = aggregate_key(d)
        ref = reference(*key_events(d, EPOCH_MS, SEED, k))
        base = EPOCH_MS // 60_000
        for m in range(N):
            if counts[m] == 0:
                assert base + m not in ref
                continue
            c, f, p = ref[base + m]
            assert (c, f) == (counts[m], errors[m])
            assert p == p95[m]


def test_events_stay_inside_their_minute(setup):
    _, draws = setup
    ts, _, _ = key_events(draws[0], EPOCH_MS, SEED, 0)
    assert (np.diff(ts) >= 0).all()
    got = np.bincount((ts - EPOCH_MS) // 60_000, minlength=N)
    assert np.array_equal(got, draws[0].counts)


def test_events_are_time_ordered_per_partition(setup):
    world, draws = setup
    seen = defaultdict(list)
    for p, key, payload in partition_events(world, list(draws.values()), EPOCH_MS, SEED, 3, 500):
        seen[p].append(orjson.loads(payload)["ts"])
        assert key.split("|")[0] == orjson.loads(payload)["tn"]
    assert set(seen) == {partition_of(k, 3) for k in range(len(world.keys))}
    for ts in seen.values():
        assert ts == sorted(ts)


def test_chunked_events_equal_whole_key_events(setup):
    world, draws = setup
    want = defaultdict(list)
    for k, d in draws.items():
        ts, latency, ok = key_events(d, EPOCH_MS, SEED, k)
        want[world.keys[k].key] = sorted(
            zip(ts.tolist(), latency.tolist(), ok.tolist(), strict=True)
        )
    got = defaultdict(list)
    for _, key, payload in partition_events(world, list(draws.values()), EPOCH_MS, SEED, 4, 500):
        e = orjson.loads(payload)
        got[key].append((e["ts"], e["lat"], e["ok"]))
    assert {k: sorted(v) for k, v in got.items()} == {k: v for k, v in want.items() if v}


def test_latency_on_the_wire_is_the_bucketed_one(setup):
    world, draws = setup
    got = defaultdict(list)
    for _, key, payload in partition_events(world, list(draws.values()), EPOCH_MS, SEED, 2, 700):
        e = orjson.loads(payload)
        got[(key, (e["ts"] - EPOCH_MS) // 60_000)].append(e["lat"])
    for k in KEYS:
        d, name = draws[k], world.keys[k].key
        ends = np.cumsum(d.counts)
        for m in range(N):
            want = d.latencies[ends[m] - d.counts[m] : ends[m]].tolist()
            assert sorted(got.get((name, m), [])) == sorted(want)


def test_offsets_do_not_change_the_aggregates(setup):
    _, draws = setup
    d = draws[0]
    a = reference(*key_events(d, EPOCH_MS, SEED, 0))
    b = reference(*key_events(d, EPOCH_MS, SEED + 100, 0))
    assert a == b
    other = key_events(d, EPOCH_MS, SEED + 100, 0)[0]
    assert not np.array_equal(key_events(d, EPOCH_MS, SEED, 0)[0], other)


def test_failed_count_per_minute_is_the_drawn_one(setup):
    _, draws = setup
    d = draws[0]
    ts, _, ok = key_events(d, EPOCH_MS, SEED, 0)
    failed = np.bincount((ts[~ok] - EPOCH_MS) // 60_000, minlength=N)
    assert np.array_equal(failed, d.errors)


def test_partition_rule():
    assert [partition_of(k, 3) for k in range(5)] == [0, 1, 2, 0, 1]


def test_eight_partitions_in_one_default_chunk(setup):
    """check-paths' layout: 8 partitions, the default chunk (a week, so one chunk for a day)."""
    world, draws = setup
    seen = defaultdict(list)
    got = defaultdict(list)
    for p, key, payload in partition_events(world, list(draws.values()), EPOCH_MS, SEED, 8):
        e = orjson.loads(payload)
        seen[p].append(e["ts"])
        got[key].append((e["ts"], e["lat"], e["ok"]))
        assert p == partition_of(next(i for i, k in enumerate(world.keys) if k.key == key), 8)
    assert len(world.keys) == 10 and set(seen) == set(range(8))  # keys 8 and 9 share 0 and 1
    for ts in seen.values():
        assert ts == sorted(ts)
    for k, d in draws.items():
        ts, latency, ok = key_events(d, EPOCH_MS, SEED, k)
        want = sorted(zip(ts.tolist(), latency.tolist(), ok.tolist(), strict=True))
        assert sorted(got[world.keys[k].key]) == want

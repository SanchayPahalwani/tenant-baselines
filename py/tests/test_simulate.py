import bisect
import dataclasses

import numpy as np
import pytest

from tenant_baselines import latency as lat
from tenant_baselines.evalmodel import (
    EPOCH_MS,
    Rates,
    TrafficParams,
    no_effects,
    rates,
)
from tenant_baselines.simulate import (
    Aggregates,
    aggregate_key,
    check_rows,
    checked,
    draw_key,
    encode_row,
    rows,
    simulate,
)
from tenant_baselines.world import build_world

N = 2880
SEED = 5


@pytest.fixture(scope="module")
def setup():
    world = build_world("paths", 7)
    r = rates(world, TrafficParams(weeks=1), 7)
    r = Rates(r.base[:, :N].copy(), r.peak, r.start)
    return world, r


def test_simulation_is_deterministic(setup):
    world, r = setup
    e = no_effects(len(world.keys), N)
    a, b = simulate(world, r, e, SEED), simulate(world, r, e, SEED)
    assert np.array_equal(a.count, b.count)
    assert np.array_equal(a.errors, b.errors)
    assert np.array_equal(a.p95, b.p95, equal_nan=True)
    c = simulate(world, r, e, SEED + 1)
    assert not np.array_equal(a.count, c.count)


def test_counts_follow_the_rate(setup):
    world, r = setup
    k = int(np.argmax(r.peak))
    d = draw_key(world, r, no_effects(len(world.keys), N), SEED, k)
    day = slice(0, 1440)
    assert abs(d.counts[day].sum() / r.base[k, day].sum() - 1) < 0.03


def test_errors_and_latency_follow_the_effects(setup):
    world, r = setup
    k = int(np.argmax(r.peak))
    n_keys = len(world.keys)
    plain = no_effects(n_keys, N)
    hit = no_effects(n_keys, N)
    hit.error_add[k, :600] = 0.08
    hit.latency[k, :600] = 2.5
    d0 = draw_key(world, r, plain, SEED, k)
    d1 = draw_key(world, r, hit, SEED, k)
    s = slice(0, 600)
    rate0 = d0.errors[s].sum() / d0.counts[s].sum()
    rate1 = d1.errors[s].sum() / d1.counts[s].sum()
    assert abs((rate1 - rate0) - 0.08) < 0.02
    p0 = aggregate_key(d0)[2][s]
    p1 = aggregate_key(d1)[2][s]
    ratio = np.nanmedian(p1) / np.nanmedian(p0)
    assert 2.2 < ratio < 2.8


def test_p95_matches_a_plain_python_aggregator(setup):
    world, r = setup
    k = int(np.argmax(r.peak))
    d = draw_key(world, r, no_effects(len(world.keys), N), SEED, k)
    p95 = aggregate_key(d)[2]
    bounds = lat.bounds().tolist()
    ends = np.cumsum(d.counts)
    for m in range(200):
        lo = int(ends[m - 1]) if m else 0
        vals = d.latencies[lo : int(ends[m])].tolist()
        n = len(vals)
        if n == 0:
            assert np.isnan(p95[m])
            continue
        hist = [0] * lat.BUCKETS
        for v in vals:
            hist[max(bisect.bisect_right(bounds, v) - 1, 0)] += 1
        rank = int(np.ceil(0.95 * n))
        run, idx = 0, lat.BUCKETS - 1
        for i, c in enumerate(hist):
            run += c
            if run >= rank:
                idx = i
                break
        assert p95[m] == lat.representative(np.array(idx))


def one_key_world(base_row, n=None):
    world = build_world("paths", 7)
    world = dataclasses.replace(world, keys=world.keys[:1])
    base = np.array([base_row], dtype=float)
    return world, Rates(base, np.array([float(base.max())]), np.zeros(1, dtype=np.int64))


def test_rows_start_at_the_first_request_and_include_zero_minutes():
    base = np.full(300, 50.0)
    base[:20] = 0.0
    base[150:210] = 0.0
    world, r = one_key_world(base)
    a = simulate(world, r, no_effects(1, 300), SEED)
    out = list(rows(a))
    assert a.first[0] == 20
    assert out[0]["minute_ms"] == EPOCH_MS + 20 * 60_000
    assert len(out) == 280
    assert out[-1]["minute_ms"] == EPOCH_MS + 299 * 60_000
    middle = [x for x in out if EPOCH_MS + 150 * 60_000 <= x["minute_ms"] < EPOCH_MS + 210 * 60_000]
    assert len(middle) == 60
    assert all(x["count"] == 0 and x["p95_latency_ms"] is None for x in middle)
    assert all(x["p95_latency_ms"] is not None for x in out if x["count"] > 0)
    assert set(out[0]) == {
        "key",
        "tenant",
        "region",
        "unit",
        "minute_ms",
        "count",
        "errors",
        "p95_latency_ms",
    }
    assert encode_row(out[0]).startswith(b"{")


def test_rows_are_in_minute_then_key_order(setup):
    world, r = setup
    a = simulate(world, r, no_effects(len(world.keys), N), SEED)
    seq = [(x["minute_ms"], [k.key for k in a.keys].index(x["key"])) for x in rows(a)]
    assert seq == sorted(seq)
    assert len(seq) == int(sum(N - f for f in a.first))


def test_check_rows_rejects_gap_repeat_and_disorder(setup):
    world, r = setup
    a = simulate(world, r, no_effects(len(world.keys), N), SEED)
    good = list(rows(a))[:200]
    assert len(list(checked(good))) == 200
    key = good[0]["key"]
    idx = [i for i, x in enumerate(good) if x["key"] == key]
    gap = [x for i, x in enumerate(good) if i != idx[2]]
    repeat = good[: idx[2] + 1] + [good[idx[2]]] + good[idx[2] + 1 :]
    back = list(good)
    back[idx[1]], back[idx[2]] = back[idx[2]], back[idx[1]]
    for bad in (gap, repeat, back):
        with pytest.raises(ValueError):
            list(checked(bad))
    base = np.full(10_300, 50.0)
    base[100 : 100 + 10_080] = 0.0
    world1, r1 = one_key_world(base)
    silent = simulate(world1, r1, no_effects(1, 10_300), SEED)
    with pytest.raises(ValueError, match="silent"):
        check_rows(silent)
    with pytest.raises(ValueError, match="no request"):
        check_rows(a)  # the cut world holds keys that start in week 2


def test_check_rows_rejects_other_bad_rows(setup):
    world, r = setup
    a = simulate(world, r, no_effects(len(world.keys), N), SEED)
    k = int(np.argmax(a.count.sum(axis=1)))
    for edit in ("errors", "nan", "set"):
        b = dataclasses.replace(a, count=a.count.copy(), errors=a.errors.copy(), p95=a.p95.copy())
        m = int(a.first[k])
        if edit == "errors":
            b.errors[k, m] = b.count[k, m] + 1
        elif edit == "nan":
            b.p95[k, m] = np.nan
        else:
            b.count[k, m + 1] = 0  # the p95 of that minute is still set
        with pytest.raises(ValueError):
            check_rows(b)


def test_aggregates_round_trip_through_npz(setup, tmp_path):
    world, r = setup
    a = simulate(world, r, no_effects(len(world.keys), N), SEED)
    a.save(tmp_path / "a.npz")
    b = Aggregates.load(tmp_path / "a.npz")
    assert b.keys == a.keys and b.epoch_ms == a.epoch_ms
    for f in ("first", "count", "errors"):
        assert np.array_equal(getattr(a, f), getattr(b, f))
    assert np.array_equal(a.p95, b.p95, equal_nan=True)

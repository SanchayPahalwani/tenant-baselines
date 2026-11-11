import dataclasses
from functools import cache

import numpy as np
import pytest

from tenant_baselines.evalmodel import (
    EPOCH_MS,
    MINUTE_MS,
    MINUTES,
    TrafficParams,
    rates,
    week_range,
)
from tenant_baselines.scenarios import (
    PlacementError,
    Scenario,
    apply,
    place,
    read_labels,
    write_labels,
)
from tenant_baselines.world import Key, build_world

SEED = 7
SHOULD_PAGE_COUNTS = {
    "regional_outage": 1,
    "drop_work": 1,
    "drop_night": 1,
    "latency_creep": 2,
    "offhours_latency": 2,
    "error_burst": 2,
    "cold_start_burst": 1,
}
LENGTHS = {
    "regional_outage": 40,
    "drop_work": 60,
    "drop_night": 60,
    "latency_creep": 420,
    "offhours_latency": 30,
    "error_burst": 20,
    "cold_start_burst": 20,
    "launch_surge": 180,
    "holiday_dip": 1440,
    "monday_peak": 180,
}


@cache
def world_and_rates(name: str = "eval", seed: int = SEED, amplitude: float = 6.0):
    world = build_world(name, seed)
    return world, rates(world, TrafficParams(amplitude=amplitude), seed)


@cache
def placed(name: str = "eval", seed: int = SEED, amplitude: float = 6.0):
    world, r = world_and_rates(name, seed, amplitude)
    return place(world, r, seed)


def minute(ms: int) -> int:
    return (ms - EPOCH_MS) // MINUTE_MS


def week_of(sc: Scenario) -> int:
    return minute(sc.start_ms) // 10_080 + 1


def test_ten_should_page_scenarios_per_scored_week():
    for week in (4, 5):
        pages = [s for s in placed() if week_of(s) == week and s.should_page]
        assert len(pages) == 10
        by_type = {t: sum(1 for s in pages if s.type == t) for t in SHOULD_PAGE_COUNTS}
        assert by_type == SHOULD_PAGE_COUNTS
        assert all(s.scored for s in pages)
        others = {s.type for s in placed() if week_of(s) == week and not s.should_page}
        assert others == {"launch_surge", "holiday_dip", "monday_peak"}


def test_fit_weeks_have_two_labelled_incidents_each():
    for week in (1, 2, 3):
        mine = [s for s in placed() if week_of(s) == week]
        assert sorted(s.type for s in mine) == ["drop_work", "error_burst"]
        assert not any(s.scored for s in mine)


def test_ids_regions_and_notes():
    sc = placed()
    assert len({s.id for s in sc}) == len(sc)
    for s in sc:
        assert s.id.startswith(f"w{week_of(s)}-{s.type}-")
        assert (s.region is not None) == (s.type in ("regional_outage", "holiday_dip"))
    outage = next(s for s in sc if s.type == "regional_outage")
    assert "UTC" in outage.note and "local" in outage.note and outage.region in outage.note


def test_magnitudes_are_the_tables():
    world, _ = world_and_rates()
    sc = placed()
    eff = apply(sc, world, MINUTES)
    index = {k.key: i for i, k in enumerate(world.keys)}

    def first(t: str) -> Scenario:
        return next(s for s in sc if s.type == t and s.scored)

    def at(s: Scenario):
        return minute(s.start_ms), minute(s.end_ms), index[s.keys[0]]

    s = first("regional_outage")
    a, b, k = at(s)
    for m, err, lat in ((a, 0.15, 1.5), (b - 1, 0.15, 1.5), (b, 0.0, 1.0)):
        assert eff.error_add[k, m] == pytest.approx(err)
        assert eff.latency[k, m] == pytest.approx(lat)
    for t, factor in (("drop_work", 0.3), ("drop_night", 0.3), ("launch_surge", 3.0)):
        s = first(t)
        a, b, k = at(s)
        assert eff.volume[k, a] == pytest.approx(factor)
        assert eff.volume[k, b - 1] == pytest.approx(factor)
        assert eff.volume[k, b] == pytest.approx(1.0)
    s = first("holiday_dip")
    a, b, k = at(s)
    assert [eff.volume[k, m] for m in (a, b - 1, b)] == pytest.approx([0.35, 0.35, 1.0])
    s = first("latency_creep")
    a, b, k = at(s)
    assert b - a == 420
    got = [eff.latency[k, a + i] for i in (0, 180, 359, 360, 419)] + [eff.latency[k, b]]
    assert got == pytest.approx([1.0, 1.75, 1 + 1.5 * 359 / 360, 2.5, 2.5, 1.0])
    s = first("offhours_latency")
    a, b, k = at(s)
    assert [eff.latency[k, m] for m in (a, b - 1, b)] == pytest.approx([2.5, 2.5, 1.0])
    for t in ("error_burst", "cold_start_burst"):
        s = first(t)
        a, b, k = at(s)
        assert [eff.error_add[k, m] for m in (a, b - 1, b)] == pytest.approx([0.08, 0.08, 0.0])
    s = first("monday_peak")
    a, b, k = at(s)
    assert eff.volume[k, a] == 1.0 and eff.error_add[k, a] == 0.0 and eff.latency[k, a] == 1.0


def test_eligibility_holds():
    world, r = world_and_rates()
    index = {k.key: i for i, k in enumerate(world.keys)}
    tenant = {t.name: t for t in world.tenants}
    dedicated = {u.name for u in world.units if u.dedicated_for}
    region_off = {g.name: g.utc_offset_h for g in world.regions}

    def tz(key: str) -> int:
        return tenant[world.keys[index[key]].tenant].tz_offset_h

    def local_parts(m: int, off: int) -> tuple[int, int]:
        loc = m + off * 60
        return loc % 1440, (loc // 1440) % 7  # minute of day, weekday (0 = Monday)

    def window(s: Scenario) -> range:
        return range(minute(s.start_ms), minute(s.end_ms))

    def base(key: str, m: int) -> float:
        return float(r.base[index[key], m])

    def sum5(key: str, m: int) -> float:
        return sum(base(key, j) for j in range(max(m - 4, 0), m + 1))

    seen = set()
    for s in placed():
        seen.add(s.type)
        w = window(s)
        assert len(w) == LENGTHS[s.type]
        ws, we = week_range(week_of(s))
        assert s.start_ms >= ws and s.end_ms <= we - 60 * MINUTE_MS
        if s.type == "regional_outage":
            want = tuple(k.key for k in world.keys if k.region == s.region)
            assert s.keys == want
        elif s.type == "drop_work":
            (key,) = s.keys
            assert world.keys[index[key]].unit in dedicated
            md, wd = local_parts(w[0], tz(key))
            assert wd < 5 and md >= 540 and md + 60 <= 1020
        elif s.type == "drop_night":
            (key,) = s.keys
            assert world.keys[index[key]].unit in dedicated
            for m in w:
                md, wd = local_parts(m, tz(key))
                assert md >= 1200 or md < 420 or wd >= 5
                assert sum5(key, m) >= 15
        elif s.type == "latency_creep":
            units = {world.keys[index[k]].unit for k in s.keys}
            assert len(units) == 1
            assert s.keys == tuple(k.key for k in world.keys if k.unit in units)
            cells = [base(k, m) >= 20 for k in s.keys for m in w]
            assert sum(cells) * 2 > len(cells)
        elif s.type == "offhours_latency":
            (key,) = s.keys
            md, _ = local_parts(w[0], tz(key))
            assert md + 30 <= 360
            assert all(base(key, m) >= 25 for m in w)
        elif s.type == "error_burst":
            (key,) = s.keys
            assert world.keys[index[key]].unit not in dedicated
            assert all(base(key, m) >= 30 for m in w)
        elif s.type == "cold_start_burst":
            (key,) = s.keys
            assert tenant[world.keys[index[key]].tenant].start_week == 4
        elif s.type == "launch_surge":
            names = {world.keys[index[k]].tenant for k in s.keys}
            assert len(names) == 1
            assert s.keys == tuple(k.key for k in world.keys if k.tenant in names)
            md, wd = local_parts(w[0], tz(s.keys[0]))
            assert wd < 5 and md >= 540 and md + 180 <= 1020
        elif s.type == "holiday_dip":
            assert s.keys == tuple(k.key for k in world.keys if k.region == s.region)
            md, wd = local_parts(w[0], region_off[s.region])
            assert md == 0 and wd < 5
        elif s.type == "monday_peak":
            md, wd = local_parts(w[0], tz(s.keys[0]))
            assert wd == 0 and md == 480
            same = {tz(k) for k in s.keys}
            assert len(same) == 1
            want = tuple(
                k.key for k in world.keys if tenant[k.tenant].tz_offset_h == same.copy().pop()
            )
            assert s.keys == want
            assert not s.should_page and s.expected_signal is None
    assert seen == set(LENGTHS)


def test_scenarios_stay_inside_their_week_and_apart():
    by_key: dict[str, list[tuple[int, int]]] = {}
    for s in placed():
        if s.type == "monday_peak":
            continue
        ws, we = week_range(week_of(s))
        assert ws <= s.start_ms < s.end_ms <= we - 60 * MINUTE_MS
        for k in s.keys:
            by_key.setdefault(k, []).append((s.start_ms, s.end_ms))
    for spans in by_key.values():
        spans.sort()
        for (_, end), (start, _) in zip(spans, spans[1:], strict=False):
            assert start - end >= 120 * MINUTE_MS


def test_placement_is_deterministic():
    world, r = world_and_rates()
    assert place(world, r, SEED) == list(placed())
    other = place(world, r, SEED + 1)
    assert [s.start_ms for s in other] != [s.start_ms for s in placed()]


def test_labels_round_trip(tmp_path):
    meta = {
        "seed": SEED,
        "amplitude": 6.0,
        "world": "eval",
        "epoch_ms": EPOCH_MS,
        "replay_end_ms": EPOCH_MS + 5 * 10_080 * MINUTE_MS,
        "data": "synthetic",
    }
    path = tmp_path / "labels.json"
    write_labels(path, placed(), meta)
    got, got_meta = read_labels(path)
    assert got == list(placed())
    assert got_meta == meta


def test_placement_error_names_the_scenario():
    world, _ = world_and_rates()
    tiny = dataclasses.replace(
        world, keys=tuple(Key(k.tenant, k.region, k.unit, 5.0) for k in world.keys)
    )
    r = rates(tiny, TrafficParams(amplitude=6.0), SEED)
    with pytest.raises(PlacementError) as err:
        place(tiny, r, SEED)
    msg = str(err.value)
    assert "error_burst" in msg and f"seed {SEED}" in msg and "week 1" in msg


@pytest.mark.parametrize("amplitude", [1.5, 6.0, 12.0])
def test_placement_succeeds_on_twenty_seeds_at_every_amplitude(amplitude):
    for seed in range(900_000, 900_020):
        world = build_world("eval", seed)
        r = rates(world, TrafficParams(amplitude=amplitude), seed)
        assert len(place(world, r, seed)) > 0


def test_placement_succeeds_on_twenty_seeds_in_the_paths_world():
    for seed in range(900_000, 900_020):
        world = build_world("paths", seed)
        r = rates(world, TrafficParams(amplitude=6.0), seed)
        assert len(place(world, r, seed)) > 0


def test_effects_shape():
    world, _ = world_and_rates()
    eff = apply([], world, 100)
    assert eff.volume.shape == (len(world.keys), 100)
    assert np.all(eff.volume == 1.0) and np.all(eff.error_add == 0.0)

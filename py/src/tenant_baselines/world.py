"""Synthetic world: regions, tenants, units and the keys that carry traffic."""

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Region:
    name: str
    utc_offset_h: int


@dataclass(frozen=True)
class Tenant:
    name: str
    home_region: str
    tz_offset_h: int
    peak_rpm: float
    steady: bool = False
    start_week: int = 1


@dataclass(frozen=True)
class Unit:
    name: str
    region: str
    dedicated_for: str | None
    base_latency_ms: float


@dataclass(frozen=True)
class Key:
    tenant: str
    region: str
    unit: str
    # Weekday peak in requests per minute; None splits the tenant's peak evenly over its keys.
    peak_rpm: float | None = None

    @property
    def key(self) -> str:
        return f"{self.tenant}|{self.region}|{self.unit}"


@dataclass(frozen=True)
class World:
    regions: tuple[Region, ...]
    tenants: tuple[Tenant, ...]
    units: tuple[Unit, ...]
    keys: tuple[Key, ...]


_DEMO_REGIONS = (Region("us-east", -5), Region("eu-central", 1), Region("ap-southeast", 8))
_N_TENANTS = 8
_MEDIAN_PEAK_RPM = 150.0
_PEAK_SIGMA = 0.8
# Tenant index whose users sit in a different time zone than its home region.
_OFFSET_TENANT = 4
_OFFSET_TENANT_TZ_H = -8
# Tenants with a dedicated unit in their home region.
_DEDICATED_TENANTS = (0, 1)
# Tenants that also send traffic to the first shared unit of another region.
_CROSS_REGION_TENANTS = (0, 3)
# Appended after the random part, so the first eight tenants and their keys do not change.
# Small tenants with a flat traffic profile, so the region always has 3 active tenants.
_STEADY_REGION = "ap-southeast"
_STEADY_TZ_H = 8
_STEADY_PEAKS = (("t09", 60.0), ("t10", 30.0), ("t11", 30.0))
_STEADY_DEDICATED = "t09"
_STEADY_UNIT_LATENCY_MS = 55.0


def build_world(name: str, seed: int) -> World:
    if name == "eval":
        return _build_eval_world(seed)
    if name == "paths":
        return _build_paths_world()
    if name != "demo":
        raise ValueError(f"unknown world: {name!r}")
    rng = np.random.default_rng(seed)
    regions = _DEMO_REGIONS
    peaks = rng.lognormal(np.log(_MEDIAN_PEAK_RPM), _PEAK_SIGMA, _N_TENANTS)
    base_latency = rng.uniform(30.0, 90.0, len(regions) * 2 + len(_DEDICATED_TENANTS))

    tenants = []
    for i in range(_N_TENANTS):
        home = regions[i % len(regions)]
        tz = _OFFSET_TENANT_TZ_H if i == _OFFSET_TENANT else home.utc_offset_h
        tenants.append(Tenant(f"t{i + 1:02d}", home.name, tz, round(float(peaks[i]), 1)))

    units: list[Unit] = []
    for r in regions:
        for s in ("s1", "s2"):
            units.append(
                Unit(f"{r.name}-{s}", r.name, None, round(float(base_latency[len(units)]), 1))
            )
    for i in _DEDICATED_TENANTS:
        t = tenants[i]
        units.append(
            Unit(
                f"{t.home_region}-d-{t.name}",
                t.home_region,
                t.name,
                round(float(base_latency[len(units)]), 1),
            )
        )

    keys: list[Key] = []
    for i, t in enumerate(tenants):
        keys.append(Key(t.name, t.home_region, f"{t.home_region}-s1"))
        second = f"{t.home_region}-d-{t.name}" if i in _DEDICATED_TENANTS else f"{t.home_region}-s2"
        keys.append(Key(t.name, t.home_region, second))
        if i in _CROSS_REGION_TENANTS:
            other = regions[(i + 1) % len(regions)]
            if other.name == t.home_region:
                other = regions[(i + 2) % len(regions)]
            keys.append(Key(t.name, other.name, f"{other.name}-s1"))

    for name_, peak in _STEADY_PEAKS:
        tenants.append(Tenant(name_, _STEADY_REGION, _STEADY_TZ_H, peak, steady=True))
    dedicated_unit = f"{_STEADY_REGION}-d-{_STEADY_DEDICATED}"
    units.append(Unit(dedicated_unit, _STEADY_REGION, _STEADY_DEDICATED, _STEADY_UNIT_LATENCY_MS))
    shared = f"{_STEADY_REGION}-s1"
    keys.append(Key("t09", _STEADY_REGION, shared))
    keys.append(Key("t09", _STEADY_REGION, dedicated_unit))
    keys.append(Key("t10", _STEADY_REGION, shared))
    keys.append(Key("t11", _STEADY_REGION, shared))
    return World(regions, tuple(tenants), tuple(units), tuple(keys))


_EVAL_N_TENANTS = 24
_EVAL_DEDICATED = 6
# Tenants that keep the hours of the next region's time zone.
_EVAL_SHIFTED = (6, 10, 14, 18, 22)
_EVAL_NEW_TENANT = 23
_EVAL_NEW_START_WEEK = 4


def _build_eval_world(seed: int) -> World:
    """24 tenants, 3 regions, 48 keys. Draw order: 12 unit latencies, then 48 key peaks."""
    rng = np.random.default_rng([seed, 0])
    regions = _DEMO_REGIONS
    n_units = len(regions) * 2 + _EVAL_DEDICATED
    latency = [round(float(x), 1) for x in rng.uniform(60.0, 120.0, n_units)]
    n_keys = _EVAL_DEDICATED * 2 + (_EVAL_N_TENANTS - _EVAL_DEDICATED) * 2
    peaks = [
        round(float(x), 1) for x in rng.lognormal(np.log(_MEDIAN_PEAK_RPM), _PEAK_SIGMA, n_keys)
    ]

    homes = [regions[i % len(regions)] for i in range(_EVAL_N_TENANTS)]
    units: list[Unit] = []
    for r in regions:
        for s in ("s1", "s2"):
            units.append(Unit(f"{r.name}-{s}", r.name, None, latency[len(units)]))
    for i in range(_EVAL_DEDICATED):
        name = f"t{i + 1:02d}"
        units.append(Unit(f"{homes[i].name}-d-{name}", homes[i].name, name, latency[len(units)]))

    key_units: list[tuple[int, str]] = []
    for i in range(_EVAL_N_TENANTS):
        h = homes[i].name
        if i < _EVAL_DEDICATED:
            key_units += [(i, f"{h}-d-t{i + 1:02d}"), (i, f"{h}-s1")]
        else:
            key_units += [(i, f"{h}-s1"), (i, f"{h}-s2")]
    keys = tuple(
        Key(f"t{i + 1:02d}", homes[i].name, unit, peaks[j]) for j, (i, unit) in enumerate(key_units)
    )

    tenants = []
    for i in range(_EVAL_N_TENANTS):
        tz = (
            regions[(i + 1) % len(regions)].utc_offset_h
            if i in _EVAL_SHIFTED
            else homes[i].utc_offset_h
        )
        total = round(sum(k.peak_rpm for k in keys if k.tenant == f"t{i + 1:02d}"), 1)
        start = _EVAL_NEW_START_WEEK if i == _EVAL_NEW_TENANT else 1
        tenants.append(Tenant(f"t{i + 1:02d}", homes[i].name, tz, total, False, start))
    return World(regions, tuple(tenants), tuple(units), keys)


def _build_paths_world() -> World:
    """Ten fixed keys in one region, for the equivalence check. The seed is not used."""
    region = Region("eu-central", 1)
    eu = region.name
    tenants = (
        Tenant("p1", eu, 1, 330.0),
        Tenant("p2", eu, 1, 310.0),
        Tenant("p3", eu, -5, 310.0),
        Tenant("p4", eu, 1, 100.0),
        Tenant("p5", eu, 1, 300.0, False, 4),
    )
    units = (
        Unit("eu-central-s1", eu, None, 70.0),
        Unit("eu-central-s2", eu, None, 90.0),
        Unit("eu-central-d-p1", eu, "p1", 110.0),
    )
    s1, s2, d1 = (u.name for u in units)
    keys = (
        Key("p1", eu, d1, 180.0),
        Key("p1", eu, s1, 150.0),
        Key("p2", eu, s1, 160.0),
        Key("p2", eu, s2, 150.0),
        Key("p3", eu, s1, 160.0),
        Key("p3", eu, s2, 150.0),
        Key("p4", eu, s1, 60.0),
        Key("p4", eu, s2, 40.0),
        Key("p5", eu, s1, 150.0),
        Key("p5", eu, s2, 150.0),
    )
    return World((region,), tenants, units, keys)

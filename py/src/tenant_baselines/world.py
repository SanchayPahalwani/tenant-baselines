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

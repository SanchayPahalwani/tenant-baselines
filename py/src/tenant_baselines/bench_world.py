"""The benchmark's synthetic worlds, built from `world.py`'s dataclasses.

`world.py` is one of the sealed eval paths, so the benchmark's worlds live here.
Synthetic data only.
"""

import numpy as np

from tenant_baselines.world import _DEMO_REGIONS, Key, Tenant, Unit, World

__all__ = ["build"]

_N_TENANTS = 1000
_UNITS_PER_REGION = 8
_PEAK_RPM = 240.0
_LATENCY_RANGE_MS = (30.0, 80.0)
_DELAY_TENANTS = 36
_DELAY_PEAK_RPM = 40.0
_DELAY_UNIT_LATENCY_MS = (70.0, 90.0)


def build(name: str, seed: int) -> World:
    if name == "bench":
        return _build_bench(seed)
    if name == "delay":
        return _build_delay()
    raise ValueError(f"unknown bench world: {name!r}")


def _build_bench(seed: int) -> World:
    """3 regions, 1,000 tenants, 24 shared units, 2,000 keys; flat traffic.

    Tenant i is homed in region i % 3 and sends to two units of that region, so the keys of a
    tenant never share a unit. Draw order: the 24 unit latencies, region by region.
    """
    regions = _DEMO_REGIONS
    rng = np.random.default_rng([seed, 5])
    lat = rng.uniform(*_LATENCY_RANGE_MS, len(regions) * _UNITS_PER_REGION)
    units = tuple(
        Unit(
            f"{r.name}-s{s + 1}",
            r.name,
            None,
            round(float(lat[ri * _UNITS_PER_REGION + s]), 1),
        )
        for ri, r in enumerate(regions)
        for s in range(_UNITS_PER_REGION)
    )
    tenants, keys = [], []
    for i in range(_N_TENANTS):
        home = regions[i % len(regions)]
        tenant = f"b{i + 1:04d}"
        tenants.append(Tenant(tenant, home.name, home.utc_offset_h, _PEAK_RPM, steady=True))
        j = i // len(regions)
        for s in ((j % _UNITS_PER_REGION) + 1, ((j + 1) % _UNITS_PER_REGION) + 1):
            keys.append(Key(tenant, home.name, f"{home.name}-s{s}"))
    return World(regions, tuple(tenants), units, tuple(keys))


def _build_delay() -> World:
    """3 regions, 36 tenants (d01 to d36) with one key each, 40 requests a minute, flat.

    Tenant i is homed in region i % 3. Each region has two shared units, `-s1` at 70 ms and
    `-s2` at 90 ms; the tenant's key is on `-s1` when (i // 3) % 2 == 0, else on `-s2`.
    The seed is not used.
    """
    regions = _DEMO_REGIONS
    units = tuple(
        Unit(f"{r.name}-s{s + 1}", r.name, None, _DELAY_UNIT_LATENCY_MS[s])
        for r in regions
        for s in range(2)
    )
    tenants, keys = [], []
    for i in range(_DELAY_TENANTS):
        home = regions[i % len(regions)]
        tenant = f"d{i + 1:02d}"
        tenants.append(Tenant(tenant, home.name, home.utc_offset_h, _DELAY_PEAK_RPM, steady=True))
        suffix = "s1" if (i // len(regions)) % 2 == 0 else "s2"
        keys.append(Key(tenant, home.name, f"{home.name}-{suffix}"))
    return World(regions, tuple(tenants), units, tuple(keys))

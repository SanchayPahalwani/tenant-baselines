"""Scripted incidents that change a key's volume, error rate or latency."""

import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from tenant_baselines.traffic import key_rate_rpm
from tenant_baselines.world import Key, World


@dataclass(frozen=True)
class Incident:
    """An incident active on [start_ms, end_ms).

    magnitude: error_spike adds it to the error probability; volume_mult is the
    factor traffic is multiplied by (0.3 leaves 30%, 3.0 is a surge); latency_spike
    multiplies latency by it. With neither region nor key set the incident applies
    to every key.
    """

    kind: Literal["error_spike", "volume_mult", "latency_spike"]
    start_ms: int
    end_ms: int
    magnitude: float
    region: str | None = None
    key: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.magnitude):
            raise ValueError(f"magnitude must be finite: {self.magnitude}")
        if self.kind == "volume_mult" and self.magnitude < 0:
            raise ValueError(f"volume_mult magnitude must not be negative: {self.magnitude}")


@dataclass(frozen=True)
class Effects:
    volume_mult: float = 1.0
    error_add: float = 0.0
    latency_mult: float = 1.0


def _matches(inc: Incident, key: Key) -> bool:
    return (inc.region is None or inc.region == key.region) and (
        inc.key is None or inc.key == key.key
    )


def effects(incidents: Iterable[Incident], key: Key, t_ms: int) -> Effects:
    volume, error, latency = 1.0, 0.0, 1.0
    for inc in incidents:
        if not (inc.start_ms <= t_ms < inc.end_ms) or not _matches(inc, key):
            continue
        if inc.kind == "error_spike":
            error += inc.magnitude
        elif inc.kind == "volume_mult":
            volume *= inc.magnitude
        else:
            latency *= inc.magnitude
    return Effects(volume, error, latency)


def _key_rates(world: World, t_ms: int) -> list[tuple[Key, float]]:
    tenants = {t.name: t for t in world.tenants}
    per_tenant: dict[str, int] = {}
    for k in world.keys:
        per_tenant[k.tenant] = per_tenant.get(k.tenant, 0) + 1
    return [(k, key_rate_rpm(tenants[k.tenant], per_tenant[k.tenant], t_ms)) for k in world.keys]


def pick_drop_key(world: World, t_ms: int) -> Key:
    """The key of a tenant that is not steady, on a dedicated unit, with the highest rate at t_ms.

    The drop shows a seasonal volume baseline, and it must not land on the burst key.
    Ties go to the first key in world order.
    """
    dedicated = {u.name for u in world.units if u.dedicated_for is not None}
    steady = {t.name for t in world.tenants if t.steady}
    candidates = [
        (k, r) for k, r in _key_rates(world, t_ms) if k.unit in dedicated and k.tenant not in steady
    ]
    return max(candidates, key=lambda kr: kr[1])[0]


def pick_burst_key(world: World) -> Key:
    """The first key in world order on a dedicated unit of a steady tenant."""
    steady = {t.name for t in world.tenants if t.steady}
    dedicated = {u.name for u in world.units if u.dedicated_for in steady}
    return next(k for k in world.keys if k.unit in dedicated)


def demo_script(world: World, t0_ms: int) -> list[Incident]:
    """A tenant drop, an error burst on one steady tenant's dedicated unit, then a regional spike.

    The burst pages as a tenant alert and drains the unit. The spike covers the burst key's
    region, so the other tenants' alerts join it into one regional alert.
    """
    minute = 60_000
    drop_start = t0_ms + 2 * minute
    burst = pick_burst_key(world)
    return [
        Incident(
            "volume_mult",
            drop_start,
            t0_ms + 12 * minute,
            0.1,
            key=pick_drop_key(world, drop_start).key,
        ),
        Incident("error_spike", t0_ms + 3 * minute, t0_ms + 21 * minute, 0.20, key=burst.key),
        Incident(
            "error_spike", t0_ms + 11 * minute, t0_ms + 21 * minute, 0.20, region=burst.region
        ),
    ]

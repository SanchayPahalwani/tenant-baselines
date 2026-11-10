"""Scripted incidents that change a key's volume, error rate or latency."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from tenant_baselines.world import Key, World


@dataclass(frozen=True)
class Incident:
    """An incident active on [start_ms, end_ms).

    magnitude: error_spike adds it to the error probability; volume_drop removes
    that fraction of traffic; latency_spike multiplies latency by it. With neither
    region nor key set the incident applies to every key.
    """

    kind: Literal["error_spike", "volume_drop", "latency_spike"]
    start_ms: int
    end_ms: int
    magnitude: float
    region: str | None = None
    key: str | None = None


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
        elif inc.kind == "volume_drop":
            volume *= 1.0 - inc.magnitude
        else:
            latency *= inc.magnitude
    return Effects(volume, error, latency)


def demo_script(world: World, t0_ms: int) -> list[Incident]:
    return [
        Incident("error_spike", t0_ms + 3 * 60_000, t0_ms + 13 * 60_000, 0.20, region="eu-central")
    ]

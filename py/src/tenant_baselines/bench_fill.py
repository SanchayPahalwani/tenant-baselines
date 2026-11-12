"""The throughput benchmark's pre-fill: flat synthetic events, one event minute at a time."""

from collections.abc import Iterator

import numpy as np

from tenant_baselines.events import encode
from tenant_baselines.traffic import LATENCY_SIGMA
from tenant_baselines.world import World

__all__ = ["LATENCY_FACTOR", "fill_events"]

LATENCY_FACTOR = 1.6
P_OK = 0.995
_MINUTE_MS = 60_000


def fill_events(
    world: World, start_ms: int, minutes: int, rpm: float, seed: int, n_partitions: int
) -> Iterator[tuple[int, bytes, bytes]]:
    """`(partition, key, payload)` for `minutes` event minutes from `start_ms`.

    Per minute and key: a Poisson count with mean `rpm`, timestamps uniform inside the minute,
    latency log-normal around the unit's base latency times LATENCY_FACTOR, `ok` with probability
    P_OK. The partition is the key's index in `world.keys` modulo `n_partitions`. A minute is
    sorted once, by partition and then timestamp, so each partition's timestamps never decrease.
    """
    if start_ms % _MINUTE_MS:
        raise ValueError(f"start_ms must be a multiple of {_MINUTE_MS}, got {start_ms}")
    rng = np.random.default_rng([seed, 6])
    base = {u.name: u.base_latency_ms for u in world.units}
    n_keys = len(world.keys)
    log_median = np.log(np.array([base[k.unit] for k in world.keys]) * LATENCY_FACTOR)
    key_bytes = [k.key.encode() for k in world.keys]
    partition_of = np.arange(n_keys) % n_partitions
    for m in range(minutes):
        minute_ms = start_ms + m * _MINUTE_MS
        counts = rng.poisson(rpm, n_keys)
        idx = np.repeat(np.arange(n_keys), counts)
        n = len(idx)
        ts = minute_ms + rng.integers(0, _MINUTE_MS, n)
        lat = rng.lognormal(log_median[idx], LATENCY_SIGMA)
        ok = rng.random(n) < P_OK
        part = partition_of[idx]
        order = np.lexsort((ts, part))
        for i, t, p, la, o in zip(
            idx[order].tolist(),
            ts[order].tolist(),
            part[order].tolist(),
            lat[order].tolist(),
            ok[order].tolist(),
            strict=True,
        ):
            k = world.keys[i]
            yield p, key_bytes[i], encode(t, k.tenant, k.region, k.unit, la, o)

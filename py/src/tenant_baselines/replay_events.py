"""Emit the simulated requests as events, from the same draws the aggregates use.

Synthetic data only. Latencies and failures come from KeyDraws, so counts, errors and p95
agree exactly with the aggregates. Only the position of a request inside its minute is new,
and it comes from its own random stream.
"""

from collections.abc import Iterator

import numpy as np

from tenant_baselines.evalmodel import MINUTE_MS
from tenant_baselines.events import encode
from tenant_baselines.simulate import KeyDraws
from tenant_baselines.world import World


def partition_of(k: int, n_partitions: int) -> int:
    """The live generator's rule: the key's index in the world's key list, modulo the partitions."""
    return k % n_partitions


def _offset_rng(seed: int, k: int) -> np.random.Generator:
    return np.random.default_rng([seed, 4, k])


def _slice(
    d: KeyDraws,
    ends: np.ndarray,
    epoch_ms: int,
    m0: int,
    m1: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Unsorted (ts, latency, ok) of minutes [m0, m1). Draws offsets from rng, in request order."""
    counts = d.counts[m0:m1]
    lo = int(ends[m0 - 1]) if m0 else 0
    hi = int(ends[m1 - 1])
    n = hi - lo
    minute = np.repeat(np.arange(m0, m1, dtype=np.int64), counts)
    ts = epoch_ms + minute * MINUTE_MS + rng.integers(0, MINUTE_MS, size=n)
    first = np.repeat(ends[m0:m1] - counts - lo, counts)
    rank = np.arange(n, dtype=np.int64) - first
    ok = rank >= np.repeat(d.errors[m0:m1], counts)
    return ts, d.latencies[lo:hi], ok


def key_events(
    d: KeyDraws, epoch_ms: int, seed: int, k: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(ts, latency, ok) sorted by ts. Within a minute the first errors[m] requests fail."""
    ends = np.cumsum(d.counts)
    ts, latency, ok = _slice(d, ends, epoch_ms, 0, len(d.counts), _offset_rng(seed, k))
    order = np.argsort(ts, kind="stable")
    return ts[order], latency[order], ok[order]


def partition_events(
    world: World,
    draws: list[KeyDraws],
    epoch_ms: int,
    seed: int,
    n_partitions: int,
    chunk_minutes: int = 10_080,
) -> Iterator[tuple[int, str, bytes]]:
    """(partition, key, payload) in time order within each partition, one chunk at a time."""
    n_minutes = len(draws[0].counts)
    ends = [np.cumsum(d.counts) for d in draws]
    rngs = [_offset_rng(seed, k) for k in range(len(draws))]
    for m0 in range(0, n_minutes, chunk_minutes):
        m1 = min(m0 + chunk_minutes, n_minutes)
        parts = [
            _slice(d, e, epoch_ms, m0, m1, r) for d, e, r in zip(draws, ends, rngs, strict=True)
        ]
        for p in range(n_partitions):
            ks = [k for k in range(len(draws)) if partition_of(k, n_partitions) == p]
            if not ks:
                continue
            ts = np.concatenate([parts[k][0] for k in ks])
            latency = np.concatenate([parts[k][1] for k in ks])
            ok = np.concatenate([parts[k][2] for k in ks])
            who = np.repeat(ks, [len(parts[k][0]) for k in ks])
            for i in np.argsort(ts, kind="stable").tolist():
                key = world.keys[int(who[i])]
                yield (
                    p,
                    key.key,
                    encode(
                        int(ts[i]), key.tenant, key.region, key.unit, float(latency[i]), bool(ok[i])
                    ),
                )
        parts.clear()

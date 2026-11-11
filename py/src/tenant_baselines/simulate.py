"""Simulate every synthetic request and aggregate each minute as the job's aggregates mode reads it.

Synthetic data only. Latencies are rounded to 0.1 ms, the precision of the event wire format,
so the aggregate path and the events path bucket the same number.
"""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import orjson

from tenant_baselines import latency as lat
from tenant_baselines.evalmodel import (
    EPOCH_MS,
    MINUTE_MS,
    Effects,
    Rates,
    error_prob,
    latency_median,
)
from tenant_baselines.world import Key, World

_WEEK_MINUTES = 7 * 1440
_LATENCY_SIGMA = 0.5
_MAX_SILENCE = _WEEK_MINUTES  # a silence this long or longer is rejected


@dataclass
class KeyDraws:
    counts: np.ndarray  # [minute] int64
    errors: np.ndarray  # [minute] int64
    latencies: np.ndarray  # every request, in minute order, rounded to 0.1 ms


def draw_key(world: World, rates: Rates, effects: Effects, seed: int, k: int) -> KeyDraws:
    rng = np.random.default_rng([seed, 3, k])
    key = world.keys[k]
    unit = next(u for u in world.units if u.name == key.unit)
    volume = effects.volume[k]
    load = rates.base[k] * volume / rates.peak[k]
    counts = rng.poisson(rates.base[k] * volume).astype(np.int64)
    errors = rng.binomial(counts, error_prob(load, error_add=effects.error_add[k])).astype(np.int64)
    median = latency_median(unit.base_latency_ms, load, latency=effects.latency[k])
    mu = np.repeat(np.log(median), counts)
    latencies = np.round(rng.lognormal(mu, _LATENCY_SIGMA), 1)
    return KeyDraws(counts, errors, latencies)


def aggregate_key(d: KeyDraws) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(counts, errors, p95) per minute; p95 is NaN where the count is 0. One week at a time."""
    n = len(d.counts)
    p95 = np.full(n, np.nan)
    ends = np.cumsum(d.counts)
    for a in range(0, n, _WEEK_MINUTES):
        b = min(a + _WEEK_MINUTES, n)
        lo = int(ends[a - 1]) if a else 0
        values = d.latencies[lo : int(ends[b - 1])]
        counts = d.counts[a:b]
        minute = np.repeat(np.arange(b - a), counts)
        flat = minute * lat.BUCKETS + lat.bucket_of(values)
        hist = np.bincount(flat, minlength=(b - a) * lat.BUCKETS).reshape(b - a, lat.BUCKETS)
        p95[a:b] = lat.p95(hist, counts)
    return d.counts, d.errors, p95


@dataclass
class Aggregates:
    keys: list[Key]
    first: np.ndarray  # [k] first minute index with a request; n_minutes if none
    count: np.ndarray  # [k, m]
    errors: np.ndarray
    p95: np.ndarray
    epoch_ms: int

    def save(self, path: str | Path) -> None:
        np.savez_compressed(
            path,
            key_tenant=np.array([k.tenant for k in self.keys]),
            key_region=np.array([k.region for k in self.keys]),
            key_unit=np.array([k.unit for k in self.keys]),
            key_peak=np.array([np.nan if k.peak_rpm is None else k.peak_rpm for k in self.keys]),
            first=self.first,
            count=self.count,
            errors=self.errors,
            p95=self.p95,
            epoch_ms=np.int64(self.epoch_ms),
        )

    @staticmethod
    def load(path: str | Path) -> "Aggregates":
        with np.load(path, allow_pickle=False) as z:
            keys = [
                Key(str(t), str(r), str(u), None if np.isnan(p) else float(p))
                for t, r, u, p in zip(
                    z["key_tenant"], z["key_region"], z["key_unit"], z["key_peak"], strict=True
                )
            ]
            return Aggregates(
                keys, z["first"], z["count"], z["errors"], z["p95"], int(z["epoch_ms"])
            )


def simulate(world: World, rates: Rates, effects: Effects, seed: int) -> Aggregates:
    n_keys, n_min = rates.base.shape
    count = np.zeros((n_keys, n_min), dtype=np.int64)
    errors = np.zeros((n_keys, n_min), dtype=np.int64)
    p95 = np.full((n_keys, n_min), np.nan)
    first = np.full(n_keys, n_min, dtype=np.int64)
    for k in range(n_keys):
        count[k], errors[k], p95[k] = aggregate_key(draw_key(world, rates, effects, seed, k))
        seen = np.flatnonzero(count[k])
        if len(seen):
            first[k] = seen[0]
    return Aggregates(list(world.keys), first, count, errors, p95, EPOCH_MS)


def check_rows(a: Aggregates) -> None:
    n_min = a.count.shape[1]
    for k, key in enumerate(a.keys):
        c = a.count[k]
        if a.first[k] >= n_min or c[a.first[k]] == 0:
            raise ValueError(f"key {key.key} has no request")
        tail = c[a.first[k] :]
        if (a.errors[k] > c).any():
            raise ValueError(f"key {key.key} has errors above count")
        if (np.isnan(a.p95[k]) & (c > 0)).any():
            raise ValueError(f"key {key.key} has a count without a p95")
        if (~np.isnan(a.p95[k]) & (c == 0)).any():
            raise ValueError(f"key {key.key} has a p95 on a zero row")
        zero = np.concatenate(([0], (tail == 0).astype(np.int8), [0]))
        edges = np.flatnonzero(np.diff(zero))
        runs = edges[1::2] - edges[0::2]
        if len(runs) and runs.max() >= _MAX_SILENCE:
            raise ValueError(f"key {key.key} is silent for {int(runs.max())} minutes")


def rows(a: Aggregates) -> Iterator[dict]:
    n_keys, n_min = a.count.shape
    count = a.count.tolist()
    errors = a.errors.tolist()
    p95 = a.p95.tolist()
    first = a.first.tolist()
    for m in range(n_min):
        minute_ms = a.epoch_ms + m * MINUTE_MS
        for k in range(n_keys):
            if first[k] > m:
                continue
            key = a.keys[k]
            c = count[k][m]
            yield {
                "key": key.key,
                "tenant": key.tenant,
                "region": key.region,
                "unit": key.unit,
                "minute_ms": minute_ms,
                "count": c,
                "errors": errors[k][m],
                "p95_latency_ms": p95[k][m] if c else None,
            }


def encode_row(row: dict) -> bytes:
    return orjson.dumps(row)


def checked(rows: Iterable[dict]) -> Iterator[dict]:
    last: dict[str, int] = {}
    for row in rows:
        key, minute_ms = row["key"], row["minute_ms"]
        if key in last and minute_ms != last[key] + MINUTE_MS:
            raise ValueError(f"{key}: minute {minute_ms} after {last[key]}")
        last[key] = minute_ms
        yield row

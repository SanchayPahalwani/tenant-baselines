"""Synthetic eval traffic model (spec 5.2): expected requests per key and minute, with nuisance."""

from dataclasses import dataclass

import numpy as np

from tenant_baselines.traffic import shape_array
from tenant_baselines.world import World

EPOCH_MS = 1_767_571_200_000  # Monday 2026-01-05 00:00 UTC
MINUTE_MS = 60_000
WEEK_MS = 7 * 24 * 60 * MINUTE_MS
WEEKS = 5
MINUTES = WEEKS * 7 * 24 * 60
REPLAY_END_MS = EPOCH_MS + WEEKS * WEEK_MS

_MIN_PER_DAY = 1440
_MIN_PER_WEEK = 7 * _MIN_PER_DAY
# Local days from the day before EPOCH: the westmost zone starts in it, the eastmost ends on day 35.
_N_DAYS = 37
_DAY_SIGMA = 0.15
_TREND_MAX = 0.03
_BURST_MEAN = 1.0
_BURST_FACTOR = 1.8
_BURST_MINUTES = 5


def week_range(w: int) -> tuple[int, int]:
    """Start and end (exclusive) in epoch ms of week w (1-based)."""
    return EPOCH_MS + (w - 1) * WEEK_MS, EPOCH_MS + w * WEEK_MS


@dataclass(frozen=True)
class TrafficParams:
    amplitude: float = 6.0
    nuisance: bool = True
    weeks: int = WEEKS


@dataclass
class Rates:
    base: np.ndarray  # [key, minute] expected requests, before any scenario
    peak: np.ndarray  # [key] weekday peak, requests per minute
    start: np.ndarray  # [key] first minute index the key exists


@dataclass
class Effects:
    volume: np.ndarray
    error_add: np.ndarray
    latency: np.ndarray


def no_effects(n_keys: int, n_minutes: int) -> Effects:
    shape_ = (n_keys, n_minutes)
    return Effects(np.ones(shape_), np.zeros(shape_), np.ones(shape_))


def error_prob(load: np.ndarray, *, error_add: np.ndarray | float) -> np.ndarray:
    return np.minimum(1.0, 0.003 + 0.002 * np.asarray(load) + error_add)


def latency_median(
    base_ms: np.ndarray | float, load: np.ndarray, *, latency: np.ndarray | float
) -> np.ndarray:
    return base_ms * (1 + 0.6 * np.asarray(load) ** 2) * latency


def _nuisance(
    seed: int, tenant_index: int, tz_offset_h: int, n_minutes: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per-minute day factor, trend and burst multiplier of one tenant.

    Draw order from default_rng([seed, 1, tenant_index]): the trend slope, 37 day factors,
    37 burst counts, then each burst's start minute (one scalar draw per burst, day by day).
    """
    rng = np.random.default_rng([seed, 1, tenant_index])
    slope = rng.uniform(-_TREND_MAX, _TREND_MAX)
    day_factors = rng.lognormal(0.0, _DAY_SIGMA, _N_DAYS)
    counts = rng.poisson(_BURST_MEAN, _N_DAYS)

    m = np.arange(n_minutes)
    local = m + tz_offset_h * 60
    day_index = local // _MIN_PER_DAY + 1  # 0 is the day before EPOCH
    day = day_factors[day_index]
    trend = (1.0 + slope) ** (m / _MIN_PER_WEEK)

    burst = np.ones(n_minutes)
    for j in range(_N_DAYS):
        for _ in range(int(counts[j])):
            local_start = (j - 1) * _MIN_PER_DAY + int(rng.integers(0, _MIN_PER_DAY))
            s = local_start - tz_offset_h * 60  # minute index since EPOCH
            burst[max(s, 0) : max(s + _BURST_MINUTES, 0)] = _BURST_FACTOR
    return day, trend, burst


def rates(world: World, params: TrafficParams, seed: int) -> Rates:
    n_min = params.weeks * _MIN_PER_WEEK
    if params.weeks > WEEKS:
        raise ValueError(f"at most {WEEKS} weeks are modelled: {params.weeks}")
    n_keys = len(world.keys)
    per_tenant: dict[str, int] = {}
    for k in world.keys:
        per_tenant[k.tenant] = per_tenant.get(k.tenant, 0) + 1

    m = np.arange(n_min)
    base = np.zeros((n_keys, n_min))
    peak = np.zeros(n_keys)
    start = np.zeros(n_keys, dtype=np.int64)
    for i, t in enumerate(world.tenants):
        idx = [j for j, k in enumerate(world.keys) if k.tenant == t.name]
        if not idx:
            continue
        if t.steady:
            level = np.ones(n_min)
        else:
            local = m + t.tz_offset_h * 60
            hour = (local % _MIN_PER_DAY) / 60.0
            weekday = (local // _MIN_PER_DAY) % 7
            level = shape_array(hour, weekday, params.amplitude)
        if params.nuisance:
            day, trend, burst = _nuisance(seed, i, t.tz_offset_h, n_min)
            level = level * day * trend * burst
        first = (t.start_week - 1) * _MIN_PER_WEEK
        level = np.where(m >= first, level, 0.0)
        for j in idx:
            k = world.keys[j]
            peak[j] = k.peak_rpm if k.peak_rpm is not None else t.peak_rpm / per_tenant[t.name]
            start[j] = first
            base[j] = peak[j] * level
    return Rates(base, peak, start)

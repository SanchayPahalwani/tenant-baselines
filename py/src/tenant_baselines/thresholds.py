"""Fit the static opponents' thresholds from masked calibration minutes (spec 4.6).

Synthetic data only. The same script writes the eval's thresholds and the live demo's.
"""

import argparse
import json
from pathlib import Path

import numpy as np

from tenant_baselines.evalmodel import (
    EPOCH_MS,
    MINUTE_MS,
    REPLAY_END_MS,
    TrafficParams,
    no_effects,
    rates,
    week_range,
)
from tenant_baselines.scenarios import Scenario
from tenant_baselines.simulate import Aggregates, simulate
from tenant_baselines.world import build_world

# setting, percentile for the up signals, percentile for volume (it fires on low values)
PERCENTILES = (
    ("p99", 99.0, 1.0),
    ("p99.9", 99.9, 0.1),
    ("p99.97", 99.97, 0.03),
    ("p99.99", 99.99, 0.01),
)
SIGNALS = ("error_rate", "latency_p95", "volume_5m")
MIN_COUNT = 20  # minimum evidence for error rate and latency
VOLUME_WINDOW = 5  # minutes in the volume sum
MASK_AFTER_MINUTES = 4  # the 5-minute sum carries a scenario this long past its end
MIN_VALUES = 1000  # a key needs this many values for a signal to get its own threshold
_MINUTES_PER_WEEK = 7 * 1440


def _masked(a: Aggregates, scenarios: list[Scenario]) -> np.ndarray:
    """[k, m] true where a labelled scenario (any but the Monday peak) covers key k."""
    n_keys, n_min = a.count.shape
    index = {key.key: k for k, key in enumerate(a.keys)}
    minute_ms = a.epoch_ms + np.arange(n_min) * MINUTE_MS
    out = np.zeros((n_keys, n_min), dtype=bool)
    for s in scenarios:
        if s.type == "monday_peak":
            continue
        # end_ms is exclusive; the mask reaches end + 4 min inclusive.
        hit = (minute_ms >= s.start_ms) & (minute_ms <= s.end_ms + MASK_AFTER_MINUTES * MINUTE_MS)
        for key in s.keys:
            out[index[key]] |= hit
    return out


def calibration_values(
    a: Aggregates, scenarios: list[Scenario], first_week: int, last_week: int
) -> dict[str, list[np.ndarray]]:
    """Per signal, one array per key, from the minutes of the weeks that the mask leaves."""
    lo = int((week_range(first_week)[0] - a.epoch_ms) // MINUTE_MS)
    hi = int((week_range(last_week)[1] - a.epoch_ms) // MINUTE_MS)
    if lo < 0 or hi > a.count.shape[1]:
        raise ValueError(f"weeks {first_week}-{last_week} are outside the aggregates")
    masked = _masked(a, scenarios)
    out: dict[str, list[np.ndarray]] = {s: [] for s in SIGNALS}
    for k in range(len(a.keys)):
        count = a.count[k]
        m = np.arange(a.count.shape[1])
        keep = ~masked[k] & (m >= lo) & (m < hi)
        enough = keep & (count >= MIN_COUNT)
        out["error_rate"].append(a.errors[k][enough] / count[enough])
        out["latency_p95"].append(a.p95[k][enough])
        ring = np.cumsum(count)
        sums = ring - np.concatenate((np.zeros(VOLUME_WINDOW, dtype=ring.dtype), ring))[: len(ring)]
        warm = keep & (m - (VOLUME_WINDOW - 1) >= a.first[k])
        out["volume_5m"].append(sums[warm].astype(float))
    return out


def level(values: np.ndarray, pct: float, signal: str) -> dict | None:
    if len(values) == 0:
        return None
    fire = float(np.percentile(values, pct, method="linear"))
    resolve = (float(np.median(values)) + fire) / 2
    ok = resolve >= fire if signal == "volume_5m" else resolve <= fire
    if not ok:
        raise ValueError(f"{signal}: percentile {pct} puts resolve on the wrong side of fire")
    return {"fire": fire, "resolve": resolve}


def _levels(values: dict[str, np.ndarray], pcts: dict[str, float]) -> dict:
    out = {}
    for signal in SIGNALS:
        lv = level(values[signal], pcts[signal], signal)
        if lv is not None:
            out[signal] = lv
    return out


def fit(
    a: Aggregates,
    scenarios: list[Scenario],
    first_week: int,
    last_week: int,
    valid_from_ms: int,
    valid_to_ms: int | None,
) -> list[dict]:
    per_key = calibration_values(a, scenarios, first_week, last_week)
    pooled = {s: np.concatenate(per_key[s]) for s in SIGNALS}
    sets = []
    for setting, up, low in PERCENTILES:
        pcts = {"error_rate": up, "latency_p95": up, "volume_5m": low}
        global_levels = _levels(pooled, pcts)
        keyed = {}
        for k, key in enumerate(a.keys):
            own = {}
            for signal in SIGNALS:
                if len(per_key[signal][k]) >= MIN_VALUES:
                    own[signal] = level(per_key[signal][k], pcts[signal], signal)
            if own:
                keyed[key.key] = own
        for detector, keys in (("global_static", {}), ("per_key_static", keyed)):
            sets.append(
                {
                    "detector": detector,
                    "setting": setting,
                    "valid_from_ms": valid_from_ms,
                    "valid_to_ms": valid_to_ms,
                    "global": global_levels,
                    "per_key": keys,
                }
            )
    return sets


def eval_thresholds(a: Aggregates, scenarios: list[Scenario]) -> dict:
    w4, w5 = week_range(4), week_range(5)
    week4 = fit(a, scenarios, 1, 3, w4[0], w4[1])
    week5 = fit(a, scenarios, 1, 4, w5[0], REPLAY_END_MS)
    return {"sets": week4 + week5}


def demo_thresholds() -> dict:
    """Same model and world as the demo's generator; the draws differ (its timeline starts at the
    wall clock)."""
    world = build_world("demo", 1)
    r = rates(world, TrafficParams(nuisance=False, weeks=3), 1)
    a = simulate(world, r, no_effects(len(world.keys), r.base.shape[1]), 1)
    assert a.epoch_ms == EPOCH_MS
    sets = fit(a, [], 1, 3, 0, None)
    return {"sets": [s for s in sets if s["setting"] == "p99.9"]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--demo", action="store_true", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    text = json.dumps(demo_thresholds(), indent=2, sort_keys=True, allow_nan=False)
    Path(args.out).write_text(text + "\n")


if __name__ == "__main__":
    main()

"""The delay benchmark's schedule, labels and measures. Synthetic data only.

Incidents are error spikes on single keys. A page matches an incident by the eval's rule
(`scoring.MATCH_GRACE_MS`): the earliest `firing` row of scope `tenant` on the incident's key and
signal, with `start <= event_time <= end + grace`. Pure functions over lists and dicts.
"""

import numpy as np

from tenant_baselines.incidents import Incident
from tenant_baselines.scoring import MATCH_GRACE_MS, nearest_rank
from tenant_baselines.world import World

__all__ = ["MATCH_GRACE_MS"]

SETTLE_MS = 300_000
SPACING_MS = 180_000
REGION_OFFSET_MS = 60_000
DURATION_MS = 360_000
MAGNITUDE = 0.20
TAIL_MS = 180_000
SIGNAL = "error_rate"
MIN_MATCHED = 0.9
_N_REGIONS = 3
_JITTER_S = 60

# Configured, not measured. Each value is read from the code it names.
DESIGN = {
    "watermark_bound_s": 20,  # Watermarks.BOUND
    "idleness_s": 60,  # Watermarks.LIVE_IDLENESS
    "fire_after_minutes": 3,  # AlertStateMachine.FIRE_AFTER
    "hold_s": 120,  # RegionGrouper.HOLD_MS
    "group_wait_s": 30,  # infra/alertmanager/alertmanager.yml, group_wait
}


def _region_of(key: str) -> str:
    return key.split("|")[1]


def schedule(world: World, t0_ms: int, n: int, seed: int) -> list[Incident]:
    """Incident j is in region j % 3, the region's incident i = j // 3, on the region's
    i % len(keys)-th key. Start: t0 + settle + i * spacing + region * offset + jitter seconds."""
    regions = [r.name for r in world.regions]
    by_region = {r: [k.key for k in world.keys if k.region == r] for r in regions}
    jitter = np.random.default_rng([seed, 7]).integers(0, _JITTER_S, n)
    out = []
    for j in range(n):
        region = regions[j % _N_REGIONS]
        i = j // _N_REGIONS
        keys = by_region[region]
        start = (
            t0_ms
            + SETTLE_MS
            + i * SPACING_MS
            + (j % _N_REGIONS) * REGION_OFFSET_MS
            + int(jitter[j]) * 1000
        )
        out.append(
            Incident("error_spike", start, start + DURATION_MS, MAGNITUDE, key=keys[i % len(keys)])
        )
    return out


def labels(incidents: list[Incident], world: World, t0_ms: int) -> dict:
    return {
        "data": "synthetic",
        "t0_ms": t0_ms,
        "incidents": [
            {
                "id": j,
                "key": inc.key,
                "region": _region_of(inc.key),
                "signal": SIGNAL,
                "start_ms": inc.start_ms,
                "end_ms": inc.end_ms,
            }
            for j, inc in enumerate(incidents)
        ],
    }


def run_end_ms(labels: dict) -> int:
    return max(i["end_ms"] for i in labels["incidents"]) + TAIL_MS


def first_appends(records: list[tuple[int, dict]]) -> list[tuple[int, dict]]:
    """One record per (alert_id, state): the one with the earliest append time."""
    best: dict[tuple[str, str], tuple[int, dict]] = {}
    for at, row in records:
        ident = (row["alert_id"], row["state"])
        if ident not in best or at < best[ident][0]:
            best[ident] = (at, row)
    return sorted(best.values(), key=lambda r: (r[0], r[1]["alert_id"], r[1]["state"]))


def summary(values_s: list[float]) -> dict:
    if not values_s:
        return {"n": 0, "p50": None, "p95": None, "min": None, "max": None}
    return {
        "n": len(values_s),
        "p50": round(nearest_rank(values_s, 0.50), 1),
        "p95": round(nearest_rank(values_s, 0.95), 1),
        "min": round(min(values_s), 1),
        "max": round(max(values_s), 1),
    }


def pairs(records: list[tuple[int, dict]]) -> list[tuple[str, str]]:
    return sorted({(r["detector"], r["setting"]) for _, r in records if r["state"] == "firing"})


def enough_matched(m: dict) -> bool:
    return m["matched"] >= MIN_MATCHED * m["incidents"]


def _in_window(row: dict, inc: dict) -> bool:
    return (
        row["key"] == inc["key"]
        and row["signal"] == inc["signal"]
        and inc["start_ms"] <= row["event_time_ms"] <= inc["end_ms"] + MATCH_GRACE_MS
    )


def measure(labels: dict, records: list[tuple[int, dict]], detector: str, setting: str) -> dict:
    t0 = labels["t0_ms"]
    mine = [
        (at, r)
        for at, r in first_appends(records)
        if r["state"] == "firing" and r["detector"] == detector and r["setting"] == setting
    ]
    tenant_rows = [(at, r) for at, r in mine if r["scope"] == "tenant"]
    missed, hits = [], []
    for inc in labels["incidents"]:
        cands = [(r["event_time_ms"], at, r) for at, r in tenant_rows if _in_window(r, inc)]
        if not cands:
            missed.append(inc["id"])
            continue
        _, at, row = min(cands, key=lambda c: (c[0], c[1]))
        hits.append((inc, at, row))
    settled = t0 + SETTLE_MS
    unmatched = sum(
        1
        for _, r in tenant_rows
        if r["event_time_ms"] >= settled and not any(_in_window(r, i) for i in labels["incidents"])
    )
    regional = sum(1 for _, r in mine if r["scope"] == "region" and r["event_time_ms"] >= settled)
    ttp = [(at - inc["start_ms"]) / 1000 for inc, at, _ in hits]
    return {
        "incidents": len(labels["incidents"]),
        "matched": len(hits),
        "missed": missed,
        "unmatched_firing_rows": unmatched,
        "regional_firing_rows": regional,
        "pipeline_delay_s": summary([(at - r["event_time_ms"]) / 1000 for _, at, r in hits]),
        "time_to_page_s": summary(ttp),
        "emit_delay_s": summary(
            [(r["emitted_at_ms"] - r["event_time_ms"]) / 1000 for *_, r in hits]
        ),
        "sink_delay_s": summary([(at - r["emitted_at_ms"]) / 1000 for _, at, r in hits]),
        "under_five_minutes": sum(1 for v in ttp if v < 300),
    }

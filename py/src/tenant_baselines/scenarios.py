"""Pre-registered incident scenarios (spec 5.4): placement, effects and labels.

The magnitudes, counts, windows and eligibility rules below are the spec's table, verbatim.
Synthetic data only.
"""

from dataclasses import asdict, dataclass

import numpy as np
import orjson

from tenant_baselines.evalmodel import (
    EPOCH_MS,
    MINUTE_MS,
    MINUTES,
    WEEKS,
    Effects,
    Rates,
    no_effects,
    week_range,
)
from tenant_baselines.world import World

_DAY = 1440
_WEEK = 7 * _DAY
_END_MARGIN = 60  # a scenario ends at least this many minutes before its week ends
_MIN_GAP = 120  # minutes between two scenarios on one key
_MAX_TRIES = 10_000
_MAX_ATTEMPTS = 20  # placements of one week before PlacementError
_SCORED_FROM = 4

# Window lengths in minutes.
_LENGTH = {
    "regional_outage": 40,
    "drop_work": 60,
    "drop_night": 60,
    "latency_creep": 420,
    "offhours_latency": 30,
    "error_burst": 20,
    "cold_start_burst": 20,
    "launch_surge": 180,
    "holiday_dip": _DAY,
}
_SIGNAL = {
    "regional_outage": "error_rate",
    "drop_work": "volume_5m",
    "drop_night": "volume_5m",
    "latency_creep": "latency_p95",
    "offhours_latency": "latency_p95",
    "error_burst": "error_rate",
    "cold_start_burst": "error_rate",
}
# Widest first. Counts are per scored week.
_PLAN = (
    ("holiday_dip", 1),
    ("latency_creep", 2),
    ("launch_surge", 1),
    ("drop_work", 1),
    ("drop_night", 1),
    ("regional_outage", 1),
    ("offhours_latency", 2),
    ("error_burst", 2),
    ("cold_start_burst", 1),
)
_FIT_PLAN = (("drop_work", 1), ("error_burst", 1))

_OUTAGE_ERROR = 0.15
_OUTAGE_LATENCY = 1.5
_DROP = 0.3
_CREEP_RAMP_MINUTES = 360
_CREEP_PEAK = 2.5
_OFFHOURS_LATENCY = 2.5
_BURST_ERROR = 0.08
_SURGE = 3.0
_HOLIDAY = 0.35
_MONDAY_START_H = 8
_MONDAY_MINUTES = 180


class PlacementError(Exception):
    pass


@dataclass(frozen=True)
class Scenario:
    id: str
    type: str
    week: int
    start_ms: int
    end_ms: int  # exclusive
    keys: tuple[str, ...]
    region: str | None
    expected_signal: str | None
    should_page: bool
    scored: bool
    note: str


@dataclass
class _Subjects:
    keys: list[tuple[int, ...]]  # key indexes of each candidate subject
    names: list[str | None]  # the region of each subject, for the regional types
    valid: np.ndarray | None  # [subject, start minute]; None means any start


def _all_true(b: np.ndarray, length: int) -> np.ndarray:
    """out[r, s] is true when b[r, s : s + length] is all true."""
    rows, n = b.shape
    c = np.zeros((rows, n + 1), dtype=np.int32)
    np.cumsum(b, axis=1, dtype=np.int32, out=c[:, 1:])
    out = np.zeros((rows, n), dtype=bool)
    out[:, : n - length + 1] = (c[:, length:] - c[:, : n + 1 - length]) == length
    return out


class _Context:
    """Per-key local time and the eligibility masks, built once for a (world, rates) pair."""

    def __init__(self, world: World, rates: Rates):
        self.world = world
        self.base = rates.base
        self.n_keys, self.n = rates.base.shape
        tenants = {t.name: t for t in world.tenants}
        self.tenant_of = [k.tenant for k in world.keys]
        tz = np.array([tenants[t].tz_offset_h for t in self.tenant_of])
        loc = np.arange(self.n)[None, :] + tz[:, None] * 60
        self.minute_of_day = loc % _DAY
        self.weekday = (loc // _DAY) % 7
        self.tz = tz
        self.dedicated = {u.name for u in world.units if u.dedicated_for}
        self._cache: dict[str, _Subjects] = {}

    def subjects(self, kind: str) -> _Subjects:
        if kind not in self._cache:
            self._cache[kind] = getattr(self, f"_{kind}")()
        return self._cache[kind]

    def _per_key(self, idx: list[int], valid: np.ndarray) -> _Subjects:
        return _Subjects([(i,) for i in idx], [None] * len(idx), valid[idx])

    def _working(self, length: int) -> np.ndarray:
        md = self.minute_of_day
        return (self.weekday < 5) & (md >= 9 * 60) & (md + length <= 17 * 60)

    def _by_tenant(self, valid: np.ndarray) -> _Subjects:
        names = sorted(set(self.tenant_of))
        groups = [[i for i, t in enumerate(self.tenant_of) if t == name] for name in names]
        return _Subjects(
            [tuple(g) for g in groups],
            [None] * len(groups),
            np.array([valid[g[0]] for g in groups]),
        )

    def _dedicated_keys(self) -> list[int]:
        return [i for i, k in enumerate(self.world.keys) if k.unit in self.dedicated]

    def _regional_outage(self) -> _Subjects:
        return self._by_region(None)

    def _by_region(self, valid: np.ndarray | None) -> _Subjects:
        groups, names = [], []
        for r in self.world.regions:
            idx = tuple(i for i, k in enumerate(self.world.keys) if k.region == r.name)
            if idx:
                groups.append(idx)
                names.append(r.name)
        return _Subjects(groups, names, valid)

    def _holiday_dip(self) -> _Subjects:
        return self._by_region(None)

    def _drop_work(self) -> _Subjects:
        return self._per_key(self._dedicated_keys(), self._working(60))

    def _drop_night(self) -> _Subjects:
        md = self.minute_of_day
        night = (md >= 20 * 60) | (md < 7 * 60) | (self.weekday >= 5)
        cs = np.cumsum(self.base, axis=1)
        sum5 = cs.copy()
        sum5[:, 5:] -= cs[:, :-5]
        valid = _all_true(night, 60) & _all_true(sum5 >= 15, 60)
        return self._per_key(self._dedicated_keys(), valid)

    def _latency_creep(self) -> _Subjects:
        length = _LENGTH["latency_creep"]
        ok = self.base >= 20
        groups, valid = [], []
        for u in self.world.units:
            idx = [i for i, k in enumerate(self.world.keys) if k.unit == u.name]
            if not idx:
                continue
            cs = np.zeros(self.n + 1, dtype=np.int64)
            np.cumsum(ok[idx].sum(axis=0), out=cs[1:])
            row = np.zeros(self.n, dtype=bool)
            row[: self.n - length + 1] = 2 * (cs[length:] - cs[: self.n + 1 - length]) > (
                len(idx) * length
            )
            groups.append(tuple(idx))
            valid.append(row)
        return _Subjects(groups, [None] * len(groups), np.array(valid))

    def _offhours_latency(self) -> _Subjects:
        valid = (self.minute_of_day + 30 <= 6 * 60) & _all_true(self.base >= 25, 30)
        return self._per_key(list(range(self.n_keys)), valid)

    def _error_burst(self) -> _Subjects:
        shared = [i for i, k in enumerate(self.world.keys) if k.unit not in self.dedicated]
        return self._per_key(shared, _all_true(self.base >= 30, 20))

    def _cold_start_burst(self) -> _Subjects:
        new = {t.name for t in self.world.tenants if t.start_week == _SCORED_FROM}
        idx = [i for i, k in enumerate(self.world.keys) if k.tenant in new]
        return _Subjects([(i,) for i in idx], [None] * len(idx), None)

    def _launch_surge(self) -> _Subjects:
        return self._by_tenant(self._working(_LENGTH["launch_surge"]))


def _free(occupied: list[list[tuple[int, int]]], keys: tuple[int, ...], s: int, e: int) -> bool:
    for k in keys:
        for a, b in occupied[k]:
            if s < b + _MIN_GAP and a < e + _MIN_GAP:
                return False
    return True


def _draw(
    kind: str,
    ctx: _Context,
    week: int,
    occupied: list[list[tuple[int, int]]],
    rng: np.random.Generator,
) -> tuple[int, int, str | None]:
    """Return (subject index, start minute, region) or raise LookupError."""
    sub = ctx.subjects(kind)
    length = _LENGTH[kind]
    if not sub.keys:
        raise LookupError(kind)
    lo = (week - 1) * _WEEK
    hi = week * _WEEK - _END_MARGIN - length  # last start, inclusive
    for _ in range(_MAX_TRIES):
        j = int(rng.integers(len(sub.keys)))
        if kind == "holiday_dip":
            # The region's local day: draw the day, then reject a weekend or a day cut by the week.
            off = _region_offset(ctx.world, sub.names[j]) * 60
            d = int(rng.integers(lo // _DAY - 1, hi // _DAY + 2))
            s = d * _DAY - off
            if d % 7 >= 5 or s < lo or s > hi:
                continue
        else:
            s = int(rng.integers(lo, hi + 1))
            if sub.valid is not None and not sub.valid[j, s]:
                continue
        if _free(occupied, sub.keys[j], s, s + length):
            return j, s, sub.names[j]
    raise LookupError(kind)


def _region_offset(world: World, name: str | None) -> int:
    return next(r.utc_offset_h for r in world.regions if r.name == name)


def _scenario(
    ctx: _Context, kind: str, week: int, n: int, j: int, s: int, region: str | None
) -> Scenario:
    sub = ctx.subjects(kind)
    keys = tuple(ctx.world.keys[i].key for i in sub.keys[j])
    note = ""
    if kind == "regional_outage":
        utc_h = (s % _DAY) // 60
        local_h = ((s + _region_offset(ctx.world, region) * 60) % _DAY) // 60
        note = f"hour {utc_h:02d} UTC, hour {local_h:02d} {region} local"
    return Scenario(
        id=f"w{week}-{kind}-{n}",
        type=kind,
        week=week,
        start_ms=EPOCH_MS + s * MINUTE_MS,
        end_ms=EPOCH_MS + (s + _LENGTH[kind]) * MINUTE_MS,
        keys=keys,
        region=region if kind in ("regional_outage", "holiday_dip") else None,
        expected_signal=_SIGNAL.get(kind),
        should_page=kind in _SIGNAL,
        scored=week >= _SCORED_FROM,
        note=note,
    )


def _monday_peaks(ctx: _Context, week: int) -> list[Scenario]:
    ws, _ = week_range(week)
    out = []
    for n, tz in enumerate(sorted({t.tz_offset_h for t in ctx.world.tenants}), start=1):
        names = {t.name for t in ctx.world.tenants if t.tz_offset_h == tz}
        keys = tuple(k.key for k in ctx.world.keys if k.tenant in names)
        if not keys:
            continue
        start = ws + (_MONDAY_START_H * 60 - tz * 60) * MINUTE_MS
        out.append(
            Scenario(
                id=f"w{week}-monday_peak-{n}",
                type="monday_peak",
                week=week,
                start_ms=start,
                end_ms=start + _MONDAY_MINUTES * MINUTE_MS,
                keys=keys,
                region=None,
                expected_signal=None,
                should_page=False,
                scored=True,
                note=f"time zone UTC{tz:+d}",
            )
        )
    return out


def place(world: World, rates: Rates, seed: int) -> list[Scenario]:
    """Place the scenarios of weeks 1 to 5. Fit weeks (1-3) get a burst and a drop only."""
    if rates.base.shape[1] != MINUTES:
        raise ValueError(f"place needs {WEEKS} weeks of rates, got {rates.base.shape[1]} minutes")
    ctx = _Context(world, rates)
    rng = np.random.default_rng([seed, 2])
    occupied: list[list[tuple[int, int]]] = [[] for _ in world.keys]
    out: list[Scenario] = []
    for week in range(1, WEEKS + 1):
        plan = _PLAN if week >= _SCORED_FROM else _FIT_PLAN
        failed = ""
        for _ in range(_MAX_ATTEMPTS):
            trial = [list(o) for o in occupied]
            placed: list[Scenario] = []
            try:
                for kind, count in plan:
                    for n in range(1, count + 1):
                        failed = kind
                        j, s, region = _draw(kind, ctx, week, trial, rng)
                        sc = _scenario(ctx, kind, week, n, j, s, region)
                        for i in ctx.subjects(kind).keys[j]:
                            trial[i].append((s, s + _LENGTH[kind]))
                        placed.append(sc)
            except LookupError:
                continue
            occupied = trial
            out += placed
            if week >= _SCORED_FROM:
                out += _monday_peaks(ctx, week)
            break
        else:
            raise PlacementError(
                f"cannot place {failed} in week {week} with seed {seed}"
                f" after {_MAX_ATTEMPTS} attempts"
            )
    return out


def apply(scenarios: list[Scenario], world: World, n_minutes: int) -> Effects:
    eff = no_effects(len(world.keys), n_minutes)
    index = {k.key: i for i, k in enumerate(world.keys)}
    for sc in scenarios:
        a = (sc.start_ms - EPOCH_MS) // MINUTE_MS
        b = min((sc.end_ms - EPOCH_MS) // MINUTE_MS, n_minutes)
        if a >= b:
            continue
        rows = [index[k] for k in sc.keys]
        for r in rows:
            _apply_one(eff, sc.type, r, a, b)
    return eff


def _apply_one(eff: Effects, kind: str, r: int, a: int, b: int) -> None:
    if kind == "regional_outage":
        eff.error_add[r, a:b] += _OUTAGE_ERROR
        eff.latency[r, a:b] *= _OUTAGE_LATENCY
    elif kind in ("drop_work", "drop_night"):
        eff.volume[r, a:b] *= _DROP
    elif kind == "latency_creep":
        i = np.arange(b - a)
        ramp = np.where(i < _CREEP_RAMP_MINUTES, 1 + 1.5 * i / _CREEP_RAMP_MINUTES, _CREEP_PEAK)
        eff.latency[r, a:b] *= ramp
    elif kind == "offhours_latency":
        eff.latency[r, a:b] *= _OFFHOURS_LATENCY
    elif kind in ("error_burst", "cold_start_burst"):
        eff.error_add[r, a:b] += _BURST_ERROR
    elif kind == "launch_surge":
        eff.volume[r, a:b] *= _SURGE
    elif kind == "holiday_dip":
        eff.volume[r, a:b] *= _HOLIDAY


_META_KEYS = ("seed", "amplitude", "world", "epoch_ms", "replay_end_ms")


def write_labels(path, scenarios: list[Scenario], meta: dict) -> None:
    """Write the labels as JSON. The meta always carries the text "synthetic" under "data"."""
    missing = [k for k in _META_KEYS if k not in meta]
    if missing:
        raise ValueError(f"labels meta is missing {missing}")
    if meta.get("data", "synthetic") != "synthetic":
        raise ValueError(f"labels meta data must be 'synthetic': {meta['data']!r}")
    meta = {**meta, "data": "synthetic"}
    doc = {"meta": meta, "scenarios": [asdict(s) for s in scenarios]}
    with open(path, "wb") as f:
        f.write(orjson.dumps(doc, option=orjson.OPT_INDENT_2))


def read_labels(path) -> tuple[list[Scenario], dict]:
    with open(path, "rb") as f:
        doc = orjson.loads(f.read())
    scenarios = [Scenario(**{**d, "keys": tuple(d["keys"])}) for d in doc["scenarios"]]
    return scenarios, doc["meta"]

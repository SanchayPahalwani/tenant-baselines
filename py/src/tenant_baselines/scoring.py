"""Score an alert log against the scenario labels (spec 5.5 and 5.6). Synthetic data only.

Pure functions over lists and dicts. A page is a `firing` row; recall for selection and the
headline is expected-signal recall (owner's decision), with any-signal recall beside it.
"""

import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field

from tenant_baselines.evalmodel import week_range
from tenant_baselines.scenarios import Scenario

_STATIC = ("p99", "p99.9", "p99.97", "p99.99")
FAMILIES = {
    "seasonal": ("k3", "k4", "k5", "k6"),
    "per_key_static": _STATIC,
    "global_static": _STATIC,
}
MATCH_GRACE_MS = 600_000
_MINUTE_MS = 60_000
_DAYS = 7
_Z = 1.959963984540054
_DROPPED = ("emitted_at_ms", "run_id")
_ORDER = ("event_time_ms", "region", "detector", "setting", "signal", "key", "state", "alert_id")
_CLOSED = ("resolved", "absorbed")


def normalise(rows: Iterable[dict], replay_end_ms: int) -> tuple[list[dict], dict]:
    """The alert log's normal form, and the counts of dropped duplicates and clipped rows."""
    seen = set()
    out = []
    duplicates = clipped = 0
    for row in rows:
        if row["event_time_ms"] > replay_end_ms:
            clipped += 1
            continue
        slim = {k: v for k, v in row.items() if k not in _DROPPED}
        ident = tuple(sorted(slim.items()))
        if ident in seen:
            duplicates += 1
            continue
        seen.add(ident)
        out.append(slim)
    out.sort(key=lambda r: tuple(r[k] for k in _ORDER))
    return out, {"duplicates": duplicates, "clipped": clipped}


@dataclass(frozen=True)
class Page:
    detector: str
    setting: str
    scope: str
    key: str
    region: str
    signal: str
    starts_at_ms: int
    event_time_ms: int
    alert_id: str


def pages(rows: Iterable[dict], week_start_ms: int, week_end_ms: int) -> list[Page]:
    out = []
    for r in rows:
        if r["state"] != "firing" or not week_start_ms <= r["starts_at_ms"] < week_end_ms:
            continue
        region = r["region"] or (r["key"] if r["scope"] == "region" else "")
        out.append(
            Page(
                r["detector"],
                r["setting"],
                r["scope"],
                r["key"],
                region,
                r["signal"],
                r["starts_at_ms"],
                r["event_time_ms"],
                r["alert_id"],
            )
        )
    out.sort(key=lambda p: p.event_time_ms)
    return out


def _key_region(key: str) -> str:
    return key.split("|")[1]


def matches(p: Page, s: Scenario, expected_only: bool = False) -> bool:
    if not s.start_ms <= p.event_time_ms <= s.end_ms + MATCH_GRACE_MS:
        return False
    if expected_only and p.signal != s.expected_signal:
        return False
    if p.scope == "tenant":
        return p.key in s.keys
    if p.scope == "region":
        return any(_key_region(k) == p.region for k in s.keys)
    return False


def wilson(k: int, n: int) -> tuple[float, float]:
    if n == 0:
        return 0.0, 1.0
    p = k / n
    z2 = _Z * _Z
    denom = 1 + z2 / n
    centre = (p + z2 / (2 * n)) / denom
    half = _Z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n)) / denom
    return max(0.0, centre - half), min(1.0, centre + half)


def nearest_rank(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(1, math.ceil(q * len(ordered))) - 1]


@dataclass
class SettingScore:
    detector: str
    setting: str
    week: int
    n_should_page: int
    detected_any: int
    detected_expected: int
    pages: int
    matched_pages: int
    false_pages: int
    false_pages_per_day: float
    refires: int
    pages_per_detected: float | None
    median_delay_min: float | None
    delays_by_type: dict[str, list[float]] = field(default_factory=dict)
    scenarios: list[dict] = field(default_factory=list)


def _delay_min(p: Page, s: Scenario) -> float:
    return (p.event_time_ms - s.start_ms) / _MINUTE_MS


def score(
    pages_of_setting: list[Page],
    scenarios_of_week: list[Scenario],
    detector: str,
    setting: str,
    week: int,
) -> SettingScore:
    ps = sorted(pages_of_setting, key=lambda p: p.event_time_ms)
    should = [s for s in scenarios_of_week if s.should_page]
    matched_ids = set()  # indexes into ps
    for i, p in enumerate(ps):
        if any(matches(p, s) for s in should):
            matched_ids.add(i)
    false_idx = [i for i in range(len(ps)) if i not in matched_ids]

    entries = []
    delays_by_type: dict[str, list[float]] = {}
    refires = 0
    for s in scenarios_of_week:
        hits = [p for p in ps if matches(p, s)]
        if not s.should_page:
            entries.append(
                {
                    "id": s.id,
                    "type": s.type,
                    "should_page": False,
                    "detected_any": False,
                    "detected_expected": False,
                    "delay_min": None,
                    "delay_any_min": None,
                    "pages": len(hits),
                    "regional_formed": any(p.scope == "region" for p in hits),
                    "false_pages_during": sum(1 for i in false_idx if matches(ps[i], s)),
                }
            )
            continue
        expected = [p for p in hits if matches(p, s, expected_only=True)]
        seen = set()
        extra = 0
        for p in hits:
            ident = (p.scope, p.key, p.signal)
            extra += ident in seen
            seen.add(ident)
        refires += extra
        delay = _delay_min(expected[0], s) if expected else None
        if delay is not None:
            delays_by_type.setdefault(s.type, []).append(delay)
        entries.append(
            {
                "id": s.id,
                "type": s.type,
                "should_page": True,
                "detected_any": bool(hits),
                "detected_expected": bool(expected),
                "delay_min": delay,
                "delay_any_min": _delay_min(hits[0], s) if hits else None,
                "pages": len(hits),
                "regional_formed": any(p.scope == "region" for p in hits),
                "false_pages_during": 0,
                "refires": extra,
            }
        )
    detected_any = sum(e["detected_any"] for e in entries if e["should_page"])
    detected_expected = sum(e["detected_expected"] for e in entries if e["should_page"])
    all_delays = [d for v in delays_by_type.values() for d in v]
    return SettingScore(
        detector=detector,
        setting=setting,
        week=week,
        n_should_page=len(should),
        detected_any=detected_any,
        detected_expected=detected_expected,
        pages=len(ps),
        matched_pages=len(matched_ids),
        false_pages=len(false_idx),
        false_pages_per_day=len(false_idx) / _DAYS,
        refires=refires,
        pages_per_detected=len(matched_ids) / detected_any if detected_any else None,
        median_delay_min=nearest_rank(all_delays, 0.5),
        delays_by_type=delays_by_type,
        scenarios=entries,
    )


@dataclass(frozen=True)
class Selection:
    setting: str
    branch: str  # "reached_8_of_10" or "highest_recall"


_TARGET = 8


def select(scores: list[SettingScore]) -> Selection:
    """Spec 5.6 on one family's week-4 scores, by expected-signal recall."""
    order = FAMILIES[scores[0].detector]

    def tie_key(s: SettingScore):
        delay = math.inf if s.median_delay_min is None else s.median_delay_min
        return (s.false_pages_per_day, delay, -order.index(s.setting))

    reached = [s for s in scores if s.detected_expected >= _TARGET]
    if reached:
        return Selection(min(reached, key=tie_key).setting, "reached_8_of_10")
    best = min(scores, key=lambda s: (-s.detected_expected, *tie_key(s)))
    return Selection(best.setting, "highest_recall")


def _open_alerts(rows: list[dict]) -> int:
    fired = {r["alert_id"] for r in rows if r["state"] == "firing"}
    closed = {r["alert_id"] for r in rows if r["state"] in _CLOSED}
    return len(fired - closed)


def summarise(rows: Iterable[dict], scenarios: list[Scenario], meta: dict) -> dict:
    """Weeks 4 and 5: every family and setting's score, the selections, the week-5 result."""
    log, counts = normalise(rows, meta["replay_end_ms"])
    out: dict = {"weeks": {}, "selection": {}, "selected_week5": {}}
    for week in (4, 5):
        start, end = week_range(week)
        week_pages = pages(log, start, end)
        week_scenarios = [s for s in scenarios if s.week == week]
        fams = {}
        for fam, settings in FAMILIES.items():
            fams[fam] = {
                st: score(
                    [p for p in week_pages if p.detector == fam and p.setting == st],
                    week_scenarios,
                    fam,
                    st,
                    week,
                )
                for st in settings
            }
        out["weeks"][str(week)] = fams
    for fam in FAMILIES:
        sel = select(list(out["weeks"]["4"][fam].values()))
        out["selection"][fam] = sel
        out["selected_week5"][fam] = out["weeks"]["5"][fam][sel.setting]
    out["open_alerts"] = _open_alerts(log)
    out["normalise"] = counts
    out["meta"] = meta
    out["synthetic"] = True
    return _plain(out)


def _plain(v):
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    if hasattr(v, "__dataclass_fields__"):
        return _plain(asdict(v))
    return v

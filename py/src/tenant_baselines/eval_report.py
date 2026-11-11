"""Render docs/results.md from the run summaries (spec 5.5 and 5.6). Synthetic data only.

`render` is pure: summaries, run log, check-paths result, freeze file and machine facts in,
text out. `main` reads the files and measures the machine.
"""

import argparse
import importlib.metadata
import json
import statistics
import subprocess
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines.scoring import FAMILIES, nearest_rank, wilson

REPO = Path(__file__).resolve().parents[3]
MAIN_AMPLITUDE = 6.0
AMPLITUDES = (1.5, 6.0, 12.0)
STATEMENT = (
    "These results test the implementation under the stated traffic assumption;"
    " they are not evidence about real traffic."
)
CURVE_HEADING = "Every setting on the test week (chosen after seeing the test week; for reference)"
DEV_LINE = "DEV SEEDS, not results"
_DAYS = 7
_WEEK_MS = 7 * 86_400_000
_HOUR_MS = 3_600_000
_DAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_REGION_OFFSET_H = {"us-east": -5, "eu-central": 1, "ap-southeast": 8}
_TYPES = (
    ("regional_outage", True),
    ("drop_work", True),
    ("drop_night", True),
    ("latency_creep", True),
    ("offhours_latency", True),
    ("error_burst", True),
    ("cold_start_burst", True),
    ("launch_surge", False),
    ("holiday_dip", False),
)
_FIXED_VERSIONS = (
    ("Flink", "2.2.1 (Java 17)"),
    ("Flink Kafka connector", "5.0.0-2.2"),
    ("Kafka", "4.4.0"),
    ("ClickHouse", "26.8"),
    ("Grafana", "13.2.5"),
    ("Alertmanager", "v0.34.2"),
)


def _versions() -> tuple[tuple[str, str], ...]:
    v = sys.version_info
    return (
        *_FIXED_VERSIONS,
        ("Python", f"{v.major}.{v.minor}.{v.micro}"),
        ("numpy", importlib.metadata.version("numpy")),
    )


# ---------------------------------------------------------------- loading


def load_summaries(directory: str | Path) -> list[dict]:
    """Every summary under `directory`, ordered by (amplitude, seed, run id).

    Two layouts: `<dir>/<run id>/summary.json`, and `<dir>/<name>.json`. A `labels.json` beside
    a summary (or `<name>.labels.json`) is attached as `_labels`, keyed by scenario id.
    """
    d = Path(directory)
    paths = sorted(d.glob("*/summary.json")) + sorted(
        p for p in d.glob("*.json") if not p.name.endswith(".labels.json")
    )
    out = []
    for p in paths:
        s = json.loads(p.read_text())
        if p.name == "summary.json":
            labels = p.with_name("labels.json")
        else:
            labels = p.with_name(p.stem + ".labels.json")
        if labels.is_file():
            scen = json.loads(labels.read_text())["scenarios"]
            s["_labels"] = {x["id"]: x for x in scen}
        out.append(s)
    out.sort(key=lambda s: (s["meta"]["amplitude"], s["meta"]["seed"], s.get("run_id", "")))
    return out


def latest_per_cell(summaries: list[dict]) -> list[dict]:
    """One summary per (kind, seed, amplitude): the one with the latest run id.

    A dev cell may have been run more than once; the report must not count it twice.
    """
    best: dict[tuple, dict] = {}
    for s in summaries:
        m = s["meta"]
        cell = (m["kind"], m["seed"], m["amplitude"])
        if cell not in best or s.get("run_id", "") > best[cell].get("run_id", ""):
            best[cell] = s
    return sorted(best.values(), key=lambda s: (s["meta"]["amplitude"], s["meta"]["seed"]))


def load_run_log(path: str | Path) -> list[dict]:
    """The run log (JSON lines, oldest first); an absent file is an empty log."""
    p = Path(path)
    if not p.is_file():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------- numbers


def _at(summaries: Iterable[dict], amplitude: float) -> list[dict]:
    return [s for s in summaries if s["meta"]["amplitude"] == amplitude]


def _spread(values: list[float]) -> dict:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def _pooled(k: int, n: int) -> dict:
    lo, hi = wilson(k, n)
    return {"k": k, "n": n, "lo": lo, "hi": hi}


def headline(summaries: list[dict], amplitude: float) -> dict:
    """Per family, over the seeds at `amplitude`: the selections and the week-5 result."""
    runs = _at(summaries, amplitude)
    out = {}
    for fam in FAMILIES:
        if not runs:
            continue
        sel = [s["selection"][fam] for s in runs]
        w5 = [s["selected_week5"][fam] for s in runs]
        n_seeds = len(runs)
        out[fam] = {
            "per_seed_n": w5[0]["n_should_page"],
            "seeds": [
                {"seed": s["meta"]["seed"], "setting": a["setting"], "branch": a["branch"]}
                for s, a in zip(runs, sel, strict=True)
            ],
            "false_per_day": _spread([x["false_pages_per_day"] for x in w5]),
            "pooled_false_per_day": sum(x["false_pages"] for x in w5) / (_DAYS * n_seeds),
            "detected_expected": _spread([x["detected_expected"] for x in w5])
            | _pooled(sum(x["detected_expected"] for x in w5), sum(x["n_should_page"] for x in w5)),
            "detected_any": _spread([x["detected_any"] for x in w5])
            | _pooled(sum(x["detected_any"] for x in w5), sum(x["n_should_page"] for x in w5)),
        }
    return out


def curve(summaries: list[dict], amplitude: float) -> list[dict]:
    """Every setting of every family on week 5, pooled over the seeds at `amplitude`."""
    runs = _at(summaries, amplitude)
    out = []
    for fam, settings in FAMILIES.items():
        for st in settings:
            if not runs:
                continue
            cells = [s["weeks"]["5"][fam][st] for s in runs]
            days = _DAYS * len(runs)
            n = sum(c["n_should_page"] for c in cells)
            out.append(
                {
                    "family": fam,
                    "setting": st,
                    "pages_per_day": sum(c["pages"] for c in cells) / days,
                    "false_per_day": sum(c["false_pages"] for c in cells) / days,
                    "expected": _pooled(sum(c["detected_expected"] for c in cells), n),
                    "any": _pooled(sum(c["detected_any"] for c in cells), n),
                    "matched": sum(c["matched_pages"] for c in cells),
                    "pages": sum(c["pages"] for c in cells),
                }
            )
    return out


def by_scenario_type(summaries: list[dict], amplitude: float) -> list[dict]:
    """Each family's selected setting on week 5, by scenario type, pooled over the seeds."""
    runs = _at(summaries, amplitude)
    out = []
    for fam in FAMILIES:
        for typ, should_page in _TYPES:
            entries = [
                e for s in runs for e in s["selected_week5"][fam]["scenarios"] if e["type"] == typ
            ]
            if not entries:
                continue
            delays = [e["delay_min"] for e in entries if e["delay_min"] is not None]
            detected_any = [e for e in entries if e["detected_any"]]
            row = {
                "family": fam,
                "type": typ,
                "should_page": should_page,
                "scenarios": len(entries),
                "expected": _pooled(sum(e["detected_expected"] for e in entries), len(entries)),
                "any": _pooled(len(detected_any), len(entries)),
                "delay_n": len(delays),
                "p50": nearest_rank(delays, 0.5),
                "p95": nearest_rank(delays, 0.95),
                "pages_per_detected": (
                    sum(e["pages"] for e in detected_any) / len(detected_any)
                    if detected_any
                    else None
                ),
                "refires": sum(e["refires"] for e in entries),
                "regional_formed": sum(e["regional_formed"] for e in entries),
                "false_during": sum(e["false_pages_during"] for e in entries),
            }
            out.append(row)
    return out


def _utc_hour(start_ms: int, epoch_ms: int, offset_h: int = 0) -> str:
    into = (start_ms - epoch_ms + offset_h * _HOUR_MS) % _WEEK_MS
    day, rest = divmod(into, 86_400_000)
    minutes = rest // 60_000
    return f"{_DAY_NAMES[day]} {minutes // 60:02d}:{minutes % 60:02d}"


def outage_rows(summaries: list[dict], amplitude: float) -> list[dict]:
    """The week-5 regional outage of each run: region, hour, and each family's result."""
    out = []
    for s in _at(summaries, amplitude):
        sid = next(
            (
                e["id"]
                for e in s["selected_week5"]["seasonal"]["scenarios"]
                if e["type"] == "regional_outage"
            ),
            None,
        )
        if sid is None:
            continue
        label = s.get("_labels", {}).get(sid)
        row = {"seed": s["meta"]["seed"], "id": sid, "region": None, "utc": None, "local": None}
        if label:
            region = label["keys"][0].split("|")[1]
            epoch = s["meta"]["epoch_ms"]
            row["region"] = region
            row["utc"] = _utc_hour(label["start_ms"], epoch)
            row["local"] = _utc_hour(label["start_ms"], epoch, _REGION_OFFSET_H[region])
        for fam in FAMILIES:
            e = next(x for x in s["selected_week5"][fam]["scenarios"] if x["id"] == sid)
            row[fam] = {
                "setting": s["selection"][fam]["setting"],
                "detected_expected": e["detected_expected"],
                "delay_min": e["delay_min"],
                "regional_formed": e["regional_formed"],
            }
        out.append(row)
    return out


# ---------------------------------------------------------------- text


def _g(x: float) -> str:
    return f"{x:g}"


def _recall(d: dict) -> str:
    return f"{d['k']}/{d['n']} ({d['lo']:.2f}–{d['hi']:.2f})"


def _spread_text(d: dict, digits: int | None = None) -> str:
    def f(x: float) -> str:
        return _g(x) if digits is None else f"{x:.{digits}f}"

    return f"{f(d['median'])} ({f(d['min'])}–{f(d['max'])})"


def _num(x: float | None, digits: int = 2) -> str:
    return "none" if x is None else f"{x:.{digits}f}"


def _table(head: list[str], rows: list[list[str]]) -> list[str]:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return out + [""]


def _fam(f: str) -> str:
    return f.replace("_", " ")


def _headline_section(summaries: list[dict], amplitude: float) -> list[str]:
    h = headline(summaries, amplitude)
    if not h:
        return [f"No runs at {_g(amplitude)}:1 yet.", ""]
    seeds = len(next(iter(h.values()))["seeds"])
    rows = [
        [
            _fam(f),
            _spread_text(v["detected_expected"]) + f" of {v['per_seed_n']}",
            _recall(v["detected_expected"]),
            _recall(v["detected_any"]),
            _spread_text(v["false_per_day"], 2),
            f"{v['pooled_false_per_day']:.2f}",
        ]
        for f, v in h.items()
    ]
    lines = [
        f"Seeds at this amplitude: {seeds}. Each seed picks its own setting on week 4,"
        " and the result below is that setting on week 5. Recall counts a scenario when a page"
        " is on its expected signal; the any-signal column counts a page on any signal."
        " Intervals are Wilson intervals at the 95 percent level, written as fractions.",
        "",
    ]
    lines += _table(
        [
            "Family",
            "Expected-signal recall per seed: median (range)",
            "Expected-signal recall, pooled k/n (interval)",
            "Any-signal recall, pooled k/n (interval)",
            "False pages per day per seed: median (range)",
            "False pages per day, pooled",
        ],
        rows,
    )
    sel_rows = [
        [_fam(f), str(x["seed"]), x["setting"], x["branch"]]
        for f, v in h.items()
        for x in v["seeds"]
    ]
    lines += ["Selected setting and branch of the selection rule, per family and seed:", ""]
    lines += _table(["Family", "Seed", "Setting", "Branch"], sel_rows)
    return lines


def _sweep_section(summaries: list[dict]) -> list[str]:
    rows = []
    for amp in AMPLITUDES:
        h = headline(summaries, amp)
        for f in FAMILIES:
            if f not in h:
                rows.append([f"{_g(amp)}:1", _fam(f), "no runs yet", "", "", ""])
                continue
            v = h[f]
            rows.append(
                [
                    f"{_g(amp)}:1",
                    _fam(f),
                    str(len(v["seeds"])),
                    _recall(v["detected_expected"]),
                    _recall(v["detected_any"]),
                    f"{_spread_text(v['false_per_day'], 2)};"
                    f" pooled {v['pooled_false_per_day']:.2f}",
                ]
            )
    return _table(
        [
            "Day/night amplitude",
            "Family",
            "Seeds",
            "Expected-signal recall, pooled",
            "Any-signal recall, pooled",
            "False pages per day: median (range); pooled",
        ],
        rows,
    )


def _curve_section(summaries: list[dict]) -> list[str]:
    c = curve(summaries, MAIN_AMPLITUDE)
    if not c:
        return [f"No runs at {_g(MAIN_AMPLITUDE)}:1 yet.", ""]
    rows = [
        [
            _fam(r["family"]),
            r["setting"],
            f"{r['pages_per_day']:.2f}",
            f"{r['false_per_day']:.2f}",
            _recall(r["expected"]),
            _recall(r["any"]),
            f"{r['matched']}/{r['pages']}" if r["pages"] else "no pages",
        ]
        for r in c
    ]
    return [
        f"Main setting ({_g(MAIN_AMPLITUDE)}:1), all seeds pooled. Pages and false pages are per"
        " day. Precision is matched pages over all pages.",
        "",
        *_table(
            [
                "Family",
                "Setting",
                "Pages per day",
                "False pages per day",
                "Expected-signal recall",
                "Any-signal recall",
                "Precision (matched/all)",
            ],
            rows,
        ),
    ]


def _type_section(summaries: list[dict]) -> list[str]:
    rows_in = by_scenario_type(summaries, MAIN_AMPLITUDE)
    if not rows_in:
        return [f"No runs at {_g(MAIN_AMPLITUDE)}:1 yet.", ""]
    rows = []
    for r in rows_in:
        if r["should_page"]:
            delay = (
                f"{_num(r['p50'], 1)} / {_num(r['p95'], 1)} (n={r['delay_n']})"
                if r["delay_n"]
                else "none (n=0)"
            )
            rows.append(
                [
                    _fam(r["family"]),
                    r["type"],
                    _recall(r["expected"]),
                    _recall(r["any"]),
                    delay,
                    _num(r["pages_per_detected"]),
                    str(r["refires"]),
                    f"{r['regional_formed']}/{r['scenarios']}",
                    "n/a",
                ]
            )
        else:
            rows.append(
                [
                    _fam(r["family"]),
                    f"{r['type']} (should not page; {r['scenarios']}"
                    f" {'scenario' if r['scenarios'] == 1 else 'scenarios'})",
                    "n/a",
                    "n/a",
                    "n/a",
                    "n/a",
                    "n/a",
                    f"{r['regional_formed']}/{r['scenarios']}",
                    str(r["false_during"]),
                ]
            )
    lines = [
        f"Each family's selected setting on week 5 at {_g(MAIN_AMPLITUDE)}:1, pooled over seeds."
        " Delay is minutes from scenario start to the first page on the expected signal: p50 /"
        " p95 by nearest rank, with the count. Pages per detected scenario counts every matching"
        " page of a scenario that was detected on any signal. The regional column is the number"
        " of scenarios in whose window a regional alert formed.",
        "",
        *_table(
            [
                "Family",
                "Scenario type",
                "Expected-signal recall",
                "Any-signal recall",
                "Delay p50 / p95, minutes",
                "Pages per detected",
                "Re-fires",
                "Regional alert formed",
                "False pages during",
            ],
            rows,
        ),
    ]
    out = outage_rows(summaries, MAIN_AMPLITUDE)
    lines += ["Regional outages on week 5, with their hour (the hour is reported, spec 5.4):", ""]
    if not out:
        return lines + ["No regional outage rows yet.", ""]
    orows = []
    for o in out:
        cells = []
        for f in FAMILIES:
            x = o[f]
            hit = "expected-signal page" if x["detected_expected"] else "no expected-signal page"
            delay = f", delay {_num(x['delay_min'], 1)} min" if x["delay_min"] is not None else ""
            reg = ", regional alert" if x["regional_formed"] else ""
            cells.append(f"{x['setting']}: {hit}{delay}{reg}")
        orows.append(
            [
                str(o["seed"]),
                o["region"] or "not recorded",
                f"{o['utc']} UTC" if o["utc"] else "not recorded",
                f"{o['local']} local" if o["local"] else "not recorded",
                *cells,
            ]
        )
    lines += _table(
        ["Seed", "Region", "Start (UTC)", "Start (local)", *[_fam(f) for f in FAMILIES]], orows
    )
    return lines


def check_passed(cp: dict) -> tuple[bool, str]:
    """Passed only when both paths agree and the log is not empty of what it should compare."""
    reasons = []
    if not cp.get("alerts_identical"):
        reasons.append("the alert logs differ")
    if not cp.get("aggregates_identical"):
        reasons.append("the aggregates differ")
    if not cp.get("alert_rows", 0) > 0:
        reasons.append("the alert log is empty")
    if not cp.get("region_firing_rows", 0) > 0:
        reasons.append("no regional alert fired, so grouping was not compared")
    return not reasons, "; ".join(reasons)


def _check_section(cp: dict | None) -> list[str]:
    if cp is None:
        return ["`make check-paths` has not been run (no result file).", ""]
    ok, why = check_passed(cp)
    verdict = "passed" if ok else f"failed: {why}"
    secs = cp.get("seconds", {})
    return [
        f"`make check-paths` {verdict}.",
        "",
        f"A synthetic world (`{cp.get('world', 'unknown')}`) went through the job once as"
        f" {cp.get('events', 0):,} events and once as {cp.get('aggregate_rows', 0):,} minute"
        f" aggregates. The alert logs compared equal: {str(cp.get('alerts_identical')).lower()}."
        f" The aggregates compared equal: {str(cp.get('aggregates_identical')).lower()}."
        f" The alert log holds {cp.get('alert_rows', 0):,} rows, {cp.get('region_firing_rows', 0)}"
        " of them regional firing rows.",
        "",
        f"Run at commit `{str(cp.get('commit', ''))[:7]}`"
        f" on {cp.get('date_utc', 'an unrecorded date')}."
        f" Seconds: events job {secs.get('events_job', 'n/a')}, aggregates job"
        f" {secs.get('aggregates_job', 'n/a')}, total {secs.get('total', 'n/a')}.",
        "",
    ]


def _events_minutes(cp: dict | None) -> str:
    secs = (cp or {}).get("seconds", {}).get("events_job")
    return "an unmeasured time" if secs is None else f"about {round(secs / 60)} minutes"


def _method_section(
    summaries: list[dict], freeze: dict | None, machine: dict, check_paths: dict | None
) -> list[str]:
    seeds = sorted({s["meta"]["seed"] for s in summaries})
    kinds = sorted({s["meta"]["kind"] for s in summaries})
    clipped = sum(s["normalise"]["clipped"] for s in summaries)
    dups = sum(s["normalise"]["duplicates"] for s in summaries)
    open_ = sum(s["open_alerts"] for s in summaries)
    if freeze:
        official = ", ".join(str(x) for x in freeze["official_seeds"])
        frozen = (
            f"The settings were frozen at commit `{str(freeze.get('commit', 'unrecorded'))[:7]}`."
        )
    else:
        official = "not frozen yet"
        frozen = "No freeze file exists, so nothing here is frozen."
    changes = machine.get("changes") or []
    lines = [
        "- **Seeds.** The code enforces the seed protocol. A dev run refuses any seed outside 7,"
        " 11 and 13. An official run needs the freeze file, uses only the seeds listed in it,"
        " and refuses a seed and amplitude that already has an official run."
        f" Official seeds: {official}. {frozen}"
        f" This render holds runs of kind {', '.join(kinds) or 'none'} at seeds"
        f" {', '.join(str(s) for s in seeds) or 'none'}.",
        "- **The world.** 3 regions, 24 tenants (8 per region) and 48 keys: 6 shared units"
        " and 6 dedicated units. The 18 shared tenants have a key on each of the two shared units"
        " of their region (36 keys). The 6 dedicated tenants have their own unit and one key on"
        " the region's first shared unit (12 keys). The spec says about 60 keys, but its two"
        " sentences give 42. Reaching 60 would need a third shared unit or keys across regions,"
        " and cross-region keys would put half the tenants partly out of their time zone, so the"
        " world has 48. Five tenants keep the hours of another region's time zone.",
        "- **Recall.** Every recall figure is expected-signal recall unless the column says"
        " any-signal. A scenario counts when a page lands on the signal the scenario is meant to"
        " move. The selection rule on week 4 uses the same count.",
        "- **The thresholds mask.** The static thresholds are fitted with every labelled scenario"
        " masked out, except the Monday peak. Launch surges and holiday dips are masked too. The"
        " mask covers the scenario's keys from its start to 4 minutes past its end, because the"
        " five-minute sum carries a scenario's effect for 4 more minutes.",
        f"- **The clip at the replay end.** The replay ends at the end of week 5. Alert rows with"
        f" an event time past it are dropped before scoring: {clipped} rows over all runs here."
        f" Alerts still open at the end count as pages: {open_} over all runs here."
        f" Duplicate rows dropped: {dups}.",
        "- **Inclusive fire comparison.** The shared state machine fires at or past the fire"
        " level (spec 4.5). The values are discrete (bucketed p95, error counts over request"
        " counts, whole 5-minute sums), so a static threshold is breached on calibration"
        " minutes more often than its nominal percentile. Static false pages therefore run"
        " somewhat higher than the percentile alone suggests. The comparison was not changed.",
        "- **What check-paths shows.** It shows that the events path and the aggregates path give"
        " the same alert log. It does not show that the detectors are right: both paths share the"
        " thresholds, every operator from the minute aggregates on, and the random draws.",
        "- **Replay mode can fail.** Replay mode aligns the watermarks of the Kafka partitions."
        " That can hit a race in the Kafka connector (FLINK-39728). It is fixed in connector 5.1.0,"
        " which is not released yet; this repo pins 5.0.0-2.2. A run that hits it fails and gives"
        " no output, so no wrong result is possible, and check-paths can be run again. The"
        " official runs use aggregates mode. It has one partition and no alignment, so it cannot"
        " hit the race.",
        f"- **Events-path speed.** The events path of check-paths is bound by the alignment:"
        f" {_events_minutes(check_paths)} for five weeks. The alignment drift was not changed.",
        "- **Single-key creeps.** A latency creep is placed on one unit. A dedicated unit is a"
        " unit, so a creep can land on a unit with a single key.",
        "- **No Monday holiday in two regions.** eu-central and ap-southeast never get a holiday"
        " dip on a Monday. Their local Monday starts before the UTC week, so that local day cannot"
        " lie inside the week.",
        "- **The traffic model and the spreads were kept as specified.** The owner reserved the"
        " decision at the freeze and then delegated it to the builder. The builder left spec 5.2"
        " and 4.3 as written. Changing them after seeing dev results would be tuning.",
        "- **Changes to the job or the run mechanics.** "
        + (
            "; ".join(changes)
            if changes
            else "None were passed to this render (alignment drift, scorer writes and"
            " state backend are as in the commit above)."
        ),
        "",
    ]
    return lines


def _log_section(run_log: list[dict]) -> list[str]:
    if not run_log:
        return ["No entries yet.", ""]
    lines = []
    for i, e in enumerate(run_log, 1):
        lines.append(
            f"{i}. `{str(e.get('commit', ''))[:7]}`, {e.get('date', 'undated')}:"
            f" {e.get('what', '')} Why: {e.get('why', 'not recorded')}"
            f" Old headline: {e.get('old', 'none')}. New headline: {e.get('new', 'none')}."
        )
    return lines + [""]


def _gib(n: int | float | None) -> str:
    return "not measured" if n is None else f"{n / 2**30:.1f} GiB"


def _setup_section(machine: dict) -> list[str]:
    host = (
        f"{machine.get('cpu', 'not measured')}, {machine.get('cores', 'n/a')} cores,"
        f" {_gib(machine.get('memory_bytes'))} memory"
    )
    vm = (
        f"Docker {machine.get('docker_version', 'not measured')},"
        f" {machine.get('docker_cpus', 'n/a')} CPUs,"
        f" {_gib(machine.get('docker_memory_bytes'))} memory"
    )
    lines = [
        f"- Commit: `{str(machine.get('commit', 'not measured'))[:7]}`",
        "- Command: `make eval`",
        f"- Date: {machine.get('date', 'not measured')}",
        f"- Machine: {host}",
        f"- Docker VM: {vm}",
        "- Versions: " + "; ".join(f"{k} {v}" for k, v in _versions()),
        "",
    ]
    return lines


def render(
    summaries: list[dict],
    run_log: list[dict],
    check_paths: dict | None,
    freeze: dict | None,
    machine: dict,
) -> str:
    out = [
        "# Results",
        "",
        STATEMENT + " All data here is synthetic: generated traffic, generated incidents,"
        " and a generated world.",
        "",
        "## Setup",
        "",
        *_setup_section(machine),
        f"## Headline: {_g(MAIN_AMPLITUDE)}:1 day/night amplitude, selected settings on the test"
        " week",
        "",
        *_headline_section(summaries, MAIN_AMPLITUDE),
        "## Sweep over the day/night amplitude",
        "",
        *_sweep_section(summaries),
        f"## {CURVE_HEADING}",
        "",
        *_curve_section(summaries),
        "## By scenario type",
        "",
        *_type_section(summaries),
        "## Event-path equivalence check",
        "",
        *_check_section(check_paths),
        "## Method notes",
        "",
        *_method_section(summaries, freeze, machine, check_paths),
        "## Run log",
        "",
        *_log_section(run_log),
    ]
    return "\n".join(out).rstrip("\n") + "\n"


# ---------------------------------------------------------------- main


def _run(*cmd: str) -> str | None:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip()


def measure_machine() -> dict:
    cpu = _run("sysctl", "-n", "machdep.cpu.brand_string")
    cores = _run("sysctl", "-n", "hw.ncpu")
    mem = _run("sysctl", "-n", "hw.memsize")
    docker = _run("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}")
    d = docker.split("|") if docker else [None, None, None]
    return {
        "commit": _run("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "date": datetime.now(UTC).strftime("%Y-%m-%d"),
        "cpu": cpu,
        "cores": cores,
        "memory_bytes": int(mem) if mem else None,
        "docker_version": d[0],
        "docker_cpus": d[1],
        "docker_memory_bytes": int(d[2]) if d[2] else None,
    }


def _load_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.is_file() else None


def check_official(summaries: list[dict]) -> None:
    """Official renders take official runs only, one per (seed, amplitude)."""
    seen: dict[tuple, str] = {}
    for s in summaries:
        m, rid = s["meta"], s.get("run_id", "")
        if m["kind"] != "off" or (rid and not rid.startswith("off-")):
            raise ValueError(f"run {rid or '?'} is not an official run (kind {m['kind']!r})")
        cell = (m["seed"], m["amplitude"])
        if cell in seen:
            raise ValueError(
                f"seed {cell[0]} at amplitude {cell[1]:g} appears twice: {seen[cell]} and {rid}"
            )
        seen[cell] = rid


def dedupe_dev(summaries: list[dict]) -> tuple[list[dict], list[str]]:
    """The newest run per cell, and a line for each dropped run (a warning if it differs)."""
    kept = latest_per_cell(summaries)
    by_cell = {(s["meta"]["seed"], s["meta"]["amplitude"]): s for s in kept}
    notes = []
    for s in summaries:
        k = by_cell[(s["meta"]["seed"], s["meta"]["amplitude"])]
        if s is k:
            continue
        notes.append(f"dropped {s.get('run_id', '?')} (kept {k.get('run_id', '?')})")
        if s["selected_week5"] != k["selected_week5"]:
            notes.append(
                f"WARNING: {s.get('run_id', '?')} and {k.get('run_id', '?')} differ in"
                " selected_week5; the same seed should give the same result"
            )
    return kept, notes


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Render docs/results.md from run summaries")
    ap.add_argument("--summaries", default=str(REPO / "docs/results/summaries"))
    ap.add_argument("--out", default=str(REPO / "docs/results.md"))
    ap.add_argument("--dev", metavar="DIR", help="render the dev summaries under DIR instead")
    ap.add_argument("--change", action="append", default=[], help="a job or mechanics change")
    args = ap.parse_args(argv)
    machine = measure_machine() | {"changes": args.change}
    check = _load_json(REPO / "docs/results/check-paths.json")
    if args.dev:
        dev = [s for s in load_summaries(args.dev) if s["meta"]["kind"] == "dev"]
        summaries, notes = dedupe_dev(dev)
        for n in notes:
            print(f"eval-report: {n}")
        # Dev summaries sit one level down, in data/eval/dev-*/summary.json.
        text = DEV_LINE + "\n\n" + render(summaries, [], check, None, machine)
        out = Path(args.dev) / "dev-results.md"
    else:
        summaries = load_summaries(args.summaries)
        check_official(summaries)
        run_log = load_run_log(REPO / "docs/results/run-log.jsonl")
        freeze = _load_json(REPO / "eval/freeze.json")
        text = render(summaries, run_log, check, freeze, machine)
        out = Path(args.out)
    out.write_text(text)
    print(f"eval-report: wrote {out} ({len(summaries)} summaries)")


if __name__ == "__main__":
    main()

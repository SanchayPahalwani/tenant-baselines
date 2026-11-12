"""Render docs/results.md from the run summaries (spec 5.5 and 5.6). Synthetic data only.

`render` is pure: summaries, run log, check-paths result, freeze file, registry, machine facts
and the render's own commit and date in, text out. `main` reads the files, checks the official
records against each other, and reads the machine facts from `docs/results/machine.json` (it
measures and writes them only when that file is absent).
"""

import argparse
import hashlib
import importlib.metadata
import json
import statistics
import subprocess
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from tenant_baselines.freeze import DEV_SEEDS, DISPLAY_ONLY_PATHS, MEASURED_PATHS, planned
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
# The check-paths stages, run one after the other: their sum cannot exceed the total.
_CHECK_STAGES = ("generate", "produce", "events_job", "aggregates_job")
_RUN_STAGES = ("generate", "produce", "job")
_SLACK_S = 1.0  # each figure is rounded to 0.1 s
_TIE_BREAKS = (
    "the fewest false pages per day, then the lower median delay, then the less sensitive setting"
)


def _python_version() -> str:
    v = sys.version_info
    return f"{v.major}.{v.minor}.{v.micro}"


def _versions(machine: dict) -> tuple[tuple[str, str], ...]:
    """The fixed versions, and Python and numpy as recorded with the machine facts."""
    return (
        *_FIXED_VERSIONS,
        ("Python", machine.get("python") or _python_version()),
        ("numpy", machine.get("numpy") or importlib.metadata.version("numpy")),
    )


# ---------------------------------------------------------------- loading


def load_summaries(directory: str | Path) -> list[dict]:
    """Every summary under `directory`, ordered by (amplitude, seed, run id).

    Two layouts: `<dir>/<run id>/summary.json`, and `<dir>/<name>.json`. A `labels.json` beside
    a summary (or `<name>.labels.json`) is attached as `_labels`, keyed by scenario id, and the
    SHA-256 of the summary file as `_sha256`.
    """
    d = Path(directory)
    paths = sorted(d.glob("*/summary.json")) + sorted(
        p for p in d.glob("*.json") if not p.name.endswith(".labels.json")
    )
    out = []
    for p in paths:
        raw = p.read_bytes()
        s = json.loads(raw)
        s["_sha256"] = hashlib.sha256(raw).hexdigest()
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


def _load_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.is_file():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def load_run_log(path: str | Path) -> list[dict]:
    """The run log (JSON lines, oldest first); an absent file is an empty log."""
    return _load_jsonl(path)


def load_registry(path: str | Path) -> list[dict]:
    """`official-runs.jsonl` (JSON lines, oldest first); an absent file is an empty registry."""
    return _load_jsonl(path)


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


def headline_text(summaries: list[dict], amplitude: float) -> str:
    """The headline in one line: each family's pooled recall and false pages per day."""
    parts = []
    for i, (fam, v) in enumerate(headline(summaries, amplitude).items()):
        e = v["detected_expected"]
        unit = " false pages per day" if i == 0 else ""
        parts.append(f"{_fam(fam)} {e['k']}/{e['n']}, {v['pooled_false_per_day']:.2f}{unit}")
    return "; ".join(parts) or "no runs"


def matched_pages(summaries: list[dict], amplitude: float) -> dict[str, tuple[int, int]]:
    """Per family, over the seeds at `amplitude`: matched and all pages of the selected setting."""
    runs = _at(summaries, amplitude)
    if not runs:
        return {}
    return {
        fam: (
            sum(s["selected_week5"][fam]["matched_pages"] for s in runs),
            sum(s["selected_week5"][fam]["pages"] for s in runs),
        )
        for fam in FAMILIES
    }


def false_pages_sentence(summaries: list[dict], amplitude: float = MAIN_AMPLITUDE) -> str | None:
    """How many of the selected settings' pages matched a scenario, pooled at `amplitude`."""
    m = matched_pages(summaries, amplitude)
    if not m:
        return None
    items = [f"{k} of {n} ({_fam(f)})" for f, (k, n) in m.items()]
    items[0] = items[0].replace(" (", " pages matched a scenario (", 1)
    listed = ", ".join(items[:-1]) + " and " + items[-1] if len(items) > 1 else items[0]
    if all(2 * k < n for k, n in m.values()):
        return f"Most pages are false in every family: at the selected settings, {listed}."
    return f"At the selected settings, {listed}."


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
                "pages": e["pages"],
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
            n = x["pages"]
            pages = f", {n} page" + ("" if n == 1 else "s")
            cells.append(f"{x['setting']}: {hit}{delay}{reg}{pages}")
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


def timing_problem(cp: dict) -> str | None:
    """Why check-paths' seconds cannot be reported, or None when they can.

    The four stages run one after the other, so their sum cannot exceed the total. The total
    comes from the host's monotonic clock, which stops while the host sleeps; the job seconds come
    from Flink's wall clock, which does not. A total below the sum means the host slept."""
    secs = cp.get("seconds") or {}
    if any(not isinstance(secs.get(k), int | float) for k in (*_CHECK_STAGES, "total")):
        return "the file does not record every stage"
    stages = sum(secs[k] for k in _CHECK_STAGES)
    if secs["total"] + _SLACK_S < stages:
        return (
            f"the host slept during the run (total {secs['total']:.1f} s is less than the"
            f" stages' sum {stages:.1f} s)"
        )
    return None


def _check_section(cp: dict | None) -> list[str]:
    if cp is None:
        return ["`make check-paths` has not been run (no result file).", ""]
    ok, why = check_passed(cp)
    verdict = "passed" if ok else f"failed: {why}"
    secs = cp.get("seconds", {})
    problem = timing_problem(cp)
    bits = [f"run id `{cp['run_id']}`"] if cp.get("run_id") else []
    if "dirty" in cp:
        bits.append("a dirty tree" if cp["dirty"] else "a clean tree")
    if cp.get("jar_sha256"):
        bits.append(f"jar SHA-256 `{cp['jar_sha256']}`")
    run = f" Recorded: {'; '.join(bits)}." if bits else ""
    timing = (
        f" Timings are not reported: {problem}."
        if problem
        else f" Seconds: events job {secs['events_job']}, aggregates job"
        f" {secs['aggregates_job']}, total {secs['total']}."
    )
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
        f" on {cp.get('date_utc', 'an unrecorded date')}.{run}{timing}",
        "",
    ]


def _events_minutes(cp: dict | None) -> str:
    if cp is None or timing_problem(cp):
        return "an unmeasured time"
    return f"about {round(cp['seconds']['events_job'] / 60)} minutes"


def _and(items: Iterable) -> str:
    xs = [str(x) for x in items]
    return ", ".join(xs[:-1]) + " and " + xs[-1] if len(xs) > 1 else "".join(xs)


def _dirty_note(summaries: list[dict]) -> list[str]:
    off = [s for s in summaries if s["meta"]["kind"] == "off"]
    n = sum(1 for s in off if s.get("dirty"))
    if not n:
        return []
    count = (
        f"Each of the {len(off)} official summaries records `dirty: true`."
        if n == len(off)
        else f"{n} of {len(off)} official summaries record `dirty: true`."
    )
    return [
        f"- **The `dirty` flag.** {count} The flag counts the driver's own registry line, written"
        " before the run. The driver refuses any other change (`eval_official.preflight`)."
    ]


def _false_pages_note(summaries: list[dict]) -> list[str]:
    sentence = false_pages_sentence(summaries)
    if sentence is None:
        return []
    cause = (
        " The traffic model's unlabelled bursts and per-day level factors (spec 5.2) are the"
        " expected cause; this eval does not measure the split."
        if sentence.startswith("Most")
        else ""
    )
    return [
        f"- **Matched and false pages.** On week 5 at {_g(MAIN_AMPLITUDE)}:1, pooled over the"
        f" seeds. {sentence}{cause}"
    ]


def _method_section(
    summaries: list[dict],
    freeze: dict | None,
    check_paths: dict | None,
    changes: list[str],
    render_date: str | None,
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
    released = (
        f"which is not released as of {render_date}"
        if render_date
        else "which was not released when this was rendered"
    )
    lines = [
        f"- **Seeds.** The code enforces the seed protocol. A dev run refuses any seed outside"
        f" {_and(DEV_SEEDS)}. An official run needs the freeze file, uses only the seeds listed"
        " in it, and refuses a seed and amplitude that already has an official run."
        f" Official seeds: {official}. {frozen}"
        f" This render holds runs of kind {', '.join(kinds) or 'none'} at seeds"
        f" {', '.join(str(s) for s in seeds) or 'none'}.",
        *_dirty_note(summaries),
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
        "- **Selection ties.** Among the settings that reach 8 of 10 (or, when none does, among"
        f" those with the highest recall), the rule takes {_TIE_BREAKS}. Spec 5.6 names the"
        " first two; the third is the builder's call, recorded in `eval/freeze.json`.",
        *_false_pages_note(summaries),
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
        f"- **Measured code.** Changed {MEASURED_CODE_CHANGE_DATE}, after the official runs: the"
        " dashboard files under infra/grafana/dashboards may differ from the freeze commit, and"
        " the Setup line names each one that does. The dashboards only draw; the eval reads pages"
        " from the alerts topic. Everything else under infra/, and docker-compose.yml, is still"
        " compared.",
        "- **Replay mode can fail.** Replay mode aligns the watermarks of the Kafka partitions."
        " That can hit a race in the Kafka connector (FLINK-39728). It is fixed in connector"
        f" 5.1.0, {released}; this repo pins 5.0.0-2.2. A run that hits it fails and gives"
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


def _sentence(x: object) -> str:
    s = str(x).strip()
    return s if s.endswith((".", "!", "?")) else s + "."


def _log_section(run_log: list[dict], first_entry: dict | None = None) -> list[str]:
    """The derived first entry, then the file's entries. A file entry's typed `old` and `new`
    are not printed: every headline here comes from the summaries."""
    lines = []
    if first_entry:
        e = first_entry
        lines.append(
            f"{len(lines) + 1}. `{e['commit'][:7]}`, {e['date']}: {_sentence(e['what'])}"
            f" Why: {_sentence(e['why'])} Old headline: {_sentence(e['old'])}"
            f" New headline: {_sentence(e['new'])}"
        )
    for e in run_log:
        sup = e.get("supersedes") or []
        replaces = (
            f" It supersedes {', '.join(sup)} when the re-run finishes;"
            " until then the earlier summary stays in `docs/results/summaries/`."
            if sup
            else ""
        )
        lines.append(
            f"{len(lines) + 1}. `{str(e.get('commit', ''))[:7]}`, {e.get('date', 'undated')}:"
            f" {_sentence(e.get('what', ''))} Why: {_sentence(e.get('why', 'not recorded'))}"
            f"{replaces} Old and new headlines not derived for this entry."
        )
    if not lines:
        return ["No entries yet.", ""]
    return lines + [""]


def _gib(x: int | float | None) -> str:
    return "not measured" if x is None else f"{x:.1f} GiB"


def _utc(at: str) -> datetime:
    return datetime.fromisoformat(at).astimezone(UTC)


def run_span(summaries: list[dict], registry: list[dict]) -> dict | None:
    """First start and last finish in the registry of the runs these summaries come from."""
    rids = {s.get("run_id") for s in summaries}
    mine = [e for e in registry if e.get("run_id") in rids and e.get("at")]
    starts = [_utc(e["at"]) for e in mine if e["event"] == "started"]
    ends = [_utc(e["at"]) for e in mine if e["event"] == "finished"]
    if not starts or not ends:
        return None
    first, last = min(starts), max(ends)
    return {"first": first, "last": last, "minutes": (last - first).total_seconds() / 60}


def _setup_section(
    summaries: list[dict],
    registry: list[dict],
    freeze: dict | None,
    machine: dict,
    rendered: dict,
) -> list[str]:
    commits = sorted({s["commit"] for s in summaries if s.get("commit")})
    rids = {s.get("run_id") for s in summaries}
    jars = sorted(
        {
            e["jar_sha256"]
            for e in registry
            if e.get("event") == "started" and e.get("run_id") in rids and e.get("jar_sha256")
        }
    )
    span = run_span(summaries, registry)
    stamp = "%Y-%m-%d %H:%M:%S UTC"
    runs = (
        f"first start {span['first']:{stamp}}, last finish {span['last']:{stamp}},"
        f" elapsed {span['minutes']:.1f} minutes (`docs/results/official-runs.jsonl`)"
        if span
        else "not recorded (no registry lines for these runs)"
    )
    host = (
        f"{machine.get('cpu', 'not measured')}, {machine.get('cores', 'n/a')} cores,"
        f" {_gib(machine.get('memory_gib'))} memory"
    )
    vm = (
        f"Docker {machine.get('docker_version', 'not measured')},"
        f" {machine.get('docker_cpus', 'n/a')} CPUs,"
        f" {_gib(machine.get('docker_memory_gib'))} memory"
    )
    frozen = (
        f"`{str(freeze['commit'])[:7]}`" if freeze and freeze.get("commit") else "not frozen yet"
    )
    lines = [
        "- Runs at commit: " + (", ".join(f"`{c[:7]}`" for c in commits) or "not recorded"),
        "- Command: `make eval`",
        f"- Settings frozen at commit: {frozen}",
        "- Jar SHA-256: " + (", ".join(f"`{j}`" for j in jars) or "not recorded"),
        f"- Runs: {runs}",
        f"- Machine: {host}",
        f"- Docker VM: {vm}",
    ]
    if machine.get("source"):
        lines.append(f"- Machine facts: {machine['source']}")
    lines.append("- Versions: " + "; ".join(f"{k} {v}" for k, v in _versions(machine)))
    lines.append(
        f"- Rendered at commit `{str(rendered.get('commit') or 'not recorded')[:7]}`"
        f" on {rendered.get('date') or 'an unrecorded date'}"
    )
    differing = rendered.get("measured_code_diff")
    if differing is not None:
        if not differing:
            state = "yes"
        elif display_only(differing):
            state = "yes, except display-only files: " + ", ".join(differing)
        else:
            state = "no"
        lines.append(f"- Measured code at the render commit equals the freeze commit: {state}")
    return lines + [""]


def render(
    summaries: list[dict],
    run_log: list[dict],
    check_paths: dict | None,
    freeze: dict | None,
    machine: dict,
    registry: list[dict] | None = None,
    first_entry: dict | None = None,
    rendered: dict | None = None,
    changes: Iterable[str] = (),
) -> str:
    """The whole document. `machine` holds the facts of the machine the runs ran on; `rendered`
    the render's own commit and date, and whether the measured code equals the freeze commit's."""
    registry = registry or []
    rendered = rendered or {}
    out = [
        "# Results",
        "",
        STATEMENT + " All data here is synthetic: generated traffic, generated incidents,"
        " and a generated world.",
        "",
        "## Setup",
        "",
        *_setup_section(summaries, registry, freeze, machine, rendered),
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
        *_method_section(summaries, freeze, check_paths, list(changes), rendered.get("date")),
        "## Run log",
        "",
        *_log_section(run_log, first_entry),
    ]
    return "\n".join(out).rstrip("\n") + "\n"


# ---------------------------------------------------------------- checks of the records


def _last_lines(registry: list[dict]) -> dict[tuple, dict]:
    last: dict[tuple, dict] = {}
    for e in registry:
        last[(e["seed"], e["amplitude"])] = e
    return last


def _check_timings(s: dict) -> None:
    t = s.get("timings") or {}
    if any(not isinstance(t.get(k), int | float) for k in (*_RUN_STAGES, "total")):
        raise ValueError(f"{s['run_id']}: the summary does not record every stage's seconds")
    stages = sum(t[k] for k in _RUN_STAGES)
    if t["total"] + _SLACK_S < stages:
        raise ValueError(
            f"{s['run_id']}: total {t['total']} s is less than the stages' sum {stages:.1f} s;"
            " the host slept during the run"
        )


def check_official(summaries: list[dict], registry: list[dict], freeze: dict | None) -> None:
    """Official renders take the registry's current runs only: one summary per pair the freeze
    plans, each the run of its pair's last `finished` line, all at one commit."""
    if freeze is None:
        raise ValueError("an official render needs eval/freeze.json")
    seen: dict[tuple, dict] = {}
    for s in summaries:
        m, rid = s["meta"], s.get("run_id")
        if not rid:
            raise ValueError(
                f"a summary of seed {m['seed']} amplitude {m['amplitude']:g} has no run_id"
            )
        if m["kind"] != "off" or not rid.startswith("off-"):
            raise ValueError(f"run {rid} is not an official run (kind {m['kind']!r})")
        cell = (m["seed"], m["amplitude"])
        if cell in seen:
            raise ValueError(
                f"seed {cell[0]} at amplitude {cell[1]:g} appears twice:"
                f" {seen[cell]['run_id']} and {rid}"
            )
        seen[cell] = s
    pairs = planned(freeze)
    extra = sorted(set(seen) - set(pairs))
    if extra:
        raise ValueError(f"summaries of pairs the freeze does not plan: {extra}")
    last = _last_lines(registry)
    for seed, amp in pairs:
        e = last.get((seed, amp))
        if e is None or e["event"] != "finished":
            state = "has no registry line" if e is None else f"ends with {e['event']!r}"
            raise ValueError(f"seed {seed} amplitude {amp:g} {state}, not 'finished'")
        s = seen.get((seed, amp))
        if s is None:
            raise ValueError(f"seed {seed} amplitude {amp:g} has no summary")
        if s["run_id"] != e["run_id"]:
            raise ValueError(
                f"seed {seed} amplitude {amp:g}: the summary is {s['run_id']}, but the last"
                f" finished run is {e['run_id']}"
            )
        want = e.get("summary_sha256")
        if want is not None and want != s.get("_sha256"):
            raise ValueError(
                f"{s['run_id']}: summary.json does not hash to the registry's summary_sha256"
            )
        _check_timings(s)
    commits = sorted({str(s.get("commit")) for s in summaries})
    if len(commits) != 1 or commits == ["None"]:
        raise ValueError(f"the summaries carry {len(commits)} commits: {commits}")


def first_run_entry(
    registry: list[dict], summaries_dir: Path, superseded_dir: Path, freeze: dict
) -> dict:
    """Run log entry 1, derived: the first official run of every planned pair.

    Each pair's first `finished` run is read from `summaries_dir`, or from `superseded_dir` when a
    later re-run moved it, so the entry keeps the first generation's numbers after a re-run."""
    pairs = planned(freeze)
    first: dict[tuple, str] = {}
    for e in registry:
        if e["event"] == "finished":
            first.setdefault((e["seed"], e["amplitude"]), e["run_id"])
    done = [p for p in pairs if p in first]
    if len(done) < len(pairs):
        missing = [p for p in pairs if p not in first]
        raise ValueError(
            f"{len(done)} of {len(pairs)} pairs have a finished run; the first official run is"
            f" not complete: {missing}"
        )
    started = {e["run_id"]: e for e in registry if e["event"] == "started"}
    generation, lines = [], []
    for p in pairs:
        rid = first[p]
        found = [d / rid / "summary.json" for d in (Path(summaries_dir), Path(superseded_dir))]
        path = next((f for f in found if f.is_file()), None)
        if path is None:
            raise FileNotFoundError(
                f"the first summary of seed {p[0]} amplitude {p[1]:g}, {rid}, is in neither"
                f" {summaries_dir} nor {superseded_dir}"
            )
        if rid not in started:
            raise ValueError(f"{rid} has no started line in the registry")
        generation.append(json.loads(path.read_text()))
        lines.append(started[rid])
    commits = sorted({e["commit"] for e in lines})
    if len(commits) != 1:
        raise ValueError(f"the first official runs started at {len(commits)} commits: {commits}")
    begun = min(_utc(e["at"]) for e in lines)
    return {
        "commit": commits[0],
        "date": begun.date().isoformat(),
        "what": (
            f"first official run: {len(pairs)} runs, {len(freeze['official_seeds'])} seeds at"
            f" {len(freeze['amplitudes'])} amplitudes"
        ),
        "why": f"the settings freeze at `{str(freeze['commit'])[:7]}`",
        "old": "none",
        "new": headline_text(generation, MAIN_AMPLITUDE),
    }


# The UTC date of the commit that added freeze.DISPLAY_ONLY_PATHS.
MEASURED_CODE_CHANGE_DATE = "2026-11-12"


def measured_code_diff(freeze_commit: str) -> list[str]:
    """The files under `freeze.MEASURED_PATHS` that differ between the freeze commit and HEAD."""
    # --no-renames: a measured file moved into a display-only path must list its old path too.
    cmd = ["git", "-C", str(REPO), "diff", "--name-only", "--no-renames"]
    cmd += [freeze_commit, "HEAD", "--"]
    r = subprocess.run([*cmd, *MEASURED_PATHS], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"git diff could not compare the measured code with {freeze_commit[:7]}"
            f" (exit {r.returncode}): {r.stderr.strip()}"
        )
    return sorted(line for line in r.stdout.splitlines() if line)


def display_only(paths: list[str]) -> bool:
    """Whether every path is under a `freeze.DISPLAY_ONLY_PATHS` entry (none is, trivially)."""
    return all(any(p.startswith(d + "/") for d in DISPLAY_ONLY_PATHS) for p in paths)


def check_measured_code(differing: list[str], run_log: list[dict]) -> None:
    """A render after the measured code changed needs a `bug:` re-run in the run log. Display-only
    files may differ."""
    if display_only(differing) or any(str(e.get("why", "")).startswith("bug:") for e in run_log):
        return
    raise ValueError(
        "the measured code differs from the freeze commit and the run log records no 'bug:'"
        " re-run; revert the change, or re-run under --rerun-reason 'bug: ...'."
        f" Differing files: {', '.join(differing)}"
    )


# ---------------------------------------------------------------- main


def _run(*cmd: str) -> str | None:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip()


def _round_gib(n: str | None) -> float | None:
    return round(int(n) / 2**30, 1) if n else None


def measure_machine() -> dict:
    """The host and Docker VM facts, measured now (for a render with no `machine.json`)."""
    cpu = _run("sysctl", "-n", "machdep.cpu.brand_string")
    cores = _run("sysctl", "-n", "hw.ncpu")
    mem = _run("sysctl", "-n", "hw.memsize")
    docker = _run("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}")
    d = docker.split("|") if docker else [None, None, None]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    return {
        "source": f"measured by eval_report on {today}, at the first render after the runs",
        "cpu": cpu,
        "cores": int(cores) if cores else None,
        "memory_gib": _round_gib(mem),
        "docker_version": d[0],
        "docker_cpus": int(d[1]) if d[1] else None,
        "docker_memory_gib": _round_gib(d[2]),
        "python": _python_version(),
        "numpy": importlib.metadata.version("numpy"),
    }


def load_machine(path: Path) -> dict:
    """The machine facts on file; measured and written only when the file is absent."""
    if path.is_file():
        return json.loads(path.read_text())
    machine = measure_machine()
    path.write_text(json.dumps(machine, indent=2, sort_keys=True) + "\n")
    return machine


def render_facts() -> dict:
    return {
        "commit": _run("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "date": datetime.now(UTC).strftime("%Y-%m-%d"),
    }


def _load_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.is_file() else None


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
    results = REPO / "docs" / "results"
    check = _load_json(results / "check-paths.json")
    rendered = render_facts()
    if args.dev:
        dev = [s for s in load_summaries(args.dev) if s["meta"]["kind"] == "dev"]
        summaries, notes = dedupe_dev(dev)
        for n in notes:
            print(f"eval-report: {n}")
        # Dev summaries sit one level down, in data/eval/dev-*/summary.json.
        machine = measure_machine()
        text = (
            DEV_LINE
            + "\n\n"
            + render(summaries, [], check, None, machine, rendered=rendered, changes=args.change)
        )
        out = Path(args.dev) / "dev-results.md"
    else:
        summaries = load_summaries(args.summaries)
        registry = load_registry(results / "official-runs.jsonl")
        freeze = _load_json(REPO / "eval/freeze.json")
        check_official(summaries, registry, freeze)
        if not freeze.get("commit"):
            raise ValueError("eval/freeze.json has no `commit`; record the freeze commit")
        run_log = load_run_log(results / "run-log.jsonl")
        differing = measured_code_diff(freeze["commit"])
        check_measured_code(differing, run_log)
        first = first_run_entry(registry, Path(args.summaries), results / "superseded", freeze)
        machine = load_machine(results / "machine.json")
        text = render(
            summaries,
            run_log,
            check,
            freeze,
            machine,
            registry=registry,
            first_entry=first,
            rendered=rendered | {"measured_code_diff": differing},
            changes=args.change,
        )
        out = Path(args.out)
    out.write_text(text)
    print(f"eval-report: wrote {out} ({len(summaries)} summaries)")


if __name__ == "__main__":
    main()

"""Render docs/performance.md from the benchmark result files. Synthetic data only.

`render` is pure: the result dicts in, text out. Every figure is read from a result file and
every design constant from the delay file's `design` block; nothing is typed here. `load_results`
reads `throughput.json`, `delay.json` and `state.json` from a directory, and `power.json` (the OS
power log during those runs, written by `bench_power`) and `pilots/*.json` (the pilots' result
files, unedited) when they are there. The report refuses a missing file, a result that is not
valid or not synthetic, a run recorded on battery or from a dirty tree, and a result whose own
fields contradict its validity. The refusals apply to the published files only: the pilots are
shown as they are.
"""

import argparse
import json
import re
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

from tenant_baselines.freeze import REPO

__all__ = ["ReportRefused", "check_results", "load_results", "main", "render"]

NAMES = ("throughput", "delay", "state")
_OFFSET_LIMIT_S = 1.0
_SLEEP_LIMIT_S = 1.0
_IDLE_LIMIT = 0.01  # a source idle less than this share of the window did not wait on the broker
_GIB = 1024**3
_MIB = 1024**2
_ZONE_NAMES = {"+05:30": "IST"}  # the laptop's zone, named for the reader
_POWER_NAME = re.compile(r"'([^']+)'")


class ReportRefused(ValueError):
    pass


# ---------------------------------------------------------------- checks


def _runs_of(name: str, doc: dict) -> list[tuple[str, dict]]:
    """The parts of a result that carry a run's own validity fields."""
    if name == "delay":
        return [("the run", doc)]
    return [(r.get("run_id", r.get("backend", "a run")), r) for r in doc.get("runs", [])]


def _battery(power_line: str | None) -> bool:
    """A `pmset -g ps` reading that is not AC; one that was not read (None or empty) is not."""
    return bool(power_line) and "AC Power" not in power_line


def _contradictions(name: str, doc: dict) -> list[str]:
    out = []
    if name == "state" and _battery(doc.get("power_end")):
        out.append(f"on battery at the end ({doc['power_end']})")
    for label, r in _runs_of(name, doc):
        if _battery(r.get("power_end")):
            out.append(f"{label}: on battery at its end ({r['power_end']})")
        if r.get("host_sleep_s", 0) > _SLEEP_LIMIT_S:
            out.append(f"{label}: the host slept {r['host_sleep_s']} s")
        if r.get("stall_gaps_s"):
            out.append(f"{label}: the driver stalled, gaps {r['stall_gaps_s']} s")
        if r.get("restarts"):
            out.append(f"{label}: the job restarted {r['restarts']} time(s)")
        if r.get("late_events"):
            out.append(f"{label}: {r['late_events']} late event(s)")
        for key in ("aborted", "window_error"):
            if r.get(key):
                out.append(f"{label}: it records {key} ({r[key]})")
    if name == "delay":
        before = doc["clock_offset"]["before"]["offset_s"]
        after = doc["clock_offset"]["after"]["offset_s"]
        if abs(after - before) > _OFFSET_LIMIT_S:
            out.append("the VM clock offset moved by more than 1 s")
    return out


def check_results(docs: dict[str, dict]) -> None:
    """Raise ReportRefused, naming the file, unless all three results may be published."""
    for name in NAMES:
        _guard(f"{name}.json", lambda name=name: _check_one(name, docs[name]))


def _check_one(name: str, doc: dict) -> None:
    file = f"{name}.json"
    if doc["env"].get("dirty"):
        raise ReportRefused(f"{file}: recorded from a dirty tree; run it again from a clean tree")
    if doc.get("data") != "synthetic":
        raise ReportRefused(f"{file}: the data is not marked synthetic ({doc.get('data')!r})")
    if not doc.get("valid"):
        why = "; ".join(doc.get("invalid_reasons") or ["no reason recorded"])
        raise ReportRefused(f"{file}: the run is not valid: {why}")
    if doc["env"].get("on_battery"):
        raise ReportRefused(
            f"{file}: recorded on battery; published runs need AC power, so run it again"
        )
    bad = _contradictions(name, doc)
    if bad:
        raise ReportRefused(
            f"{file}: marked valid, but its fields say otherwise: " + "; ".join(bad)
        )


# ---------------------------------------------------------------- formatting


def n(x, digits: int = 1) -> str:
    """A number with thousands separators; whole numbers without a decimal."""
    if x is None:
        return "n/a"
    if isinstance(x, int) or (isinstance(x, float) and x == int(x) and digits == 0):
        return f"{x:,.0f}"
    return f"{x:,.{digits}f}"


def count(x) -> str:
    return f"{x:,}"


def pct(share: float | None, digits: int = 1) -> str:
    return "n/a" if share is None else f"{share * 100:.{digits}f}%"


def size(b) -> str:
    return f"{b:,.0f} B ({b / _MIB:,.1f} MiB)"


def table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(" --- " for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def _join(parts: Sequence[str]) -> str:
    """'a', 'a and b', 'a, b and c'."""
    if len(parts) < 2:
        return "".join(parts)
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def _join_clauses(parts: Sequence[str]) -> str:
    """Clauses that hold an "and" of their own: 'a', 'a, and b', 'a, b, and c'."""
    if len(parts) < 2:
        return "".join(parts)
    return ", ".join(parts[:-1]) + ", and " + parts[-1]


def _short_power(line: str | None) -> str:
    """The source a `pmset -g ps` line names ("AC Power"), else the line; not read: so said."""
    if not line:
        return "not recorded"
    m = _POWER_NAME.search(line)
    return m.group(1) if m else line


def _power_pair(start: str | None, end: str | None) -> str:
    return f"{_short_power(start)} at the start, {_short_power(end)} at the end"


def _power_end(kind: str, doc: dict) -> str | None:
    """The reading at the end: the last run's for throughput, the file's for the others."""
    if kind == "throughput":
        runs = doc.get("runs") or [{}]
        return runs[-1].get("power_end")
    return doc.get("power_end")


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def _clock(iso: str) -> str:
    return _dt(iso).strftime("%H:%M:%S")


def _seconds(a: str, b: str) -> float:
    return (_dt(b) - _dt(a)).total_seconds()


def _zone(iso: str) -> tuple[str, str]:
    """The name of a time's zone, when known, and its offset: ("IST", "UTC+05:30")."""
    minutes = int(_dt(iso).utcoffset().total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    offset = f"{sign}{abs(minutes) // 60:02}:{abs(minutes) % 60:02}"
    return _ZONE_NAMES.get(offset, f"UTC{offset}"), f"UTC{offset}"


# ---------------------------------------------------------------- sections


def _what_this_is() -> str:
    return (
        "# Performance\n\n"
        "## What this is\n\n"
        "Three measurements on one laptop, on synthetic data: how fast the job reads a filled "
        "topic, how long an alert takes from an injected incident to the alerts topic, and how "
        "much state the job keeps per key. They describe this build on this machine. They are "
        "not evidence about any real system or any larger deployment.\n"
    )


def _text(x) -> str:
    return "not recorded" if x is None else str(x)


def _env_rows(docs: dict[str, dict]) -> list[tuple[str, list[str]]]:
    def row(label, get):
        return label, [_text(get(docs[k]["env"])) for k in NAMES]

    return [
        row("Date (UTC)", lambda e: e["date_utc"]),
        row("Commit", lambda e: e["commit"]),
        row("CPU", lambda e: e["host"]["cpu"]),
        row("Cores", lambda e: e["host"]["cores"]),
        row("Memory (GB)", lambda e: e["host"]["memory_gb"]),
        row("OS", lambda e: e["host"]["os"]),
        row("Docker VM CPUs", lambda e: e["vm"]["cpus"]),
        row("Docker VM memory (GB)", lambda e: e["vm"]["memory_gb"]),
        row("Other containers in the VM", lambda e: e["other_containers"]),
        row("Images", lambda e: ", ".join(dict.fromkeys(e["images"]))),
        row("Flink version", lambda e: e["flink"]["version"]),
        row("Parallelism", lambda e: e["flink"]["parallelism.default"]),
        row("Task slots", lambda e: e["flink"]["taskmanager.numberOfTaskSlots"]),
        row("TaskManager memory", lambda e: e["flink"]["taskmanager.memory.process.size"]),
        (
            "Power at start and end",
            [_power_pair(docs[k]["env"]["power"], _power_end(k, docs[k])) for k in NAMES],
        ),
    ]


def _machine(docs: dict[str, dict]) -> str:
    rows = _env_rows(docs)
    if any(len(set(vals)) > 1 for _, vals in rows):
        body = table(["Item", *(k.capitalize() for k in NAMES)], [[lab, *v] for lab, v in rows])
    else:
        body = table(["Item", "Value"], [[lab, vals[0]] for lab, vals in rows])
    return (
        "## Machine and versions\n\n"
        "These figures describe the machine that produced the synthetic runs below. Every row "
        "is read from the result files' environment records. There is one column per measurement "
        "when any row differs between the files.\n\n"
        f"{body}\n\n"
        "Power is read when a run starts and when it ends, not in between. The OS power log, in "
        "the next section, shows what came between.\n\n"
        "The state runs' backend and checkpoint interval are in the state-size section, not in "
        "this table.\n"
    )


def _interval_cell(iv: dict, run: dict) -> str:
    cell = f"{iv['source']}, {n(iv['overlap_s'], 0)} s"
    if iv["source"] == "AC Power":
        return cell
    a = max(_dt(iv["from"]), _dt(run["start"])).isoformat()
    b = min(_dt(iv["to"]), _dt(run["end"])).isoformat() if iv["to"] else run["end"]
    return f"{cell} ({_clock(a)} to {_clock(b)})"


def _battery_pilot(delay: dict, pilots: dict[str, dict]) -> str:
    """The delay pilots on battery at both readings, set beside this run's pipeline delay."""
    here = delay["headline"]["pipeline_delay_s"]
    out = []
    for run_id, doc in pilots.items():
        if _pilot_kind(doc) != "delay":
            continue
        if not (_battery(doc["env"].get("power")) and _battery(doc.get("power_end"))):
            continue
        p = doc["headline"]["pipeline_delay_s"]
        out.append(
            f"The delay pilot {run_id} was on battery at its start and at its end; it gave a"
            f" pipeline delay of {n(p['p50'])} s at p50 and {n(p['p95'])} s at p95, against"
            f" {n(here['p50'])} s and {n(here['p95'])} s in this run."
        )
    return " ".join(out)


def _delay_battery(run: dict, iv: dict, zone: str) -> str:
    """What the delay run was doing while it was on battery, from its own records."""
    a = max(_dt(iv["from"]), _dt(run["start"])).isoformat()
    b = min(_dt(iv["to"]), _dt(run["end"])).isoformat() if iv["to"] else run["end"]
    text = (
        f"The delay run ({run['run_id']}) had the charger out from {_clock(a)} to {_clock(b)}"
        f" {zone}, for {n(iv['overlap_s'], 0)} s, and ran on battery."
    )
    inc = run.get("incidents")
    if not inc:
        return text
    settle_end = (_dt(inc["caught_up"]) + timedelta(seconds=inc["settle_s"])).isoformat()
    moments = [f"the generator had written the history at {_clock(inc['caught_up'])}"]
    if inc.get("job_caught_up"):
        moments.append(f"the job caught up on it at {_clock(inc['job_caught_up'])}")
    moments += [
        f"the {count(inc['settle_s'])} s settle period ended at {_clock(settle_end)}",
        f"the first incident started at {_clock(inc['first_incident_start'])}",
    ]
    text += f" In the run's own records, {_join(moments)}."
    order = [inc["caught_up"], a, inc.get("job_caught_up"), settle_end]
    order += [inc["first_incident_start"], b]
    if None not in order and all(_dt(x) <= _dt(y) for x, y in zip(order, order[1:], strict=False)):
        into = _seconds(inc["first_incident_start"], b)
        text += (
            f" So the charger was out for the job's catch-up from {_clock(a)} on, the rest of"
            f" the settle period and the first {n(into, 0)} s of the first incident."
        )
    if inc["matched_page_on_battery"] is False:
        text += (
            " No incident was paged on battery: the earliest a page matched to an incident could"
            " be appended is the first incident's start plus the shortest time-to-page of any"
            f" setting, {n(inc['shortest_time_to_page_s'])} s, at {_clock(inc['earliest_page'])},"
            f" {n(_seconds(b, inc['earliest_page']), 0)} s after the charger came back."
        )
    else:
        text += " The records cannot rule out a page appended on battery."
    return text


def _power(power: dict | None, docs: dict[str, dict], pilots: dict[str, dict]) -> str:
    head = "## Power during the runs\n\nThis section is about the machine during the synthetic runs"
    if power is None:
        return f"{head}, not about the data. The OS power log was not read for these runs.\n"
    runs = {k: power["runs"][k] for k in NAMES}
    zone, offset = _zone(runs["throughput"]["start"])
    folder = Path(runs["throughput"]["log"]).parent
    rows = [
        [
            k.capitalize(),
            r["run_id"],
            _clock(r["start"]),
            _clock(r["end"]),
            "; ".join(_interval_cell(iv, r) for iv in r["intervals"]) or "no reading",
        ]
        for k, r in runs.items()
    ]
    notes, last_delay = [], None
    for k, r in runs.items():
        if not r["covered_by_log"]:
            notes.append(f"The log does not cover the whole of the {k} run ({r['run_id']}).")
        for iv in r["intervals"]:
            if iv["source"] == "AC Power":
                continue
            if k == "delay":
                notes.append(_delay_battery(r, iv, zone))
                last_delay = len(notes) - 1
            else:
                a = max(_dt(iv["from"]), _dt(r["start"])).isoformat()
                b = min(_dt(iv["to"]), _dt(r["end"])).isoformat() if iv["to"] else r["end"]
                notes.append(
                    f"The {k} run ({r['run_id']}) was on {iv['source']} from {_clock(a)} to"
                    f" {_clock(b)} {zone}, for {n(iv['overlap_s'], 0)} s."
                )
    if not any(iv["source"] != "AC Power" for r in runs.values() for iv in r["intervals"]):
        notes.append("The log shows AC Power throughout every run.")
    pilot = _battery_pilot(docs["delay"], pilots)
    if last_delay is not None and pilot:  # once, after the delay run's last battery span
        notes[last_delay] += f" {pilot}"
    return (
        f"{head} above, not about the data. The drivers read the power source only when a run"
        " starts and when it ends. The OS also logs every change of power source: this section"
        " reads the lines of `pmset -g log` that say `Using AC` or `Using Batt`, saved during the"
        f" sitting under `docs/performance/{folder}/`."
        " The table lists each source the log shows during each run, with its seconds inside the"
        f" run. Times are the log's local time, {zone} ({offset}). A run's end is when its driver"
        " wrote its result file; the result files do not record an end time.\n\n"
        + table(
            [
                "Measurement",
                "Run",
                f"Start ({zone})",
                f"End ({zone})",
                "Power source in the OS log",
            ],
            rows,
        )
        + "\n\n"
        + "\n\n".join(notes)
        + "\n"
    )


def _load_cell(load: dict) -> list[str]:
    if load.get("host_load_1m") is None:
        return ["not recorded"] * 3 + [str(load.get("error", ""))]
    return [
        n(load["host_load_1m"], 2),
        n(load["project_containers_cpu_pct"]),
        n(load["other_containers_cpu_pct"]),
        f"{load['vm_cpus']} VM CPUs; {load['cpu_pct_unit']}",
    ]


def _load_table(loads: list[tuple[str, dict]]) -> str:
    rows = [[label, *_load_cell(load)] for label, load in loads]
    return table(
        [
            "Reading",
            "Host load, 1 min",
            "This project's containers, CPU",
            "Other containers, CPU",
            "Scale",
        ],
        rows,
    )


def _run_loads(runs: list[dict]) -> list[tuple[str, dict]]:
    out = []
    for r in runs:
        out += [(f"{r['run_id']} start", r["load_start"]), (f"{r['run_id']} end", r["load_end"])]
    return out


def _share_note(loads: list[dict]) -> str:
    seen = [x for x in loads if x.get("other_containers_cpu_pct") is not None]
    if not seen:
        return (
            "The Docker VM is shared with other projects' containers. Their CPU use was not "
            "recorded for these runs, and run-to-run rates can differ.\n"
        )
    others = [x["other_containers_cpu_pct"] for x in seen]
    first = seen[0]
    return (
        "The Docker VM is shared. At the readings above, other projects' containers used"
        f" {n(min(others))} to {n(max(others))} {first['cpu_pct_unit']} (the VM has"
        f" {first['vm_cpus']} CPUs, so {count(first['vm_cpus'] * 100)} in all), so run-to-run"
        " rates can differ.\n"
    )


def _who_set_the_pace(runs: list[dict]) -> str:
    """The conclusion the source's idle share allows: under 1% in every run, the job set it."""
    if all(r["source"]["idle"] < _IDLE_LIMIT for r in runs):
        parts = [
            f"back-pressured {pct(r['source']['backpressured'])}"
            + (" of the window" if i == 0 else "")
            + f" and idle {pct(r['source']['idle'])} in {r['run_id']}"
            for i, r in enumerate(runs)
        ]
        return f"The source was {_join_clauses(parts)}: the job set the pace, not the broker."
    parts = [
        f"{pct(r['source']['idle'])}" + (" of the window" if i == 0 else "") + f" in {r['run_id']}"
        for i, r in enumerate(runs)
    ]
    return (
        f"The source was idle {_join(parts)}: for that share it waited on the broker, so the rate"
        " is not the job's alone."
    )


def _run_order(runs: list[dict]) -> str:
    """Whether the colder first job was the slower one, said from the rates."""
    faster = max(runs, key=lambda r: r["events_per_s"])
    text = f"{faster['run_id']} was the faster run."
    if len(runs) < 2:
        return text
    if faster is runs[0]:
        return (
            f"{text} The first job on a TaskManager starts colder, which would make it the"
            " slower one. Here it was the faster, so the order does not explain the spread."
        )
    return (
        f"{text} The first job on a TaskManager starts colder, which would make it the slower"
        " one, and here it was."
    )


def _share_table(runs: list[dict]) -> str:
    names = list(runs[0]["shares"])
    header = ["Vertex"]
    for r in runs:
        header += [f"{r['run_id']} busy", f"{r['run_id']} back-pressured", f"{r['run_id']} idle"]
    rows = []
    for name in names:
        cells = [name]
        for r in runs:
            s = r["shares"].get(name, {})
            cells += [pct(s.get(k)) for k in ("busy", "backpressured", "idle")]
        rows.append(cells)
    return table(header, rows)


def _vertex_share(r: dict, prefix: str) -> dict | None:
    found = [v for name, v in r["shares"].items() if name.startswith(prefix)]
    return found[0] if len(found) == 1 else None


def _drop_ticks(runs: list[dict]) -> str:
    ticks = "; ".join(f"{r['run_id']}: {pct(r['drop_ticks_busy'])}" for r in runs)
    shares = [(r["run_id"], _vertex_share(r, "drop-ticks")) for r in runs]
    text = (
        "The `drop-ticks` vertex's busy share, as a share of parallelism times the window, is"
        f" {ticks}. It is reported as a share of CPU time, not as a throughput difference. The"
        " hop adds one read of each event, the filter and one write. The drop-ticks task's busy"
        " time is those plus choosing the key's channel, which the source would do without the"
        " union, so the share is an upper bound on what the hop adds."
    )
    if all(s is not None for _, s in shares):
        text += " The task's own share: " + "; ".join(
            f"{run_id}: back-pressured {pct(s['backpressured'])} of the window"
            for run_id, s in shares
        )
        text += "."
        if all(s["backpressured"] > 0.5 for _, s in shares):
            text += (
                " It was back-pressured for more than half of the window in every run, so it did"
                " not set the pace."
            )
    return text


def _throughput(t: dict) -> str:
    f, runs = t["fill"], t["runs"]
    rate_rows = [
        [
            r["run_id"],
            n(r["events_per_s"], 0),
            n(r["events_per_s_source_clock"], 0),
            n(r["window_s"]),
            f"{n(r['min_interval_rate'])} to {n(r['max_interval_rate'])}",
            pct(r["source"]["backpressured"]),
            pct(r["source"]["idle"]),
            f"{r['stale_samples']} / {r['reread_samples']}",
        ]
        for r in runs
    ]
    busiest = "; ".join(
        f"{r['run_id']}: {r['busiest_vertex']} at {pct(r['shares'][r['busiest_vertex']]['busy'])}"
        for r in runs
    )
    errors = "; ".join(f"{r['run_id']}: {n(r['timing_error_pct'])}%" for r in runs)
    alert_keys = sorted({k for r in runs for k in r["alert_rows"]})
    if alert_keys:
        alerts = table(
            ["Detector / setting / scope", *(r["run_id"] for r in runs)],
            [[k, *(count(r["alert_rows"].get(k, 0)) for r in runs)] for k in alert_keys],
        )
    else:
        alerts = (
            "No run wrote an alert row: the bench world is built to stay under every fire level."
        )
    mem = "\n".join(
        f"- {r['run_id']}: {r['taskmanager_memory']['used_bytes'] / _GIB:.2f} GiB of"
        f" {r['taskmanager_memory']['limit_bytes'] / _GIB:.2f} GiB"
        f" ({pct(r['taskmanager_memory']['used_bytes'] / r['taskmanager_memory']['limit_bytes'])})"
        for r in runs
    )
    names = [v["name"] for v in runs[0]["checkpoint"]["vertices"]]
    sizes = [{v["name"]: v["state_size"] for v in r["checkpoint"]["vertices"]} for r in runs]
    ck = table(
        ["Vertex", *(f"{r['run_id']} (checkpoint {r['checkpoint']['id']})" for r in runs)],
        [[nm, *(count(s.get(nm, 0)) for s in sizes)] for nm in names]
        + [["Whole checkpoint", *(count(r["checkpoint"]["state_size"]) for r in runs)]],
    )
    loads = [x for _, x in _run_loads(runs)]
    return (
        "## Throughput\n\n"
        "This section uses synthetic data. "
        f"The fill had {count(f['keys'])} keys at {n(f['rpm_per_key'], 0)} requests a minute each,"
        f" for {count(f['event_minutes'])} event minutes: {count(f['events'])} events in"
        f" {count(f['partitions'])} partitions. The generator filled the topic at"
        f" {n(f['events_per_s'], 0)} events a second ({n(f['seconds'])} s), so the job's rate"
        " can be set beside the rate at which the same events were written.\n\n"
        "The topic was complete before the job started, and nothing produced during the run. "
        "The rate is the rise of the source's record counter over a window that leaves out the"
        f" first {count(t['warmup_s'])} s after the first sample that showed a count and the last"
        f" {pct(1 - t['end_share'], 0)} of the fill, sampled every {count(t['sample_interval_s'])}"
        " s.\n\n"
        + table(
            [
                "Run",
                "Sustained events a second",
                "Source's own clock, events a second",
                "Window (s)",
                "Lowest to highest interval rate",
                "Source back-pressured",
                "Source idle",
                "Stale / re-read samples",
            ],
            rate_rows,
        )
        + "\n\n"
        "A source that is neither busy nor back-pressured is waiting on the broker. "
        f"{_who_set_the_pace(runs)}\n\n"
        f"The two runs differ by {n(t['spread_pct'])}% of their mean. {_run_order(runs)} The"
        " source's own clock is a cross-check that does not depend on the host's timing.\n\n"
        f"`timing_error_pct` is a bound, computed from the longest measured read delay at the"
        f" window's two ends over the window: {errors}.\n\n"
        f"The busiest vertex: {busiest}. Every vertex's busy, back-pressured and idle share, as a"
        " share of parallelism times the window:\n\n"
        f"{_share_table(runs)}\n\n"
        f"{_drop_ticks(runs)}\n\n"
        f"The job ran {count(t['settings'])} settings at once; the bench world's keys have no"
        " per-key entry in `thresholds-demo.json`, so `per_key_static` used the global levels."
        f" The alert rows each run wrote, counted from `bench.alerts`:\n\n{alerts}\n\n"
        "The TaskManager's memory against its limit. The first reading is for one job; the"
        " second is cumulative, taken after the TaskManager had already run the first job:\n\n"
        f"{mem}\n\n"
        f"The last checkpoint's size per vertex, in bytes, at {count(f['keys'])} keys:\n\n{ck}\n\n"
        f"The fill is {count(f['event_minutes'])} event minutes, so no key has the history the"
        " seasonal detector needs and every key is scored against the fallback baselines. No"
        " unit/tenant pair goes stale in this world. So these sizes are not the state of a"
        " warmed-up job.\n\n"
        "What shares the VM: the broker, ClickHouse reading the bench topics, and"
        f" {count(t['env']['other_containers'])} other containers when the run began.\n\n"
        "What the machine was doing:\n\n"
        + _load_table(_run_loads(runs))
        + "\n\n"
        + _share_note(loads)
    )


def _row(label: str, m: dict) -> list[str]:
    def cell(key, q):
        return f"{n(m[key][q])} (n={m[key]['n']})"

    return [
        label,
        count(m["incidents"]),
        count(m["matched"]),
        cell("pipeline_delay_s", "p50"),
        cell("pipeline_delay_s", "p95"),
        cell("time_to_page_s", "p50"),
        cell("time_to_page_s", "p95"),
        cell("time_to_page_s", "min"),
    ]


def _static_rows(by: dict) -> str:
    """When `per_key_static`'s row equals `global_static`'s for a setting, say why."""
    for key, m in by.items():
        if not key.startswith("global_static/"):
            continue
        twin = "per_key_static/" + key.split("/", 1)[1]
        if by.get(twin) == m:
            return (
                "\n\nThe delay world's keys have no per-key entry in `thresholds-demo.json`, so"
                " `per_key_static` falls back to the global level and its row equals"
                " `global_static`'s."
            )
    return ""


def _delay(d: dict) -> str:
    w, sch, dz, h = d["world"], d["schedule"], d["design"], d["headline"]
    lag, off = d["watermark_lag_s"], d["clock_offset"]
    heading = f"{h['detector']}/{h['setting']}"
    rows = [
        _row(k + (" (headline)" if k == heading else ""), m) for k, m in d["by_detector"].items()
    ]
    return (
        "## Delay\n\n"
        "This section uses synthetic data. "
        f"The world has {count(w['keys'])} keys at up to {n(w['rpm_per_key'], 0)} requests a"
        f" minute per key, in {count(w['partitions'])} partitions, with"
        f" {count(w['history_days'])} days of history before live traffic."
        f" The run injected {count(d['incidents'])} incidents, each"
        f" an error-rate rise of {n(sch['magnitude'] * 100, 0)} points on one key for"
        f" {count(sch['duration_s'])} s. The first starts {count(sch['settle_s'])} s after the"
        f" generator catches up; incidents in one region are {count(sch['spacing_s'])} s apart;"
        f" the run reads for {count(sch['tail_s'])} s after the last one ends.\n\n"
        "Pipeline delay is the time from a row's event time, which for a tenant alert is the end"
        " of its hold, to the broker's append time of that row in `bench.alerts`. Time-to-page is"
        " the time from the incident's start to the append of the first matching row."
        " Percentiles are nearest-rank, with their `n`. Times are in"
        f" seconds. The headline setting is {heading}.\n\n"
        + table(
            [
                "Detector / setting",
                "Incidents",
                "Matched",
                "Pipeline delay p50",
                "Pipeline delay p95",
                "Time to page p50",
                "Time to page p95",
                "Time to page minimum",
            ],
            rows,
        )
        + _static_rows(d["by_detector"])
        + "\n\n"
        "How time-to-page is built (configured, not measured): the detector fires after"
        f" {count(dz['fire_after_minutes'])} scored minutes in a row, and the alert is then held"
        f" for {count(dz['hold_s'])} s. Both are counted from the start of the first breaching"
        " minute, not from the incident's start. A spike of this size is expected to breach in"
        " the minute it starts in, so the count often starts up to a minute before the incident."
        f" The measured minimum is {n(h['time_to_page_s']['min'])} s, and"
        f" {count(h['under_five_minutes'])} of {count(h['time_to_page_s']['n'])} matched"
        " incidents were paged in under five minutes from their start.\n\n"
        f"The watermark lag, measured during the run and corrected by the VM clock's offset"
        f" (before {n(off['before']['offset_s'], 3)} s, after {n(off['after']['offset_s'], 3)} s):"
        f" minimum {n(lag['min'])} s, median {n(lag['median'])} s, maximum {n(lag['max'])} s over"
        f" {count(lag['samples'])} samples. By design (configured, not measured), the lag is the"
        f" {count(dz['watermark_bound_s'])} s watermark bound plus the age of the newest event on"
        " the slowest partition that is not idle. This benchmark sends at a fixed rate, so every"
        " partition is busy. A sparse partition adds up to"
        f" {count(dz['idleness_s'])} s (the idleness timeout, configured); that case was not"
        " measured here.\n\n"
        "Both figures end at the append to the alerts topic. The forwarder and Alertmanager's"
        f" `group_wait` ({count(dz['group_wait_s'])} s, configured) come after it."
        " Page times read at the webhook include the forwarder and `group_wait`, and are not"
        " comparable.\n\n"
        "The split of the headline pipeline delay, in seconds: decision to emit"
        f" {n(h['emit_delay_s']['p50'])} at p50 and {n(h['emit_delay_s']['p95'])} at p95; emit to"
        f" append {n(h['sink_delay_s']['p50'])} at p50 and {n(h['sink_delay_s']['p95'])} at"
        " p95.\n\n"
        f"For the headline setting: {count(len(h['missed']))} incidents were missed,"
        f" {count(h['unmatched_firing_rows'])} firing rows matched no incident, and"
        f" {count(h['regional_firing_rows'])} regional firing rows were written.\n\n"
        "What the machine was doing:\n\n"
        + _load_table([("start", d["load_start"]), ("end", d["load_end"])])
        + "\n"
    )


def _after_finish(x) -> str:
    return "not recorded" if x is None else str(x).lower()


def _state_run(r: dict, spec: int, label: str) -> str:
    c, pk = r["checkpoint"], r["per_key"]
    vertices = table(
        ["Vertex", "State size (bytes)"],
        [[v["name"], count(v["state_size"])] for v in c["vertices"]],
    )
    out = (
        f"{label}: checkpoint {c['id']} was the latest completed one triggered before the source"
        f" vertex was reported finished, at {pct(c['trigger_share'], 0)} of the run,"
        f" interval {count(r['interval_ms'])} ms, total state {size(c['state_size'])}. The scorer"
        f" holds {size(pk['scorer_bytes'])}, which is {n(pk['per_key_bytes'], 0)} bytes per key"
        f" over {count(r['keys'])} keys. The spec's estimate is {count(spec)} bytes per key"
        " (an estimate, not a measurement). Method note: checkpoints after tasks finish:"
        f" {_after_finish(r.get('checkpoints_after_tasks_finish'))}.\n\n"
        f"{vertices}\n"
    )
    rng = r.get("range") or {}
    if rng.get("checkpoints"):
        if rng["checkpoints"] == 1 or rng["min"] == rng["max"]:
            how = (
                "one checkpoint"
                if rng["checkpoints"] == 1
                else f"the same in all {count(rng['checkpoints'])} checkpoints"
            )
            out += (
                f"\nOver the last stretch of the run the total state size was"
                f" {count(rng['min'])} bytes ({how}).\n"
            )
        else:
            out += (
                f"\nOver the {count(rng['checkpoints'])} checkpoints of the last stretch of the"
                f" run, the total state size ranged from {count(rng['min'])} to"
                f" {count(rng['max'])} bytes.\n"
            )
        if r["backend"] == "rocksdb":
            out += (
                "The figure moves from one checkpoint to the next: the state is still growing in"
                " the last stretch of the run, and SST files that are not yet compacted are"
                " counted.\n"
            )
    return out


def _backend_gap(by: dict) -> str:
    """How the two backends' per-key figures compare, with the cause the docs give."""
    if "hashmap" not in by or "rocksdb" not in by:
        return ""
    h = by["hashmap"]["per_key"]["per_key_bytes"]
    r = by["rocksdb"]["per_key"]["per_key_bytes"]
    if r >= h:
        return f"\nThe RocksDB figure is {n(r / h)} times the hashmap figure.\n"
    return (
        "\nThe two backends store the same state in different formats. The hashmap figure is"
        f" {n(h / r)} times the RocksDB figure. RocksDB keeps its SST files compressed: Flink's"
        " default for `state.backend.rocksdb.compression.per.level` is `SNAPPY_COMPRESSION` on"
        " every level, and this stack does not set it (a documented default, not measured"
        " here).\n"
    )


def _state(s: dict, throughput_settings: int) -> str:
    w = s["world"]
    by = {r["backend"]: r for r in s["runs"]}
    head = s["headline_backend"]
    order = [head] + [b for b in by if b != head]
    names = {
        "hashmap": "The hashmap run (the per-key headline)",
        "rocksdb": "The RocksDB run, with incremental checkpoints (the stack's backend)",
    }
    parts = [_state_run(by[b], s["spec_estimate_bytes_per_key"], names.get(b, b)) for b in order]
    return (
        "## State size\n\n"
        "This section uses synthetic data. "
        f"The world is `{w['name']}` (seed {w['seed']}, amplitude {n(w['amplitude'])}) with"
        f" {count(w['keys'])} keys, {count(w['keys_with_five_weeks'])} of which run all"
        f" {count(w['weeks'])} weeks."
        f" {count(w['aggregate_rows'])} aggregate rows went through the job in"
        " aggregates mode.\n\n" + "\n".join(parts) + _backend_gap(by) + "\n"
        f"This job ran against {count(s['settings'])} settings; the live job runs"
        f" {count(throughput_settings)}. The baseline history, which all settings share, is most"
        " of a key's state.\n"
    )


def extrapolation_table(s: dict) -> str:
    """The Markdown table of the state extrapolation, and nothing else."""
    ex = s["extrapolation"]
    backends = [b for b in ex if b != "label"]
    keys = [r["keys"] for r in ex[backends[0]]]
    rows = [
        [count(k), *(count(next(r["bytes"] for r in ex[b] if r["keys"] == k)) for b in backends)]
        for k in keys
    ]
    return table(["Keys", *(f"{b} (bytes)" for b in backends)], rows)


def _extrapolation(s: dict) -> str:
    w = s["world"]
    short = w["keys"] - w["keys_with_five_weeks"]
    divisor = (
        f" {count(short)} of the {count(w['keys'])} measured keys ran fewer weeks, so the per-key"
        " figure is slightly under a full key's."
        if short > 0
        else ""
    )
    return (
        "## Extrapolation, not measured\n\n"
        "This section is an extrapolation from synthetic measurements, not a measurement. Bytes"
        " are the scorer's bytes per key times the key count.\n\n" + extrapolation_table(s) + "\n\n"
        f"It assumes every key has a full {count(w['weeks'])}-week history and that state grows"
        f" linearly with keys.{divisor} Neither assumption was tested above the measured key"
        " count. It leaves out the region grouper's per-key activity, the minute aggregator's"
        " state in events mode and the population baselines.\n"
    )


def readme_block(throughput: dict, delay: dict, state: dict) -> str:
    """The README's performance table; every value comes from the three result files."""
    check_results({"throughput": throughput, "delay": delay, "state": state})
    try:
        fill, h, lag = throughput["fill"], delay["headline"], delay["watermark_lag_s"]
        rates = " and ".join(n(r["events_per_s"], 0) for r in throughput["runs"])
        pd, tp = h["pipeline_delay_s"], h["time_to_page_s"]
        run = next(r for r in state["runs"] if r["backend"] == state["headline_backend"])
        rows = [
            [
                f"Sustained throughput of the live job, {count(fill['keys'])} keys",
                f"{rates} events a second ({count(len(throughput['runs']))} runs)",
            ],
            [
                f"Pipeline delay, {h['detector']}/{h['setting']}",
                f"p50 {n(pd['p50'])} s, p95 {n(pd['p95'])} s (n={count(pd['n'])})",
            ],
            [
                "Time to page, incident start to the alerts topic",
                f"p50 {n(tp['p50'])} s, p95 {n(tp['p95'])} s, minimum {n(tp['min'])} s"
                f" (n={count(tp['n'])})",
            ],
            [
                "Watermark lag during the delay run",
                f"median {n(lag['median'])} s ({n(lag['min'])} to {n(lag['max'])})",
            ],
            [
                f"Scorer state per key, mean over {count(run['keys'])} keys after"
                f" {count(state['world']['weeks'])} replayed weeks ({state['headline_backend']})",
                f"{n(run['per_key']['per_key_bytes'], 0)} bytes",
            ],
        ]
    except (KeyError, StopIteration, TypeError, IndexError) as e:
        raise ReportRefused(f"the README block: a result file lacks a field ({e!r})") from e
    return "| Measurement (synthetic data, one laptop) | Figure |\n|---|---|\n" + "\n".join(
        "| " + " | ".join(r) + " |" for r in rows
    )


def _pilot_kind(doc: dict) -> str:
    if "fill" in doc:
        return "throughput"
    if "headline_backend" in doc:
        return "state"
    if "headline" in doc:
        return "delay"
    return "unknown"


def _pilot_figure(kind: str, doc: dict) -> str:
    if kind == "throughput":
        parts = []
        for r in doc.get("runs", []):
            if r.get("events_per_s") is not None:
                parts.append(f"{n(r['events_per_s'], 0)} events a second")
            elif r.get("events_per_s_source_clock") is not None:
                rate = n(r["events_per_s_source_clock"], 0)
                parts.append(f"no window; {rate} events a second on the source's clock")
            else:
                parts.append("no window")
        return "; ".join(parts) or "no run"
    if kind == "delay":
        p = doc["headline"]["pipeline_delay_s"]
        return f"pipeline delay p50 {n(p['p50'])} s, p95 {n(p['p95'])} s (n={p['n']})"
    if kind == "state":
        return "; ".join(
            f"{r['backend']} {n(r['per_key']['per_key_bytes'], 0)} bytes per key"
            for r in doc.get("runs", [])
        )
    return "not recognised"


def _pilot_loads(kind: str, doc: dict) -> str:
    pairs = [(doc.get("load_start"), doc.get("load_end"))] if kind == "delay" else []
    pairs += [(r.get("load_start"), r.get("load_end")) for r in doc.get("runs", [])]

    def cpu(load):
        x = (load or {}).get("other_containers_cpu_pct")
        return "not recorded" if x is None else n(x)

    cells = [f"{cpu(a)} → {cpu(b)}" for a, b in pairs if a or b]
    return "; ".join(cells) or "not recorded"


def _pilots(pilots: dict[str, dict]) -> str:
    if not pilots:
        return ""
    order = sorted(pilots, key=lambda k: (pilots[k].get("env", {}).get("date_utc") or "", k))
    rows, notes = [], []
    for run_id in order:
        doc = pilots[run_id]
        env = doc.get("env", {})
        kind = _pilot_kind(doc)
        rows.append(
            [
                run_id,
                (env.get("commit") or "not recorded")[:7],
                env.get("date_utc") or "not recorded",
                _power_pair(env.get("power"), _power_end(kind, doc)),
                "yes" if doc.get("valid") else "no",
                _pilot_figure(kind, doc),
                _pilot_loads(kind, doc),
            ]
        )
        if not doc.get("valid"):
            why = "; ".join(doc.get("invalid_reasons") or ["no reason recorded"])
            notes.append(f"- {run_id}: not valid: {why}")
        if env.get("dirty"):
            notes.append(f"- {run_id}: recorded from a dirty tree.")
    return (
        "## Pilot runs (not the published figures)\n\n"
        "These pilot runs use synthetic data. They are the result files of the pilots that ran"
        " to a result, committed unedited in `docs/performance/pilots/`. They ran on earlier"
        " commits of the drivers, some on battery, and none is in the figures above; each is"
        " shown as it is.\n\n"
        + table(
            [
                "Run",
                "Commit",
                "Date (UTC)",
                "Power at start and end",
                "Valid",
                "Rate, headline delay or bytes per key",
                "Other containers' CPU, start → end (percent of one CPU)",
            ],
            rows,
        )
        + "\n\n"
        "The pilots ran on the same machine with other projects' containers busier; their rates"
        " show how much the figure depends on what else the VM is doing.\n"
        + ("\n" + "\n".join(notes) + "\n" if notes else "")
    )


def _limits(d: dict, t: dict, power_read: bool) -> str:
    par = d["env"]["flink"]["parallelism.default"]
    scorer = [(r["run_id"], _vertex_share(r, "scorer")) for r in t["runs"]]
    busy = [
        f"{pct(s['busy'])}" + (" of the window" if i == 0 else "") + f" in {run_id}"
        for i, (run_id, s) in enumerate(scorer)
        if s is not None
    ]
    cold = (
        f"Throughput was measured on a {count(t['fill']['event_minutes'])}-minute fill, so every"
        " key was scored against the fallback baselines"
        + (f"; the scorer was busy {_join(busy)}." if busy else ".")
    )
    log = (
        "the OS power log above shows it"
        if power_read
        else "the OS power log was not read for these runs"
    )
    return (
        "## What these numbers do not show\n\n"
        "All three measurements use synthetic data. They come from one laptop, with a single"
        f" broker on the same VM and parallelism {par}. Two things are read only when a run starts"
        " and when it ends: the shared VM's load, since other projects' containers run in it,"
        " and the power source. So the page does not show the load during a run, and the"
        f" drivers do not see power between the two readings; {log}. {cold} No sparse partition"
        " was measured. The delay run had no failover."
        " A host sleep steps the VM's clock, and Flink then restarts the job; a run in which that"
        " happened is discarded.\n"
    )


def _reproduce() -> str:
    return (
        "## Reproduce\n\n"
        "The data is synthetic and the generators are seeded. `make bench` runs the three"
        " targets and then renders this page:\n\n"
        "- `make bench-throughput` fills a topic, then reads it twice with the live job.\n"
        "- `make bench-delay` runs history, live traffic and injected incidents.\n"
        "- `make bench-state` runs five weeks through the job on two state backends.\n"
        "- `make power-log` reads the saved OS power log into `docs/performance/power.json`.\n"
        "- `make performance` renders this page from `docs/performance/*.json`.\n\n"
        "Run conditions: AC power, the lid open throughout, no other job on the cluster and the"
        " Flink web UI closed. `KEEP_AWAKE=1` wraps each driver in `caffeinate -is`. The report"
        " refuses a result recorded on battery, a result recorded from a dirty tree, a result"
        " marked invalid, a result that is not marked synthetic and a result whose own fields"
        " contradict its validity. It does not refuse the pilot runs, which are shown as they"
        " are.\n"
    )


def _guard(file: str, build):
    """Run `build`; a missing key or a wrong shape is a refusal naming the file and the key."""
    try:
        return build()
    except ReportRefused:
        raise
    except KeyError as e:
        raise ReportRefused(f"{file}: the result has no key {e.args[0]!r}") from e
    except (TypeError, IndexError, ValueError) as e:
        raise ReportRefused(f"{file}: the result has an unexpected shape ({e})") from e


def render(
    throughput: dict,
    delay: dict,
    state: dict,
    power: dict | None = None,
    pilots: dict[str, dict] | None = None,
) -> str:
    docs = {"throughput": throughput, "delay": delay, "state": state}
    pilots = pilots or {}
    check_results(docs)
    sections = [
        _what_this_is(),
        _guard("a result file's env record", lambda: _machine(docs)),
        _guard("power.json", lambda: _power(power, docs, pilots)),
        _guard("throughput.json", lambda: _throughput(throughput)),
        _guard("delay.json", lambda: _delay(delay)),
        _guard("state.json", lambda: _state(state, throughput["settings"])),
        _guard("state.json", lambda: _extrapolation(state)),
        _guard("a pilot file", lambda: _pilots(pilots)),
        _guard("delay.json", lambda: _limits(delay, throughput, power is not None)),
        _reproduce(),
    ]
    return "\n".join(s for s in sections if s)


def _read(path: Path, label: str):
    try:
        return json.loads(path.read_text())
    except ValueError as e:
        raise ReportRefused(f"{label}: not valid JSON ({e})") from e


def load_results(results: str | Path) -> dict:
    """The keyword arguments of `render`, read from a results directory."""
    results = Path(results)
    out: dict = {}
    for name in NAMES:
        path = results / f"{name}.json"
        if not path.is_file():
            raise ReportRefused(f"{name}.json: not found in {results}; run `make bench`")
        out[name] = _read(path, f"{name}.json")
    power = results / "power.json"
    out["power"] = _read(power, "power.json") if power.is_file() else None
    pilots = sorted((results / "pilots").glob("*.json"))
    out["pilots"] = {p.stem: _read(p, f"pilots/{p.name}") for p in pilots}
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Render docs/performance.md from the result files")
    ap.add_argument("--results", default=str(REPO / "docs" / "performance"))
    ap.add_argument("--out", default=str(REPO / "docs" / "performance.md"))
    ap.add_argument("--readme-block", action="store_true", help="print the README's table")
    ap.add_argument(
        "--extrapolation-table", action="store_true", help="print the state extrapolation table"
    )
    args = ap.parse_args(argv)
    loaded = load_results(args.results)
    if args.readme_block:
        print(readme_block(loaded["throughput"], loaded["delay"], loaded["state"]))
        return
    if args.extrapolation_table:
        check_results({k: loaded[k] for k in NAMES})
        print(extrapolation_table(loaded["state"]))
        return
    text = render(**loaded)
    Path(args.out).write_text(text)
    print(f"bench-report: wrote {args.out}")


if __name__ == "__main__":
    main()

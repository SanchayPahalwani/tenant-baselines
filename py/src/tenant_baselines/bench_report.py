"""Render docs/performance.md from the three benchmark result files. Synthetic data only.

`render` is pure: the three result dicts in, text out. Every figure is read from a result file and
every design constant from the delay file's `design` block; nothing is typed here. `main` reads
`throughput.json`, `delay.json` and `state.json` from a directory and refuses a missing file, a
result that is not valid or not synthetic, a run recorded on battery, and a result whose own fields
contradict its validity.
"""

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from tenant_baselines.freeze import REPO

__all__ = ["ReportRefused", "check_results", "main", "render"]

NAMES = ("throughput", "delay", "state")
_OFFSET_LIMIT_S = 1.0
_SLEEP_LIMIT_S = 1.0
_GIB = 1024**3
_MIB = 1024**2


class ReportRefused(ValueError):
    pass


# ---------------------------------------------------------------- checks


def _runs_of(name: str, doc: dict) -> list[tuple[str, dict]]:
    """The parts of a result that carry a run's own validity fields."""
    if name == "delay":
        return [("the run", doc)]
    return [(r.get("run_id", r.get("backend", "a run")), r) for r in doc.get("runs", [])]


def _contradictions(name: str, doc: dict) -> list[str]:
    out = []
    for label, r in _runs_of(name, doc):
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
        row("Images", lambda e: ", ".join(e["images"])),
        row("Flink version", lambda e: e["flink"]["version"]),
        row("Parallelism", lambda e: e["flink"]["parallelism.default"]),
        row("Task slots", lambda e: e["flink"]["taskmanager.numberOfTaskSlots"]),
        row("TaskManager memory", lambda e: e["flink"]["taskmanager.memory.process.size"]),
        row("Power", lambda e: e["power"] or "not recorded"),
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
        "The state runs' backend and checkpoint interval are in the state-size section, not in "
        "this table.\n"
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
    ticks = "; ".join(f"{r['run_id']}: {pct(r['drop_ticks_busy'])}" for r in runs)
    errors = "; ".join(f"{r['run_id']}: {n(r['timing_error_pct'])}%" for r in runs)
    alert_keys = sorted({k for r in runs for k in r["alert_rows"]})
    alerts = table(
        ["Detector / setting / scope", *(r["run_id"] for r in runs)],
        [[k, *(count(r["alert_rows"].get(k, 0)) for r in runs)] for k in alert_keys],
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
        f" first {count(t['warmup_s'])} s after the counter moved and the last"
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
        f"The two runs differ by {n(t['spread_pct'])}% of their mean. The first job on a"
        " TaskManager is the colder one. The source's own clock is a cross-check that does not"
        " depend on the host's timing. A source that is neither busy nor back-pressured would be"
        " waiting on the broker; the idle share above shows whether that happened.\n\n"
        f"`timing_error_pct` is a bound, computed from the longest measured read delay at the"
        f" window's two ends over the window: {errors}.\n\n"
        f"The busiest vertex: {busiest}.\n\n"
        "The `drop-ticks` vertex's busy share, as a share of parallelism times the window, is"
        f" {ticks}. It is reported as a share of CPU time, not as a"
        " throughput difference. The drop-ticks task's busy time also covers handing each event"
        " on to the keyed shuffle, work the source task would do itself without the union, so"
        " this share is an upper bound on the extra hop's cost.\n\n"
        f"The job ran {count(t['settings'])} settings at once. The alert rows each run wrote,"
        f" counted from `bench.alerts`:\n\n{alerts}\n\n"
        "The TaskManager's memory against its limit. The first reading is for one job; the"
        " second is cumulative, taken after the TaskManager had already run the first job:\n\n"
        f"{mem}\n\n"
        f"The last checkpoint's size per vertex, in bytes, at {count(f['keys'])} keys:\n\n{ck}\n\n"
        "The baselines are cold after the fill's event time, and no unit/tenant pair goes stale"
        " in this world, so these sizes are not the state of a warmed-up job.\n\n"
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
        f"The world has {count(w['keys'])} keys at up to {count(w['rpm_per_key'])} requests a"
        f" minute per key, in {count(w['partitions'])} partitions, with"
        f" {count(w['history_days'])} days of history before live traffic."
        f" The run injected {count(d['incidents'])} incidents, each"
        f" an error-rate rise of {n(sch['magnitude'] * 100, 0)} points on one key for"
        f" {count(sch['duration_s'])} s. The first starts {count(sch['settle_s'])} s after the"
        f" generator catches up; incidents in one region are {count(sch['spacing_s'])} s apart;"
        f" the run reads for {count(sch['tail_s'])} s after the last one ends.\n\n"
        "Pipeline delay is the time from a row's event time to the broker's append time of that"
        " row in `bench.alerts`. Time-to-page is the time from the incident's start to the append"
        " of the first matching row. Percentiles are nearest-rank, with their `n`. Times are in"
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
        f" `group_wait` ({count(dz['group_wait_s'])} s, configured) come after it. The README's"
        " milestone 3 page times were measured at the webhook, from the incident's start minute,"
        " and are not comparable.\n\n"
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
            out += "The figure moves because SST files that are not yet compacted are counted.\n"
    return out


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
        " aggregates mode.\n\n" + "\n".join(parts) + "\n"
        f"This job ran against {count(s['settings'])} settings; the live job runs"
        f" {count(throughput_settings)}. The baseline history, which all settings share, is most"
        " of a key's state.\n"
    )


def _extrapolation(s: dict) -> str:
    ex = s["extrapolation"]
    backends = [b for b in ex if b != "label"]
    keys = [r["keys"] for r in ex[backends[0]]]
    rows = [
        [count(k), *(count(next(r["bytes"] for r in ex[b] if r["keys"] == k)) for b in backends)]
        for k in keys
    ]
    return (
        "## Extrapolation, not measured\n\n"
        "This section is an extrapolation from synthetic measurements, not a measurement. Bytes"
        " are the scorer's bytes per key times the key count.\n\n"
        + table(["Keys", *(f"{b} (bytes)" for b in backends)], rows)
        + "\n\n"
        "It assumes every key has a full"
        f" {count(s['world']['weeks'])}-week history and that state grows linearly with keys."
        " Neither was tested above the measured key count. It leaves out the region grouper's"
        " per-key activity, the minute aggregator's state in events mode and the population"
        " baselines.\n"
    )


def _limits(d: dict) -> str:
    par = d["env"]["flink"]["parallelism.default"]
    return (
        "## What these numbers do not show\n\n"
        "All three measurements use synthetic data. They come from one laptop, with a single"
        f" broker on the same VM and parallelism {par}. Throughput was measured with cold"
        " baselines. No sparse partition was measured. The delay run had no failover. A host"
        " sleep steps the VM's clock, and Flink then restarts the job; a run in which that"
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
        "- `make performance` renders this page from `docs/performance/*.json`.\n\n"
        "Run conditions: AC power, the lid open throughout, no other job on the cluster and the"
        " Flink web UI closed. `KEEP_AWAKE=1` wraps each driver in `caffeinate -is`. The report"
        " refuses a result recorded on battery, a result marked invalid and a result that is not"
        " marked synthetic.\n"
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


def render(throughput: dict, delay: dict, state: dict) -> str:
    docs = {"throughput": throughput, "delay": delay, "state": state}
    check_results(docs)
    return "\n".join(
        [
            _what_this_is(),
            _guard("a result file's env record", lambda: _machine(docs)),
            _guard("throughput.json", lambda: _throughput(throughput)),
            _guard("delay.json", lambda: _delay(delay)),
            _guard("state.json", lambda: _state(state, throughput["settings"])),
            _guard("state.json", lambda: _extrapolation(state)),
            _guard("delay.json", lambda: _limits(delay)),
            _reproduce(),
        ]
    )


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Render docs/performance.md from the result files")
    ap.add_argument("--results", default=str(REPO / "docs" / "performance"))
    ap.add_argument("--out", default=str(REPO / "docs" / "performance.md"))
    args = ap.parse_args(argv)
    docs = {}
    for name in NAMES:
        path = Path(args.results) / f"{name}.json"
        if not path.is_file():
            raise ReportRefused(f"{name}.json: not found in {args.results}; run `make bench`")
        try:
            docs[name] = json.loads(path.read_text())
        except ValueError as e:
            raise ReportRefused(f"{name}.json: not valid JSON ({e})") from e
    text = render(docs["throughput"], docs["delay"], docs["state"])
    Path(args.out).write_text(text)
    print(f"bench-report: wrote {args.out}")


if __name__ == "__main__":
    main()

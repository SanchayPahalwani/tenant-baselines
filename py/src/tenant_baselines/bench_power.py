"""The power source during each published bench run, from the OS's own power log.

The drivers read `pmset -g ps` only when a run starts and when it ends. macOS also logs every
change of power source (`pmset -g log`: a "Summary" line that says `Using AC` or `Using Batt`),
but that log rotates. An excerpt of it was saved beside each published run's result
(`data/bench/<run_id>/pmset-log-excerpt.txt`). `save_logs` copies the power-source lines of each
excerpt, unedited, to `docs/performance/power/<run_id>.log`; the excerpt's other lines name the
programs that ran and are not kept. `build` reads those logs into `docs/performance/power.json`:
for each published run, its start and end and every power-source interval that overlaps it.

A run's start is its result's `env.date_utc`. Its end is the time the driver wrote its
`result.json`, which is the driver's last step; the result files do not record an end time.
Run with `make power-log`.
"""

import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from tenant_baselines.freeze import REPO

__all__ = [
    "SOURCE",
    "build",
    "changes",
    "covered",
    "extract",
    "intervals",
    "main",
    "overlapping",
    "parse",
    "save_logs",
]

NAMES = ("throughput", "delay", "state")
SOURCE = "pmset -g log: the lines that say Using AC or Using Batt"
EXCERPT = "pmset-log-excerpt.txt"
_STAMP = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4})"
_SOURCE_LINE = re.compile(_STAMP + r" .*\bUsing (AC|Batt)\(Charge: (\d+)\)")
_READ_LINE = re.compile(_STAMP + r" : Showing all currently held IOKit power assertions")
_SOURCES = {"AC": "AC Power", "Batt": "Battery Power"}


def _at(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S %z")


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def extract(text: str) -> str:
    """The lines of a `pmset -g log` excerpt that this module reads, unedited: every power-source
    line and the line `pmset` prints when the log is read."""
    keep = [x for x in text.splitlines() if _SOURCE_LINE.match(x) or _READ_LINE.match(x)]
    return "".join(x + "\n" for x in keep)


def parse(text: str) -> dict:
    """The power-source readings of a log, in order, the time of the first and the time the log
    was read (None when the log does not say)."""
    readings, read_at = [], None
    for line in text.splitlines():
        if m := _SOURCE_LINE.match(line):
            at = _at(m.group(1)).isoformat()
            readings.append({"at": at, "source": _SOURCES[m.group(2)], "charge_pct": int(m[3])})
        elif m := _READ_LINE.match(line):
            read_at = _at(m.group(1)).isoformat()
    if not readings:
        raise ValueError("the log has no line that says Using AC or Using Batt")
    return {"readings": readings, "first_at": readings[0]["at"], "read_at": read_at}


def changes(readings: list[dict]) -> list[dict]:
    """Every reading whose source differs from the one before it."""
    return [b for a, b in zip(readings, readings[1:], strict=False) if b["source"] != a["source"]]


def intervals(parsed: dict) -> list[dict]:
    """The log as intervals of one source each: from the first reading or a change, to the next
    change or to the time the log was read."""
    starts = [parsed["readings"][0], *changes(parsed["readings"])]
    ends = [s["at"] for s in starts[1:]] + [parsed["read_at"]]
    return [
        {"source": s["source"], "from": s["at"], "to": e} for s, e in zip(starts, ends, strict=True)
    ]


def overlapping(ivs: list[dict], start: str, end: str) -> list[dict]:
    """The intervals that overlap `[start, end]`, each with the seconds of the overlap."""
    a, b = _dt(start), _dt(end)
    out = []
    for iv in ivs:
        lo, hi = _dt(iv["from"]), _dt(iv["to"]) if iv["to"] else b
        if lo < b and hi > a:
            seconds = (min(hi, b) - max(lo, a)).total_seconds()
            out.append({**iv, "overlap_s": round(seconds, 1)})
    return out


def covered(parsed: dict, start: str, end: str) -> bool:
    """Whether the log spans the whole of `[start, end]`."""
    if parsed["read_at"] is None:
        return False
    return _dt(parsed["first_at"]) <= _dt(start) and _dt(end) <= _dt(parsed["read_at"])


def _run_dir(name: str, results: Path, runs: Path) -> Path:
    """The driver's run directory whose `result.json` is the published file, byte for byte."""
    text = (Path(results) / f"{name}.json").read_text()
    found = [
        d
        for d in sorted(Path(runs).iterdir())
        if (d / "result.json").is_file() and (d / "result.json").read_text() == text
    ]
    if len(found) != 1:
        raise ValueError(
            f"{name}.json: no run directory in {runs} holds this result"
            if not found
            else f"{name}.json: {len(found)} run directories hold this result"
        )
    return found[0]


def save_logs(results: Path, runs: Path, logs: Path) -> None:
    """Write `<logs>/<run_id>.log`, the power-source lines of each published run's excerpt."""
    Path(logs).mkdir(parents=True, exist_ok=True)
    for name in NAMES:
        run_dir = _run_dir(name, results, runs)
        excerpt = run_dir / EXCERPT
        if not excerpt.is_file():
            raise ValueError(f"{name}.json: no {EXCERPT} in {run_dir}")
        (Path(logs) / f"{run_dir.name}.log").write_text(extract(excerpt.read_text()))


def _incidents(doc: dict, run_dir: Path, ivs: list[dict], end: str, tz) -> dict | None:
    """When the delay run could first append a page matched to an incident: the first incident's
    start (from the run's labels) plus the shortest time-to-page of any setting. A battery
    interval after that moment means the records cannot rule a page on battery out (None)."""
    labels = run_dir / "labels.json"
    shortest = [
        m["time_to_page_s"]["min"]
        for m in doc["by_detector"].values()
        if m["time_to_page_s"]["min"] is not None
    ]
    if not labels.is_file() or not shortest:
        return None
    starts = [i["start_ms"] for i in json.loads(labels.read_text())["incidents"]]
    first = datetime.fromtimestamp(min(starts) / 1000, tz)
    earliest = first + timedelta(seconds=min(shortest))
    after = [x for x in overlapping(ivs, earliest.isoformat(), end) if x["source"] != "AC Power"]
    caught_up = datetime.fromtimestamp(doc["generator"]["caught_up_ms"] / 1000, tz)
    return {
        "caught_up": caught_up.isoformat(),
        "settle_s": doc["schedule"]["settle_s"],
        "first_incident_start": first.isoformat(),
        "shortest_time_to_page_s": min(shortest),
        "earliest_page": earliest.isoformat(),
        "matched_page_on_battery": None if after else False,
    }


def build(results: Path, logs: Path, runs: Path) -> dict:
    out: dict = {"source": SOURCE, "runs": {}}
    dirs = {name: _run_dir(name, results, runs) for name in NAMES}  # all found before any read
    for name, run_dir in dirs.items():
        run_id = run_dir.name
        parsed = parse((Path(logs) / f"{run_id}.log").read_text())
        tz = _dt(parsed["first_at"]).tzinfo
        doc = json.loads((run_dir / "result.json").read_text())
        start = _dt(doc["env"]["date_utc"]).astimezone(tz).isoformat()
        written = int((run_dir / "result.json").stat().st_mtime)  # to the second, as the start
        end = datetime.fromtimestamp(written, tz).isoformat()
        ivs = intervals(parsed)
        run = {
            "run_id": run_id,
            "log": f"{Path(logs).name}/{run_id}.log",
            "log_from": parsed["first_at"],
            "log_read_at": parsed["read_at"],
            "start": start,
            "end": end,
            "end_from": f"the write time of data/bench/{run_id}/result.json",
            "covered_by_log": covered(parsed, start, end),
            "intervals": overlapping(ivs, start, end),
        }
        if name == "delay" and (inc := _incidents(doc, run_dir, ivs, end, tz)) is not None:
            run["incidents"] = inc
        out["runs"][name] = run
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="The OS power log during the published bench runs")
    ap.add_argument("--results", default=str(REPO / "docs" / "performance"))
    ap.add_argument("--logs", default=str(REPO / "docs" / "performance" / "power"))
    ap.add_argument("--runs", default=str(REPO / "data" / "bench"))
    ap.add_argument("--out", default=str(REPO / "docs" / "performance" / "power.json"))
    ap.add_argument(
        "--save-logs",
        action="store_true",
        help=f"first copy the power-source lines of each run's {EXCERPT} to --logs",
    )
    args = ap.parse_args(argv)
    if args.save_logs:
        save_logs(Path(args.results), Path(args.runs), Path(args.logs))
    doc = build(Path(args.results), Path(args.logs), Path(args.runs))
    text = json.dumps(doc, indent=2, sort_keys=True, allow_nan=False) + "\n"
    Path(args.out).write_text(text)
    print(f"bench-power: wrote {args.out}")


if __name__ == "__main__":
    main()

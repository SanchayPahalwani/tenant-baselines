import json
import os
from datetime import UTC, datetime

import pytest

from tenant_baselines import bench_power as bp

# Three lines of the OS power log saved during the sitting, as `pmset -g log` printed them.
AC_1 = (
    "2026-11-12 20:55:02 +0530 Assertions          \tSummary- [System: PrevIdle DeclUser kDisp]"
    " Using AC(Charge: 59)          "
)
BATT = (
    "2026-11-12 21:21:33 +0530 Assertions          \tSummary- [System: PrevIdle PrevDisp DeclUser"
    " kDisp] Using Batt(Charge: 89)          "
)
AC_2 = (
    "2026-11-12 21:27:03 +0530 Assertions          \tSummary- [System: PrevIdle DeclUser"
    " IntPrevDisp kDisp] Using AC(Charge: 89)          "
)
AC_REPEAT = (
    "2026-11-12 21:46:47 +0530 Assertions          \tSummary- [System: PrevIdle]"
    " Using AC(Charge: 95)          "
)
READ = "2026-11-12 23:11:47 +0530 : Showing all currently held IOKit power assertions"
OTHER = (
    "2026-11-12 21:02:00 +0530 Assertions          \tPID 109(powerd) TurnedOn"
    ' InternalPreventDisplaySleep "x" 00:00:00  id:0x0x1 [System: PrevIdle kDisp]          '
)


def log(*lines):
    return "".join(line + "\n" for line in lines)


def test_the_three_real_lines_give_two_changes():
    p = bp.parse(log(AC_1, BATT, AC_2))
    assert [r["source"] for r in p["readings"]] == ["AC Power", "Battery Power", "AC Power"]
    assert [r["charge_pct"] for r in p["readings"]] == [59, 89, 89]
    assert bp.changes(p["readings"]) == [
        {"at": "2026-11-12T21:21:33+05:30", "source": "Battery Power", "charge_pct": 89},
        {"at": "2026-11-12T21:27:03+05:30", "source": "AC Power", "charge_pct": 89},
    ]


def test_a_repeated_source_is_not_a_change_and_other_lines_are_skipped():
    p = bp.parse(log(AC_1, OTHER, BATT, AC_2, AC_REPEAT, READ))
    assert len(p["readings"]) == 4 and len(bp.changes(p["readings"])) == 2
    assert p["read_at"] == "2026-11-12T23:11:47+05:30"
    assert p["first_at"] == "2026-11-12T20:55:02+05:30"


def test_a_log_without_a_power_source_line_is_refused():
    with pytest.raises(ValueError, match="no line"):
        bp.parse(log(OTHER, READ))


def test_extract_keeps_the_power_source_lines_and_the_read_line_unedited():
    raw = log(AC_1, OTHER, BATT, OTHER, AC_2, READ)
    assert bp.extract(raw) == log(AC_1, BATT, AC_2, READ)


def test_intervals_run_from_change_to_change_and_the_last_to_the_read():
    p = bp.parse(log(AC_1, BATT, AC_2, AC_REPEAT, READ))
    day = "2026-11-12T"
    assert bp.intervals(p) == [
        {"source": "AC Power", "from": f"{day}20:55:02+05:30", "to": f"{day}21:21:33+05:30"},
        {"source": "Battery Power", "from": f"{day}21:21:33+05:30", "to": f"{day}21:27:03+05:30"},
        {"source": "AC Power", "from": f"{day}21:27:03+05:30", "to": f"{day}23:11:47+05:30"},
    ]


def test_overlapping_gives_each_interval_inside_the_run_with_its_seconds():
    ivs = bp.intervals(bp.parse(log(AC_1, BATT, AC_2, READ)))
    # the delay run: 21:16:46 to 22:34:54 in the log's local time
    got = bp.overlapping(ivs, "2026-11-12T15:46:46+00:00", "2026-11-12T22:34:54+05:30")
    assert [(x["source"], x["overlap_s"]) for x in got] == [
        ("AC Power", 287.0),
        ("Battery Power", 330.0),
        ("AC Power", 4071.0),
    ]
    # a run wholly inside one interval
    got = bp.overlapping(ivs, "2026-11-12T15:32:47+00:00", "2026-11-12T21:16:44+05:30")
    assert [(x["source"], x["overlap_s"]) for x in got] == [("AC Power", 837.0)]


def test_a_run_outside_the_log_is_not_covered():
    p = bp.parse(log(AC_1, BATT, READ))
    assert bp.covered(p, "2026-11-12T15:32:47+00:00", "2026-11-12T21:30:00+05:30")
    assert not bp.covered(p, "2026-11-12T15:00:00+00:00", "2026-11-12T21:30:00+05:30")
    assert not bp.covered(p, "2026-11-12T15:32:47+00:00", "2026-11-12T23:30:00+05:30")


# ---- the whole script, on files ----


def published(name, date_utc, **extra):
    return {"data": "synthetic", "env": {"date_utc": date_utc}, "name": name, **extra}


def delay_doc():
    t0 = int(datetime(2026, 11, 12, 15, 50, 54, tzinfo=UTC).timestamp() * 1000)
    return published(
        "delay",
        "2026-11-12T15:46:46+00:00",
        generator={"caught_up_ms": t0, "failed_deliveries": 0},
        schedule={"settle_s": 300},
        by_detector={
            "seasonal/k4": {"time_to_page_s": {"min": 280.0}},
            "global_static/p99.9": {"time_to_page_s": {"min": 269.4}},
        },
    )


def setup_files(tmp_path, write_labels=True):
    results, runs, logs = tmp_path / "performance", tmp_path / "bench", tmp_path / "power"
    for d in (results, runs, logs):
        d.mkdir()
    docs = {
        "throughput": ("tp-x", published("throughput", "2026-11-12T15:32:47+00:00"), "21:16:44"),
        "delay": ("delay-x", delay_doc(), "22:34:54"),
        "state": ("state-x", published("state", "2026-11-12T17:04:55+00:00"), "22:38:35"),
    }
    for name, (run_id, doc, end) in docs.items():
        text = json.dumps(doc, indent=2, sort_keys=True) + "\n"
        (results / f"{name}.json").write_text(text)
        (runs / run_id).mkdir()
        (runs / run_id / "result.json").write_text(text)
        at = datetime.fromisoformat(f"2026-11-12T{end}+05:30").timestamp()
        os.utime(runs / run_id / "result.json", (at, at))
        (runs / run_id / "pmset-log-excerpt.txt").write_text(log(AC_1, OTHER, BATT, AC_2, READ))
    # a pilot beside them, whose result is not a published file
    (runs / "tp-pilot").mkdir()
    (runs / "tp-pilot" / "result.json").write_text("{}\n")
    if write_labels:
        start = int(datetime(2026, 11, 12, 15, 56, 40, tzinfo=UTC).timestamp() * 1000)
        incidents = [{"start_ms": start + 60_000}, {"start_ms": start}]
        (runs / "delay-x" / "labels.json").write_text(json.dumps({"incidents": incidents}))
    return results, runs, logs


def test_build_finds_each_runs_directory_by_its_result_and_lists_the_overlaps(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    bp.save_logs(results, runs, logs)
    out = bp.build(results, logs, runs)
    assert out["source"] == "pmset -g log: the lines that say Using AC or Using Batt"
    tp, d, s = (out["runs"][k] for k in ("throughput", "delay", "state"))
    assert (tp["run_id"], d["run_id"], s["run_id"]) == ("tp-x", "delay-x", "state-x")
    assert tp["log"] == "power/tp-x.log" and tp["covered_by_log"] is True
    assert tp["start"] == "2026-11-12T21:02:47+05:30" and tp["end"] == "2026-11-12T21:16:44+05:30"
    assert "result.json" in tp["end_from"]
    assert [x["source"] for x in tp["intervals"]] == ["AC Power"]
    assert [x["source"] for x in d["intervals"]] == ["AC Power", "Battery Power", "AC Power"]
    assert [x["source"] for x in s["intervals"]] == ["AC Power"]
    assert s["intervals"][0]["from"] == "2026-11-12T21:27:03+05:30"


def test_build_records_when_the_delay_run_could_first_page(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    bp.save_logs(results, runs, logs)
    inc = bp.build(results, logs, runs)["runs"]["delay"]["incidents"]
    assert inc["caught_up"] == "2026-11-12T21:20:54+05:30"
    assert inc["settle_s"] == 300
    assert inc["first_incident_start"] == "2026-11-12T21:26:40+05:30"
    assert inc["shortest_time_to_page_s"] == 269.4  # the shortest over every setting
    assert inc["earliest_page"] == "2026-11-12T21:31:09.400000+05:30"
    assert inc["matched_page_on_battery"] is False


def test_a_page_window_that_overlaps_battery_is_not_ruled_out(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    late_batt = BATT.replace("21:21:33", "21:40:00")
    late_ac = AC_2.replace("21:27:03", "21:45:00")
    for run_id in ("tp-x", "delay-x", "state-x"):
        (logs / f"{run_id}.log").write_text(log(AC_1, late_batt, late_ac, READ))
    inc = bp.build(results, logs, runs)["runs"]["delay"]["incidents"]
    assert inc["matched_page_on_battery"] is None


def test_without_labels_the_delay_run_has_no_incident_block(tmp_path):
    results, runs, logs = setup_files(tmp_path, write_labels=False)
    bp.save_logs(results, runs, logs)
    assert "incidents" not in bp.build(results, logs, runs)["runs"]["delay"]


def test_save_logs_writes_the_extract_of_each_published_runs_excerpt(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    bp.save_logs(results, runs, logs)
    assert sorted(p.name for p in logs.iterdir()) == ["delay-x.log", "state-x.log", "tp-x.log"]
    assert (logs / "delay-x.log").read_text() == log(AC_1, BATT, AC_2, READ)


def test_a_published_file_with_no_run_directory_is_refused(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    (runs / "state-x" / "result.json").write_text("{}\n")
    with pytest.raises(ValueError, match=r"state\.json.*no run directory"):
        bp.build(results, logs, runs)


def test_main_writes_power_json(tmp_path):
    results, runs, logs = setup_files(tmp_path)
    out = tmp_path / "power.json"
    argv = ["--results", str(results), "--logs", str(logs), "--runs", str(runs)]
    bp.main([*argv, "--out", str(out), "--save-logs"])
    doc = json.loads(out.read_text())
    assert set(doc["runs"]) == {"throughput", "delay", "state"}
    assert out.read_text() == json.dumps(doc, indent=2, sort_keys=True) + "\n"

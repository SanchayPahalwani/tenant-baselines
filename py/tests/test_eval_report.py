import copy
import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from tenant_baselines import eval_report
from tenant_baselines.eval_report import (
    CURVE_HEADING,
    STATEMENT,
    by_scenario_type,
    check_official,
    check_passed,
    curve,
    first_run_entry,
    headline,
    load_run_log,
    load_summaries,
    outage_rows,
    render,
)
from tenant_baselines.scoring import FAMILIES, wilson

BANNED = ("spearheaded", "leveraged", "synergy", "cutting-edge", "results-driven", "passionate")
EPOCH = 1_767_571_200_000
MACHINE = {
    "source": "a test fixture",
    "cpu": "Test CPU",
    "cores": 8,
    "memory_gib": 16.0,
    "docker_version": "29.0",
    "docker_cpus": 6,
    "docker_memory_gib": 8.0,
    "python": "3.12.0",
    "numpy": "2.0.0",
}
RENDERED = {"commit": "abcdef0123", "date": "2026-11-12"}


def entry(sid, typ, should=True, expected=True, any_=True, delay=5.0, regional=False, fp=0):
    return {
        "id": sid,
        "type": typ,
        "should_page": should,
        "detected_any": any_ if should else False,
        "detected_expected": expected if should else False,
        "delay_min": delay if should and expected else None,
        "delay_any_min": delay if should and any_ else None,
        "pages": 2 if any_ and should else fp,
        "regional_formed": regional,
        "false_pages_during": fp,
        "refires": 1 if should and any_ else 0,
    }


def score(fam, setting, expected, any_, false, pages=20):
    return {
        "detector": fam,
        "setting": setting,
        "week": 5,
        "n_should_page": 10,
        "detected_any": any_,
        "detected_expected": expected,
        "pages": pages,
        "matched_pages": pages - false,
        "false_pages": false,
        "false_pages_per_day": false / 7,
        "refires": 1,
        "pages_per_detected": 1.5,
        "median_delay_min": 5.0,
        "delays_by_type": {},
        "scenarios": [
            entry("w5-regional_outage-1", "regional_outage", regional=True),
            entry("w5-error_burst-1", "error_burst", expected=expected >= 8, delay=6.0),
            entry("w5-launch_surge-1", "launch_surge", should=False, fp=1),
        ],
    }


def summary(seed, amp, expected, any_, false=14):
    weeks5 = {
        fam: {st: score(fam, st, expected, any_, false) for st in settings}
        for fam, settings in FAMILIES.items()
    }
    sel = {
        fam: {"setting": settings[0], "branch": "reached_8_of_10"}
        for fam, settings in FAMILIES.items()
    }
    return {
        "meta": {
            "kind": "dev",
            "world": "eval",
            "seed": seed,
            "amplitude": amp,
            "epoch_ms": EPOCH,
            "replay_end_ms": EPOCH + 5 * 604_800_000,
            "data": "synthetic",
        },
        "weeks": {"4": weeks5, "5": weeks5},
        "selection": sel,
        "selected_week5": {fam: weeks5[fam][sel[fam]["setting"]] for fam in FAMILIES},
        "open_alerts": 2,
        "normalise": {"clipped": 1, "duplicates": 0},
        "synthetic": True,
        "run_id": f"dev-s{seed}-a{amp:g}",
    }


@pytest.fixture
def summaries():
    out = []
    for amp in (1.5, 6.0, 12.0):
        out += [summary(7, amp, 8, 9), summary(11, amp, 6, 8)]
    return out


CHECK = {
    "commit": "0d93f8b5bf",
    "date_utc": "2026-11-11T23:19:33Z",
    "world": "paths (synthetic)",
    "events": 100,
    "aggregate_rows": 50,
    "alert_rows": 10,
    "region_firing_rows": 2,
    "alerts_identical": True,
    "aggregates_identical": True,
    "seconds": {
        "generate": 0.5,
        "produce": 0.5,
        "events_job": 1.0,
        "aggregates_job": 2.0,
        "total": 4.5,
    },
}
FREEZE = {
    "official_seeds": [101, 102, 103, 104, 105],
    "amplitudes": [1.5, 6.0, 12.0],
    "main_amplitude": 6.0,
    "commit": "1234567890" + "0" * 30,
}


def test_headline_median_range_and_pooled_counts(summaries):
    h = headline(summaries, 6.0)["seasonal"]
    e = h["detected_expected"]
    assert (e["median"], e["min"], e["max"]) == (7, 6, 8)
    assert (e["k"], e["n"]) == (14, 20)
    assert (e["lo"], e["hi"]) == wilson(14, 20)
    a = h["detected_any"]
    assert (a["k"], a["n"]) == (17, 20)
    assert h["pooled_false_per_day"] == 28 / 14
    assert [x["seed"] for x in h["seeds"]] == [7, 11]
    assert h["seeds"][0]["branch"] == "reached_8_of_10"


def test_curve_covers_every_setting_with_pooled_precision(summaries):
    c = curve(summaries, 6.0)
    assert len(c) == sum(len(v) for v in FAMILIES.values())
    first = c[0]
    assert first["pages_per_day"] == 40 / 14
    assert first["false_per_day"] == 28 / 14
    assert (first["matched"], first["pages"]) == (12, 40)
    assert (first["any"]["k"], first["any"]["n"]) == (17, 20)


def test_by_scenario_type_pools_selected_settings(summaries):
    rows = {(r["family"], r["type"]): r for r in by_scenario_type(summaries, 6.0)}
    eb = rows[("seasonal", "error_burst")]
    assert (eb["expected"]["k"], eb["expected"]["n"]) == (1, 2)
    assert eb["delay_n"] == 1 and eb["p50"] == 6.0
    ls = rows[("seasonal", "launch_surge")]
    assert not ls["should_page"] and ls["false_during"] == 2


def test_outage_rows_carry_the_hour_from_labels(summaries):
    s = summaries[2]  # seed 7 at 6:1
    s["_labels"] = {
        "w5-regional_outage-1": {
            "start_ms": EPOCH + 4 * 604_800_000 + 86_400_000 + 14 * 3_600_000 + 20 * 60_000,
            "keys": ["t01|eu-central|eu-central-s1"],
        }
    }
    row = outage_rows(summaries, 6.0)[0]
    assert row["region"] == "eu-central"
    assert row["utc"] == "Tue 14:20" and row["local"] == "Tue 15:20"


def test_check_paths_passed_needs_all_five_conditions():
    assert check_passed(CHECK)[0]
    for key, bad in [
        ("alerts_identical", False),
        ("aggregates_identical", False),
        ("alert_rows", 0),
        ("region_firing_rows", 0),
    ]:
        ok, why = check_passed(CHECK | {key: bad})
        assert not ok and why


def text(summaries, log=(), check=CHECK, freeze=FREEZE, **kw):
    return render(summaries, list(log), check, freeze, MACHINE, rendered=RENDERED, **kw)


def test_recall_is_never_a_bare_percentage(summaries):
    t = text(summaries)
    assert "14/20" in t
    assert "%" not in t


def test_statement_and_label_are_present(summaries):
    t = text(summaries)
    assert STATEMENT in t
    assert f"## {CURVE_HEADING}" in t
    assert "synthetic" in t.split("\n\n")[1]


def test_banned_words_absent(summaries):
    t = text(summaries).lower()
    assert not [w for w in BANNED if w in t]


def test_run_log_is_rendered_in_order(summaries):
    log = [
        {"commit": "aaaaaaa1", "date": "2026-11-20", "what": "First entry.", "why": "Reason one."},
        {"commit": "bbbbbbb2", "date": "2026-11-21", "what": "Second entry.", "why": "Reason two."},
    ]
    t = text(summaries, log)
    assert t.index("First entry.") < t.index("Second entry.")
    assert t.index("aaaaaaa") < t.index("bbbbbbb")


def test_sections_come_in_the_contracted_order(summaries):
    t = text(summaries)
    heads = [
        "## Setup",
        "## Headline",
        "## Sweep",
        f"## {CURVE_HEADING}",
        "## By scenario type",
        "## Event-path equivalence check",
        "## Method notes",
        "## Run log",
    ]
    pos = [t.index(h) for h in heads]
    assert pos == sorted(pos)
    assert "`make eval`" in t and "abcdef0" in t and "Test CPU" in t and "Docker 29.0" in t
    assert "16.0 GiB" in t and "8.0 GiB" in t and "Python 3.12.0; numpy 2.0.0" in t


def test_failed_check_paths_is_rendered_with_its_reason(summaries):
    t = text(summaries, check=CHECK | {"region_firing_rows": 0})
    assert "failed: no regional alert fired" in t
    assert "passed" in text(summaries)


def test_missing_amplitude_and_freeze_render_without_error(summaries):
    only = [s for s in summaries if s["meta"]["amplitude"] == 6.0]
    t = text(only, freeze=None, check=None)
    assert "no runs yet" in t
    assert "not frozen yet" in t
    assert "has not been run" in t


def test_method_notes_keep_the_ruled_figures(summaries):
    t = text(summaries)
    for needle in ("FLINK-39728", "5.1.0", "5.0.0-2.2", "48 keys", "42"):
        assert needle in t


def test_load_summaries_attaches_labels_and_orders(tmp_path):
    for seed, amp in ((11, 6.0), (7, 6.0), (7, 1.5)):
        d = tmp_path / f"dev-s{seed}-a{amp:g}"
        d.mkdir()
        (d / "summary.json").write_text(json.dumps(summary(seed, amp, 8, 9)))
        (d / "labels.json").write_text(
            json.dumps({"scenarios": [{"id": "x", "keys": [], "start_ms": 0}]})
        )
    got = load_summaries(tmp_path)
    assert [(s["meta"]["amplitude"], s["meta"]["seed"]) for s in got] == [
        (1.5, 7),
        (6.0, 7),
        (6.0, 11),
    ]
    assert "x" in got[0]["_labels"]


def test_load_run_log_reads_lines_and_tolerates_absence(tmp_path):
    assert load_run_log(tmp_path / "none.jsonl") == []
    p = tmp_path / "log.jsonl"
    p.write_text('{"what": "a"}\n\n{"what": "b"}\n')
    assert [e["what"] for e in load_run_log(p)] == ["a", "b"]


def test_latest_per_cell_keeps_the_newest_rerun():
    old = summary(7, 6.0, 5, 5) | {"run_id": "dev-s7-a6-20261111T100000"}
    new = summary(7, 6.0, 8, 9) | {"run_id": "dev-s7-a6-20261111T230000"}
    other = summary(11, 6.0, 6, 8)
    got = eval_report.latest_per_cell([old, other, new])
    assert [(s["meta"]["seed"], s["run_id"]) for s in got] == [
        (7, "dev-s7-a6-20261111T230000"),
        (11, "dev-s11-a6"),
    ]


def test_false_pages_per_day_are_two_decimals(summaries):
    assert "2.00 (2.00–2.00)" in text(summaries)


def test_headline_median_of_an_even_count_is_the_midpoint():
    h = headline([summary(7, 6.0, 8, 9), summary(11, 6.0, 5, 6)], 6.0)["seasonal"]
    assert h["detected_expected"]["median"] == 6.5


def test_outage_hour_wraps_across_the_week(summaries):
    s = summaries[2]
    # Sunday 23:30 UTC in ap-southeast (+8) is Monday 07:30 local.
    s["_labels"] = {
        "w5-regional_outage-1": {
            "start_ms": EPOCH + 4 * 604_800_000 + 6 * 86_400_000 + 23 * 3_600_000 + 30 * 60_000,
            "keys": ["t01|ap-southeast|ap-southeast-s1"],
        }
    }
    row = outage_rows(summaries, 6.0)[0]
    assert row["utc"] == "Sun 23:30" and row["local"] == "Mon 07:30"


def test_by_scenario_type_with_no_expected_signal_delays():
    s = summary(7, 6.0, 8, 9)
    for fam in FAMILIES:
        for e in s["selected_week5"][fam]["scenarios"]:
            if e["type"] == "error_burst":
                e["detected_expected"] = False
                e["delay_min"] = None
    row = next(r for r in by_scenario_type([s], 6.0) if r["type"] == "error_burst")
    assert (row["delay_n"], row["p50"], row["p95"]) == (0, None, None)
    assert "none (n=0)" in render([s], [], CHECK, FREEZE, MACHINE, rendered=RENDERED)


def test_plural_n_a_and_seed_count_of_scenarios(summaries):
    t = text(summaries)
    assert "(should not page; 2 scenarios)" in t
    one = summary(7, 6.0, 8, 9)
    assert "(should not page; 1 scenario)" in render(
        [one], [], CHECK, FREEZE, MACHINE, rendered=RENDERED
    )
    assert "n/a" in t and " of 10" in t


def seconds(events_job, aggregates_job=26.5):
    return {
        "generate": 2.5,
        "produce": 53.1,
        "events_job": events_job,
        "aggregates_job": aggregates_job,
        "total": round(2.5 + 53.1 + events_job + aggregates_job + 2.0, 1),
    }


def test_method_notes_derive_events_time_and_carry_no_typed_dev_figure(summaries):
    t = text(summaries, check=CHECK | {"seconds": seconds(1802.1)})
    assert "about 30 minutes" in t
    assert "about 5 minutes" in text(summaries, check=CHECK | {"seconds": seconds(300)})
    assert "times the nominal" not in t


def test_dedupe_dev_names_dropped_runs_and_warns_on_a_difference():
    old = summary(7, 6.0, 5, 5) | {"run_id": "dev-s7-a6-1"}
    new = summary(7, 6.0, 8, 9) | {"run_id": "dev-s7-a6-2"}
    kept, notes = eval_report.dedupe_dev([old, new])
    assert [s["run_id"] for s in kept] == ["dev-s7-a6-2"]
    assert "dropped dev-s7-a6-1 (kept dev-s7-a6-2)" in notes
    assert any(n.startswith("WARNING") for n in notes)
    same = new | {"run_id": "dev-s7-a6-0"}
    assert not any(n.startswith("WARNING") for n in eval_report.dedupe_dev([same, new])[1])


def test_main_dev_writes_the_dev_render(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(eval_report, "measure_machine", lambda: MACHINE)
    monkeypatch.setattr(eval_report, "render_facts", lambda: RENDERED)
    for rid, e in (("dev-s7-a6-1", 8), ("dev-s7-a6-2", 9)):
        d = tmp_path / rid
        d.mkdir()
        (d / "summary.json").write_text(json.dumps(summary(7, 6.0, e, 9) | {"run_id": rid}))
    eval_report.main(["--dev", str(tmp_path)])
    out = (tmp_path / "dev-results.md").read_text()
    assert out.startswith("DEV SEEDS, not results\n")
    assert STATEMENT in out
    printed = capsys.readouterr().out
    assert "dropped dev-s7-a6-1" in printed and "WARNING" in printed


# ---------------------------------------------------------------- official records

OFF_COMMIT = "c" * 40
JAR = "j" * 64
T0 = datetime(2026, 11, 12, 6, 0, 0, tzinfo=UTC)
STAGES = {"generate": 1.0, "produce": 1.0, "job": 5.0, "total": 8.0}


def _iso(t: datetime) -> str:
    return t.isoformat(timespec="seconds")


def official(seed, amp, rid, expected=8, commit=OFF_COMMIT):
    s = copy.deepcopy(summary(seed, amp, expected, 9))
    s["meta"]["kind"] = "off"
    return s | {"run_id": rid, "commit": commit, "dirty": True, "timings": dict(STAGES)}


def records():
    """15 official summaries and a registry in which each pair started and finished once."""
    summaries, registry, t = [], [], T0
    for seed, amp in eval_report.planned(FREEZE):
        rid = f"off-s{seed}-a{amp:g}-1"
        summaries.append(official(seed, amp, rid))
        registry.append(
            {"event": "started", "seed": seed, "amplitude": amp, "run_id": rid}
            | {"commit": OFF_COMMIT, "reason": None, "jar_sha256": JAR, "at": _iso(t)}
        )
        t += timedelta(minutes=2, seconds=30)
        registry.append(
            {"event": "finished", "seed": seed, "amplitude": amp, "run_id": rid}
            | {"timings": dict(STAGES), "at": _iso(t)}
        )
    return summaries, registry


def rerun(registry, seed, amp, rid, event="finished"):
    """Registry lines of a later run of one pair."""
    at = _iso(T0 + timedelta(hours=2))
    started = {"event": "started", "seed": seed, "amplitude": amp, "run_id": rid}
    started |= {"commit": OFF_COMMIT, "reason": "bug: x", "jar_sha256": JAR, "at": at}
    return [*registry, started, {"event": event, "seed": seed, "amplitude": amp, "run_id": rid}]


def test_check_official_takes_one_finished_run_per_planned_pair():
    summaries, registry = records()
    check_official(summaries, registry, FREEZE)
    with pytest.raises(ValueError, match="no summary"):
        check_official(summaries[1:], registry, FREEZE)
    with pytest.raises(ValueError, match="'failed'"):
        check_official(summaries, rerun(registry, 101, 6.0, "off-s101-a6-2", "failed"), FREEZE)
    with pytest.raises(ValueError, match="'started'"):
        check_official(summaries, rerun(registry, 101, 6.0, "off-s101-a6-2")[:-1], FREEZE)
    with pytest.raises(ValueError, match="last finished run is off-s101-a6-2"):
        check_official(summaries, rerun(registry, 101, 6.0, "off-s101-a6-2"), FREEZE)
    two = copy.deepcopy(summaries)
    two[3]["commit"] = "d" * 40
    with pytest.raises(ValueError, match="2 commits"):
        check_official(two, registry, FREEZE)
    nameless = copy.deepcopy(summaries)
    del nameless[0]["run_id"]
    with pytest.raises(ValueError, match="no run_id"):
        check_official(nameless, registry, FREEZE)


def test_check_official_rejects_duplicates_dev_runs_and_no_freeze():
    summaries, registry = records()
    with pytest.raises(ValueError, match="appears twice"):
        check_official([*summaries, official(101, 6.0, "off-s101-a6-0")], registry, FREEZE)
    with pytest.raises(ValueError, match="not an official run"):
        check_official([*summaries[1:], summary(101, 6.0, 8, 9)], registry, FREEZE)
    renamed = copy.deepcopy(summaries)
    renamed[0]["run_id"] = "dev-s101-a6-1"
    with pytest.raises(ValueError, match="not an official run"):
        check_official(renamed, registry, FREEZE)
    with pytest.raises(ValueError, match="freeze"):
        check_official(summaries, registry, None)


def test_check_official_rejects_a_summary_whose_clock_stopped():
    summaries, registry = records()
    slept = copy.deepcopy(summaries)
    slept[2]["timings"] = {"generate": 12.8, "produce": 4.1, "job": 3000.0, "total": 141.0}
    with pytest.raises(ValueError, match="stages' sum"):
        check_official(slept, registry, FREEZE)
    edge = copy.deepcopy(summaries)
    edge[2]["timings"] = {"generate": 1.0, "produce": 1.0, "job": 5.0, "total": 6.0}
    check_official(edge, registry, FREEZE)  # rounding: one second of slack


def test_check_official_compares_the_summary_hash_when_the_registry_has_one():
    summaries, registry = records()
    for s in summaries:
        s["_sha256"] = "a" * 64
    hashed = copy.deepcopy(registry)
    hashed[1]["summary_sha256"] = "a" * 64
    check_official(summaries, hashed, FREEZE)
    hashed[1]["summary_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="summary_sha256"):
        check_official(summaries, hashed, FREEZE)


def test_load_summaries_records_each_files_hash(tmp_path):
    import hashlib

    d = tmp_path / "off-s101-a6-1"
    d.mkdir()
    raw = json.dumps(summary(101, 6.0, 8, 9)).encode()
    (d / "summary.json").write_bytes(raw)
    (got,) = load_summaries(tmp_path)
    assert got["_sha256"] == hashlib.sha256(raw).hexdigest()


def write_generation(directory, summaries):
    for s in summaries:
        d = directory / s["run_id"]
        d.mkdir(parents=True)
        (d / "summary.json").write_text(json.dumps(s))


def headline_cells(t):
    """'family k/n, pooled' from the rows of the 6:1 headline table."""
    section = t[t.index("## Headline") : t.index("## Sweep")]
    rows = [r for r in section.splitlines() if r.startswith("| ") and not r.startswith("| Family")]
    out = []
    for r in rows[: len(FAMILIES)]:
        cells = [c.strip() for c in r.strip("|").split("|")]
        out.append((cells[0], cells[2].split(" ")[0], cells[5]))
    return out


def log_section(t):
    return t[t.index("## Run log") :]


def test_the_first_run_log_entry_is_derived_from_the_first_generation(tmp_path):
    summaries, registry = records()
    write_generation(tmp_path / "summaries", summaries)
    first = first_run_entry(registry, tmp_path / "summaries", tmp_path / "superseded", FREEZE)
    t = text(summaries, freeze=FREEZE, registry=registry, first_entry=first)
    log = log_section(t)
    assert log.splitlines()[2].startswith(f"1. `{OFF_COMMIT[:7]}`, 2026-11-12:")
    assert "first official run: 15 runs, 5 seeds at 3 amplitudes" in log
    assert f"the settings freeze at `{FREEZE['commit'][:7]}`" in log
    (seasonal, per_key, glob) = headline_cells(t)
    want = (
        f"{seasonal[0]} {seasonal[1]}, {seasonal[2]} false pages per day;"
        f" {per_key[0]} {per_key[1]}, {per_key[2]}; {glob[0]} {glob[1]}, {glob[2]}"
    )
    assert f"New headline: {want}." in log
    assert "Old headline: none." in log


def test_the_first_entry_follows_the_summaries_it_is_built_from(tmp_path):
    summaries, registry = records()
    summaries[0] = official(101, 6.0, "off-s101-a6-1", expected=10)
    write_generation(tmp_path / "summaries", summaries)
    first = first_run_entry(registry, tmp_path / "summaries", tmp_path / "superseded", FREEZE)
    t = text(summaries, freeze=FREEZE, registry=registry, first_entry=first)
    (seasonal, _, _) = headline_cells(t)
    assert seasonal[1] == "42/50"
    assert "seasonal 42/50," in log_section(t)


def test_the_first_entry_keeps_the_first_generation_after_a_rerun(tmp_path):
    summaries, registry = records()
    write_generation(tmp_path / "superseded", summaries[:1])
    later = official(101, 6.0, "off-s101-a6-2", expected=10)
    write_generation(tmp_path / "summaries", [later, *summaries[1:]])
    registry = rerun(registry, 101, 6.0, "off-s101-a6-2")
    first = first_run_entry(registry, tmp_path / "summaries", tmp_path / "superseded", FREEZE)
    assert first["new"].startswith("seasonal 40/50,")
    shutil_rmtree(tmp_path / "superseded")
    with pytest.raises(FileNotFoundError, match="off-s101-a6-1"):
        first_run_entry(registry, tmp_path / "summaries", tmp_path / "superseded", FREEZE)


def shutil_rmtree(p):
    import shutil

    shutil.rmtree(p)


def test_the_first_entry_needs_every_pair_finished_at_one_commit(tmp_path):
    summaries, registry = records()
    write_generation(tmp_path / "summaries", summaries)
    dirs = (tmp_path / "summaries", tmp_path / "superseded")
    with pytest.raises(ValueError, match="14 of 15"):
        first_run_entry(registry[:-1], *dirs, FREEZE)
    mixed = copy.deepcopy(registry)
    mixed[0]["commit"] = "d" * 40
    with pytest.raises(ValueError, match="commits"):
        first_run_entry(mixed, *dirs, FREEZE)


def test_file_entries_print_no_typed_headline_and_say_supersedes():
    summaries, registry = records()
    log = [
        {
            "commit": "e" * 40,
            "date": "2026-11-20",
            "what": "run under a reason: official run off-s101-a6-2",
            "why": "bug: the scorer",
            "old": None,
            "new": None,
            "supersedes": ["off-s101-a6-1"],
        },
        {"commit": "f" * 40, "date": "2026-11-21", "what": "x", "why": "y", "old": "45/50"},
    ]
    t = log_section(text(summaries, log, registry=registry))
    assert "supersedes off-s101-a6-1 when the re-run finishes" in t
    assert "moved" not in t and "45/50" not in t
    assert t.count("headlines not derived") == 2


def test_setup_describes_the_runs_not_the_render():
    summaries, registry = records()
    rendered = {"commit": "0123456" + "9" * 33, "date": "2026-11-13", "measured_code_diff": []}
    t = render(summaries, [], CHECK, FREEZE, MACHINE, registry=registry, rendered=rendered)
    setup = t[t.index("## Setup") : t.index("## Headline")]
    assert f"- Runs at commit: `{OFF_COMMIT[:7]}`" in setup
    assert "- Rendered at commit `0123456` on 2026-11-13" in setup
    assert f"- Settings frozen at commit: `{FREEZE['commit'][:7]}`" in setup
    assert f"- Jar SHA-256: `{JAR}`" in setup
    # first start 06:00:00, last finish after 15 runs of 2 min 30 s each: 37.5 minutes
    assert "first start 2026-11-12 06:00:00 UTC, last finish 2026-11-12 06:37:30 UTC" in setup
    assert "elapsed 37.5 minutes" in setup
    assert "- Measured code at the render commit equals the freeze commit: yes" in setup
    no = rendered | {"measured_code_diff": ["infra/kafka/create-topics.sh"]}
    assert "freeze commit: no" in render(
        summaries, [], CHECK, FREEZE, MACHINE, registry=registry, rendered=no
    )


DASHBOARD = "infra/grafana/dashboards/tenant-baselines.json"


def test_display_only():
    assert eval_report.display_only([])
    assert eval_report.display_only([DASHBOARD])
    assert not eval_report.display_only(["infra/grafana/provisioning/datasources/clickhouse.yml"])
    assert not eval_report.display_only(["infra/grafana/dashboards/x.json", "docker-compose.yml"])


@pytest.mark.parametrize("code", [1, 128])
def test_measured_code_diff_raises_when_git_fails(monkeypatch, code):
    def git(cmd, **kw):
        return subprocess.CompletedProcess(cmd, code, "", "fatal")

    monkeypatch.setattr(eval_report.subprocess, "run", git)
    with pytest.raises(RuntimeError, match=str(code)):
        eval_report.measured_code_diff("a" * 40)


def test_measured_code_diff_lists_sorted_paths(monkeypatch):
    def git(cmd, **kw):
        assert "--name-only" in cmd
        return subprocess.CompletedProcess(cmd, 0, "shared/b\nDOCKER\nshared/a\n", "")

    monkeypatch.setattr(eval_report.subprocess, "run", git)
    assert eval_report.measured_code_diff("a" * 40) == ["DOCKER", "shared/a", "shared/b"]


def test_check_measured_code_allows_only_display_files():
    eval_report.check_measured_code([DASHBOARD], [])
    other = "infra/kafka/create-topics.sh"
    with pytest.raises(ValueError, match=other):
        eval_report.check_measured_code([DASHBOARD, other], [])
    eval_report.check_measured_code([DASHBOARD, other], [{"why": "bug: the scorer"}])


def test_setup_line_names_display_only_files(summaries):
    def setup(diff):
        t = render(
            summaries, [], CHECK, FREEZE, MACHINE, rendered=RENDERED | {"measured_code_diff": diff}
        )
        return t[t.index("## Setup") : t.index("## Headline")]

    head = "- Measured code at the render commit equals the freeze commit: "
    assert head + "yes\n" in setup([])
    assert head + f"yes, except display-only files: {DASHBOARD}\n" in setup([DASHBOARD])
    assert head + "no\n" in setup(["infra/kafka/create-topics.sh"])


def test_method_notes_date_the_change(summaries):
    t = text(summaries)
    assert f"Changed {eval_report.MEASURED_CODE_CHANGE_DATE}, after the official runs" in t
    assert "Everything else under infra/, and docker-compose.yml, is still compared." in t


def test_no_measured_file_reads_grafana():
    """A pin: the exemption is safe because no measured file reads a dashboard."""
    from tenant_baselines import freeze

    hits = []
    for rel in freeze.MEASURED_PATHS:
        base = freeze.REPO / rel
        files = [base] if base.is_file() else [f for f in base.rglob("*") if f.is_file()]
        for f in files:
            r = f.relative_to(freeze.REPO).as_posix()
            if r == "docker-compose.yml" or r.startswith("infra/grafana/"):
                continue
            if "grafana" in f.read_text(errors="ignore").lower():
                hits.append(r)
    assert hits == []


def official_repo(tmp_path, machine=None):
    """A repo dir holding the official records eval_report.main reads."""
    summaries, registry = records()
    results = tmp_path / "docs" / "results"
    write_generation(results / "summaries", summaries)
    (results / "official-runs.jsonl").write_text("".join(json.dumps(e) + "\n" for e in registry))
    (results / "run-log.jsonl").write_text("")
    (results / "check-paths.json").write_text(json.dumps(CHECK))
    if machine is not None:
        (results / "machine.json").write_text(json.dumps(machine))
    (tmp_path / "eval").mkdir()
    (tmp_path / "eval" / "freeze.json").write_text(json.dumps(FREEZE))
    return results


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_report, "REPO", tmp_path)
    monkeypatch.setattr(eval_report, "render_facts", lambda: RENDERED)
    monkeypatch.setattr(eval_report, "measured_code_diff", lambda commit: [])
    return tmp_path


def main_official(repo):
    out = repo / "results.md"
    eval_report.main(["--summaries", str(repo / "docs/results/summaries"), "--out", str(out)])
    return out.read_text()


def test_main_reads_machine_json_and_does_not_measure(repo, monkeypatch):
    official_repo(repo, machine=MACHINE | {"cpu": "Recorded CPU"})

    def measure():
        raise AssertionError("the machine facts are on file")

    monkeypatch.setattr(eval_report, "measure_machine", measure)
    t = main_official(repo)
    assert "Recorded CPU" in t
    assert "1. `ccccccc`" in t and "elapsed 37.5 minutes" in t


def test_main_measures_and_writes_machine_json_when_absent(repo, monkeypatch):
    results = official_repo(repo)
    monkeypatch.setattr(eval_report, "measure_machine", lambda: MACHINE | {"cpu": "Fresh CPU"})
    assert "Fresh CPU" in main_official(repo)
    assert json.loads((results / "machine.json").read_text())["cpu"] == "Fresh CPU"


def test_main_refuses_when_the_measured_code_changed_without_a_bug_entry(tmp_path, monkeypatch):
    results = official_repo(tmp_path, machine=MACHINE)
    monkeypatch.setattr(eval_report, "REPO", tmp_path)
    monkeypatch.setattr(eval_report, "render_facts", lambda: RENDERED)
    calls = []

    def git(cmd, **kw):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "infra/kafka/create-topics.sh\n", "")

    monkeypatch.setattr(eval_report.subprocess, "run", git)
    with pytest.raises(ValueError, match="measured code"):
        main_official(tmp_path)
    (cmd,) = calls
    assert cmd[cmd.index("diff") :][:4] == ["diff", "--name-only", FREEZE["commit"], "HEAD"]
    assert "flink-job/src/main" in cmd and "py/src/tenant_baselines/scoring.py" in cmd
    entry = {"commit": "e" * 40, "date": "2026-11-20", "what": "re-run", "why": "bug: the scorer"}
    (results / "run-log.jsonl").write_text(json.dumps(entry) + "\n")
    assert "freeze commit: no" in main_official(tmp_path)


SLEPT = CHECK | {
    "commit": "49c7b72dacdca4e7477db151b09f2a3b5c647093",
    "seconds": {
        "generate": 2.5,
        "produce": 53.1,
        "events_job": 3026.2,
        "aggregates_job": 3993.4,
        "total": 2378.7,
    },
}


def test_check_paths_timings_after_a_host_sleep_are_not_reported(summaries):
    t = text(summaries, check=SLEPT)
    section = t[t.index("## Event-path") : t.index("## Method notes")]
    assert "`make check-paths` passed." in section
    assert (
        "Timings are not reported: the host slept during the run"
        " (total 2378.7 s is less than the stages' sum 7075.2 s)." in section
    )
    assert "3993.4" not in t and "3026.2" not in t
    assert "about 50 minutes" not in t and "an unmeasured time" in t


def test_consistent_check_paths_timings_are_reported(summaries):
    t = text(summaries, check=CHECK | {"seconds": seconds(1802.1)})
    assert "events job 1802.1, aggregates job 26.5, total 1886.2" in t
    assert "about 30 minutes" in t and "not reported" not in t


def test_check_paths_fields_of_the_newer_file_are_shown(summaries):
    newer = CHECK | {"run_id": "paths-s7-a6-1", "dirty": False, "jar_sha256": "b" * 64}
    t = text(summaries, check=newer | {"passed": True})
    assert "`paths-s7-a6-1`" in t and "clean tree" in t and "b" * 64 in t
    assert "`paths-s7-a6-1`" not in text(summaries)  # an older file renders without them


def test_method_notes_count_the_dirty_flags_of_official_summaries():
    summaries, registry = records()
    t = text(summaries, registry=registry)
    assert "Each of the 15 official summaries records `dirty: true`." in t
    assert "the driver's own registry line, written before the run" in t
    for s in summaries[3:]:
        s["dirty"] = False
    assert "3 of 15 official summaries record `dirty: true`." in text(summaries, registry=registry)
    for s in summaries:
        s["dirty"] = False
    assert "`dirty: true`" not in text(summaries, registry=registry)


def test_method_notes_say_how_many_pages_matched(summaries):
    # at 6:1, seeds 7 and 11; give each family its own counts
    for i, fam in enumerate(FAMILIES):
        for s in summaries:
            if s["meta"]["amplitude"] == 6.0:
                s["selected_week5"] = dict(s["selected_week5"])
                s["selected_week5"][fam] = s["selected_week5"][fam] | {
                    "pages": 50 + i,
                    "matched_pages": 3 + i,
                }
    t = text(summaries)
    m = {
        fam: (
            sum(s["selected_week5"][fam]["matched_pages"] for s in summaries[2:4]),
            sum(s["selected_week5"][fam]["pages"] for s in summaries[2:4]),
        )
        for fam in FAMILIES
    }
    assert m["seasonal"] == (6, 100)
    assert (
        "Most pages are false in every family: at the selected settings,"
        f" {m['seasonal'][0]} of {m['seasonal'][1]} pages matched a scenario (seasonal),"
        f" {m['per_key_static'][0]} of {m['per_key_static'][1]} (per key static) and"
        f" {m['global_static'][0]} of {m['global_static'][1]} (global static)." in t
    )
    assert "this eval does not measure the split" in t


def test_matched_pages_without_a_false_majority_are_not_called_most(summaries):
    for s in summaries:
        for fam in FAMILIES:
            s["selected_week5"][fam] = s["selected_week5"][fam] | {"matched_pages": 15}
    t = text(summaries)
    assert "Most pages are false" not in t
    assert "30 of 40 pages matched a scenario (seasonal)" in t


def test_method_notes_take_the_dev_seeds_from_the_code(summaries, monkeypatch):
    assert "refuses any seed outside 7, 11 and 13" in text(summaries)
    monkeypatch.setattr(eval_report, "DEV_SEEDS", (1, 2, 5, 8))
    assert "refuses any seed outside 1, 2, 5 and 8" in text(summaries)


def test_method_notes_name_every_tie_break_and_date_the_connector(summaries):
    t = text(summaries)
    assert "then the lower median delay, then the less sensitive setting" in t
    assert "connector 5.1.0, which is not released as of 2026-11-12" in t

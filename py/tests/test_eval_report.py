import json

import pytest

from tenant_baselines import eval_report
from tenant_baselines.eval_report import (
    CURVE_HEADING,
    STATEMENT,
    by_scenario_type,
    check_passed,
    curve,
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
    "commit": "abcdef0123",
    "date": "2026-11-12",
    "cpu": "Test CPU",
    "cores": "8",
    "memory_bytes": 16 * 2**30,
    "docker_version": "29.0",
    "docker_cpus": "6",
    "docker_memory_bytes": 8 * 2**30,
}


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
    "seconds": {"events_job": 1.0, "aggregates_job": 2.0, "total": 3.0},
}
FREEZE = {"official_seeds": [101, 102, 103, 104, 105], "commit": "1234567890"}


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


def text(summaries, log=(), check=CHECK, freeze=FREEZE):
    return render(summaries, list(log), check, freeze, MACHINE)


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
    assert "none (n=0)" in render([s], [], CHECK, FREEZE, MACHINE)


def test_plural_n_a_and_seed_count_of_scenarios(summaries):
    t = text(summaries)
    assert "(should not page; 2 scenarios)" in t
    one = summary(7, 6.0, 8, 9)
    assert "(should not page; 1 scenario)" in render([one], [], CHECK, FREEZE, MACHINE)
    assert "n/a" in t and " of 10" in t


def test_method_notes_derive_events_time_and_carry_no_typed_dev_figure(summaries):
    t = text(summaries, check=CHECK | {"seconds": {"events_job": 1802.1}})
    assert "about 30 minutes" in t
    assert "about 5 minutes" in text(summaries, check=CHECK | {"seconds": {"events_job": 300}})
    assert "times the nominal" not in t


def _official(seed, amp, rid):
    s = summary(seed, amp, 8, 9)
    s["meta"]["kind"] = "off"
    s["run_id"] = rid
    return s


def test_check_official_rejects_duplicates_and_dev_runs():
    eval_report.check_official([_official(1, 6.0, "off-s1-a6-x"), _official(2, 6.0, "off-s2-a6-x")])
    with pytest.raises(ValueError, match="appears twice"):
        eval_report.check_official(
            [_official(1, 6.0, "off-s1-a6-x"), _official(1, 6.0, "off-s1-a6-y")]
        )
    with pytest.raises(ValueError, match="not an official run"):
        eval_report.check_official([summary(7, 6.0, 8, 9)])
    with pytest.raises(ValueError, match="not an official run"):
        eval_report.check_official([_official(1, 6.0, "dev-s1-a6-x")])


def test_dedupe_dev_names_dropped_runs_and_warns_on_a_difference():
    old = summary(7, 6.0, 5, 5) | {"run_id": "dev-s7-a6-1"}
    new = summary(7, 6.0, 8, 9) | {"run_id": "dev-s7-a6-2"}
    kept, notes = eval_report.dedupe_dev([old, new])
    assert [s["run_id"] for s in kept] == ["dev-s7-a6-2"]
    assert "dropped dev-s7-a6-1 (kept dev-s7-a6-2)" in notes
    assert any(n.startswith("WARNING") for n in notes)
    same = new | {"run_id": "dev-s7-a6-0"}
    assert not any(n.startswith("WARNING") for n in eval_report.dedupe_dev([same, new])[1])


def test_main_dev_writes_the_dev_render(tmp_path, capsys):
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

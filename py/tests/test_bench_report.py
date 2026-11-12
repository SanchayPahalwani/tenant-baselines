import copy
import json

import pytest

from tenant_baselines import bench_report as br

LOAD = {
    "host_load_1m": 3.5,
    "project_containers_cpu_pct": 412.3,
    "other_containers_cpu_pct": 87.6,
    "vm_cpus": 8,
    "cpu_pct_unit": "percent of one CPU",
}


def env(commit="abc1234"):
    return {
        "data": "synthetic",
        "date_utc": "2026-11-12T10:00:00+00:00",
        "commit": commit,
        "dirty": False,
        "on_battery": False,
        "host": {"cpu": "Test CPU", "cores": 10, "memory_gb": 32.0, "os": "26.0", "arch": "arm64"},
        "power": "Now drawing from 'AC Power'",
        "vm": {"cpus": 8, "memory_gb": 9.7, "server_version": "29.0"},
        "other_containers": 3,
        "images": ["clickhouse:26.8", "kafka:4.4.0", "kafka:4.4.0"],
        "flink": {
            "version": "2.2.1",
            "parallelism.default": "2",
            "taskmanager.numberOfTaskSlots": "2",
            "taskmanager.memory.process.size": "3gb",
        },
    }


def tp_run(run_id, rate):
    return {
        "run_id": run_id,
        "job_id": "j" + run_id,
        "events": 1_000_000,
        "stale_samples": 4,
        "reread_samples": 2,
        "max_read_s": 1.1,
        "events_per_s": rate,
        "events_per_s_source_clock": 111_111.1,
        "window_s": 300.0,
        "intervals": 25,
        "min_interval_rate": 90_001.5,
        "max_interval_rate": 150_002.5,
        "timing_error_pct": 1.7,
        "source": {"busy": 0.5, "backpressured": 0.25, "idle": 0.125},
        "drop_ticks_busy": 0.0875,
        "busiest_vertex": "scorer",
        "shares": {
            "Source: events": {"busy": 0.5, "backpressured": 0.25, "idle": 0.125},
            "drop-ticks": {"busy": 0.0875, "backpressured": 0.9, "idle": 0.0125},
            "scorer": {"busy": 0.75, "backpressured": 0.0, "idle": 0.1},
        },
        "stall_gaps_s": [],
        "host_sleep_s": 0.0,
        "restarts": 0,
        "late_events": 0,
        "power_end": "Now drawing from 'AC Power'",
        "checkpoint": {
            "id": 7,
            "state_size": 5_000_000,
            "checkpointed_size": 900_000,
            "vertices": [
                {"name": "scorer", "state_size": 4_000_000, "checkpointed_size": 800_000},
                {"name": "drop-ticks", "state_size": 0, "checkpointed_size": 0},
            ],
        },
        "taskmanager_memory": {"used_bytes": 1_610_612_736, "limit_bytes": 3_221_225_472},
        "alert_rows": {"seasonal/k4/tenant": 12, "static/p99.9/region": 2},
        "load_start": dict(LOAD),
        "load_end": {**LOAD, "other_containers_cpu_pct": 140.2},
    }


def throughput():
    return {
        "data": "synthetic",
        "env": env(),
        "valid": True,
        "invalid_reasons": [],
        "fill": {
            "topic": "tp-x.events",
            "keys": 2000,
            "partitions": 8,
            "event_minutes": 120,
            "rpm_per_key": 120.0,
            "events": 1_000_000,
            "seconds": 40.5,
            "events_per_s": 200_000.5,
            "kafka_bytes": 16_000_000,
            "bytes_per_event": 16.0,
            "compression": "producer",
        },
        "warmup_s": 60,
        "end_share": 0.9,
        "sample_interval_s": 12,
        "settings": 5,
        "runs": [tp_run("tp-x-r1", 123_457.0), tp_run("tp-x-r2", 130_000.0)],
        "spread_pct": 5.2,
    }


def pct(n, p50, p95, lo, hi):
    return {"n": n, "p50": p50, "p95": p95, "min": lo, "max": hi}


def measured(matched=57):
    return {
        "incidents": 60,
        "matched": matched,
        "missed": [3, 9, 11],
        "unmatched_firing_rows": 2,
        "extra_matched_rows": 1,
        "regional_firing_rows": 4,
        "pipeline_delay_s": pct(matched, 40.1, 98.7, 20.2, 120.3),
        "time_to_page_s": pct(matched, 290.5, 320.1, 263.4, 335.0),
        "emit_delay_s": pct(matched, 5.5, 8.5, 1.5, 9.5),
        "sink_delay_s": pct(matched, 0.2, 0.4, 0.1, 0.6),
        "under_five_minutes": 31,
    }


def delay():
    return {
        "data": "synthetic",
        "env": env(),
        "valid": True,
        "invalid_reasons": [],
        "run_id": "delay-x",
        "job_id": "jd",
        "world": {"keys": 36, "rpm_per_key": 40, "partitions": 8, "history_days": 21},
        "incidents": 60,
        "schedule": {
            "settle_s": 300,
            "spacing_s": 180,
            "duration_s": 360,
            "magnitude": 0.2,
            "tail_s": 180,
        },
        "design": {
            "watermark_bound_s": 20,
            "idleness_s": 61,
            "fire_after_minutes": 3,
            "hold_s": 120,
            "group_wait_s": 30,
        },
        "settings": 5,
        "headline": {"detector": "seasonal", "setting": "k4", **measured()},
        "by_detector": {"seasonal/k4": measured(), "static/p99.9": measured(58)},
        "watermark_lag_s": {"samples": 40, "min": 21.5, "median": 23.9, "max": 31.2},
        "lag_check": {"stale_lag_samples": 1},
        "clock_offset": {
            "before": {"offset_s": 0.123, "bracket_s": 0.05},
            "after": {"offset_s": 0.456, "bracket_s": 0.04},
        },
        "generator": {"caught_up_ms": 1, "failed_deliveries": 0},
        "late_events": 0,
        "restarts": 0,
        "stall_gaps_s": [],
        "host_sleep_s": 0.0,
        "power_end": "Now drawing from 'AC Power'",
        "load_start": dict(LOAD),
        "load_end": dict(LOAD),
    }


def state_run(backend, per_key, with_range):
    return {
        "backend": backend,
        "state_backend": "X",
        "interval_ms": 5000,
        "job_seconds": 100.0,
        "history_checkpoints": 10,
        "history_entries": 10,
        "checkpoints_after_tasks_finish": True if backend == "hashmap" else None,
        "keys": 48,
        "aggregate_rows": 999,
        "settings": 9,
        "restarts": 0,
        "stall_gaps_s": [],
        "host_sleep_s": 0.0,
        "checkpoint": {
            "id": 21,
            "trigger_share": 0.93,
            "state_size": 40_000_000,
            "checkpointed_size": 40_000_000,
            "vertices": [{"name": "scorer", "state_size": 36_000_000, "checkpointed_size": 1}],
        },
        "range": {"checkpoints": 4, "min": 30_000_000, "max": 41_000_000}
        if with_range
        else {"checkpoints": 3, "min": 39_000_000, "max": 40_000_000},
        "per_key": {
            "scorer_bytes": 36_000_000,
            "per_key_bytes": per_key,
            "other_vertices": [{"name": "region-grouper", "state_size": 1_234}],
        },
        "load_start": dict(LOAD),
        "load_end": dict(LOAD),
    }


def state():
    return {
        "data": "synthetic",
        "env": env(),
        "valid": True,
        "invalid_reasons": [],
        "world": {
            "name": "eval",
            "seed": 7,
            "amplitude": 6.0,
            "keys": 48,
            "keys_with_five_weeks": 46,
            "weeks": 5,
            "aggregate_rows": 999,
        },
        "settings": 9,
        "spec_estimate_bytes_per_key": 725_760,
        "runs": [state_run("hashmap", 765_432.0, False), state_run("rocksdb", 700_001.0, True)],
        "headline_backend": "hashmap",
        "extrapolation": {
            "label": "extrapolation, not measured",
            "hashmap": [{"keys": 2000, "bytes": 1_530_864_000}],
            "rocksdb": [{"keys": 2000, "bytes": 1_400_002_000}],
        },
        "power_end": "Now drawing from 'AC Power'",
        "headline_per_key_bytes": 765_432.0,
    }


def render(t=None, d=None, s=None):
    return br.render(t or throughput(), d or delay(), s or state())


def test_render_has_every_required_statement():
    out = render()
    for text in (
        "synthetic",
        "configured, not measured",
        "start of the first breaching minute",
        "not measured here",
        "Extrapolation, not measured",
        "upper bound",
        "group_wait",
    ):
        assert text in out
    assert "at least about 5 minutes" not in out


def test_every_number_comes_from_the_fixtures():
    out = render()
    for text in ("123,457", "98.7", "263.4", "23.9", "765,432", "61", "1.7"):
        assert text in out
    t = throughput()
    t["runs"][0]["events_per_s"] = 987_654.0
    assert "987,654" in render(t=t)
    assert "123,457" not in render(t=t)


def test_machine_load_is_printed_for_each_run():
    out = render()
    assert "412.3" in out
    assert "87.6" in out
    assert "140.2" in out
    assert "percent of one CPU" in out
    assert "run-to-run" in out


def test_load_read_failure_prints_not_recorded():
    t = throughput()
    t["runs"][0]["load_start"] = {
        "host_load_1m": None,
        "project_containers_cpu_pct": None,
        "other_containers_cpu_pct": None,
        "error": "docker stats failed",
    }
    assert "not recorded" in render(t=t)


def test_throughput_figures_and_bounds_are_printed():
    out = render()
    for text in ("111,111", "90,001.5", "150,002.5", "stale", "re-read", "bound", "8.8%"):
        assert text in out


def test_time_to_page_is_explained_not_asserted():
    out = render()
    assert "3 scored minutes in a row" in out
    assert "held for 120 s" in out
    assert "31 of 57" in out


def test_lag_says_what_was_measured_and_what_was_not():
    out = render()
    assert "every partition" in out
    assert "up to 61 s" in out
    assert "0.123" in out and "0.456" in out


def test_extrapolation_sits_under_its_own_heading_after_state_size():
    out = render()
    assert out.index("## State size") < out.index("## Extrapolation, not measured")
    tail = out[out.index("## Extrapolation, not measured") :]
    assert "1,530,864,000" in tail
    assert "region grouper" in tail


def test_hashmap_is_the_headline_and_rocksdb_sits_beside_it():
    out = render()
    sec = out[out.index("## State size") : out.index("## Extrapolation")]
    assert sec.index("hashmap") < sec.index("RocksDB")
    assert "700,001" in sec
    assert "30,000,000" in sec and "41,000,000" in sec
    assert "725,760" in sec and "estimate" in sec


def test_every_section_says_synthetic():
    out = render()
    sections = out.split("\n## ")[1:]
    assert len(sections) >= 8
    for sec in sections:
        assert "synthetic" in sec.lower() or sec.startswith("Reproduce"), sec[:40]


@pytest.mark.parametrize("which", ["throughput", "delay", "state"])
def test_report_refuses_an_invalid_result(which):
    docs = {"throughput": throughput(), "delay": delay(), "state": state()}
    docs[which]["valid"] = False
    docs[which]["invalid_reasons"] = ["the job restarted 1 time(s)"]
    with pytest.raises(br.ReportRefused) as e:
        br.render(docs["throughput"], docs["delay"], docs["state"])
    assert f"{which}.json" in str(e.value)
    assert "the job restarted 1 time(s)" in str(e.value)


def test_report_refuses_data_that_is_not_synthetic():
    d = delay()
    d["data"] = "real"
    with pytest.raises(br.ReportRefused, match=r"delay\.json.*synthetic"):
        render(d=d)


@pytest.mark.parametrize("which", ["throughput", "delay", "state"])
def test_report_refuses_a_run_on_battery(which):
    docs = {"throughput": throughput(), "delay": delay(), "state": state()}
    docs[which]["env"]["on_battery"] = True
    with pytest.raises(br.ReportRefused, match=rf"{which}\.json.*battery"):
        br.render(docs["throughput"], docs["delay"], docs["state"])


def test_power_source_is_printed_as_ac():
    assert "AC Power" in render()


def test_report_refuses_results_that_contradict_their_validity():
    t = throughput()
    t["runs"][1]["host_sleep_s"] = 2.5
    with pytest.raises(br.ReportRefused, match=r"throughput\.json.*slept"):
        render(t=t)
    d = delay()
    d["stall_gaps_s"] = [75.0]
    with pytest.raises(br.ReportRefused, match=r"delay\.json.*stalled"):
        render(d=d)
    s = state()
    s["runs"][0]["restarts"] = 1
    with pytest.raises(br.ReportRefused, match=r"state\.json.*restart"):
        render(s=s)
    t = throughput()
    t["runs"][0]["window_error"] = "too short"
    with pytest.raises(br.ReportRefused, match=r"throughput\.json.*window"):
        render(t=t)
    d = delay()
    d["late_events"] = 3
    with pytest.raises(br.ReportRefused, match=r"delay\.json.*late"):
        render(d=d)


BATTERY = "Now drawing from 'Battery Power'"


@pytest.mark.parametrize(
    ("which", "edit", "reason"),
    [
        ("throughput", lambda d: d["runs"][1].update(power_end=BATTERY), "tp-x-r2: on battery"),
        ("delay", lambda d: d.update(power_end=BATTERY), "the run: on battery"),
        ("state", lambda d: d.update(power_end=BATTERY), "on battery at the end"),
        (
            "delay",
            lambda d: d["clock_offset"]["after"].update(offset_s=1.5),
            "the VM clock offset moved by more than 1 s",
        ),
    ],
)
def test_report_refuses_results_that_contradict_their_valid(which, edit, reason):
    docs = {"throughput": throughput(), "delay": delay(), "state": state()}
    edit(docs[which])
    with pytest.raises(br.ReportRefused) as e:
        br.render(docs["throughput"], docs["delay"], docs["state"])
    assert f"{which}.json: marked valid, but its fields say otherwise" in str(e.value)
    assert reason in str(e.value)


def test_a_power_end_that_was_not_read_is_not_a_contradiction():
    t, d = throughput(), delay()
    t["runs"][0]["power_end"] = None
    d["power_end"] = ""
    assert "## Delay" in render(t=t, d=d)


def test_columns_split_when_environments_differ():
    one = render()
    assert "abc1234" in one
    d = delay()
    d["env"]["commit"] = "fff9999"
    two = render(d=d)
    assert "abc1234" in two and "fff9999" in two
    assert "| Throughput | Delay | State |" in two


def test_single_column_when_environments_agree():
    d, s = delay(), state()
    for doc in (d, s):
        doc["env"]["date_utc"] = throughput()["env"]["date_utc"]
    out = render(d=d, s=s)
    assert "| Throughput | Delay | State |" not in out
    assert "| Item | Value |" in out


def test_no_banned_words():
    out = render().lower()
    for w in (
        "spearheaded",
        "leveraged",
        "synergy",
        "cutting-edge",
        "results-driven",
        "passionate",
    ):
        assert w not in out


def test_main_reads_the_files_and_writes_the_page(tmp_path):
    res = tmp_path / "performance"
    res.mkdir()
    for name, doc in (("throughput", throughput()), ("delay", delay()), ("state", state())):
        (res / f"{name}.json").write_text(json.dumps(doc))
    out = tmp_path / "performance.md"
    br.main(["--results", str(res), "--out", str(out)])
    assert out.read_text() == render()


def test_main_raises_on_a_missing_file(tmp_path):
    res = tmp_path / "performance"
    res.mkdir()
    (res / "throughput.json").write_text(json.dumps(throughput()))
    with pytest.raises(br.ReportRefused, match=r"delay\.json"):
        br.main(["--results", str(res), "--out", str(tmp_path / "x.md")])
    assert not (tmp_path / "x.md").exists()


def test_render_does_not_change_its_inputs():
    t, d, s = throughput(), delay(), state()
    before = copy.deepcopy((t, d, s))
    br.render(t, d, s)
    assert (t, d, s) == before


def test_sst_sentence_only_in_the_rocksdb_paragraph():
    out = render()
    sec = out[out.index("## State size") : out.index("## Extrapolation")]
    split = sec.index("The RocksDB run")
    hashmap, rocks = sec[:split], sec[split:]
    assert "SST" not in hashmap
    assert "SST" in rocks
    assert "39,000,000" in hashmap and "40,000,000" in hashmap
    assert "30,000,000" in rocks and "41,000,000" in rocks


def test_a_range_of_one_checkpoint_is_not_a_range():
    s = state()
    s["runs"][0]["range"] = {"checkpoints": 1, "min": 39_500_000, "max": 39_500_000}
    out = render(s=s)
    assert "was 39,500,000 bytes (one checkpoint)" in out
    assert "ranged from 39,500,000 to 39,500,000" not in out


def test_no_key_count_reconciliation_sentence():
    assert "section 6" not in render()


def test_drop_ticks_sentence_is_a_design_explanation():
    out = render()
    assert "The hop adds one read of each event, the filter and one write." in out
    assert "choosing the key's channel, which the source would do without the union" in out
    assert "the share is an upper bound on what the hop adds" in out
    assert "tp-x-r1: back-pressured 90.0% of the window" in out
    assert "so it did not set the pace" in out


def test_drop_ticks_that_was_not_back_pressured_is_not_said_to_follow():
    t = throughput()
    for r in t["runs"]:
        r["shares"]["drop-ticks"]["backpressured"] = 0.2
    out = render(t=t)
    assert "back-pressured 20.0% of the window" in out
    assert "did not set the pace" not in out


def test_report_refuses_a_dirty_tree():
    t = throughput()
    t["env"]["dirty"] = True
    with pytest.raises(br.ReportRefused, match=r"throughput\.json.*(dirty|clean)"):
        render(t=t)


def test_commit_is_printed():
    assert "abc1234" in render()


def test_missing_config_value_prints_not_recorded():
    t = throughput()
    t["env"]["flink"]["taskmanager.memory.process.size"] = None
    out = render(t=t)
    assert "None" not in out
    assert "not recorded" in out


def test_delay_world_says_up_to_requests_per_key():
    assert "up to 40 requests a minute per key" in render()


def test_load_sentence_gives_the_vm_total():
    out = render()
    assert "used 87.6 to 140.2 percent of one CPU" in out
    assert "the VM has 8 CPUs, so 800 in all" in out


def test_a_missing_key_is_a_refusal_naming_file_and_key():
    d = delay()
    del d["watermark_lag_s"]
    with pytest.raises(br.ReportRefused, match=r"delay\.json.*watermark_lag_s"):
        render(d=d)
    t = throughput()
    del t["runs"][0]["source"]
    with pytest.raises(br.ReportRefused, match=r"throughput\.json.*source"):
        render(t=t)


def test_main_wraps_a_json_decode_error(tmp_path):
    res = tmp_path / "performance"
    res.mkdir()
    for name, doc in (("throughput", throughput()), ("delay", delay()), ("state", state())):
        (res / f"{name}.json").write_text(json.dumps(doc))
    (res / "state.json").write_text("{not json")
    with pytest.raises(br.ReportRefused, match=r"state\.json"):
        br.main(["--results", str(res), "--out", str(tmp_path / "x.md")])


@pytest.mark.parametrize(
    ("which", "edit", "reason"),
    [
        (
            "throughput",
            lambda d: d.update(valid=False, invalid_reasons=["the job restarted"]),
            "restarted",
        ),
        ("delay", lambda d: d["env"].update(dirty=True), "dirty"),
        ("state", lambda d: d["env"].update(on_battery=True), "battery"),
        ("delay", lambda d: d.update(stall_gaps_s=[70.0]), "stalled"),
    ],
)
def test_refusals_keep_their_own_message(which, edit, reason):
    docs = {"throughput": throughput(), "delay": delay(), "state": state()}
    edit(docs[which])
    with pytest.raises(br.ReportRefused) as e:
        br.render(docs["throughput"], docs["delay"], docs["state"])
    assert f"{which}.json" in str(e.value)
    assert reason in str(e.value)
    assert "unexpected shape" not in str(e.value)


def test_a_file_missing_env_is_named():
    d = delay()
    del d["env"]
    with pytest.raises(br.ReportRefused) as e:
        render(d=d)
    assert "delay.json" in str(e.value)
    assert "'env'" in str(e.value)


def test_state_wording_and_after_tasks_finish():
    out = render()
    sec = out[out.index("## State size") : out.index("## Extrapolation")]
    assert "triggered before the source vertex was reported finished" in sec
    assert "before the source finished" not in sec
    assert "checkpoints after tasks finish: true" in sec
    assert "checkpoints after tasks finish: not recorded" in sec


def test_no_alert_rows_prints_a_sentence_not_an_empty_table():
    t = throughput()
    for r in t["runs"]:
        r["alert_rows"] = {}
    out = render(t=t)
    no_rows = "No run wrote an alert row: the bench world is built to stay under every fire level."
    assert no_rows in out
    assert "| Detector / setting / scope |" not in out
    # with rows, the table stays
    assert "| Detector / setting / scope |" in render()


def test_spread_names_the_faster_run_and_does_not_assume_the_first_is_colder():
    out = render()
    assert "tp-x-r2 was the faster" in out
    t = throughput()
    t["runs"][0]["events_per_s"] = 200_000.0
    out = render(t=t)
    assert "tp-x-r1 was the faster" in out
    assert "is the colder one" not in out


# ---- the power source, read from power.json ----


def iv(source, a, b, seconds):
    day = "2026-11-12T"
    return {"source": source, "from": f"{day}{a}+05:30", "to": f"{day}{b}+05:30",
            "overlap_s": seconds}  # fmt: skip


def power_run(run_id, start, end, intervals, **extra):
    day = "2026-11-12T"
    return {
        "run_id": run_id,
        "log": f"power/{run_id}.log",
        "log_from": f"{day}20:55:02+05:30",
        "log_read_at": f"{day}23:11:47+05:30",
        "start": f"{day}{start}+05:30",
        "end": f"{day}{end}+05:30",
        "end_from": f"the write time of data/bench/{run_id}/result.json",
        "covered_by_log": True,
        "intervals": intervals,
        **extra,
    }


def power():
    day = "2026-11-12T"
    incidents = {
        "caught_up": f"{day}21:20:54+05:30",
        "job_caught_up": f"{day}21:25:29.693549+05:30",
        "settle_s": 300,
        "first_incident_start": f"{day}21:26:40+05:30",
        "shortest_time_to_page_s": 269.4,
        "earliest_page": f"{day}21:31:09.400000+05:30",
        "matched_page_on_battery": False,
    }
    delay_ivs = [
        iv("AC Power", "20:55:02", "21:21:33", 287.0),
        iv("Battery Power", "21:21:33", "21:27:03", 330.0),
        iv("AC Power", "21:27:03", "22:44:24", 4071.0),
    ]
    return {
        "source": "pmset -g log: the lines that say Using AC or Using Batt",
        "runs": {
            "throughput": power_run(
                "tp-x", "21:02:47", "21:16:44", [iv("AC Power", "20:55:02", "21:21:33", 837.0)]
            ),
            "delay": power_run("delay-x", "21:16:46", "22:34:54", delay_ivs, incidents=incidents),
            "state": power_run(
                "state-x", "22:34:55", "22:38:35", [iv("AC Power", "21:27:03", "22:44:24", 220.0)]
            ),
        },
    }


def test_the_power_row_names_both_readings_and_says_when_they_were_taken():
    out = render()
    assert "| Power at start and end |" in out
    assert "| Power |" not in out
    assert "AC Power at the start, AC Power at the end" in out
    assert "Power is read when a run starts and when it ends, not in between." in out


def test_the_power_row_reads_the_last_throughput_run_and_the_files_power_end():
    t, s = throughput(), state()
    t["runs"][-1]["power_end"] = None
    s["power_end"] = None
    out = render(t=t, s=s)
    assert "AC Power at the start, not recorded at the end" in out


def test_without_power_json_the_page_says_the_log_was_not_read():
    out = render()
    sec = out[out.index("## Power during the runs") : out.index("## Throughput")]
    assert "The OS power log was not read for these runs." in sec
    assert "synthetic" in sec


def test_the_power_section_lists_each_runs_sources_and_names_the_battery_span():
    out = br.render(throughput(), delay(), state(), power=power())
    sec = out[out.index("## Power during the runs") : out.index("## Throughput")]
    assert "| Throughput | tp-x | 21:02:47 | 21:16:44 | AC Power, 837 s |" in sec
    assert (
        "| Delay | delay-x | 21:16:46 | 22:34:54 | AC Power, 287 s; Battery Power, 330 s"
        " (21:21:33 to 21:27:03); AC Power, 4,071 s |"
    ) in sec
    assert "Start (IST)" in sec and "UTC+05:30" in sec
    assert (
        "The delay run (delay-x) had the charger out from 21:21:33 to 21:27:03 IST, for 330 s,"
        " and ran on battery." in sec
    )
    assert "the generator had written the history at 21:20:54" in sec
    assert "the job caught up on it at 21:25:29" in sec
    assert "the 300 s settle period ended at 21:25:54" in sec
    assert "the first incident started at 21:26:40" in sec
    assert (
        "So the charger was out for the job's catch-up from 21:21:33 on, the rest of the settle"
        " period and the first 23 s of the first incident." in sec
    )
    assert "No incident was paged on battery" in sec
    assert "shortest time-to-page of any setting, 269.4 s" in sec
    assert "at 21:31:09, 246 s after the charger came back" in sec


def test_the_power_section_says_so_when_every_run_was_on_ac():
    p = power()
    p["runs"]["delay"]["intervals"] = [iv("AC Power", "20:55:02", "22:44:24", 4688.0)]
    out = br.render(throughput(), delay(), state(), power=p)
    assert "The log shows AC Power throughout every run." in out
    assert "charger out" not in out


def test_a_page_window_on_battery_is_not_ruled_out():
    p = power()
    p["runs"]["delay"]["incidents"]["matched_page_on_battery"] = None
    out = br.render(throughput(), delay(), state(), power=p)
    assert "No incident was paged on battery" not in out
    assert "The records cannot rule out a page appended on battery." in out


def test_a_run_the_log_does_not_cover_is_named():
    p = power()
    p["runs"]["state"]["covered_by_log"] = False
    out = br.render(throughput(), delay(), state(), power=p)
    assert "The log does not cover the whole of the state run (state-x)." in out


# ---- pilots ----


def tp_pilot(run_id, rate, power_start, power_end=None, **edit):
    doc = throughput()
    doc["env"].update(power=power_start, on_battery=True, commit="8ac86e8a63")
    doc["env"]["date_utc"] = "2026-11-12T09:49:26+00:00"
    run = {"run_id": f"{run_id}-r1", "events_per_s": rate}
    if power_end is not None:
        run["power_end"] = power_end
    doc["runs"] = [run | edit]
    return doc


def delay_pilot():
    d = delay()
    d["env"].update(power=BATTERY, on_battery=True, commit="e4c95759ab")
    d["env"]["date_utc"] = "2026-11-12T14:39:35+00:00"
    d["power_end"] = BATTERY
    d["headline"]["pipeline_delay_s"] = pct(6, 21.5, 22.6, 21.4, 22.6)
    return d


def pilots():
    no_window = {
        "window_error": "no sample lies between the warm-up and the last tenth",
        "events_per_s_source_clock": 104_039.5,
        "load_start": {"other_containers_cpu_pct": 69.1},
        "load_end": {"other_containers_cpu_pct": 154.8},
    }
    bad = tp_pilot("tp-b", None, "Now drawing from 'AC Power'", BATTERY, **no_window)
    bad["runs"][0].pop("events_per_s")
    bad.update(valid=False, invalid_reasons=["run tp-b-r1: no sample lies between the warm-up"])
    bad["env"].update(dirty=True, date_utc="2026-11-12T11:04:55+00:00")
    return {
        "tp-a": tp_pilot("tp-a", 13_102.4, BATTERY),
        "tp-b": bad,
        "delay-p": delay_pilot(),
    }


def test_the_pilot_table_shows_each_pilot_as_it_is():
    out = br.render(throughput(), delay(), state(), pilots=pilots())
    sec = out[out.index("## Pilot runs (not the published figures)") :]
    sec = sec[: sec.index("\n## ")]
    assert "synthetic" in sec
    assert (
        "The pilots ran on the same machine with other projects' containers busier; their rates"
        " show how much the figure depends on what else the VM is doing." in sec
    )
    assert "| tp-a | 8ac86e8 | 2026-11-12T09:49:26+00:00 | Battery Power at the start," in sec
    assert "13,102 events a second" in sec
    assert "no window; 104,040 events a second on the source's clock" in sec
    assert "AC Power at the start, Battery Power at the end" in sec
    assert "69.1 → 154.8" in sec
    assert "pipeline delay p50 21.5 s, p95 22.6 s (n=6)" in sec
    assert "| no |" in sec and "| yes |" in sec
    assert "- tp-b: not valid: run tp-b-r1: no sample lies between the warm-up" in sec
    assert "- tp-b: recorded from a dirty tree." in sec
    # in the order they ran, and the published figures are not changed by them
    assert sec.index("| tp-a |") < sec.index("| tp-b |") < sec.index("| delay-p |")
    assert "123,457" in out[: out.index("## Pilot runs")]


def test_a_state_pilot_shows_bytes_per_key_per_backend():
    out = br.render(throughput(), delay(), state(), pilots={"state-p": state()})
    assert "hashmap 765,432 bytes per key; rocksdb 700,001 bytes per key" in out


def test_without_pilots_there_is_no_pilot_section():
    assert "## Pilot runs" not in render()


def test_the_battery_delay_pilot_is_set_beside_the_published_delay():
    out = br.render(throughput(), delay(), state(), power=power(), pilots=pilots())
    sec = out[out.index("## Power during the runs") : out.index("## Throughput")]
    assert (
        "The delay pilot delay-p was on battery at its start and at its end; it gave a pipeline"
        " delay of 21.5 s at p50 and 22.6 s at p95, against 40.1 s and 98.7 s in this run." in sec
    )


def test_pilots_are_not_refused():
    p = pilots()
    p["tp-a"]["valid"] = False
    p["tp-a"]["data"] = "unknown"
    assert "## Pilot runs" in br.render(throughput(), delay(), state(), pilots=p)


# ---- the static rows ----


def test_equal_static_rows_are_explained():
    d = delay()
    d["by_detector"] = {
        "global_static/p99.9": measured(58),
        "per_key_static/p99.9": measured(58),
        "seasonal/k4": measured(),
    }
    sentence = (
        "The delay world's keys have no per-key entry in `thresholds-demo.json`, so"
        " `per_key_static` falls back to the global level and its row equals `global_static`'s."
    )
    assert sentence in render(d=d)
    d["by_detector"]["per_key_static/p99.9"] = measured(57)
    assert sentence not in render(d=d)


def test_the_throughput_settings_sentence_says_per_key_static_used_the_global_levels():
    assert (
        "The job ran 5 settings at once; the bench world's keys have no per-key entry in"
        " `thresholds-demo.json`, so `per_key_static` used the global levels." in render()
    )


def test_the_bench_worlds_keys_have_no_per_key_level_and_the_global_levels_agree():
    from tenant_baselines import bench_world, freeze

    sets = json.loads((freeze.REPO / "infra/flink/thresholds-demo.json").read_text())["sets"]
    by = {s["detector"]: s for s in sets}
    per_key = set(by["per_key_static"]["per_key"])
    assert per_key  # the demo world's keys
    for name in ("bench", "delay"):
        keys = {k.key for k in bench_world.build(name, 1).keys}
        assert keys and not keys & per_key, name
    assert by["per_key_static"]["global"] == by["global_static"]["global"]


# ---- throughput wording ----


def test_the_idle_share_conclusion_follows_the_data():
    t = throughput()
    out = render(t=t)  # idle 12.5%: the source did wait on the broker part of the time
    assert "the job set the pace" not in out
    assert "The source was idle 12.5% of the window in tp-x-r1" in out
    for r in t["runs"]:
        r["source"] = {"busy": 0.12, "backpressured": 0.875, "idle": 0.002}
    out = render(t=t)
    assert (
        "The source was back-pressured 87.5% of the window and idle 0.2% in tp-x-r1, and"
        " back-pressured 87.5% and idle 0.2% in tp-x-r2: the job set the pace, not the broker."
        in out
    )


def test_the_run_order_sentence_follows_which_run_was_faster():
    out = render()  # r2 faster: the colder first run was the slower one
    assert "tp-x-r2 was the faster run." in out
    assert "which would make it the slower one, and here it was." in out
    t = throughput()
    t["runs"][0]["events_per_s"] = 200_000.0
    out = render(t=t)
    assert "tp-x-r1 was the faster run." in out
    assert (
        "The first job on a TaskManager starts colder, which would make it the slower one. Here"
        " it was the faster, so the order does not explain the spread." in out
    )


def test_every_vertex_share_is_printed_per_run():
    out = render()
    assert "| Vertex | tp-x-r1 busy | tp-x-r1 back-pressured | tp-x-r1 idle |" in out
    assert "| drop-ticks | 8.8% | 90.0% | 1.2% | 8.8% | 90.0% | 1.2% |" in out
    assert "| scorer | 75.0% | 0.0% | 10.0% | 75.0% | 0.0% | 10.0% |" in out


def test_the_cold_baselines_are_explained_from_the_fill():
    out = render()
    assert (
        "The fill is 120 event minutes, so no key has the history the seasonal detector needs and"
        " every key is scored against the fallback baselines." in out
    )
    assert "The baselines are cold after the fill's event time" not in out
    limits = out[out.index("## What these numbers do not show") :]
    assert "Throughput was measured with cold baselines." not in limits
    assert "the scorer was busy 75.0% of the window in tp-x-r1 and 75.0% in tp-x-r2" in limits


def test_the_window_start_is_worded_from_the_samples():
    assert "the first 60 s after the first sample that showed a count" in render()


def test_each_image_is_printed_once():
    assert "clickhouse:26.8, kafka:4.4.0 |" in render()


def test_the_limits_name_the_shared_vm_and_the_power_between_readings():
    limits = render()[render().index("## What these numbers do not show") :]
    assert "the shared VM's load" in limits
    assert "power between the two readings" in limits


# ---- state, extrapolation and delay wording ----


def test_the_rocksdb_range_and_the_backend_gap_are_explained():
    out = render()
    sec = out[out.index("## State size") : out.index("## Extrapolation")]
    assert (
        "The figure moves from one checkpoint to the next: the state is still growing in the last"
        " stretch of the run, and SST files that are not yet compacted are counted." in sec
    )
    assert "The hashmap figure is 1.1 times the RocksDB figure." in sec
    assert "`SNAPPY_COMPRESSION`" in sec and "state.backend.rocksdb.compression.per.level" in sec
    assert "documented default, not measured here" in sec


def test_the_extrapolation_names_the_keys_with_a_shorter_history():
    out = render()
    assert (
        "2 of the 48 measured keys ran fewer weeks, so the per-key figure is slightly under a"
        " full key's." in out
    )
    s = state()
    s["world"]["keys_with_five_weeks"] = 48
    assert "ran fewer weeks" not in render(s=s)


def test_pipeline_delay_is_defined_from_the_hold():
    assert (
        "Pipeline delay is the time from a row's event time, which for a tenant alert is the end"
        " of its hold, to the broker's append time of that row in `bench.alerts`." in render()
    )


def test_reproduce_lists_every_refusal():
    out = render()
    sec = out[out.index("## Reproduce") :]
    assert "from a dirty tree" in sec and "on battery" in sec and "contradict" in sec
    assert "`make power-log`" in sec


# ---- main and the committed page ----


def test_main_reads_power_json_and_the_pilots(tmp_path):
    res = tmp_path / "performance"
    (res / "pilots").mkdir(parents=True)
    for name, doc in (("throughput", throughput()), ("delay", delay()), ("state", state())):
        (res / f"{name}.json").write_text(json.dumps(doc))
    (res / "power.json").write_text(json.dumps(power()))
    for run_id, doc in pilots().items():
        (res / "pilots" / f"{run_id}.json").write_text(json.dumps(doc))
    out = tmp_path / "performance.md"
    br.main(["--results", str(res), "--out", str(out)])
    assert out.read_text() == br.render(
        throughput(), delay(), state(), power=power(), pilots=pilots()
    )

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
        "images": ["kafka:4.4.0", "clickhouse:26.8"],
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
        "shares": {"scorer": {"busy": 0.75, "backpressured": 0.0, "idle": 0.1}},
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
    assert "handing each event on to the keyed shuffle" in out
    assert "upper bound on the extra hop's cost" in out


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
    assert "No run wrote an alert row: the bench world stays under every fire level." in out
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

import json
from pathlib import Path

import pytest

from tenant_baselines import bench_delay as bd
from tenant_baselines import bench_world, freeze
from tenant_baselines.incidents import Incident

T0 = 1_800_000_000_000
KEY1 = "d01|us-east|us-east-s1"
KEY2 = "d04|us-east|us-east-s2"
KEY3 = "d07|us-east|us-east-s1"


def _world():
    return bench_world.build("delay", 1)


def _region(key):
    return key.split("|")[1]


def test_schedule_shape():
    w = _world()
    inc = bd.schedule(w, T0, 60, 1)
    assert len(inc) == 60
    by_region = {}
    for i in inc:
        by_region.setdefault(_region(i.key), []).append(i)
    assert sorted(len(v) for v in by_region.values()) == [20, 20, 20]
    for i in inc:
        assert (i.kind, i.magnitude, i.end_ms - i.start_ms) == ("error_spike", 0.20, 360_000)
    assert min(i.start_ms for i in inc) >= T0 + 300_000
    for v in by_region.values():
        starts = [i.start_ms for i in v]
        assert all(120_000 < b - a < 240_000 for a, b in zip(starts, starts[1:], strict=False))
    by_key = {}
    for i in inc:
        by_key.setdefault(i.key, []).append(i.start_ms)
    for starts in by_key.values():
        assert all(b - a >= 2_100_000 for a, b in zip(starts, starts[1:], strict=False))
    assert len({i.start_ms % 60_000 for i in inc}) >= 20
    assert inc == bd.schedule(w, T0, 60, 1)
    assert inc[0].key == KEY1


def test_schedule_never_reaches_half_a_region():
    inc = bd.schedule(_world(), T0, 60, 1)
    for region in {_region(i.key) for i in inc}:
        mine = [i for i in inc if _region(i.key) == region]
        for at in mine:
            n = sum(o.start_ms + 180_000 <= at.start_ms < o.start_ms + 720_000 for o in mine)
            assert n <= 4


def test_labels_round_trip():
    w = _world()
    inc = bd.schedule(w, T0, 60, 1)
    lab = json.loads(json.dumps(bd.labels(inc, w, T0)))
    assert lab["data"] == "synthetic" and lab["t0_ms"] == T0
    assert [i["id"] for i in lab["incidents"]] == list(range(60))
    assert all(i["signal"] == "error_rate" for i in lab["incidents"])
    assert lab["incidents"][0]["region"] == "us-east"
    assert bd.run_end_ms(lab) == max(i.end_ms for i in inc) + 180_000


def test_design_constants_match_the_sources():
    root = Path(freeze.REPO)
    java = root / "flink-job/src/main/java/io/github/sanchaypahalwani/tenantbaselines"
    wm = (java / "time/Watermarks.java").read_text() if (java / "time").exists() else None
    if wm is None:
        wm = next(java.rglob("Watermarks.java")).read_text()
    asm = next(java.rglob("AlertStateMachine.java")).read_text()
    rg = next(java.rglob("RegionGrouper.java")).read_text()
    am = (root / "infra/alertmanager/alertmanager.yml").read_text()
    assert "BOUND = Duration.ofSeconds(20)" in wm
    assert "LIVE_IDLENESS = Duration.ofSeconds(60)" in wm
    assert "FIRE_AFTER = 3" in asm
    assert "HOLD_MS = 120_000L" in rg
    assert "group_wait: 30s" in am
    assert bd.DESIGN == {
        "watermark_bound_s": 20,
        "idleness_s": 60,
        "fire_after_minutes": 3,
        "hold_s": 120,
        "group_wait_s": 30,
    }


def _row(
    key=KEY1,
    event=0,
    emitted=None,
    state="firing",
    scope="tenant",
    signal="error_rate",
    detector="seasonal",
    setting="k4",
    alert_id="a1",
    region=None,
):
    return {
        "detector": detector,
        "setting": setting,
        "scope": scope,
        "key": key,
        "region": _region(key) if region is None else region,
        "signal": signal,
        "state": state,
        "alert_id": alert_id,
        "event_time_ms": event,
        "emitted_at_ms": event + 1 if emitted is None else emitted,
    }


def _lab(starts_keys):
    return {
        "data": "synthetic",
        "t0_ms": T0,
        "incidents": [
            {
                "id": j,
                "key": k,
                "region": _region(k),
                "signal": "error_rate",
                "start_ms": s,
                "end_ms": s + 360_000,
            }
            for j, (s, k) in enumerate(starts_keys)
        ],
    }


def test_first_appends_keeps_the_earliest():
    a, b = _row(alert_id="x"), _row(alert_id="x")
    out = bd.first_appends([(5_000, b), (1_000, a)])
    assert [t for t, _ in out] == [1_000]
    out = bd.first_appends(
        [(1_000, _row(alert_id="x")), (2_000, _row(alert_id="x", state="resolved"))]
    )
    assert len(out) == 2


@pytest.mark.parametrize(("sec", "ttp", "under"), [(20, 301.5, 0), (40, 281.5, 1)])
def test_measure_two_definitions(sec, ttp, under):
    s = T0 + 320_000 + (sec - 20) * 1000
    ev = s - sec * 1000 + 300_000
    row = _row(event=ev, emitted=ev + 21_000)
    m = bd.measure(_lab([(s, KEY1)]), [(ev + 21_500, row)], "seasonal", "k4")
    assert (m["incidents"], m["matched"], m["missed"]) == (1, 1, [])
    assert m["time_to_page_s"]["p50"] == ttp
    assert m["under_five_minutes"] == under
    if sec == 20:
        assert m["pipeline_delay_s"]["p50"] == 21.5
        assert m["emit_delay_s"]["p50"] == 21.0
        assert m["sink_delay_s"]["p50"] == 0.5
        for k in ("pipeline_delay_s", "time_to_page_s", "emit_delay_s", "sink_delay_s"):
            assert m[k]["n"] == 1


def test_measure_counts_misses_unmatched_and_regional():
    s0, s1 = T0 + 400_000, T0 + 800_000
    lab = _lab([(s0, KEY1), (s1, KEY2)])
    rows = [
        (1, _row(KEY1, s0 + 300_000)),
        (2, _row(KEY3, s0 + 300_000, alert_id="b")),
        (3, _row(KEY1, s0 + 300_000, scope="region", alert_id="c")),
        (4, _row(KEY2, s1 - 1, alert_id="d")),
    ]
    m = bd.measure(lab, rows, "seasonal", "k4")
    assert (m["matched"], m["missed"]) == (1, [1])
    assert (m["unmatched_firing_rows"], m["regional_firing_rows"]) == (2, 1)


def test_measure_ignores_other_detectors_and_signals():
    s = T0 + 400_000
    rows = [
        (1, _row(KEY1, s + 1000, detector="global_static", setting="p99", alert_id="a")),
        (2, _row(KEY1, s + 1000, signal="latency_p95", alert_id="b")),
    ]
    m = bd.measure(_lab([(s, KEY1)]), rows, "seasonal", "k4")
    assert m["matched"] == 0 and m["time_to_page_s"]["n"] == 0
    assert m["unmatched_firing_rows"] == 1


def test_match_window_ends_ten_minutes_after_the_incident():
    s = T0 + 400_000
    end = s + 360_000
    lab = _lab([(s, KEY1)])
    assert bd.measure(lab, [(1, _row(KEY1, end + 600_000))], "seasonal", "k4")["matched"] == 1
    assert bd.measure(lab, [(1, _row(KEY1, end + 600_001))], "seasonal", "k4")["matched"] == 0


def test_rows_in_the_settle_window_are_not_counted_unmatched():
    rows = [(1, _row(KEY3, T0 + 299_999))]
    assert bd.measure(_lab([]), rows, "seasonal", "k4")["unmatched_firing_rows"] == 0


def test_run_is_invalid_under_ninety_percent():
    assert not bd.enough_matched({"matched": 53, "incidents": 60})
    assert bd.enough_matched({"matched": 54, "incidents": 60})


def test_summary_nearest_rank():
    s = bd.summary([float(i) for i in range(1, 21)])
    assert (s["p50"], s["p95"], s["n"], s["min"], s["max"]) == (10, 19, 20, 1, 20)
    e = bd.summary([])
    assert e["n"] == 0 and e["p50"] is None and e["max"] is None


def test_pairs_are_sorted_firing_pairs():
    rows = [
        (1, _row(detector="seasonal", setting="k4")),
        (2, _row(detector="global_static", setting="p99")),
        (3, _row(detector="zzz", setting="x", state="resolved")),
    ]
    assert bd.pairs(rows) == [("global_static", "p99"), ("seasonal", "k4")]


def test_incident_type_is_the_incidents_one():
    assert isinstance(bd.schedule(_world(), T0, 1, 1)[0], Incident)

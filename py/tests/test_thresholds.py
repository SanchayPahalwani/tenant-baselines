import json

import numpy as np
import pytest

from tenant_baselines.evalmodel import EPOCH_MS, MINUTE_MS, REPLAY_END_MS, WEEK_MS, week_range
from tenant_baselines.scenarios import Scenario
from tenant_baselines.simulate import Aggregates
from tenant_baselines.thresholds import (
    PERCENTILES,
    calibration_values,
    demo_thresholds,
    eval_thresholds,
    fit,
    level,
)
from tenant_baselines.world import Key

WEEK = 7 * 1440
KEYS = [Key("t0", "r0", "u0", 10.0), Key("t1", "r0", "u0", 10.0)]


def agg(weeks=3, first=None):
    n = weeks * WEEK
    count = np.full((2, n), 100, dtype=np.int64)
    errors = np.ones((2, n), dtype=np.int64)
    p95 = np.full((2, n), 100.0)
    f = np.zeros(2, dtype=np.int64) if first is None else np.array(first, dtype=np.int64)
    for k in range(2):
        count[k, : f[k]] = 0
        errors[k, : f[k]] = 0
        p95[k, : f[k]] = np.nan
    return Aggregates(list(KEYS), f, count, errors, p95, EPOCH_MS)


def scenario(type_, start_min, minutes=20, keys=("t0|r0|u0",)):
    start = EPOCH_MS + start_min * MINUTE_MS
    return Scenario(
        id=f"{type_}-{start_min}",
        type=type_,
        week=1,
        start_ms=start,
        end_ms=start + minutes * MINUTE_MS,
        keys=keys,
        region=None,
        expected_signal=None,
        should_page=False,
        scored=False,
        note="",
    )


def test_percentile_and_resolve():
    lv = level(np.arange(1000.0), 99.0, "error_rate")
    assert lv["fire"] == pytest.approx(989.01)
    assert lv["resolve"] == pytest.approx((499.5 + 989.01) / 2)
    assert level(np.array([]), 99.0, "error_rate") is None


def test_volume_uses_the_mirrored_percentile_and_resolves_above_fire():
    a = agg(weeks=1)
    a.count[0] = np.arange(WEEK) % 500 + 20
    sets = fit(a, [], 1, 1, 0, None)
    by = {s["setting"]: s for s in sets if s["detector"] == "global_static"}
    assert [s for s, _, _ in PERCENTILES] == list(by)
    for setting, up, low in PERCENTILES:
        g = by[setting]["global"]["volume_5m"]
        assert g["resolve"] > g["fire"]
        assert g["fire"] < 500 * 5 and low < 50 and up > 50
    # lower percentile fires lower
    assert by["p99.99"]["global"]["volume_5m"]["fire"] <= by["p99"]["global"]["volume_5m"]["fire"]


def test_masking_removes_incident_minutes():
    a = agg(weeks=1)
    a.errors[0, 1000:1020] = 50
    unmasked = calibration_values(a, [], 1, 1)["error_rate"][0]
    assert level(unmasked, 99.9, "error_rate")["fire"] == pytest.approx(0.5)
    s = scenario("error_burst", 1000)
    masked = calibration_values(a, [s], 1, 1)["error_rate"][0]
    assert level(masked, 99.9, "error_rate")["fire"] == pytest.approx(0.01)
    # end is exclusive (minute 1020); end + 4 min is out, end + 5 min is in
    a.errors[0, 1024] = 7
    a.errors[0, 1025] = 9
    masked = calibration_values(a, [s], 1, 1)["error_rate"][0]
    assert 0.07 not in masked
    assert 0.09 in masked
    assert len(masked) == WEEK - 25


def test_every_labelled_scenario_but_the_monday_peak_is_masked():
    a = agg(weeks=1)
    base = calibration_values(a, [], 1, 1)
    for type_ in ("launch_surge", "holiday_dip", "error_burst"):
        v = calibration_values(a, [scenario(type_, 2000)], 1, 1)
        assert len(v["error_rate"][0]) == WEEK - 25
        assert len(v["latency_p95"][0]) == WEEK - 25
        assert len(v["volume_5m"][0]) == len(base["volume_5m"][0]) - 25
        assert len(v["error_rate"][1]) == WEEK
    v = calibration_values(a, [scenario("monday_peak", 2000)], 1, 1)
    for signal in base:
        assert len(v[signal][0]) == len(base[signal][0])


def test_minimum_evidence_and_five_minute_sum():
    a = agg(weeks=1)
    a.count[0, 500:510] = 19
    a.errors[0, 500:510] = 0
    v = calibration_values(a, [], 1, 1)
    assert len(v["error_rate"][0]) == WEEK - 10
    assert len(v["latency_p95"][0]) == WEEK - 10
    assert len(v["volume_5m"][0]) == WEEK - 4
    assert v["volume_5m"][1][0] == 500
    assert 19 * 5 in v["volume_5m"][0]
    b = agg(weeks=1, first=[100, 0])
    vb = calibration_values(b, [], 1, 1)
    assert len(vb["volume_5m"][0]) == WEEK - 104  # minutes 104 ... end have four rows before
    assert len(vb["error_rate"][0]) == WEEK - 100


def test_key_without_calibration_falls_back_to_global():
    a = agg(weeks=4, first=[0, 3 * WEEK])
    sets = fit(a, [], 1, 3, week_range(4)[0], week_range(4)[1])
    pk = next(s for s in sets if s["detector"] == "per_key_static")
    assert list(pk["per_key"]) == ["t0|r0|u0"]
    assert set(pk["per_key"]["t0|r0|u0"]) == {"error_rate", "latency_p95", "volume_5m"}
    gl = next(s for s in sets if s["detector"] == "global_static")
    assert gl["per_key"] == {}
    assert gl["global"] == pk["global"]


def test_week_five_refits_on_four_weeks_with_the_same_percentile():
    a = agg(weeks=5)
    a.errors[:, 3 * WEEK : 4 * WEEK : 7] = 30
    sets = eval_thresholds(a, [])["sets"]
    assert len(sets) == 16
    w4 = next(
        s
        for s in sets
        if (s["detector"], s["setting"]) == ("global_static", "p99.9")
        and s["valid_from_ms"] == week_range(4)[0]
    )
    w5 = next(
        s
        for s in sets
        if (s["detector"], s["setting"]) == ("global_static", "p99.9")
        and s["valid_from_ms"] == week_range(5)[0]
    )
    assert w4["global"]["error_rate"] != w5["global"]["error_rate"]
    assert w4["valid_to_ms"] == w5["valid_from_ms"] == EPOCH_MS + 4 * WEEK_MS
    assert w5["valid_to_ms"] == REPLAY_END_MS


def test_file_has_no_nan():
    a = agg(weeks=5)
    json.dumps(eval_thresholds(a, []), allow_nan=False)


def test_demo_thresholds_are_two_open_ended_p999_sets():
    sets = demo_thresholds()["sets"]
    assert [(s["detector"], s["setting"]) for s in sets] == [
        ("global_static", "p99.9"),
        ("per_key_static", "p99.9"),
    ]
    assert all(s["valid_from_ms"] == 0 and s["valid_to_ms"] is None for s in sets)
    assert sets[1]["per_key"]
    json.dumps(sets, allow_nan=False)

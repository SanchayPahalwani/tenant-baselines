import pytest

from tenant_baselines.evalmodel import MINUTE_MS, week_range
from tenant_baselines.scenarios import Scenario
from tenant_baselines.scoring import (
    FAMILIES,
    Page,
    SettingScore,
    matches,
    nearest_rank,
    normalise,
    pages,
    score,
    select,
    summarise,
    wilson,
)

W4, W4_END = week_range(4)
W5, W5_END = week_range(5)
A, B, C = "t1|us-east|s1", "t2|us-east|s1", "t3|eu-central|s1"
START = W4 + 1000 * MINUTE_MS
END = START + 20 * MINUTE_MS
M = MINUTE_MS


def scen(
    id="s1", type="error_burst", keys=(A,), signal="error_rate", should=True, start=START, end=END
):
    return Scenario(
        id, type, 4, start, end, keys, None, signal if should else None, should, True, ""
    )


def row(**kw):
    base = {
        "alert_id": "a1",
        "run_id": "r",
        "detector": "seasonal",
        "setting": "k4",
        "scope": "tenant",
        "key": A,
        "tenant": "t1",
        "region": "us-east",
        "unit": "s1",
        "signal": "error_rate",
        "state": "firing",
        "starts_at_ms": START,
        "event_time_ms": START,
        "emitted_at_ms": 5,
        "value": 1.0,
        "reference": 0.1,
        "reason": "",
    }
    base.update(kw)
    return base


def page(**kw):
    base = dict(
        detector="seasonal",
        setting="k4",
        scope="tenant",
        key=A,
        region="us-east",
        signal="error_rate",
        starts_at_ms=START,
        event_time_ms=START,
        alert_id="a1",
    )
    base.update(kw)
    return Page(**base)


def test_wilson_known_values():
    assert wilson(8, 10) == pytest.approx((0.4902, 0.9433), abs=1e-4)
    assert wilson(0, 10) == pytest.approx((0.0, 0.2775), abs=1e-4)
    assert wilson(10, 10) == pytest.approx((0.7225, 1.0), abs=1e-4)
    assert wilson(40, 50) == pytest.approx((0.6696, 0.8876), abs=1e-4)
    assert wilson(0, 0) == (0.0, 1.0)


def test_normalise_drops_duplicates_and_emitted_at():
    rows = [row(emitted_at_ms=1), row(emitted_at_ms=2)]
    rows += [
        row(alert_id="x", state="absorbed", reason="r1"),
        row(alert_id="x", state="absorbed", reason="r2"),
    ]
    out, counts = normalise(rows, W5_END)
    assert len(out) == 3
    assert counts == {"duplicates": 1, "clipped": 0}
    assert all("emitted_at_ms" not in r and "run_id" not in r for r in out)


def test_normalise_orders_by_event_time_then_region():
    rows = [
        row(alert_id="1", event_time_ms=START + 1, region="b"),
        row(alert_id="2", event_time_ms=START, region="z"),
        row(alert_id="3", event_time_ms=START, region="a"),
    ]
    out, _ = normalise(rows, W5_END)
    assert [r["alert_id"] for r in out] == ["3", "2", "1"]


def test_rows_past_replay_end_are_clipped():
    rows = [row(), row(alert_id="late", event_time_ms=W5_END + 1)]
    out, counts = normalise(rows, W5_END)
    assert [r["alert_id"] for r in out] == ["a1"]
    assert counts["clipped"] == 1


def test_only_firing_rows_are_pages_and_the_week_is_by_start():
    rows = [
        row(starts_at_ms=W4_END - M, event_time_ms=W5 + 5 * M),
        row(alert_id="r", state="resolved"),
        row(alert_id="ab", state="absorbed"),
        row(alert_id="w5", starts_at_ms=W5),
    ]
    got = pages(rows, W4, W4_END)
    assert [p.alert_id for p in got] == ["a1"]
    assert got[0].event_time_ms == W5 + 5 * M


def test_match_window_and_subject():
    s = scen()
    assert matches(page(event_time_ms=START), s)
    assert matches(page(event_time_ms=END + 10 * M), s)
    assert not matches(page(event_time_ms=END + 10 * M + 1), s)
    assert not matches(page(event_time_ms=START - 1), s)
    assert not matches(page(key=B), s)
    regional = page(scope="region", key="us-east", region="us-east")
    assert matches(regional, s)
    assert not matches(page(scope="region", key="eu-central", region="eu-central"), s)
    assert not matches(page(signal="latency_p95"), s, expected_only=True)
    assert matches(page(signal="latency_p95"), s)


def test_detected_any_and_expected():
    s = scen()
    r = score([page(signal="latency_p95")], [s], "seasonal", "k4", 4)
    assert (r.detected_any, r.detected_expected) == (1, 0)
    assert r.false_pages == 0 and r.matched_pages == 1
    assert r.scenarios[0]["delay_min"] is None
    assert r.scenarios[0]["delay_any_min"] == 0.0
    assert r.median_delay_min is None and r.delays_by_type == {}


def test_delay_uses_event_time():
    p = page(starts_at_ms=START + M, event_time_ms=START + 6 * M)
    r = score([p], [scen()], "seasonal", "k4", 4)
    assert r.scenarios[0]["delay_min"] == 6.0
    assert r.median_delay_min == 6.0
    assert r.delays_by_type == {"error_burst": [6.0]}


def test_false_pages_refires_and_precision():
    ps = [
        page(event_time_ms=START, alert_id="1"),
        page(event_time_ms=START + 5 * M, alert_id="2"),
        page(key=B, event_time_ms=START + 6 * M, alert_id="3"),
        page(key=C, event_time_ms=START + 7 * M, alert_id="4"),
        page(event_time_ms=START + 9000 * M, alert_id="5"),
    ]
    s = scen(keys=(A, B))
    r = score(ps, [s], "seasonal", "k4", 4)
    assert r.pages == 5 and r.matched_pages == 3
    assert r.false_pages == 2
    assert r.false_pages_per_day == 2 / 7
    assert r.refires == 1
    assert r.pages_per_detected == 3.0


def test_pages_during_a_should_not_page_scenario_are_false_and_counted_there():
    mon = scen(id="m", type="monday_peak", should=False)
    r = score([page()], [mon], "seasonal", "k4", 4)
    assert r.false_pages == 1 and r.matched_pages == 0 and r.n_should_page == 0
    assert r.scenarios[0]["false_pages_during"] == 1
    assert r.scenarios[0]["should_page"] is False


def test_regional_formed_is_reported():
    s = scen()
    r = score([page(scope="region", key="us-east")], [s], "seasonal", "k4", 4)
    assert r.scenarios[0]["regional_formed"] is True
    r = score([page()], [s], "seasonal", "k4", 4)
    assert r.scenarios[0]["regional_formed"] is False


def test_empty_family_scores_without_error():
    r = score([], [scen()], "seasonal", "k4", 4)
    assert r.detected_any == 0
    assert r.pages_per_detected is None
    assert r.median_delay_min is None
    assert r.false_pages_per_day == 0


def sc(setting, expected, fpd, delay, any_=None, detector="seasonal"):
    return SettingScore(
        detector, setting, 4, 10, expected if any_ is None else any_, expected,
        0, 0, 0, fpd, 0, None, delay,
    )  # fmt: skip


def test_selection_rule():
    k = FAMILIES["seasonal"]
    got = select(
        [sc(k[0], 9, 3.0, 7), sc(k[1], 8, 1.0, 6), sc(k[2], 8, 1.0, 9), sc(k[3], 6, 0.1, 5)]
    )
    assert (got.setting, got.branch) == (k[1], "reached_8_of_10")
    got = select([sc(s, 8, 1.0, 6) for s in k])
    assert got.setting == k[3]


def test_selection_when_nothing_reaches_eight():
    k = FAMILIES["seasonal"]
    got = select(
        [sc(k[0], 7, 2.0, 5), sc(k[1], 7, 1.0, 5), sc(k[2], 5, 0.0, 5), sc(k[3], 2, 0.0, 5)]
    )
    assert (got.setting, got.branch) == (k[1], "highest_recall")


def test_selection_uses_expected_signal_recall():
    k = FAMILIES["per_key_static"]
    d = "per_key_static"
    rest = [sc(k[2], 0, 0.0, None, detector=d), sc(k[3], 0, 0.0, None, detector=d)]
    got = select([sc(k[0], 7, 0.0, 5, any_=9, detector=d), sc(k[1], 8, 5.0, 5, detector=d), *rest])
    assert (got.setting, got.branch) == (k[1], "reached_8_of_10")


def test_missing_delay_sorts_last():
    k = FAMILIES["seasonal"]
    got = select([sc(s, 0, 0.0, None) for s in k])
    assert got.setting == k[3]
    got = select(
        [
            sc(k[0], 0, 0.0, None),
            sc(k[1], 0, 0.0, 4.0),
            sc(k[2], 0, 0.0, None),
            sc(k[3], 0, 0.0, None),
        ]
    )
    assert got.setting == k[1]


def test_nearest_rank():
    assert nearest_rank([1, 2, 3, 4], 0.5) == 2
    assert nearest_rank([1, 2, 3, 4], 0.95) == 4
    assert nearest_rank([], 0.5) is None


def test_summarise_end_to_end():
    rows = [
        *_bursts(4, "k4")[1],
        row(
            alert_id="b",
            starts_at_ms=W5 + M,
            event_time_ms=W5 + M,
            detector="per_key_static",
            setting="p99",
        ),
        row(
            alert_id="b",
            state="resolved",
            starts_at_ms=W5 + M,
            event_time_ms=W5 + 9 * M,
            detector="per_key_static",
            setting="p99",
        ),  # fmt: skip
        row(alert_id="late", event_time_ms=W5_END + 1),
    ]
    out = summarise(rows, TEN, {"replay_end_ms": W5_END})
    assert out["synthetic"] is True
    assert out["normalise"] == {"duplicates": 0, "clipped": 1}
    assert out["open_alerts"] == 1
    assert out["weeks"]["4"]["seasonal"]["k4"]["detected_expected"] == 1
    assert out["weeks"]["5"]["per_key_static"]["p99"]["pages"] == 1
    assert set(out["selection"]) == set(FAMILIES)
    assert out["selection"]["seasonal"]["branch"] == "highest_recall"
    sel = out["selection"]["seasonal"]["setting"]
    assert out["selected_week5"]["seasonal"]["setting"] == sel


def _bursts(week, hit_setting):
    start, _ = week_range(week)
    out, rows = [], []
    for i in range(10):
        key = f"t{i}|us-east|s1"
        s0 = start + (100 + 200 * i) * MINUTE_MS
        out.append(
            Scenario(
                f"w{week}-{i}",
                "error_burst",
                week,
                s0,
                s0 + 20 * M,
                (key,),
                None,
                "error_rate",
                True,
                True,
                "",
            )
        )
        if i == 0:
            rows.append(
                row(
                    alert_id=f"{week}{i}",
                    setting=hit_setting,
                    key=key,
                    starts_at_ms=s0,
                    event_time_ms=s0,
                )
            )
    return out, rows


TEN = _bursts(4, "k4")[0] + _bursts(5, "k4")[0]


def test_open_alerts_only_resolved_closes():
    def n(rows):
        return summarise(rows, TEN, {"replay_end_ms": W5_END})["open_alerts"]

    t = START
    assert (
        n(
            [
                row(alert_id="x", state="absorbed", event_time_ms=t),
                row(alert_id="x", event_time_ms=t + M),
            ]
        )
        == 1
    )
    assert n([row(alert_id="x"), row(alert_id="x", state="resolved", event_time_ms=t + M)]) == 0
    assert n([row(alert_id="x", state="absorbed")]) == 0
    assert n([row(alert_id="x"), row(alert_id="x", state="absorbed", event_time_ms=t + M)]) == 0


def test_selection_is_made_on_week_4():
    s4, r4 = _bursts(4, "k3")
    s5, r5 = _bursts(5, "k6")
    out = summarise(r4 + r5, s4 + s5, {"replay_end_ms": W5_END})
    assert out["selection"]["seasonal"] == {"setting": "k3", "branch": "highest_recall"}
    assert out["selected_week5"]["seasonal"]["setting"] == "k3"
    assert out["selected_week5"]["seasonal"]["detected_expected"] == 0
    assert out["weeks"]["5"]["seasonal"]["k6"]["detected_expected"] == 1


def test_matched_page_in_overlapping_scenarios_is_not_false():
    burst = scen(id="burst")
    other = scen(
        id="lat",
        type="offhours_latency",
        keys=(B,),
        signal="latency_p95",
        start=START + 5 * M,
        end=START + 25 * M,
    )
    monday = scen(
        id="mon",
        type="monday_peak",
        keys=(A, B),
        should=False,
        start=START - 60 * M,
        end=START + 180 * M,
    )
    rows = [
        row(alert_id="1"),
        row(alert_id="2", key=B, event_time_ms=START + 50 * M, starts_at_ms=START + 50 * M),
        row(
            alert_id="3",
            scope="region",
            key="us-east",
            tenant="",
            unit="",
            event_time_ms=START + 10 * M,
            starts_at_ms=START + 10 * M,
        ),
        row(
            alert_id="4",
            key=C,
            region="eu-central",
            event_time_ms=START + 7 * M,
            starts_at_ms=START + 7 * M,
        ),
        row(alert_id="1", state="resolved", event_time_ms=START + 40 * M),
        row(alert_id="3", state="resolved", event_time_ms=START + 50 * M),
        row(alert_id="9", state="absorbed", key=B),
    ]
    got = pages(rows, W4, W4_END)
    r = score(got, [burst, other, monday], "seasonal", "k4", 4)
    assert (r.pages, r.matched_pages, r.false_pages) == (4, 2, 2)
    by_id = {e["id"]: e for e in r.scenarios}
    assert by_id["mon"]["false_pages_during"] == 1  # only the page on B at +50 min
    assert by_id["mon"]["pages"] == 3
    assert by_id["burst"]["pages"] == 2 and by_id["lat"]["pages"] == 1
    assert by_id["lat"]["detected_any"] and not by_id["lat"]["detected_expected"]
    assert r.refires == 0 and by_id["mon"]["refires"] == 0


def test_other_signal_on_same_key_is_not_a_refire():
    ps = [page(), page(signal="latency_p95", event_time_ms=START + M, alert_id="2")]
    assert score(ps, [scen()], "seasonal", "k4", 4).refires == 0
    ps = [page(), page(event_time_ms=START + M, alert_id="2")]
    assert score(ps, [scen()], "seasonal", "k4", 4).refires == 1


def test_clip_boundary_is_inclusive():
    out, counts = normalise(
        [row(event_time_ms=W5_END), row(alert_id="b", event_time_ms=W5_END + 1)], W5_END
    )
    assert [r["alert_id"] for r in out] == ["a1"] and counts["clipped"] == 1


def test_regional_page_during_a_dip_is_regional_formed():
    dip = scen(id="d", type="holiday_dip", should=False)
    r = score([page(scope="region", key="us-east")], [dip], "seasonal", "k4", 4)
    assert r.scenarios[0]["regional_formed"] is True


def test_select_validates_its_input():
    k = FAMILIES["seasonal"]
    good = [sc(s, 8, 1.0, 6) for s in k]
    other_week = sc(k[0], 8, 1.0, 6)
    other_week.week = 5
    with pytest.raises(ValueError):
        select([other_week, *good[1:]])
    with pytest.raises(ValueError):
        select([sc(k[0], 8, 1.0, 6, detector="per_key_static"), *good[1:]])
    short = sc(k[0], 8, 1.0, 6)
    short.n_should_page = 9
    with pytest.raises(ValueError):
        select([short, *good[1:]])

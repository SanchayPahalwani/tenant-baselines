import logging

import httpx

from tenant_baselines.forwarder import ForwarderState, post_due, to_am_alert


def event(**over) -> dict:
    e = {
        "alert_id": "a1",
        "run_id": "r",
        "detector": "global_static",
        "setting": "default",
        "scope": "tenant",
        "key": "acme",
        "tenant": "acme",
        "region": "",
        "unit": "checkout",
        "signal": "error_rate",
        "state": "firing",
        "starts_at_ms": 0,
        "event_time_ms": 180_000,
        "emitted_at_ms": 181_000,
        "value": 0.4,
        "reference": 0.05,
        "reason": None,
    }
    e.update(over)
    return e


def new_state() -> ForwarderState:
    return ForwarderState("global_static", "default")


def test_fire_is_due_then_resent_after_60s():
    s = new_state()
    s.apply(event(), now_s=1000)
    assert len(s.due(1000)) == 1
    s.mark_sent(["a1"], 1000)
    assert s.due(1059) == []
    assert len(s.due(1060)) == 1


def test_duplicate_fire_is_idempotent():
    s = new_state()
    s.apply(event(), now_s=1000)
    s.apply(event(), now_s=1001)
    assert len(s.due(1001)) == 1


def test_resolve_sets_endsAt_and_drops_after_300s():
    s = new_state()
    s.apply(event(), now_s=1000)
    s.mark_sent(["a1"], 1000)
    assert "endsAt" not in s.due(1060)[0]
    s.apply(event(state="resolved", event_time_ms=600_000), now_s=1100)
    out = s.due(1100)
    assert len(out) == 1
    assert out[0]["endsAt"] == "1970-01-01T00:10:00Z"
    s.mark_sent(["a1"], 1100)
    assert len(s.due(1160)) == 1
    assert s.due(1100 + 301) == []


def test_resolve_is_due_immediately():
    s = new_state()
    s.apply(event(), now_s=1000)
    s.mark_sent(["a1"], 1000)
    s.apply(event(state="resolved", event_time_ms=600_000), now_s=1010)
    out = s.due(1010)
    assert len(out) == 1
    assert out[0]["endsAt"] == "1970-01-01T00:10:00Z"


def test_new_firing_alert_ends_older_firing_alert_with_same_labels():
    # An alert left firing by a job that was replaced without its state never gets a
    # resolved event. Alertmanager knows both by the same labels.
    s = new_state()
    s.apply(event(), now_s=1000)
    s.mark_sent(["a1"], 1000)
    s.apply(event(alert_id="a2", starts_at_ms=600_000, event_time_ms=780_000), now_s=2000)
    ids = [i for i, _ in s.due_items(2000)]
    out = s.due(2000)
    # The older one goes first, so Alertmanager ends it before it takes the new one.
    assert ids == ["a1", "a2"]
    assert out[0]["startsAt"] == "1970-01-01T00:00:00Z"
    assert out[0]["endsAt"] == "1970-01-01T00:10:00Z"
    assert "endsAt" not in out[1]
    s.mark_sent(ids, 2000)
    assert [i for i, _ in s.due_items(2060)] == ["a1", "a2"]
    # The ended one is dropped 300 s later; the new one is still sent.
    assert [i for i, _ in s.due_items(2000 + 301)] == ["a2"]


def test_new_firing_alert_leaves_other_labels_and_resolved_alerts_alone():
    s = new_state()
    s.apply(event(), now_s=1000)
    s.apply(event(state="resolved", event_time_ms=600_000), now_s=1100)
    s.apply(event(alert_id="b1", tenant="other", key="other"), now_s=1100)
    s.apply(event(alert_id="a2", starts_at_ms=1_200_000, event_time_ms=1_380_000), now_s=1200)
    out = dict(s.due_items(1200))
    assert out["a1"]["endsAt"] == "1970-01-01T00:10:00Z"
    assert "endsAt" not in out["b1"]
    assert "endsAt" not in out["a2"]


def test_resolve_of_unknown_id_does_nothing():
    s = new_state()
    s.apply(event(state="resolved"), now_s=1000)
    assert s.due(1000) == []


def test_stale_backfill_alert_is_skipped():
    s = new_state()
    s.apply(event(emitted_at_ms=180_000 + 3 * 3600 * 1000), now_s=1000)
    assert s.due(1000) == []


def test_stale_boundary_is_600s():
    kept = new_state()
    kept.apply(event(event_time_ms=180_000, emitted_at_ms=780_000), now_s=1000)
    assert len(kept.due(1000)) == 1
    skipped = new_state()
    skipped.apply(event(event_time_ms=180_000, emitted_at_ms=780_001), now_s=1000)
    assert skipped.due(1000) == []


def test_malformed_firing_event_is_rejected_by_apply(caplog):
    s = new_state()
    bad = event()
    del bad["value"]
    with caplog.at_level(logging.WARNING, logger="forwarder"):
        s.apply(bad, now_s=1000)
    assert "skipping bad alert event" in caplog.text
    assert s.due(1000) == []
    s.apply(event(alert_id="a2"), now_s=1001)
    assert len(s.due(1001)) == 1


def test_malformed_resolved_event_leaves_alert_firing():
    s = new_state()
    s.apply(event(), now_s=1000)
    s.apply(event(state="resolved", event_time_ms="soon"), now_s=1001)
    out = s.due(1001)
    assert len(out) == 1
    assert "endsAt" not in out[0]


def test_other_detector_ignored():
    s = new_state()
    s.apply(event(detector="other"), now_s=1000)
    s.apply(event(alert_id="a2", setting="strict"), now_s=1000)
    assert s.due(1000) == []


def test_failed_post_is_retried():
    s = new_state()
    s.apply(event(), now_s=1000)
    calls = []

    def fail(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(fail)) as client:
        assert post_due(client, "http://am:9093", s, 1000) == 0
    assert len(calls) == 1
    assert calls[0].url.path == "/api/v2/alerts"
    assert len(s.due(1005)) == 1


def test_post_error_is_not_raised():
    s = new_state()
    s.apply(event(), now_s=1000)

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    with httpx.Client(transport=httpx.MockTransport(boom)) as client:
        assert post_due(client, "http://am:9093", s, 1000) == 0
    assert len(s.due(1005)) == 1


def test_successful_post_marks_sent_and_skips_empty():
    s = new_state()
    s.apply(event(), now_s=1000)
    calls = []

    def ok(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200)

    with httpx.Client(transport=httpx.MockTransport(ok)) as client:
        assert post_due(client, "http://am:9093/", s, 1000) == 1
        assert post_due(client, "http://am:9093/", s, 1005) == 0
    assert len(calls) == 1
    assert s.due(1005) == []


def test_labels_and_rfc3339():
    a = to_am_alert(event())
    assert a["startsAt"] == "1970-01-01T00:00:00Z"
    assert a["labels"] == {
        "alertname": "TenantAnomaly",
        "scope": "tenant",
        "tenant": "acme",
        "unit": "checkout",
        "signal": "error_rate",
        "detector": "global_static",
        "setting": "default",
    }
    assert set(a["annotations"]) == {"summary", "value", "reference"}
    assert "endsAt" not in a
    assert to_am_alert(event(scope="region", region="eu"))["labels"]["alertname"] == "RegionAnomaly"

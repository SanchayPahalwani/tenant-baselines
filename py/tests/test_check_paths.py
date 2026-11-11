import random

import numpy as np
import pytest

from tenant_baselines import latency as lat
from tenant_baselines.check_paths import diff_aggregates, diff_logs, verdict
from tenant_baselines.evalmodel import EPOCH_MS, MINUTE_MS, REPLAY_END_MS
from tenant_baselines.simulate import Aggregates, rows
from tenant_baselines.world import Key

M = MINUTE_MS
KEY = "p1|eu-central|eu-central-s1"


def alert(**kw):
    base = {
        "alert_id": "a1",
        "run_id": "r-ev",
        "detector": "seasonal",
        "setting": "k4",
        "scope": "tenant",
        "key": KEY,
        "tenant": "p1",
        "region": "eu-central",
        "unit": "eu-central-s1",
        "signal": "error_rate",
        "state": "firing",
        "starts_at_ms": EPOCH_MS,
        "event_time_ms": EPOCH_MS,
        "emitted_at_ms": 5,
        "value": 0.25,
        "reference": 0.01,
        "reason": None,
    }
    base.update(kw)
    return base


def regional(**kw):
    return alert(
        alert_id="g1",
        scope="region",
        key="eu-central",
        tenant="",
        unit="",
        **kw,
    )


def log(n=40):
    """n alert rows a minute apart, the fifth a regional page."""
    out = [alert(alert_id=f"a{i}", event_time_ms=EPOCH_MS + i * M) for i in range(n)]
    out[4] = regional(event_time_ms=EPOCH_MS + 4 * M)
    return out


def other_run(rows_):
    """The same rows as another run writes them: shuffled, with its own run id and emit times."""
    out = [r | {"run_id": "r-ag", "emitted_at_ms": 1_000 + i} for i, r in enumerate(rows_)]
    random.Random(3).shuffle(out)
    return out


def test_equal_logs_have_no_diff():
    a = log()
    b = other_run(a) + [a[7] | {"emitted_at_ms": 99}]  # a duplicate, emitted again
    assert diff_logs(a, b) == []
    assert diff_logs([], []) == []


def test_diff_logs_reports_the_first_difference():
    a = log()
    b = other_run(a)
    changed = next(i for i, r in enumerate(b) if r["alert_id"] == "a9")
    b[changed] = b[changed] | {"value": 0.5}
    lines = diff_logs(a, b)
    assert len(lines) == 2
    assert lines[0].startswith("- ") and lines[1].startswith("+ ")
    assert '"alert_id":"a9"' in lines[0] and '"value":0.25' in lines[0]
    assert '"alert_id":"a9"' in lines[1] and '"value":0.5' in lines[1]
    assert "run_id" not in lines[0] and "emitted_at_ms" not in lines[1]

    # A row only in one log, and a row past the replay end that the normal form drops.
    b = other_run(a[:12] + a[13:]) + [alert(alert_id="late", event_time_ms=REPLAY_END_MS + 1)]
    lines = diff_logs(a, b)
    assert len(lines) == 1 and lines[0].startswith("- ") and '"alert_id":"a12"' in lines[0]

    # The first 20 differences, in normal-form order (event time first).
    b = other_run([r | {"value": 9.0} for r in a])
    lines = diff_logs(a, b)
    assert len(lines) == 20
    assert all('"alert_id":"a0"' in line for line in lines[:2])
    assert all('"alert_id":"a9"' in line for line in lines[18:])


def tiny_aggregates():
    """Two keys, six minutes; the second key's first request is in minute 2, minute 3 is silent."""
    keys = [Key("p1", "eu-central", "eu-central-s1"), Key("p2", "eu-central", "eu-central-s2")]
    count = np.array([[3, 1, 2, 4, 2, 1], [0, 0, 5, 0, 2, 7]], dtype=np.int64)
    errors = np.array([[1, 0, 0, 2, 0, 0], [0, 0, 1, 0, 0, 3]], dtype=np.int64)
    p95 = np.where(count > 0, lat.representative(np.full(count.shape, 80)), np.nan)
    return Aggregates(keys, np.array([0, 2]), count, errors, p95, EPOCH_MS)


def job_rows(a):
    """The rows as the events job writes them: with a run id, in no particular order."""
    out = [r | {"run_id": "r-ev"} for r in rows(a)]
    random.Random(5).shuffle(out)
    return out


def test_diff_aggregates_catches_a_missing_zero_row_and_a_p95_off_by_a_bucket():
    a = tiny_aggregates()
    good = job_rows(a)
    assert len(good) == 6 + 4
    assert diff_aggregates(good, a) == []

    silent = next(
        r for r in good if r["key"].startswith("p2|") and r["minute_ms"] == EPOCH_MS + 3 * M
    )
    assert silent["count"] == 0 and silent["p95_latency_ms"] is None
    missing = [r for r in good if r is not silent]
    (line,) = diff_aggregates(missing, a)
    assert "p2|" in line and str(EPOCH_MS + 3 * M) in line

    off = [dict(r) for r in good]
    target = next(r for r in off if r["key"].startswith("p1|") and r["minute_ms"] == EPOCH_MS + M)
    target["p95_latency_ms"] = float(lat.representative(81))
    (line,) = diff_aggregates(off, a)
    assert "p1|" in line and str(EPOCH_MS + M) in line

    # A row twice, a row before the key's first request, and a p95 on a zero row also differ.
    assert len(diff_aggregates(good + [good[0]], a)) == 1
    early = silent | {"minute_ms": EPOCH_MS}
    assert len(diff_aggregates(good + [early], a)) == 1
    zero_p95 = [r | {"p95_latency_ms": 1.0} if r is silent else r for r in good]
    assert len(diff_aggregates(zero_p95, a)) == 1


@pytest.fixture
def logs():
    a = log()
    return a, other_run(a)


def test_verdict_fails_on_empty_logs_no_regional_row_or_a_row_count_mismatch(logs):
    ev, ag = logs
    assert verdict(ev, ag, 10, 10, []) == []

    (empty,) = [r for r in verdict([], ag, 10, 10, []) if "empty" in r]
    assert "events" in empty
    (empty,) = [r for r in verdict(ev, [], 10, 10, []) if "empty" in r]
    assert "aggregates" in empty

    local = [r for r in ev if r["scope"] != "region"]
    (reason,) = verdict(local, other_run(local), 10, 10, [])
    assert "region" in reason
    # A regional row that is not a page does not count.
    resolved = local + [regional(state="resolved", event_time_ms=EPOCH_MS + 50 * M)]
    assert len(verdict(resolved, other_run(resolved), 10, 10, [])) == 1

    (reason,) = verdict(ev, ag, 9, 10, [])
    assert "9" in reason and "10" in reason

    (reason,) = verdict(ev, ag, 10, 10, ["p1|… minute 0: job …, python …"])
    assert "aggregates" in reason

    differs = [r | {"value": 2.0} if r["alert_id"] == "a3" else r for r in ag]
    (reason,) = verdict(ev, differs, 10, 10, [])
    assert "differ" in reason

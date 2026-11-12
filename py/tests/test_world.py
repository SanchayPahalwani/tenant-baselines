from collections import Counter

import pytest

from tenant_baselines.world import build_world


def test_demo_world_is_deterministic():
    assert build_world("demo", 1) == build_world("demo", 1)


def test_demo_world_shape():
    w = build_world("demo", 1)
    assert len(w.regions) == 3
    assert len(w.tenants) == 11
    assert len({k.key for k in w.keys}) == len(w.keys)
    assert len(w.keys) == 22
    assert sum(1 for u in w.units if u.dedicated_for) == 3
    assert {t.name for t in w.tenants if t.steady} == {"t09", "t10", "t11"}


def test_first_eight_tenants_are_unchanged():
    w = build_world("demo", 1)
    peaks = [t.peak_rpm for t in w.tenants[:8]]
    assert peaks == [197.8, 289.4, 195.4, 52.9, 309.5, 214.4, 97.6, 238.8]
    assert w.keys[0].key == "t01|us-east|us-east-s1"
    assert w.keys[17].key == "t08|eu-central|eu-central-s2"
    assert w.keys[19].key == "t09|ap-southeast|ap-southeast-d-t09"


def test_demo_world_unchanged():
    """The live demo's world (seed 1), pinned whole: milestones 1 to 3 measured on it."""
    w = build_world("demo", 1)
    assert [(r.name, r.utc_offset_h) for r in w.regions] == [
        ("us-east", -5),
        ("eu-central", 1),
        ("ap-southeast", 8),
    ]
    assert [(t.name, t.home_region, t.tz_offset_h, t.peak_rpm, t.steady) for t in w.tenants] == [
        ("t01", "us-east", -5, 197.8, False),
        ("t02", "eu-central", 1, 289.4, False),
        ("t03", "ap-southeast", 8, 195.4, False),
        ("t04", "us-east", -5, 52.9, False),
        ("t05", "eu-central", -8, 309.5, False),
        ("t06", "ap-southeast", 8, 214.4, False),
        ("t07", "us-east", -5, 97.6, False),
        ("t08", "eu-central", 1, 238.8, False),
        ("t09", "ap-southeast", 8, 60.0, True),
        ("t10", "ap-southeast", 8, 30.0, True),
        ("t11", "ap-southeast", 8, 30.0, True),
    ]
    assert all(t.start_week == 1 for t in w.tenants)
    assert [(u.name, u.region, u.dedicated_for, u.base_latency_ms) for u in w.units] == [
        ("us-east-s1", "us-east", None, 63.0),
        ("us-east-s2", "us-east", None, 31.7),
        ("eu-central-s1", "eu-central", None, 75.2),
        ("eu-central-s2", "eu-central", None, 62.3),
        ("ap-southeast-s1", "ap-southeast", None, 49.8),
        ("ap-southeast-s2", "ap-southeast", None, 77.3),
        ("us-east-d-t01", "us-east", "t01", 48.2),
        ("eu-central-d-t02", "eu-central", "t02", 57.2),
        ("ap-southeast-d-t09", "ap-southeast", "t09", 55.0),
    ]
    assert [k.key for k in w.keys] == [
        "t01|us-east|us-east-s1",
        "t01|us-east|us-east-d-t01",
        "t01|eu-central|eu-central-s1",
        "t02|eu-central|eu-central-s1",
        "t02|eu-central|eu-central-d-t02",
        "t03|ap-southeast|ap-southeast-s1",
        "t03|ap-southeast|ap-southeast-s2",
        "t04|us-east|us-east-s1",
        "t04|us-east|us-east-s2",
        "t04|eu-central|eu-central-s1",
        "t05|eu-central|eu-central-s1",
        "t05|eu-central|eu-central-s2",
        "t06|ap-southeast|ap-southeast-s1",
        "t06|ap-southeast|ap-southeast-s2",
        "t07|us-east|us-east-s1",
        "t07|us-east|us-east-s2",
        "t08|eu-central|eu-central-s1",
        "t08|eu-central|eu-central-s2",
        "t09|ap-southeast|ap-southeast-s1",
        "t09|ap-southeast|ap-southeast-d-t09",
        "t10|ap-southeast|ap-southeast-s1",
        "t11|ap-southeast|ap-southeast-s1",
    ]
    assert all(k.peak_rpm is None for k in w.keys)  # the tenant's peak is split over its keys


def test_key_string_and_offsets():
    w = build_world("demo", 1)
    k = w.keys[0]
    assert k.key == f"{k.tenant}|{k.region}|{k.unit}"
    offsets = {r.name: r.utc_offset_h for r in w.regions}
    differing = [t for t in w.tenants if t.tz_offset_h != offsets[t.home_region]]
    assert len(differing) == 1


def test_eval_world_shape():
    w = build_world("eval", 7)
    assert len(w.regions) == 3
    assert len(w.tenants) == 24
    home = Counter(t.home_region for t in w.tenants)
    assert set(home.values()) == {8}
    assert len(w.units) == 12
    assert len(w.keys) == 48
    assert len({k.key for k in w.keys}) == 48
    assert Counter(k.region for k in w.keys) == {r.name: 16 for r in w.regions}
    assert sum(1 for u in w.units if u.dedicated_for) == 6
    tenants = {t.name: t for t in w.tenants}
    assert all(k.region == tenants[k.tenant].home_region for k in w.keys)
    offsets = {r.name: r.utc_offset_h for r in w.regions}
    differing = [t for t in w.tenants if t.tz_offset_h != offsets[t.home_region]]
    assert [t.name for t in differing] == ["t07", "t11", "t15", "t19", "t23"]
    assert Counter(t.home_region for t in differing) == {
        "us-east": 2,
        "eu-central": 2,
        "ap-southeast": 1,
    }
    assert [t.name for t in w.tenants if t.start_week == 4] == ["t24"]
    assert all(k.peak_rpm is not None and k.peak_rpm > 0 for k in w.keys)
    assert all(u.region in offsets for u in w.units)


def test_eval_world_keys_follow_the_layout():
    w = build_world("eval", 7)
    units = {u.name: u for u in w.units}
    by_tenant: dict[str, list[str]] = {}
    for k in w.keys:
        by_tenant.setdefault(k.tenant, []).append(k.unit)
    assert by_tenant["t01"] == ["us-east-d-t01", "us-east-s1"]
    assert by_tenant["t07"] == ["us-east-s1", "us-east-s2"]
    assert by_tenant["t24"] == ["ap-southeast-s1", "ap-southeast-s2"]
    for name, us in by_tenant.items():
        assert len(us) == 2
        for u in us:
            assert units[u].dedicated_for in (None, name)
    assert all(60 <= u.base_latency_ms <= 120 for u in w.units)


def test_eval_world_is_seeded():
    assert build_world("eval", 7) == build_world("eval", 7)
    a, b = build_world("eval", 7), build_world("eval", 11)
    assert [k.peak_rpm for k in a.keys] != [k.peak_rpm for k in b.keys]


def test_paths_world_is_fixed():
    w = build_world("paths", 1)
    assert len(w.keys) == 10
    assert sum(k.peak_rpm for k in w.keys) == 1350
    assert w == build_world("paths", 99)
    tenants = {t.name: t for t in w.tenants}
    assert tenants["p3"].tz_offset_h == -5
    assert tenants["p5"].start_week == 4
    assert [u.name for u in w.units] == ["eu-central-s1", "eu-central-s2", "eu-central-d-p1"]


def test_unknown_world_is_rejected():
    with pytest.raises(ValueError):
        build_world("nope", 1)

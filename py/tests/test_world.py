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


def test_key_string_and_offsets():
    w = build_world("demo", 1)
    k = w.keys[0]
    assert k.key == f"{k.tenant}|{k.region}|{k.unit}"
    offsets = {r.name: r.utc_offset_h for r in w.regions}
    differing = [t for t in w.tenants if t.tz_offset_h != offsets[t.home_region]]
    assert len(differing) == 1

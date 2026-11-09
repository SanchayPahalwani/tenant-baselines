from tenant_baselines.world import build_world


def test_demo_world_is_deterministic():
    assert build_world("demo", 1) == build_world("demo", 1)


def test_demo_world_shape():
    w = build_world("demo", 1)
    assert len(w.regions) == 3
    assert len(w.tenants) == 8
    assert len({k.key for k in w.keys}) == len(w.keys)
    assert 18 <= len(w.keys) <= 22
    assert sum(1 for u in w.units if u.dedicated_for) >= 2


def test_key_string_and_offsets():
    w = build_world("demo", 1)
    k = w.keys[0]
    assert k.key == f"{k.tenant}|{k.region}|{k.unit}"
    offsets = {r.name: r.utc_offset_h for r in w.regions}
    differing = [t for t in w.tenants if t.tz_offset_h != offsets[t.home_region]]
    assert len(differing) == 1

import pytest

from tenant_baselines.gen_live import LiveGenerator
from tenant_baselines.incidents import Incident, demo_script, effects
from tenant_baselines.world import Key, build_world


def test_region_incident_window_and_scope():
    inc = Incident("error_spike", 1000, 2000, 0.2, region="eu-central")
    eu = Key("t01", "eu-central", "s1")
    us = Key("t01", "us-east", "s1")
    assert effects([inc], eu, 1000).error_add == pytest.approx(0.2)
    assert effects([inc], eu, 1999).error_add == pytest.approx(0.2)
    assert effects([inc], eu, 999).error_add == 0
    assert effects([inc], eu, 2000).error_add == 0
    assert effects([inc], us, 1500).error_add == 0


def test_combination_and_key_scope():
    k = Key("t01", "eu-central", "s1")
    incs = [
        Incident("volume_mult", 0, 10, 0.5, key=k.key),
        Incident("latency_spike", 0, 10, 3.0, region="eu-central"),
        Incident("volume_mult", 0, 10, 0.5, key="other|x|y"),
    ]
    e = effects(incs, k, 5)
    assert e.volume_mult == pytest.approx(0.5)
    assert e.latency_mult == pytest.approx(3.0)
    assert e.error_add == 0


def test_demo_script_shape():
    w = build_world("demo", 1)
    units = {u.name: u for u in w.units}
    drop, spike = demo_script(w, 1_000_000)
    assert drop.kind == "volume_mult"
    assert drop.magnitude == pytest.approx(0.1)
    key = next(k for k in w.keys if k.key == drop.key)
    assert units[key.unit].dedicated_for is not None
    assert drop.end_ms - drop.start_ms == 10 * 60_000
    assert spike.kind == "error_spike"
    assert spike.magnitude == pytest.approx(0.20)
    assert spike.region is not None
    assert spike.start_ms == drop.end_ms + 2 * 60_000


def test_demo_script_targets_are_detectable_all_week():
    w = build_world("demo", 1)
    gen = LiveGenerator(w, 1, [])
    t_start = 1_767_225_600_000  # a Thursday 00:00 UTC
    for q in range(7 * 24 * 4):
        t0 = t_start + q * 15 * 60_000
        drop, spike = demo_script(w, t0)
        key = next(k for k in w.keys if k.key == drop.key)
        assert gen.key_rate_rpm(key, drop.start_ms) >= 7
        busy = [
            k
            for k in w.keys
            if k.region == spike.region and gen.key_rate_rpm(k, spike.start_ms) >= 25
        ]
        assert len(busy) >= 2


def test_volume_mult_scales_volume():
    k = Key("t01", "eu-central", "s1")
    inc = Incident("volume_mult", 0, 60_000, 0.3, key=k.key)
    assert effects([inc], k, 30_000).volume_mult == pytest.approx(0.3)
    assert effects([inc], k, 60_000).volume_mult == 1.0
    both = [Incident("volume_mult", 0, 10, 0.5), Incident("volume_mult", 0, 10, 0.5)]
    assert effects(both, k, 5).volume_mult == pytest.approx(0.25)


def test_negative_volume_mult_is_rejected():
    with pytest.raises(ValueError):
        Incident("volume_mult", 0, 1, -0.1)

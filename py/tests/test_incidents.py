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
    steady = {t.name for t in w.tenants if t.steady}
    t0 = 1_000_000
    minute = 60_000
    drop, burst, spike = demo_script(w, t0)
    assert drop.kind == "volume_mult"
    assert drop.magnitude == pytest.approx(0.1)
    drop_key = next(k for k in w.keys if k.key == drop.key)
    assert units[drop_key.unit].dedicated_for is not None
    assert drop_key.tenant not in steady
    assert drop.end_ms - drop.start_ms == 10 * minute
    assert burst.kind == "error_spike"
    assert burst.magnitude == pytest.approx(0.20)
    burst_key = next(k for k in w.keys if k.key == burst.key)
    assert units[burst_key.unit].dedicated_for in steady
    assert (burst.start_ms, burst.end_ms) == (t0 + 3 * minute, t0 + 21 * minute)
    assert spike.kind == "error_spike"
    assert spike.magnitude == pytest.approx(0.20)
    assert spike.region == burst_key.region
    assert spike.key is None
    assert (spike.start_ms, spike.end_ms) == (t0 + 11 * minute, t0 + 21 * minute)


def test_demo_script_targets_are_detectable_all_week():
    """The rates and shares allow each alert and a regional alert at every quarter-hour.

    This does not show that the detector fires.
    """
    w = build_world("demo", 1)
    gen = LiveGenerator(w, 1, [])
    t_start = 1_767_225_600_000  # a Thursday 00:00 UTC
    minute = 60_000

    def active(region: str, t: int) -> set[str]:
        out = set()
        for k in w.keys:
            if k.region != region:
                continue
            mean = sum(gen.key_rate_rpm(k, t - m * minute) for m in range(1, 61)) / 60
            if mean >= 20:
                out.add(k.tenant)
        return out

    def strong(region: str, t: int, start: int, end: int) -> set[str]:
        return {
            k.tenant
            for k in w.keys
            if k.region == region
            and k.tenant in active(region, t)
            and gen.key_rate_rpm(k, start) >= 25
            and gen.key_rate_rpm(k, end) >= 25
        }

    for q in range(7 * 24 * 4):
        t0 = t_start + q * 15 * minute
        drop, burst, spike = demo_script(w, t0)
        drop_key = next(k for k in w.keys if k.key == drop.key)
        assert gen.key_rate_rpm(drop_key, drop.start_ms) >= 7
        burst_key = next(k for k in w.keys if k.key == burst.key)
        assert gen.key_rate_rpm(burst_key, burst.start_ms) >= 25
        assert gen.key_rate_rpm(burst_key, burst.end_ms) >= 25
        region = burst_key.region
        assert len(active(region, t0 + 8 * minute)) >= 3
        for at in (16, 19):
            t = t0 + at * minute
            n_active = len(active(region, t))
            n_strong = len(strong(region, t, spike.start_ms, spike.end_ms))
            assert n_active >= 3
            assert n_strong >= 3
            assert 2 * n_strong >= n_active


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


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("kind", ["error_spike", "volume_mult", "latency_spike"])
def test_non_finite_magnitude_is_rejected(kind, bad):
    with pytest.raises(ValueError):
        Incident(kind, 0, 1, bad)

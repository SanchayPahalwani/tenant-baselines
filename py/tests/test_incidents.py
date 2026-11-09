import pytest

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
        Incident("volume_drop", 0, 10, 0.5, key=k.key),
        Incident("latency_spike", 0, 10, 3.0, region="eu-central"),
        Incident("volume_drop", 0, 10, 0.5, key="other|x|y"),
    ]
    e = effects(incs, k, 5)
    assert e.volume_mult == pytest.approx(0.5)
    assert e.latency_mult == pytest.approx(3.0)
    assert e.error_add == 0


def test_demo_script():
    (inc,) = demo_script(build_world("demo", 1), 1_000_000)
    assert inc.kind == "error_spike"
    assert inc.region == "eu-central"
    assert inc.start_ms == 1_000_000 + 180_000
    assert inc.end_ms == 1_000_000 + 780_000
    assert inc.magnitude == pytest.approx(0.20)

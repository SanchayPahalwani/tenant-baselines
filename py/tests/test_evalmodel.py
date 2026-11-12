import numpy as np
import pytest

from tenant_baselines import evalmodel as em
from tenant_baselines.evalmodel import (
    EPOCH_MS,
    MINUTES,
    Effects,
    TrafficParams,
    error_prob,
    latency_median,
    no_effects,
    rates,
    week_range,
)
from tenant_baselines.traffic import shape
from tenant_baselines.world import Key, Region, Tenant, Unit, World, build_world

PLAIN = TrafficParams(nuisance=False)


def one_tenant_world(tz: int, peak: float = 100.0, start_week: int = 1, steady: bool = False):
    t = Tenant("t1", "r", tz, peak, steady, start_week)
    return World(
        (Region("r", tz),),
        (t,),
        (Unit("r-s1", "r", None, 80.0),),
        (Key("t1", "r", "r-s1", peak),),
    )


def minute_of(day: int, hour: int, minute: int = 0) -> int:
    return day * 1440 + hour * 60 + minute


def test_epoch_is_a_monday_week_boundary():
    assert (EPOCH_MS + 259_200_000) % 604_800_000 == 0
    assert MINUTES == 50_400
    assert week_range(1) == (EPOCH_MS, EPOCH_MS + em.WEEK_MS)
    assert week_range(5)[1] == em.REPLAY_END_MS


def test_rates_are_deterministic():
    w = build_world("eval", 7)
    a, b = rates(w, TrafficParams(), 7), rates(w, TrafficParams(), 7)
    assert np.array_equal(a.base, b.base)
    assert a.base.shape == (48, MINUTES)
    c = rates(w, TrafficParams(), 11)
    assert not np.array_equal(a.base, c.base)


def test_no_nuisance_is_peak_times_shape():
    w = one_tenant_world(tz=1, peak=120.0)
    r = rates(w, PLAIN, 3)
    assert r.peak[0] == 120.0
    # Tue 12:00 local = 11:00 UTC; Mon 09:30 local = 08:30 UTC; Sun 03:00 local = 02:00 UTC.
    for m, hour, wd in [
        (minute_of(1, 11), 12, 1),
        (minute_of(0, 8, 30), 9.5, 0),
        (minute_of(6, 2), 3, 6),
    ]:
        assert r.base[0, m] == pytest.approx(120.0 * shape(hour, wd))


def test_steady_tenant_shape_is_one():
    r = rates(one_tenant_world(tz=0, steady=True), PLAIN, 3)
    assert np.all(r.base == 100.0)


@pytest.mark.parametrize("amplitude,ratio", [(1.5, 1 / 1.5), (6.0, 1 / 6), (12.0, 1 / 12)])
def test_amplitude_sets_the_night_level(amplitude, ratio):
    r = rates(one_tenant_world(tz=0), TrafficParams(amplitude=amplitude, nuisance=False), 3)
    night, day = r.base[0, minute_of(2, 3)], r.base[0, minute_of(2, 12)]
    assert night / day == pytest.approx(ratio)


def test_new_tenant_is_silent_before_week_four():
    w = build_world("eval", 7)
    r = rates(w, TrafficParams(), 7)
    new = [i for i, key in enumerate(w.keys) if key.tenant == "t24"]
    assert len(new) == 2
    for k in new:
        assert r.start[k] == 3 * 10_080
        assert np.all(r.base[k, : 3 * 10_080] == 0)
        assert np.all(r.base[k, 3 * 10_080 :] > 0)
    others = [i for i in range(len(w.keys)) if i not in new]
    assert np.all(r.start[others] == 0)
    assert np.all(r.base[others] > 0)  # every other key has traffic from the first minute


def test_day_factor_changes_at_local_midnight():
    tz = 8
    day, _, _ = em._nuisance(7, 0, tz, MINUTES)
    changes = np.flatnonzero(day[1:] != day[:-1]) + 1
    assert len(changes) == 35
    assert np.all((changes + tz * 60) % 1440 == 0)
    assert np.std(np.log(day)) > 0.05


def test_bursts_last_five_minutes_and_do_not_stack():
    w = one_tenant_world(tz=-5)
    plain = rates(w, PLAIN, 5).base[0]
    full = rates(w, TrafficParams(), 5).base[0]
    day, trend, burst = em._nuisance(5, 0, -5, MINUTES)
    assert np.allclose(full, plain * day * trend * burst)
    assert set(np.unique(burst)) <= {1.0, 1.8}
    on = burst > 1
    edges = np.flatnonzero(np.diff(np.concatenate([[0], on.astype(int), [0]])))
    starts, ends = edges[0::2], edges[1::2]
    runs = ends - starts
    # Bursts are 5 minutes; overlapping ones merge into a longer run, never a shorter one. Only
    # a run cut by the first or the last minute of the replay may be shorter.
    inside = (starts > 0) & (ends < MINUTES)
    assert inside.sum() >= 25 and runs[inside].min() == 5
    assert on.sum() / MINUTES == pytest.approx(5 / 1440, rel=0.4)


def test_trend_stays_within_three_percent_a_week():
    for i in range(24):
        _, trend, _ = em._nuisance(7, i, 0, MINUTES)
        weekly = trend[10_080:] / trend[:-10_080]
        assert np.all(np.abs(weekly - 1) <= 0.03 + 1e-12)
        assert trend[0] == pytest.approx(1.0)


def test_peak_splits_tenant_peak_when_key_has_none():
    w = build_world("demo", 1)
    r = rates(w, PLAIN, 1)
    t1 = w.tenants[0]
    n = sum(1 for k in w.keys if k.tenant == t1.name)
    assert n == 3
    assert r.peak[0] == pytest.approx(t1.peak_rpm / 3)
    assert r.base.shape == (len(w.keys), MINUTES)


def test_effects_helpers():
    fx = no_effects(3, 4)
    assert isinstance(fx, Effects)
    assert fx.volume.shape == fx.error_add.shape == fx.latency.shape == (3, 4)
    assert np.all(fx.volume == 1) and np.all(fx.error_add == 0) and np.all(fx.latency == 1)
    assert error_prob(np.array([0.0, 1.0]), error_add=np.zeros(2)) == pytest.approx([0.003, 0.005])
    assert error_prob(np.array([1.0]), error_add=np.array([5.0]))[0] == 1.0
    assert latency_median(50.0, np.array([0.0, 1.0]), latency=np.ones(2)) == pytest.approx([50, 80])
    assert latency_median(50.0, np.array([1.0]), latency=np.array([2.0]))[0] == pytest.approx(160)


def test_effect_arguments_are_required():
    with pytest.raises(TypeError):
        error_prob(np.array([1.0]))  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        latency_median(50.0, np.array([1.0]))  # type: ignore[call-arg]

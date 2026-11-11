from datetime import UTC, datetime

import numpy as np
import pytest

from tenant_baselines.traffic import (
    error_prob,
    key_rate_rpm,
    latency_median_ms,
    rate_rpm,
    shape,
    shape_array,
)
from tenant_baselines.world import Tenant


def test_shape_points():
    assert shape(3, 2) == pytest.approx(1 / 6)
    assert shape(12, 2) == 1
    assert shape(12, 6) == pytest.approx(0.3)
    assert shape(9.5, 0) == pytest.approx(1.25)
    assert shape(8, 2) == pytest.approx((1 / 6 + 1) / 2)
    assert shape(22, 2) == pytest.approx(1 / 6)


def test_rate_uses_tenant_local_time():
    t = Tenant("tx", "ap-southeast", 8, 100.0)
    tue = datetime(2026, 11, 10, tzinfo=UTC)  # a Tuesday
    ms = int(tue.timestamp() * 1000)
    hour = 3_600_000
    assert rate_rpm(t, ms + 4 * hour) == pytest.approx(100.0)
    assert rate_rpm(t, ms + 20 * hour) < 100.0 / 5  # local 04:00 the next day


def test_error_and_latency():
    assert error_prob(0) == pytest.approx(0.003)
    assert error_prob(1) == pytest.approx(0.005)
    assert latency_median_ms(50, 0) == 50
    assert latency_median_ms(50, 1) == pytest.approx(80)


def test_key_rate_splits_tenant_rate():
    t = Tenant("tx", "us-east", 0, 120.0)
    wed_noon = int(datetime(2026, 11, 11, 12, tzinfo=UTC).timestamp() * 1000)
    assert key_rate_rpm(t, 3, wed_noon) == pytest.approx(40.0)


def test_steady_tenant_has_flat_load():
    t09 = Tenant("t09", "ap-southeast", 8, 60.0, steady=True)
    noon = int(datetime(2026, 11, 10, 4, tzinfo=UTC).timestamp() * 1000)  # Tue 12:00 local
    night = int(datetime(2026, 11, 7, 19, tzinfo=UTC).timestamp() * 1000)  # Sun 03:00 local
    assert key_rate_rpm(t09, 2, noon) == 30.0
    assert key_rate_rpm(t09, 2, night) == 30.0


@pytest.mark.parametrize("amplitude", [1.5, 6.0, 12.0])
def test_shape_array_matches_shape(amplitude):
    hours = np.tile(np.arange(0, 24, 0.25), 7)
    days = np.repeat(np.arange(7), 96)
    got = shape_array(hours, days, amplitude)
    want = [shape(float(h), int(d), amplitude) for h, d in zip(hours, days, strict=True)]
    assert got == pytest.approx(want)

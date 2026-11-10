from datetime import UTC, datetime

import pytest

from tenant_baselines.traffic import error_prob, key_rate_rpm, latency_median_ms, rate_rpm, shape
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
    assert key_rate_rpm(t, 3, wed_noon) == 40.0

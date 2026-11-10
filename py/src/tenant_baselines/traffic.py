"""Pure traffic model: daily/weekly load shape, error probability, latency."""

from tenant_baselines.world import Tenant

_MS_PER_HOUR = 3_600_000
_MS_PER_DAY = 86_400_000
# 1970-01-01 was a Thursday (weekday 3, Monday = 0).
_EPOCH_WEEKDAY = 3
LATENCY_SIGMA = 0.5


def shape(local_hour: float, weekday: int, amplitude: float = 6.0) -> float:
    """Fraction of peak load at a local hour (0-24) and weekday (Monday = 0)."""
    low = 1.0 / amplitude
    h = local_hour
    if h < 7:
        v = low
    elif h < 9:
        v = low + (1 - low) * (h - 7) / 2
    elif h < 17:
        v = 1.0
    elif h < 20:
        v = 1 - (1 - low) * (h - 17) / 3
    else:
        v = low
    if weekday >= 5:
        v *= 0.3
    if weekday == 0 and 8 <= h < 11:
        v *= 1.25
    return v


def local_time(t_ms: int, tz_offset_h: int) -> tuple[float, int]:
    """Return (local hour of day, weekday) for an epoch-ms time and a UTC offset."""
    local_ms = t_ms + tz_offset_h * _MS_PER_HOUR
    day, ms_of_day = divmod(local_ms, _MS_PER_DAY)
    return ms_of_day / _MS_PER_HOUR, (day + _EPOCH_WEEKDAY) % 7


def load(tenant: Tenant, t_ms: int) -> float:
    hour, weekday = local_time(t_ms, tenant.tz_offset_h)
    return shape(hour, weekday)


def rate_rpm(tenant: Tenant, t_ms: int) -> float:
    return tenant.peak_rpm * load(tenant, t_ms)


def error_prob(load: float) -> float:
    return 0.003 + 0.002 * load


def latency_median_ms(base: float, load: float) -> float:
    return base * (1 + 0.6 * load**2)

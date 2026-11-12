import numpy as np
import pytest

from tenant_baselines import latency


def test_bounds_match_the_shared_file():
    b = latency.bounds()
    assert b.shape == (202,)
    assert b[0] == 0.5
    assert np.all(np.diff(b) > 0)


def test_bucket_edges():
    x = np.array([0.1, 0.5, 0.52999, 0.53, 1e9])
    assert latency.bucket_of(x).tolist() == [0, 0, 0, 1, 201]


def test_representative():
    b = latency.bounds()
    got = latency.representative(np.array([0, 10, 201]))
    assert got[0] == pytest.approx(np.sqrt(b[0] * b[1]))
    assert got[1] == pytest.approx(np.sqrt(b[10] * b[11]))
    assert got[2] == b[201]


def test_rank_is_the_ceiling_of_ninety_five_percent():
    counts = np.zeros((2, 202), dtype=np.int64)
    counts[:, 10] = [19, 19]
    counts[:, 50] = [1, 2]
    got = latency.p95(counts, np.array([20, 21]))
    assert got.tolist() == latency.representative(np.array([10, 50])).tolist()


def test_empty_minute_has_no_p95():
    counts = np.zeros((2, 202), dtype=np.int64)
    counts[1, 5] = 3
    got = latency.p95(counts, np.array([0, 3]))
    assert np.isnan(got[0])
    assert got[1] == latency.representative(np.array([5]))[0]


def test_short_counts_fall_to_the_last_bucket():
    counts = np.zeros((1, 202), dtype=np.int64)
    counts[0, 10] = 5
    got = latency.p95(counts, np.array([100]))
    assert got[0] == latency.bounds()[201]


def test_rejects_nan_and_negative():
    with pytest.raises(ValueError):
        latency.bucket_of(np.array([1.0, np.nan]))
    with pytest.raises(ValueError):
        latency.bucket_of(np.array([-0.1]))


def test_every_bound_is_its_own_bucket_and_just_below_is_the_one_before():
    b = latency.bounds()
    assert latency.bucket_of(b).tolist() == list(range(202))
    below = np.nextafter(b, -np.inf)
    assert latency.bucket_of(below).tolist() == [0, *range(201)]


def test_p95_rank_over_a_sweep_of_minute_counts():
    """The rank is ceil(0.95 n) in float64, as in LatencyHistogram: pinned for n up to 5,000.

    Bucket 0 holds `c0` of the n requests and bucket 1 the rest, so the p95 is bucket 0 exactly
    when c0 reaches the rank."""
    n = np.arange(1, 5001)
    rank = -(-19 * n // 20)  # the exact ceiling of 0.95 n
    assert np.array_equal(np.ceil(0.95 * n.astype(np.float64)), rank)
    for c0, bucket in ((rank, 0), (rank - 1, 1)):
        counts = np.zeros((len(n), 202), dtype=np.int64)
        counts[:, 0] = c0
        counts[:, 1] = n - c0
        want = latency.representative(np.array([bucket]))[0]
        assert np.all(latency.p95(counts, n) == want), bucket

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

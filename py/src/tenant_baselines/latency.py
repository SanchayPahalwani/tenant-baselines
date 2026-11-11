"""Latency buckets and p95, exactly as the Flink job's LatencyHistogram does."""

from functools import cache
from pathlib import Path

import numpy as np

BUCKETS = 202


@cache
def bounds() -> np.ndarray:
    """The lower bounds in ms from shared/latency_bounds_ms.txt, read once."""
    path = Path(__file__).resolve().parents[3] / "shared" / "latency_bounds_ms.txt"
    if not path.is_file():
        raise FileNotFoundError(str(path))
    values = [float(s) for s in path.read_text().split()]
    if len(values) != BUCKETS:
        raise ValueError(f"expected {BUCKETS} bounds in {path}, found {len(values)}")
    out = np.array(values, dtype=np.float64)
    out.setflags(write=False)
    return out


def bucket_of(latency_ms: np.ndarray) -> np.ndarray:
    """Index of the largest bound at most the latency; 0 below the first bound."""
    x = np.asarray(latency_ms, dtype=np.float64)
    if np.isnan(x).any():
        raise ValueError("latency is NaN")
    if (x < 0).any():
        raise ValueError("latency is negative")
    return np.maximum(np.searchsorted(bounds(), x, side="right") - 1, 0)


def representative(bucket: np.ndarray) -> np.ndarray:
    """Geometric midpoint of the bucket; the last bucket gives its lower bound."""
    b = bounds()
    i = np.asarray(bucket)
    nxt = b[np.minimum(i + 1, BUCKETS - 1)]
    return np.where(i >= BUCKETS - 1, b[BUCKETS - 1], np.sqrt(b[i] * nxt))


def p95(counts: np.ndarray, n: np.ndarray) -> np.ndarray:
    """p95 per row of (minutes, 202) counts; NaN where n == 0.

    Rank is ceil(0.95 * n) in float64. If the counts never reach the rank, the
    last bucket is used, as in the job.
    """
    counts = np.asarray(counts)
    n = np.asarray(n)
    rank = np.ceil(0.95 * n.astype(np.float64))
    reached = np.cumsum(counts, axis=1) >= rank[:, None]
    idx = np.where(reached.any(axis=1), reached.argmax(axis=1), counts.shape[1] - 1)
    return np.where(n == 0, np.nan, representative(idx))

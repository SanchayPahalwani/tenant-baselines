import numpy as np
import orjson

from tenant_baselines.events import encode


def test_encode_wire_format():
    out = orjson.loads(encode(60000, "t01", "eu-central", "s1", 83.24, False))
    assert out == {
        "ts": 60000,
        "tn": "t01",
        "rg": "eu-central",
        "un": "s1",
        "lat": 83.2,
        "ok": False,
    }


def test_latency_is_rounded_once_to_a_tenth():
    """The draws are rounded to 0.1 ms by numpy; the encoder's own rounding must keep those values
    as they are, and round anything finer to a tenth."""
    drawn = np.round(np.array([0.1, 99.95, 123456.7, 0.05, 83.25, 1e-3, 2999.99]), 1)
    for x in [*drawn.tolist(), *np.round(np.random.default_rng(1).lognormal(4, 1, 2000), 1)]:
        assert orjson.loads(encode(0, "t", "r", "u", float(x), True))["lat"] == float(x)
    for raw, tenth in ((0.123, 0.1), (99.951, 100.0), (123456.749, 123456.7), (83.24, 83.2)):
        assert orjson.loads(encode(0, "t", "r", "u", raw, True))["lat"] == tenth

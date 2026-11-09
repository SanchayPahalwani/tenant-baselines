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

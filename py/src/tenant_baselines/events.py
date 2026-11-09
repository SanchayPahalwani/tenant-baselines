"""Wire format of live events."""

import orjson


def encode(ts_ms: int, tenant: str, region: str, unit: str, latency_ms: float, ok: bool) -> bytes:
    return orjson.dumps(
        {"ts": ts_ms, "tn": tenant, "rg": region, "un": unit, "lat": round(latency_ms, 1), "ok": ok}
    )

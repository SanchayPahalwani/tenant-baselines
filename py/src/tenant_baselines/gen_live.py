"""Live event generator: one second of synthetic events per tick, written to Kafka."""

import argparse
import logging
import time

import numpy as np

from tenant_baselines.events import encode
from tenant_baselines.incidents import Incident, demo_script, effects
from tenant_baselines.traffic import (
    LATENCY_SIGMA,
    error_prob,
    key_rate_rpm,
    latency_median_ms,
    load,
)
from tenant_baselines.world import Key, World, build_world

log = logging.getLogger("gen_live")


class LiveGenerator:
    def __init__(self, world: World, seed: int, incidents: list[Incident]):
        self._world = world
        self._rng = np.random.default_rng(seed)
        self._incidents = incidents
        tenants = {t.name: t for t in world.tenants}
        units = {u.name: u for u in world.units}
        per_tenant: dict[str, int] = {}
        for k in world.keys:
            per_tenant[k.tenant] = per_tenant.get(k.tenant, 0) + 1
        self._rows = [
            (k, tenants[k.tenant], units[k.unit].base_latency_ms, per_tenant[k.tenant])
            for k in world.keys
        ]

    def key_rate_rpm(self, key: Key, t_ms: int) -> float:
        """Base rate of a key: its tenant's rate split evenly over the tenant's keys."""
        for k, tenant, _, n_keys in self._rows:
            if k == key:
                return key_rate_rpm(tenant, n_keys, t_ms)
        raise KeyError(key)

    def tick(self, second_ms: int) -> list[tuple[str, bytes]]:
        """Events for [second_ms, second_ms + 1000), sorted by timestamp."""
        out: list[tuple[int, str, bytes]] = []
        for key, tenant, base_latency, n_keys in self._rows:
            fx = effects(self._incidents, key, second_ms)
            lo = load(tenant, second_ms)
            rate = key_rate_rpm(tenant, n_keys, second_ms) / 60.0 * fx.volume_mult
            n = int(self._rng.poisson(rate))
            if n == 0:
                continue
            offsets = self._rng.integers(0, 1000, n)
            median = latency_median_ms(base_latency, lo) * fx.latency_mult
            lat = self._rng.lognormal(np.log(median), LATENCY_SIGMA, n)
            p_ok = 1.0 - min(1.0, error_prob(lo) + fx.error_add)
            ok = self._rng.random(n) < p_ok
            for i in range(n):
                ts = second_ms + int(offsets[i])
                payload = encode(ts, key.tenant, key.region, key.unit, float(lat[i]), bool(ok[i]))
                out.append((ts, key.key, payload))
        out.sort(key=lambda e: e[0])
        return [(k, p) for _, k, p in out]


class DeliveryStats:
    """Counts delivery results; `on_delivery` is the producer callback."""

    def __init__(self) -> None:
        self.failed = 0
        self.delivered = 0

    def on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.warning("delivery failed: %s", err)
        else:
            self.delivered += 1


def main() -> None:
    from confluent_kafka import Producer

    ap = argparse.ArgumentParser(description="Generate synthetic live events into Kafka")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="live.events")
    ap.add_argument("--world", default="demo")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--script", choices=["demo", "none"], default="demo")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    world = build_world(args.world, args.seed)
    next_second = int(time.time()) * 1000
    script = demo_script(world, next_second) if args.script == "demo" else []
    gen = LiveGenerator(world, args.seed, script)
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 50,
            "compression.type": "zstd",
            # A retry must not reorder messages within a partition.
            "enable.idempotence": True,
        }
    )

    stats = DeliveryStats()
    sent = 0
    sent_total = 0
    last_delivered = last_failed = 0
    last_log = time.monotonic()
    while True:
        # Produce a second only after it has ended, so events stay in time order.
        wait = (next_second + 1000) / 1000 - time.time()
        if wait > 0:
            time.sleep(wait)
        for key, payload in gen.tick(next_second):
            while True:
                try:
                    producer.produce(
                        args.topic,
                        value=payload,
                        key=key.encode(),
                        on_delivery=stats.on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            sent += 1
        producer.poll(0)
        next_second += 1000
        if time.monotonic() - last_log >= 10:
            sent_total += sent
            log.info(
                "queued %d, delivered %d, failed %d in last 10s (%d queued total)",
                sent,
                stats.delivered - last_delivered,
                stats.failed - last_failed,
                sent_total,
            )
            sent = 0
            last_delivered, last_failed = stats.delivered, stats.failed
            last_log = time.monotonic()


if __name__ == "__main__":
    main()

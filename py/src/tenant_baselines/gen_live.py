"""Live event generator: one second of synthetic events per tick, written to Kafka."""

import argparse
import logging
import signal
import sys
import time
from typing import Literal

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


def next_step(next_ms: int, now_ms: int) -> Literal["minute", "second", "wait"]:
    """What to produce next: a whole minute when it has ended and starts on a minute boundary,
    else one second when that has ended, else wait."""
    if next_ms % 60_000 == 0 and next_ms + 60_000 <= now_ms:
        return "minute"
    if next_ms + 1000 <= now_ms:
        return "second"
    return "wait"


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
        self._key_index = {k.key: i for i, k in enumerate(world.keys)}
        self._rows = [
            (k, tenants[k.tenant], units[k.unit].base_latency_ms, per_tenant[k.tenant])
            for k in world.keys
        ]

    def set_incidents(self, incidents: list[Incident]) -> None:
        self._incidents = incidents

    def partition_of(self, key: str, n_partitions: int) -> int:
        """Partition for a key: its index in the world's key list modulo n_partitions."""
        return self._key_index[key] % n_partitions

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

    def minute(self, minute_ms: int) -> list[tuple[str, bytes]]:
        """Events for [minute_ms, minute_ms + 60_000), sorted by timestamp.

        Counts, load and incident effects are taken at the minute's start. Draws are
        vectorised per key and the minute is sorted once.
        """
        ts_parts, lat_parts, ok_parts, idx_parts = [], [], [], []
        for i, (key, tenant, base_latency, n_keys) in enumerate(self._rows):
            fx = effects(self._incidents, key, minute_ms)
            lo = load(tenant, minute_ms)
            n = int(self._rng.poisson(key_rate_rpm(tenant, n_keys, minute_ms) * fx.volume_mult))
            if n == 0:
                continue
            ts_parts.append(minute_ms + self._rng.integers(0, 60_000, n))
            median = latency_median_ms(base_latency, lo) * fx.latency_mult
            lat_parts.append(self._rng.lognormal(np.log(median), LATENCY_SIGMA, n))
            p_ok = 1.0 - min(1.0, error_prob(lo) + fx.error_add)
            ok_parts.append(self._rng.random(n) < p_ok)
            idx_parts.append(np.full(n, i, dtype=np.int32))
        if not ts_parts:
            return []
        ts = np.concatenate(ts_parts)
        order = np.argsort(ts, kind="stable")
        ts = ts[order].tolist()
        lat = np.concatenate(lat_parts)[order].tolist()
        ok = np.concatenate(ok_parts)[order].tolist()
        idx = np.concatenate(idx_parts)[order].tolist()
        rows = self._rows
        out = []
        for t, la, o, i in zip(ts, lat, ok, idx, strict=True):
            key = rows[i][0]
            out.append((key.key, encode(t, key.tenant, key.region, key.unit, la, o)))
        return out


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


def _fmt_utc(ms: int) -> str:
    return time.strftime("%H:%M", time.gmtime(ms / 1000))


def main() -> None:
    from confluent_kafka import Producer

    ap = argparse.ArgumentParser(description="Generate synthetic live events into Kafka")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="live.events")
    ap.add_argument("--world", default="demo")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--script", choices=["demo", "none"], default="demo")
    ap.add_argument("--history-days", type=int, default=21)
    ap.add_argument("--partitions", type=int, default=8)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    world = build_world(args.world, args.seed)
    gen = LiveGenerator(world, args.seed, [])
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 50,
            "compression.type": "zstd",
            # A retry must not reorder messages within a partition.
            "enable.idempotence": True,
        }
    )

    def stop(signum, frame) -> None:
        producer.flush(10)
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    start_ms = int(time.time() * 1000)
    catching_up = args.history_days > 0
    if catching_up:
        next_ms = (start_ms - args.history_days * 86_400_000) // 60_000 * 60_000
    else:
        # Live only, as in milestone 1: start at the current second with the script on.
        next_ms = start_ms // 1000 * 1000
        if args.script == "demo":
            gen.set_incidents(demo_script(world, next_ms))
    history_events = 0
    history_start = time.monotonic()
    history_start_ms = next_ms

    stats = DeliveryStats()
    sent = 0
    sent_total = 0
    last_delivered = last_failed = 0
    last_log = time.monotonic()
    while True:
        now_ms = int(time.time() * 1000)
        step = next_step(next_ms, now_ms)
        if step == "wait":
            if catching_up:
                catching_up = False
                if args.script == "demo":
                    script = demo_script(world, next_ms)
                    gen.set_incidents(script)
                    drop, spike = script
                    log.info(
                        "caught up after %.0f s, %d events; demo script: volume x0.1 on %s "
                        "%s-%s UTC, error spike in %s %s-%s UTC",
                        time.monotonic() - history_start,
                        history_events,
                        drop.key,
                        _fmt_utc(drop.start_ms),
                        _fmt_utc(drop.end_ms),
                        spike.region,
                        _fmt_utc(spike.start_ms),
                        _fmt_utc(spike.end_ms),
                    )
                else:
                    log.info(
                        "caught up after %.0f s, %d events",
                        time.monotonic() - history_start,
                        history_events,
                    )
            time.sleep(max(0.0, (next_ms + 1000 - now_ms) / 1000))
            continue
        if step == "minute":
            batch = gen.minute(next_ms)
            next_ms += 60_000
        else:
            batch = gen.tick(next_ms)
            next_ms += 1000
        for key, payload in batch:
            while True:
                try:
                    producer.produce(
                        args.topic,
                        value=payload,
                        key=key.encode(),
                        partition=gen.partition_of(key, args.partitions),
                        on_delivery=stats.on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
        sent += len(batch)
        if catching_up:
            history_events += len(batch)
        producer.poll(0)
        if time.monotonic() - last_log >= 10:
            sent_total += sent
            if catching_up:
                done = (next_ms - history_start_ms) / max(1, start_ms - history_start_ms)
                log.info(
                    "history: reached %s UTC, %.0f%% done, %.0f events/s, %d failed",
                    time.strftime("%Y-%m-%d %H:%M", time.gmtime(next_ms / 1000)),
                    100 * min(1.0, done),
                    sent / 10,
                    stats.failed,
                )
            else:
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

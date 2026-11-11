"""Kafka topics, production and complete reads for the eval runs. Synthetic data only.

Every per-run topic is created with append-time stamps, and no producer here passes a
timestamp: the broker stamps each record when it appends it, so retention counts wall-clock
age and not the replay's event time. Topics are never deleted, and a run id is never reused.
"""

import time
from collections.abc import Iterable

import orjson
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
# The stack publishes Kafka on 127.0.0.1 only, and the broker advertises `localhost`, which
# resolves to ::1 first. A refused ::1 connection sometimes failed a watermark query with
# _DESTROY_BROKER (7 of 30 tries on the stack); IPv4 only, none did.
_CLIENT = {"broker.address.family": "v4"}
_TIMEOUT_S = 30
_FLUSH_S = 300
READ_IDLE_S = 30  # a read gives up after this long without a record
_BATCH = 10_000


class IncompleteRead(Exception):
    pass


def create_topic(
    name: str, partitions: int, retention_ms: int = 86_400_000, bootstrap: str = BOOTSTRAP
) -> None:
    """Create the topic, zstd-compressed and stamped with append time. Raises if it exists."""
    topic = NewTopic(
        name,
        num_partitions=partitions,
        replication_factor=1,
        config={
            "compression.type": "zstd",
            "message.timestamp.type": "LogAppendTime",
            "retention.ms": str(retention_ms),
        },
    )
    admin = AdminClient({"bootstrap.servers": bootstrap, **_CLIENT})
    admin.create_topics([topic], operation_timeout=_TIMEOUT_S)[name].result()


def topics(bootstrap: str = BOOTSTRAP) -> set[str]:
    admin = AdminClient({"bootstrap.servers": bootstrap, **_CLIENT})
    return set(admin.list_topics(timeout=_TIMEOUT_S).topics)


def produce_all(
    topic: str, records: Iterable[tuple[int, bytes | None, bytes]], bootstrap: str = BOOTSTRAP
) -> int:
    """Produce `(partition, key, value)` records in order and wait for every delivery.

    The producer is idempotent, so a retry neither duplicates nor reorders a record within a
    partition. Raises if any delivery failed; returns the number produced.
    """
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            **_CLIENT,
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 50,
        }
    )
    delivered = 0
    errors: list = []

    def on_delivery(err, msg) -> None:
        nonlocal delivered
        if err is None:
            delivered += 1
        else:
            errors.append(err)

    n = 0
    for partition, key, value in records:
        while True:
            try:
                producer.produce(
                    topic, value=value, key=key, partition=partition, on_delivery=on_delivery
                )
                break
            except BufferError:
                producer.poll(0.1)
        n += 1
        if n % _BATCH == 0:
            producer.poll(0)
    queued = producer.flush(_FLUSH_S)
    if errors:
        raise RuntimeError(f"{len(errors)} of {n} deliveries to {topic} failed: {errors[0]}")
    if queued or delivered != n:
        raise RuntimeError(f"{delivered} of {n} records to {topic} were delivered")
    return n


def _consumer(bootstrap: str) -> Consumer:
    # Reads assign partitions at explicit offsets; the group is never joined and commits nothing.
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            **_CLIENT,
            "group.id": "tb-eval-read",
            "auto.offset.reset": "error",
            "enable.auto.commit": False,
        }
    )


def _watermarks(topic: str, bootstrap: str) -> dict[int, tuple[int, int]]:
    c = _consumer(bootstrap)
    try:
        meta = c.list_topics(topic, timeout=_TIMEOUT_S).topics.get(topic)
        if meta is None or meta.error is not None:
            raise ValueError(f"no topic {topic}: {None if meta is None else meta.error}")
        return {
            p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=_TIMEOUT_S)
            for p in sorted(meta.partitions)
        }
    finally:
        c.close()


def end_offsets(topic: str, bootstrap: str = BOOTSTRAP) -> dict[int, int]:
    return {p: high for p, (_, high) in _watermarks(topic, bootstrap).items()}


def low_offsets(topic: str, bootstrap: str = BOOTSTRAP) -> dict[int, int]:
    return {p: low for p, (low, _) in _watermarks(topic, bootstrap).items()}


def read_range(
    topic: str,
    start: dict[int, int],
    end: dict[int, int],
    consumer=None,
    bootstrap: str = BOOTSTRAP,
) -> list[dict]:
    """Every record of `[start, end)` in each partition, parsed as JSON, partition by partition.

    Raises IncompleteRead unless the read is complete: each start at or after the partition's low
    watermark, the offsets contiguous, and as many records as `end - start`. The topics read here
    are written without transactions and are not compacted, so a complete range has no gap.
    """
    own = consumer is None
    c = _consumer(bootstrap) if own else consumer
    try:
        return _read(c, topic, start, end)
    finally:
        if own:
            c.close()


def _read(c, topic: str, start: dict[int, int], end: dict[int, int]) -> list[dict]:
    if set(start) != set(end):
        raise ValueError(f"start and end name different partitions: {sorted(start)}, {sorted(end)}")
    for p in sorted(start):
        if end[p] < start[p]:
            raise IncompleteRead(f"{topic}[{p}]: end {end[p]} is before start {start[p]}")
        low, _ = c.get_watermark_offsets(TopicPartition(topic, p), timeout=_TIMEOUT_S)
        if start[p] < low:
            raise IncompleteRead(f"{topic}[{p}]: start {start[p]} is below the low watermark {low}")
    want = {p: start[p] for p in start if end[p] > start[p]}  # next offset expected
    rows: dict[int, list[dict]] = {p: [] for p in start}
    if want:
        c.assign([TopicPartition(topic, p, o) for p, o in sorted(want.items())])
    idle_since = time.monotonic()
    while want:
        batch = c.consume(num_messages=_BATCH, timeout=1.0)
        if not batch:
            if time.monotonic() - idle_since >= READ_IDLE_S:
                break
            continue
        idle_since = time.monotonic()
        for msg in batch:
            if msg.error() is not None:
                raise IncompleteRead(f"{topic}: {msg.error()}")
            p, offset = msg.partition(), msg.offset()
            if p not in want:
                continue  # done, or past its end
            if offset != want[p]:
                raise IncompleteRead(f"{topic}[{p}]: offset {offset} where {want[p]} was next")
            rows[p].append(orjson.loads(msg.value()))
            want[p] += 1
            if want[p] == end[p]:
                del want[p]
    expected = sum(end[p] - start[p] for p in start)
    got = sum(len(r) for r in rows.values())
    if got != expected:
        raise IncompleteRead(f"{topic}: read {got} records of {expected} in the range")
    return [r for p in sorted(rows) for r in rows[p]]

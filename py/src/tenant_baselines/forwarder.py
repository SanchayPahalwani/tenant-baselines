"""Forward alert events from Kafka to Alertmanager.

The Flink job writes one event per state change. Alertmanager expects a client to
re-send firing alerts until they resolve, and to keep re-sending resolved alerts for a
few minutes. This process holds that state in memory and rebuilds it from the topic
on start.
"""

import argparse
import logging
import time
import uuid
from datetime import UTC, datetime

import httpx
import orjson

log = logging.getLogger("forwarder")

LABEL_FIELDS = ("scope", "region", "tenant", "unit", "signal", "detector", "setting")


def _rfc3339(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_am_alert(event: dict) -> dict:
    """Alertmanager API v2 entry for a firing alert event, without endsAt.

    Raises KeyError, TypeError or ValueError on a missing or malformed field.
    """
    labels = {"alertname": "TenantAnomaly" if event["scope"] == "tenant" else "RegionAnomaly"}
    for name in LABEL_FIELDS:
        if event.get(name):
            labels[name] = event[name]
    who = event.get("tenant") or event.get("region") or event.get("key")
    return {
        "labels": labels,
        "annotations": {
            "summary": f"{event['signal']} for {who} / {event.get('unit') or 'all units'}",
            "value": str(event["value"]),
            "reference": str(event["reference"]),
        },
        "startsAt": _rfc3339(event["starts_at_ms"]),
    }


class ForwarderState:
    def __init__(
        self,
        live_detector: str,
        live_setting: str,
        resend_s: float = 60,
        resolved_keep_s: float = 300,
        stale_s: float = 600,
    ):
        self._detector = live_detector
        self._setting = live_setting
        self._resend_s = resend_s
        self._resolved_keep_s = resolved_keep_s
        self._stale_s = stale_s
        self._alerts: dict[str, dict] = {}

    def apply(self, event: dict, now_s: float) -> None:
        """Apply one alert event. A malformed event is logged and leaves the state as it was."""
        try:
            self._apply(event, now_s)
        except (KeyError, TypeError, ValueError, OverflowError) as e:
            log.warning("skipping bad alert event (%s: %s)", type(e).__name__, e)

    def _apply(self, event: dict, now_s: float) -> None:
        if event["detector"] != self._detector or event["setting"] != self._setting:
            return
        alert_id = event["alert_id"]
        if event["state"] == "firing":
            if event["emitted_at_ms"] - event["event_time_ms"] > self._stale_s * 1000:
                return  # decided during backfill, not a live alert
            if alert_id not in self._alerts:
                entry = to_am_alert(event)
                self._end_same_labels(alert_id, entry, now_s)
                self._alerts[alert_id] = {
                    "entry": entry,
                    "resolved_seen_s": None,
                    "last_sent_s": None,
                }
        elif event["state"] == "resolved":
            a = self._alerts.get(alert_id)
            if a is not None and a["resolved_seen_s"] is None:
                self._end(a, _rfc3339(event["event_time_ms"]), now_s)

    def _end_same_labels(self, new_id: str, entry: dict, now_s: float) -> None:
        """End the firing alerts that have the labels of a new firing alert.

        Alertmanager tells alerts apart by their labels. A firing alert with the same labels
        and another id was left by a job replaced without its state, and no resolved event
        will come for it: end it where the new one starts. It was stored first, so it stays
        ahead of the new one in every batch and Alertmanager ends it before taking the new one.
        """
        for alert_id, a in self._alerts.items():
            if a["resolved_seen_s"] is None and a["entry"]["labels"] == entry["labels"]:
                log.info("ending alert %s: %s fires with the same labels", alert_id, new_id)
                self._end(a, entry["startsAt"], now_s)

    @staticmethod
    def _end(a: dict, ends_at: str, now_s: float) -> None:
        a["entry"] = {**a["entry"], "endsAt": ends_at}
        a["resolved_seen_s"] = now_s
        a["last_sent_s"] = None  # send the resolution right away

    def due_items(self, now_s: float) -> list[tuple[str, dict]]:
        """(alert_id, API entry) pairs that are due; also drops expired resolved alerts."""
        for alert_id in [
            i
            for i, a in self._alerts.items()
            if a["resolved_seen_s"] is not None
            and now_s - a["resolved_seen_s"] > self._resolved_keep_s
        ]:
            del self._alerts[alert_id]
        return [
            (i, a["entry"])
            for i, a in self._alerts.items()
            if a["last_sent_s"] is None or now_s - a["last_sent_s"] >= self._resend_s
        ]

    def due(self, now_s: float) -> list[dict]:
        return [entry for _, entry in self.due_items(now_s)]

    def mark_sent(self, alert_ids: list[str], now_s: float) -> None:
        for alert_id in alert_ids:
            if alert_id in self._alerts:
                self._alerts[alert_id]["last_sent_s"] = now_s


def post_due(
    client: httpx.Client, alertmanager_url: str, state: ForwarderState, now_s: float
) -> int:
    """POST the due alerts; mark them sent on 2xx. Returns the number sent, 0 on any failure."""
    items = state.due_items(now_s)
    if not items:
        return 0
    ids = [i for i, _ in items]
    entries = [e for _, e in items]
    try:
        resp = client.post(f"{alertmanager_url.rstrip('/')}/api/v2/alerts", json=entries)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        log.warning("post to alertmanager failed: %s", e)
        return 0
    state.mark_sent(ids, now_s)
    return len(entries)


def main() -> None:
    from confluent_kafka import Consumer

    ap = argparse.ArgumentParser(description="Forward live.alerts events to Alertmanager")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="live.alerts")
    ap.add_argument("--alertmanager", default="http://alertmanager:9093")
    ap.add_argument("--live-detector", default="seasonal")
    ap.add_argument("--live-setting", default="k4")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    state = ForwarderState(args.live_detector, args.live_setting)
    # A fresh group that reads from the start rebuilds the state from the topic.
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "forwarder-" + str(uuid.uuid4()),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    next_post = time.monotonic()
    with httpx.Client(timeout=5) as client:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                else:
                    try:
                        state.apply(orjson.loads(msg.value()), time.time())
                    except orjson.JSONDecodeError as e:
                        log.warning("skipping alert event that is not JSON: %s", e)
            if time.monotonic() >= next_post:
                sent = post_due(client, args.alertmanager, state, time.time())
                if sent:
                    log.info("sent %d alerts", sent)
                next_post = time.monotonic() + 5


if __name__ == "__main__":
    main()

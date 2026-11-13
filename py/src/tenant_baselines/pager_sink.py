"""A stand-in pager: logs every webhook notification Alertmanager sends it."""

import argparse
import json
import logging
import threading
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tenant_baselines.forwarder import LABEL_FIELDS

log = logging.getLogger("pager_sink")

# The columns of GET /notifications: the forwarder's labels, around the record's own fields.
COLUMNS = ("received_at", "receiver", "status", "alertname", *LABEL_FIELDS, "starts_at", "ends_at")
LATEST_RECORDS = 500


def flatten(record: dict) -> list[dict]:
    """One row per alert in the record's body, with exactly the keys of COLUMNS, all strings."""
    body = record.get("body")
    alerts = body.get("alerts") if isinstance(body, dict) else None
    if not isinstance(alerts, list):
        return []
    rows = []
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        labels = alert.get("labels")
        labels = labels if isinstance(labels, dict) else {}
        row = {"received_at": record.get("received_at"), "receiver": record.get("receiver")}
        row["status"] = alert.get("status")
        row["starts_at"] = alert.get("startsAt")
        row["ends_at"] = alert.get("endsAt")
        row.update({name: labels.get(name) for name in ("alertname", *LABEL_FIELDS)})
        rows.append({c: "" if row[c] is None else str(row[c]) for c in COLUMNS})
    return rows


def _latest_rows(log_path: str) -> bytes:
    try:
        with open(log_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return b""
    out = []
    for line in lines[-LATEST_RECORDS:]:
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict):
            out.extend(json.dumps(row) + "\n" for row in flatten(record))
    return "".join(out).encode("utf-8")


def make_server(
    host: str, port: int, log_path: str, write_lock: AbstractContextManager | None = None
) -> ThreadingHTTPServer:
    """The server; `write_lock` (a new lock by default) orders the log's writes."""
    write_lock = write_lock or threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw)
            except ValueError:
                body = raw.decode("utf-8", "replace")
            line = json.dumps(
                {
                    "received_at": datetime.now(UTC).isoformat(),
                    "receiver": self.path,
                    "body": body,
                }
            )
            with write_lock:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                print(line, flush=True)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            if self.path != "/notifications":
                self.send_error(404)
                return
            # Read without the write lock, so a webhook POST never waits on a dashboard refresh.
            # A line still being written does not parse, and _latest_rows skips it.
            payload = _latest_rows(log_path)
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args) -> None:
            pass

    return ThreadingHTTPServer((host, port), Handler)


def main() -> None:
    ap = argparse.ArgumentParser(description="Log webhook notifications to a file")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--log", default="/data/notifications.jsonl")
    args = ap.parse_args()
    server = make_server("0.0.0.0", args.port, args.log)  # noqa: S104 - reached by service name
    server.serve_forever()


if __name__ == "__main__":
    main()

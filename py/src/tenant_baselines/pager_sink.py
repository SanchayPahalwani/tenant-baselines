"""A stand-in pager: logs every webhook notification Alertmanager sends it."""

import argparse
import json
import logging
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("pager_sink")


def make_server(host: str, port: int, log_path: str) -> ThreadingHTTPServer:
    write_lock = threading.Lock()

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

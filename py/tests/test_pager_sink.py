import json
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager

from tenant_baselines.pager_sink import COLUMNS, flatten, make_server

ALERT = {
    "status": "firing",
    "labels": {
        "alertname": "TenantAnomaly",
        "scope": "tenant",
        "region": "r1",
        "signal": "error_rate",
    },
    "startsAt": "2026-01-01T00:00:00Z",
}


@contextmanager
def running(log_path):
    server = make_server("127.0.0.1", 0, str(log_path))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def post(base, path, data):
    req = urllib.request.Request(base + path, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status


def get(base, path):
    with urllib.request.urlopen(base + path, timeout=5) as resp:
        return resp.status, resp.headers.get("Content-Type"), resp.read()


def alert_body(n=1):
    return json.dumps(
        {"alerts": [{**ALERT, "labels": {**ALERT["labels"], "tenant": f"t{n}"}}]}
    ).encode()


def test_flatten_gives_one_row_per_alert():
    record = {
        "received_at": "2026-01-01T00:00:01+00:00",
        "receiver": "/tenant",
        "body": {
            "alerts": [
                ALERT,
                {
                    "status": "resolved",
                    "labels": {"alertname": "RegionAnomaly"},
                    "startsAt": "2026-01-01T00:00:00Z",
                    "endsAt": "2026-01-01T00:05:00Z",
                },
            ]
        },
    }
    rows = flatten(record)
    assert len(rows) == 2
    assert all(list(r) == list(COLUMNS) for r in rows)
    assert all(isinstance(v, str) for r in rows for v in r.values())
    assert rows[0]["status"] == "firing"
    assert rows[0]["signal"] == "error_rate"
    assert rows[0]["receiver"] == "/tenant"
    assert rows[0]["tenant"] == ""
    assert rows[1]["status"] == "resolved"
    assert rows[1]["ends_at"] == "2026-01-01T00:05:00Z"
    assert rows[1]["scope"] == ""


def test_flatten_ignores_a_body_that_is_not_an_object():
    assert flatten({"received_at": "x", "receiver": "/", "body": "plain text"}) == []
    assert flatten({"received_at": "x", "receiver": "/", "body": {"x": 1}}) == []


def test_post_with_a_non_json_body_is_logged_as_text(tmp_path):
    log = tmp_path / "n.jsonl"
    with running(log) as base:
        assert post(base, "/tenant", b"not json") == 200
    assert json.loads(log.read_text().splitlines()[0])["body"] == "not json"


def test_get_notifications_returns_the_flattened_log(tmp_path):
    with running(tmp_path / "n.jsonl") as base:
        post(base, "/tenant", alert_body(1))
        post(base, "/regional", alert_body(2))
        status, content_type, body = get(base, "/notifications")
    assert status == 200
    assert content_type == "application/x-ndjson"
    rows = [json.loads(line) for line in body.decode().splitlines()]
    assert len(rows) == 2
    assert all(list(r) == list(COLUMNS) for r in rows)
    assert [r["receiver"] for r in rows] == ["/tenant", "/regional"]


def test_get_notifications_without_a_log_is_empty(tmp_path):
    with running(tmp_path / "missing.jsonl") as base:
        status, _, body = get(base, "/notifications")
    assert status == 200
    assert body == b""


def test_get_notifications_keeps_the_last_500_records(tmp_path):
    with running(tmp_path / "n.jsonl") as base:
        for n in range(510):
            post(base, "/tenant", alert_body(n))
        _, _, body = get(base, "/notifications")
    rows = [json.loads(line) for line in body.decode().splitlines()]
    assert len(rows) == 500
    assert rows[0]["tenant"] == "t10"
    assert rows[-1]["tenant"] == "t509"


def test_get_of_another_path_is_404(tmp_path):
    with running(tmp_path / "n.jsonl") as base:
        try:
            get(base, "/other")
        except urllib.error.HTTPError as e:
            assert e.code == 404
        else:
            raise AssertionError("expected 404")


def test_post_is_logged_as_one_json_line(tmp_path):
    log = tmp_path / "n.jsonl"
    server = make_server("127.0.0.1", 0, str(log))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{server.server_address[1]}/tenant",
            data=b'{"x":1}',
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
    finally:
        server.shutdown()
        server.server_close()
    lines = log.read_text().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["receiver"] == "/tenant"
    assert rec["body"] == {"x": 1}
    assert "received_at" in rec

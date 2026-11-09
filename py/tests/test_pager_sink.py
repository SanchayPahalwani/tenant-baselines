import json
import threading
import urllib.request

from tenant_baselines.pager_sink import make_server


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

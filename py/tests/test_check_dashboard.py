import json

import httpx

from tenant_baselines import check_dashboard as cd

FILE = json.loads(cd.DASHBOARD_PATH.read_text())
TITLES = sorted({p["title"] for p in FILE["panels"]})


class Resp:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


def frames(rows):
    values = [["a"] * rows] if rows else [[]]
    return {"results": {"A": {"status": 200, "frames": [{"data": {"values": values}}]}}}


def served(titles):
    return {"dashboard": {"panels": [{"title": t} for t in titles]}}


def fakes(titles=TITLES, rows=1, fail_on=None, fail_body=None):
    def get(url, **_):
        assert url.endswith("/api/dashboards/uid/tenant-baselines")
        return Resp(200, served(titles))

    def post(url, json=None, **_):
        assert url.endswith("/api/ds/query")
        sql = json["queries"][0]["rawSql"]
        assert "${" not in sql
        if fail_on and fail_on in sql:
            return Resp(400, fail_body)
        return Resp(200, frames(rows))

    return get, post


def test_substitute():
    assert cd.substitute("a '${key}' ${signal}", {"key": "k1", "signal": "error_rate"}) == (
        "a 'k1' error_rate"
    )


def test_check_passes():
    get, post = fakes()
    assert cd.check(get=get, post=post) == []


def test_check_reports_a_missing_panel():
    get, post = fakes(titles=[t for t in TITLES if t != "Unit status (live)"])
    problems = cd.check(get=get, post=post)
    assert len(problems) == 1
    assert "Unit status (live)" in problems[0]


def test_check_reports_a_query_error():
    get, post = fakes(fail_on="pager-sink", fail_body={"message": "bad", "error": "Code: 62"})
    problems = cd.check(get=get, post=post)
    assert len(problems) == 1
    assert "Notifications (latest 100, from pager-sink)" in problems[0]
    assert "Code: 62" in problems[0]


def test_check_allows_an_empty_panel_unless_rows_are_required():
    get, post = fakes(rows=0)
    assert cd.check(get=get, post=post) == []
    problems = cd.check(get=get, post=post, require_rows=True)
    assert problems
    assert any("no rows" in p and "Unit status (live)" in p for p in problems)


def test_check_reports_a_dashboard_answer_that_is_not_200():
    _, post = fakes()
    problems = cd.check(get=lambda url, **_: Resp(500, {"message": "boom"}), post=post)
    assert problems == ["Grafana answered HTTP 500 for the dashboard"]


def test_check_reports_grafana_that_is_not_reachable():
    def refused(url, **_):
        raise httpx.ConnectError("[Errno 61] Connection refused")

    _, post = fakes()
    problems = cd.check(get=refused, post=post)
    assert problems == ["Grafana is not reachable ([Errno 61] Connection refused)"]

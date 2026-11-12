"""The provisioned dashboard file (no stack needed)."""

import json
import re
from pathlib import Path

from tenant_baselines.pager_sink import COLUMNS

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = json.loads((ROOT / "infra/grafana/dashboards/tenant-baselines.json").read_text())
PANELS = {p["id"]: p for p in DASHBOARD["panels"]}


def raw_sqls():
    return [t["rawSql"] for p in DASHBOARD["panels"] for t in p.get("targets", []) if "rawSql" in t]


def test_panels():
    assert [p["id"] for p in DASHBOARD["panels"]] == [1, 2, 3, 4, 5]
    assert [p["type"] for p in DASHBOARD["panels"]] == [
        "timeseries",
        "table",
        "status-history",
        "table",
        "table",
    ]
    assert PANELS[3]["title"] == "Score by key: $region"
    assert PANELS[4]["title"] == "Notifications (latest 100, from pager-sink)"
    assert PANELS[5]["title"] == "Unit status (live)"
    assert PANELS[3]["repeat"] == "region"


def test_every_query_reads_the_live_namespace_deduplicated():
    named = [s for s in raw_sqls() if "tb." in s]
    assert len(named) == 4
    for sql in named:
        assert "ns = 'live'" in sql or "ns='live'" in sql
        assert "FINAL" in sql


def test_notifications_structure_is_the_sinks_columns():
    sql = PANELS[4]["targets"][0]["rawSql"]
    assert "http://pager-sink:8099/notifications" in sql
    assert ", ".join(f"{c} String" for c in COLUMNS) in sql
    compose = (ROOT / "docker-compose.yml").read_text()
    assert '"tenant_baselines.pager_sink", "--port", "8099"' in compose


def test_every_description_says_synthetic():
    for d in (DASHBOARD, PANELS[1], PANELS[3], PANELS[4], PANELS[5]):
        assert "ynthetic" in d["description"]
    for title in ("Score by key", "Notifications", "Unit status"):
        assert title in DASHBOARD["description"]


def test_variables():
    names = [v["name"] for v in DASHBOARD["templating"]["list"]]
    assert names == ["key", "signal", "region"]
    used = {m for s in raw_sqls() for m in re.findall(r"\$\{(\w+)\}", s)}
    assert used <= set(names)
    region = DASHBOARD["templating"]["list"][2]
    assert region["type"] == "query"
    assert region["multi"] and region["includeAll"] and region["hide"] == 2
    # All regions are selected when the dashboard opens, so panel 3 repeats once per region;
    # an empty `current` would let Grafana pick only the first one.
    assert region["current"] == {"text": ["All"], "value": ["$__all"]}
    assert (
        "SELECT DISTINCT region FROM tb.scores WHERE ns = 'live' ORDER BY region" in region["query"]
    )


def test_cold_key_lower_line_is_null():
    assert "isNull(median)" in PANELS[1]["targets"][0]["rawSql"]
    assert "Units: error_rate is a fraction of requests" in PANELS[1]["description"]


def test_not_editable():
    assert DASHBOARD["editable"] is False
    assert DASHBOARD["uid"] == "tenant-baselines"

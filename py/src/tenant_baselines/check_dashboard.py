"""Check that Grafana serves the dashboard's panels and that each panel's query runs.

Synthetic data only. Run by smoke check 13 against the demo stack: it asks Grafana for the
provisioned dashboard, then sends each panel's SQL through Grafana's own data source proxy.
"""

import argparse
import json
import sys
from pathlib import Path

import httpx

from tenant_baselines.freeze import REPO

DASHBOARD_PATH = REPO / "infra" / "grafana" / "dashboards" / "tenant-baselines.json"
UID = "tenant-baselines"
EMPTY_NOTE = (
    "empty panels are allowed; the notifications and unit-status panels fill after the first "
    "page and the first drain"
)


def substitute(sql: str, values: dict[str, str]) -> str:
    for name, value in values.items():
        sql = sql.replace("${" + name + "}", value)
    return sql


def _targets(panel: dict) -> list[dict]:
    return [t for t in panel.get("targets", []) if "rawSql" in t]


def _query(post, grafana: str, target: dict, sql: str):
    body = {
        "from": "now-3h",
        "to": "now",
        "queries": [{**target, "rawSql": sql, "intervalMs": 60000, "maxDataPoints": 500}],
    }
    return post(f"{grafana}/api/ds/query", json=body, timeout=30)


def _error(resp) -> str | None:
    """The error of a data source response, or None when it answered 200 without one."""
    if resp.status_code != 200:
        try:
            detail = resp.json()
            detail = detail.get("error") or detail.get("message") or resp.text
        except ValueError:
            detail = resp.text
        return f"HTTP {resp.status_code}: {detail}"
    for result in resp.json().get("results", {}).values():
        if result.get("error"):
            return str(result["error"])
    return None


def _row_count(resp) -> int:
    count = 0
    for result in resp.json().get("results", {}).values():
        for frame in result.get("frames", []):
            values = frame.get("data", {}).get("values", [])
            count += max((len(column) for column in values), default=0)
    return count


def _first_value(post, grafana: str, variable: dict) -> str | None:
    target = {
        "refId": "A",
        "datasource": variable["datasource"],
        "editorType": "sql",
        "queryType": "table",
        "format": 1,
    }
    resp = _query(post, grafana, target, variable["query"])
    if _error(resp):
        return None
    for result in resp.json().get("results", {}).values():
        for frame in result.get("frames", []):
            for column in frame.get("data", {}).get("values", []):
                if column:
                    return str(column[0])
    return None


def check(
    grafana: str = "http://localhost:3000",
    path: Path = DASHBOARD_PATH,
    require_rows: bool = False,
    get=httpx.get,
    post=httpx.post,
) -> list[str]:
    """The problems found; empty when all is well. Without the served dashboard, that is the
    only problem: the panel checks need it."""
    dashboard = json.loads(Path(path).read_text())
    problems = []
    try:
        resp = get(f"{grafana}/api/dashboards/uid/{UID}", timeout=30)
    except httpx.HTTPError as e:
        return [f"Grafana is not reachable ({e})"]
    if resp.status_code != 200:
        return [f"Grafana answered HTTP {resp.status_code} for the dashboard"]
    served = {p["title"] for p in resp.json().get("dashboard", {}).get("panels", [])}
    for title in sorted({p["title"] for p in dashboard["panels"]} - served):
        problems.append(f"panel '{title}' is not served by Grafana")

    values = {"signal": "error_rate"}
    for variable in dashboard["templating"]["list"]:
        if variable["name"] in ("key", "region"):
            first = _first_value(post, grafana, variable)
            if first is None and require_rows:
                problems.append(f"variable '{variable['name']}' has no value")
            values[variable["name"]] = first or ""
    for panel in dashboard["panels"]:
        for target in _targets(panel):
            resp = _query(post, grafana, target, substitute(target["rawSql"], values))
            error = _error(resp)
            if error:
                problems.append(f"panel '{panel['title']}': {error}")
            elif require_rows and _row_count(resp) == 0:
                problems.append(f"panel '{panel['title']}': no rows")
    return problems


def main() -> None:
    ap = argparse.ArgumentParser(description="Check the dashboard's panels against Grafana")
    ap.add_argument("--grafana", default="http://localhost:3000")
    ap.add_argument("--require-rows", action="store_true")
    args = ap.parse_args()
    problems = check(grafana=args.grafana, require_rows=args.require_rows)
    if problems:
        for p in problems:
            print(p)
        sys.exit(1)
    n = len(json.loads(DASHBOARD_PATH.read_text())["panels"])
    print(f"dashboard OK: {n} panels")
    if not args.require_rows:
        print(EMPTY_NOTE)


if __name__ == "__main__":
    main()

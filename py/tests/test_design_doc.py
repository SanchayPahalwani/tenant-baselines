"""`docs/design.md`: its sections, its diagram, and where its figures come from.

Synthetic data only. The design doc's measured figures sit in three blocks that must equal what
the report scripts print, and the few figures in its prose sit on lines tagged with their source
file, whose every number must appear in that file. Any other line that carries a decimal number
or a number of four or more digits must cite the spec or say the value is configured or an
estimate.
"""

import json
import re

from test_docs import headline_rows

from tenant_baselines import bench_report
from tenant_baselines.freeze import REPO

DOC = REPO / "docs" / "design.md"
DOCS = REPO / "docs"
PERFORMANCE = DOCS / "performance"

TITLES = [
    "What this is",
    "Architecture",
    "Events, watermarks and minute aggregates",
    "The seasonal baseline",
    "Cold start",
    "Fire, resolve and the static opponents",
    "Regional grouping and unit status",
    "Sinks, storage and delivery",
    "The eval",
    "Performance",
    "What a much larger deployment would change",
    "What the repo does not do",
    "Where the build departs from the spec",
]
SPEC_SECTIONS = [*(f"4.{i}" for i in range(1, 11)), *(f"5.{i}" for i in range(1, 8)), "6"]
TAGS = {
    "([results](results.md))": "results.md",
    "([performance](performance.md))": "performance.md",
}
PLACEHOLDERS = ("TODO", "TBD", "PENDING", "XXX")
# The rendered blocks, in the doc's order; each must equal what a report script prints.
BLOCKS = ("headline", "performance", "extrapolation")
BLOCK_BEGIN = re.compile(r"<!-- (\w+):begin -->")

TOKEN = re.compile(r"\d+(?:,\d{3})*(?:\.\d+)?(?:/\d+)?")
# Removed, in this order, before a line is checked: none of these can be a measured figure.
NOT_FIGURES = (
    re.compile(r"^#+ \d+(\.\d+)*\.? "),  # a heading's section number
    re.compile(r"\d{4}-\d{2}-\d{2}"),  # dates
    re.compile(r"FLINK-39728"),
    re.compile(r"\d+\.\d+\.\d+-\d+\.\d+"),  # versions such as 5.0.0-2.2
    re.compile(r"\d+\.\d+\.\d+"),  # versions such as 5.1.0
    re.compile(r"p\d+(?:\.\d+)?"),  # setting names such as p99.9, and p50 or p95
    re.compile(r"\d+(?:\.\d+)?:1"),  # ratios such as 1.5:1
)
EXCUSED = re.compile(r"\bspec \d|\bconfigured\b|\bestimate")
SPEC_REF = re.compile(r"\bspec \d+(?:\.\d+)?")
SENTENCE_END = re.compile(r"(?<=[.;])\s+")
CODE_SPAN = re.compile(r"`[^`]*`")


def text() -> str:
    return DOC.read_text(encoding="utf-8")


def tokens(line: str) -> list[str]:
    return TOKEN.findall(line)


def block(name: str, doc: str | None = None) -> str:
    doc = text() if doc is None else doc
    begin, end = f"<!-- {name}:begin -->", f"<!-- {name}:end -->"
    assert doc.count(begin) == 1 and doc.count(end) == 1, name
    return doc[doc.index(begin) + len(begin) : doc.index(end)].strip()


def block_names(doc: str) -> list[str]:
    """The names of the doc's `<!-- name:begin -->` lines, in order."""
    return [m.group(1) for line in doc.splitlines() if (m := BLOCK_BEGIN.fullmatch(line.strip()))]


def mermaid() -> str:
    fences = re.findall(r"```mermaid\n(.*?)```", text(), re.S)
    assert len(fences) == 1
    return fences[0]


def tag_of(line: str) -> str | None:
    stripped = line.rstrip()
    return next((tag for tag in TAGS if stripped.endswith(tag)), None)


def prose_lines() -> list[str]:
    """The doc's lines outside the three blocks and the mermaid fence."""
    out, skipping = [], None
    for line in text().splitlines():
        if skipping:
            if line.strip() == skipping:
                skipping = None
            continue
        begin = BLOCK_BEGIN.fullmatch(line.strip())
        if begin:
            skipping = f"<!-- {begin.group(1)}:end -->"
            continue
        if line.startswith("```mermaid"):
            skipping = "```"
            continue
        out.append(line)
    return out


def missing_tokens(body: str, source: str) -> list[str]:
    """The number tokens of `body` that `source` does not hold as a whole number: "69.4" is not
    found inside "269.4", nor "6" inside "1.6" or "46/50"."""
    return [
        t
        for t in tokens(body)
        if not re.search(r"(?<![\d.,/])" + re.escape(t) + r"(?![\d/]|[.,]\d)", source)
    ]


def measured_looking(line: str) -> bool:
    """Whether a sentence of the line carries a number that could be a measured figure and is
    not excused. Each sentence is judged alone, so a spec citation excuses only its own."""
    line = NOT_FIGURES[0].sub("", line)
    return any(_sentence_measured_looking(s) for s in SENTENCE_END.split(line))


def _sentence_measured_looking(sentence: str) -> bool:
    for pattern in NOT_FIGURES[1:]:
        sentence = pattern.sub("", sentence)
    if EXCUSED.search(sentence):
        return False
    for token in tokens(sentence):
        for part in token.split("/"):
            if "." in part or len(part.replace(",", "")) >= 4:
                return True
    return False


def test_sections_in_order():
    headings = [line[3:] for line in text().splitlines() if line.startswith("## ")]
    assert headings == [f"{n}. {title}" for n, title in enumerate(TITLES, start=1)]


def test_every_spec_subsection_is_cited():
    doc = text()
    missing = [n for n in SPEC_SECTIONS if not re.search(rf"spec {re.escape(n)}(?!\d|\.\d)", doc)]
    assert missing == []


def _create_topics() -> tuple[list[str], set[str]]:
    script = (REPO / "infra" / "kafka" / "create-topics.sh").read_text()
    namespaces = re.search(r"^for ns in ([a-z ]+); do$", script, re.M).group(1).split()
    suffixes = set(re.findall(r'create "\$ns\.([a-z-]+)"', script))
    return namespaces, suffixes


def _compose_services() -> set[str]:
    compose = (REPO / "docker-compose.yml").read_text()
    services = compose[compose.index("\nservices:\n") : compose.index("\nnetworks:\n")]
    return set(re.findall(r"^  ([a-z][a-z0-9-]*):\s*$", services, re.M))


def test_the_diagram():
    fence = mermaid()
    assert fence.startswith("flowchart LR\n")
    labels = re.findall(r'"([^"]*)"', fence)
    assert labels
    assert [label for label in labels if re.search(r"[<>|]", label)] == []
    namespaces, suffixes = _create_topics()
    assert "live" in namespaces
    for topic in ("live.events", "live.alerts"):
        assert topic in fence
        assert topic.split(".", 1)[1] in suffixes
    for suffix in ("aggregates", "scores", "alerts", "unit-status"):
        assert suffix in suffixes
        assert suffix in fence
    services = _compose_services()
    lowered = " ".join(labels).lower()
    for service in ("forwarder", "pager-sink", "alertmanager", "clickhouse", "grafana"):
        assert service in services, service
        assert service in lowered, service
    main = REPO / "flink-job" / "src" / "main"
    for cls in ("MinuteAggregator", "Scorer", "PopulationBaselines", "RegionGrouper"):
        assert list(main.rglob(f"{cls}.java")), cls
        assert cls in fence, cls
    assert "agg --> grp" in fence
    assert 'pg -. "notifications panel" .-> ch' in fence


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def test_the_only_blocks_are_the_three_the_scripts_print():
    """The prose checks skip every block, and only these three are compared with a script."""
    assert block_names(text()) == list(BLOCKS)
    planted = text().replace(
        "## 10. Performance", "<!-- note:begin -->\n9.99\n<!-- note:end -->\n\n## 10. Performance"
    )
    assert block_names(planted) == ["headline", "note", "performance", "extrapolation"]


def test_the_headline_block_is_the_results():
    results = (DOCS / "results.md").read_text()
    lines = block("headline").splitlines()
    source = next(
        line
        for line in results[results.index("## Headline") :].splitlines()
        if line.startswith("| Family")
    )
    assert _cells(lines[0]) == [_cells(source)[i] for i in (0, 2, 5)]
    rows = [tuple(_cells(line)) for line in lines if line.startswith("| ") and line != lines[0]]
    assert len(rows) == 3
    assert rows == headline_rows(results)


def _performance_files() -> dict[str, dict]:
    names = ("throughput", "delay", "state")
    return {n: json.loads((PERFORMANCE / f"{n}.json").read_text()) for n in names}


def test_the_performance_block_is_the_scripts():
    files = _performance_files()
    assert block("performance") == bench_report.readme_block(
        files["throughput"], files["delay"], files["state"]
    )


def test_the_extrapolation_block_is_the_scripts():
    state = _performance_files()["state"]
    assert block("extrapolation") == bench_report.extrapolation_table(state)
    above = text()[: text().index("<!-- extrapolation:begin -->")]
    headings = [line for line in above.splitlines() if line.startswith("#")]
    assert headings[-1] == "### Extrapolation, not measured"


def test_tokens():
    assert tokens("410, 9.63, 45/50, 24,338,688, p95") == [
        "410",
        "9.63",
        "45/50",
        "24,338,688",
        "95",
    ]


def test_tagged_lines_carry_only_their_sources_figures():
    tagged = 0
    for line in prose_lines():
        tag = tag_of(line)
        if tag is None:
            continue
        tagged += 1
        source = (DOCS / TAGS[tag]).read_text(encoding="utf-8")
        body = SPEC_REF.sub("", line.rstrip()[: -len(tag)])
        missing = missing_tokens(body, source)
        assert missing == [], f"{TAGS[tag]} lacks {missing}: {line}"
    assert tagged >= 3


def test_a_tagged_figure_must_stand_alone_in_its_source():
    assert missing_tokens("69.4", "a minimum of 269.4 s") == ["69.4"]
    assert missing_tokens("6", "a 1.6 share and 46/50") == ["6"]
    assert missing_tokens("46/50", "146/500") == ["46/50"]
    assert missing_tokens("1,300", "1,300,000") == ["1,300"]
    assert missing_tokens("9.63 and 46/50 and k6", "k6: 9.63. Then 46/50 (0.81).") == []
    assert missing_tokens("35 of 60", "35 of 60 matched") == []


def test_every_tag_ends_its_line():
    """A tag that does not end its line, a full stop after it say, would switch off the check."""
    misplaced = [
        line
        for line in prose_lines()
        if any(tag in CODE_SPAN.sub("", line) for tag in TAGS) and tag_of(line) is None
    ]
    assert misplaced == []


def test_untagged_lines_carry_no_measured_looking_number():
    offending = [line for line in prose_lines() if tag_of(line) is None and measured_looking(line)]
    assert offending == []


def test_the_number_rule_on_literal_lines():
    assert not measured_looking("### 5.1 Input")
    assert measured_looking("### 5.1 Recall was 9.63")
    assert measured_looking("a specific 9.63")
    assert not measured_looking("p99.9 at 1.5:1 on connector 5.0.0-2.2")
    assert not measured_looking("1.4826 × MAD (spec 4.3)")
    assert measured_looking("Recall was 9.63. See spec 4.3.")
    assert not measured_looking("Recall is about 2.3 (spec 4.3); the bound is 20 s.")


def test_the_world_is_48_keys():
    assert "48 keys" in text()
    assert "about 60" in text()


def test_required_statements():
    doc = text()
    for phrase in (
        "Why a single OLAP store",
        "Design reasoning, not measured",
        "Key partitioning",
        "Hot tenants",
        "Older data on cheaper storage",
        "Flink under Kubernetes",
    ):
        assert phrase in doc, phrase
    assert "not evidence about any real" in "\n".join(doc.splitlines()[:12])


def test_relative_links_resolve():
    doc = text()
    links = re.findall(r"\]\(([^)]+)\)", doc)
    assert links
    missing = [
        link
        for link in links
        if not link.startswith(("http", "#")) and not (DOCS / link.split("#")[0]).exists()
    ]
    assert missing == []
    assert ".superpowers" not in doc


def test_no_placeholder_is_left():
    doc = text()
    assert [word for word in PLACEHOLDERS if word in doc] == []

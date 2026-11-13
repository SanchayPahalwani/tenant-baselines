"""The README's eval figures come from the committed records, through the report script.

Synthetic data only. The README's measured figures (the headline table, the sweep and
false-page figures, the wall time, the machine and the check-paths commit) are rebuilt here from
the summaries, the registry, the freeze file, `machine.json` and `check-paths.json` (spec 2: no
number that no script produces), and the README's table is compared with `docs/results.md`.
The design figures it also states (the seed count, the amplitudes, the replay length) are the
freeze file's and the spec's, and are not rebuilt here.
"""

import json
import re
import shutil
import subprocess

import pytest

from tenant_baselines import bench_report, eval_report
from tenant_baselines.freeze import REPO, planned

README = (REPO / "README.md").read_text()
RESULTS = REPO / "docs" / "results"


def eval_section() -> str:
    return README[README.index("### Detection eval") : README.index("### Performance")]


@pytest.fixture(scope="module")
def records():
    summaries = eval_report.load_summaries(RESULTS / "summaries")
    registry = eval_report.load_registry(RESULTS / "official-runs.jsonl")
    freeze = json.loads((REPO / "eval" / "freeze.json").read_text())
    eval_report.check_official(summaries, registry, freeze)
    return summaries, registry, freeze


@pytest.fixture(scope="module")
def check_paths():
    return json.loads((RESULTS / "check-paths.json").read_text())


@pytest.fixture(scope="module")
def machine():
    return json.loads((RESULTS / "machine.json").read_text())


@pytest.fixture(scope="module")
def rendered(records, check_paths, machine):
    """docs/results.md as `make results` renders it from the committed records."""
    summaries, registry, freeze = records
    first = eval_report.first_run_entry(
        registry, RESULTS / "summaries", RESULTS / "superseded", freeze
    )
    run_log = eval_report.load_run_log(RESULTS / "run-log.jsonl")
    return eval_report.render(
        summaries, run_log, check_paths, freeze, machine, registry=registry, first_entry=first
    )


def headline_rows(text: str) -> list[tuple[str, str, str]]:
    """(family, pooled expected-signal recall, pooled false pages per day) of the first table
    after the 6:1 headline heading."""
    section = text[text.index("## Headline") :]
    rows = []
    for line in section.splitlines():
        if not line.startswith("| ") and rows:
            break
        if line.startswith("| ") and not line.startswith("| Family"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            rows.append((cells[0], cells[2], cells[5]))
    return rows


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def headline_header(text: str) -> list[str]:
    """The header cells of the first table after the 6:1 headline heading."""
    section = text[text.index("## Headline") :]
    return _cells(next(line for line in section.splitlines() if line.startswith("| Family")))


def readme_rows() -> list[tuple[str, str, str]]:
    rows = [line for line in eval_section().splitlines() if line.startswith("| ")]
    cells = [[c.strip() for c in r.strip("|").split("|")] for r in rows]
    return [tuple(c) for c in cells if c[0] != "Family"]


def test_the_readme_table_is_the_headline_table_of_results_md(rendered):
    readme = readme_rows()
    assert len(readme) == 3
    assert readme == headline_rows((REPO / "docs" / "results.md").read_text())
    assert readme == headline_rows(rendered)


def test_the_readme_table_header_is_the_header_of_results_md(rendered):
    header = next(line for line in eval_section().splitlines() if line.startswith("| Family"))
    for source in ((REPO / "docs" / "results.md").read_text(), rendered):
        assert _cells(header) == [headline_header(source)[i] for i in (0, 2, 5)]


def test_the_readme_sentences_carry_the_scripts_figures(records, machine, check_paths):
    summaries, registry, freeze = records
    text = eval_section()
    main = eval_report.headline(summaries, eval_report.MAIN_AMPLITUDE)
    weak = eval_report.headline(summaries, 1.5)
    s, p, g = (main[f] for f in ("seasonal", "per_key_static", "global_static"))
    seeds = len(freeze["official_seeds"])
    span = eval_report.run_span(summaries, registry)
    want = [
        f"pooled over the {seeds} seeds",
        "Pooled over the seeds it had fewer false pages per day than both"
        f" ({s['pooled_false_per_day']:.2f} against {p['pooled_false_per_day']:.2f} and"
        f" {g['pooled_false_per_day']:.2f}).",
        f"Per seed, the global static range ({g['false_per_day']['min']:.2f}–"
        f"{g['false_per_day']['max']:.2f}) overlaps the seasonal range"
        f" ({s['false_per_day']['min']:.2f}–{s['false_per_day']['max']:.2f}), so the gap to"
        f" global static is not settled with {seeds} seeds.",
        "At 1.5:1 the seasonal detector found every scenario"
        f" ({weak['seasonal']['detected_expected']['k']}/"
        f"{weak['seasonal']['detected_expected']['n']}), but the global static detector had"
        f" fewer false pages per day ({weak['global_static']['pooled_false_per_day']:.2f}"
        f" against {weak['seasonal']['pooled_false_per_day']:.2f}, pooled).",
        eval_report.false_pages_sentence(summaries),
        f"`make eval` ran the {len(planned(freeze))} official runs. Each pair runs once, so it"
        " now only renders `docs/results.md`.",
        f"The runs took {span['minutes']:.1f} minutes on this machine (an {machine['cpu']} laptop)",
        f"At commit `{check_paths['commit'][:7]}`, with a jar built from the same source as the"
        " frozen one",
        f"unchanged since {freeze['commit'][:7]})",
        f"The check took {check_paths['seconds']['total'] / 60:.1f} minutes on this machine.",
    ]
    assert eval_report.timing_problem(check_paths) is None
    missing = [w for w in want if w not in text]
    assert not missing, missing
    assert "paged less than both" not in README


def test_the_readme_says_which_seasonal_setting_was_selected_and_which_one_runs_live(records):
    """The headline's seasonal row is the setting every seed selected; the demo and the delay
    benchmark run another one, and the README says so with that setting's false pages."""
    summaries, _, _ = records
    main = eval_report.MAIN_AMPLITUDE
    seasonal = eval_report.headline(summaries, main)["seasonal"]
    selected = {x["setting"] for x in seasonal["seeds"]}
    assert len(selected) == 1, selected
    (chosen,) = selected
    compose = (REPO / "docker-compose.yml").read_text()
    live = re.search(r"- --live-setting\n\s*- (\w+)\n", compose).group(1)
    bench = json.loads((PERFORMANCE / "delay.json").read_text())["headline"]
    assert (bench["detector"], bench["setting"]) == ("seasonal", live)
    assert live != chosen
    row = next(
        r
        for r in eval_report.curve(summaries, main)
        if (r["family"], r["setting"]) == ("seasonal", live)
    )
    assert row["false_per_day"] > seasonal["pooled_false_per_day"]
    sentence = (
        f"Every official seed selected {chosen} for the seasonal detector; the demo and the delay"
        f" benchmark run {live}, which had {row['false_per_day']:.2f} false pages per day on the"
        " same week."
    )
    assert sentence in eval_section()


def test_the_readme_claims_hold_for_the_figures(records, check_paths):
    summaries, _, _ = records
    main = eval_report.headline(summaries, eval_report.MAIN_AMPLITUDE)
    weak = eval_report.headline(summaries, 1.5)
    s, p, g = (main[f] for f in ("seasonal", "per_key_static", "global_static"))
    # found more scenarios than either static detector
    assert s["detected_expected"]["k"] > max(
        p["detected_expected"]["k"], g["detected_expected"]["k"]
    )
    # fewer false pages than both, pooled; the per-seed ranges overlap with global static
    assert s["pooled_false_per_day"] < min(p["pooled_false_per_day"], g["pooled_false_per_day"])
    sr, gr = s["false_per_day"], g["false_per_day"]
    assert gr["min"] <= sr["max"] and sr["min"] <= gr["max"]
    # the recall intervals of seasonal and per key static overlap
    assert s["detected_expected"]["lo"] <= p["detected_expected"]["hi"]
    # at 1.5:1: every scenario found; global static has fewer false pages, pooled
    e = weak["seasonal"]["detected_expected"]
    assert e["k"] == e["n"]
    assert weak["global_static"]["pooled_false_per_day"] < weak["seasonal"]["pooled_false_per_day"]
    # most pages are false in every family
    assert eval_report.false_pages_sentence(summaries).startswith("Most pages are false")
    # both paths identical at the recorded commit
    assert eval_report.check_passed(check_paths)[0]


def test_the_render_states_the_readme_figures(rendered, records, machine):
    summaries, registry, _ = records
    span = eval_report.run_span(summaries, registry)
    assert f"elapsed {span['minutes']:.1f} minutes" in rendered
    assert f"- Machine: {machine['cpu']}," in rendered
    assert eval_report.false_pages_sentence(summaries) in rendered
    weak = eval_report.headline(summaries, 1.5)
    for fam in ("seasonal", "global_static"):
        assert f"pooled {weak[fam]['pooled_false_per_day']:.2f}" in rendered
    e = weak["seasonal"]["detected_expected"]
    assert f"{e['k']}/{e['n']}" in rendered


@pytest.mark.parametrize("name,version", eval_report._FIXED_VERSIONS)
def test_each_fixed_version_is_the_one_the_build_uses(name, version):
    pinned = (REPO / "flink-job" / "pom.xml").read_text() + (
        REPO / "docker-compose.yml"
    ).read_text()
    number = version.split(" ")[0]
    assert re.search(rf"[>:]{re.escape(number)}[<.\-\s]", pinned), (name, number)
    java = re.search(r"\(Java (\d+)\)", version)
    if java:
        assert f"<maven.compiler.release>{java[1]}</maven.compiler.release>" in pinned


def test_the_jar_sources_are_unchanged_since_the_freeze_commit_the_readme_names():
    named = re.search(
        r"\(`flink-job/pom.xml`, `flink-job/src/main` and `shared/` are unchanged since"
        r" ([0-9a-f]{7,40})\)",
        README,
    )
    assert named, "the README no longer states which jar sources are unchanged"
    freeze = json.loads((REPO / "eval" / "freeze.json").read_text())
    assert freeze["commit"].startswith(named.group(1))
    # The CI container copies the tree without .git, and a shallow clone lacks the freeze commit.
    if (
        shutil.which("git") is None
        or subprocess.run(
            ["git", "cat-file", "-e", f"{named.group(1)}^{{commit}}"], cwd=REPO, capture_output=True
        ).returncode
    ):
        pytest.skip("needs the git history back to the freeze commit")
    diff = subprocess.run(
        ["git", "diff", "--quiet", named.group(1), "HEAD", "--"]
        + ["flink-job/pom.xml", "flink-job/src/main", "shared"],
        cwd=REPO,
    )
    assert diff.returncode == 0


PERFORMANCE = REPO / "docs" / "performance"
DESIGN = (REPO / "docs" / "design.md").read_text()
ALLOWED = {"8", "10", "10.231.0.0/24", "127.0.0.1", "Apache-2.0"}
FENCE = re.compile(r"```mermaid\n.*?```", re.S)
LINK = re.compile(r"\[[^\]]*\]\([^)]*\)")


def test_the_readme_performance_block_is_the_scripts():
    begin, end = "<!-- performance:begin -->", "<!-- performance:end -->"
    block = README[README.index(begin) + len(begin) : README.index(end)].strip()
    data = [
        json.loads((PERFORMANCE / f"{n}.json").read_text())
        for n in ("throughput", "delay", "state")
    ]
    assert block == bench_report.readme_block(*data).strip()


def test_the_readme_diagram_is_the_design_docs():
    readme, design = FENCE.findall(README), FENCE.findall(DESIGN)
    assert len(readme) == 1 and len(design) == 1
    assert readme == design


def test_readme_sections_in_order():
    headings = re.findall(r"^## (.+)$", README, re.M)
    assert headings == [
        "Architecture",
        "Quick start",
        "Results",
        "Design",
        "What this repo does not do",
    ]


def test_no_number_outside_the_results_section_but_the_allowed_ones():
    outside = README[: README.index("## Results")] + README[README.index("## Design") :]
    outside = LINK.sub(" ", FENCE.sub(" ", outside))
    bad = []
    for line in outside.splitlines():
        for token in line.split():
            token = token.strip("`").strip(".,;:()").strip("`")
            if any(c.isdigit() for c in token) and token not in ALLOWED:
                bad.append((token, line))
    assert not bad, bad


def test_readme_states_its_limits():
    for phrase in (
        "Synthetic data only",
        "production use",
        "not evidence about any real",
        "docs/design.md",
        "docs/performance.md",
        "docs/results.md",
    ):
        assert phrase in README, phrase
    assert "Work in progress" not in README
    assert "milestone 5 of 6" not in README


def test_readme_links_resolve():
    for target in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", README):
        if "://" in target:
            continue
        assert (REPO / target).exists(), target

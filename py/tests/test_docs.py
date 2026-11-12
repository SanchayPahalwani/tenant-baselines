"""The README's eval figures come from the committed records, through the report script.

Synthetic data only. Every number in the README's Eval section is rebuilt here from the
summaries, the registry, the freeze file, `machine.json` and `check-paths.json` (spec 2: no
number that no script produces), and the README's table is compared with `docs/results.md`.
"""

import json
import re

import pytest

from tenant_baselines import eval_report
from tenant_baselines.freeze import REPO, planned

README = (REPO / "README.md").read_text()
RESULTS = REPO / "docs" / "results"


def eval_section() -> str:
    return README[README.index("## Eval") : README.index("## Design")]


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


def readme_rows() -> list[tuple[str, str, str]]:
    rows = [line for line in eval_section().splitlines() if line.startswith("| ")]
    cells = [[c.strip() for c in r.strip("|").split("|")] for r in rows]
    return [tuple(c) for c in cells if c[0] != "Family"]


def test_the_readme_table_is_the_headline_table_of_results_md(rendered):
    readme = readme_rows()
    assert len(readme) == 3
    assert readme == headline_rows((REPO / "docs" / "results.md").read_text())
    assert readme == headline_rows(rendered)


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
    ]
    missing = [w for w in want if w not in text]
    assert not missing, missing
    assert "paged less than both" not in README


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

"""`make ci` must run the commands of the CI workflow, in the workflow's order."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

WORKFLOW_COMMANDS = [
    "mvn -B verify",
    "uv sync --locked",
    "uv run ruff check .",
    "uv run ruff format --check .",
    "uv run pytest -q",
    "scripts/check-am.sh",
]


def test_make_ci_runs_the_workflows_commands_in_order():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    runs = re.findall(r"^\s*- run: (.+?)\s*$", workflow, flags=re.MULTILINE)
    assert runs == WORKFLOW_COMMANDS

    lines = (ROOT / "Makefile").read_text().splitlines()
    start = lines.index("ci:") + 1
    recipe = []
    for line in lines[start:]:
        if not line.strip():
            break
        recipe.append(line)
    text = "\n".join(recipe)

    positions = []
    for command in WORKFLOW_COMMANDS:
        assert command in text, command
        positions.append(text.index(command))
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)

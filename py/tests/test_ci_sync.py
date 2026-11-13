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


def workflow_runs(workflow: str) -> list[str]:
    """The `run:` commands of a workflow's steps, in order."""
    return re.findall(r"^\s*(?:- )?run: (.+?)\s*$", workflow, flags=re.MULTILINE)


def test_workflow_runs_finds_a_run_after_a_name():
    workflow = (
        "defaults:\n"
        "  run:\n"
        "    working-directory: py\n"
        "steps:\n"
        "  - name: x\n"
        "    run: y\n"
        "  - run: z\n"
    )
    assert workflow_runs(workflow) == ["y", "z"]


def test_make_ci_runs_the_workflows_commands_in_order():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert workflow_runs(workflow) == WORKFLOW_COMMANDS

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

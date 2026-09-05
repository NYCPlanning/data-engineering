"""The `dcp_trigger_*` scripts dispatch workflows with `-f <input>=`.

An input the workflow doesn't declare fails the dispatch, and one it declares that the
script never passes is invisible until someone needs it. Both are easy to miss, since
the script and the workflow are edited in different places.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parents[2]

# script, workflow it dispatches, and whether the script means to expose every input
TRIGGER_SCRIPTS = [
    ("dcp_trigger_ingest", "ingest_single.yml", True),
    ("dcp_trigger_build", "build.yml", False),
]


def _inputs_sent(script_name: str) -> set[str]:
    script = (REPO_ROOT / "bash" / "bin" / script_name).read_text()
    return set(re.findall(r"-f (\w+)=", script))


def _inputs_declared(workflow_name: str) -> set[str]:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows" / workflow_name).read_text()
    )
    # yaml 1.1 reads a bare `on:` key as True
    triggers = workflow.get("on", workflow.get(True))
    return set(triggers["workflow_dispatch"]["inputs"])


@pytest.mark.parametrize(
    ("script", "workflow", "expect_parity"),
    TRIGGER_SCRIPTS,
    ids=[s for s, _, _ in TRIGGER_SCRIPTS],
)
def test_trigger_script_inputs_match_workflow(
    script: str, workflow: str, expect_parity: bool
):
    sent = _inputs_sent(script)
    declared = _inputs_declared(workflow)

    assert not (sent - declared), (
        f"{script} passes input(s) {sorted(sent - declared)} that {workflow} "
        "doesn't declare. The dispatch would fail."
    )

    if expect_parity:
        assert not (declared - sent), (
            f"{workflow} declares input(s) {sorted(declared - sent)} that {script} "
            f"can't pass. Add them to the script, or drop {script} from the parity list."
        )

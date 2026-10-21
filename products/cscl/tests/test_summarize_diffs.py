"""summarize_diffs: export group labels come from custom.qa_group."""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("VALIDATION_DIR", "/tmp")
sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

from summarize_diffs import qa_group  # noqa: E402


def test_qa_group_reads_custom_field():
    assert qa_group(SimpleNamespace(custom={"qa_group": "SAF", "x": 1})) == "SAF"


def test_qa_group_missing_is_empty():
    assert qa_group(SimpleNamespace(custom={"x": 1})) == ""
    assert qa_group(SimpleNamespace(custom=None)) == ""

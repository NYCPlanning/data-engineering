"""summarize_diffs: export group labels come from custom.qa_group; diff row counts."""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("VALIDATION_DIR", "/tmp")
sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

from summarize_diffs import PROD_ONLY_SEPARATOR, count_diff_rows, qa_group  # noqa: E402


def test_qa_group_reads_custom_field():
    assert qa_group(SimpleNamespace(custom={"qa_group": "SAF", "x": 1})) == "SAF"


def test_qa_group_missing_is_empty():
    assert qa_group(SimpleNamespace(custom={"x": 1})) == ""
    assert qa_group(SimpleNamespace(custom=None)) == ""


def test_count_diff_rows_skips_prod_only_separator(tmp_path):
    path = tmp_path / "nyc.thinlion"
    path.write_text(f"dev 1\ndev 2\n{PROD_ONLY_SEPARATOR}\nprod 1\nprod 2\n\n")
    assert count_diff_rows(path) == 4


def test_count_diff_rows_without_prod_only_rows(tmp_path):
    path = tmp_path / "Exception.txt"
    path.write_text("dev 1\n")
    assert count_diff_rows(path) == 1

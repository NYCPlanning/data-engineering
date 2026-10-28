"""build_diffs_report: the record order columns and the step summary's order headline."""

import os
import sys
from pathlib import Path

os.environ.setdefault("VALIDATION_DIR", "/tmp")
sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

import build_diffs_report as r  # noqa: E402


def _row(file_id: str, order_rule: str, rows_out_of_order: int | None) -> dict:
    row = {col: None for col in r.REPORT_COLUMNS}
    row |= {
        "output_group": "flat_files",
        "file_group": "g",
        "file_id": file_id,
        "diffable": "true",
        "order_rule": order_rule,
        "rows_out_of_order": rows_out_of_order,
    }
    return row


def test_out_of_prod_order():
    assert r.out_of_prod_order(_row("a", "prod", 3)) is True
    assert r.out_of_prod_order(_row("a", "prod", 0)) is False
    # no comparison, or no order to match
    assert r.out_of_prod_order(_row("a", "prod", None)) is None
    assert r.out_of_prod_order(_row("a", "none", 3)) is None


def test_step_summary_counts_files_out_of_prod_order():
    rows = [
        _row("a", "prod", 3),
        _row("b", "prod", 0),
        _row("c", "prod", None),
        _row("d", "none", 9),
    ]
    summary = r.build_step_summary(rows)
    assert "**1 of 3 flat files that must be in prod's record order aren't**" in summary
    assert "1 have no order check" in summary
    assert "Rows out of prod order" in summary

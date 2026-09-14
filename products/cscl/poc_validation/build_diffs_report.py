"""
Builds a single per-output-file diff report, using seeds/lion_outputs.csv as the
backbone, and writes it to the build folder. Also appends a condensed version to the
GitHub Actions step summary, when running in CI.

Marries two systems that otherwise don't talk to each other:
  - qa__diffs_all_summary: a dbt view aggregating qa__diffs_all's field-level diffs
    (row + changed-field granularity, with an accounted_for flag for known/legacy
    bugs) by output_file_id. Exported to CSV here.
  - validation_summary.csv: whole-line file diffs from validate_outputs.sh, keyed by
    filename. Covers every export (including the ones with no dbt QA model yet).

Run from the products/cscl directory, after validate_outputs.sh and
summarize_diffs.py (needs validation_summary.csv) and after `dbt build` (needs the
qa__diffs_all_summary table in the build schema):
    python poc_validation/build_diffs_report.py

Outputs:
  - output/validation_output/qa__diffs_all_summary.csv (raw export of the dbt view)
  - output/validation_output/diffs_report.csv (one row per lion_outputs.csv file_id)
  - $GITHUB_STEP_SUMMARY (if set): headline counts + a table of files with diffs
    (sorted by `gap` - see that function - so the most-worth-investigating files
    are at the top), with the full per-file table tucked into a <details> block
Report-only: never fails the build.
"""

import csv
import os
from pathlib import Path

from dcpy.utils.postgres import PostgresClient

LION_OUTPUTS_SEED_PATH = Path("seeds/lion_outputs.csv")
VALIDATION_DIR = Path("output/validation_output")
OUTPUT_PATH = VALIDATION_DIR / "diffs_report.csv"
VALIDATION_SUMMARY_PATH = VALIDATION_DIR / "validation_summary.csv"
DIFFS_SUMMARY_PATH = VALIDATION_DIR / "qa__diffs_all_summary.csv"

REPORT_COLUMNS = [
    "file_group",
    "subgroup",
    "type",
    "filename",
    "file_id",
    "skip_qa",
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_fields",
    "unaccounted_discrepant_rows",
    "discrepant_rows_from_file_comparison",
]
FIELD_LEVEL_COUNT_COLUMNS = [
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_rows",
    "unaccounted_discrepant_fields",
]
# Columns shown in the step summary tables (narrower than REPORT_COLUMNS - drops
# type/skip_qa, which aren't useful for a quick read).
SUMMARY_DISPLAY_COLUMNS = [
    ("file_group", "Group"),
    ("subgroup", "Subgroup"),
    ("filename", "Filename"),
    ("file_id", "File ID"),
    ("accounted_for_discrepant_rows", "Accounted-for rows"),
    ("unaccounted_discrepant_rows", "Unaccounted rows"),
    ("unaccounted_discrepant_fields", "Unaccounted fields"),
    ("discrepant_rows_from_file_comparison", "File-diff rows"),
    ("_gap", "Gap"),
]


def load_backbone() -> list[dict]:
    with LION_OUTPUTS_SEED_PATH.open(newline="") as f:
        return list(csv.DictReader(f))


def load_field_level_counts() -> dict[str, dict[str, int]]:
    """output_file_id -> field-level count columns, from qa__diffs_all_summary."""
    with DIFFS_SUMMARY_PATH.open(newline="") as f:
        return {
            row["output_file_id"]: {
                col: int(row[col]) for col in FIELD_LEVEL_COUNT_COLUMNS
            }
            for row in csv.DictReader(f)
        }


def load_file_comparison_counts() -> dict[str, int]:
    """filename -> mismatched row count, from the line-level file comparison."""
    if not VALIDATION_SUMMARY_PATH.exists():
        return {}
    with VALIDATION_SUMMARY_PATH.open(newline="") as f:
        return {
            row["filename"]: int(row["mismatched_rows"]) for row in csv.DictReader(f)
        }


def has_diffs(row: dict) -> bool:
    """Whether a row has anything worth a human looking at (accounted-for diffs
    don't count - they're already known/expected)."""
    return (row["unaccounted_discrepant_rows"] or 0) > 0 or (
        row["discrepant_rows_from_file_comparison"] or 0
    ) > 0


def has_coverage(row: dict) -> bool:
    """Whether this file has any comparison at all - field-level, file-level, or both."""
    return (
        row["unaccounted_discrepant_rows"] is not None
        or row["discrepant_rows_from_file_comparison"] is not None
    )


def gap(row: dict) -> int:
    """How far the line-level file comparison and the field-level QA models
    disagree about how many rows actually differ.

    A large gap means one of the two systems is blind to something - see
    products/cscl/docs/prod_bugs for examples (a too-broad diff key silently
    hiding real per-field diffs, or an export bug corrupting every line of a
    file that field-level QA says is clean). Sorting on this surfaces exactly
    the kind of file worth a closer look, ahead of files with a merely large
    but well-understood diff count.
    """
    accounted_for = row["accounted_for_discrepant_rows"] or 0
    unaccounted = row["unaccounted_discrepant_rows"] or 0
    file_level = row["discrepant_rows_from_file_comparison"] or 0
    return abs(file_level - (accounted_for + unaccounted))


def _markdown_cell(value) -> str:
    return "–" if value is None else str(value)


def _markdown_table(rows: list[dict]) -> str:
    header = "| " + " | ".join(label for _, label in SUMMARY_DISPLAY_COLUMNS) + " |"
    sep = "| " + " | ".join("---" for _ in SUMMARY_DISPLAY_COLUMNS) + " |"
    body_lines = [
        "| "
        + " | ".join(_markdown_cell(row[col]) for col, _ in SUMMARY_DISPLAY_COLUMNS)
        + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body_lines])


def build_step_summary(rows: list[dict]) -> str:
    for row in rows:
        row["_gap"] = gap(row)

    flagged = sorted((r for r in rows if has_diffs(r)), key=gap, reverse=True)
    uncovered = [r for r in rows if not has_coverage(r)]

    lines = [
        "## CSCL diff report",
        "",
        f"- {len(rows)} files tracked in `lion_outputs.csv`",
        f"- **{len(flagged)} have diffs to review**",
        f"- {len(uncovered)} have no comparison at all "
        "(no field-level QA model, no matching prod file to diff)",
        "",
    ]

    if flagged:
        lines += ["### Files with diffs", "", _markdown_table(flagged)]
    else:
        lines.append("No diffs found across any tracked file.")

    lines += [
        "",
        "<details>",
        f"<summary>All {len(rows)} tracked files</summary>",
        "",
        _markdown_table(rows),
        "",
        "</details>",
        "",
        "Full data: `output/validation_output/diffs_report.csv` (uploaded with the build).",
    ]
    return "\n".join(lines)


def main() -> None:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    PostgresClient().export_to_csv("qa__diffs_all_summary", DIFFS_SUMMARY_PATH)

    backbone = load_backbone()
    field_level_by_file = load_field_level_counts()
    file_comparison_counts = load_file_comparison_counts()

    rows = []
    for record in backbone:
        field_counts = field_level_by_file.get(record["file_id"])
        rows.append(
            {
                **record,
                **{
                    col: (field_counts[col] if field_counts else None)
                    for col in FIELD_LEVEL_COUNT_COLUMNS
                },
                "discrepant_rows_from_file_comparison": file_comparison_counts.get(
                    record["filename"]
                ),
            }
        )

    with OUTPUT_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    n_with_field_qa = sum(
        1 for r in rows if r["unaccounted_discrepant_rows"] is not None
    )
    n_with_file_comparison = sum(
        1 for r in rows if r["discrepant_rows_from_file_comparison"] is not None
    )
    n_with_unaccounted = sum(1 for r in rows if has_diffs(r))
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")
    print(f"  {n_with_field_qa}/{len(rows)} have field-level (qa__diffs_all) coverage")
    print(f"  {n_with_file_comparison}/{len(rows)} have a file-level comparison")
    print(f"  {n_with_unaccounted} have diffs to review")

    step_summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary_path:
        with open(step_summary_path, "a") as f:
            f.write(build_step_summary(rows) + "\n")


if __name__ == "__main__":
    main()

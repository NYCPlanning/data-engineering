"""
Builds a single per-output-file diff report, using seeds/lion_outputs.csv as the
backbone, and writes it to the build folder. Also appends a condensed version to the
GitHub Actions step summary, when running in CI.

Marries three systems that otherwise don't talk to each other:
  - qa__diffs_all_summary: a dbt view aggregating qa__diffs_all's field-level diffs
    (row + changed-field granularity, with an accounted_for flag for known/legacy
    bugs) by output_file_id, plus the LDF's separate count-based QA models
    (qa__ldf_summary, qa__ldf_header_diffs). Exported to CSV here.
  - validation_summary.csv: whole-line file diffs from validate_outputs.sh, keyed by
    filename. Covers every flat-file export (including the ones with no dbt QA
    model yet).
  - compare_gdb.py's per-gdb-export <name>_comparison.csv files: row-count deltas
    for gdb layers (there's no line-level analog for a gdb), plus its consolidated
    layer_note (structure/row/area) surfaced in the notes column. Keyed by layer
    name.

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

from dcpy.lifecycle.builds import plan
from dcpy.utils.postgres import PostgresClient

RECIPE_PATH = Path("recipe.yml")
LION_OUTPUTS_SEED_PATH = Path("seeds/lion_outputs.csv")
VALIDATION_DIR = Path("output/validation_output")
OUTPUT_PATH = VALIDATION_DIR / "diffs_report.csv"
VALIDATION_SUMMARY_PATH = VALIDATION_DIR / "validation_summary.csv"
DIFFS_SUMMARY_PATH = VALIDATION_DIR / "qa__diffs_all_summary.csv"

# Static, human-written notes for known/open issues that don't come from any QA
# model - keyed by lion_outputs.csv file_id. Left deliberately unaccounted (not
# folded into accounted_for_discrepant_rows) pending outside confirmation.
KNOWN_NOTES: dict[str, str] = {
    "ldf_dat": (
        "CSCL-LDF-01 (open, see data_issues.md): transitory-elimination residual "
        "- our lineage-graph approximation of GR's undisclosed suppression rule "
        "disagrees with prod on which intermediate segment/node records survive. "
        "Documented as ~3% as of 26b; currently measuring ~8% on this same "
        "version - worth a look, may be drift rather than the same known gap. "
        "Follow up with GR."
    ),
}

REPORT_COLUMNS = [
    "file_group",
    "subgroup",
    "type",
    "filename",
    "file_id",
    "skip_qa",
    "diffable",
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_fields",
    "unaccounted_discrepant_rows",
    "discrepant_rows_from_file_comparison",
    "notes",
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
    ("notes", "Notes"),
]


def load_backbone() -> list[dict]:
    with LION_OUTPUTS_SEED_PATH.open(newline="") as f:
        return list(csv.DictReader(f))


def load_field_level_counts() -> dict[str, dict[str, int | None]]:
    """output_file_id -> field-level count columns, from qa__diffs_all_summary.

    Postgres COPY renders SQL NULL as an empty string - the LDF rows leave
    unaccounted_discrepant_fields null (no per-field concept for a count-based
    comparison), so that has to map back to None rather than fail int().
    """
    with DIFFS_SUMMARY_PATH.open(newline="") as f:
        return {
            row["output_file_id"]: {
                col: (int(row[col]) if row[col] != "" else None)
                for col in FIELD_LEVEL_COUNT_COLUMNS
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


def load_gdb_layer_stats() -> dict[str, dict]:
    """layer name -> {"row_diff": int, "note": str}, from compare_gdb.py's
    per-gdb-export CSVs (one row per (layer, column); the layer-level fields -
    rows_only_in_dev/rows_only_in_prod/rows_modified and the consolidated
    layer_note - repeat down each layer's rows, so take the first).

    row_diff here is the real keyed row-level diff total (only_in_dev +
    only_in_prod + modified), not a net row-count delta - a layer can have equal
    row counts on both sides while every row's attributes differ, which a naive
    count comparison would completely miss. See compare_gdb.py's
    _row_level_diff for how it's computed (and when "modified" isn't available
    because no column was unique enough to pair rows precisely).

    lion_outputs.csv's filename column holds the layer name for gdb-type rows
    (not the gdb zip's filename), which is what this is keyed on. Every gdb
    export in recipe.yml is covered, not just one - compare_gdb.py runs once
    per gdb-format export.
    """
    if not RECIPE_PATH.exists():
        return {}
    recipe = plan.recipe_from_yaml(RECIPE_PATH)
    assert recipe.exports
    gdb_filenames = sorted(
        {
            export.filename
            for export in recipe.exports.datasets
            if export.format.value == "gdb" and export.filename
        }
    )

    stats: dict[str, dict] = {}
    for filename in gdb_filenames:
        report_name = Path(filename).name.split(".")[0]
        comparison_path = VALIDATION_DIR / f"{report_name}_comparison.csv"
        if not comparison_path.exists():
            continue
        with comparison_path.open(newline="") as f:
            for row in csv.DictReader(f):
                layer = row["layer"]
                if layer in stats:
                    continue
                only_in_dev = int(row["rows_only_in_dev"])
                only_in_prod = int(row["rows_only_in_prod"])
                modified = (
                    int(row["rows_modified"]) if row["rows_modified"] != "" else 0
                )
                stats[layer] = {
                    "row_diff": only_in_dev + only_in_prod + modified,
                    "note": row.get("layer_note", ""),
                }
    return stats


def has_diffs(row: dict) -> bool:
    """Whether a row has anything worth a human looking at.

    Two independent triggers: the accounted-for count doesn't fully explain the
    file-level diff count (equal means every discrepant row in the raw file
    comparison is already a known/expected diff), OR there's any unaccounted
    row from qa__diffs_all directly. The second guards against a case the first
    alone would miss: accounted_for + unaccounted could exceed file_level while
    accounted_for alone happens to equal it, which would otherwise hide a real,
    unexplained diff behind a coincidental count match.
    """
    accounted_for = row["accounted_for_discrepant_rows"] or 0
    file_level = row["discrepant_rows_from_file_comparison"] or 0
    unaccounted = row["unaccounted_discrepant_rows"] or 0
    return accounted_for != file_level or unaccounted > 0


def has_coverage(row: dict) -> bool:
    """Whether this file has any comparison at all - field-level, file-level, or both."""
    return (
        row["unaccounted_discrepant_rows"] is not None
        or row["discrepant_rows_from_file_comparison"] is not None
    )


def is_diffable(row: dict) -> bool:
    """Whether lion_outputs.csv marks this file as comparable at all (false for
    build logs and other artifacts with no prod counterpart to diff against)."""
    return row["diffable"] == "true"


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

    diffable_rows = [r for r in rows if is_diffable(r)]
    not_diffable = [r for r in rows if not is_diffable(r)]
    flagged = sorted((r for r in diffable_rows if has_diffs(r)), key=gap, reverse=True)
    uncovered = [r for r in diffable_rows if not has_coverage(r)]

    lines = [
        "## CSCL diff report",
        "",
        f"- {len(rows)} files tracked in `lion_outputs.csv` "
        f"({len(not_diffable)} marked not diffable - logs and similar - excluded below)",
        f"- **{len(flagged)} have diffs to review**",
        f"- {len(uncovered)} are diffable but have no comparison at all "
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
    gdb_layer_stats = load_gdb_layer_stats()
    gdb_row_diffs = {layer: s["row_diff"] for layer, s in gdb_layer_stats.items()}
    # Disjoint key spaces (flat filenames vs. gdb layer names) - safe to merge.
    row_comparison_counts = {**file_comparison_counts, **gdb_row_diffs}

    rows = []
    for record in backbone:
        field_counts = field_level_by_file.get(record["file_id"])
        gdb_stats = gdb_layer_stats.get(record["filename"])
        note = gdb_stats["note"] if gdb_stats else ""
        note = note if note != "OK" else ""
        note = note or KNOWN_NOTES.get(record["file_id"], "")
        rows.append(
            {
                **record,
                **{
                    col: (field_counts[col] if field_counts else None)
                    for col in FIELD_LEVEL_COUNT_COLUMNS
                },
                "discrepant_rows_from_file_comparison": row_comparison_counts.get(
                    record["filename"]
                ),
                "notes": note,
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

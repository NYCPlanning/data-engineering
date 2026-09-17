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
from collections import Counter
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
    "ldf_header": (
        "Not an independent diff - the header's own record_count field is body "
        "records + 1, so it inherits ldf_dat's CSCL-LDF-01 residual exactly: "
        "dev/prod header record_count differed by 25 on 2026-09-15, matching "
        "qa__ldf_summary's dev/prod body-record gap (1217 vs 1192) that same "
        "run. Resolves whenever CSCL-LDF-01 does - see the ldf_dat note."
    ),
}

# Seed columns this report actually uses. load_backbone() drops everything else
# from lion_outputs.csv (e.g. key_columns, which is compare_gdb.py's concern, not
# this report's) - otherwise a new seed column breaks csv.DictWriter below, which
# rejects any dict key not in REPORT_COLUMNS. This is what broke the build after
# key_columns was added: https://github.com/NYCPlanning/data-engineering/actions/runs/35026079124
SEED_COLUMNS = [
    "output_group",
    "file_group",
    "subgroup",
    "type",
    "filename",
    "file_id",
    "skip_qa",
    "diffable",
]
REPORT_COLUMNS = SEED_COLUMNS + [
    "prod_row_count",
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_fields",
    "unaccounted_discrepant_rows",
    "discrepant_rows_from_file_comparison",
    "pct_diff",
    "unaccounted_pct_diff",
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
    ("output_group", "Output"),
    ("file_group", "Group"),
    ("subgroup", "Subgroup"),
    ("filename", "Filename"),
    ("file_id", "File ID"),
    ("prod_row_count", "Prod rows"),
    ("accounted_for_discrepant_rows", "Accounted-for rows"),
    ("unaccounted_discrepant_rows", "Unaccounted rows"),
    ("unaccounted_discrepant_fields", "Unaccounted fields"),
    ("discrepant_rows_from_file_comparison", "File-diff rows"),
    ("pct_diff", "% diff"),
    ("unaccounted_pct_diff", "% diff (unaccounted)"),
    ("_gap", "Gap"),
    ("notes", "Notes"),
]


def load_backbone() -> list[dict]:
    with LION_OUTPUTS_SEED_PATH.open(newline="") as f:
        return [{col: row[col] for col in SEED_COLUMNS} for row in csv.DictReader(f)]


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


def load_file_comparison_counts() -> dict[str, dict[str, int]]:
    """filename -> {mismatched_rows, prod_row_count}, from the line-level file
    comparison (validate_outputs.sh already computes prod_row_count per file -
    this just carries it through instead of dropping it)."""
    if not VALIDATION_SUMMARY_PATH.exists():
        return {}
    with VALIDATION_SUMMARY_PATH.open(newline="") as f:
        return {
            row["filename"]: {
                "mismatched_rows": int(row["mismatched_rows"]),
                "prod_row_count": int(row["prod_row_count"]),
            }
            for row in csv.DictReader(f)
        }


def load_gdb_layer_stats() -> dict[str, dict]:
    """layer name -> {"row_diff": int, "note": str, "prod_row_count": int}, from
    compare_gdb.py's per-gdb-export CSVs (one row per (layer, column); the
    layer-level fields - rows_only_in_dev/rows_only_in_prod/rows_modified,
    prod_row_count, and the consolidated layer_note - repeat down each layer's
    rows, so take the first).

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
                    "prod_row_count": int(row["prod_row_count"]),
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


def unaccounted_pct_diff(row: dict) -> float | None:
    """% of prod rows that differ *and* aren't already a known/explained diff -
    the number that actually indicates outstanding work (a file fully accounted
    for has a large `pct_diff` but a near-zero value here).

    Only qa__diffs_all-covered files (and the LDF) have a real accounted_for
    split - `unaccounted_discrepant_rows` there and `pct_diff`'s denominator
    (`prod_row_count`) are independent, so dividing one by the other is a
    legitimate (if approximate - see `gap`) read on remaining work. Everything
    else (gdb layers via compare_gdb.py, plain file-line comparisons) has no
    numeric accounted_for breakdown at all yet - falls back to `pct_diff`
    unchanged, i.e. treated as 100% unaccounted, rather than silently implying
    0% accounted-for progress that was never actually measured.
    """
    unaccounted = row["unaccounted_discrepant_rows"]
    prod_row_count = row["prod_row_count"]
    if unaccounted is not None and prod_row_count:
        return round(unaccounted / prod_row_count * 100, 2)
    return row["pct_diff"]


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


def _markdown_cell(col: str, value) -> str:
    if value is None:
        return "–"
    if col in ("pct_diff", "unaccounted_pct_diff"):
        return f"{value:.2f}%"
    return str(value)


def _markdown_table(rows: list[dict]) -> str:
    header = "| " + " | ".join(label for _, label in SUMMARY_DISPLAY_COLUMNS) + " |"
    sep = "| " + " | ".join("---" for _ in SUMMARY_DISPLAY_COLUMNS) + " |"
    body_lines = [
        "| "
        + " | ".join(
            _markdown_cell(col, row[col]) for col, _ in SUMMARY_DISPLAY_COLUMNS
        )
        + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body_lines])


def build_step_summary(rows: list[dict]) -> str:
    for row in rows:
        row["_gap"] = gap(row)

    diffable_rows = [r for r in rows if is_diffable(r)]
    not_diffable = [r for r in rows if not is_diffable(r)]
    flagged = sorted(
        (r for r in diffable_rows if has_diffs(r)),
        key=lambda r: r["unaccounted_pct_diff"] or 0,
        reverse=True,
    )
    uncovered = [r for r in diffable_rows if not has_coverage(r)]

    flagged_by_group = Counter(r["output_group"] for r in flagged)
    diffable_by_group = Counter(r["output_group"] for r in diffable_rows)
    group_breakdown = ", ".join(
        f"{group} {flagged_by_group.get(group, 0)}/{count}"
        for group, count in sorted(diffable_by_group.items())
    )

    lines = [
        "## CSCL diff report",
        "",
        f"- {len(rows)} files tracked in `lion_outputs.csv` "
        f"({len(not_diffable)} marked not diffable - logs and similar - excluded below)",
        f"- **{len(flagged)} have diffs to review** ({group_breakdown})",
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
    # Disjoint key spaces (flat filenames vs. gdb layer names) - safe to merge.
    row_comparison_counts = {
        filename: counts["mismatched_rows"]
        for filename, counts in file_comparison_counts.items()
    } | {layer: s["row_diff"] for layer, s in gdb_layer_stats.items()}
    prod_row_counts = {
        filename: counts["prod_row_count"]
        for filename, counts in file_comparison_counts.items()
    } | {layer: s["prod_row_count"] for layer, s in gdb_layer_stats.items()}

    rows = []
    for record in backbone:
        field_counts = field_level_by_file.get(record["file_id"])
        gdb_stats = gdb_layer_stats.get(record["filename"])
        note = gdb_stats["note"] if gdb_stats else ""
        note = note if note != "OK" else ""
        note = note or KNOWN_NOTES.get(record["file_id"], "")
        discrepant_rows = row_comparison_counts.get(record["filename"])
        prod_row_count = prod_row_counts.get(record["filename"])
        row = {
            **record,
            **{
                col: (field_counts[col] if field_counts else None)
                for col in FIELD_LEVEL_COUNT_COLUMNS
            },
            "prod_row_count": prod_row_count,
            "discrepant_rows_from_file_comparison": discrepant_rows,
            "pct_diff": (
                round(discrepant_rows / prod_row_count * 100, 2)
                if discrepant_rows is not None and prod_row_count
                else None
            ),
            "notes": note,
        }
        row["unaccounted_pct_diff"] = unaccounted_pct_diff(row)
        rows.append(row)

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

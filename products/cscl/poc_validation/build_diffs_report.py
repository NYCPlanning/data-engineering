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
  - <QA_DIR>/validation_output/qa__diffs_all_summary.csv (raw export of the dbt view)
  - <QA_DIR>/validation_output/diffs_report.csv (one row per lion_outputs.csv file_id)
  - $GITHUB_STEP_SUMMARY (if set): headline counts + one table of every tracked
    file. On a branch build (BUILD_NAME other than main), each numeric cell also
    shows its change vs main's latest diffs_report.csv (MAIN_REPORT_URL).

To render the step summary from existing report CSVs, without Postgres:
    python poc_validation/build_diffs_report.py --summary-only <diffs_report.csv> \
        [--main-csv <main's diffs_report.csv>]
Report-only: never fails the build.
"""

import argparse
import csv
import io
import os
from collections import Counter
from pathlib import Path

import requests
from paths import validation_dir

from dcpy.lifecycle.builds import plan
from dcpy.utils.duckdb import DuckDBClient

RECIPE_PATH = Path("recipe.yml")
LION_OUTPUTS_SEED_PATH = Path("seeds/lion_outputs.csv")
VALIDATION_DIR = validation_dir()
OUTPUT_PATH = VALIDATION_DIR / "diffs_report.csv"
VALIDATION_SUMMARY_PATH = VALIDATION_DIR / "validation_summary.csv"
DIFFS_SUMMARY_PATH = VALIDATION_DIR / "qa__diffs_all_summary.csv"
# main's latest diffs_report.csv - branch builds show each file's change against it
MAIN_REPORT_URL = "https://edm-publishing.nyc3.digitaloceanspaces.com/db-cscl/build/main/validation_output/diffs_report.csv"

# Static, human-written notes for known/open issues that don't come from any QA
# model - keyed by lion_outputs.csv file_id. Left deliberately unaccounted (not
# folded into accounted_for_discrepant_rows) pending outside confirmation.
#
# ldf_dat/ldf_header used to live here; both now have real status/status_summary/
# notes directly in lion_outputs.csv (load_seed_notes always wins over this dict,
# so keeping stale duplicate entries here would just be dead code).
KNOWN_NOTES: dict[str, str] = {}

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
    "has_db_qa",
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
    "status",
    "status_summary",
    "notes",
]
FIELD_LEVEL_COUNT_COLUMNS = [
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_rows",
    "unaccounted_discrepant_fields",
]
# Columns shown in the step summary table (narrower than REPORT_COLUMNS - drops
# subgroup/filename/type/has_db_qa, plus the field count and raw % diff, which
# aren't useful for a quick read). Shows status_summary, not notes - the full
# explanation is one click away in diffs_report.csv/lion_outputs.csv itself; this
# table is for skimming.
SUMMARY_DISPLAY_COLUMNS = [
    ("output_group", "Output"),
    ("file_group", "Group"),
    ("file_id", "File ID"),
    ("prod_row_count", "Prod rows"),
    ("accounted_for_discrepant_rows", "Accounted-for rows"),
    ("unaccounted_discrepant_rows", "Unaccounted rows"),
    ("discrepant_rows_from_file_comparison", "File-diff rows"),
    ("unaccounted_pct_diff", "% diff (unaccounted)"),
    ("_gap", "Gap"),
    ("status", "Status"),
    ("status_summary", "Status summary"),
]
# Numeric report columns, for reading a diffs_report.csv back in (main's, or a
# local one via --summary-only). Empty cells are None.
INT_REPORT_COLUMNS = [
    "prod_row_count",
    "accounted_for_discrepant_rows",
    "unaccounted_discrepant_fields",
    "unaccounted_discrepant_rows",
    "discrepant_rows_from_file_comparison",
]
PCT_COLUMNS = ["pct_diff", "unaccounted_pct_diff"]
# Summary columns that show their change vs main
DELTA_COLUMNS = INT_REPORT_COLUMNS + PCT_COLUMNS + ["_gap"]


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


def load_seed_column(column: str) -> dict[str, str]:
    """file_id -> a raw lion_outputs.csv column value, non-empty entries only."""
    with LION_OUTPUTS_SEED_PATH.open(newline="") as f:
        return {
            row["file_id"]: row[column].strip()
            for row in csv.DictReader(f)
            if row.get(column, "").strip()
        }


def load_seed_notes() -> dict[str, str]:
    """file_id -> a human-curated note, from lion_outputs.csv's own status/notes
    columns - the durable, manually-maintained source of truth for known/blocked
    issues (e.g. "BLOCKED: Layer frozen since 2009 - see Bug 013...").
    Takes priority over any auto-generated note in build_step_summary/the report
    below - see main()."""
    with LION_OUTPUTS_SEED_PATH.open(newline="") as f:
        result: dict[str, str] = {}
        for row in csv.DictReader(f):
            status = (row.get("status") or "").strip()
            note = (row.get("notes") or "").strip()
            if status and note:
                result[row["file_id"]] = f"{status.upper()}: {note}"
            elif status:
                result[row["file_id"]] = status.upper()
            elif note:
                result[row["file_id"]] = note
    return result


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
    data_issues.md for examples (a too-broad diff key silently
    hiding real per-field diffs, or an export bug corrupting every line of a
    file that field-level QA says is clean). Sorting on this surfaces exactly
    the kind of file worth a closer look, ahead of files with a merely large
    but well-understood diff count.
    """
    accounted_for = row["accounted_for_discrepant_rows"] or 0
    unaccounted = row["unaccounted_discrepant_rows"] or 0
    file_level = row["discrepant_rows_from_file_comparison"] or 0
    return abs(file_level - (accounted_for + unaccounted))


def read_report_csv(source: Path | str) -> list[dict]:
    """Rows of a diffs_report.csv (a path, or the CSV text itself), with the numeric
    columns converted back from strings - the inverse of what main() writes."""
    text = source.read_text() if isinstance(source, Path) else source
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        for col in INT_REPORT_COLUMNS:
            row[col] = int(row[col]) if row[col] != "" else None
        for col in PCT_COLUMNS:
            row[col] = float(row[col]) if row[col] != "" else None
        rows.append(row)
    return rows


def load_main_rows() -> dict[str, dict] | None:
    """file_id -> row of main's latest diffs_report.csv, or None if it can't be
    loaded (a missing or unreadable report never fails the build)."""
    try:
        response = requests.get(MAIN_REPORT_URL, timeout=30)
        response.raise_for_status()
        return {row["file_id"]: row for row in read_report_csv(response.text)}
    except Exception as e:
        print(f"Couldn't load main's diffs report ({MAIN_REPORT_URL}): {e}")
        return None


def _format_value(col: str, value) -> str:
    if col in PCT_COLUMNS:
        return f"{value:.2f}%"
    return str(value)


def _delta(col: str, value, main_value) -> str:
    """The change vs main, as GitHub-rendered colored math: red for an increase
    (more diffs), green for a decrease. Empty when there's nothing to compare."""
    if value is None or main_value is None:
        return ""
    change = value - main_value
    if col in PCT_COLUMNS:
        change = round(change, 2)
        if change == 0:
            return ""
        text = f"{change:+.2f}\\%"
    else:
        if change == 0:
            return ""
        text = f"{change:+d}"
    color = "red" if change > 0 else "green"
    return f" $\\color{{{color}}}{{\\text{{{text}}}}}$"


def _markdown_cell(col: str, row: dict, main_row: dict | None) -> str:
    value = row[col]
    if value is None:
        return "–"
    cell = _format_value(col, value)
    if main_row is not None and col in DELTA_COLUMNS:
        cell += _delta(col, value, main_row[col])
    return cell


def _markdown_table(rows: list[dict], main_rows: dict[str, dict] | None) -> str:
    header = "| " + " | ".join(label for _, label in SUMMARY_DISPLAY_COLUMNS) + " |"
    sep = "| " + " | ".join("---" for _ in SUMMARY_DISPLAY_COLUMNS) + " |"
    body_lines = []
    for row in rows:
        main_row = main_rows.get(row["file_id"]) if main_rows is not None else None
        cells = [
            _markdown_cell(col, row, main_row) for col, _ in SUMMARY_DISPLAY_COLUMNS
        ]
        if main_rows is not None and main_row is None:
            cells[[c for c, _ in SUMMARY_DISPLAY_COLUMNS].index("file_id")] += " (new)"
        body_lines.append("| " + " | ".join(cells) + " |")
    return "\n".join([header, sep, *body_lines])


def build_step_summary(
    rows: list[dict], main_rows: dict[str, dict] | None = None, vs_main: bool = False
) -> str:
    """The step summary: headline counts and one table of every tracked file. With
    main_rows (main's latest report, keyed by file_id), numeric cells also show
    their change vs main. vs_main without main_rows means main's report couldn't be
    loaded - noted, and the table renders without deltas."""
    for row in rows:
        row["_gap"] = gap(row)
    if main_rows is not None:
        for main_row in main_rows.values():
            main_row["_gap"] = gap(main_row)

    diffable_rows = [r for r in rows if is_diffable(r)]
    not_diffable = [r for r in rows if not is_diffable(r)]
    flagged = [r for r in diffable_rows if has_diffs(r)]
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
        f"({len(not_diffable)} marked not diffable - logs and similar)",
        f"- **{len(flagged)} have diffs to review** ({group_breakdown})",
        f"- {len(uncovered)} are diffable but have no comparison at all "
        "(no field-level QA model, no matching prod file to diff)",
    ]
    if main_rows is not None:
        lines.append(
            "- Changes vs main's latest build are shown in each cell "
            "(red: more than main, green: fewer)"
        )
    elif vs_main:
        lines.append(f"- Main's diffs report couldn't be loaded ({MAIN_REPORT_URL})")
    lines += [
        "",
        _markdown_table(rows, main_rows),
        "",
        "Full data: `<QA_DIR>/validation_output/diffs_report.csv` (uploaded with the build).",
    ]
    return "\n".join(lines)


def is_branch_build() -> bool:
    build_name = os.environ.get("BUILD_NAME")
    return bool(build_name) and build_name != "main"


def print_summary_from_csv(current_csv: Path, main_csv: Path | None) -> None:
    """Render the step summary from report CSVs, without Postgres or a build."""
    rows = read_report_csv(current_csv)
    main_rows = (
        {row["file_id"]: row for row in read_report_csv(main_csv)}
        if main_csv and main_csv.exists()
        else None
    )
    print(build_step_summary(rows, main_rows, vs_main=main_csv is not None))


def main() -> None:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    DuckDBClient(
        Path(os.environ["DUCKDB_PATH"]), os.environ["BUILD_ENGINE_SCHEMA"]
    ).export_to_csv(table_name="qa__diffs_all_summary", output_path=DIFFS_SUMMARY_PATH)

    backbone = load_backbone()
    field_level_by_file = load_field_level_counts()
    file_comparison_counts = load_file_comparison_counts()
    gdb_layer_stats = load_gdb_layer_stats()
    seed_notes = load_seed_notes()
    seed_status = load_seed_column("status")
    seed_status_summary = load_seed_column("status_summary")
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
        # lion_outputs.csv's own status/notes are the curated, durable source of
        # truth (see load_seed_notes) - only fall back to compare_gdb.py's
        # auto-generated technical note or the static KNOWN_NOTES dict when we
        # haven't manually annotated this file yet.
        note = seed_notes.get(record["file_id"], "")
        if not note:
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
            "status": seed_status.get(record["file_id"], ""),
            "status_summary": seed_status_summary.get(record["file_id"], ""),
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
        vs_main = is_branch_build()
        main_rows = load_main_rows() if vs_main else None
        with open(step_summary_path, "a") as f:
            f.write(build_step_summary(rows, main_rows, vs_main) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary-only",
        type=Path,
        metavar="REPORT_CSV",
        help="print the step summary for an existing diffs_report.csv and exit",
    )
    parser.add_argument(
        "--main-csv",
        type=Path,
        help="main's diffs_report.csv, to show changes against (with --summary-only)",
    )
    args = parser.parse_args()
    if args.summary_only:
        print_summary_from_csv(args.summary_only, args.main_csv)
    else:
        main()

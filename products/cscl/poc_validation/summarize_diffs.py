"""
Parses validation output text files and produces a CSV summary of diffs.

Run from the products/cscl directory:
    python poc_validation/summarize_diffs.py

Outputs: output/validation_output/diffs_summary.csv
"""

import csv
from pathlib import Path

from dcpy.lifecycle.builds import plan

VALIDATION_DIR = Path("output/validation_output")
OUTPUT_PATH = VALIDATION_DIR / "diffs_summary.csv"

FIELDNAMES = [
    "Group",
    "File name",
    "Table name",
    "Name",
    "Has diffs",
    "# of rows with diffs",
    "DE Note",
]

# Files to exclude from the summary
EXCLUDE = {"diffs_summary.csv", "log.csv"}


def build_export_name_to_group() -> dict[str, str]:
    """Scan the raw YAML to capture the comment-based group above each export entry."""
    lines = Path("recipe.yml").read_text().splitlines()
    in_exports = False
    in_datasets = False
    current_group = ""
    result: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if stripped == "exports:":
            in_exports = True
        elif in_exports and stripped == "datasets:":
            in_datasets = True
        elif in_datasets:
            if stripped.startswith("# "):
                current_group = stripped[2:].strip()
            elif stripped.startswith("- name:"):
                name = stripped[len("- name:") :].strip()
                result[name] = current_group
    return result


def build_filename_to_table_name() -> dict[str, str]:
    recipe = plan.recipe_from_yaml(Path("recipe.yml"))
    assert recipe.exports
    return {
        export.filename: export.name
        for export in recipe.exports.datasets
        if export.filename
    }


def count_diff_rows(path: Path) -> int:
    """Count non-empty lines in a validation output file."""
    text = path.read_text(errors="replace")
    return sum(1 for line in text.splitlines() if line.strip())


def main() -> None:
    filename_to_table = build_filename_to_table_name()
    export_name_to_group = build_export_name_to_group()

    files = sorted(
        p for p in VALIDATION_DIR.iterdir() if p.is_file() and p.name not in EXCLUDE
    )

    rows = []
    for path in files:
        diff_count = count_diff_rows(path)
        table_name = filename_to_table.get(path.name, "")
        group_name = export_name_to_group.get(table_name, "")
        display_name = " ".join([group_name, path.name.split(".")[0]]).strip()
        rows.append(
            {
                "Group": group_name,
                "File name": path.name,
                "Table name": table_name,
                "Name": display_name,
                "Has diffs": diff_count > 0,
                "# of rows with diffs": diff_count,
                "DE Note": "",
            }
        )

    rows.sort(key=lambda r: r["Group"])

    with OUTPUT_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")
    diffs = sum(1 for r in rows if r["Has diffs"])
    print(f"Files with diffs: {diffs}/{len(rows)}")


if __name__ == "__main__":
    main()

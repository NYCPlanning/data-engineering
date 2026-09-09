"""Take a DCAS LIFT master data fields workbook into the repo as products/lift/data_dictionary.csv.

Two jobs, because they're both done exactly once per revision DCAS sends: strip the
workbook's authoring metadata in place, then write the CSV from it. Stripping is
idempotent, so rerunning against the already-clean checked-in workbook only rewrites
the CSV.

The workbook is checked in under resources/ with the filename DCAS sent it under. Rerun
this against the next revision they send.

Usage:
    python products/lift/scripts/update_data_dictionary.py \
        "products/lift/resources/LIFT Master Data Fields - Clean 20260630.xlsx" \
        products/lift/data_dictionary.csv

Cell values are copied verbatim, including trailing whitespace, so the output diffs
cleanly against the next revision. Three exceptions: smart punctuation is folded to
ASCII, field names are stripped (they're identifiers other things will key off, and a
stray space breaks that silently), and the swapped columns below are put back in order.
"""

import csv
import re
import sys
import zipfile
from pathlib import Path

import openpyxl

# DCAS has revised the sheet name across revisions ("All Fields Rev4"), so match the
# stable prefix rather than pinning the revision.
SHEET_PREFIX = "All Fields"
# Column A is unlabeled in the workbook and vertically merged into runs of rows.
GROUP_HEADER = "Field Group"
FIELD_NAME_COLUMN = 2

# `Lease Exp Date` is the one row where the format string sits in Type and the datatype
# in Expected Values, breaking the Type column's `Text`/`Number`/`Date` domain. Keyed on
# the values as the workbook currently has them, so a fix upstream is a no-op here rather
# than a swap back to wrong.
SWAPPED_EXPECTED_VALUES_AND_TYPE = {"Lease Exp Date": ("Date", "MM/DD/YYY")}
EXPECTED_VALUES_INDEX, TYPE_INDEX = 3, 4

# Autocorrect artifacts from editing the workbook in Excel. A curly apostrophe reads as
# a backtick in a lot of fonts, and none of these survive a trip through a non-UTF-8
# reader intact.
ASCII_PUNCTUATION = str.maketrans(
    {
        "‘": "'",
        "’": "'",
        "“": '"',
        "”": '"',
        "–": "-",
        "—": "-",
        "…": "...",
        " ": " ",
    }
)

# The workbook is authored in Office 365 and arrives carrying the author's and last
# editor's names, an absolute path into someone's OneDrive, a Purview label, and tenant
# GUIDs. This repo is public, so none of that gets checked in.
METADATA_PART = "docProps/custom.xml"
METADATA_SUBSTITUTIONS = {
    "docProps/core.xml": [
        (r"(<dc:creator>).*?(</dc:creator>)", r"\1\2"),
        (r"(<cp:lastModifiedBy>).*?(</cp:lastModifiedBy>)", r"\1\2"),
    ],
    "xl/workbook.xml": [
        (r"<mc:AlternateContent\b.*?</mc:AlternateContent>", ""),
        (r"<xr:revisionPtr\b[^>]*/>", ""),
    ],
    "[Content_Types].xml": [
        (r'<Override PartName="/' + re.escape(METADATA_PART) + r'"[^>]*/>', ""),
    ],
    "_rels/.rels": [
        (r'<Relationship\b[^>]*Target="' + re.escape(METADATA_PART) + r'"[^>]*/>', ""),
    ],
}


def _scrub(part_name: str, data: bytes) -> bytes:
    substitutions = METADATA_SUBSTITUTIONS.get(part_name)
    if not substitutions:
        return data
    text = data.decode("utf-8")
    for pattern, replacement in substitutions:
        text = re.sub(pattern, replacement, text, flags=re.S)
    return text.encode("utf-8")


def strip_metadata(xlsx_path: Path) -> bool:
    """Remove authoring metadata in place. Returns whether anything changed."""
    with zipfile.ZipFile(xlsx_path) as zin:
        original = [(info, zin.read(info.filename)) for info in zin.infolist()]
    scrubbed = [
        (info, _scrub(info.filename, data))
        for info, data in original
        if info.filename != METADATA_PART
    ]
    if [d for _, d in scrubbed] == [d for _, d in original]:
        return False

    tmp = xlsx_path.with_name(xlsx_path.name + ".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in scrubbed:
            zout.writestr(info, data)
    tmp.replace(xlsx_path)
    return True


def _worksheet(workbook: openpyxl.Workbook):
    matches = [name for name in workbook.sheetnames if name.startswith(SHEET_PREFIX)]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one sheet named {SHEET_PREFIX}*, found {matches}. "
            f"Sheets in this workbook: {workbook.sheetnames}"
        )
    return workbook[matches[0]]


def main(xlsx_path: str, csv_path: str) -> None:
    xlsx = Path(xlsx_path)
    if strip_metadata(xlsx):
        print(f"stripped authoring metadata from {xlsx.name}")

    ws = _worksheet(openpyxl.load_workbook(xlsx, data_only=True))

    header = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    header[0] = GROUP_HEADER

    rows = []
    group = None
    for r in range(2, ws.max_row + 1):
        # A merged range holds its value in the top-left cell only; carry it down so
        # every row is self-describing.
        cell_group = ws.cell(r, 1).value
        if cell_group is not None:
            # Newlines in a group label are text wrapping in the merged cell, not
            # content - collapsing them keeps each CSV row on one physical line.
            # Newlines elsewhere separate enumerated codes and are left alone.
            group = " ".join(str(cell_group).translate(ASCII_PUNCTUATION).split())

        values = []
        for c in range(2, ws.max_column + 1):
            v = ws.cell(r, c).value
            v = "" if v is None else str(v).translate(ASCII_PUNCTUATION)
            values.append(v.strip() if c == FIELD_NAME_COLUMN else v)

        if group is None and not any(values):
            continue

        row = [group] + values
        pair = (row[EXPECTED_VALUES_INDEX], row[TYPE_INDEX])
        if SWAPPED_EXPECTED_VALUES_AND_TYPE.get(row[1]) == pair:
            row[EXPECTED_VALUES_INDEX], row[TYPE_INDEX] = pair[1], pair[0]
        rows.append(row)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)

    print(f"{len(rows)} rows -> {csv_path}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

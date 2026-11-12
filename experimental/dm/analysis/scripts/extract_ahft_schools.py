"""Extract the No Action school utilization from the AHFT schools workbook into seeds/.

Reads the AHFT_ONLY sheet, the school subdistricts related to AHFT districts:

    python scripts/extract_ahft_schools.py "path/to/AHFT Schools Analysis.xlsx"

Writes seeds/ahft_school_subdistricts.csv, one row per subdistrict, with the CD and AHFT priority as printed and
the No Action primary and intermediate students, seats, and utilization. The With Action columns are left out.
"""

import csv
import sys
from pathlib import Path

import openpyxl

SEED = Path(__file__).parent.parent / "seeds" / "ahft_school_subdistricts.csv"

HEADER_ROW = 6
HEADER = [
    "ID",
    "Community School District",
    "Subdistrict",
    "CD",
    "Boro",
    "AHFT Priority",
    "Students (Total Enrollment)",
    "Seats (Projected Capacity)",
    "Utilization (%)",
    "Students (Total Enrollment)",
    "Seats (Projected Capacity)",
    "Utilization (%)",
]


def main(workbook: Path) -> None:
    sheet = openpyxl.load_workbook(workbook, read_only=True)["AHFT_ONLY"]
    rows = list(sheet.iter_rows(values_only=True))
    # column A holds the * for subdistricts that barely intersect their CD, B onward the table
    assert [str(v).strip() for v in rows[HEADER_ROW - 1][1:13]] == HEADER, rows[
        HEADER_ROW - 1
    ]
    assert rows[HEADER_ROW - 2][7] == "Primary School", rows[HEADER_ROW - 2]
    assert rows[HEADER_ROW - 2][10] == "Intermediate School", rows[HEADER_ROW - 2]
    seed_rows = []
    # the table ends at the first row without a school district, above the footnote
    for row in rows[HEADER_ROW:]:
        if not isinstance(row[2], int):
            break
        flag, _id, district, subdistrict, cd, _boro, priority, *values = row[:13]
        seed_rows.append(
            {
                "school_district": district,
                "subdistrict": subdistrict,
                "community_districts": cd.strip(),
                "ahft_priority": priority.strip(),
                "barely_intersects_cd": flag is not None and flag.strip() == "*",
                "ps_students": values[0],
                "ps_seats": values[1],
                "ps_utilization": values[2],
                "is_students": values[3],
                "is_seats": values[4],
                "is_utilization": values[5],
            }
        )
    keys = [(r["school_district"], r["subdistrict"]) for r in seed_rows]
    assert len(keys) == len(set(keys)), keys
    with open(SEED, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(seed_rows[0]))
        writer.writeheader()
        writer.writerows(seed_rows)
    print(f"wrote {len(seed_rows)} rows to seeds/{SEED.name}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))

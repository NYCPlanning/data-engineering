"""Extract the WRRF table from DEP's Map of the New York City Wastewater System into seeds/.

Reads the PDF's text layer with word positions (pdftotext -bbox, poppler). scripts/download.py downloads the PDF:

    python scripts/extract_dep_wastewater_map.py data/sources/dep_wastewater_system_map.pdf

Writes two seeds:
- dep_wastewater_facilities.csv: one row per WRRF, with every column in the table
- dep_wastewater_facility_functions.csv: one row per WRRF and function it has a dot for
"""

import csv
import re
import subprocess
import sys
from pathlib import Path

SEEDS_DIR = Path(__file__).parent.parent / "seeds"

# The table's columns, by the x range (in points) their words start in. The legend's text sits under the table
# in the text layer, but never on a row's exact baseline.
COLUMNS = {
    "map_code": (25, 36),
    "wastewater_facility_name": (38, 75),
    "year": (75, 86),
    "capacity_mgd": (86, 105),
    "wet_weather_flow_mgd": (105, 123),
    "borough": (124, 150),
    "population_served": (150, 168),
    "drainage_area_acres": (168, 193),
}
ROW_CODE = re.compile(r"^(\d+)?[A-Z]{1,2}$")
SAME_ROW_POINTS = 0.2
# the bold row codes sit a little higher, and no legend text is near them
SAME_ROW_POINTS_CODE = 0.5

# The function columns' headers are rotated text, so they're listed here, left to right, with the x (in points)
# each column's dots are centered on.
FUNCTIONS = [
    ("Wastewater Treatment", "Preliminary Treatment", 204.0),
    ("Wastewater Treatment", "Secondary Treatment", 211.2),
    ("Wastewater Treatment", "Biological Nutrient Removal (BNR)", 220.2),
    ("Wastewater Treatment", "Chlorination", 229.2),
    ("Wastewater Treatment", "Dechlorination", 236.4),
    ("Solids Handling", "Sludge Dewatering", 243.6),
    ("Solids Handling", "Sludge Pumping", 250.8),
    ("Solids Handling", "Docks", 258.0),
    ("Energy Generation", "Gas to Grid / Food Waste", 264.3),
    ("Energy Generation", "Cogen Engines", 272.4),
    ("Energy Generation", "Title V Facility", 279.6),
    ("Energy Generation", "CSO Facilities", 286.8),
]
# Hollow dots, "Emergency Only" in the legend, use the same character as filled ones, so these were read from
# the rendered map.
EMERGENCY_ONLY = {("owls_head", "Cogen Engines"), ("coney_island", "Cogen Engines")}


def facility_id(name: str) -> str:
    return re.sub(r"\W+", "_", name.strip().lower()).strip("_")


def read_words(pdf: Path) -> list[tuple[float, float, str]]:
    """Each word's left and top edges, and its text. Tops rather than centers, since the legend's text is a
    different size and its centers can line up with a row's."""
    html = subprocess.run(
        ["pdftotext", "-bbox", str(pdf), "-"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    words = re.findall(r'xMin="([\d.]+)" yMin="([\d.]+)"[^>]*>([^<]*)', html)
    return [(float(x0), float(y0), text) for x0, y0, text in words]


def main(pdf: Path) -> None:
    words = read_words(pdf)
    # a row is the line its year is on, so row codes elsewhere on the map (the legend's NC icon) don't count
    year_low, year_high = COLUMNS["year"]
    row_ys = sorted(
        y
        for x, y, text in words
        if year_low <= x < year_high and re.fullmatch(r"(19|20)\d\d", text)
    )
    facilities = []
    for row_y in row_ys:
        row = {column: [] for column in COLUMNS}
        for x, y, text in sorted(words):
            for column, (low, high) in COLUMNS.items():
                tolerance = (
                    SAME_ROW_POINTS_CODE if column == "map_code" else SAME_ROW_POINTS
                )
                if low <= x < high and abs(y - row_y) <= tolerance:
                    row[column].append(text)
        text = {column: " ".join(parts) for column, parts in row.items()}
        assert ROW_CODE.match(text["map_code"]), text
        numbers = [
            "year",
            "capacity_mgd",
            "wet_weather_flow_mgd",
            "population_served",
            "drainage_area_acres",
        ]
        facilities.append(
            {
                "wastewater_facility_id": facility_id(text["wastewater_facility_name"]),
                **text,
                **{column: int(text[column].replace(",", "")) for column in numbers},
            }
        )
    assert len(facilities) == 14, [f["map_code"] for f in facilities]

    functions = []
    for x, y, text in words:
        if text != "●" or x < FUNCTIONS[0][2] - 4:
            continue
        nearest = min(
            facilities, key=lambda facility: abs(row_ys[facilities.index(facility)] - y)
        )
        if abs(row_ys[facilities.index(nearest)] - y) > 4:
            continue  # the legend's own dot
        group, function, center = min(FUNCTIONS, key=lambda f: abs(f[2] - (x + 1.5)))
        assert abs(center - (x + 1.5)) < 2, (nearest["map_code"], x)
        functions.append(
            {
                "wastewater_facility_id": nearest["wastewater_facility_id"],
                "function_group": group,
                "function": function,
                "is_emergency_only": (nearest["wastewater_facility_id"], function)
                in EMERGENCY_ONLY,
            }
        )
    found = {(f["wastewater_facility_id"], f["function"]) for f in functions}
    assert len(found) == len(functions) and EMERGENCY_ONLY <= found
    order = [f["wastewater_facility_id"] for f in facilities]
    names = [f[1] for f in FUNCTIONS]
    functions.sort(
        key=lambda f: (
            order.index(f["wastewater_facility_id"]),
            names.index(f["function"]),
        )
    )

    for filename, rows in [
        ("dep_wastewater_facilities.csv", facilities),
        ("dep_wastewater_facility_functions.csv", functions),
    ]:
        with open(SEEDS_DIR / filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        print(f"wrote {len(rows)} rows to seeds/{filename}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))

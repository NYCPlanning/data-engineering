"""Extract the Fair Housing Growth Strategy's community district tables into seeds/.

Reads the report's text layer with pdftotext (poppler), so the PDF has to be downloaded first:

    curl -o data/sources/fhgs.pdf https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf
    python scripts/extract_fhgs.py data/sources/fhgs.pdf

Writes four seeds:
- fhgs_2030_targets.csv: 2030 targets by community district (pages 52-57)
- fhgs_2030_borough_totals.csv: the borough total rows of those tables, for checking the rows
- fhgs_2030_target_inputs.csv: Appendix B, 2030 Housing Production Target Inputs (pages 111-116)
- fhgs_growth_rates_2016_2025.csv: Figure 3.2, annual rate of new homes added 2016-2025 (page 29)

Percentages are written on a 0-100 scale, and N/A as empty.
"""

import csv
import re
import subprocess
import sys
from pathlib import Path

SEEDS_DIR = Path(__file__).parent.parent / "seeds"

CD_CODE = re.compile(r"^\s*(BX|BK|MN|QN|SI) ?(\d+)\s*$")
TARGET_ROW = re.compile(
    r"^(?P<name>.*?)\s{2,}(?P<cohort>High|Medium|Low)\s+(?P<baseline>[\d.]+)%\s+(?P<laa>[\d.]+%|N/A)"
    r"\s+(?P<transit>[\d.]+%|N/A)\s+(?P<flooding>[\d.]+%|N/A)\s+(?P<target>[\d.]+)%(?:\s+(?P<homes>[\d,]+))?\s*$"
)
# a few rows print total homes a line below the rest of the row
HOMES_ONLY = re.compile(r"^(?P<name>.*?)\s{2,}(?P<homes>\d{1,3},\d{3})\s*$")
COUNT = r"(\d{1,3}(?:,\d{3})*)"
TARGETS_ROW = re.compile(rf"^(?P<name>.*?)\s{{2,}}{COUNT}\s+{COUNT}\s+{COUNT}\s+{COUNT}\s+{COUNT}\s*$")
BOROUGH_TOTAL = re.compile(rf"(BRONX|BROOKLYN|MANHATTAN|QUEENS|STATEN ISLAND) TOTAL\s+{COUNT}\s+{COUNT}\s+{COUNT}\s+{COUNT}")
GROWTH_RATE = re.compile(r"(BX|BK|MN|QN|SI) ?(\d+)\s+([\d.]+)%")
# text on the appendix pages that isn't part of a district name
NOT_A_NAME = re.compile(
    r"TARGET INPUTS|FAIR HOUSING|LOCAL LAW|Community District|Cohort|Limited|Baseline|Growth"
    r"|Adjustment|Increase|Reduction|Transit|Tidal|Total|Access|Flooding|Affordability|\(x|2025"
)


def pdf_text(pdf: Path, first: int, last: int) -> list[str]:
    result = subprocess.run(
        ["pdftotext", "-layout", "-f", str(first), "-l", str(last), str(pdf), "-"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def pct(value: str) -> str:
    return "" if value == "N/A" else value.rstrip("%")


def target_inputs(pdf: Path) -> list[dict]:
    rows: list[dict] = []
    name_parts: list[str] = []
    for line in pdf_text(pdf, 111, 116):
        if NOT_A_NAME.search(line):
            continue
        code = CD_CODE.match(line)
        if code:
            if rows:
                rows[-1]["community_district_name"] = " ".join(name_parts)
            rows.append({"community_district": f"{code[1]} {code[2]}"})
            name_parts = []
            continue
        if not rows:
            continue
        data = TARGET_ROW.match(line)
        if data:
            rows[-1] |= {
                "growth_cohort": data["cohort"],
                "baseline_target_growth_rate_pct": pct(data["baseline"] + "%"),
                "laa_adjustment_pct": pct(data["laa"]),
                "transit_access_adjustment_pct": pct(data["transit"]),
                "tidal_flooding_adjustment_pct": pct(data["flooding"]),
                "target_growth_rate_pct": data["target"],
            }
            if data["homes"]:
                rows[-1]["total_homes_2025"] = data["homes"].replace(",", "")
            line = data["name"]
        elif homes := HOMES_ONLY.match(line):
            rows[-1]["total_homes_2025"] = homes["homes"].replace(",", "")
            line = homes["name"]
        if line.strip():
            name_parts.append(line.strip())
    rows[-1]["community_district_name"] = " ".join(name_parts)
    for row in rows:
        # rejoin names hyphenated across lines, e.g. Bedford-Stuyvesant
        row["community_district_name"] = row["community_district_name"].replace("- ", "-")
    return rows


def count(value: str) -> str:
    return value.replace(",", "")


def targets(pdf: Path) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    totals: list[dict] = []
    for line in pdf_text(pdf, 52, 57):
        if total := BOROUGH_TOTAL.search(line):
            totals.append(
                {
                    "borough": total[1].title(),
                    "target_new_homes": count(total[2]),
                    "target_affordable_homes": count(total[3]),
                    "target_deeply_affordable_homes": count(total[4]),
                    "target_homes_for_homeless_households": count(total[5]),
                }
            )
        elif code := CD_CODE.match(line):
            rows.append({"community_district": f"{code[1]} {code[2]}"})
        elif rows and (data := TARGETS_ROW.match(line)):
            rows[-1] |= {
                "homes_added_2021_2025": count(data[2]),
                "target_new_homes": count(data[3]),
                "target_affordable_homes": count(data[4]),
                "target_deeply_affordable_homes": count(data[5]),
                "target_homes_for_homeless_households": count(data[6]),
            }
    return rows, totals


def growth_rates(pdf: Path) -> list[dict]:
    text = "\n".join(pdf_text(pdf, 29, 29))
    return [
        {"community_district": f"{boro} {num}", "annual_growth_rate_2016_2025_pct": rate}
        for boro, num, rate in GROWTH_RATE.findall(text)
    ]


def write(rows: list[dict], filename: str) -> None:
    with open(SEEDS_DIR / filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{filename}: {len(rows)} rows")


if __name__ == "__main__":
    pdf = Path(sys.argv[1])
    district_targets, borough_totals = targets(pdf)
    inputs = target_inputs(pdf)
    rates = growth_rates(pdf)
    # the report covers the 59 community districts, no joint interest areas
    assert len(inputs) == 59 and all("total_homes_2025" in r for r in inputs), len(inputs)
    assert len(district_targets) == 59 and all(len(r) == 6 for r in district_targets), district_targets
    assert len(borough_totals) == 5, borough_totals
    assert len(rates) == 59 and len({r["community_district"] for r in rates}) == 59, len(rates)
    SEEDS_DIR.mkdir(exist_ok=True)
    write(district_targets, "fhgs_2030_targets.csv")
    write(borough_totals, "fhgs_2030_borough_totals.csv")
    write(inputs, "fhgs_2030_target_inputs.csv")
    write(rates, "fhgs_growth_rates_2016_2025.csv")

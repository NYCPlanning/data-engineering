"""Extract the tables in NYC's 2026 True Cost of Living (TCOL) report into seeds/.

Reads the report's text layer with pdftotext (poppler). scripts/download.py downloads the PDF:

    python scripts/extract_tcol.py data/sources/tcol_report_2026.pdf

Writes seven seeds, each a table or two from the report, as printed:
- tcol_family_types.csv: median costs and resources (Table 2), and rates and gaps (Table 6), by family type
- tcol_budget_components.csv: cost and resource components for three broad family types (Tables 3 and 13)
- tcol_disability_costs.csv: median costs by age group and disability (Table 4)
- tcol_group_rates.csv: rates and gaps by age, broad family type, race and ethnicity, and special family type
  (Tables 5, 7, and 8)
- tcol_borough_rates.csv: rates and gaps by borough (Table 9)
- tcol_borough_race_rates.csv: rates by borough and race and ethnicity (Table 10)
- tcol_resource_distribution.csv: people by resources relative to their threshold (Table 11)

Percentages are written on a 0-100 scale, and suppressed or n.a. values as empty.
"""

import csv
import re
import subprocess
import sys
from pathlib import Path

SEEDS_DIR = Path(__file__).parent.parent / "seeds"

MONEY = r"\$[\d,]+"
PERCENT = r"[\d.]+%"
VALUE = rf"(?:{MONEY}|{PERCENT}|\*|n\.a\.)"
ROW = re.compile(rf"^\s*(?P<label>\S.*?)\s{{2,}}(?P<values>{VALUE}(?:\s+{VALUE})*)\s*$")

BOROUGHS = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island", "NYC Overall"]
BROAD_FAMILY_TYPES = [
    "Adults under 65, no children",
    "Adults under 65, with children",
    "Adults 65 and older",
]

# Table 10 is an image in the PDF, so its values were typed from it. Its NYC Overall row is checked against
# Table 7's text below.
BOROUGH_RACE_RATES = {
    "Bronx": {"Hispanic": 81.8, "Black": 69.7, "Asian/PI": 72.0, "White": 46.5},
    "Brooklyn": {"Hispanic": 75.1, "Black": 64.8, "Asian/PI": 64.7, "White": 50.3},
    "Manhattan": {"Hispanic": 85.3, "Black": 79.7, "Asian/PI": 54.5, "White": 32.9},
    "Queens": {"Hispanic": 71.1, "Black": 56.2, "Asian/PI": 65.3, "White": 48.4},
    "Staten Island": {"Hispanic": 63.9, "Black": 62.8, "Asian/PI": 55.2, "White": 39.6},
    "NYC Overall": {"Hispanic": 77.6, "Black": 65.6, "Asian/PI": 63.3, "White": 43.7},
}
# Table 7's names for the same groups
TABLE_7_RACES = {
    "Hispanic": "Hispanic People",
    "Black": "Black People",
    "Asian/PI": "Asian and Pacific Islanders",
    "White": "White People",
}


def number(value: str) -> int | float | None:
    """Dollars as whole numbers, percentages as written."""
    if value in ("*", "n.a."):
        return None
    if value.startswith("$"):
        return int(value.replace("$", "").replace(",", ""))
    return float(value.replace("%", ""))


def table_rows(text: str, title: str) -> list[tuple[str, list[str]]]:
    """Label and values of each row of the first table titled `title`, up to its Source line."""
    start = text.index(title)
    end = text.index("Source:", start)
    rows = []
    for line in text[start:end].splitlines()[1:]:
        match = ROW.match(line)
        if match:
            rows.append((match["label"].strip(), match["values"].split()))
    return rows


def family_type(label: str, adult_age_group: str) -> dict:
    """Family type attributes from a label like 'Two+ Adults, 1 Child' or 'Three+ Adults'."""
    adults_label, _, children_label = label.partition(", ")
    adults = {
        "One Adult": "1",
        "Two Adults": "2",
        "Two+ Adults": "2+",
        "Three+ Adults": "3+",
    }[adults_label]
    children = {
        "": None,
        "No Children": "0",
        "1 Child": "1",
        "2 Children": "2",
        "3+ Children": "3+",
        "1+ Children": "1+",
    }[children_label]
    return {
        "tcol_family_type_id": re.sub(
            r"\W+", "_", f"{adult_age_group} {label}".lower().replace("+", " plus")
        ).strip("_"),
        "adult_age_group": adult_age_group,
        "family_type": label,
        "adults": adults,
        "children": children,
        # under 65, the 1+ children rows total the 1, 2, and 3+ children rows above them. Over 65 there are no
        # rows by number of children, so the 1+ rows are the only ones.
        "is_rollup": children == "1+" and adult_age_group == "Under 65",
    }


def family_types(text: str) -> list[dict]:
    costs = table_rows(
        text,
        "Table 2. Median Costs and Resources of True Cost of Living by Family Type",
    )
    rates = table_rows(
        text, "Table 6. TCOL Resource Gap Rates and Average Gaps by Family Type"
    )
    assert len(costs) == len(rates) == 15, (len(costs), len(rates))
    rows = []
    for i, ((cost_label, cost_values), (label, rate_values)) in enumerate(
        zip(costs, rates)
    ):
        # Table 2 has Two Adults where Table 6 has Two+ Adults for the under-65 row without children, between rows
        # both tables call Two+ Adults, so they're taken as the same group, with Table 6's label
        assert cost_label == label or (cost_label, label) == (
            "Two Adults, No Children",
            "Two+ Adults, No Children",
        ), (
            cost_label,
            label,
        )
        # each table lists the 10 families with adults under 65 first, then the 5 with adults 65 and older
        adult_age_group = "Under 65" if i < 10 else "65 and older"
        rows.append(
            {
                **family_type(label, adult_age_group),
                "median_cost": number(cost_values[0]),
                "median_resources": number(cost_values[1]),
                "pct_not_meeting": number(rate_values[0]),
                "avg_resource_gap": number(rate_values[1]),
            }
        )
    return rows


def budget_components(text: str) -> list[dict]:
    components = table_rows(
        text, "Table 3. Median Costs and Resources by Family Type, New York City, 2022"
    )
    # Table 13 wraps its family type labels, leaving some rows' numbers on a line of their own, so its rows are the
    # lines with four amounts, in order
    start = text.index("Table 13. Median Costs and Resources by Family Type")
    totals = [
        [number(v) for v in re.findall(MONEY, line)]
        for line in text[start : text.index("Source:", start)].splitlines()
        if len(re.findall(MONEY, line)) == 4
    ]
    assert len(totals) == 3, totals
    rows = []
    for label, values in components:
        assert len(values) == 3, (label, values)
        component = label.rstrip("*")
        kind = "resource" if "Resources" in component else "cost"
        rows += [
            {
                "broad_family_type": family,
                "component": component,
                "component_kind": kind,
                "median_amount": number(v),
            }
            for family, v in zip(BROAD_FAMILY_TYPES, values)
        ]
    by_key = {
        (r["broad_family_type"], r["component"]): r["median_amount"] for r in rows
    }
    for family, (cost, total, market, non_market) in zip(BROAD_FAMILY_TYPES, totals):
        assert (
            by_key[(family, "Total Cost")],
            by_key[(family, "Total Resources")],
        ) == (cost, total), family
        assert by_key[(family, "Market Resources")] == market, family
        rows.append(
            {
                "broad_family_type": family,
                "component": "Non-Market Resources",
                "component_kind": "resource",
                "median_amount": non_market,
            }
        )
    return rows


def disability_costs(text: str) -> list[dict]:
    rows = []
    for label, values in table_rows(
        text, "Table 4. Median Costs and Resources for Four Special Family Types"
    ):
        for status, value in zip(
            ["No disability", "Any disability", "Self-care disability"], values
        ):
            rows.append(
                {
                    "adult_age_group": label,
                    "disability": status,
                    "median_cost": number(value),
                }
            )
    assert len(rows) == 6, rows
    return rows


def group_rates(text: str) -> list[dict]:
    rows = []
    dimension = "All"
    for label, values in table_rows(
        text, "Table 5. TCOL Resource Gap Rates and Average Resource Gap by Age"
    ):
        rows.append(
            {
                "dimension": dimension,
                "group": label,
                "pct_not_meeting": number(values[0]),
                "avg_resource_gap": number(values[1]),
            }
        )
        dimension = "Age" if len(rows) < 4 else "Broad family type"
    for label, values in table_rows(
        text, "Table 7. TCOL Resource Gap Rates and Average Resource Gap by Race"
    ):
        if label != "All Persons":
            rows.append(
                {
                    "dimension": "Race and ethnicity",
                    "group": label,
                    "pct_not_meeting": number(values[0]),
                    "avg_resource_gap": number(values[1]),
                }
            )
    special = dict(
        table_rows(
            text, "Table 8. TCOL Resource Gap Rates for Four Special Family Types"
        )
    )
    # Table 8's column headers span two lines, so they're listed here, left to right
    columns = [
        "Three-generation families",
        "Families with children and student debt",
        "Any disability, adults under 65",
        "Any disability, adults 65 and older",
        "Self-care disability, adults under 65",
        "Self-care disability, adults 65 and older",
    ]
    for group, rate, gap in zip(
        columns, special["% Not Meeting TCOL"], special["Avg Resource Gap"]
    ):
        rows.append(
            {
                "dimension": "Special family type",
                "group": group,
                "pct_not_meeting": number(rate),
                "avg_resource_gap": number(gap),
            }
        )
    assert len(rows) == 7 + 5 + 6, len(rows)
    return rows


def borough_rates(text: str) -> list[dict]:
    rows = [
        {
            "borough": label,
            "pct_not_meeting": number(values[0]),
            "pct_adults_no_children_not_meeting": number(values[1]),
            "pct_families_with_children_not_meeting": number(values[2]),
            "avg_resource_gap": number(values[3]),
        }
        for label, values in table_rows(
            text, "Table 9. TCOL Resource Gap Rates by Borough"
        )
    ]
    assert sorted(r["borough"] for r in rows) == sorted(BOROUGHS), rows
    return rows


def borough_race_rates(text: str) -> list[dict]:
    citywide = {
        label: number(values[0])
        for label, values in table_rows(text, "Table 7. TCOL Resource Gap Rates")
    }
    for race, table_7_label in TABLE_7_RACES.items():
        assert BOROUGH_RACE_RATES["NYC Overall"][race] == citywide[table_7_label], race
    return [
        {"borough": borough, "race_ethnicity": race, "pct_not_meeting": rate}
        for borough, rates in BOROUGH_RACE_RATES.items()
        for race, rate in rates.items()
    ]


def resource_distribution(text: str) -> list[dict]:
    rows = [
        {
            "resource_band": label,
            "pct_of_nyc": number(values[0]),
            "avg_resources_pct_of_threshold": number(values[1]),
            "avg_resource_gap": number(values[2]),
            "median_cost": number(values[3]),
            "median_resources": number(values[4]),
        }
        for label, values in table_rows(
            text, "Table 11. Distributional Analysis of TCOL Resource Gaps"
        )
    ]
    assert len(rows) == 6, rows
    return rows


def main(pdf: Path) -> None:
    text = subprocess.run(
        ["pdftotext", "-layout", str(pdf), "-"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    seeds = {
        "tcol_family_types.csv": family_types(text),
        "tcol_budget_components.csv": budget_components(text),
        "tcol_disability_costs.csv": disability_costs(text),
        "tcol_group_rates.csv": group_rates(text),
        "tcol_borough_rates.csv": borough_rates(text),
        "tcol_borough_race_rates.csv": borough_race_rates(text),
        "tcol_resource_distribution.csv": resource_distribution(text),
    }
    for filename, rows in seeds.items():
        with open(SEEDS_DIR / filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        print(f"wrote {len(rows)} rows to seeds/{filename}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))

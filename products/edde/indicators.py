"""Loader for indicators.csv - per-table year band configuration for EDDE site config.

indicators.csv is keyed by data_table (e.g. "2.02") and has up to three year "bands" -
earliest/middle/current - each with a start_year and (optional) end_year. A blank
end_year means the band is a single year rather than a range; it's treated as equal
to start_year. Which bands are populated varies by table: some have all three
(e.g. a census point + two ACS windows), some have only two (old/new ACS), some
only one (a single-year snapshot). See products/edde/solutions.md and the "great
callout" thread that led here for the full convention.
"""

import csv
from dataclasses import dataclass
from pathlib import Path

INDICATORS_CSV_PATH = Path(__file__).parent / "indicators.csv"


@dataclass(frozen=True)
class YearBand:
    start_year: str | None
    end_year: str | None

    @property
    def is_populated(self) -> bool:
        return self.start_year is not None

    @property
    def range(self) -> str:
        """e.g. '2008-2012' for a range, or '2020' for a single year. Empty string if unpopulated."""
        if self.start_year is None:
            return ""
        if self.end_year and self.end_year != self.start_year:
            return f"{self.start_year}-{self.end_year}"
        return self.start_year

    @property
    def end(self) -> str:
        """The end year (or the only year, for a single-year band). Empty string if unpopulated."""
        if self.start_year is None:
            return ""
        return self.end_year or self.start_year


@dataclass(frozen=True)
class IndicatorYears:
    data_table: str
    indicator_name: str
    earliest: YearBand
    middle: YearBand
    current: YearBand
    notes: str

    # Convenience flat accessors for Jinja templates, which can't easily reach
    # into nested attributes with a dot in the outer key (e.g. `indicators["2.02"]`
    # is fine, but this avoids `.earliest.range` chains in every template string).
    @property
    def earliest_range(self) -> str:
        return self.earliest.range

    @property
    def earliest_end(self) -> str:
        return self.earliest.end

    @property
    def middle_range(self) -> str:
        return self.middle.range

    @property
    def middle_end(self) -> str:
        return self.middle.end

    @property
    def current_range(self) -> str:
        return self.current.range

    @property
    def current_end(self) -> str:
        return self.current.end


def _band(row: dict, prefix: str) -> YearBand:
    start = row.get(f"{prefix}_start_year") or None
    end = row.get(f"{prefix}_end_year") or None
    return YearBand(start_year=start, end_year=end)


def load_indicators(path: Path = INDICATORS_CSV_PATH) -> dict[str, IndicatorYears]:
    """Load indicators.csv, keyed by data_table (e.g. '2.02').

    A few table IDs (2.05, 2.06) appear on more than one row, one per sub-indicator
    shown on that table's page (e.g. "Occupation" and "Median Wages by Occupation").
    Both rows for a given table always carry the same year bands - the split exists
    only because it maps to the source tracker's per-indicator rows, not because the
    years differ - so the first row for a table id wins and later duplicates are
    skipped.
    """
    result: dict[str, IndicatorYears] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            table = row["data_table"]
            if table in result:
                continue
            result[table] = IndicatorYears(
                data_table=table,
                indicator_name=row["indicator_name"],
                earliest=_band(row, "earliest"),
                middle=_band(row, "middle"),
                current=_band(row, "current"),
                notes=row.get("notes", ""),
            )
    return result

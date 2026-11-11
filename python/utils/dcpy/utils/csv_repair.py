import re
from typing import Callable, Literal

from pydantic import BaseModel

from dcpy.utils.logging import logger

# Checks a template can assign to an anchor column. Blank passes every check except
# `borough`, since the anchors are only meant to catch values shifted out of place.
AnchorCheck = Literal[
    "borough", "digits", "number", "iso_date", "state_code", "non_numeric"
]
_ANCHOR_PATTERNS: dict[str, re.Pattern] = {
    "borough": re.compile(r"(?i)MANHATTAN|BROOKLYN|BRONX|QUEENS|STATEN ISLAND"),
    "digits": re.compile(r"\d*"),
    "number": re.compile(r"(-?\d*\.?\d+)?"),
    "iso_date": re.compile(r"(\d{4}-\d{2}-\d{2})?"),
    "state_code": re.compile(r"([A-Za-z]{2})?"),
    "non_numeric": re.compile(r"(?!\d+$).*"),
}


class UnescapedDelimiterRepair(BaseModel, extra="forbid"):
    """Repair rows split by a delimiter typed, unescaped, into a free-text field.

    A row with extra fields is rebuilt by merging the extra fields back into one of
    `free_text_columns`. A merge is kept only if every `anchor_columns` value then
    passes its check. Rows that can't be repaired with confidence are dropped
    with a warning rather than loaded misaligned.
    """

    free_text_columns: list[str]
    anchor_columns: dict[str, AnchorCheck]
    # Adjacent free-text columns (e.g. a business name next to a street name) can both
    # produce a valid merge, and either choice leaves every later column correct. Valid
    # merges farther apart than this mean we can't tell where the delimiter belongs.
    max_candidate_spread: int = 5


def make_bad_line_handler(
    header: list[str], repair: UnescapedDelimiterRepair, delimiter: str
) -> Callable[[list[str]], list[str] | None]:
    """Build an `on_bad_lines` callable for `pd.read_csv(engine="python")`."""
    missing = [
        c
        for c in [*repair.free_text_columns, *repair.anchor_columns]
        if c not in header
    ]
    if missing:
        raise ValueError(
            f"Columns configured for repair aren't in the header: {missing}"
        )

    n_expected = len(header)
    candidates = sorted(header.index(c) for c in repair.free_text_columns)
    anchors = [
        (header.index(c), _ANCHOR_PATTERNS[check])
        for c, check in repair.anchor_columns.items()
    ]

    def is_plausible(row: list[str]) -> bool:
        return all(p.fullmatch(row[i]) for i, p in anchors)

    def handler(bad_line: list[str]) -> list[str] | None:
        row_id = bad_line[0] if bad_line else "unknown"
        extra = len(bad_line) - n_expected
        passing = []
        for i in candidates:
            end = i + extra + 1
            if extra <= 0 or end > len(bad_line):
                continue
            row = bad_line[:i] + [delimiter.join(bad_line[i:end])] + bad_line[end:]
            if is_plausible(row):
                passing.append((i, row))

        if not passing:
            logger.warning(
                f"Dropping row {row_id!r} with {len(bad_line)} fields: "
                "no free-text column merge produced a valid row"
            )
            return None
        positions = [i for i, _ in passing]
        if max(positions) - min(positions) > repair.max_candidate_spread:
            logger.warning(
                f"Dropping row {row_id!r} with {len(bad_line)} fields: valid merges "
                f"into unrelated columns {[header[i] for i in positions]}"
            )
            return None
        i, row = passing[0]
        logger.warning(f"Repaired row {row_id!r} by merging fields into {header[i]!r}")
        return row

    return handler

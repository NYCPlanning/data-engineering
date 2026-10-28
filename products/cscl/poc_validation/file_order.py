"""Record order of one of our flat files against prod's file of the same name.

The line diffs (validate_outputs.sh, run_validation.py) sort both files first, so they
can't see order. GSS's loader reads the flat files in order and picks values by position,
so order is part of what has to match. This compares order only: content differences
are the line diff's job.

Rows are matched by their full line. A line that appears k times on a side is matched
occurrence by occurrence (the first copy in ours with the first copy in prod's, and so
on); identical lines are interchangeable, so that never adds a departure. Rows only one
side has are left out. On the matched rows:

- rows_out_of_order: the fewest matched rows that would have to be moved for ours to
  be in prod's order. That's the matched rows minus the longest run of them (not
  necessarily adjacent) that is already in prod's order: the longest increasing
  subsequence of prod's line positions taken in our order. 0 means the same order.
- the first departure: walking both files' matched rows in step, the first place where
  the next row differs, as a line number (1-based, in the file as written) and the
  line on each side.

Runs in well under a second on a LION borough file (~75k lines).

As a command (validate_outputs.sh calls it once per file):
    python poc_validation/file_order.py <ours> <prod> [--details <file_order.csv>]
prints the summary columns (SUMMARY_COLUMNS) as one CSV row, and with --details appends
the full result, both lines included, to that CSV.
"""

import argparse
import bisect
import csv
import io
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

# The columns validate_outputs.sh and run_validation.py add to validation_summary.csv
SUMMARY_COLUMNS = [
    "order_rows_compared",
    "rows_out_of_order",
    "order_matches_prod",
    "first_order_departure_prod_line",
    "first_order_departure_dev_line",
]
DETAIL_COLUMNS = (
    ["filename"]
    + SUMMARY_COLUMNS
    + [
        "first_order_departure_prod_text",
        "first_order_departure_dev_text",
    ]
)


@dataclass
class OrderComparison:
    order_rows_compared: int
    rows_out_of_order: int
    order_matches_prod: bool
    first_order_departure_prod_line: int | None = None
    first_order_departure_dev_line: int | None = None
    first_order_departure_prod_text: str | None = None
    first_order_departure_dev_text: str | None = None


def lines_from_bytes(raw: bytes) -> list[tuple[int, str]]:
    """(line number, line) for each non-blank line. Line numbers count every line as
    written, so they point into the file. CR and the DOS end-of-file marker (0x1A,
    which ends prod's nyc.thinlion) aren't part of a record."""
    text = raw.decode("latin-1")
    return [
        (number, line)
        for number, line in enumerate(
            (line.rstrip("\r") for line in text.split("\n")), start=1
        )
        if line.strip("\x1a ")
    ]


def read_lines(path: Path) -> list[tuple[int, str]]:
    return lines_from_bytes(Path(path).read_bytes())


def _with_occurrence(lines: list[tuple[int, str]]) -> list[tuple[str, int]]:
    seen: dict[str, int] = {}
    keyed = []
    for _, line in lines:
        seen[line] = seen.get(line, 0) + 1
        keyed.append((line, seen[line]))
    return keyed


def _longest_increasing_subsequence(values: list[int]) -> int:
    tails: list[int] = []
    for value in values:
        i = bisect.bisect_left(tails, value)
        if i == len(tails):
            tails.append(value)
        else:
            tails[i] = value
    return len(tails)


def compare(prod: list[tuple[int, str]], dev: list[tuple[int, str]]) -> OrderComparison:
    """Order of `dev` (ours) against `prod`, each a file's (line number, line) list."""
    prod_keys = _with_occurrence(prod)
    dev_keys = _with_occurrence(dev)
    dev_keyset = set(dev_keys)
    prod_index = {key: i for i, key in enumerate(prod_keys) if key in dev_keyset}
    # matched rows, each side in its own order, as indexes into its own list
    prod_matched = [i for i, key in enumerate(prod_keys) if key in prod_index]
    dev_matched = [i for i, key in enumerate(dev_keys) if key in prod_index]
    in_order = _longest_increasing_subsequence(
        [prod_index[dev_keys[i]] for i in dev_matched]
    )
    result = OrderComparison(
        order_rows_compared=len(dev_matched),
        rows_out_of_order=len(dev_matched) - in_order,
        order_matches_prod=in_order == len(dev_matched),
    )
    if not result.order_matches_prod:
        p, d = next(
            (p, d)
            for p, d in zip(prod_matched, dev_matched)
            if prod_keys[p] != dev_keys[d]
        )
        result.first_order_departure_prod_line = prod[p][0]
        result.first_order_departure_dev_line = dev[d][0]
        result.first_order_departure_prod_text = prod[p][1]
        result.first_order_departure_dev_text = dev[d][1]
    return result


def summary_values(result: OrderComparison) -> list:
    return [
        ""
        if value is None
        else str(value).lower()
        if isinstance(value, bool)
        else value
        for value in (getattr(result, col) for col in SUMMARY_COLUMNS)
    ]


def append_details(path: Path, filename: str, result: OrderComparison) -> None:
    """Append one file's full result to a file_order.csv, writing the header first if
    the file is new."""
    new = not path.exists()
    with path.open("a", newline="", encoding="latin-1") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_COLUMNS)
        if new:
            writer.writeheader()
        row = {k: v for k, v in asdict(result).items()}
        row["order_matches_prod"] = str(result.order_matches_prod).lower()
        writer.writerow({"filename": filename, **row})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("dev", type=Path, help="our file")
    parser.add_argument("prod", type=Path, help="prod's file")
    parser.add_argument("--details", type=Path, help="file_order.csv to append to")
    args = parser.parse_args()
    result = compare(read_lines(args.prod), read_lines(args.dev))
    if args.details:
        append_details(args.details, args.dev.name, result)
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(summary_values(result))
    sys.stdout.write(out.getvalue() + "\n")


if __name__ == "__main__":
    main()

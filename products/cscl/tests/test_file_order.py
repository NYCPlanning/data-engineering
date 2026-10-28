"""file_order: our flat file's record order against prod's."""

import csv
import random
import subprocess
import sys
from pathlib import Path

POC = Path(__file__).parent.parent / "poc_validation"
sys.path.insert(0, str(POC))

import file_order as fo  # noqa: E402


def _numbered(lines: list[str]) -> list[tuple[int, str]]:
    return list(enumerate(lines, start=1))


PROD = [f"record {i:03d}" for i in range(200)]


def test_identical_file_matches():
    result = fo.compare(_numbered(PROD), _numbered(PROD))
    assert result.order_rows_compared == 200
    assert result.rows_out_of_order == 0
    assert result.order_matches_prod
    assert result.first_order_departure_prod_line is None


def test_shuffled_file_is_flagged():
    shuffled = PROD[:]
    random.Random(1).shuffle(shuffled)
    result = fo.compare(_numbered(PROD), _numbered(shuffled))
    assert result.order_rows_compared == 200
    assert not result.order_matches_prod
    # a random order keeps only a short in-order run (~2*sqrt(n))
    assert result.rows_out_of_order > 150
    first = next(i for i, (a, b) in enumerate(zip(PROD, shuffled)) if a != b)
    assert result.first_order_departure_prod_line == first + 1
    assert result.first_order_departure_dev_text == shuffled[first]


def test_one_moved_row_counts_one():
    # moving one row to the end shifts every row after it, but only one row is out of order
    moved = PROD[:10] + PROD[11:] + [PROD[10]]
    result = fo.compare(_numbered(PROD), _numbered(moved))
    assert result.rows_out_of_order == 1
    assert (
        result.first_order_departure_prod_line,
        result.first_order_departure_dev_line,
    ) == (11, 11)
    assert result.first_order_departure_prod_text == "record 010"
    assert result.first_order_departure_dev_text == "record 011"


def test_rows_only_on_one_side_are_left_out():
    dev = ["new row"] + PROD[:50] + PROD[60:]
    result = fo.compare(_numbered(PROD), _numbered(dev))
    assert result.order_rows_compared == 190
    assert result.order_matches_prod


def test_duplicate_lines_match_by_occurrence():
    prod = ["a", "b", "a", "c"]
    assert fo.compare(
        _numbered(prod), _numbered(["a", "b", "a", "c"])
    ).order_matches_prod
    result = fo.compare(_numbered(prod), _numbered(["a", "a", "b", "c"]))
    assert result.rows_out_of_order == 1
    assert result.order_rows_compared == 4


def test_line_numbers_point_into_the_file():
    # blank lines and prod's trailing end-of-file marker aren't records, but still count
    # as lines
    raw = b"x\r\n\r\ny\r\nz\r\n\x1a"
    assert fo.lines_from_bytes(raw) == [(1, "x"), (3, "y"), (4, "z")]


def test_command_prints_summary_and_appends_details(tmp_path):
    prod = tmp_path / "prod" / "F.txt"
    dev = tmp_path / "dev" / "F.txt"
    prod.parent.mkdir()
    dev.parent.mkdir()
    prod.write_bytes(b"a\r\nb\r\nc\r\n")
    dev.write_bytes(b"a\r\nc\r\nb\r\n")
    details = tmp_path / "file_order.csv"
    for _ in range(2):
        out = subprocess.run(
            [sys.executable, str(POC / "file_order.py"), str(dev), str(prod)]
            + ["--details", str(details)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    assert out.strip() == "3,1,false,2,2"
    rows = list(csv.DictReader(details.open()))
    assert len(rows) == 2
    assert rows[0]["filename"] == "F.txt"
    assert rows[0]["first_order_departure_prod_text"] == "b"
    assert rows[0]["first_order_departure_dev_text"] == "c"

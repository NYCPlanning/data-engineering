"""Fixed-width (DAT) <-> parquet, checked against real CSCL LION output.

resources/fixed_width/lion_dat_sample.parquet holds 40 unformatted int__lion rows
(8 per borough) and lion_dat_sample.dat the exact lines the build wrote for them
(LION .dat, 400 characters, CRLF). lion_dat_layout.csv is CSCL's
text_formatting__lion_dat seed.
"""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from dcpy.utils.fixed_width import (
    Field,
    dat_to_parquet,
    dat_to_table,
    format_value,
    load_layout,
    parquet_to_dat,
)

RESOURCES = Path(__file__).parent / "resources" / "fixed_width"
LAYOUT = load_layout(RESOURCES / "lion_dat_layout.csv")


def test_layout_loads():
    assert len(LAYOUT) == 114
    assert sum(f.length for f in LAYOUT) == 400
    lgc2 = next(f for f in LAYOUT if f.name == "lgc2")
    assert (lgc2.length, lgc2.left_justified, lgc2.fill, lgc2.blank_if_none) == (
        2,
        False,
        "0",
        True,
    )


@pytest.mark.parametrize(
    "value, field, expected",
    [
        (42, Field("n", 5, fill="0"), "00042"),
        ("ab", Field("s", 4, left_justified=True), "ab  "),
        (None, Field("n", 3, fill="0"), "000"),
        (None, Field("n", 3, fill="0", blank_if_none=True), "   "),
        ("abcdef", Field("s", 3), "abc"),  # truncated, keeping the left
        ("x", Field("filler_1", 2, fill="0"), "00"),
    ],
)
def test_format_value(value, field, expected):
    assert format_value(value, field) == expected


def test_format_value_rejects_floats():
    with pytest.raises(TypeError):
        format_value(8.0, Field("n", 3))


def test_parquet_to_dat_matches_build_output(tmp_path):
    out = tmp_path / "lion.dat"
    n = parquet_to_dat(RESOURCES / "lion_dat_sample.parquet", LAYOUT, out)
    assert n == 40
    assert out.read_bytes() == (RESOURCES / "lion_dat_sample.dat").read_bytes()


def test_dat_round_trip(tmp_path):
    table = dat_to_table(RESOURCES / "lion_dat_sample.dat", LAYOUT)
    assert table.num_rows == 40
    assert not any(name.startswith("filler_") for name in table.column_names)
    # formatted values come back verbatim, so formatting them again changes nothing
    pq.write_table(table, tmp_path / "formatted.parquet")
    parquet_to_dat(tmp_path / "formatted.parquet", LAYOUT, tmp_path / "again.dat")
    assert (tmp_path / "again.dat").read_bytes() == (
        RESOURCES / "lion_dat_sample.dat"
    ).read_bytes()


def test_dat_to_parquet_strip(tmp_path):
    n = dat_to_parquet(
        RESOURCES / "lion_dat_sample.dat", LAYOUT, tmp_path / "x.parquet", strip=True
    )
    assert n == 40
    first = next(f.name for f in LAYOUT if not f.is_filler)
    values = pq.read_table(tmp_path / "x.parquet").column(first).to_pylist()
    assert all(v == v.strip() for v in values)


def test_wrong_width_raises(tmp_path):
    (tmp_path / "bad.dat").write_text("too short\r\n")
    with pytest.raises(ValueError, match="characters"):
        dat_to_table(tmp_path / "bad.dat", LAYOUT)

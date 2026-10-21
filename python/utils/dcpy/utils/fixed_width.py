"""Fixed-width text files (DAT) to and from Parquet, given a column layout.

A layout is an ordered list of fields, each with a width, justification, fill character,
and blank-if-null flag: the same spec as CSCL's text_formatting__* seeds (field_name,
field_length, justify_and_fill, blank_if_none). A row is its formatted fields
concatenated in layout order.

Formatting a value (``format_value``):
- NULL becomes '' (and, if ``blank_if_none``, the fill becomes a space);
- the value's text is padded to the width: right-justified pads on the left,
  left-justified pads on the right;
- text longer than the width is truncated, keeping its leftmost characters;
- ``filler_*`` fields carry no value, just the fill.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FILLER_PREFIX = "filler_"


@dataclass(frozen=True)
class Field:
    name: str
    length: int
    left_justified: bool = False
    fill: str = " "
    blank_if_none: bool = False

    @property
    def is_filler(self) -> bool:
        return self.name.startswith(FILLER_PREFIX)


def load_layout(path: Path) -> list[Field]:
    """Read a layout CSV in the text_formatting seed format.

    ``justify_and_fill`` is a 4-character code: LJ (left-justified) or anything else
    (right-justified), then ZF (zero fill) or anything else (space fill).
    """
    with open(path, newline="") as f:
        return [
            Field(
                name=row["field_name"],
                length=int(row["field_length"]),
                left_justified=row["justify_and_fill"][:2] == "LJ",
                fill="0" if row["justify_and_fill"][-2:] == "ZF" else " ",
                blank_if_none=row["blank_if_none"].strip().upper() == "TRUE",
            )
            for row in csv.DictReader(f)
        ]


def _to_text(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, Decimal)):
        return str(value)
    if isinstance(value, float):
        # SQL casts render whole floats as e.g. '8.0' in DuckDB but '8' in Postgres;
        # refuse rather than guess, so a layout column is exported as text or integer
        raise TypeError(f"float values are ambiguous in fixed-width text: {value!r}")
    raise TypeError(f"unsupported value type for fixed-width text: {type(value)}")


def format_value(value: object, field: Field) -> str:
    fill = field.fill
    if field.is_filler:
        text = ""
    elif value is None:
        text = ""
        if field.blank_if_none:
            fill = " "
    else:
        text = _to_text(value)
    if len(text) >= field.length:
        return text[: field.length]
    padding = fill * (field.length - len(text))
    return text + padding if field.left_justified else padding + text


def format_row(row: Mapping[str, object], layout: list[Field]) -> str:
    return "".join(
        format_value(None if f.is_filler else row[f.name], f) for f in layout
    )


def format_rows(
    rows: Iterable[Mapping[str, object]], layout: list[Field]
) -> Iterator[str]:
    for row in rows:
        yield format_row(row, layout)


def parquet_to_dat(
    parquet_path: Path,
    layout: list[Field],
    dat_path: Path,
    *,
    line_ending: str = "\r\n",
    batch_size: int = 10_000,
) -> int:
    """Write each parquet row as one fixed-width line. Returns the number of lines.

    Rows are written in the parquet file's order.
    """
    missing = [
        f.name
        for f in layout
        if not f.is_filler and f.name not in pq.read_schema(parquet_path).names
    ]
    if missing:
        raise ValueError(f"{parquet_path} has no column(s) {missing} for the layout")
    n = 0
    dat_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dat_path, "w", newline="") as out:
        for batch in pq.ParquetFile(parquet_path).iter_batches(batch_size):
            for line in format_rows(batch.to_pylist(), layout):
                out.write(line + line_ending)
                n += 1
    return n


def dat_to_table(
    dat_path: Path, layout: list[Field], *, strip: bool = False
) -> pa.Table:
    """Read a fixed-width file into a table with one string column per layout field.

    Filler fields are dropped. With ``strip``, each value's surrounding spaces are
    removed. Fill and padding are otherwise kept: un-formatting isn't lossless
    (e.g. a zero-filled '00042' could have been 42 or '042').
    """
    width = sum(f.length for f in layout)
    columns: dict[str, list[str]] = {f.name: [] for f in layout if not f.is_filler}
    with open(dat_path, newline="") as f:
        for i, raw in enumerate(f, start=1):
            line = raw.rstrip("\r\n")
            if len(line) != width:
                raise ValueError(
                    f"{dat_path} line {i} is {len(line)} characters; the layout is {width}"
                )
            pos = 0
            for field in layout:
                value = line[pos : pos + field.length]
                pos += field.length
                if not field.is_filler:
                    columns[field.name].append(value.strip() if strip else value)
    return pa.table(columns)


def dat_to_parquet(
    dat_path: Path, layout: list[Field], parquet_path: Path, *, strip: bool = False
) -> int:
    """``dat_to_table`` written to parquet. Returns the number of rows."""
    table = dat_to_table(dat_path, layout, strip=strip)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, parquet_path)
    return table.num_rows

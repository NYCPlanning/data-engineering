import typing
from pathlib import Path

from pydantic import BaseModel


class Font(BaseModel):
    name: str | None = None
    size: float | None = None
    rgb: str | None = None
    italic: bool = False
    bold: bool = False
    monospaced: bool = False


class CellStyle(BaseModel):
    font: Font = Font()
    borders: list[str] | None = None
    text_alignment_vertical: str | None = None
    text_alignment_horizontal: str | None = None


class Image(BaseModel):
    path: Path


class Cell(BaseModel):
    value: typing.Any | Image | list["Cell"]  # can be a value or inline cells
    style: CellStyle = CellStyle()


class Row(BaseModel):
    cells: list[Cell]
    merge_cells: bool = False
    is_top_row: bool = False
    height: float | None = None
    skip_default_styling: bool = False


class Table(BaseModel):
    title: str
    subtitle: str | None = None
    description: str | None = None
    rows: list[Row]
    column_widths: list[float] | None = []

    def total_cols(self):
        return max(len(r.cells) for r in self.rows)

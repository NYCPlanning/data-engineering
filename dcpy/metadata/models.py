from pathlib import Path
from pydantic import BaseModel, conlist
from typing import Any, Literal
import yaml


class BytesDestination(BaseModel, extra="forbid"):
    type: Literal["bytes"]
    id: str
    datasets: list[str]


class SocrataDestination(BytesDestination, extra="forbid"):
    id: str
    type: Literal["socrata"]
    four_four: str
    attachments: list[str] = []
    datasets: conlist(item_type=str, max_length=1)


class Column(BaseModel, extra="forbid"):
    name: str
    display_name: str
    description: str
    data_type: str

    # Optional
    appears_in: Any | None = None
    data_source: str | None = None
    example: Any | None = None
    format_overrides: Any | None = {}
    is_nullable: bool = True
    is_primary_key: bool = False
    readme_data_type: str | None = None
    values: list[tuple] = []


class Package(BaseModel, extra="forbid"):
    datasets: list[str]
    attachments: list[str]


class Metadata(BaseModel, extra="forbid"):
    name: str
    display_name: str
    summary: str
    description: str
    tags: list[str]
    each_row_is_a: str

    destinations: list[BytesDestination | SocrataDestination]
    package: Package
    columns: list[Column]

    def from_yaml(path: Path):
        return Metadata(**yaml.safe_load(open(path, "rb")))

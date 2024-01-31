from pathlib import Path
from pydantic import BaseModel, conlist
from typing import Any, Literal
import yaml


class BytesDestination(BaseModel, extra="forbid"):
    type: Literal["bytes"]
    id: str
    datasets: list[str]


class SocrataDestination(BaseModel, extra="forbid"):
    id: str
    type: Literal["socrata"]
    four_four: str
    attachments: list[str] = []
    datasets: conlist(item_type=str, max_length=1)
    omit_columns: list[str]
    column_details: dict


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


class DatasetOverrides(BaseModel, extra="forbid"):
    omit_columns: list[str] = []
    columns: dict = {}


class Dataset(BaseModel, extra="forbid"):
    name: str
    type: str
    filename: str
    overrides: DatasetOverrides | None = None

    def get_columns(self):
        pass


class DatasetPackage(BaseModel, extra="forbid"):
    datasets: list[Dataset]
    attachments: list[str]

    def get_dataset(ds_id: str):
        pass


class Metadata(BaseModel, extra="forbid"):
    name: str
    display_name: str
    summary: str
    description: str
    tags: list[str]
    each_row_is_a: str

    destinations: list[BytesDestination | SocrataDestination]
    dataset_package: DatasetPackage
    columns: list[Column]

    def from_yaml(path: Path):
        return Metadata(**yaml.safe_load(open(path, "r")))

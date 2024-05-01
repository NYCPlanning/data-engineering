from __future__ import annotations
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
    type: Literal["socrata"] = "socrata"
    four_four: str
    attachments: list[str] = []
    datasets: conlist(item_type=str, max_length=1)  # type:ignore
    omit_columns: list[str]
    column_details: dict[str, SocrataColumn] = {}

    def get_column_overrides(self, col_name):
        col = self.column_details.get(col_name, SocrataColumn())
        # Unfortunate reality of serializing with pydantic from a yaml dictionary.
        # columns overrides aren't deserialized with their name value,
        # since the name is a dictionary key in the metadata. So:
        col.name = col_name
        return col

    def destination_column_metadata(self, metadata: Metadata) -> list[SocrataColumn]:
        soc_cols = []
        dataset = metadata.package.get_dataset(self.datasets[0])
        for col in dataset.get_columns(metadata):
            if col.name in self.omit_columns:
                continue
            overrides = self.get_column_overrides(col.name)

            soc_cols.append(
                SocrataColumn(
                    name=col.name,
                    api_name=overrides.api_name or col.name,
                    display_name=overrides.display_name or col.display_name,
                    description=overrides.description or col.description,
                )
            )
        return soc_cols


class Column(BaseModel, extra="forbid"):
    name: str
    display_name: str
    description: str
    data_type: str

    # Optional
    data_source: str | None = None
    example: Any | None = None
    non_nullable: bool | None = None
    is_primary_key: bool | None = None
    readme_data_type: str | None = None
    values: list[list] | None = None


class SocrataColumn(BaseModel, extra="forbid"):
    name: str | None = None
    api_name: str | None = None
    display_name: str | None = None
    description: str | None = None
    is_primary_key: bool = False


class DatasetOverrides(BaseModel, extra="forbid"):
    omit_columns: list[str] = []
    columns: dict = {}


class DatasetFile(BaseModel, extra="forbid"):
    name: str
    type: str
    filename: str
    overrides: DatasetOverrides = DatasetOverrides()

    def get_columns(self, metadata: Metadata) -> list[Column]:
        cols = []
        for col in metadata.columns:
            if col.name in self.overrides.omit_columns:
                continue
            new_col = col.model_dump()
            maybe_new_name = self.overrides.columns.get(col.name, {}).get("name")
            new_col["name"] = maybe_new_name or col.name
            new_col["display_name"] = maybe_new_name or col.name
            cols.append(Column(**new_col))
        return cols


class Package(BaseModel, extra="forbid"):
    dataset_files: list[DatasetFile]
    attachments: list[str]

    def get_dataset(self, ds_id: str) -> DatasetFile:
        ds = [d for d in self.dataset_files if d.name == ds_id]
        if len(ds) == 1:
            return ds[0]
        raise Exception(f"No dataset named {ds_id}")


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

    @staticmethod
    def from_yaml(path: Path):
        return Metadata(**yaml.safe_load(open(path, "r")))

    def get_destination(self, dest_id) -> BytesDestination | SocrataDestination:
        ds = [d for d in self.destinations if d.id == dest_id]
        if len(ds) == 1:
            return ds[0]
        raise Exception(f"No destination named {dest_id}")

    def validate(self):
        col_names = [c.name for c in self.columns]
        assert len(col_names) == len(
            set(col_names)
        ), "There should be no duplicate column names"
        # TODO: all the rest

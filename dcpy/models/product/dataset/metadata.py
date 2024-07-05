from __future__ import annotations
import jinja2
from pathlib import Path
from pydantic import BaseModel, conlist
from typing import Any, Literal
import yaml

from dcpy.utils.logging import logger

# Putting this here so we can have a constant value in all connectors
# and throw an exception when we attempt to deserialize files that contain it.
FILL_ME_IN_PLACEHOLDER = "<FILL ME IN!>"


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
    deprecated: bool | None = None
    values: list[list] | None = None


class SocrataColumn(BaseModel, extra="forbid"):
    name: str | None = None
    api_name: str | None = None
    display_name: str | None = None
    description: str | None = None
    is_primary_key: bool = False


class DatasetOverrides(BaseModel, extra="forbid"):
    omit_columns: list[str] = []
    ignore_validation: list[str] = []
    columns: dict = {}
    display_name: str | None = None
    description: str | None = None
    tags: list[str] = []
    destination_file_name: str | None = None


class RemoteFile(BaseModel, extra="forbid"):
    name: str
    url: str


class BytesDestination(BaseModel, extra="forbid"):
    type: Literal["bytes"]
    id: str
    files: list[RemoteFile]
    overrides: DatasetOverrides = DatasetOverrides()


DEFAULT_SOCRATA_CATEGORY = "city government"


class SocrataMetada(BaseModel, extra="forbid"):
    name: str
    description: str
    tags: list[str] = []
    metadata: dict[str, str] = {}
    # category: str = DEFAULT_SOCRATA_CATEGORY


class SocrataDestination(BaseModel, extra="forbid"):
    id: str
    type: Literal["socrata"] = "socrata"
    four_four: str
    attachments: list[str] = []
    datasets: conlist(item_type=str, max_length=1)  # type:ignore
    omit_columns: list[str] = []
    column_details: dict[str, SocrataColumn] = {}
    overrides: DatasetOverrides = DatasetOverrides()
    is_unparsed_dataset: bool = False

    def get_metadata(self, md: Metadata) -> SocrataMetada:
        dataset_file_overrides = md.package.get_dataset(self.datasets[0]).overrides

        # It would be nice to have a more comprehensive way to combine these overrides
        return SocrataMetada(
            name=self.overrides.display_name
            or dataset_file_overrides.display_name
            or md.display_name,
            description=self.overrides.description
            or dataset_file_overrides.description
            or md.description,
            tags=self.overrides.tags or dataset_file_overrides.tags or md.tags,
            metadata={"rowLabel": md.each_row_is_a},
        )

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


class File(BaseModel, extra="forbid"):
    name: str
    type: str
    filename: str


class DatasetFile(File, extra="forbid"):
    overrides: DatasetOverrides = DatasetOverrides()

    def get_columns(self, metadata: Metadata) -> list[Column]:
        cols = []
        for col in metadata.columns:
            if col.name in self.overrides.omit_columns:
                continue
            overrides = self.overrides.columns.get(col.name, {})
            new_col = col.model_dump()
            new_col.update(overrides)
            cols.append(Column(**new_col))
        return cols


class ZipFile(File, extra="forbid"):
    type: Literal["Zip"] = "Zip"
    contains: list[str]


class Package(BaseModel, extra="forbid"):
    dataset_files: list[DatasetFile]
    attachments: list[str]
    zip_files: list[ZipFile] = []

    def get_dataset(self, ds_id: str) -> DatasetFile:
        ds = [d for d in self.dataset_files if d.name == ds_id]
        if len(ds) == 1:
            return ds[0]
        raise Exception(f"No dataset named {ds_id}")

    def _files_by_name(self):
        return {ds.name: ds for ds in self.dataset_files + self.zip_files} | {
            ds: ds for ds in self.attachments
        }


class Metadata(BaseModel, extra="forbid"):
    name: str
    display_name: str
    summary: str  # TODO: potentially remove this field
    description: str
    tags: list[str]
    each_row_is_a: str

    destinations: list[BytesDestination | SocrataDestination]
    package: Package
    columns: list[Column]

    @staticmethod
    def from_yaml(path: Path, *, template_vars=None):
        with open(path, "r") as raw:
            logger.info(f"Templating the metadata with vars: {template_vars}")
            templated = jinja2.Template(
                raw.read(), undefined=jinja2.StrictUndefined
            ).render(template_vars or {})

            return Metadata(**yaml.safe_load(templated))  # type: ignore

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

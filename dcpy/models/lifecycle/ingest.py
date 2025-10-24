from __future__ import annotations
from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices, model_validator
from typing import Any

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase
from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES


class Source(BaseModel, extra="allow"):
    type: str
    key: str = Field(
        validation_alias=AliasChoices(
            "key", "path", "url", "endpoint", "name", "uid", "dataset", "product"
        )
    )
    _ds_id: str | None = None


class ProcessingStep(SortedSerializedBase):
    name: str
    args: dict[str, Any] = {}
    # mode allows for certain processing steps only to be run if specified at runtime
    mode: str | None = None


class DatasetAttributes(SortedSerializedBase):
    name: str | None = None
    description: str | None = None
    url: str | None = None
    custom: dict | None = None

    _head_sort_order = ["name", "description", "url"]


class ArchivalMetadata(SortedSerializedBase):
    archival_timestamp: datetime
    raw_filename: str
    acl: recipes.ValidAclValues | None = None


class ProcessingSummary(SortedSerializedBase):
    """Summary of the changes from a data processing function."""

    name: str
    description: str
    data_modifications: dict = {}
    column_modifications: dict = {}
    custom: dict = {}


class ProcessingResult(SortedSerializedBase, arbitrary_types_allowed=True):
    df: pd.DataFrame
    summary: ProcessingSummary


class Ingestion(SortedSerializedBase):
    target_crs: str | None = None
    source: Source
    file_format: file.Format
    processing_mode: str | None = None
    processing_steps: list[ProcessingStep] = []
    processing_steps_summaries: list[ProcessingSummary] = []


class Column(BaseColumn):
    _head_sort_order = ["id", "data_type", "description"]

    data_type: COLUMN_TYPES  # override BaseColumn `data_type` to be required field


class Template(BaseModel, extra="forbid"):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    id: str
    acl: recipes.ValidAclValues | None = None

    attributes: DatasetAttributes
    ingestion: Ingestion
    columns: list[Column] = []
    checks: list[str | dict[str, Any]] | None = None

    @property
    def has_geom(self):
        match self.ingestion.file_format:
            case file.Shapefile() | file.Geodatabase() | file.GeoJson():
                return True
            case file.Csv() | file.Excel() | file.Json() | file.Html() as format:
                return format.geometry is not None

    @model_validator(mode="after")
    def validate_objects(self):
        self.ingestion.source._ds_id = self.id


class Config(SortedSerializedBase, extra="forbid"):
    """
    Computed Template of ingest dataset
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    crs: str | None = None

    attributes: DatasetAttributes
    archival: ArchivalMetadata
    ingestion: Ingestion
    columns: list[Column] = []
    run_details: RunDetails

    _head_sort_order = [
        "id",
        "version",
        "crs",
        "attributes",
        "archival",
        "ingestion",
        "columns",
        "run_details",
    ]

    @property
    def dataset(self) -> recipes.Dataset:
        return recipes.Dataset(
            id=self.id, version=self.version, file_type=recipes.DatasetType.parquet
        )

    @property
    def dataset_key(self) -> recipes.DatasetKey:
        return recipes.DatasetKey(id=self.id, version=self.version)

    @property
    def filename(self) -> str:
        return f"{self.id}.parquet"

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(
            id=self.id,
            timestamp=self.archival.archival_timestamp,
            filename=self.archival.raw_filename,
        )


class SparseConfig(BaseModel, extra="allow"):
    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    run_timestamp: datetime | None = None

    @model_validator(mode="before")
    def _validate(cls, values: dict[str, Any]) -> dict[str, Any]:
        metadata_field = "run_details"
        # library datasets - main model is subfield, different metadata field
        if "dataset" in values:
            values = values["dataset"]
            metadata_field = "execution_details"
        # raw datasets - different version
        if "timestamp" in values:
            values["version"] = values["timestamp"]
        values["run_timestamp"] = values.get(metadata_field, {}).get("timestamp")
        return values

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field, AliasChoices
from typing import Any, Literal, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes, publishing
from dcpy.models.connectors import web, socrata
from dcpy.models import library, file
from dcpy.models.base import SortedSerializedBase


class LocalFileSource(BaseModel, extra="forbid"):
    type: Literal["local_file"]
    path: Path


class S3Source(BaseModel, extra="forbid"):
    type: Literal["s3"]
    bucket: str
    key: str


class ScriptSource(BaseModel, extra="forbid"):
    type: Literal["script"]
    connector: str
    function: str


Source: TypeAlias = (
    LocalFileSource
    | web.FileDownloadSource
    | web.GenericApiSource
    | socrata.Source
    | publishing.GisDataset
    | S3Source
    | ScriptSource
)


class PreprocessingStep(SortedSerializedBase):
    name: str
    args: dict[str, Any] = {}
    # mode allows for certain preprocessing steps only to be run if specified at runtime
    mode: str | None = None


class DatasetAttributes(SortedSerializedBase):
    name: str | None = None
    description: str | None = None
    url: str | None = None
    custom: dict | None = None

    _head_sort_order = ["name", "description", "url"]


class ArchivalMetadata(SortedSerializedBase):
    archival_timestamp: datetime
    check_timestamps: list[datetime] = []
    raw_filename: str
    acl: recipes.ValidAclValues


class Ingestion(SortedSerializedBase):
    target_crs: str | None = None
    source: Source
    file_format: file.Format
    processing_mode: str | None = None
    processing_steps: list[PreprocessingStep] = []


class Column(SortedSerializedBase):
    id: str
    data_type: Literal["text", "integer", "decimal", "geometry", "bool", "datetime"]
    description: str | None = None


class Template(BaseModel, extra="forbid"):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    id: str
    acl: recipes.ValidAclValues

    attributes: DatasetAttributes | None = None
    ingestion: Ingestion
    columns: list[Column] = []

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: library.DatasetDefinition | None = None


class Config(SortedSerializedBase, extra="forbid"):
    """
    Computed Template of ingest dataset
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    crs: str | None = None

    attributes: DatasetAttributes | None = None
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
            id=self.id, timestamp=self.archival.archival_timestamp
        )

    @property
    def freshness(self) -> datetime:
        return (
            self.archival.archival_timestamp
            if not self.archival.check_timestamps
            else max(self.archival.check_timestamps)
        )

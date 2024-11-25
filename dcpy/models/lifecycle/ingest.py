from __future__ import annotations
from abc import ABC
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field, AliasChoices
from typing import Any, Literal, Annotated

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase

from pydantic import PlainValidator, ValidationError


class Source(BaseModel, ABC):
    type: str

    def download(self, version: str, output_dir: Path):
        raise NotImplementedError

    def filename(self):
        raise NotImplementedError

    def version(self, timestamp: datetime):
        raise NotImplementedError


def _validate_source(obj) -> Source:
    if isinstance(obj, Source):
        return obj
    errors = []
    for c in Source.__subclasses__():
        print(obj)
        print(c)
        try:
            return c(**obj)
        except ValidationError as e:
            errors.append(str(e))
    raise ValidationError(errors)


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
    check_timestamps: list[datetime] = []
    raw_filename: str
    acl: recipes.ValidAclValues


class Ingestion(SortedSerializedBase):
    target_crs: str | None = None
    source: Annotated[Source, PlainValidator(_validate_source)]
    file_format: file.Format
    processing_mode: str | None = None
    processing_steps: list[ProcessingStep] = []


class Column(SortedSerializedBase):
    id: str
    data_type: Literal[
        "text", "integer", "decimal", "geometry", "bool", "date", "datetime"
    ]
    description: str | None = None


class Template(BaseModel, extra="forbid"):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    id: str
    acl: recipes.ValidAclValues

    attributes: DatasetAttributes
    ingestion: Ingestion
    columns: list[Column] = []

    @property
    def has_geom(self):
        match self.ingestion.file_format:
            case file.Shapefile() | file.Geodatabase() | file.GeoJson():
                return True
            case file.Csv() | file.Xlsx() | file.Json() as format:
                return format.geometry is not None


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
            id=self.id, timestamp=self.archival.archival_timestamp
        )

    @property
    def freshness(self) -> datetime:
        return (
            self.archival.archival_timestamp
            if not self.archival.check_timestamps
            else max(self.archival.check_timestamps)
        )

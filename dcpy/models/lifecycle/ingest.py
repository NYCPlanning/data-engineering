from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field, AliasChoices
from typing import Any, Literal, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes, publishing
from dcpy.models.connectors import web, socrata
from dcpy.models import library, file


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


class PreprocessingStep(BaseModel):
    name: str
    args: dict[str, Any] = {}
    # mode allows for certain preprocessing steps only to be run if specified at runtime
    mode: str | None = None


class Template(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    id: str
    acl: recipes.ValidAclValues

    target_crs: str | None = None

    ## these two fields might merge to "source" or something equivalent at some point
    ## for now, they are distinct so that they can be worked on separately
    ## when implemented, "None" should not be valid type
    source: Source
    file_format: file.Format

    processing_steps: list[PreprocessingStep] = []

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: library.DatasetDefinition | None = None


class Config(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    """
    Computed Template of ingest dataset
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    archival_timestamp: datetime
    check_timestamps: list[datetime] = []
    raw_filename: str
    acl: recipes.ValidAclValues

    target_crs: str | None = None

    source: Source
    file_format: file.Format
    processing_mode: str | None = None
    processing_steps: list[PreprocessingStep] = []

    run_details: RunDetails

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
        return recipes.RawDatasetKey(id=self.id, timestamp=self.archival_timestamp)

    @property
    def freshness(self) -> datetime:
        return (
            self.archival_timestamp
            if not self.check_timestamps
            else max(self.check_timestamps)
        )

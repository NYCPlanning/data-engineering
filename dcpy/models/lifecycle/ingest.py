from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, field_serializer
from typing import Any, Literal, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes, publishing
from dcpy.models.connectors import web, socrata
from dcpy.models import library, file


class LocalFileSource(BaseModel, extra="forbid"):
    type: Literal["local_file"]
    path: Path

    @field_serializer("path")
    def _serialize_path(self, path: Path, _info) -> str:
        return str(path)


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


class FunctionCall(BaseModel):
    name: str
    args: dict[str, Any] = {}


class Template(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    name: str
    acl: recipes.ValidAclValues

    ## these two fields might merge to "source" or something equivalent at some point
    ## for now, they are distinct so that they can be worked on separately
    ## when implemented, "None" should not be valid type
    source: Source
    file_format: file.Format
    processing_steps: list[FunctionCall] = []

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: library.DatasetDefinition | None = None


class Config(
    BaseModel, extra="ignore", arbitrary_types_allowed=True
):  # TODO switch extra back to "forbid" when library_dataset is dropped from template
    """New object corresponding to computed template in dcpy.lifecycle.extract
    Meant to be stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    At some point backwards compatability with LibraryConfig should be considered"""

    name: str
    version: str
    archival_timestamp: datetime
    raw_filename: str
    acl: recipes.ValidAclValues
    source: Source
    file_format: file.Format
    processing_steps: list[FunctionCall] = []
    run_details: RunDetails

    @property
    def dataset(self) -> recipes.Dataset:
        return recipes.Dataset(
            name=self.name, version=self.version, file_type=recipes.DatasetType.parquet
        )

    @property
    def dataset_key(self) -> recipes.DatasetKey:
        return recipes.DatasetKey(name=self.name, version=self.version)

    @property
    def filename(self) -> str:
        return f"{self.name}.parquet"

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(name=self.name, timestamp=self.archival_timestamp)

    def s3_file_key(self, prefix: str) -> str:
        return self.dataset.s3_file_key(prefix)

    def raw_s3_key(self, prefix: str) -> Path:
        return self.raw_dataset_key.s3_path(prefix) / self.raw_filename

    @field_serializer("archival_timestamp")
    def _serialize_timestamp(self, archival_timestamp: datetime, _info) -> str:
        return archival_timestamp.isoformat()

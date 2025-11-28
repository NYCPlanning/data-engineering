from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices, AliasPath, TypeAdapter
from typing import Any

from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES
from dcpy.models import file
from dcpy.models.connectors.edm import recipes
from dcpy.models.base import SortedSerializedBase, TemplatedYamlReader
from dcpy.utils.metadata import RunDetails


class DatasetAttributes(SortedSerializedBase):
    name: str | None = None
    description: str | None = None
    url: str | None = None
    custom: dict | None = None

    _head_sort_order = ["name", "description", "url"]


class Source(BaseModel, extra="allow"):
    type: str
    key: str = Field(
        validation_alias=AliasChoices(
            "key", "path", "url", "endpoint", "name", "uid", "dataset", "product"
        )
    )


class ProcessingStep(SortedSerializedBase):
    name: str
    args: dict[str, Any] = {}
    # mode allows for certain processing steps only to be run if specified at runtime
    mode: str | None = None


class Ingestion(BaseModel, extra="forbid"):
    target_crs: str | None = None
    source: Source
    file_format: file.Format
    processing_steps: list[ProcessingStep] = []


class Column(BaseColumn):
    _head_sort_order = ["id", "data_type", "description"]

    data_type: COLUMN_TYPES  # override BaseColumn `data_type` to be required field


class DatasetDefinition(TemplatedYamlReader, extra="forbid"):
    """
    Definition of a dataset for ingestion/processing/archiving in edm-recipes

    1-1 mapping of data source to output dataset
    """

    id: str
    acl: recipes.ValidAclValues | None = None

    attributes: DatasetAttributes
    ingestion: Ingestion
    columns: list[Column] = []
    checks: list[str | dict[str, Any]] | None = None

    @property
    def source(self) -> Source:
        return self.ingestion.source


class DownstreamDatasetDefinition(BaseModel, extra="forbid"):
    """
    Definition of a dataset for ingestion/processing/archiving in edm-recipes

    Single dataset that must be defined in context of an associated data source,
    which is not defined in this record
    """

    id: str
    acl: recipes.ValidAclValues | None = None
    attributes: DatasetAttributes

    target_crs: str | None = None
    file_format: dict
    processing_steps: list[ProcessingStep] = []
    columns: list[Column] = []
    checks: list[str | dict[str, Any]] | None = None


class DatasetDefaults(BaseModel, extra="forbid"):
    attributes: dict | None = None
    target_crs: str | None = None
    file_format: dict | None = None
    processing_steps: list[ProcessingStep] | None = None


class DataSourceDefinition(TemplatedYamlReader, extra="forbid"):
    """
    Definition of a data source for ingestion/processing/archiving in edm-recipes

    1-n mapping from data source to downstream datasets (defined within this object).
    An example use case is a gdb with multiple layers or excel with multiple tabs,
    each of which becomes a distinct table/parquet file in edm-recipes.

    The data source is extracted/archived once, and downstream datasets are processed
    from that one source
    """

    id: str
    acl: recipes.ValidAclValues | None = None
    attributes: DatasetAttributes
    source: Source
    dataset_defaults: DatasetDefaults = DatasetDefaults()
    datasets: list[DownstreamDatasetDefinition]


IngestDefinition = TypeAdapter(DatasetDefinition | DataSourceDefinition)  # type: ignore


class ResolvedDownstreamDataset(SortedSerializedBase):
    id: str
    acl: recipes.ValidAclValues | None = None
    attributes: DatasetAttributes

    target_crs: str | None = None
    file_format: file.Format
    processing_steps: list[ProcessingStep] = []
    columns: list[Column] = []
    checks: list[str | dict[str, Any]] | None = None


class ResolvedDataSource(SortedSerializedBase, TemplatedYamlReader):
    """After reading in definitions from yml, the first object created"""

    id: str
    acl: recipes.ValidAclValues | None = None

    version: str | None = None
    attributes: DatasetAttributes
    source: Source
    datasets: list[ResolvedDownstreamDataset]

    _head_sort_order = ["id", "acl", "version", "attributes", "source", "datasets"]


class DataSourceArchivalDetails(SortedSerializedBase):
    id: str
    acl: recipes.ValidAclValues | None = None
    timestamp: datetime
    attributes: DatasetAttributes
    version: str | None = None
    source: Source
    raw_filename: str
    run_details: RunDetails

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(
            id=self.id,
            timestamp=self.timestamp,
            filename=self.raw_filename,
        )

    @property
    def raw_dataset_path(self) -> str:
        return f"{self.id}/{self.timestamp.isoformat()}/{self.raw_filename}"

    @property
    def pull_kwargs(self) -> dict[str, str]:
        return {"key": self.id, "version": self.timestamp.isoformat()}


class ArchivedDataSource(DataSourceArchivalDetails, TemplatedYamlReader):
    """This object is stored with an archived raw dataset as 'config.json'"""

    datasets: list[ResolvedDownstreamDataset]

    @property
    def details(self) -> DataSourceArchivalDetails:
        dumped = self.model_dump()
        dumped.pop("datasets")
        return DataSourceArchivalDetails(**dumped)


class ProcessingSummary(SortedSerializedBase):
    """Summary of the changes from a data processing function."""

    name: str
    description: str
    data_modifications: dict = {}
    column_modifications: dict = {}
    custom: dict = {}


class Transformation(SortedSerializedBase):
    target_crs: str | None = None
    file_format: file.Format
    processing_steps: list[ProcessingStep] = []
    processing_mode: str | None = None
    processing_steps_summaries: list[ProcessingSummary] = []
    run_details: RunDetails


class IngestedDataset(SortedSerializedBase, TemplatedYamlReader, extra="forbid"):
    """
    Resolved definition with details of
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    crs: str | None = None

    acl: recipes.ValidAclValues | None = None
    attributes: DatasetAttributes
    source: DataSourceArchivalDetails
    transformation: Transformation
    columns: list[Column] = []

    _head_sort_order = [
        "id",
        "version",
        "crs",
        "attributes",
        "archival",
        "transformation",
        "columns",
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
    def dataset_path(self) -> str:
        return f"{self.id}/{self.version}/{self.id}.parquet"

    @property
    def filename(self) -> str:
        return f"{self.id}.parquet"

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(
            id=self.id,
            timestamp=self.source.run_details.timestamp,
            filename=self.source.raw_filename,
        )

    @property
    def raw_dataset_path(self) -> str:
        return self.source.raw_dataset_path


class ProcessingResult(SortedSerializedBase, arbitrary_types_allowed=True):
    df: pd.DataFrame
    summary: ProcessingSummary


class SparseConfig(BaseModel, extra="allow"):
    id: str = Field(validation_alias=AliasChoices("id", AliasPath("dataset", "name")))
    version: str = Field(
        validation_alias=AliasChoices(
            "version",  # ingest
            "timestamp",  # ingest - raw
            AliasPath("dataset", "version"),  # library
        )
    )
    run_timestamp: datetime | None = Field(
        None,
        validation_alias=AliasChoices(
            AliasPath("transformation", "run_details", "timestamp"),  # ingest
            "timestamp",  # ingest - raw
            AliasPath("run_details", "timestamp"),  # ingest - outdated
            AliasPath("execution_details", "timestamp"),
        ),
    )

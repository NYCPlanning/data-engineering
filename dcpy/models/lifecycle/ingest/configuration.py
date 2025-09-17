from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices

from dcpy.models import file
from dcpy.models.connectors.edm import recipes
from dcpy.models.base import SortedSerializedBase
from dcpy.utils.metadata import RunDetails

from dcpy.models.lifecycle.ingest import definitions


class ResolvedConfig(BaseModel, extra="forbid"):
    """After reading in definitions from yml, the first object created"""

    id: str
    acl: recipes.ValidAclValues | None = None

    version: str | None = None
    attributes: definitions.DatasetAttributes
    source: definitions.Source
    transformation: list[definitions.DatasetTransformation]


class Archival(SortedSerializedBase):
    id: str
    source: definitions.Source
    raw_filename: str
    acl: recipes.ValidAclValues | None = None
    run_details: RunDetails


class DatasourceConfig(SortedSerializedBase):
    """What is stored with an archived raw dataset"""

    id: str
    acl: recipes.ValidAclValues | None = None
    timestamp: datetime

    version: str | None = None
    attributes: definitions.DatasetAttributes
    archival: Archival
    transformation: list[definitions.DatasetTransformation]

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(
            id=self.id,
            timestamp=self.timestamp,
            filename=self.archival.raw_filename,
        )

    @property  # todo cache
    def transformations(self) -> dict[str, definitions.DatasetTransformation]:
        return {t.id: t for t in self.transformation}


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
    processing_steps: list[definitions.ProcessingStep] = []
    processing_mode: str | None = None
    processing_steps_summaries: list[ProcessingSummary] = []
    run_details: RunDetails


class DatasetConfig(SortedSerializedBase, extra="forbid"):
    """
    Computed Template of ingest dataset
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    crs: str | None = None

    attributes: definitions.DatasetAttributes
    archival: Archival
    transformation: Transformation
    columns: list[definitions.Column] = []

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
    def filename(self) -> str:
        return f"{self.id}.parquet"

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(
            id=self.archival.id,
            timestamp=self.archival.run_details.timestamp,
            filename=self.archival.raw_filename,
        )


class ProcessingResult(SortedSerializedBase, arbitrary_types_allowed=True):
    df: pd.DataFrame
    summary: ProcessingSummary


class SparseConfig(BaseModel, extra="allow"):
    id: str
    version: str = Field(validation_alias=AliasChoices("version", "timestamp"))

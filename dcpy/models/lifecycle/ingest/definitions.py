from pydantic import BaseModel, Field, AliasChoices, TypeAdapter
from typing import Any

from dcpy.models.connectors.edm import recipes
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase
from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES


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


class IngestDefinitionSimple(BaseModel, extra="forbid"):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

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


class IngestDefinitionOneToMany(BaseModel, extra="forbid"):
    id: str
    acl: recipes.ValidAclValues | None = None
    attributes: DatasetAttributes
    source: Source
    dataset_defaults: DatasetDefaults = DatasetDefaults()
    datasets: list[DownstreamDatasetDefinition]


IngestDefinition = TypeAdapter(IngestDefinitionSimple | IngestDefinitionOneToMany)  # type: ignore

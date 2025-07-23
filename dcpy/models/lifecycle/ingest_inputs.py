from __future__ import annotations
from datetime import datetime
import inspect
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices, model_validator, ValidationError
from typing import Any, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase
from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES

from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils.introspect import validate_kwargs


class Source(BaseModel, extra="allow"):
    type: str
    key: str = Field(
        validation_alias=AliasChoices(
            "key", "path", "url", "endpoint", "name", "uid", "dataset", "product"
        )
    )
    _ds_id: str | None = None

    @model_validator(mode="after")
    def validate_fields(self):
        conn_type = self.type
        if conn_type not in connectors.pull.list_registered():
            raise ValueError(f"Connector type '{conn_type}' not registered.")
        connector = connectors.pull[conn_type]
        missing = validate_kwargs(connector.pull, self.model_dump(), ignore_args=["destination_path"])
        if missing:
            raise ValueError(
                f"Missing required fields for '{conn_type}': {missing}"
            )
        return self


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

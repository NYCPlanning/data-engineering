from __future__ import annotations
from functools import cached_property
from datetime import datetime
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, Field, AliasChoices
from typing import Any, Literal, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes
from dcpy.models.connectors import socrata, esri
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase
from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES

from dcpy.connectors.esri import arcgis_feature_service


class LocalFileSource(BaseModel, extra="forbid"):
    type: Literal["local_file"]
    path: Path


class S3Source(BaseModel, extra="forbid"):
    type: Literal["s3"]
    bucket: str
    key: str


class FileDownloadSource(BaseModel, extra="forbid"):
    type: Literal["file_download"]
    url: str


class GenericApiSource(BaseModel, extra="forbid"):
    type: Literal["api"]
    endpoint: str
    format: Literal["json", "csv"]


class DEPublished(BaseModel, extra="forbid"):
    type: Literal["edm.publishing.published"]
    product: str
    filename: str


class GisDataset(BaseModel, extra="forbid"):
    """Dataset published by GIS in edm-publishing/datasets"""

    # Some datasets here will phased out if we eventually get data
    # directly from GR or other sources
    type: Literal["edm.publishing.gis"]
    name: str


class SocrataSource(BaseModel, extra="forbid"):
    type: Literal["socrata"]
    org: socrata.Org
    uid: str
    format: socrata.ValidSourceFormats

    @property
    def extension(self) -> str:
        if self.format == "shapefile":
            return "zip"
        else:
            return self.format


class ESRIFeatureServer(BaseModel, extra="forbid"):
    type: Literal["arcgis_feature_server"]
    server: esri.Server
    dataset: str
    layer_name: str | None = None
    layer_id: int | None = None
    crs: str = "EPSG:4326"  # The default value here is geojson specification

    @property
    def feature_server(self) -> esri.FeatureServer:
        return esri.FeatureServer(server=self.server, name=self.dataset)

    @cached_property
    def feature_server_layer(self) -> esri.FeatureServerLayer:
        feature_server_layer = arcgis_feature_service.resolve_layer(
            feature_server=self.feature_server,
            layer_name=self.layer_name,
            layer_id=self.layer_id,
        )
        return feature_server_layer


Source: TypeAlias = (
    LocalFileSource
    | FileDownloadSource
    | GenericApiSource
    | S3Source
    | SocrataSource
    | GisDataset
    | DEPublished
    | ESRIFeatureServer
)


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

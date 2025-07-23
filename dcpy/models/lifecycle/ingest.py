from __future__ import annotations
from abc import ABC, abstractmethod
from functools import cached_property
from datetime import datetime
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, Field, AliasChoices, model_validator
from typing import Any, Literal, TypeAlias

from dcpy.utils.metadata import RunDetails
from dcpy.models.connectors.edm import recipes
from dcpy.models.connectors import socrata, esri
from dcpy.models import file
from dcpy.models.base import SortedSerializedBase
from dcpy.models.dataset import Column as BaseColumn, COLUMN_TYPES

from dcpy.connectors.esri import arcgis_feature_service


class ConnectorSource(BaseModel):
    _ds_id: str | None = None

    @abstractmethod
    def get_key(self) -> str:
        """unique identifier of a dataset for this source type"""


class LocalFileSource(ConnectorSource, extra="forbid"):
    type: Literal["local_file"]
    path: Path

    def get_key(self) -> str:
        return str(self.path)


class S3Source(ConnectorSource, extra="forbid"):
    type: Literal["s3"]
    bucket: str
    key: str

    def get_key(self) -> str:
        return self.key


class FileDownloadSource(ConnectorSource, extra="forbid"):
    type: Literal["file_download"]
    url: str

    def get_key(self) -> str:
        return self.url


class GenericApiSource(ConnectorSource, extra="forbid"):
    type: Literal["api"]
    endpoint: str
    format: Literal["json", "csv"]

    def get_key(self) -> str:
        return self.endpoint

    def model_dump(self, **kwargs) -> dict[str, Any]:
        dump = super().model_dump(**kwargs)
        dump["filename"] = f"{self._ds_id or 'raw'}.{self.format}"
        return dump


class DEPublished(ConnectorSource, extra="forbid"):
    type: Literal["edm.publishing.published"]
    product: str
    filepath: str

    def get_key(self) -> str:
        return self.product


class GisDataset(ConnectorSource, extra="forbid"):
    """Dataset published by GIS in edm-publishing/datasets"""

    # Some datasets here will phased out if we eventually get data
    # directly from GR or other sources
    type: Literal["edm.publishing.gis"]
    name: str

    def get_key(self) -> str:
        return self.name


class SocrataSource(ConnectorSource, extra="forbid"):
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

    def get_key(self) -> str:
        return self.uid


class ESRIFeatureServer(ConnectorSource, extra="forbid"):
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

    def get_key(self) -> str:
        return self.dataset


class Config(SortedSerializedBase, extra="forbid"):
    """
    Computed Template of ingest dataset
    Stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    """

    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str
    crs: str | None = None

    attributes: dict
    archival: dict
    ingestion: dict
    columns: list[dict] = []
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
            timestamp=self.archival["archival_timestamp"],
            filename=self.archival["raw_filename"],
        )

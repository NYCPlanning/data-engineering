from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel
from typing import Literal

from dcpy.utils import metadata
from dcpy.models.connectors.edm import recipes
from dcpy.models.connectors import esri


#### library objects
class GeometryType(BaseModel):
    SRS: str | None = None
    type: Literal[
        "NONE",
        "GEOMETRY",
        "POINT",
        "LINESTRING",
        "POLYGON",
        "GEOMETRYCOLLECTION",
        "MULTIPOINT",
        "MULTIPOLYGON",
        "MULTILINESTRING",
        "CIRCULARSTRING",
        "COMPOUNDCURVE",
        "CURVEPOLYGON",
        "MULTICURVE",
        "MULTISURFACE",
    ]


class DatasetDefinition(BaseModel):
    """Computed templates from dcpy.library. Stored in config.json files in edm-recipes/datasets"""

    name: str
    version: str
    acl: recipes.ValidAclValues
    source: SourceSection
    destination: DestinationSection
    info: InfoSection | None = None

    class SourceSection(BaseModel):
        url: Url | None = None
        script: Script | None = None
        socrata: Socrata | None = None
        arcgis_feature_server: FeatureServerLayerDefinition | None = None
        layer_name: str | None = None
        geometry: GeometryType | None = None
        options: list[str] | None = None
        gdalpath: str | None = None

        class Url(BaseModel):
            path: str
            subpath: str = ""

        class Socrata(BaseModel):
            uid: str
            format: Literal["csv", "geojson", "shapefile"]

        class Script(BaseModel, extra="allow"):
            name: str | None = None
            path: str | None = None

        class FeatureServerLayerDefinition(BaseModel, extra="forbid"):
            server: esri.Server
            name: str
            layer_name: str | None = None
            layer_id: int | None = None

            @property
            def feature_server(self) -> esri.FeatureServer:
                return esri.FeatureServer(server=self.server, name=self.name)

    class DestinationSection(BaseModel):
        geometry: GeometryType
        options: list[str] | None = None
        fields: list[str] | None = None
        sql: str | None = None

    class InfoSection(BaseModel):
        info: str | None = None
        url: str | None = None
        dependents: list[str] | None = None

    @property
    def dataset(self) -> recipes.Dataset:
        return recipes.Dataset(id=self.name, version=self.version)


class Config(BaseModel, extra="forbid"):
    dataset: DatasetDefinition
    execution_details: metadata.RunDetails | None = None

    @property
    def version(self) -> str:
        return self.dataset.version

    @property
    def sparse_dataset(self) -> recipes.Dataset:
        return self.dataset.dataset

    @property
    def dataset_key(self) -> recipes.DatasetKey:
        return self.sparse_dataset.key


@dataclass
class ArchivalMetadata:
    name: str
    version: str
    timestamp: datetime
    config: dict | None = None
    runner: str | None = None

    def to_row(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "timestamp": self.timestamp,
            "runner": self.runner,
        }

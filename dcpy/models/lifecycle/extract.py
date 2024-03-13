from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, field_serializer
from typing import Literal, TypeAlias

from dcpy.models.connectors.edm import recipes, publishing
from dcpy.models.connectors import web, socrata
from dcpy.models import library


class Geometry(BaseModel):
    """
    Represents the geometric configuration for geospatial data.
    Attributes:
        geom_column: The name of geometry column in the dataset, or an instance of PointColumns if geometry is defined by separate longitude and latitude columns.
        crs: The coordinate reference system (CRS) for the geometry.
    Nested Classes:
        PointColumns: Defines the names of the longitude and latitude columns for point geometries.
    """

    geom_column: str | PointColumns
    crs: str

    class PointColumns(BaseModel):
        """This class defines longitude and latitude column names."""

        x: str
        y: str


class LocalFileSource(BaseModel, extra="forbid"):
    type: Literal["local_file"]
    path: Path


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
    | ScriptSource
)


class ToParquetMeta:
    """
    Represents config info needed for translation of raw data into parquet format.
    Config attributes vary by raw data format.
    """

    class Csv(BaseModel, extra="forbid"):
        format: Literal["csv"]
        encoding: str = "utf-8"
        delimiter: str | None = None
        geometry: Geometry | None = None

    class Xlsx(BaseModel, extra="forbid"):
        format: Literal["xlsx"]
        tab_name: str
        encoding: str = "utf-8"
        geometry: Geometry | None = None

    class Shapefile(BaseModel, extra="forbid"):
        format: Literal["shapefile"]
        encoding: str = "utf-8"
        crs: str

    class Geodatabase(BaseModel, extra="forbid"):
        format: Literal["geodatabase"]
        layer: str | None = None
        encoding: str = "utf-8"
        crs: str

    # TODO: implement JSON and GEOJSON
    class Json(BaseModel):
        format: Literal["json"]

    Options: TypeAlias = Csv | Xlsx | Shapefile | Geodatabase | Json


class Template(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    """Definition of a dataset for ingestion/processing/archiving in edm-recipes"""

    name: str
    acl: recipes.ValidAclValues

    ## these two fields might merge to "source" or something equivalent at some point
    ## for now, they are distinct so that they can be worked on separately
    ## when implemented, "None" should not be valid type
    source: Source
    transform_to_parquet_metadata: ToParquetMeta.Options

    ## this is the original library template, included just for reference while we build out our new templates
    library_dataset: library.DatasetDefinition | None = None


class Config(BaseModel, extra="forbid"):
    """New object corresponding to computed template in dcpy.lifecycle.extract
    Meant to be stored in config.json in edm-recipes/raw_datasets and edm-recipes/datasets
    At some point backwards compatability with LibraryConfig should be considered"""

    name: str
    version: str
    archival_timestamp: datetime
    raw_filename: str
    acl: recipes.ValidAclValues
    source: Source
    transform_to_parquet_metadata: ToParquetMeta.Options

    @property
    def dataset(self) -> recipes.Dataset:
        return recipes.Dataset(name=self.name, version=self.version)

    @property
    def dataset_key(self) -> recipes.DatasetKey:
        return recipes.DatasetKey(name=self.name, version=self.version)

    @property
    def raw_dataset_key(self) -> recipes.RawDatasetKey:
        return recipes.RawDatasetKey(name=self.name, timestamp=self.archival_timestamp)

    def raw_dataset_s3_filepath(self, prefix: str) -> Path:
        return self.raw_dataset_key.s3_path(prefix) / self.raw_filename

    @field_serializer("archival_timestamp")
    def _serialize_timestamp(self, archival_timestamp: datetime, _info) -> str:
        return archival_timestamp.isoformat()

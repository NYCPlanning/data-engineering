from __future__ import annotations
from pydantic import BaseModel
from typing import Literal, TypeAlias


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


class Csv(BaseModel, extra="forbid"):
    type: Literal["csv"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    delimiter: str | None = None
    column_names: list[str] | None = None
    dtype: str | None = None
    geometry: Geometry | None = None


class Xlsx(BaseModel, extra="forbid"):
    type: Literal["xlsx"]
    unzipped_filename: str | None = None
    tab_name: str
    geometry: Geometry | None = None


class Shapefile(BaseModel, extra="forbid"):
    type: Literal["shapefile"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    crs: str


class Geodatabase(BaseModel, extra="forbid"):
    type: Literal["geodatabase"]
    unzipped_filename: str | None = None
    layer: str | None = None
    encoding: str = "utf-8"
    crs: str


class Json(BaseModel):
    type: Literal["json"]
    json_read_meta: JsonMeta
    columns: list[str] = []
    unzipped_filename: str | None = None
    geometry: Geometry | None = None


class JsonNormalize(BaseModel):
    json_read_fn: Literal["normalize"]
    json_read_kwargs: dict = {"record_path": None}


class ReadJson(BaseModel):
    json_read_fn: Literal["read_json"]
    json_read_kwargs: dict = {"orient": None}


JsonMeta: TypeAlias = JsonNormalize | ReadJson


class GeoJson(BaseModel, extra="forbid"):
    type: Literal["geojson"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    # Note, crs is not an attribute for geojson format. Geojson has a specification of "EPSG:4326"


Format: TypeAlias = Csv | Xlsx | Shapefile | Geodatabase | Json | GeoJson

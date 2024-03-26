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


Format: TypeAlias = Csv | Xlsx | Shapefile | Geodatabase | Json

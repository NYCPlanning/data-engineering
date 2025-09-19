from __future__ import annotations
from typing import Literal, TypeAlias

from dcpy.models.base import SortedSerializedBase
from dcpy.models.geospatial import geometry


class Geometry(SortedSerializedBase, extra="forbid"):
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
    format: geometry.GeometryFormat | None = None

    class PointColumns(SortedSerializedBase, extra="forbid"):
        """This class defines longitude and latitude column names."""

        x: str
        y: str


class File(SortedSerializedBase, extra="allow"):
    type: (
        Literal[
            "csv",
            "excel",
            "xlsx",
            "shapefile",
            "geodatabase",
            "json",
            "geojson",
            "html",
        ]
        | None
    ) = None
    geometry: Geometry | None = None
    unzipped_filepath: str | None = None


class Csv(SortedSerializedBase, extra="forbid"):
    type: Literal["csv"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    delimiter: str | None = None
    column_names: list[str] | None = None
    dtype: str | dict | None = None
    geometry: Geometry | None = None
    usecols: list[str] | list[int] | None = None


class Excel(SortedSerializedBase, extra="forbid"):
    type: Literal["excel", "xlsx"]  # union for backwards compatability
    unzipped_filename: str | None = None
    sheet_name: str | int
    engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"] | None = None
    dtype: str | dict | None = None
    geometry: Geometry | None = None


class Shapefile(SortedSerializedBase, extra="forbid"):
    type: Literal["shapefile"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    crs: str


class Geodatabase(SortedSerializedBase, extra="forbid"):
    type: Literal["geodatabase"]
    unzipped_filename: str | None = None
    layer: str | None = None
    encoding: str = "utf-8"
    crs: str


class Json(SortedSerializedBase, extra="forbid"):
    type: Literal["json"]
    json_read_fn: Literal["normalize", "read_json"]
    json_read_kwargs: dict = {}
    unzipped_filename: str | None = None
    geometry: Geometry | None = None


class GeoJson(SortedSerializedBase, extra="forbid"):
    type: Literal["geojson"]
    unzipped_filename: str | None = None
    encoding: str = "utf-8"
    # Note, crs is not an attribute for geojson format. Geojson has a specification of "EPSG:4326"


class Html(SortedSerializedBase, extra="forbid"):
    type: Literal["html"]
    kwargs: dict
    table: int = 0
    unzipped_filename: str | None = None
    geometry: Geometry | None = None


Format: TypeAlias = Csv | Excel | Shapefile | Geodatabase | Json | GeoJson | Html

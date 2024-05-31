from pyarrow.parquet import FileMetaData
from pydantic import BaseModel
from typing import Literal

from . import projjson as proj

GEOPARQUET_METADATA_KEY = b"geo"


class Columns(BaseModel, extra="forbid"):
    """GeoParquet column metadata as specified by https://geoparquet.org/releases/v1.0.0/schema.json"""

    encoding: Literal["WKB"]
    geometry_types: set[
        Literal[
            "Point",
            "LineString",
            "Polygon",
            "MultiPoint",
            "MultiLineString",
            "MultiPolygon",
            "GeometryCollection",
        ]
    ]
    crs: proj.Model
    edges: Literal["planar", "spherical"] | None = None
    orientation: Literal["counterclockwise"] = "counterclockwise"
    bbox: list[float] | None = None
    epoch: float | None = None


class GeoParquet(BaseModel, extra="forbid"):
    """GeoParquet metadata as specified by https://geoparquet.org/releases/v1.0.0/schema.json"""

    version: Literal["1.0.0"]
    primary_column: str
    columns: dict[str, Columns]
    creator: dict | None = None


class MetaData:
    file_metadata: FileMetaData
    geo_parquet: GeoParquet

    def __init__(self, file_metadata: FileMetaData, geo_parquet: GeoParquet):
        self.file_metadata = file_metadata
        self.geo_parquet = geo_parquet

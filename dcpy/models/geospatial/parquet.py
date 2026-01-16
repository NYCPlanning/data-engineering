from pyarrow.parquet import FileMetaData
from pydantic import BaseModel, Field
from typing import Literal

GEOPARQUET_METADATA_KEY = b"geo"


class CrsId(BaseModel, extra="forbid"):
    authority: str
    code: str | int
    version: str | float | None = None
    authority_citation: str | None = None
    uri: str | None = None

    @property
    def authority_string(self):
        return f"{self.authority}:{self.code}"


class Crs(BaseModel, extra="ignore"):
    id: CrsId


class Columns(BaseModel, extra="ignore"):
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
    crs: Crs

    @property
    def crs_string(self):
        return self.crs.id.authority_string


class GeoParquet(BaseModel, extra="forbid"):
    """GeoParquet metadata as specified by https://geoparquet.org/releases/v1.0.0/schema.json"""

    version: Literal["1.0.0"]
    primary_column_str: str = Field(alias="primary_column")
    columns: dict[str, Columns]
    creator: dict | None = None

    @property
    def primary_column(self):
        return self.columns[self.primary_column_str]


class MetaData:
    file_metadata: FileMetaData
    geo_parquet: GeoParquet

    def __init__(self, file_metadata: FileMetaData, geo_parquet: GeoParquet):
        self.file_metadata = file_metadata
        self.geo_parquet = geo_parquet

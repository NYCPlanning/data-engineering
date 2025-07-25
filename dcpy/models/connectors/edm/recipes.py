from __future__ import annotations
from datetime import datetime
from enum import StrEnum
from pydantic import BaseModel
from typing import Literal

ValidAclValues = Literal["public-read", "private"]


#### extract objects
class RawDatasetKey(BaseModel, extra="forbid"):
    id: str
    timestamp: datetime
    filename: str


class DatasetKey(BaseModel, extra="forbid"):
    id: str
    version: str


class DatasetType(StrEnum):
    pg_dump = "pg_dump"
    csv = "csv"
    parquet = "parquet"
    xlsx = "xlsx"  # needed for a few "legacy" products. Aim to phase out
    json = "json"
    shapefile = "shapefile"


def _type_to_extension(dst: DatasetType) -> str:
    mapping = {"pg_dump": "sql", "csv": "csv", "parquet": "parquet", "xlsx": "xlsx"}
    return mapping[dst]


class Dataset(BaseModel, extra="forbid"):
    id: str
    version: str
    file_type: DatasetType | None = None

    @property
    def file_name(self) -> str:
        if self.file_type is None:
            raise Exception("File type must be defined to get file name")
        return f"{self.id}.{_type_to_extension(self.file_type)}"

    @property
    def key(self) -> DatasetKey:
        return DatasetKey(id=self.id, version=self.version)

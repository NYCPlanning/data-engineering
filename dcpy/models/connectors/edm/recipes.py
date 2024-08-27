from __future__ import annotations
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel
from typing import Literal

ValidAclValues = Literal["public-read", "private"]


#### extract objects
class RawDatasetKey(BaseModel, extra="forbid"):
    id: str
    timestamp: datetime

    def s3_path(self, prefix: str) -> Path:
        return Path(prefix) / self.id / self.timestamp.isoformat()


class DatasetKey(BaseModel, extra="forbid"):
    id: str
    version: str

    def s3_path(self, prefix: str) -> Path:
        return Path(prefix) / self.id / self.version


class DatasetType(StrEnum):
    pg_dump = "pg_dump"
    csv = "csv"
    parquet = "parquet"
    xlsx = "xlsx"  # needed for a few "legacy" products. Aim to phase out


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

    def s3_folder_key(self, prefix: str) -> str:
        return f"{prefix}/{self.id}/{self.version}"

    def s3_file_key(self, prefix: str) -> str:
        return f"{self.s3_folder_key(prefix)}/{self.file_name}"

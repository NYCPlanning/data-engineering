from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel

ValidAclValues = Literal["public-read", "private"]


#### Publishing/Artifact Keys ####


class ProductKey(ABC):
    product: str

    @property
    @abstractmethod
    def path(self) -> str:
        raise NotImplementedError("ProductKey is an abstract class")


@dataclass
class PublishKey(ProductKey):
    product: str
    version: str

    def __str__(self):
        return f"{self.product} - {self.version}"

    @property
    def path(self) -> str:
        return f"{self.product}/publish/{self.version}"


@dataclass
class DraftKey(ProductKey):
    product: str
    version: str
    revision: str

    def __str__(self):
        return f"Draft: {self.product} - {self.version} ({self.revision})"

    @property
    def path(self) -> str:
        return f"{self.product}/draft/{self.version}/{self.revision}"


@dataclass
class BuildKey(ProductKey):
    product: str
    build: str

    def __str__(self):
        return f"Build: {self.product} - {self.build}"

    @property
    def path(self) -> str:
        return f"{self.product}/build/{self.build}"


@dataclass
class PlanKey(ProductKey):
    product: str
    version: str
    revision: str

    def __str__(self):
        return f"Plan: {self.product} - {self.version} ({self.revision})"

    @property
    def path(self) -> str:
        return f"{self.product}/plan/{self.version}/{self.revision}"


#### Recipe/Dataset Models ####


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
    geodatabase = "gdb"
    other = "Other"

    def to_extension(self) -> str:
        mapping = {
            "pg_dump": "sql",
            "csv": "csv",
            "parquet": "parquet",
            "xlsx": "xlsx",
            "json": "json",
            "shapefile": "shp",
            "gdb": "gdb.zip",
            "other": "dat",
        }
        return mapping[self.value]

    @classmethod
    def from_extension(cls, s: str) -> "DatasetType | None":
        match s:
            case "sql":
                return cls.pg_dump
            case "csv":
                return cls.csv
            case "parquet":
                return cls.parquet
            case "xlsx":
                return cls.xlsx
            case _:
                return None


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

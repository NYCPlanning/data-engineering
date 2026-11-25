from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import AliasChoices, AliasPath, BaseModel, Field

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


@dataclass
class PrivateKey(ProductKey):
    """Key for private datasets stored in edm-private bucket.

    Simple structure: {dataset}/{version}
    No folder_name hierarchy like publish/draft/build.
    """

    product: str
    version: str

    def __str__(self):
        return f"Private: {self.product} - {self.version}"

    @property
    def path(self) -> str:
        return f"{self.product}/{self.version}"


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
    # a zip of parquet files, loaded as one table per file in a schema named import_as
    parquet_bundle = "parquet_bundle"
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
            "parquet_bundle": "zip",
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


# Reads the config.json of any edm-recipes archive, ingest or library-era.
class SparseConfig(BaseModel, extra="allow"):
    id: str = Field(
        validation_alias=AliasChoices(
            "id",  # ingest
            "name",  # ingest - outdated
            AliasPath("dataset", "name"),  # library
        )
    )
    version: str = Field(
        validation_alias=AliasChoices(
            "timestamp",  # ingest - raw
            "version",  # ingest
            AliasPath("dataset", "version"),  # library
        )
    )
    name: str | None = Field(
        None,
        validation_alias=AliasChoices(
            AliasPath("attributes", "name"),  # ingest
        ),
    )
    run_timestamp: datetime | None = Field(
        None,
        validation_alias=AliasChoices(
            AliasPath("transformation", "run_details", "timestamp"),  # ingest
            "timestamp",  # ingest - raw
            AliasPath("run_details", "timestamp"),  # ingest - outdated
            AliasPath("execution_details", "timestamp"),
        ),
    )


# Where a dataset config.json records when it was archived: ingest, older ingest, library.
_ARCHIVE_TIMESTAMP_PATHS = [
    ("transformation", "run_details", "timestamp"),
    ("run_details", "timestamp"),
    ("execution_details", "timestamp"),
]


def archive_timestamp_from_config(config: dict) -> datetime | None:
    """When the archive was written, by the run's own clock, from its config.json.

    Unlike object dates, this survives a copy to another store. None when the config
    doesn't say, or says something unparseable.
    """
    for path in _ARCHIVE_TIMESTAMP_PATHS:
        value: object = config
        for part in path:
            value = value.get(part) if isinstance(value, dict) else None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                continue
    return None

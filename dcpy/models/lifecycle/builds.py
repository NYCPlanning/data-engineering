from __future__ import annotations
from datetime import datetime
from enum import StrEnum
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, field_serializer
from typing import List

from dcpy.utils import versions
from dcpy.models.connectors.edm import recipes


class RecipeInputsVersionStrategy(StrEnum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class DataPreprocessor(BaseModel, extra="forbid"):
    module: str
    function: str


class InputDatasetDestination(StrEnum):
    postgres = "postgres"
    df = "df"
    file = "file"


class InputDataset(BaseModel, extra="forbid"):
    name: str
    version: str | None = None
    file_type: recipes.DatasetType | None = None
    version_env_var: str | None = None
    import_as: str | None = None
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination | None = None

    @property
    def is_resolved(self):
        return self.version is not None and self.version != "latest"

    @property
    def dataset(self):
        if self.version is None:
            raise Exception(f"Dataset {self.name} requires version")

        return recipes.Dataset(
            name=self.name, version=self.version, file_type=self.file_type
        )


class InputDatasetDefaults(BaseModel):
    file_type: recipes.DatasetType | None = None
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination = InputDatasetDestination.postgres


class RecipeInputs(BaseModel):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: List[InputDataset] = []
    dataset_defaults: InputDatasetDefaults | None = None


class Recipe(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    name: str
    product: str
    base_recipe: str | None = None
    version_type: versions.VersionSubType | None = None
    version_strategy: versions.VersionStrategy | None = None
    version: str | None = None
    vars: dict[str, str] | None = None
    inputs: RecipeInputs

    def is_resolved(self):
        return self.version is not None and (
            len(self.inputs.datasets) == 0
            or len([x for x in self.inputs.datasets if not x.is_resolved()]) == 0
        )


class ImportedDataset(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    name: str
    version: str
    file_type: recipes.DatasetType
    destination: str | pd.DataFrame | Path

    @staticmethod
    def from_input(
        ds: InputDataset, result: str | pd.DataFrame | Path
    ) -> ImportedDataset:
        assert ds.version, f"Version of {ds.name} not resolved"
        assert ds.file_type, f"File type of {ds.name} not resolved"
        return ImportedDataset(
            name=ds.name, version=ds.version, file_type=ds.file_type, destination=result
        )


class LoadResult(BaseModel, extra="forbid"):
    name: str
    build_name: str
    datasets: dict[str, ImportedDataset]


class EventType(StrEnum):
    BUILD = "build"
    PROMOTE_TO_DRAFT = "promote_to_draft"
    PUBLISH = "publish"


class EventLog(BaseModel, extra="forbid"):
    event: EventType
    product: str
    version: str
    path: str
    old_path: str | None
    timestamp: datetime
    runner_type: str
    runner: str
    custom_fields: dict = {}

    @field_serializer("timestamp")
    def _serialize_timestamp(self, timestamp: datetime, _info) -> str:
        return timestamp.isoformat()


class BuildMetadata(BaseModel, extra="forbid"):
    timestamp: datetime
    commit: str | None = None
    run_url: str | None = None
    version: str
    draft_revision_name: str | None = None
    recipe: Recipe

    def __init__(self, **data):
        if "version" not in data:
            recipe = data["recipe"]
            if recipe.version is not None:
                data["version"] = recipe.version
        super().__init__(**data)

    @field_serializer("timestamp")
    def _serialize_timestamp(self, timestamp: datetime, _info) -> str:
        return timestamp.isoformat()

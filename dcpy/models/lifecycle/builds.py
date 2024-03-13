from __future__ import annotations
from datetime import datetime
from enum import Enum
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List

from dcpy.utils import versions
from dcpy.models.connectors.edm import recipes


class RecipeInputsVersionStrategy(str, Enum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class DataPreprocessor(BaseModel, extra="forbid"):
    module: str
    function: str


class InputDatasetDestination(str, Enum):
    postgres = "postgres"
    df = "df"
    file = "file"


class InputDataset(BaseModel, use_enum_values=True, extra="forbid"):
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


class InputDatasetDefaults(BaseModel, use_enum_values=True):
    file_type: recipes.DatasetType | None = None
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination = Field(
        default=InputDatasetDestination.postgres, validate_default=True
    )


class RecipeInputs(BaseModel, use_enum_values=True):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: List[InputDataset] = []
    dataset_defaults: InputDatasetDefaults | None = None


class Recipe(
    BaseModel, use_enum_values=True, extra="forbid", arbitrary_types_allowed=True
):
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


class ImportedDataset(
    BaseModel, use_enum_values=True, extra="forbid", arbitrary_types_allowed=True
):
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


class LoadResult(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    build_name: str
    datasets: dict[str, ImportedDataset]


class BuildMetadata(BaseModel, extra="forbid"):
    timestamp: datetime
    commit: str | None = None
    run_url: str | None = None
    version: str
    recipe: Recipe

    def __init__(self, **data):
        if "version" not in data:
            recipe = data["recipe"]
            if recipe.version is not None:
                data["version"] = recipe.version
        super().__init__(**data)

    def dump(self):
        json = self.model_dump(exclude_none=True)
        json["timestamp"] = self.timestamp.isoformat()
        return json

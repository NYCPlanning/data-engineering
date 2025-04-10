from __future__ import annotations
from datetime import datetime
from enum import StrEnum
import pandas as pd
from pathlib import Path
from pydantic import AliasChoices, BaseModel, Field, model_validator
from typing import Any, List, Optional, ClassVar
from typing_extensions import Self

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
    id: str = Field(validation_alias=AliasChoices("id", "name"))
    version: str | None = None
    source: str | None = None
    file_type: recipes.DatasetType | None = None
    version_env_var: str | None = None
    import_as: str | None = None
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination | None = None
    custom: dict = Field(default_factory=dict)

    @property
    def is_resolved(self):
        return self.version is not None and self.version != "latest"

    @property
    def dataset(self):
        if self.version is None:
            raise ValueError(f"Dataset '{self.id}' requires version")

        return recipes.Dataset(
            id=self.id, version=self.version, file_type=self.file_type
        )


class InputDatasetDefaults(BaseModel):
    file_type: recipes.DatasetType | None = None
    source: str = "edm.recipes"
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination = InputDatasetDestination.postgres


class RecipeInputs(BaseModel):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: List[InputDataset] = []
    dataset_defaults: InputDatasetDefaults | None = None


class StageConfValue(BaseModel, extra="forbid"):
    UNRESOLVABLE_ERROR: ClassVar[str] = (
        "Stage Conf Value requires either `value` or `value_from`"
    )

    name: str
    value: str | None = None
    value_from: Optional[dict[str, str]] = None

    @model_validator(mode="after")
    def check_resolvable(self) -> Self:
        if not self.value and not self.value_from:
            raise ValueError(self.UNRESOLVABLE_ERROR)
        return self


class StageConf(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    destination: Optional[str] = None
    destination_key: Optional[str] = None
    connector_args: Optional[list[StageConfValue]] = []

    def get_connector_args_dict(self) -> dict[str, Any]:
        return {a.name: a.value for a in self.connector_args or []}


class Recipe(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    name: str
    product: str
    base_recipe: str | None = None
    version_type: versions.VersionSubType | None = None
    version_strategy: versions.VersionStrategy | None = None
    version: str | None = None
    vars: dict[str, str] | None = None
    inputs: RecipeInputs
    stage_config: dict[str, StageConf] = {}

    def is_resolved(self):
        return (
            self.version is not None
            and (
                len(self.inputs.datasets) == 0
                or len([x for x in self.inputs.datasets if not x.is_resolved()]) == 0
            )
            and not self.get_unresolved_conf_values()
        )

    def get_unresolved_conf_values(self):
        unresolved = []
        for stage_name, conf in self.stage_config.items():
            for conn_args in conf.connector_args or []:
                if conn_args.value_from and not conn_args.value:
                    unresolved.append(conn_args)
        return unresolved


class ImportedDataset(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    id: str
    version: str
    file_type: recipes.DatasetType
    destination: str | pd.DataFrame | Path
    destination_type: InputDatasetDestination | None = None

    @staticmethod
    def from_input(
        ds: InputDataset, result: str | pd.DataFrame | Path
    ) -> ImportedDataset:
        assert ds.version, f"Version of {ds.id} not resolved"
        assert ds.file_type, f"File type of {ds.id} not resolved"
        return ImportedDataset(
            id=ds.id,
            version=ds.version,
            file_type=ds.file_type,
            destination=result,
            destination_type=ds.destination,
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

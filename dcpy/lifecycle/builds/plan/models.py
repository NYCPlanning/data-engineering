from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import AliasChoices, BaseModel, Field, model_validator
from typing_extensions import Self

from dcpy.connectors.edm import models as recipes
from dcpy.utils import versions


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
    name: str | None = None
    version_env_var: str | None = None
    import_as: str | None = None
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination | None = None
    load_engine: str | None = None
    archive_date: date | None = None
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
    source: str = "edm.recipes.datasets"
    preprocessor: DataPreprocessor | None = None
    destination: InputDatasetDestination = InputDatasetDestination.postgres
    load_engine: str = "pandas"


class RecipeInputsVersionStrategy(StrEnum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class ExportFormat(StrEnum):
    """TODO - resolve this with recipes.DatasetType?"""

    csv = "csv"
    parquet = "parquet"
    shapefile = "shp"
    gdb = "gdb"
    dat = "dat"


class ExportDataset(BaseModel, extra="forbid"):
    """Assumed to come from postgres for now"""

    name: str
    filename: str | None = None
    format: ExportFormat
    custom: dict | None = None


class BuildExports(BaseModel, extra="forbid"):
    output_folder: Path | None = None
    zip_name: str | None = None
    datasets: list[ExportDataset] = []


class StageConfigValue(BaseModel, extra="forbid"):
    UNRESOLVABLE_ERROR: ClassVar[str] = (
        "Stage Conf Value requires either `value` or `value_from`"
    )

    name: str
    value: str | None = None
    value_from: dict[str, str] = {}

    @model_validator(mode="after")
    def check_resolvable(self) -> Self:
        if not self.value and not self.value_from:
            raise ValueError(self.UNRESOLVABLE_ERROR)
        return self


class CommandType(StrEnum):
    """Type of command execution."""

    shell = "shell"  # Execute as shell command
    python = "python"  # Import and execute as Python module


class BuildCommand(BaseModel, extra="forbid"):
    """A build command to execute during the build stage."""

    name: str
    run: str
    command_type: CommandType = CommandType.shell


class StageConfig(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    destination: str | None = None
    destination_key: str | None = None
    connector_args: list[StageConfigValue] = []
    commands: list[BuildCommand] = []

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
    exports: BuildExports | None = None
    stage_config: dict[str, StageConfig] = {}
    custom: dict[str, Any] | None = None

    def is_resolved(self) -> bool:
        return (
            self.version is not None
            and (
                len(self.inputs.datasets) == 0
                or len([x for x in self.inputs.datasets if not x.is_resolved]) == 0
            )
            and not self.get_unresolved_stage_config_values()
        )

    def get_unresolved_stage_config_values(self) -> list[StageConfigValue]:
        unresolved = []
        for _, conf in self.stage_config.items():
            for conn_args in conf.connector_args or []:
                if conn_args.value_from and not conn_args.value:
                    unresolved.append(conn_args)
        return unresolved


class RecipeInputs(BaseModel):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: list[InputDataset] = []
    dataset_defaults: InputDatasetDefaults | None = None

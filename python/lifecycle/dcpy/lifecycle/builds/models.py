from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
from pydantic import AliasChoices, BaseModel, Field, model_serializer, model_validator
from typing_extensions import Self

from dcpy.connectors.edm import models as recipes
from dcpy.utils import versions


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
    duckdb = "duckdb"


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
    url: str | None = None
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


class RecipeInputs(BaseModel):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: list[InputDataset] = []
    dataset_defaults: InputDatasetDefaults | None = None


class ExportFormat(StrEnum):
    """TODO - resolve this with recipes.DatasetType?"""

    csv = "csv"
    parquet = "parquet"
    shapefile = "shp"
    gdb = "gdb"
    geopackage = "gpkg"
    geoparquet = "geoparquet"
    dat = "dat"


# Formats where entries sharing a filename are written as layers of one file.
LAYERED_EXPORT_FORMATS = {ExportFormat.gdb, ExportFormat.geopackage}


class ExportDataset(BaseModel, extra="forbid"):
    """Assumed to come from postgres for now"""

    name: str
    filename: str | None = None
    format: ExportFormat
    custom: dict | None = None

    @property
    def output_filename(self) -> str:
        if self.filename:
            return self.filename
        if self.format in (ExportFormat.shapefile, ExportFormat.gdb):
            default_ext = "zip"
        elif self.format == ExportFormat.geoparquet:
            default_ext = "parquet"
        else:
            default_ext = self.format.value
        return f"{self.name}.{default_ext}"

    @property
    def layer_name(self) -> str:
        return (self.custom or {}).get("layer", self.name)


class BuildExports(BaseModel, extra="forbid"):
    output_folder: Path | None = None
    zip_name: str | None = None
    datasets: list[ExportDataset] = []

    @model_validator(mode="after")
    def _check_filename_collisions(self) -> Self:
        """Fail at plan time, not after the build, when two entries would write the
        same file. Only layers of one gdb or gpkg may share a filename."""
        by_filename: dict[str, list[ExportDataset]] = {}
        for ds in self.datasets:
            by_filename.setdefault(ds.output_filename, []).append(ds)

        errors = []
        for filename, entries in by_filename.items():
            formats = {ds.format for ds in entries}
            if len(formats) > 1:
                errors.append(
                    f"'{filename}' is written by more than one format: "
                    f"{sorted(f.value for f in formats)}"
                )
            elif formats <= LAYERED_EXPORT_FORMATS:
                layers = [ds.layer_name for ds in entries]
                duplicates = sorted({x for x in layers if layers.count(x) > 1})
                if duplicates:
                    errors.append(f"'{filename}' has duplicate layers: {duplicates}")
            elif len(entries) > 1:
                errors.append(
                    f"'{filename}' is written by {len(entries)} entries: "
                    f"{[ds.name for ds in entries]}"
                )
        if errors:
            raise ValueError(
                "Export filename collisions (set `filename` to disambiguate): "
                + "; ".join(errors)
            )
        return self


class Distribution(BaseModel, extra="forbid"):
    """Distribution configuration for publishing builds."""

    destination_ids: list[str] = []  # List of destination identifiers


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
    env: dict[str, str] = Field(default_factory=dict)  # Environment variables to set


class StageConfig(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    destination: str | None = None
    destination_key: str | None = None
    connector_args: list[StageConfigValue] = []
    env: dict[str, str] = Field(
        default_factory=dict
    )  # Stage-level environment variables
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
    branch: str | None = None  # Optional branch identifier for the build
    build_name: str | None = None  # Build identifier (e.g., branch name, partition key)
    env: dict[str, str] = Field(
        default_factory=dict
    )  # Recipe-level environment variables
    vars: dict[str, str] | None = Field(
        default=None,
        deprecated="'vars' field is deprecated. Use 'env' instead. Will be automatically migrated to 'env'.",
    )  # DEPRECATED: Use 'env' instead
    inputs: RecipeInputs
    exports: BuildExports | None = None
    distribution: Distribution | None = None
    stage_config: dict[str, StageConfig] = {}
    custom: dict[str, Any] | None = None

    @model_validator(mode="after")
    def migrate_vars_to_env(self):
        """Automatically migrate deprecated 'vars' field to 'env' field."""
        if self.vars:
            # Merge vars into env, with env taking precedence
            merged_env = {**self.vars, **self.env}
            self.env = merged_env
            # Clear vars after migration
            self.vars = None
        return self

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

    @model_serializer
    def _model_dump(self):
        return {
            "id": self.id,
            "version": self.version,
            "file_type": self.file_type,
            "destination_type": self.destination_type,
            "destination": self.destination
            if type(self.destination) is not pd.DataFrame
            else "dataframe",
        }


class LoadResult(BaseModel, extra="forbid"):
    name: str
    build_name: str
    datasets: dict[str, dict[str, ImportedDataset]]

    def get_dataset_versions(self, ds_name: str) -> list[str]:
        return list(self.datasets[ds_name].keys())

    def get_latest_version_str(self, ds_name: str) -> str:
        return self.get_dataset_versions(ds_name)[-1]

    def get_latest_version(self, ds_name: str) -> ImportedDataset:
        return self.datasets[ds_name][self.get_latest_version_str(ds_name)]


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
    load_result: LoadResult | None = None

    def __init__(self, **data):
        if "version" not in data:
            recipe = data["recipe"]
            if recipe.version is not None:
                data["version"] = recipe.version
        super().__init__(**data)

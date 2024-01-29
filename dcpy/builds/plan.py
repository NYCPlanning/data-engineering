import csv
from enum import Enum
import os
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import typer
from typing import List
import yaml

from dcpy.utils import versions
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing

DEFAULT_RECIPE = "recipe.yml"
LIBRARY_DEFAULT_PATH = recipes.LIBRARY_DEFAULT_PATH
RECIPE_FILE_TYPE_PREFERENCE = [
    recipes.DatasetType.pg_dump,
    recipes.DatasetType.parquet,
    recipes.DatasetType.csv,
]


class RecipeInputsVersionStrategy(str, Enum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class DataPreprocessor(BaseModel, use_enum_values=True, extra="forbid"):
    module: str
    function: str


class InputDataset(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    version: str | None = None
    file_type: recipes.DatasetType | None = None
    version_env_var: str | None = None
    import_as: str | None = None
    preprocessor: DataPreprocessor | None = None

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


def plan_recipe(recipe_path: Path, version: str | None = None) -> Recipe:
    """Plan recipe versions and file types for a product.

    Similar to pip freeze, determines recipe versions and file types to use for a build.
    A base_recipe may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.
    """
    recipe: Recipe = recipe_from_yaml(recipe_path)

    # Determine the recipe version
    if version is None and recipe.version is None:
        match recipe.version_strategy:
            case None:
                raise Exception("No version provided")
            case versions.SimpleVersionStrategy.first_of_month:
                recipe.version = versions.FirstOfMonth.generate().label
            case versions.SimpleVersionStrategy.bump_latest_release:
                recipe.version = versions.bump(
                    previous_version=publishing.get_latest_version(recipe.product),
                    bump_type=recipe.version_type,
                )
            case versions.BumpLatestRelease() as bump:
                recipe.version = versions.bump(
                    previous_version=publishing.get_latest_version(recipe.product),
                    bump_type=recipe.version_type,
                    bump_by=bump.bump_latest_release,
                )
    elif version is not None:
        recipe.version = version
    assert recipe.version is not None
    recipe.vars = recipe.vars or {}
    recipe.vars["VERSION"] = recipe.version

    # Determine previous version
    previous_recipe = publishing.try_get_previous_version(
        recipe.product, recipe.version
    )
    if previous_recipe is not None:
        recipe.vars["VERSION_PREV"] = previous_recipe.label

    # Add vars to environ so both can be accessed in environ
    os.environ.update(recipe.vars)

    # merge in base recipe inputs
    base_recipe = (
        recipe_from_yaml(recipe_path.parent / recipe.base_recipe)
        if recipe.base_recipe is not None
        else None
    )

    input_dataset_names = {d.name for d in recipe.inputs.datasets}
    if base_recipe is not None:
        for base_ds in base_recipe.inputs.datasets:
            if base_ds.name not in input_dataset_names:
                recipe.inputs.datasets.append(base_ds)

    # Fill in omitted versions
    previous_versions = {}
    if (
        recipe.inputs.missing_versions_strategy
        == RecipeInputsVersionStrategy.copy_latest_release
    ):
        previous_versions = publishing.get_source_data_versions(
            publishing.PublishKey(recipe.product, "latest")
        ).to_dict()["version"]

    for ds in recipe.inputs.datasets:
        if ds.version is None:
            if ds.version_env_var is not None:
                version = os.getenv(ds.version_env_var)
                if version is None:
                    raise Exception(
                        f"Dataset {ds.name} requires version env var: {ds.version_env_var}"
                    )
                ds.version = version
            elif (
                recipe.inputs.missing_versions_strategy
                == RecipeInputsVersionStrategy.copy_latest_release
            ):
                ds.version = previous_versions[ds.name]
            else:
                ds.version = "latest"

        if ds.version == "latest":
            ds.version = recipes.get_config(ds.name, "latest")["dataset"]["version"]

    # Determine the recipe file type
    for ds in recipe.inputs.datasets:
        if ds.dataset.file_type is None:
            ds.file_type = ds.dataset.assign_file_type(RECIPE_FILE_TYPE_PREFERENCE)
        ds.file_type = ds.dataset.file_type

    return recipe


def get_source_data_versions(recipe: Recipe):
    """Get source data versions table in form of [schema_name, v, file_type]."""
    return [["schema_name", "v", "file_type"]] + [
        [d.name, d.version, d.file_type] for d in recipe.inputs.datasets
    ]


def _apply_recipe_defaults(recipe: Recipe):
    if recipe.inputs.dataset_defaults is not None:
        for ds in recipe.inputs.datasets:
            ds.preprocessor = (
                ds.preprocessor or recipe.inputs.dataset_defaults.preprocessor
            )
            ds.file_type = ds.file_type or recipe.inputs.dataset_defaults.file_type


def recipe_from_yaml(path: Path) -> Recipe:
    """Import a recipe file from yaml, and validate schema."""
    with open(path, "r", encoding="utf-8") as f:
        s = yaml.safe_load(f)
        recipe = Recipe(**s)
        _apply_recipe_defaults(recipe)
        return recipe


def repeat_recipe_from_source_data_versions(
    version: str, source_data_versions: pd.DataFrame, template_recipe: Recipe
) -> Recipe:
    recipe = template_recipe.model_copy()
    recipe.version = version
    print(source_data_versions.columns)
    version_by_source_data_name = {
        name: row["version"] for name, row in source_data_versions.iterrows()
    }
    for ds in recipe.inputs.datasets:
        if ds.name in version_by_source_data_name:
            ds.version = version_by_source_data_name[ds.name]
        else:
            raise Exception(
                "Dataset found in template recipe not found in historical source data versions, \
                cannot repeat build."
            )

    return recipe


def plan(recipe_file: Path, version: str | None = None, repeat: bool = False) -> Path:
    lock_file = (
        recipe_file.parent / f"{recipe_file.stem}.lock.yml"
        if recipe_file
        else Path("recipe.lock.yml")
    )
    if not repeat:
        logger.info(f"Planning recipe from {recipe_file}")
        recipe = plan_recipe(recipe_file, version)
    else:
        if version is None:
            raise Exception("Version must be supplied if repeating a specific build")
        template_recipe = recipe_from_yaml(recipe_file)
        logger.info(f"Attempting to repeat recipe for {version}")
        product_key = publishing.PublishKey(template_recipe.product, version)
        if publishing.file_exists(product_key, "build_metadata.json"):
            with publishing.get_file(product_key, "build_metadata.json") as file:
                s = yaml.safe_load(file)["recipe"]
                recipe = Recipe(**s)
        elif publishing.file_exists(product_key, "source_data_versions.csv"):
            if not recipe_file:
                raise Exception(
                    "Template recipe file must be provided to repeat build from existing source_data_versions.csv"
                )
            source_data_versions = publishing.get_source_data_versions(product_key)
            recipe = repeat_recipe_from_source_data_versions(
                version, source_data_versions, template_recipe
            )

        else:
            raise Exception(
                "Neither 'build_metadata.json' nor 'source_data_versions.csv' can be found. Build cannot be repeated"
            )

    with open(lock_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing recipe lockfile to {str(lock_file.absolute())}")
        yaml.dump(recipe.model_dump(), f)

    return lock_file


def write_source_data_versions(recipe_file: Path):
    recipe = recipe_from_yaml(recipe_file)
    source_data_versions_path = recipe_file.parent / "source_data_versions.csv"
    logger.info(f"Writing source data versions to {source_data_versions_path}")

    sdv = get_source_data_versions(recipe)
    unresolved_versions = [[s, v, f] for s, v, f in sdv if v == "latest"]
    if len(unresolved_versions) > 0:
        exception = (
            "Recipe has unresolved versions! Can't write source "
            + f"data versions {unresolved_versions}"
        )
        logger.error(exception)
        raise Exception(exception)

    with open(source_data_versions_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for s, v, f in sdv:
            writer.writerow([s, v, f])


app = typer.Typer(add_completion=False)


@app.command("recipe")
def _cli_wrapper_plan_recipe(
    recipe_path: Path = typer.Option(
        Path(DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use",
    ),
    version=typer.Option(
        None,
        "-v",
        help="Version of dataset being built",
    ),
    repeat: bool = typer.Option(
        False, "--repeat", help="Repeat specific published build"
    ),
):
    plan(recipe_path, version, repeat)


if __name__ == "__main__":
    app()

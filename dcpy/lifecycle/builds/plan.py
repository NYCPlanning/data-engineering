import csv
import os
import pandas as pd
from pathlib import Path
import typer
import yaml

from dcpy.utils import versions
from dcpy.utils.logging import logger
from dcpy.models.lifecycle.builds import (
    Recipe,
    RecipeInputsVersionStrategy,
    InputDatasetDefaults,
)
from dcpy.connectors.edm import recipes, publishing

DEFAULT_RECIPE = "recipe.yml"
LIBRARY_DEFAULT_PATH = recipes.LIBRARY_DEFAULT_PATH
RECIPE_FILE_TYPE_PREFERENCE = [
    recipes.DatasetType.pg_dump,
    recipes.DatasetType.parquet,
    recipes.DatasetType.csv,
]


def resolve_version(recipe: Recipe) -> str:
    match recipe.version_strategy:
        case None:
            raise Exception("No version or version_strategy provided")
        case versions.SimpleVersionStrategy.first_of_month:
            return versions.generate_first_of_month().label
        case versions.SimpleVersionStrategy.bump_latest_release:
            previous_version = publishing.get_latest_version(recipe.product)
            assert previous_version is not None
            return versions.bump(
                previous_version=previous_version,
                bump_type=recipe.version_type,
            ).label
        case versions.BumpLatestRelease() as bump:
            previous_version = publishing.get_latest_version(recipe.product)
            assert previous_version is not None
            return versions.bump(
                previous_version=previous_version,
                bump_type=recipe.version_type,
                bump_by=bump.bump_latest_release,
            ).label
        case versions.PinToSourceDataset():
            dataset = recipe.version_strategy.pin_to_source_dataset
            inputs_by_name = {d.id: d for d in recipe.inputs.datasets}
            if dataset not in inputs_by_name:
                raise ValueError(
                    f"Cannot pin build version to dataset '{dataset}' as it is not an input dataset"
                )
            input = inputs_by_name[dataset]
            if not input.version and (
                input.version_env_var
                or (
                    recipe.inputs.missing_versions_strategy
                    != RecipeInputsVersionStrategy.find_latest
                )
            ):
                raise ValueError(
                    "To use 'pin to source dataset' version strategy, source input dataset must either be latest or explicit version"
                )
            return input.version or recipes.get_latest_version(dataset)


def plan_recipe(recipe_path: Path, version: str | None = None) -> Recipe:
    """Plan recipe versions and file types for a product.

    Similar to pip freeze, determines recipe versions and file types to use for a build.
    A base_recipe may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.
    """
    recipe: Recipe = recipe_from_yaml(recipe_path)

    # Determine the recipe version
    if version:
        recipe.version = version
    elif recipe.version is None:
        recipe.version = resolve_version(recipe)

    recipe.vars = recipe.vars or {}
    recipe.vars["VERSION"] = recipe.version

    # Determine previous version
    try:
        previous_recipe = publishing.get_previous_version(
            product=recipe.product, version=recipe.version
        )
        logger.info(
            f"Previous version of {recipe.product}: {previous_recipe.label} ({previous_recipe})"
        )
        recipe.vars["VERSION_PREV"] = previous_recipe.label
    except (
        LookupError,
        ValueError,
        TypeError,
    ) as e:  # versions not found, or don't parse correctly
        logger.error(f"Error: {e}")

    # Add vars to environ so both can be accessed in environ
    logger.info(f"Export envars: {recipe.vars}")
    os.environ.update(recipe.vars)

    # merge in base recipe inputs
    base_recipe = (
        recipe_from_yaml(recipe_path.parent / recipe.base_recipe)
        if recipe.base_recipe is not None
        else None
    )

    input_dataset_names = {d.id for d in recipe.inputs.datasets}
    if base_recipe is not None:
        for base_ds in base_recipe.inputs.datasets:
            if base_ds.id not in input_dataset_names:
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
                        f"Dataset {ds.id} requires version env var: {ds.version_env_var}"
                    )
                ds.version = version
            elif (
                recipe.inputs.missing_versions_strategy
                == RecipeInputsVersionStrategy.copy_latest_release
            ):
                ds.version = previous_versions[ds.id]
            else:
                ds.version = "latest"

        if ds.version == "latest":
            ds.version = recipes.get_latest_version(ds.id)

    # Determine the recipe file type
    for ds in recipe.inputs.datasets:
        ds.file_type = ds.file_type or recipes.get_preferred_file_type(
            ds.dataset, RECIPE_FILE_TYPE_PREFERENCE
        )

    return recipe


def get_source_data_versions(recipe: Recipe):
    """Get source data versions table in form of [schema_name, v, file_type]."""
    return [["schema_name", "v", "file_type"]] + [
        [d.id, d.version, d.file_type] for d in recipe.inputs.datasets
    ]


def _apply_recipe_defaults(recipe: Recipe):
    recipe.inputs.dataset_defaults = (
        recipe.inputs.dataset_defaults or InputDatasetDefaults()
    )
    for ds in recipe.inputs.datasets:
        ds.preprocessor = ds.preprocessor or recipe.inputs.dataset_defaults.preprocessor
        ds.file_type = ds.file_type or recipe.inputs.dataset_defaults.file_type
        ds.destination = ds.destination or recipe.inputs.dataset_defaults.destination


def recipe_from_yaml(path: Path) -> Recipe:
    """Import a recipe file from yaml, and validate schema."""
    with open(path, "r", encoding="utf-8") as f:
        s = yaml.safe_load(f)
        recipe = Recipe(**s)
        _apply_recipe_defaults(recipe)
        return recipe


def generate_lock_file(recipe_file: Path, recipe: Recipe) -> Path:
    lock_file = recipe_file.parent / f"{recipe_file.stem}.lock.yml"
    with open(lock_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing recipe lockfile to {str(lock_file.absolute())}")
        yaml.dump(recipe.model_dump(mode="json"), f)
    return lock_file


def plan(recipe_file: Path, version: str | None = None) -> Path:
    logger.info(f"Planning recipe from {recipe_file}")
    recipe = plan_recipe(recipe_file, version)
    lock_file = generate_lock_file(recipe_file, recipe)
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


def repeat_recipe_from_source_data_versions(
    version: str, source_data_versions: pd.DataFrame, template_recipe: Recipe
) -> Recipe:
    recipe = template_recipe.model_copy()
    recipe.version = version
    version_by_source_data_name = {
        name: row["version"] for name, row in source_data_versions.iterrows()
    }
    for ds in recipe.inputs.datasets:
        if ds.id in version_by_source_data_name:
            ds.version = version_by_source_data_name[ds.id]
        else:
            raise Exception(
                "Dataset found in template recipe not found in historical source data versions, \
                cannot repeat build."
            )

    return recipe


def repeat_build(
    product_key: publishing.ProductKey,
    recipe_file: Path | None = None,
    manual_version: str | None = None,
) -> Path:
    if publishing.file_exists(product_key, "build_metadata.json"):
        with publishing.get_file(product_key, "build_metadata.json") as file:
            s = yaml.safe_load(file)["recipe"]
            recipe = Recipe(**s)
    elif publishing.file_exists(product_key, "source_data_versions.csv"):
        logger.info(
            f"Attempting to repeat recipe for {product_key} from source_data_versions.csv"
        )

        if not (recipe_file and recipe_file.exists()):
            raise ValueError(
                "Recipe file for template must be supplied in if repeating an older build without build_metadata.json"
            )

        if isinstance(product_key, publishing.PublishKey) or isinstance(
            product_key, publishing.DraftKey
        ):
            version = product_key.version
        elif manual_version:
            version = manual_version
        else:
            raise ValueError(
                "Version must be supplied manually if repeating an older build without build_metadata.json"
            )

        template_recipe = recipe_from_yaml(recipe_file)
        source_data_versions = publishing.get_source_data_versions(product_key)
        print(source_data_versions)
        recipe = repeat_recipe_from_source_data_versions(
            version, source_data_versions, template_recipe
        )

    else:
        raise Exception(
            f"Neither 'build_metadata.json' nor 'source_data_versions.csv' can be found. '{product_key}' cannot be repeated"
        )

    recipe_file = recipe_file or Path(DEFAULT_RECIPE)
    lock_file = generate_lock_file(recipe_file, recipe)
    return lock_file


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
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
    repeat: bool = typer.Option(
        False, "--repeat", help="Repeat specific published build"
    ),
):
    plan(recipe_path, version)


@app.command("repeat")
def _cli_wrapper_repeat_recipe(
    product: str = typer.Argument(help="Name of the product to build"),
    product_type: str = typer.Option(
        None, "--product-type", "-t", help="Product/build type ('draft' or 'publish')"
    ),
    version_or_build: str = typer.Option(
        None,
        "--version-or-build",
        "-vb",
        help="Unique key for build/draft/publish build, either version or build name, respectively",
    ),
    draft_revision_number: int = typer.Option(
        None,
        "--draft-number",
        "-dn",
        help="If --product-type is 'draft', must provide draft revision number. Otherwise leave this blank",
    ),
    recipe_path: Path = typer.Option(
        Path(DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use. Only needed if attempting to rebuild from older builds without build_metadata.json",
    ),
    manual_version: str = typer.Option(
        None,
        "--manual-version",
        "--mv",
        help="Manually specified version. Only needed if attempting to rebuild and older draft where version cannot be easily determined.",
    ),
):
    product_key: publishing.BuildKey | publishing.DraftKey | publishing.PublishKey

    match product_type:
        case "build":
            product_key = publishing.BuildKey(
                product=f"db-{product}", build=version_or_build
            )
        case "draft":
            if draft_revision_number is None:
                raise ValueError(
                    "For repeating builds of 'draft' type, need to provide draft revision number"
                )
            draft_revision = publishing.get_draft_revision_label(
                product=f"db-{product}",
                version=version_or_build,
                revision_num=draft_revision_number,
            )
            product_key = publishing.DraftKey(
                product=f"db-{product}",
                version=version_or_build,
                revision=draft_revision,
            )
        case "publish":
            product_key = publishing.PublishKey(
                product=f"db-{product}", version=version_or_build
            )
        case _:
            raise ValueError(
                f"Invalid product/build type supplied: '{version_or_build}'. Only options are 'build', 'draft', or 'publish'"
            )
    repeat_build(product_key, recipe_path), manual_version


if __name__ == "__main__":
    app()

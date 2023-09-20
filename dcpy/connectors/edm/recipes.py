import os
import csv
from enum import Enum
import json
from pathlib import Path
from pydantic import BaseModel
import shutil
from sqlalchemy import text, update, Table, MetaData
import typer
from typing import Optional, List
import yaml


from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils import postgres
from dcpy.utils import git
from dcpy.utils import versions
from dcpy.utils.logging import logger

# In order to keep things sane, maybe we should allow recipes to import
# publishing but not the other way around?
from . import publishing

BUCKET = "edm-recipes"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com/datasets"
LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"


class RecipeInputsVersionStrategy(str, Enum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class VersionStrategy(str, Enum):
    bump_latest_release = "bump_latest_release"


class Dataset(BaseModel, use_enum_values=True):
    name: str
    version: Optional[str] = None
    import_as: Optional[str] = None

    def is_resolved(self):
        return self.version is not None and self.version != "latest"


class RecipeInputs(BaseModel, use_enum_values=True):
    missing_versions_strategy: Optional[RecipeInputsVersionStrategy] = None
    datasets: List[Dataset] = []


class DatasetVersionType(str, Enum):
    major = "major"
    minor = "minor"


class Recipe(BaseModel, use_enum_values=True):
    name: str
    product: str
    base_recipe: Optional[str] = None
    version_type: Optional[DatasetVersionType] = None
    version_strategy: Optional[VersionStrategy] = None
    version: Optional[str] = None
    inputs: RecipeInputs

    def is_resolved(self):
        return self.version is not None and (
            len(self.inputs.datasets) == 0
            or len([x for x in self.inputs.datasets if not x.is_resolved()]) == 0
        )


def get_dataset_sql_path(dataset: str, version: str = "latest"):
    return f"{BASE_URL}/{dataset}/{version}/{dataset}.sql"


def get_dataset_config_path(dataset: str, version: str = "latest"):
    return f"{BASE_URL}/{dataset}/{version}/config.json"


def get_config(name, version="latest"):
    """Retrieve a recipe config from s3."""
    obj = s3.client().get_object(
        Bucket=BUCKET, Key=f"datasets/{name}/{version}/config.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return json.loads(file_content)


def get_latest_version(name):
    """Retrieve a recipe config from s3."""
    return get_config(name)["dataset"]["version"]


def fetch_sql(ds: Dataset, local_library_dir):
    """Retrieve SQL dump file from edm-recipes. Returns fetched file's path."""
    target_dir = local_library_dir / "datasets" / ds.name / ds.version
    target_file_path = target_dir / (ds.name + ".sql")
    if (target_file_path).exists():
        print(f"✅ {ds.name}.sql exists in cache")
    else:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        print(f"🛠 {ds.name}.sql doesn't exists in cache, downloading")
        s3.download_file(
            bucket=BUCKET,
            key=f"datasets/{ds.name}/{ds.version}/{ds.name}.sql",
            path=target_file_path,
        )
    return target_file_path


def import_dataset(
    dataset: Dataset,
    pg_client: postgres.PostgresClient,
    *,
    local_library_dir=LIBRARY_DEFAULT_PATH,
):
    """Import a recipe to local data library folder and build engine."""
    logger.info(
        f"Importing {dataset.name} into {pg_client.database}.{pg_client.schema}"
    )
    if dataset.version == "latest" or dataset.version is None:
        raise Exception(
            f"Cannot import a dataset without a resolved version: {dataset}"
        )
    sql_script_path = fetch_sql(dataset, local_library_dir)

    postgres.execute_file_via_shell(pg_client.engine_uri, sql_script_path)

    with pg_client.engine.begin() as con:
        con.execute(text(f"ALTER TABLE {dataset.name} SET SCHEMA {pg_client.schema};"))

        con.execute(
            text(f"ALTER TABLE {dataset.name} ADD COLUMN data_library_version text;")
        )

        recipes_table = Table(dataset.name, MetaData(), autoload_with=con)
        con.execute(update(recipes_table).values(data_library_version=dataset.version))

        if dataset.import_as is not None:
            logger.info(f"Renaming table {dataset.name} to {dataset.import_as}")
            con.execute(
                text(f"ALTER TABLE {dataset.name} RENAME TO {dataset.import_as};")
            )


def plan_recipe(recipe_path: Path) -> Recipe:
    """Plan recipe versions for a product.

    Similar to pip freeze, determines recipe versions to use for a build.
    A base_recipe may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.
    """
    recipe: Recipe = recipe_from_yaml(recipe_path)

    # Determine the recipe version
    if recipe.version is None and recipe.version_strategy is not None:
        if recipe.version_strategy == VersionStrategy.bump_latest_release:
            if recipe.version_type is None:
                raise Exception("Recipe needs a 'version_type' to bump")
            prev_version = publishing.get_latest_version(recipe.product)
            recipe.version = versions.bump(
                prev_version, bumped_part=recipe.version_type
            )

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
            if (
                recipe.inputs.missing_versions_strategy
                == RecipeInputsVersionStrategy.copy_latest_release
            ):
                ds.version = previous_versions[ds.name]
            else:
                ds.version = "latest"

        if ds.version == "latest":
            ds.version = get_config(ds.name, "latest")["dataset"]["version"]

    return recipe


def get_source_data_versions(recipe: Recipe):
    """Get source data versions table in form of [schema_name, v]."""
    return [["schema_name", "v"]] + [
        [d.name, d.version] for d in recipe.inputs.datasets
    ]


def recipe_from_yaml(path: Path) -> Recipe:
    """Import a recipe file from yaml, and validate schema."""
    with open(path, "r", encoding="utf-8") as f:
        s = yaml.safe_load(f)
    return Recipe(**s)


app = typer.Typer(add_completion=False)

_typer_recipe_file_opt = typer.Option(
    None, "--recipe-file", "-f", help="Recipe file path"
)


@app.command()
def plan(
    recipe_file: Path = _typer_recipe_file_opt,
    lock_file: Path = typer.Option(
        None,
        "-o",
        "--lock-file",
        help="Lockfile path",
    ),
):
    logger.info("Planning recipe")

    recipe = plan_recipe(recipe_file)
    if lock_file is not None:
        with open(lock_file, "w", encoding="utf-8") as f:
            logger.info(f"Writing recipe lockfile to {str(lock_file.absolute())}")
            yaml.dump(recipe.model_dump(), f)
    else:
        print(recipe)


@app.command("import")
def import_datasets(planned_recipe_file: Path = _typer_recipe_file_opt):
    """Import all datasets from a recipe.

    Note, the recipe's datasets must not have unresolved versions, ie either
    'latest' or None.
    """
    logger.info("Importing Recipe Datasets")
    recipe = recipe_from_yaml(planned_recipe_file)
    pg_client = postgres.PostgresClient(
        schema=git.run_name(),
    )
    [import_dataset(ds, pg_client) for ds in recipe.inputs.datasets]


@app.command()
def purge_recipe_cache():
    """Delete locally stored recipe files."""
    logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
    shutil.rmtree(LIBRARY_DEFAULT_PATH)


@app.command()
def write_source_data_versions(recipe_file: Path = _typer_recipe_file_opt):
    recipe = recipe_from_yaml(recipe_file)
    source_data_versions_path = recipe_file.parent / "source_data_versions.csv"
    logger.info(f"Writing source data versions to {source_data_versions_path}")

    sdv = get_source_data_versions(recipe)
    unresolved_versions = [[k, v] for k, v in sdv if v == "latest"]
    if len(unresolved_versions) > 0:
        exception = (
            "Recipe has unresolved versions! Can't write source "
            + f"data versions {unresolved_versions}"
        )
        logger.error(exception)
        raise Exception(exception)

    with open(source_data_versions_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for key, value in sdv:
            writer.writerow([key, value])


if __name__ == "__main__":
    app()

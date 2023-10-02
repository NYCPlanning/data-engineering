import os
import csv
from enum import Enum
import importlib
import json
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import shutil
from sqlalchemy import text, update, Table, MetaData
from typing import List
import yaml

from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils import postgres
from dcpy.utils import versions
from dcpy.utils.logging import logger

# In order to keep things sane, maybe we should allow recipes to import
# publishing but not the other way around?
from . import publishing

BUCKET = "edm-recipes"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com/datasets"
LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"

BUILD_SCHEMA = os.environ.get("BUILD_ENGINE_SCHEMA", postgres.DEFAULT_POSTGRES_SCHEMA)


class RecipeInputsVersionStrategy(str, Enum):
    find_latest = "find_latest"
    copy_latest_release = "copy_latest_release"


class VersionStrategy(str, Enum):
    bump_latest_release = "bump_latest_release"


class DatasetType(str, Enum):
    pg_dump = "pg_dump"
    csv = "csv"


class DataPreprocessor(BaseModel, use_enum_values=True, extra="forbid"):
    module: str
    function: str


class Dataset(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    version: str | None = None
    version_env_var: str | None = None
    import_as: str | None = None
    file_type: DatasetType = None
    preprocessor: DataPreprocessor | None = None

    def is_resolved(self):
        return self.version is not None and self.version != "latest"

    @property
    def file_name(self) -> str:
        suffix = "sql" if self.file_type == DatasetType.pg_dump else self.file_type
        return f"{self.name}.{suffix}"

    @property
    def s3_key(self) -> str:
        return f"datasets/{self.name}/{self.version}/{self.file_name}"


class DatasetDefaults(BaseModel, use_enum_values=True):
    file_type: DatasetType | None = None
    preprocessor: DataPreprocessor | None = None


class RecipeInputs(BaseModel, use_enum_values=True):
    missing_versions_strategy: RecipeInputsVersionStrategy | None = None
    datasets: List[Dataset] = []
    dataset_defaults: DatasetDefaults = None


class DatasetVersionType(str, Enum):
    major = "major"
    minor = "minor"


class Recipe(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    product: str
    base_recipe: str | None = None
    version_type: DatasetVersionType | None = None
    version_strategy: VersionStrategy | None = None
    version: str | None = None
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


def fetch_dataset(ds: Dataset, local_library_dir=LIBRARY_DEFAULT_PATH) -> Path:
    """Retrieve dataset file from edm-recipes. Returns fetched file's path."""
    target_dir = local_library_dir / "datasets" / ds.name / ds.version
    target_file_path = target_dir / ds.file_name
    if (target_file_path).exists():
        print(f"✅ {ds.file_name} exists in cache")
    else:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        print(f"🛠 {ds.file_name} doesn't exists in cache, downloading")

        s3.download_file(
            bucket=BUCKET,
            key=ds.s3_key,
            path=target_file_path,
        )
    return target_file_path


def import_dataset(
    ds: Dataset,
    *,
    local_library_dir=LIBRARY_DEFAULT_PATH,
):
    """Import a recipe to local data library folder and build engine."""
    pg_client = postgres.PostgresClient(
        schema=BUILD_SCHEMA,
    )
    logger.info(f"Importing {ds.name} into {pg_client.database}.{pg_client.schema}")
    if ds.version == "latest" or ds.version is None:
        raise Exception(f"Cannot import a dataset without a resolved version: {ds}")

    local_dataset_path = fetch_dataset(ds, local_library_dir)

    match ds.file_type:
        case DatasetType.pg_dump:
            postgres.execute_file_via_shell(pg_client.engine_uri, local_dataset_path)
        case DatasetType.csv:
            df = pd.read_csv(local_dataset_path, dtype=str)
            if ds.preprocessor is not None:
                preproc_mod = importlib.import_module(ds.preprocessor.module)
                preproc_func = getattr(preproc_mod, ds.preprocessor.function)
                df = preproc_func(ds.name, df)
            table_name = ds.import_as or ds.name
            logger.info(f"Inserting dataframe into {table_name}")
            pg_client.insert_dataframe(df, table_name)

    with pg_client.engine.begin() as con:
        con.execute(
            text(
                f"ALTER TABLE {postgres.DEFAULT_POSTGRES_SCHEMA}.{ds.name} SET SCHEMA {pg_client.schema};"
            )
        )

        con.execute(
            text(f"ALTER TABLE {ds.name} ADD COLUMN data_library_version text;")
        )

        recipes_table = Table(ds.name, MetaData(), autoload_with=con)
        con.execute(update(recipes_table).values(data_library_version=ds.version))

        if ds.import_as is not None:
            logger.info(f"Renaming table {ds.name} to {ds.import_as}")
            con.execute(text(f"ALTER TABLE {ds.name} RENAME TO {ds.import_as};"))


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
            ds.version = get_config(ds.name, "latest")["dataset"]["version"]

    return recipe


def get_source_data_versions(recipe: Recipe):
    """Get source data versions table in form of [schema_name, v]."""
    return [["schema_name", "v"]] + [
        [d.name, d.version] for d in recipe.inputs.datasets
    ]


def _apply_recipe_defaults(recipe: Recipe) -> Recipe:
    for ds in recipe.inputs.datasets:
        ds.file_type = ds.file_type or recipe.inputs.dataset_defaults.file_type
        ds.preprocessor = ds.preprocessor or recipe.inputs.dataset_defaults.preprocessor


def recipe_from_yaml(path: Path) -> Recipe:
    """Import a recipe file from yaml, and validate schema."""
    with open(path, "r", encoding="utf-8") as f:
        s = yaml.safe_load(f)
        recipe = Recipe(**s)
        _apply_recipe_defaults(recipe)
        return recipe


def plan(recipe_file: Path) -> Path:
    logger.info("Planning recipe")
    lock_file = recipe_file.parent / f"{recipe_file.stem}.lock.yml"

    recipe = plan_recipe(recipe_file)

    with open(lock_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing recipe lockfile to {str(lock_file.absolute())}")
        yaml.dump(recipe.model_dump(), f)

    return lock_file


def write_source_data_versions(recipe_file: Path):
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


def purge_recipe_cache():
    """Delete locally stored recipe files."""
    logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
    shutil.rmtree(LIBRARY_DEFAULT_PATH)

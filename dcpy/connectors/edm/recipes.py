import argparse
import cerberus
import csv
import json
from pathlib import Path
import shutil
import sys
from typing import Optional
import yaml

from sqlalchemy import text, update, Table, MetaData


from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.utils import postgres
from dcpy.utils import versions

# In order to keep things sane, maybe we should allow recipes to import
# publishing but not the other way around?
from . import publishing

BUCKET = "edm-recipes"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com/datasets"
LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"

RECIPE_FILE_SCHEMA = {
    "name": {"type": "string", "required": True},
    "product": {"type": "string", "required": True},
    "base_recipe": {"type": "string"},
    "version_type": {"type": "string", "allowed": ["major", "minor"]},
    "version_strategy": {"type": "string", "allowed": ["bump_latest_release"]},
    "version": {"type": "string"},
    "inputs": {
        "type": "dict",
        "schema": {
            "missing_versions_strategy": {
                "type": "string",
                "allowed": ["copy_latest_release", "find_latest"],
            },
            "datasets": {
                "type": "list",
                "schema": {
                    "type": "dict",
                    "schema": {
                        "name": {"type": "string"},
                        "version": {"type": "string"},
                        "import_as": {"type": "string"},
                    },
                },
            },
        },
    },
}


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


def fetch_sql(name: str, version: str, local_library_dir):
    """Retrieve SQL dump file from edm-recipes. Returns fetched file's path."""
    target_dir = local_library_dir / "datasets" / name / version
    target_file_path = target_dir / (name + ".sql")
    if (target_file_path).exists():
        print(f"✅ {name}.sql exists in cache")
    else:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        print(f"🛠 {name}.sql doesn't exists in cache, downloading")
        s3.download_file(
            bucket=BUCKET,
            key=f"datasets/{name}/{version}/{name}.sql",
            path=target_file_path,
        )
    return target_file_path


def import_recipe_dataset(
    recipe_name: str,
    *,
    version="latest",
    local_library_dir=LIBRARY_DEFAULT_PATH,
    import_table_name: Optional[str] = None,
):
    """Import a recipe to local data library folder and build engine."""
    config = get_config(recipe_name, version)
    recipe_version = config["dataset"]["version"]
    sql_script_path = fetch_sql(recipe_name, recipe_version, local_library_dir)

    postgres.execute_file_via_shell(postgres.BUILD_ENGINE_RAW, sql_script_path)

    with postgres.build_engine.connect() as con:
        con.execute(
            text(f"ALTER TABLE {recipe_name} ADD COLUMN data_library_version text;")
        )
        con.commit()

        recipes_table = Table(recipe_name, MetaData(), autoload_with=con)
        con.execute(update(recipes_table).values(data_library_version=version))

        con.commit()

        if import_table_name is not None:
            logger.info(f"Renaming table {recipe_name} to {import_table_name}")
            con.execute(
                text(f"ALTER TABLE {recipe_name} RENAME TO {import_table_name};")
            )
            con.commit()


def _recipe_datasets(recipe):
    return recipe.get("inputs", {}).get("datasets", []) or []


def plan_recipe(recipe_path: Path) -> dict:
    """Plan recipe versions for a product.

    Similar to pip freeze, determines recipe versions to use for a build.
    A base_recipe may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.
    """
    recipe = recipe_from_yaml(recipe_path)

    if "version_strategy" in recipe:
        strat = recipe["version_strategy"]
        if strat == "bump_latest_release":
            prev_version = publishing.get_latest_version(recipe["product"])
            recipe["version"] = versions.bump(
                prev_version, bumped_part=recipe["version_type"]
            )

    # merge in base recipe inputs
    base_recipe = (
        recipe_from_yaml(recipe_path.parent / recipe["base_recipe"])
        if "base_recipe" in recipe
        else {}
    )

    input_dataset_names = {i["name"] for i in _recipe_datasets(recipe)}
    for ds in _recipe_datasets(base_recipe):
        if ds["name"] not in input_dataset_names:
            recipe["inputs"]["datasets"].append(ds)

    # Fill in omitted versions
    previous_versions = {}
    missing_version_strat = recipe["inputs"].get("missing_versions_strategy")
    if missing_version_strat == "copy_latest_release":
        previous_versions = publishing.get_source_data_versions(
            recipe["product"]
        ).to_dict()["version"]

    for ds in _recipe_datasets(recipe):
        if "version" not in ds:
            if missing_version_strat == "copy_latest_release":
                ds["version"] = previous_versions[ds["name"]]
            else:
                ds["version"] = "latest"

        if ds["version"] == "latest":
            ds["version"] = get_config(ds["name"], "latest")["dataset"]["version"]

    return recipe


def import_recipe_datasets(recipe):
    """Import all datasets specified in a recipe."""
    for rec in recipe["inputs"]["datasets"]:
        import_recipe_dataset(
            rec["name"], version=rec["version"], import_table_name=rec.get("import_as")
        )


def purge_recipe_cache(local_library_dir=LIBRARY_DEFAULT_PATH):
    """Delete locally stored recipe files."""
    shutil.rmtree(local_library_dir)


def get_source_data_versions(recipe):
    """Get source data versions table in form of [schema_name, v]."""
    return [["schema_name", "v"]] + [
        [d["name"], d["version"]] for d in _recipe_datasets(recipe)
    ]


def recipe_from_yaml(path: Path):
    """Import a recipe file from yaml, and validate schema."""
    v = cerberus.Validator(RECIPE_FILE_SCHEMA)
    with open(path, "r", encoding="utf-8") as f:
        s = yaml.safe_load(f)
    if v.validate(s):
        return s
    else:
        raise Exception(f"Invalid recipe file: {path}. {v.errors}")


if __name__ == "__main__":
    flags_parser = argparse.ArgumentParser()
    flags_parser.add_argument("cmd")
    cmd = sys.argv[1]

    if cmd == "plan":
        logger.info("Planning recipe")
        flags_parser.add_argument(
            "-f", "--recipe-file", help="Recipe file path", type=Path
        )
        flags_parser.add_argument(
            "-o",
            "--lock-file",
            help="Lockfile path",
            type=Path,
            required=False,
        )
        flags = flags_parser.parse_args()

        recipe = plan_recipe(Path(flags.recipe_file))
        if flags.lock_file is not None:
            with open(flags.lock_file, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing recipe lockfile to {str(flags.lock_file.absolute())}"
                )
                yaml.dump(recipe, f)
        else:
            print(recipe)

    elif cmd == "import":
        flags_parser.add_argument(
            "-f", "--recipe-file", help="Recipe file path", type=Path
        )
        flags = flags_parser.parse_args()

        with open(flags.recipe_file, "r", encoding="utf-8") as f:
            recipe = yaml.safe_load(f)
        logger.info("Importing Recipes")
        import_recipe_datasets(recipe)

    elif cmd == "purge-recipe-cache":
        logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
        purge_recipe_cache()

    elif cmd == "write-source-data-versions":
        flags_parser.add_argument(
            "-f", "--recipe-file", help="Recipe file path", type=Path
        )
        flags = flags_parser.parse_args()

        recipe = recipe_from_yaml(flags.recipe_file)
        source_data_versions_path = (
            flags.recipe_file.parent / "source_data_versions.csv"
        )
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

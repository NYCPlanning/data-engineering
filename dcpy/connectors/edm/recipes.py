import argparse
import copy
import csv
import json
from pathlib import Path
import shutil
import sys
import yaml

from sqlalchemy import text, update, Table, MetaData


from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.utils import postgres
from dcpy.utils import versions

# In order to keep things sane, we should allow recipes to import publishing
# but not the other way around
from . import publishing

BUCKET = "edm-recipes"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com/datasets"
LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"


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


def import_recipe(
    recipe_name: str,
    *,
    version="latest",
    local_library_dir=LIBRARY_DEFAULT_PATH,
    import_table_name: str = None,
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


def plan_build_versions(build_file: Path) -> dict:
    """Plan build versions for a product release.

    Similar to pip freeze, determines recipe versions to use for a build.
    A base_build may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.
    """
    with open(build_file, "r", encoding="utf-8") as f:
        build = yaml.safe_load(f)
    build_input_names = {i["name"] for i in build["recipe"]["inputs"]}
    merged_build = copy.deepcopy(build)

    if "version_strategy" in merged_build:
        strat = merged_build["version_strategy"]
        if strat == "bump_latest_release":
            prev_version = publishing.get_latest_version(
                merged_build["product"], branch="main"
            )
            merged_build["version"] = versions.bump(
                prev_version, bumped_part=merged_build["version_type"]
            )
        else:
            raise Exception(f"Invalid 'missing_version_strategy': {strat}")

    # merge in base build's recipe inputs
    base_build = {}
    if "base_build" in build:
        with open(build_file.parent / build["base_build"], "r", encoding="utf-8") as f:
            base_build = yaml.safe_load(f)

    for rec in base_build.get("recipe", {}).get("inputs", []):
        if rec["name"] not in build_input_names:
            merged_build["recipe"]["inputs"].append(rec)

    # Fill in omitted versions
    previous_versions = {}
    recipe = merged_build["recipe"]
    if recipe["missing_versions_strategy"] == "use_latest_release":
        previous_versions = publishing.get_source_data_versions(
            merged_build["product"], branch="main"
        ).to_dict()["version"]

    for rec in recipe["inputs"]:
        if "version" not in rec:
            if recipe["missing_versions_strategy"] == "use_latest_release":
                rec["version"] = previous_versions[rec["name"]]
            else:
                rec["version"] = "latest"

    for rec in recipe["inputs"]:
        if rec["version"] == "latest":
            rec["version"] = get_config(rec["name"], "latest")["dataset"]["version"]

    return merged_build


def import_build_datasets(build):
    """Import all datasets specified in a build."""
    # TODO was version setting important?
    # create_source_data_table()
    for rec in build["recipe"]["inputs"]:
        # import_recipe(rec['name'], version=rec['version'], set_version=True)
        import_recipe(
            rec["name"], version=rec["version"], import_table_name=rec.get("import_as")
        )


def purge_recipe_cache(local_library_dir=LIBRARY_DEFAULT_PATH):
    """Delete locally stored recipes."""
    shutil.rmtree(local_library_dir)


def get_source_data_versions(build):
    """Get source data versions table in form of [schema_name, v]."""
    return [["schema_name", "v"]] + [
        [x["name"], x["version"]] for x in build["recipe"]["inputs"]
    ]


if __name__ == "__main__":
    flags_parser = argparse.ArgumentParser()
    flags_parser.add_argument("cmd")

    if sys.argv[1] == "plan":
        logger.info("Planning build")
        flags_parser.add_argument(
            "-f", "--build-file", help="Build file path", type=Path
        )
        flags_parser.add_argument(
            "-o",
            "--lock-file",
            help="Lockfile path",
            type=Path,
            required=False,
        )
        flags = flags_parser.parse_args()

        build = plan_build_versions(Path(flags.build_file))
        if flags.lock_file is not None:
            with open(flags.lock_file, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing build lockfile to {str(flags.lock_file.absolute())}"
                )
                yaml.dump(build, f)
        else:
            print(build)

    elif sys.argv[1] == "import":
        flags_parser.add_argument(
            "-f", "--build-file", help="Build file path", type=Path
        )
        flags = flags_parser.parse_args()

        with open(flags.build_file, "r", encoding="utf-8") as f:
            build = yaml.safe_load(f)
        logger.info("Importing Recipes")
        import_build_datasets(build)

    elif sys.argv[1] == "purge-recipe-cache":
        logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
        purge_recipe_cache()

    elif sys.argv[1] == "write-source-data-versions":
        flags_parser.add_argument(
            "-f", "--build-file", help="Build file path", type=Path
        )
        flags = flags_parser.parse_args()

        with open(flags.build_file, "r", encoding="utf-8") as f:
            build = yaml.safe_load(f)
        source_data_versions_path = flags.build_file.parent / "source_data_versions.csv"
        logger.info(f"Writing source data versions to {source_data_versions_path}")

        sdv = get_source_data_versions(build)
        unresolved_versions = [[k, v] for k, v in sdv if v == "latest"]
        if len(unresolved_versions) > 0:
            exception = (
                "Build has unresolved versions! Can't write source "
                + f"data versions {unresolved_versions}"
            )
            logger.error(exception)
            raise Exception(exception)

        with open(source_data_versions_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for key, value in sdv:
                writer.writerow([key, value])

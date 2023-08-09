import copy
import json
from pathlib import Path
import yaml

from sqlalchemy import text, update, Table, MetaData


from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils import postgres

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


def plan_build_versions(build_file: Path):
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

    return merged_build


def import_build_datasets(build):
    """Import all datasets specified in a build."""
    # TODO was version setting important?
    # create_source_data_table()
    for rec in build["recipe"]["inputs"]:
        # import_recipe(rec['name'], version=rec['version'], set_version=True)
        import_recipe(rec["name"], version=rec["version"])

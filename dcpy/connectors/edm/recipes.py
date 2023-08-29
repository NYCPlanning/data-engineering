import json

from sqlalchemy import text, update, Table, MetaData

from dcpy import DCPY_ROOT_PATH
from dcpy.utils import s3
from dcpy.utils import postgres

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
        print("🛠 {name}.sql doesn't exists in cache, downloading")
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
    """
    Imports a recipe to local data library folder and build engine,
    and adds versioning info to the imported table.
    """
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

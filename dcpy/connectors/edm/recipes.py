from enum import Enum
import json
import os
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import shutil
from typing import Callable

from dcpy.utils import s3
from dcpy.utils import postgres
from dcpy.utils.logging import logger


BUCKET = "edm-recipes"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com/datasets"
LIBRARY_DEFAULT_PATH = (
    Path(os.environ.get("PROJECT_ROOT_PATH") or os.getcwd()) / ".library"
)


class DatasetType(str, Enum):
    pg_dump = "pg_dump"
    csv = "csv"
    parquet = "parquet"


_dataset_extensions = {"pg_dump": "sql", "csv": "csv", "parquet": "parquet"}


class Dataset(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    version: str
    file_type: DatasetType

    @property
    def file_name(self) -> str:
        return f"{self.name}.{_dataset_extensions[self.file_type]}"

    @property
    def s3_key(self) -> str:
        return f"datasets/{self.name}/{self.version}/{self.file_name}"


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


def read_csv(
    ds: Dataset, local_cache_dir: Path | None = None, **kwargs
) -> pd.DataFrame:
    """Read a recipe dataset csv as a pandas DataFrame."""
    if ds.file_type != DatasetType.csv:
        raise Exception(
            "csv output is currently only format that recipes.read_csv is implemented for"
        )
    if local_cache_dir:
        path = fetch_dataset(ds, local_library_dir=local_cache_dir)
        return pd.read_csv(path, **kwargs)
    else:
        with s3.get_file_as_stream(BUCKET, ds.s3_key) as stream:
            data = pd.read_csv(stream, **kwargs)
        return data


def import_dataset(
    ds: Dataset,
    pg_client: postgres.PostgresClient,
    *,
    local_library_dir=LIBRARY_DEFAULT_PATH,
    import_as: str | None = None,
    preprocessor: Callable[[str, pd.DataFrame], pd.DataFrame] | None = None,
):
    """Import a recipe to local data library folder and build engine."""
    ds_table_name = import_as or ds.name
    logger.info(
        f"Importing {ds.name} into {pg_client.database}.{pg_client.schema}.{ds_table_name}"
    )

    local_dataset_path = fetch_dataset(ds, local_library_dir)

    if ds.file_type == DatasetType.pg_dump:
        pg_client.import_pg_dump(
            local_dataset_path,
            pg_dump_table_name=ds.name,
            target_table_name=ds_table_name,
        )
    elif ds.file_type in (DatasetType.csv, DatasetType.parquet):
        df = (
            pd.read_csv(local_dataset_path, dtype=str)
            if ds.file_type == DatasetType.csv
            else pd.read_parquet(local_dataset_path)
        )
        if preprocessor is not None:
            df = preprocessor(ds.name, df)
        pg_client.insert_dataframe(df, ds_table_name)

    pg_client.add_table_column(
        ds_table_name,
        col_name="data_library_version",
        col_type="text",
        default_value=ds.version,
    )


def purge_recipe_cache():
    """Delete locally stored recipe files."""
    logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
    shutil.rmtree(LIBRARY_DEFAULT_PATH)

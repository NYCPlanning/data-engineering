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
    file_type: DatasetType | None = None

    @property
    def file_name(self) -> str:
        if self.file_type is None:
            raise Exception("File type must be defined to get file name")
        return f"{self.name}.{_dataset_extensions[self.file_type]}"

    @property
    def s3_folder(self) -> str:
        return f"datasets/{self.name}/{self.version}"

    @property
    def s3_key(self) -> str:
        return f"{self.s3_folder}/{self.file_name}"

    def assign_file_type(
        self, file_type_preferences: list[DatasetType], mutate=True
    ) -> DatasetType:
        file_type: DatasetType | None = None
        for _file_type in file_type_preferences:
            if s3.exists(
                BUCKET,
                f"datasets/{self.name}/{self.version}/{self.name}.{_dataset_extensions[_file_type]}",
            ):
                file_type = _file_type
                break
        if file_type is None:
            raise FileNotFoundError(
                f"No datasets of types {file_type_preferences} found in {self.s3_folder}"
            )
        if mutate:
            self.file_type = file_type
        return file_type


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


def pd_reader(file_type: DatasetType):
    match file_type:
        case DatasetType.csv:
            return pd.read_csv
        case DatasetType.parquet:
            return pd.read_parquet
        case _:
            raise Exception(f"Cannot read pandas dataframe from type {file_type}")


def read_df(ds: Dataset, local_cache_dir: Path | None = None, **kwargs) -> pd.DataFrame:
    """Read a recipe dataset parquet or csv file as a pandas DataFrame."""
    file_type = ds.file_type or ds.assign_file_type(
        [DatasetType.parquet, DatasetType.csv], mutate=False
    )
    reader = pd_reader(file_type)
    if local_cache_dir:
        path = fetch_dataset(ds, local_library_dir=local_cache_dir)
        return reader(path, **kwargs)
    else:
        with s3.get_file_as_stream(BUCKET, ds.s3_key) as stream:
            data = reader(stream, **kwargs)
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
        f"Importing {ds.name} {ds.file_type} into {pg_client.database}.{pg_client.schema}.{ds_table_name}"
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

        # make column names more sql-friendly
        columns = {
            column: column.strip().replace("-", "_").replace("'", "_").replace(" ", "_")
            for column in df.columns
        }
        df.rename(columns=columns, inplace=True)
        pg_client.insert_dataframe(df, ds_table_name)
        pg_client.add_pk(ds_table_name, "ogc_fid")

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

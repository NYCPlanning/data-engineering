from datetime import datetime
from io import BytesIO
import json
import os
import pandas as pd
from pathlib import Path
from pyarrow import parquet
from pydantic import BaseModel
import shutil
from tempfile import TemporaryDirectory
from typing import Callable, Any
import yaml

from dcpy import configuration
from dcpy.models.connectors.edm.recipes import (
    Dataset,
    DatasetType,
    DatasetKey,
    RawDatasetKey,
    ValidAclValues,
)
from dcpy.models import library
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.lifecycle import ingest
from dcpy.utils import s3, postgres
from dcpy.utils.geospatial import parquet as geoparquet
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert configuration.RECIPES_BUCKET, (
        "'RECIPES__bucket()' must be defined to use edm.recipes connector"
    )
    return configuration.RECIPES_BUCKET


LIBRARY_DEFAULT_PATH = (
    Path(os.environ.get("PROJECT_ROOT_PATH") or os.getcwd()) / ".library"
)
DATASET_FOLDER = "datasets"
RAW_FOLDER = "raw_datasets"
LOGGING_DB = "edm-qaqc"
LOGGING_SCHEMA = "source_data"
LOGGING_TABLE_NAME = "metadata_logging"


def s3_folder_path(ds: Dataset | DatasetKey) -> str:
    return f"{DATASET_FOLDER}/{ds.id}/{ds.version}"


def s3_file_path(ds: Dataset) -> str:
    return f"{s3_folder_path(ds)}/{ds.file_name}"


def s3_raw_folder_path(ds: RawDatasetKey) -> str:
    return f"{RAW_FOLDER}/{ds.id}/{ds.timestamp.isoformat()}"


def s3_raw_file_path(ds: RawDatasetKey) -> str:
    return f"{s3_raw_folder_path(ds)}/{ds.filename}"


def exists(ds: Dataset) -> bool:
    return s3.folder_exists(_bucket(), s3_folder_path(ds))


def archive_dataset(
    config: ingest.Config,
    file_path: Path,
    *,
    acl: ValidAclValues,
    raw: bool = False,
    latest: bool = False,
) -> None:
    """
    Given a config and a path to a file and an s3_path, archive it in edm-recipe
    It is assumed that s3_path has taken care of figuring out which top-level folder,
    how the dataset is being versioned, etc.
    """
    bucket = _bucket()
    s3_path = (
        s3_raw_folder_path(config.raw_dataset_key)
        if raw
        else s3_folder_path(config.dataset_key)
    )
    if s3.folder_exists(bucket, s3_path):
        raise Exception(
            f"Archived dataset at {s3_path} already exists, cannot overwrite"
        )
    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        shutil.copy(file_path, tmp_dir_path)
        with open(tmp_dir_path / "config.json", "w") as f:
            f.write(
                json.dumps(config.model_dump(exclude_none=True, mode="json"), indent=4)
            )
        s3.upload_folder(
            bucket,
            tmp_dir_path,
            Path(s3_path),
            acl=acl,
            contents_only=True,
        )
    if latest:
        assert not raw, "Cannot set raw dataset to 'latest'"
        set_latest(config.dataset_key, acl)


def set_latest(key: DatasetKey, acl):
    s3.copy_folder(
        _bucket(),
        f"{s3_folder_path(key)}/",
        f"{DATASET_FOLDER}/{key.id}/latest/",
        acl=acl,
    )


def update_freshness(ds: DatasetKey, timestamp: datetime) -> datetime:
    bucket = _bucket()
    path = f"{DATASET_FOLDER}/{ds.id}/{ds.version}/config.json"
    config = get_config(ds.id, ds.version)
    if isinstance(config, library.Config):
        raise TypeError(
            f"Cannot update freshness of dataset {ds} as it was archived by library, not ingest"
        )
    config.archival.check_timestamps.append(timestamp)
    config_str = json.dumps(config.model_dump(mode="json"))
    assert config.archival.acl, "Impossible - s3-archived dataset missing acl"
    s3.upload_file_obj(
        BytesIO(config_str.encode()),
        bucket,
        path,
        config.archival.acl,
        metadata=s3.get_custom_metadata(bucket, path),
    )
    return config.archival.archival_timestamp


def get_config(name: str, version="latest") -> library.Config | ingest.Config:
    """Retrieve a recipe config from s3."""
    bucket = _bucket()
    ds_conf_path = f"{DATASET_FOLDER}/{name}/{version}/config.json"
    logger.info(f"Retrieving config at {bucket}.{ds_conf_path}")
    obj = s3.get_file_as_stream(bucket, ds_conf_path)
    config = yaml.safe_load(obj)
    if "dataset" in config:
        return library.Config(**config)
    else:
        return ingest.Config(**config)


def try_get_config(dataset: Dataset) -> library.Config | ingest.Config | None:
    """Retrieve a recipe config object, if exists"""
    if not exists(dataset):
        return None
    else:
        return get_config(dataset.id, dataset.version)


def get_parquet_metadata(id: str, version="latest") -> parquet.FileMetaData:
    s3_fs = s3.pyarrow_fs()
    ds = parquet.ParquetDataset(
        f"{_bucket()}/{DATASET_FOLDER}/{id}/{version}/{id}.parquet", filesystem=s3_fs
    )

    assert len(ds.fragments) == 1, "recipes does not support multi-fragment datasets"
    return ds.fragments[0].metadata


def get_latest_version(name: str) -> str:
    """Retrieve a recipe config from s3."""
    return get_config(name).dataset.version


def get_all_versions(name: str) -> list[str]:
    """Get all versions of a specific recipe dataset"""
    return [
        folder
        for folder in s3.get_subfolders(_bucket(), f"{DATASET_FOLDER}/{name}/")
        if folder != "latest"
    ]


def _dataset_type_from_extension(s: str) -> DatasetType | None:
    match s:
        case "sql":
            return DatasetType.pg_dump
        case "csv":
            return DatasetType.csv
        case "parquet":
            return DatasetType.parquet
        case "xlsx":
            return DatasetType.xlsx
        case _:
            return None


def get_file_types(dataset: Dataset | DatasetKey) -> set[DatasetType]:
    files = s3.get_filenames(bucket=_bucket(), prefix=s3_folder_path(dataset))
    valid_types = {
        _dataset_type_from_extension(Path(file).suffix.strip(".")) for file in files
    }
    return {t for t in valid_types if t is not None}


def get_preferred_file_type(
    dataset: Dataset | DatasetKey, preferences: list[DatasetType]
) -> DatasetType:
    file_types = get_file_types(dataset)
    if len(file_types.intersection(preferences)) == 0:
        raise FileNotFoundError(
            f"Dataset {dataset.id} could not find filetype of any of {preferences}. Found filetypes for {dataset.id}: {file_types}"
        )
    return next(t for t in preferences if t in file_types)


def fetch_dataset(
    ds: Dataset,
    *,
    target_dir: Path,
    _target_dataset_path_override: Path | None = None,  # Hack
) -> Path:
    """Retrieve dataset file from edm-recipes. Returns fetched file's path."""
    target_dir = (
        _target_dataset_path_override
        # AR Note: we should refactor this, but punting for now. There are a few references
        # to `target_dir`, but at the moment I need a way to skip calculating the path
        or target_dir / DATASET_FOLDER / ds.id / ds.version
    )
    target_file_path = target_dir / ds.file_name
    if (target_file_path).exists():
        logger.info(f"✅ {ds.file_name} exists in cache")
    else:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        key = s3_file_path(ds)
        logger.info(f"🛠 {ds.file_name} doesn't exists in cache, downloading {key}")

        s3.download_file(
            bucket=_bucket(),
            key=key,
            path=target_file_path,
        )
    return target_file_path


def _pd_reader(file_type: DatasetType):
    match file_type:
        case DatasetType.csv:
            return pd.read_csv
        case DatasetType.parquet:
            return pd.read_parquet
        case _:
            raise Exception(f"Cannot read pandas dataframe from type {file_type}")


def read_df(
    ds: Dataset,
    local_cache_dir: Path | None = None,
    preferred_file_types: list[DatasetType] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Read a recipe dataset parquet or csv file as a pandas DataFrame."""
    preferred_file_types = preferred_file_types or [
        DatasetType.parquet,
        DatasetType.csv,
    ]
    ds.file_type = ds.file_type or get_preferred_file_type(ds, preferred_file_types)
    reader = _pd_reader(ds.file_type)
    if local_cache_dir:
        path = fetch_dataset(ds, target_dir=local_cache_dir)
        return reader(path, **kwargs)
    else:
        with s3.get_file_as_stream(_bucket(), s3_file_path(ds)) as stream:
            data = reader(stream, **kwargs)
        return data


def load_into_postgres(
    ds: Dataset,
    pg_client: postgres.PostgresClient,
    local_dataset_path: Path,
    *,
    local_library_dir=LIBRARY_DEFAULT_PATH,
    import_as: str | None = None,
    preprocessor: Callable[[str, pd.DataFrame], pd.DataFrame] | None = None,
):
    logger.info(f"preproc: {preprocessor}")
    ds_table_name = import_as or ds.id
    if ds.file_type == DatasetType.pg_dump:
        pg_client.import_pg_dump(
            local_dataset_path,
            pg_dump_table_name=ds.id,
            target_table_name=ds_table_name,
        )

    elif ds.file_type in (DatasetType.csv, DatasetType.parquet):
        df = (
            pd.read_csv(local_dataset_path, dtype=str)
            if ds.file_type == DatasetType.csv
            else geoparquet.read_df(local_dataset_path)
        )
        if preprocessor is not None:
            df = preprocessor(ds.id, df)

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

    return f"{pg_client.schema}.{ds_table_name}"


# TODO: Kill this after evaluating references in Ingest and the QAQC app
def import_dataset(
    ds: Dataset,
    pg_client: postgres.PostgresClient,
    *,
    local_library_dir=LIBRARY_DEFAULT_PATH,
    import_as: str | None = None,
    preprocessor: Callable[[str, pd.DataFrame], pd.DataFrame] | None = None,
) -> str:
    """Import a recipe to local data library folder and build engine."""
    assert ds.file_type, f"Cannot import dataset {ds.id}, no file type defined."
    logger.info(
        f"Importing {ds.id} {ds.file_type} into {pg_client.database}.{pg_client.schema}"
    )

    local_dataset_path = fetch_dataset(ds, target_dir=local_library_dir)
    return load_into_postgres(
        ds=ds,
        pg_client=pg_client,
        local_dataset_path=local_dataset_path,
        local_library_dir=local_library_dir,
        import_as=import_as,
        preprocessor=preprocessor,
    )


def purge_recipe_cache():
    """Delete locally stored recipe files."""
    logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
    shutil.rmtree(LIBRARY_DEFAULT_PATH)


def log_metadata(config: library.Config):
    # long term, it probably makes more sense to have db interactions isolated by environment as well
    # at that point, this is unnecessary
    if configuration.DEV_FLAG:
        logger.info("DEV_FLAG env var found, skipping metadata logging")
        return
    logger.info(f"Logging library run metadata for dataset {config.dataset.name}")
    assert config.execution_details, (
        f"Provided config for dataset {config.dataset.name} does not have run details specified. Cannot log."
    )
    pg_client = postgres.PostgresClient(database=LOGGING_DB, schema=LOGGING_SCHEMA)
    query = f"""
        INSERT INTO {LOGGING_SCHEMA}.{LOGGING_TABLE_NAME} (name, version, timestamp, runner, event_source)
        VALUES (:name, :version, :timestamp, :runner, 'library run')"""
    pg_client.execute_query(
        query,
        name=config.dataset.name,
        version=config.dataset.version,
        timestamp=config.execution_details.timestamp,
        runner=config.execution_details.runner_string,
    )


### Logic for QA source data dashboard


def get_archival_metadata(
    name: str, version: str | None = None
) -> library.ArchivalMetadata:
    if version is None:
        version = get_latest_version(name)
    logger.info(f"looking up metadata for {name}/{version}")
    config = get_config(name, version)
    match config:
        case library.Config():
            execution_details = config.execution_details
        case ingest.Config():
            execution_details = config.run_details
    if execution_details:
        timestamp = execution_details.timestamp
        runner = execution_details.runner_string
    else:
        s3metadata = s3.get_metadata(
            _bucket(),
            f"{DATASET_FOLDER}/{name}/{version}/config.json",
        )
        date_created = s3metadata.custom.get("date-created")
        if date_created is None:
            timestamp = s3metadata.last_modified
        else:
            timestamp = datetime.fromisoformat(date_created)
        runner = None
    return library.ArchivalMetadata(
        name=name,
        version=version,
        timestamp=timestamp,
        config=config.model_dump(mode="json"),
        runner=runner,
    )


def scrape_metadata(dataset: str) -> None:
    """For a given recipes dataset, regenerates logs"""
    logger.info(f"Re-scraping metadata for dataset {dataset}")
    pg_client = postgres.PostgresClient(database=LOGGING_DB, schema=LOGGING_SCHEMA)

    ## remove records in db for this recipe dataset
    try:
        pg_client.execute_query(
            f"""
            DELETE FROM {LOGGING_TABLE_NAME}
            WHERE name = :dataset AND event_source='scrape';""",
            dataset=dataset,
        )
    except Exception as e:
        logger.warn(f"Could not delete from table: {e}")

    logged_metadata = get_logged_metadata([dataset])
    logged_versions = {m["version"] for _, m in logged_metadata.iterrows()}

    ## scrape metadata, insert into table
    versions = get_all_versions(dataset)
    metadata = [
        get_archival_metadata(dataset, v).to_row()
        for v in versions
        if v not in logged_versions
        ## Todo - find a more elegant solution than this. These are malformed
        and v
        not in [
            "testor",
            "2023_executive_points",
            "2017_adopted_points",
            "2017_adopted_polygons",
        ]
        and not (dataset == "doitt_buildingfootprints" and v == "20230414")
        and not (dataset == "test_nypl_libraries")
    ]
    metadata_df = pd.DataFrame.from_records(metadata)
    metadata_df["event_source"] = "scrape"
    pg_client.insert_dataframe(
        metadata_df, table_name=LOGGING_TABLE_NAME, if_exists="append"
    )


def scrape_all_metadata(rerun_existing=True) -> None:
    datasets = s3.get_subfolders(_bucket(), "datasets")
    pg_client = postgres.PostgresClient(database=LOGGING_DB, schema=LOGGING_SCHEMA)
    scraped = pg_client.execute_select_query(
        f"select distinct name from {LOGGING_TABLE_NAME}"
    )
    for ds in datasets:
        if rerun_existing or ds not in list(scraped["name"]):
            scrape_metadata(ds)


def get_logged_metadata(datasets: list[str]) -> pd.DataFrame:
    pg_client = postgres.PostgresClient(database=LOGGING_DB, schema=LOGGING_SCHEMA)
    query = f"""
        SELECT
            name,
            version,
            timestamp,
            runner
        FROM
            {LOGGING_TABLE_NAME}
        WHERE
            name = ANY(:datasets)
    """
    return pg_client.execute_select_query(query, datasets=datasets)


class Connector(BaseModel, VersionedConnector):
    conn_type: str = "edm.recipes"

    def push(self, key: str, version, push_conf: dict | None = {}) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        pull_conf: dict | None = {},
    ) -> dict:
        assert pull_conf and "file_type" in pull_conf
        return {
            "path": fetch_dataset(
                Dataset(id=key, version=version, file_type=pull_conf["file_type"]),
                target_dir=Path(),
                _target_dataset_path_override=destination_path,
            )
        }

    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        return sorted(get_all_versions(name=key), reverse=sort_desc)

    def query_latest_version(self, key: str) -> str:
        return get_latest_version(name=key)

    def version_exists(self, key: str, version: str) -> bool:
        return exists(Dataset(id=key, version=version))

    def data_local_sub_path(
        self, key: str, version: str, pull_conf: Any | None = None
    ) -> Path:
        return Path("edm") / "recipes" / DATASET_FOLDER / key / version

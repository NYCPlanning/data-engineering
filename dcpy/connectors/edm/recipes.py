from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import os
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import shutil
from typing import Callable, Literal
import yaml

from dcpy.utils import s3, postgres, metadata
from dcpy.utils.logging import logger


BUCKET = "edm-recipes"
LIBRARY_DEFAULT_PATH = (
    Path(os.environ.get("PROJECT_ROOT_PATH") or os.getcwd()) / ".library"
)
LOGGING_DB = "edm-qaqc"
LOGGING_SCHEMA = "source_data"
LOGGING_TABLE_NAME = "metadata_logging"

ValidAclValues = Literal["public-read", "private"]
ValidSocrataFormats = Literal["csv", "geojson", "shapefile"]


class GeometryType(BaseModel):
    SRS: str | None = None
    type: Literal[
        "NONE",
        "GEOMETRY",
        "POINT",
        "LINESTRING",
        "POLYGON",
        "GEOMETRYCOLLECTION",
        "MULTIPOINT",
        "MULTIPOLYGON",
        "MULTILINESTRING",
        "CIRCULARSTRING",
        "COMPOUNDCURVE",
        "CURVEPOLYGON",
        "MULTICURVE",
        "MULTISURFACE",
    ]


class DatasetDefinition(BaseModel):
    name: str
    version: str
    acl: ValidAclValues
    source: SourceSection
    destination: DestinationSection
    info: InfoSection | None = None

    class SourceSection(BaseModel):
        url: Url | None = None
        script: Script | None = None
        socrata: Socrata | None = None
        layer_name: str | None = None
        geometry: GeometryType | None = None
        options: list[str] | None = None
        gdalpath: str | None = None

        class Url(BaseModel):
            path: str
            subpath: str = ""

        class Socrata(BaseModel):
            uid: str
            format: ValidSocrataFormats

        class Script(BaseModel, extra="allow"):
            name: str | None = None

    class DestinationSection(BaseModel):
        geometry: GeometryType
        options: list[str] | None = None
        fields: list[str] | None = None
        sql: str | None = None

    class InfoSection(BaseModel):
        info: str | None = None
        url: str | None = None
        dependents: list[str] | None = None

    @property
    def dataset(self) -> Dataset:
        return Dataset(name=self.name, version=self.version)


class DatasetType(str, Enum):
    pg_dump = "pg_dump"
    csv = "csv"
    parquet = "parquet"
    xlsx = "xlsx"  # needed for a few "legacy" products. Aim to phase out


## would prefer to have these be class methods but enum_as_value treats StrEnums as strings
## so class methods not usable
def _type_to_extension(dst: DatasetType) -> str:
    match dst:
        case DatasetType.pg_dump:
            return "sql"
        case DatasetType.csv:
            return "csv"
        case DatasetType.parquet:
            return "parquet"
        case DatasetType.xlsx:
            return "xlsx"
        case _:
            raise Exception(f"Unknown DatasetType '{dst}'")


def _type_from_extension(s: str) -> DatasetType | None:
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


class Config(BaseModel, extra="forbid"):
    dataset: DatasetDefinition
    execution_details: metadata.RunDetails | None = None

    @property
    def version(self) -> str:
        return self.dataset.version

    @property
    def sparse_dataset(self) -> Dataset:
        return self.dataset.dataset


class Dataset(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    version: str
    file_type: DatasetType | None = None

    @property
    def file_name(self) -> str:
        if self.file_type is None:
            raise Exception("File type must be defined to get file name")
        return f"{self.name}.{_type_to_extension(self.file_type)}"

    @property
    def s3_folder(self) -> str:
        return f"datasets/{self.name}/{self.version}"

    @property
    def s3_key(self) -> str:
        return f"{self.s3_folder}/{self.file_name}"

    def get_file_types(self) -> set[DatasetType]:
        files = s3.get_filenames(bucket=BUCKET, prefix=f"{self.s3_folder}/{self.name}")
        valid_types = {
            _type_from_extension(Path(file).suffix.strip(".")) for file in files
        }
        return {t for t in valid_types if t is not None}

    def get_preferred_file_type(self, preferences: list[DatasetType]) -> DatasetType:
        file_types = self.get_file_types()
        if len(file_types.intersection(preferences)) == 0:
            raise FileNotFoundError(
                f"Dataset {self.name} could not find filetype of any of {preferences}. Found filetypes for {self.name}: {file_types}"
            )
        return next(t for t in preferences if t in file_types)


@dataclass
class ArchivalMetadata:
    name: str
    version: str
    timestamp: datetime
    config: Config | None = None
    runner: str | None = None

    def to_row(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "timestamp": self.timestamp,
            "runner": self.runner,
        }


def archive(config: Config, file_path: Path):
    ### this could either take a config object and path to a file, and archive them both in a folder together,
    ### or take path to folder that takes dumped config as well as file output and loads whole folder
    ### likely the latter in case of multiple output formats. Not necessarily something we want to be doing long term
    ### but for flexibility for now it might make sense
    raise NotImplemented


def get_config(name: str, version="latest") -> Config:
    """Retrieve a recipe config from s3."""
    obj = s3.client().get_object(
        Bucket=BUCKET, Key=f"datasets/{name}/{version}/config.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return Config(**yaml.safe_load(file_content))


def get_latest_version(name: str) -> str:
    """Retrieve a recipe config from s3."""
    return get_config(name).dataset.version


def get_all_versions(name: str) -> list[str]:
    """Get all versions of a specific recipe dataset"""
    return [
        folder
        for folder in s3.get_subfolders(BUCKET, f"datasets/{name}/")
        if folder != "latest"
    ]


def get_archival_metadata(name: str, version: str | None = None) -> ArchivalMetadata:
    logger.info(f"looking up metadata for {name}/{version}")
    if version is None:
        version = get_latest_version(name)
    config = get_config(name, version)
    if config.execution_details:
        timestamp = config.execution_details.timestamp
        runner = config.execution_details.runner_string
    else:
        s3metadata = s3.get_metadata(BUCKET, f"datasets/{name}/{version}/config.json")
        date_created = s3metadata.custom.get("date_created")
        if date_created is None:
            timestamp = s3metadata.last_modified
        else:
            timestamp = datetime.fromisoformat(date_created)
        runner = None
    return ArchivalMetadata(
        name=name, version=version, timestamp=timestamp, config=config, runner=runner
    )


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
    ds.file_type = ds.file_type or ds.get_preferred_file_type(preferred_file_types)
    reader = _pd_reader(ds.file_type)
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
) -> str:
    """Import a recipe to local data library folder and build engine."""
    assert ds.file_type, f"Cannot import dataset {ds.name}, no file type defined."
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

    return f"{pg_client.schema}.{ds_table_name}"


def purge_recipe_cache():
    """Delete locally stored recipe files."""
    logger.info(f"Purging local recipes from {LIBRARY_DEFAULT_PATH}")
    shutil.rmtree(LIBRARY_DEFAULT_PATH)


def log_metadata(config: Config):
    logger.info(f"Logging library run metadata for dataset {config.dataset.name}")
    assert (
        config.execution_details
    ), f"Provided config for dataset {config.dataset.name} does not have run details specified. Cannot log."
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
    datasets = s3.get_subfolders(BUCKET, "datasets")
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

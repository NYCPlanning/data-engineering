import pandas as pd
from pathlib import Path

from dcpy import configuration
from dcpy.models.connectors.edm.recipes import (
    Dataset,
    DatasetType,
    DatasetKey,
)
from dcpy.utils import s3
from dcpy.utils.logging import logger


# TODO: continuing hacking away until this module is no more.


def _bucket() -> str:
    assert configuration.RECIPES_BUCKET, (
        "'RECIPES_BUCKET' must be defined to use edm.recipes connector"
    )
    return configuration.RECIPES_BUCKET


DATASET_FOLDER = "datasets"


def s3_folder_path(ds: Dataset | DatasetKey) -> str:
    return f"{DATASET_FOLDER}/{ds.id}/{ds.version}"


def s3_file_path(ds: Dataset) -> str:
    return f"{s3_folder_path(ds)}/{ds.file_name}"


def get_all_versions(name: str) -> list[str]:
    """Get all versions of a specific recipe dataset"""
    return [
        folder
        for folder in s3.get_subfolders(_bucket(), f"{DATASET_FOLDER}/{name}/")
        if folder != "latest"
    ]


def get_file_types(dataset: Dataset | DatasetKey) -> set[DatasetType]:
    files = s3.get_filenames(bucket=_bucket(), prefix=s3_folder_path(dataset))
    valid_types = {
        DatasetType.from_extension(Path(file).suffix.strip(".")) for file in files
    }
    return {t for t in valid_types if t is not None}


def get_preferred_file_type(
    dataset: Dataset | DatasetKey, preferences: list[DatasetType]
) -> DatasetType:
    file_types = get_file_types(dataset)
    if len(file_types) == 0:
        raise FileNotFoundError(
            f"No files found for dataset {dataset.id} {dataset.version}."
        )
    if len(file_types.intersection(preferences)) == 0:
        raise FileNotFoundError(
            f"No preferred file types found for dataset {dataset.id} {dataset.version}. Preferred file types: {preferences}. Found filetypes: {file_types}."
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

"""Miscellaneous ingestion related tasks"""

from pathlib import Path

from pandas import DataFrame
import pandas as pd
import yaml
from .data_library.metadata import add_version, dump_metadata
from . import BASE_URL


def add_leading_zero_PUMA(df: DataFrame) -> DataFrame:
    df["puma"] = "0" + df["puma"].astype(str)
    return df


def read_datasets_yml() -> dict:
    with open(
        Path(__file__).parent.parent / ("ingest/data_library/datasets.yml"), "r"
    ) as f:
        return yaml.safe_load(f.read())["datasets"]


def get_dataset_version(name: str) -> str:
    datasets = read_datasets_yml()
    dataset = next(filter(lambda x: x["name"] == name, datasets), None)
    assert dataset, f"{name} is not included as a dataset in datasets.yml"
    return str(dataset.get("version", "latest"))


def read_from_S3(name: str, category: str, cols: list = None) -> pd.DataFrame:
    read_version = get_dataset_version(name)
    df = pd.read_csv(
        f"{BASE_URL}/{name}/{read_version}/{name}.csv",
        dtype=str,
        index_col=False,
        usecols=cols,
    )
    add_version(dataset=name, version=read_version)
    dump_metadata(category)
    return df


def read_from_excel(
    file_path, category: str, sheet_name: str = None, columns: str = None, **kwargs
) -> pd.DataFrame:
    read_excel_args = {
        "io": file_path,
        "sheet_name": sheet_name,
        "usecols": columns,
    } | kwargs
    df = pd.read_excel(**read_excel_args)

    add_version(dataset=f"{file_path}/{sheet_name}", version="FILE_IN_REPO")
    dump_metadata(category)
    return df

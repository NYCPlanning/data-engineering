import os
from pathlib import Path

import pandas as pd
import yaml
from facdb import BASE_URL, CACHE_PATH

from dcpy.connectors.edm import recipes

from .metadata import add_version
from .utils import format_field_names, hash_each_row


def read_datasets_yml() -> dict:
    with open(Path(__file__).parent.parent / ("datasets.yml"), "r") as f:
        return yaml.safe_load(f.read())["datasets"]


def get_dataset_version(name: str) -> str:
    datasets = read_datasets_yml()
    dataset = next(filter(lambda x: x["name"] == name, datasets), None)
    assert dataset, f"{name} is not included as a dataset in datasets.yml"
    return str(dataset.get("version", "latest"))


def read_from_S3(name: str) -> pd.DataFrame:
    read_version = get_dataset_version(name)
    df = pd.read_csv(
        f"{BASE_URL}/{name}/{read_version}/{name}.csv", dtype=str, index_col=False
    )
    version_from_config(name, read_version)
    return df


def version_from_config(name, read_version):
    json_content = recipes.get_config(name, read_version)
    version = json_content["dataset"]["version"]
    if version.isnumeric():
        version = int(version)
    add_version(dataset=name, version=version)


def prepare(name: str) -> pd.DataFrame:
    pkl_path = CACHE_PATH / f"{name}.pkl"
    if not os.path.isfile(pkl_path):
        # pull from data library
        print(f"pulling from {name} data library")
        # fmt:off
        df = read_from_S3(name)\
            .pipe(hash_each_row)\
            .pipe(format_field_names)
        df.to_pickle(pkl_path)
        # fmt:on
    else:
        # read from cache
        print("reading from cached data")
        df = pd.read_pickle(pkl_path)

    df["source"] = name
    return df

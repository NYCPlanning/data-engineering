import argparse
from datetime import date
from typing import Tuple
from pathlib import Path

from dcpy.connectors.s3 import client, upload_file
from dcpy.utils import git_branch

from pipelines import PRODUCT_PATH


def parse_args() -> Tuple[str, str, bool]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="The ACS5 year, e.g. 2020 (2016-2020)",
        choices=["2010", "2020", "2021"],
    )
    parser.add_argument(
        "-g",
        "--geography",
        type=str,
        help="The geography year, e.g. 2010_to_2020",
    )
    parser.add_argument(
        "-u",
        "--upload",
        action="store_const",
        const=True,
        default=False,
        help="Uploads to Digital Ocean",
    )

    args = parser.parse_args()
    return args.year, args.geography, args.upload


def download_manual_update(update_type, version, overwrite=False):
    recipe_names = {"acs": "dcp_pop_acs", "decennial": "dcp_pop_decennial_dhc"}
    if update_type not in recipe_names:
        raise ValueError("'update_type' must either be 'acs' or 'decennial'")
    output_folder = (
        PRODUCT_PATH / f"factfinder/data/{update_type}_manual_updates/{version}"
    )
    filepath = output_folder / f"{recipe_names[update_type]}.xlsx"
    if not filepath.exists() or overwrite:
        print(f"Downloading {update_type} manual update data ...")
        filepath.parent.mkdir(parents=True, exist_ok=True)
        client().download_file(
            Bucket="edm-recipes",
            Key=f"datasets/{recipe_names[update_type]}/{version}/{recipe_names[update_type]}.xlsx",
            Filename=str(filepath),
        )
    else:
        print(f"{update_type} manual update data already downloaded locally.")
    return filepath


def s3_upload(file: Path, latest=True):
    export_type = file.stem
    year = file.parent.name
    prefix = Path("db-factfinder") / git_branch() / export_type / year
    key = prefix / str(date.today()) / file.name
    upload_file("edm-publishing", file, str(key), "public-read")
    if latest:
        ## much faster than uploading again
        client().copy_object(
            CopySource={"Bucket": "edm-publishing", "Key": str(key)},
            Bucket="edm-publishing",
            Key=str(prefix / "latest" / file.name),
            ACL="public-read",
        )

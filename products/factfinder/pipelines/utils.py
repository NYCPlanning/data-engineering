import argparse
from datetime import date
from typing import Tuple
from pathlib import Path

from dcpy.utils import s3, git
from dcpy.connectors.edm import publishing

from pipelines import PRODUCT_PATH


DATA_PATH = PRODUCT_PATH / "factfinder" / "data"


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
    output_folder = DATA_PATH / f"{update_type}_manual_updates" / version
    filepath = output_folder / f"{recipe_names[update_type]}.xlsx"
    if not filepath.exists() or overwrite:
        print(f"Downloading {update_type} manual update data ...")
        filepath.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(
            "edm-recipes",
            f"datasets/{recipe_names[update_type]}/{version}/{recipe_names[update_type]}.xlsx",
            filepath,
        )
    else:
        print(f"{update_type} manual update data already downloaded locally.")
    return filepath


def s3_upload(file: Path, latest=True):
    if file.suffix == ".json":
        export_type = file.parent.parent.name
    else:
        export_type = file.stem
    year = file.parent.name
    publishing.legacy_upload(
        output=file,
        publishing_folder="db-factfinder",
        version=str(date.today()),
        acl="public-read",
        s3_subpath=Path(git.branch()) / export_type / year,
        latest=latest,
    )

import argparse
from datetime import date
from typing import Tuple
from pathlib import Path

from dcpy.connectors.edm import publishing
from dcpy.builds import metadata


def parse_args() -> Tuple[str, str, bool]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="The ACS5 year, e.g. 2020 (2016-2020)",
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


def s3_upload(folder: Path, latest=True):
    year = folder.name
    output = folder.parent.name
    publishing.legacy_upload(
        output=folder,
        publishing_folder="db-factfinder",
        version=str(date.today()),
        acl="public-read",
        s3_subpath=str(Path(metadata.build_name()) / output / year),
        latest=latest,
        contents_only=True,
    )

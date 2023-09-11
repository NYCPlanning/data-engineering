import sys
import argparse
from pathlib import Path
import requests
from zipfile import ZipFile
import pandas as pd
from typing import Optional
import geopandas as gpd
from dataclasses import dataclass
from functools import cached_property

from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import build_metadata

BUCKET = "edm-publishing"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com"


@dataclass
class ProductKey:
    product: str
    version: str

    @property
    def label(self):
        return f"{self.product} - {self.version}"


@dataclass
class DraftKey(ProductKey):
    build: str

    def __init__(self, product: str, build: str):
        self.product = product
        self.build = build

    def __getattr__(self, name):
        if name == "version":
            return self.__version

        return self.__getattribute__(name)

    @cached_property
    def __version(self):
        url = _get_product_url(self) + "/version.txt"
        print(f"finding latest version from {url}")
        return requests.get(
            url,
            timeout=10,
        ).text.splitlines()[0]

    @property
    def label(self):
        return f"{self.product} - {self.version} - {self.build}"


def _get_product_path(product_key: ProductKey) -> str:
    if isinstance(product_key, DraftKey):
        return f"{product_key.product}/draft/{product_key.build}"
    else:
        return f"{product_key.product}/publish/{product_key.version}"


def _get_product_url(product_key: ProductKey) -> str:
    return f"{BASE_URL}/{_get_product_path(product_key)}"


def get_latest_version(product: str) -> str:
    """Given product name, gets latest version
    Assumes existence of version.txt in output folder
    """
    url = _get_product_url(ProductKey(product, "latest")) + "/version.txt"
    print(f"finding latest version from {url}")
    return requests.get(
        url,
        timeout=10,
    ).text.splitlines()[0]


def get_published_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/publish/"), reverse=True)


def get_draft_builds(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/draft/"), reverse=True)


def _get_source_data_versions_helper(url):
    source_data_versions = pd.read_csv(
        f"{url}/source_data_versions.csv",
        index_col=False,
        dtype=str,
    )
    source_data_versions.rename(
        columns={
            "schema_name": "datalibrary_name",
            "v": "version",
        },
        errors="raise",
        inplace=True,
    )
    source_data_versions.sort_values(
        by=["datalibrary_name"], ascending=True
    ).reset_index(drop=True, inplace=True)
    source_data_versions.set_index("datalibrary_name", inplace=True)
    return source_data_versions


def get_source_data_versions(product_key: ProductKey) -> pd.DataFrame:
    """Given product name, gets source data versions of published version"""
    output_url = _get_product_url(product_key)
    return _get_source_data_versions_helper(output_url)


def upload(
    output_path: Path,
    draft_key: DraftKey,
    *,
    acl: str,
    max_files: int = 20,
):
    """Upload build output(s) to draft folder in edm-publishing"""
    draft_path = _get_product_path(draft_key)
    meta = build_metadata.generate()
    if output_path.is_dir():
        s3.upload_folder(
            BUCKET,
            output_path,
            Path(draft_path),
            acl,
            include_foldername=False,
            max_files=max_files,
            metadata=meta,
        )
    else:
        s3.upload_file(
            "edm-publishing",
            output_path,
            f"{draft_path}/{output_path.name}",
            acl,
            metadata=meta,
        )


def legacy_upload(
    output: Path,
    publishing_folder: str,
    version: str,
    acl: str,
    *,
    s3_subpath: Optional[str] = None,
    latest: bool = True,
    include_foldername: bool = False,
    max_files: int = 20,
):
    """Upload file or folder to publishing, with more flexibility around s3 subpath
    Currently used only by db-factfinder"""
    if s3_subpath is None:
        prefix = Path(publishing_folder)
    else:
        prefix = Path(publishing_folder) / s3_subpath
    version_folder = prefix / version
    key = version_folder / output.name
    meta = build_metadata.generate()
    if output.is_dir():
        s3.upload_folder(
            BUCKET,
            output,
            key,
            acl,
            include_foldername=include_foldername,
            max_files=max_files,
            metadata=meta,
        )
        if latest:
            ## much faster than uploading again
            s3.copy_folder(
                BUCKET,
                str(version_folder),
                str(prefix / "latest"),
                acl,
                max_files=max_files,
            )
    else:
        s3.upload_file("edm-publishing", output, str(key), "public-read", metadata=meta)
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)


def publish(
    draft_key: DraftKey,
    *,
    acl: str,
    publishing_version: Optional[str] = None,
    keep_draft: bool = True,
    max_files: int = 30,
):
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if publishing_version is None:
        publishing_version = draft_key.version
    source = _get_product_path(draft_key)
    target = f"{draft_key.product}/publish/{publishing_version}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)
    if not keep_draft:
        s3.delete(BUCKET, source)


def _read_data_helper(path: str, filereader, **kwargs):
    with s3.get_file_as_stream(BUCKET, path) as stream:
        data = filereader(stream, **kwargs)
    return data


def read_csv(product_key: ProductKey, filepath: str, **kwargs):
    """Reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs
    """
    output_path = _get_product_path(product_key)
    return _read_data_helper(f"{output_path}/{filepath}", pd.read_csv, **kwargs)


def read_csv_legacy(dataset, version, filepath, **kwargs):
    with s3.get_file_as_stream(BUCKET, f"{dataset}/{version}/{filepath}") as stream:
        df = pd.read_csv(stream, **kwargs)
    return df


def read_shapefile(product_key: ProductKey, filepath: str):
    """Reads published shapefile into geopandas dataframe from edm-publishing"""
    output_path = _get_product_path(product_key)
    return _read_data_helper(f"{output_path}/{filepath}", gpd.read_file)


def get_zip(product_key: ProductKey, filepath: str):
    output_path = _get_product_path(product_key)
    stream = s3.get_file_as_stream(BUCKET, f"{output_path}/{filepath}")
    zip = ZipFile(stream)
    return zip


def publish_add_created_date(
    product: str,
    source: str,
    acl: str,
    *,
    publishing_version: Optional[str] = None,
    file_for_creation_date: str = "version.txt",
    max_files: int = 30,
):
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if publishing_version is None:
        with s3.get_file(BUCKET, f"{source}version.txt") as f:
            publishing_version = f.read()
    print(publishing_version)
    old_metadata = s3.get_metadata(BUCKET, f"{source}{file_for_creation_date}")
    target = f"{product}/publish/{publishing_version}/"
    s3.copy_folder(
        BUCKET,
        source,
        target,
        acl,
        max_files=max_files,
        metadata={
            "date_created": old_metadata["last-modified"].strftime("%Y-%m-%d %H:%M:%S")
        },
    )
    return {"date_created": old_metadata["last-modified"].strftime("%Y-%m-%d %H:%M:%S")}


if __name__ == "__main__":
    flags_parser = argparse.ArgumentParser()
    flags_parser.add_argument("cmd")
    cmd = sys.argv[1]

    if cmd == "upload":
        logger.info("Uploading product")
        flags_parser.add_argument(
            "-o", "--output-path", help="Path to local output folder", type=Path
        )
        flags_parser.add_argument(
            "-p",
            "--product",
            help="Name of data product (publishing folder in s3)",
            type=str,
        )
        flags_parser.add_argument("-b", "--build", help="Label of build", type=str)
        flags_parser.add_argument(
            "-a", "--acl", help="Access level of file in s3", type=str
        )
        flags = flags_parser.parse_args()
        logger.info(
            f'Uploading {flags.output_path} to {flags.product}/draft/{flags.build} with ACL "{flags.acl}"'
        )
        upload(
            Path(flags.output_path), DraftKey(flags.product, flags.build), acl=flags.acl
        )

    if cmd == "publish":
        logger.info("Publishing product")
        flags_parser.add_argument(
            "-p",
            "--product",
            help="Name of data product (publishing folder in s3)",
            type=str,
        )
        flags_parser.add_argument("-b", "--build", help="Label of build", type=str)
        flags_parser.add_argument(
            "-v", "--publishing-version", help="Version ", type=Path
        )
        flags_parser.add_argument(
            "-a", "--acl", help="Access level of file in s3", type=str
        )
        flags = flags_parser.parse_args()
        logger.info(
            f'Publishing {flags.product}/draft/{flags.build} with ACL "{flags.acl}"'
        )
        publish(
            DraftKey(flags.product, flags.build),
            acl=flags.acl,
            publishing_version=flags.get("publishing_version"),
        )

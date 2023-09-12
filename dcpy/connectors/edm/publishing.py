import sys
import argparse
from pathlib import Path
import pandas as pd
import geopandas as gpd
from io import BytesIO
from zipfile import ZipFile
from typing import Dict, Optional, Callable, TypeVar
from dataclasses import dataclass
from functools import cached_property

from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import build_metadata

BUCKET = "edm-publishing"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com"


T = TypeVar("T")


@dataclass
class Product:
    name: str
    version: str

    @property
    def label(self) -> str:
        return f"{self.name} - {self.version}"

    @property
    def __product_path(self) -> str:
        return f"{self.name}/publish/{self.version}"

    @cached_property
    def logged_version(self) -> str:
        return s3.get_file_as_text(
            BUCKET, f"{self.__product_path}/version.txt"
        ).splitlines()[0]

    @cached_property
    def source_data_versions(self) -> pd.DataFrame:
        source_data_versions = self.read_csv("source_data_versions.csv", dtype=str)
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

    def __get_file_as_stream(self, filepath: str) -> BytesIO:
        return s3.get_file_as_stream(BUCKET, f"{self.__product_path}/{filepath}")

    def __read_data_helper(
        self, filepath: str, filereader: Callable[[BytesIO], T], **kwargs
    ) -> T:
        with self.__get_file_as_stream(filepath) as stream:
            data = filereader(stream, **kwargs)
        return data

    def read_csv(self, filepath: str, **kwargs) -> pd.DataFrame:
        """Reads csv into pandas dataframe from edm-publishing
        Works for zipped or standard csvs

        Keyword arguments:
        product_key -- a key to find a specific instance of a data product
        filepath -- the filepath of the desired csv in the output folder
        """
        return self.__read_data_helper(filepath, pd.read_csv, **kwargs)

    def read_shapefile(self, filepath: str) -> gpd.GeoDataFrame:
        """Reads published shapefile into geopandas dataframe from edm-publishing

        Keyword arguments:
        product_key -- a key to find a specific instance of a data product
        filepath -- the filepath of the desired csv in the output folder
        """
        return self.__read_data_helper(filepath, gpd.read_file)

    def get_zip(self, filepath: str) -> ZipFile:
        """Reads zip file into ZipFile object from edm-publishing

        Keyword arguments:
        product_key -- a key to find a specific instance of a data product
        filepath -- the filepath of the desired csv in the output folder
        """
        stream = self.__get_file_as_stream(filepath)
        zip = ZipFile(stream)
        return zip


@dataclass
class Draft(Product):
    build: str

    def __init__(self, name: str, build: str):
        self.name = name
        self.build = build

    def __getattr__(self, name):
        if name == "version":
            return self.logged_version

        return self.__getattribute__(name)

    @property
    def label(self) -> str:
        return f"{self.name} - {self.version} - {self.build}"

    @property
    def __product_path(self) -> str:
        return f"{self.name}/draft/{self.build}"

    def upload(
        self,
        output_path: Path,
        *,
        acl: str,
        max_files: int = 20,
    ) -> None:
        """Upload build output(s) to draft folder in edm-publishing"""
        meta = build_metadata.generate()
        if output_path.is_dir():
            s3.upload_folder(
                BUCKET,
                output_path,
                Path(self.__product_path),
                acl,
                include_foldername=False,
                max_files=max_files,
                metadata=meta,
            )
        else:
            s3.upload_file(
                "edm-publishing",
                output_path,
                f"{self.__product_path}/{output_path.name}",
                acl,
                metadata=meta,
            )

    def publish(
        self,
        *,
        acl: str,
        publishing_version: Optional[str] = None,
        keep_draft: bool = True,
        max_files: int = 30,
    ) -> Product:
        """Publishes a specific draft build of a data product
        By default, keeps draft output folder"""
        if publishing_version is None:
            publishing_version = self.version
        source = self.__product_path
        target = f"{self.name}/publish/{publishing_version}/"
        s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)
        if not keep_draft:
            s3.delete(BUCKET, source)
        return Product(self.name, publishing_version)


def get_latest_version(product: str) -> str:
    """Given product name, gets latest version
    Assumes existence of version.txt in output folder
    """
    return Product(product, "latest").logged_version


def get_latest_source_versions(product: str) -> pd.DataFrame:
    return Product(product, "latest").source_data_versions


def get_published_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/publish/"), reverse=True)


def get_draft_builds(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/draft/"), reverse=True)


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
) -> None:
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


def read_csv_legacy(
    product: str, version: str, filepath: str, **kwargs
) -> pd.DataFrame:
    """Legacy function which reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs
    Replaced by read_csv which assumes outputs are in "publish" or "draft" folders in s3

    Keyword arguments:
    product -- the name of the data product
    version -- a specific version of a product
    filepath -- the filepath of the desired csv in the output folder
    """
    with s3.get_file_as_stream(BUCKET, f"{product}/{version}/{filepath}") as stream:
        df = pd.read_csv(stream, **kwargs)
    return df


def publish_add_created_date(
    product: str,
    source: str,
    acl: str,
    *,
    publishing_version: Optional[str] = None,
    file_for_creation_date: str = "version.txt",
    max_files: int = 30,
) -> Dict[str, str]:
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
        Draft(flags.product, flags.build).upload(Path(flags.output_path), acl=flags.acl)

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
        draft = Draft(flags.product, flags.build)
        logger.info(f'Publishing {draft.label} with ACL "{flags.acl}"')
        published_product = draft.publish(
            acl=flags.acl, publishing_version=flags.get("publishing_version")
        )
        logger.info(f"Published {draft.label} to {published_product.label}")

import os
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlencode, urljoin
import pandas as pd
import geopandas as gpd
from datetime import datetime
import pytz
from io import BytesIO
from zipfile import ZipFile
import typer
from typing import Callable, TypeVar
from dataclasses import dataclass

from dcpy.utils import s3
from dcpy.utils import git
from dcpy.utils.logging import logger

BUCKET = "edm-publishing"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com"
BASE_DO_URL = f"https://cloud.digitalocean.com/spaces/{BUCKET}"


class ProductKey(ABC):
    product: str

    @property
    @abstractmethod
    def path(self) -> str:
        raise NotImplementedError("ProductKey is an abstract class")


@dataclass
class PublishKey(ProductKey):
    product: str
    version: str

    def __str__(self):
        return f"{self.product} - {self.version}"

    @property
    def path(self) -> str:
        return f"{self.product}/publish/{self.version}"


@dataclass
class DraftKey(ProductKey):
    product: str
    build: str

    def __str__(self):
        return f"Draft: {self.product} - {self.build}"

    @property
    def path(self) -> str:
        return f"{self.product}/draft/{self.build}"


def get_version(product_key: ProductKey) -> str:
    """Given product key, gets version
    Assumes existence of version.txt in output folder
    """
    return s3.get_file_as_text(BUCKET, f"{product_key.path}/version.txt").splitlines()[
        0
    ]


def get_latest_version(product: str) -> str:
    """Given product name, gets latest version
    Assumes existence of version.txt in output folder
    """
    return get_version(PublishKey(product, "latest"))


def get_published_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/publish/"), reverse=True)


def get_draft_builds(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/draft/"), reverse=True)


def get_source_data_versions(product_key: ProductKey) -> pd.DataFrame:
    """Given product name, gets source data versions of published version"""
    source_data_versions = read_csv(product_key, "source_data_versions.csv", dtype=str)
    print(source_data_versions)
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


def generate_metadata() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date_created": datetime.now(pytz.timezone("America/New_York")).strftime(
            "%Y-%m-%d %H:%M:%S %z"
        )
    }
    metadata["commit"] = git.commit_hash()
    if os.environ.get("CI"):
        metadata["run_url"] = git.action_url()
    return metadata


def upload(
    output_path: Path,
    draft_key: DraftKey,
    *,
    acl: s3.ACL,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload build output(s) to draft folder in edm-publishing"""
    draft_path = draft_key.path
    meta = generate_metadata()
    if output_path.is_dir():
        s3.upload_folder(
            BUCKET,
            output_path,
            Path(draft_path),
            acl,
            contents_only=True,
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
    acl: s3.ACL,
    *,
    s3_subpath: str | None = None,
    latest: bool = True,
    contents_only: bool = False,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload file or folder to publishing, with more flexibility around s3 subpath
    Currently used only by db-factfinder"""
    if s3_subpath is None:
        prefix = Path(publishing_folder)
    else:
        prefix = Path(publishing_folder) / s3_subpath
    version_folder = prefix / version
    key = version_folder / output.name
    meta = generate_metadata()
    if output.is_dir():
        s3.upload_folder(
            BUCKET,
            output,
            key,
            acl,
            contents_only=contents_only,
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
    acl: s3.ACL,
    publishing_version: str | None = None,
    keep_draft: bool = True,
    max_files: int = s3.MAX_FILE_COUNT,
    latest: bool = False,
) -> None:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if publishing_version is None:
        publishing_version = get_version(draft_key)
    source = draft_key.path + "/"
    target = f"{draft_key.product}/publish/{publishing_version}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)
    if latest:
        target = f"{draft_key.product}/publish/latest/"
        s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)
    if not keep_draft:
        s3.delete(BUCKET, source)


def file_exists(product_key: ProductKey, filepath: str) -> bool:
    """Returns true if given file exists within outputs for given product key"""
    return s3.exists(bucket=BUCKET, key=f"{product_key.path}/{filepath}")


def get_file(product_key: ProductKey, filepath: str) -> BytesIO:
    """Returns file as BytesIO given product key and path within output folder"""
    return s3.get_file_as_stream(BUCKET, f"{product_key.path}/{filepath}")


T = TypeVar("T")


def _read_data_helper(path: str, filereader: Callable[[BytesIO], T], **kwargs) -> T:
    with s3.get_file_as_stream(BUCKET, path) as stream:
        data = filereader(stream, **kwargs)
    return data


def read_csv(product_key: ProductKey, filepath: str, **kwargs) -> pd.DataFrame:
    """Reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    return _read_data_helper(f"{product_key.path}/{filepath}", pd.read_csv, **kwargs)


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


def read_shapefile(product_key: ProductKey, filepath: str) -> gpd.GeoDataFrame:
    """Reads published shapefile into geopandas dataframe from edm-publishing

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    return _read_data_helper(f"{product_key.path}/{filepath}", gpd.read_file)


def get_zip(product_key: ProductKey, filepath: str) -> ZipFile:
    """Reads zip file into ZipFile object from edm-publishing

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    stream = s3.get_file_as_stream(BUCKET, f"{product_key.path}/{filepath}")
    zip = ZipFile(stream)
    return zip


def download_file(
    product_key: ProductKey, filepath: str, output_dir: Path | None = None
) -> Path:
    output_dir = output_dir or Path(".")
    output_filepath = output_dir / Path(filepath).name
    s3.download_file(BUCKET, f"{product_key.path}/{filepath}", output_filepath)
    return output_filepath


def publish_add_created_date(
    product: str,
    source: str,
    acl: s3.ACL,
    *,
    publishing_version: str | None = None,
    file_for_creation_date: str = "version.txt",
    max_files: int = s3.MAX_FILE_COUNT,
) -> dict[str, str]:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if publishing_version is None:
        with s3.get_file(BUCKET, f"{source}version.txt") as f:
            publishing_version = str(f.read())
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
            "date_created": old_metadata.last_modified.strftime("%Y-%m-%d %H:%M:%S")
        },
    )
    return {"date_created": old_metadata.last_modified.strftime("%Y-%m-%d %H:%M:%S")}


def get_data_directory_url(product_key: ProductKey) -> str:
    """Returns url of the data directory in Digital Ocean."""

    path = product_key.path
    if not path.endswith("/"):
        path += "/"
    endpoint = urlencode({"path": path})
    url = urljoin(BASE_DO_URL, "?" + endpoint)

    return url


app = typer.Typer(add_completion=False)


@app.command("upload")
def _cli_wrapper_upload(
    output_path: Path = typer.Option(
        Path("output"), "-o", "--output-path", help="Path to local output folder"
    ),
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
):
    acl_literal = s3.string_as_acl(acl)
    if not output_path.exists():
        raise Exception(f"Path {output_path} does not exist")
    build_name = build or os.environ.get("BUILD_NAME")
    if build_name is None:
        raise Exception(
            "Build name must either be supplied via CLI or found in env var 'BUILD_NAME'."
        )
    logger.info(
        f'Uploading {output_path} to {product}/draft/{build_name} with ACL "{acl}"'
    )
    upload(output_path, DraftKey(product, build_name), acl=acl_literal)


@app.command("publish")
def _cli_wrapper_publish(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    publishing_version: str = typer.Option(
        None,
        "-v",
        "--publishing-version",
        help="Version ",
    ),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Publish to latest folder as well?"
    ),
):
    acl_literal = s3.string_as_acl(acl)
    logger.info(
        f'Publishing {product}/draft/{build} as version {publishing_version} with ACL "{acl}"'
    )
    publish(
        DraftKey(product, build),
        acl=acl_literal,
        publishing_version=publishing_version,
        latest=latest,
    )


@app.command("download_file")
def _cli_wrapper_download_file(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    version: str = typer.Option(None, "-v", "--version", help="Product version"),
    filepath: str = typer.Option(
        None, "-f", "--filepath", help="Filepath within s3 output folder"
    ),
    output_dir: Path = typer.Option(
        None, "-o", "--output-dir", help="Folder to download file to"
    ),
):
    download_file(PublishKey(product, version), filepath, output_dir)


if __name__ == "__main__":
    app()

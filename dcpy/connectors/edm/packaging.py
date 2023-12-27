import os
import shutil
from pathlib import Path
from typing import Callable
from dataclasses import dataclass
import typer

from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import publishing

BUCKET = "edm-distributions"
BUCKET_ACL = s3.string_as_acl("public-read")

DOWNLOAD_ROOT_PATH = Path(".library")
OUTPUT_ROOT_PATH = Path(".output")


@dataclass
class PackageKey(publishing.ProductKey):
    product: str
    version: str

    def __str__(self):
        return f"{self.product} - {self.version}"

    @property
    def path(self) -> str:
        return f"{self.product}/{self.version}"


def colp(publish_key: publishing.PublishKey, package_key: PackageKey):
    raise NotImplementedError


def templatedb(publish_key: publishing.PublishKey, package_key: PackageKey):
    output_path = OUTPUT_ROOT_PATH / package_key.path
    shutil.copytree(
        DOWNLOAD_ROOT_PATH / publish_key.path,
        output_path,
        dirs_exist_ok=True,
    )
    shutil.copy(
        output_path / "templatedb.csv",
        output_path / "templatedb_packaged.csv",
    )


@dataclass
class PackageMetadata:
    packaged_name: str
    packaging_function: Callable


DATASET_PACKAGE_METADATA = {
    "db-colp": PackageMetadata("dcp_colp", colp),
    "db-template": PackageMetadata("dcp_template_db", templatedb),
}


def upload(package_key: PackageKey) -> None:
    local_folder_path = OUTPUT_ROOT_PATH / package_key.path
    meta = s3.generate_metadata()
    s3.upload_folder(
        BUCKET,
        local_folder_path,
        Path(package_key.path),
        acl=BUCKET_ACL,
        contents_only=True,
        metadata=meta,
    )


def get_packaged_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/"), reverse=True)


def transform_for_packaging(
    publish_key: publishing.PublishKey,
    package_key: PackageKey,
    packaging_function: Callable,
):
    packaging_function(publish_key, package_key)


def download_packaged_version(package_key: PackageKey) -> None:
    packaged_versions = get_packaged_versions(product=package_key.product)
    assert (
        package_key.version in packaged_versions
    ), f"{package_key} not found in S3 bucket '{BUCKET}'. Packaged versions are {packaged_versions}"
    s3.download_folder(BUCKET, f"{package_key.path}/", DOWNLOAD_ROOT_PATH)


def package(publish_key: publishing.PublishKey) -> None:
    # download published build
    logger.info(
        f"Downloading published build '{publish_key}' to {DOWNLOAD_ROOT_PATH.absolute()}"
    )
    publishing.download_published_version(publish_key, DOWNLOAD_ROOT_PATH)

    # perform product-specific packaging steps
    package_metadata = DATASET_PACKAGE_METADATA[publish_key.product]
    package_key = PackageKey(
        package_metadata.packaged_name,
        publish_key.version,
    )
    logger.info(
        f"Starting packaging for '{package_key}' to {OUTPUT_ROOT_PATH.absolute()}"
    )
    transform_for_packaging(
        publish_key, package_key, package_metadata.packaging_function
    )

    # upload packaged build to bucket
    logger.info(f"Uploading packaged '{package_key}'")
    upload(package_key)


app = typer.Typer(add_completion=False)


@app.command("package")
def _cli_wrapper_package(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Data product name (folder name in S3 publishing bucket)",
    ),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Data product release version",
    ),
):
    logger.info(f"Packaging {product} version {version}")
    publish_key = publishing.PublishKey(product, version)
    package(publish_key)


@app.command("placeholder")
def _placeholder():
    # If you only have one command defined, typer ignores the specified command name.
    assert False, "Please don't invoke me."


if __name__ == "__main__":
    app()

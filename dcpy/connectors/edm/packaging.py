from pathlib import Path
from dataclasses import dataclass
import typer

from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import publishing

BUCKET = "edm-publishing"
BUCKET_FOLDER = "product_datasets"
BUCKET_ACL: s3.ACL = "public-read"
PACKAGE_FOLDER = "package"

DOWNLOAD_ROOT_PATH = Path(".package")
OUTPUT_ROOT_PATH = Path(".output")


@dataclass
class PackageKey(publishing.ProductKey):
    product: str
    version: str
    dataset: str

    def __str__(self):
        return f"{self.product} - {self.version} - {self.dataset}"

    @property
    def path(self) -> str:
        return f"{BUCKET_FOLDER}/{self.product}/package/{self.version}/{self.dataset}"


def upload(local_folder_path: Path, package_key: PackageKey) -> None:
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
    return sorted(
        s3.get_subfolders(BUCKET, f"{BUCKET_FOLDER}/{product}/{PACKAGE_FOLDER}"),
        reverse=True,
    )


def download_packaged_version(package_key: PackageKey) -> None:
    packaged_versions = get_packaged_versions(product=package_key.product)
    assert (
        package_key.version in packaged_versions
    ), f"{package_key} not found in S3 bucket '{BUCKET}'. Packaged versions are {packaged_versions}"
    s3.download_folder(BUCKET, f"{package_key.path}/", DOWNLOAD_ROOT_PATH)


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
    # package(publish_key)


@app.command("versions")
def _list_packages(product_name):
    return print(get_packaged_versions(product_name))


@app.command("download")
def _download_packages(
    product_name,
    version,
    dataset: str = typer.Option(
        None,
        "-d",
        "--dataset",
        help="Dataset of the product",
    ),
):
    download_packaged_version(
        PackageKey(product_name, version, dataset or product_name)
    )

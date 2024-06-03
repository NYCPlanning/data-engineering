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

    def __str__(self):
        return f"{self.product} - {self.version}"

    @property
    def relative_path(self) -> str:
        return f"{self.product}/package/{self.version}"

    @property
    def path(self) -> str:
        return f"{BUCKET_FOLDER}/{self.relative_path}"


@dataclass
class DatasetPackageKey(publishing.ProductKey):
    product: str
    version: str
    dataset: str

    def __str__(self):
        return f"{self.product} - {self.version} - {self.dataset}"

    @property
    def relative_path(self) -> str:
        return f"{self.product}/package/{self.version}/{self.dataset}"

    @property
    def path(self) -> str:
        return f"{BUCKET_FOLDER}/{self.relative_path}"


def upload(local_folder_path: Path, package_key: DatasetPackageKey) -> None:
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


def pull(package_key: DatasetPackageKey | PackageKey) -> Path:
    """Pull a specific dataset or package. Returns local path of downloaded dataset."""
    packaged_versions = get_packaged_versions(product=package_key.product)
    assert (
        package_key.version in packaged_versions
    ), f"{package_key} not found in S3 bucket '{BUCKET}'. Packaged versions are {packaged_versions}"
    s3.download_folder(BUCKET, f"{package_key.path}/", DOWNLOAD_ROOT_PATH)
    return DOWNLOAD_ROOT_PATH / package_key.relative_path


dataset_app = typer.Typer()


@dataset_app.command("pull")
def _download_package_dataset(
    product_name: str = typer.Argument(),
    version: str = typer.Argument(),
    dataset: str = typer.Argument(),
):
    pull(DatasetPackageKey(product_name, version, dataset or product_name))


package_app = typer.Typer()
package_app.add_typer(dataset_app, name="dataset")


@package_app.command("package")
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


@package_app.command("versions")
def _list_packages(product_name):
    return print(get_packaged_versions(product_name))


@package_app.command("pull")
def _download_package(
    product_name: str = typer.Argument(),
    version: str = typer.Argument(),
):
    out_path = pull(PackageKey(product_name, version))
    print(f"downloaded to {out_path}")

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import pytz
from urllib.parse import urlencode, urljoin

from dcpy.configuration import (
    PUBLISHING_BUCKET,
    CI,
    BUILD_NAME,
)
from dcpy.connectors.registry import GenericConnector
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    BuildKey,
)
from dcpy.utils import s3, git
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


def generate_metadata() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    metadata["commit"] = git.commit_hash()
    if CI:
        metadata["run-url"] = git.action_url()
    return metadata


def exists(key: ProductKey) -> bool:
    return s3.folder_exists(_bucket(), key.path)


def upload(
    output_path: Path,
    build_key: BuildKey,
    *,
    acl: s3.ACL,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload build output(s) to build folder in edm-publishing"""
    meta = generate_metadata()
    if not output_path.is_dir():
        raise Exception("'upload' expects output_path to be a directory, not a file")
    s3.upload_folder(
        _bucket(),
        output_path,
        Path(build_key.path),
        acl,
        contents_only=True,
        max_files=max_files,
        metadata=meta,
    )


def upload_build(
    output_path: Path,
    product: str,
    *,
    acl: s3.ACL,
    build: str | None = None,
    max_files: int = s3.MAX_FILE_COUNT,
) -> BuildKey:
    """
    Uploads a product build to an S3 bucket.

    This function handles uploading a local output folder to a specified
    location in an S3 bucket. The path, product, and build name must be
    provided, along with an optional ACL (Access Control List) to control
    file access in S3.

    Raises:
        FileNotFoundError: If the provided output_path does not exist.
        ValueError: If the build name is not provided and cannot be found in the environment variables.
    """
    if not output_path.exists():
        raise FileNotFoundError(f"Path {output_path} does not exist")
    build_name = build or BUILD_NAME
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )
    build_key = BuildKey(product, build_name)
    logger.info(f'Uploading {output_path} to {build_key.path} with ACL "{acl}"')
    upload(output_path, build_key, acl=acl, max_files=max_files)
    return build_key


def get_data_directory_url(product_key: ProductKey) -> str:
    """Returns url of the data directory in Digital Ocean."""

    path = product_key.path
    if not path.endswith("/"):
        path += "/"
    endpoint = urlencode({"path": path})
    url = urljoin(f"https://cloud.digitalocean.com/spaces/{_bucket()}", "?" + endpoint)

    return url


class BuildsConnector(GenericConnector):
    conn_type: str = "edm.publishing.builds"

    def push(self, key: str, **kwargs) -> dict:
        connector_args = kwargs["connector_args"]

        logger.info(
            f"Pushing build for product: {key}, with note: {connector_args['build_note']}"
        )
        result = upload_build(
            output_path=kwargs["build_path"],
            product=key,
            acl=s3.string_as_acl(connector_args["acl"]),
            build=connector_args["build_note"],
        )
        return asdict(result)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        raise Exception("TODO")

    def data_local_sub_path(self, key: str, pull_conf) -> Path:
        assert pull_conf and "revision" in pull_conf
        return Path("edm") / "builds" / "datasets" / key / pull_conf["revision"]

from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pytz

from dcpy.configuration import (
    BUILD_NAME,
    CI,
    PUBLISHING_BUCKET,
    PUBLISHING_BUCKET_ROOT_FOLDER,
)
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.connectors.edm.publishing import (
    BuildKey,
)
from dcpy.utils import git, s3
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


_TEMP_PUBLISHING_FILE_SUFFIXES = {
    ".zip",
    ".parquet",
    ".csv",
    ".pdf",
    ".xlsx",
    ".json",
    ".text",
}


def get_builds(product: str) -> list[str]:
    """Get all build versions for a product."""
    return sorted(s3.get_subfolders(_bucket(), f"{product}/build/"), reverse=True)


class BuildsConnector(VersionedConnector, arbitrary_types_allowed=True):
    conn_type: str = "edm.publishing.builds"
    _storage: PathedStorageConnector | None = None

    def __init__(self, storage: PathedStorageConnector | None = None, **kwargs):
        """Initialize BuildsConnector with optional storage."""
        super().__init__(**kwargs)
        if storage is not None:
            self._storage = storage

    @property
    def storage(self) -> PathedStorageConnector:
        """Lazy-loaded storage connector. Only initializes when first accessed."""
        if self._storage is None:
            self._storage = PathedStorageConnector.from_storage_kwargs(
                conn_type="edm.publishing.builds",
                storage_backend=StorageType.S3,
                s3_bucket=_bucket(),
                root_folder=PUBLISHING_BUCKET_ROOT_FOLDER,
                _validate_root_path=False,
            )
        return self._storage

    @staticmethod
    def create() -> "BuildsConnector":
        """Create a BuildsConnector with lazy-loaded S3 storage."""
        return BuildsConnector()

    def _generate_metadata(self) -> dict[str, str]:
        """Generates "standard" s3 metadata for our files"""
        metadata = {
            "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
        }
        metadata["commit"] = git.commit_hash()
        if CI:
            metadata["run-url"] = git.action_url()
        return metadata

    def _upload_build(
        self,
        build_dir: Path,
        product: str,
        *,
        acl: s3.ACL | None = None,
        build_name: str | None = None,
        # max_files: int = s3.MAX_FILE_COUNT, # TODO
    ) -> BuildKey:
        """
        Uploads a product build to an S3 bucket using cloudpathlib.

        This function handles uploading a local output folder to a specified
        location in an S3 bucket. The path, product, and build name must be
        provided, along with an optional ACL (Access Control List) to control
        file access in S3.

        Raises:
            FileNotFoundError: If the provided output_path does not exist.
            ValueError: If the build name is not provided and cannot be found in the environment variables.
        """
        if not build_dir.exists():
            raise FileNotFoundError(f"Path {build_dir} does not exist")
        build_name = build_name or BUILD_NAME
        if not build_name:
            raise ValueError(
                f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
            )
        build_key = BuildKey(product, build_name)

        logger.info(f'Uploading {build_dir} to {build_key.path} with ACL "{acl}"')
        self.storage.push(
            key=build_key.path,
            filepath=str(build_dir),
            acl=str(acl),
            metadata=self._generate_metadata(),
        )

        return build_key

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        # For builds, the "version" is the build name/ID
        connector_args = kwargs["connector_args"]
        acl = (
            s3.string_as_acl(connector_args["acl"])
            if connector_args.get("acl")
            else None
        )

        logger.info(f"Pushing build for product: {key}, build: {version}")
        result = self._upload_build(
            build_dir=kwargs["build_path"],
            product=key,
            acl=acl,
            build_name=version,
        )
        return asdict(result)

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        filepath: str = "",
        **kwargs,
    ) -> dict:
        build_key = BuildKey(key, version)

        # Construct the source key for the file
        source_key = f"{build_key.path}/{filepath}"

        # Check if the file exists
        if not self.storage.exists(source_key):
            raise FileNotFoundError(f"File {source_key} not found")

        # Determine output path
        is_file_path = destination_path.suffix in _TEMP_PUBLISHING_FILE_SUFFIXES
        output_filepath = (
            destination_path / Path(filepath).name
            if not is_file_path
            else destination_path
        )

        logger.info(
            f"Downloading {build_key}, {filepath}, {source_key} -> {output_filepath}"
        )

        # Use PathedStorageConnector's pull method
        self.storage.pull(key=source_key, destination_path=output_filepath)
        return {"path": output_filepath}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self._pull(key, version, destination_path, **kwargs)

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        """List all build versions (build names) for a product."""
        build_folder_key = f"{key}/build"
        if not self.storage.exists(build_folder_key):
            return []

        return sorted(self.storage.get_subfolders(build_folder_key), reverse=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        """Builds don't have a meaningful 'latest' version concept."""
        raise NotImplementedError(
            "Builds don't have a meaningful 'latest' version. Use list_versions() to see available builds."
        )

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        """Check if a specific build exists."""
        return version in self.list_versions(key)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:
        return Path("edm") / "builds" / "datasets" / key / version

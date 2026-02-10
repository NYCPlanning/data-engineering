from pathlib import Path

from dcpy.configuration import PUBLISHING_BUCKET, PUBLISHING_BUCKET_ROOT_FOLDER
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.connectors.edm.publishing import PublishKey
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.published connector"
    )
    return PUBLISHING_BUCKET


# This is a (hopefully) temporary hack while we think about
# distinguishing filepaths vs directories in the connector interface.
_TEMP_PUBLISHING_FILE_SUFFIXES = {
    ".zip",
    ".parquet",
    ".csv",
    ".pdf",
    ".xlsx",
    ".json",
    ".text",
}


class PublishedConnector(VersionedConnector, arbitrary_types_allowed=True):
    conn_type: str = "edm.publishing.published"
    _storage: PathedStorageConnector | None = None

    def __init__(self, storage: PathedStorageConnector | None = None, **kwargs):
        """Initialize PublishedConnector with optional storage."""
        super().__init__(**kwargs)
        if storage is not None:
            self._storage = storage

    @property
    def storage(self) -> PathedStorageConnector:
        """Lazy-loaded storage connector. Only initializes when first accessed."""
        if self._storage is None:
            self._storage = PathedStorageConnector.from_storage_kwargs(
                conn_type="edm.publishing.published",
                storage_backend=StorageType.S3,
                s3_bucket=_bucket(),
                root_folder=PUBLISHING_BUCKET_ROOT_FOLDER,
                _validate_root_path=False,
            )
        return self._storage

    @staticmethod
    def create() -> "PublishedConnector":
        """Create a PublishedConnector with lazy-loaded S3 storage."""
        return PublishedConnector()

    def _download_file(
        self, product_key, filepath: str, output_dir: Path | None = None
    ) -> Path:
        """Download a file from storage using cloudpathlib."""
        output_dir = output_dir or Path(".")
        is_file_path = output_dir.suffix in _TEMP_PUBLISHING_FILE_SUFFIXES
        output_filepath = (
            output_dir / Path(filepath).name if not is_file_path else output_dir
        )
        logger.info(f"Downloading {product_key}, {filepath} -> {output_filepath}")

        source_key = f"{product_key.path}/{filepath}"
        if not self.storage.exists(source_key):
            raise FileNotFoundError(f"File {source_key} not found")

        # Use PathedStorageConnector's pull method
        self.storage.pull(key=source_key, destination_path=output_filepath)
        return output_filepath

    def _get_published_versions(
        self, key: str, exclude_latest: bool = True
    ) -> list[str]:
        """Get all published versions for a product using PathedStorageConnector."""
        product = key
        publish_folder_key = f"{product}/publish"
        if not self.storage.exists(publish_folder_key):
            return []

        # Get all version folders
        versions = self.storage.get_subfolders(publish_folder_key)

        # Filter out 'latest' if requested
        if exclude_latest:
            versions = [v for v in versions if v != "latest"]

        return sorted(versions, reverse=True)

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        filepath: str = "",
        dataset: str | None = None,
        **kwargs,
    ) -> dict:
        pub_key = PublishKey(key, version)

        s3_path = dataset + "/" if dataset else ""
        full_filepath = s3_path + filepath

        pulled_path = self._download_file(
            pub_key,
            full_filepath,
            output_dir=destination_path,
        )
        return {"path": pulled_path}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self._pull(key, version, destination_path, **kwargs)

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        """Push data to published folder with version structure.

        Args:
            key: Product name
            version: Version string (e.g., '1.0', '1.0.1', 'latest')
            **kwargs: Additional arguments including:
                - source_path: Local path to push from
                - acl: Access control level
                - target_path: Optional specific target path within version folder
        """
        product = key

        source_path = kwargs.get("source_path")
        if not source_path:
            raise ValueError("source_path is required for push_versioned")

        # Build the published path: {product}/publish/{version}/
        publish_folder_key = f"{product}/publish/{version}"

        # If target_path specified, use it; otherwise use the full folder
        target_path = kwargs.get("target_path", "")
        full_target_key = (
            f"{publish_folder_key}/{target_path}" if target_path else publish_folder_key
        )

        logger.info(f"Pushing to published: {source_path} -> {full_target_key}")

        # Use PathedStorageConnector's push method
        result = self.storage.push(
            key=full_target_key,
            filepath=source_path,
            **{
                k: v
                for k, v in kwargs.items()
                if k not in ["source_path", "target_path"]
            },
        )

        return result

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        versions = self._get_published_versions(key, **kwargs)
        return sorted(versions, reverse=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.list_versions(key)[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:  # type: ignore[override]
        return Path("edm") / "publishing" / "datasets" / key / version

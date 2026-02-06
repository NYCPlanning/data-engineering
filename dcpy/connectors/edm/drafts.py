from pathlib import Path

from dcpy.configuration import (
    PUBLISHING_BUCKET,
    PUBLISHING_BUCKET_ROOT_FOLDER,
)
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.connectors.edm.publishing import (
    DraftKey,
    ProductKey,
)
from dcpy.utils.logging import logger

_TEMP_PUBLISHING_FILE_SUFFIXES = {
    ".zip",
    ".parquet",
    ".csv",
    ".pdf",
    ".xlsx",
    ".json",
    ".text",
}


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


class DraftsConnector(VersionedConnector, arbitrary_types_allowed=True):
    conn_type: str = "edm.publishing.drafts"
    _storage: PathedStorageConnector | None = None

    def __init__(self, storage: PathedStorageConnector | None = None, **kwargs):
        """Initialize DraftsConnector with optional storage."""
        super().__init__(**kwargs)
        if storage is not None:
            self._storage = storage

    @property
    def storage(self) -> PathedStorageConnector:
        """Lazy-loaded storage connector. Only initializes when first accessed."""
        if self._storage is None:
            self._storage = PathedStorageConnector.from_storage_kwargs(
                conn_type="edm.publishing.drafts",
                storage_backend=StorageType.S3,
                s3_bucket=_bucket(),
                root_folder=PUBLISHING_BUCKET_ROOT_FOLDER,
                _validate_root_path=False,
            )
        return self._storage

    @staticmethod
    def create() -> "DraftsConnector":
        """Create a DraftsConnector with lazy-loaded S3 storage."""
        return DraftsConnector()

    def _parse_version(self, version: str) -> tuple[str, str]:
        """Parse version in format 'version.revision' into (version, revision)."""
        if "." not in version:
            raise ValueError(
                f"Version '{version}' should be in format 'version.revision'"
            )
        parts = version.rsplit(".", 1)  # Split on last dot
        return parts[0], parts[1]

    def _get_draft_versions(self, key: str) -> list[str]:
        """Get all draft revisions for a specific product. Key should be product name."""
        product = key
        draft_folder_key = f"{product}/draft"
        if not self.storage.exists(draft_folder_key):
            return []

        # Get all version folders
        version_folders = self.storage.get_subfolders(draft_folder_key)

        # Get all version.revision combinations
        versions = []
        for version_name in version_folders:
            revision_folder_key = f"{product}/draft/{version_name}"
            revision_folders = self.storage.get_subfolders(revision_folder_key)
            for revision_name in revision_folders:
                versions.append(f"{version_name}.{revision_name}")
        return sorted(versions, reverse=True)

    def _download_file(
        self, product_key: ProductKey, filepath: str, output_dir: Path | None = None
    ) -> Path:
        """Download a file from storage using PathedStorageConnector."""
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

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        revision: str,
        source_path: str = "",
        dataset: str | None = None,
        **kwargs,
    ) -> dict:
        """Pulls draft to destination path.

        `source_path` can be either a file or a directory. When it is a directory
        the contents of that directory will be copied recursively to destination_path
        """
        # key is product name, version is 'version.revision' format
        product = key

        draft_key = DraftKey(product, version=version, revision=revision)

        path_prefix = dataset + "/" if dataset else ""
        file_path = f"{path_prefix}{source_path}"
        logger.info(
            f"Pulling Draft for {draft_key}, path={file_path}, to={destination_path}"
        )
        pulled_path = self._download_file(
            draft_key, file_path, output_dir=destination_path
        )
        return {"path": pulled_path}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        version_parsed, revision_parsed = self._parse_version(version)
        # Remove revision from kwargs to avoid duplicate parameter
        kwargs.pop("revision", None)
        return self._pull(
            key,
            version=version_parsed,
            destination_path=destination_path,
            revision=revision_parsed,
            **kwargs,
        )

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        """Push data to drafts folder with version.revision structure.

        Args:
            key: Product name
            version: Version in format 'version.revision'
            **kwargs: Additional arguments including:
                - source_path: Local path to push from
                - acl: Access control level
                - target_path: Optional specific target path within version folder
        """
        product = key
        draft_version, revision = self._parse_version(version)

        source_path = kwargs.get("source_path")
        if not source_path:
            raise ValueError("source_path is required for push_versioned")

        # Build the draft path: {product}/draft/{version}/{revision}/
        draft_folder_key = f"{product}/draft/{draft_version}/{revision}"

        # If target_path specified, use it; otherwise use the full folder
        target_path = kwargs.get("target_path", "")
        full_target_key = (
            f"{draft_folder_key}/{target_path}" if target_path else draft_folder_key
        )

        logger.info(f"Pushing to draft: {source_path} -> {full_target_key}")

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
        logger.info(f"Listing versions for {key}")
        return self._get_draft_versions(key)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.list_versions(key)[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def data_local_sub_path(
        self, key: str, *, version: str, revision: str, **kwargs
    ) -> Path:
        product = key
        draft_version, draft_revision = self._parse_version(version)
        return (
            Path("edm")
            / "publishing"
            / "datasets"
            / product
            / draft_version
            / draft_revision
        )

import re
from pathlib import Path

from dcpy.configuration import (
    PUBLISHING_BUCKET,
)
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.registry import VersionedConnector
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


class GisDatasetsConnector(VersionedConnector, arbitrary_types_allowed=True):
    conn_type: str = "edm.publishing.gis"
    _storage: PathedStorageConnector | None = None

    def __init__(self, storage: PathedStorageConnector | None = None, **kwargs):
        """Initialize GisDatasetsConnector with optional storage."""
        super().__init__(**kwargs)
        if storage is not None:
            self._storage = storage

    @property
    def storage(self) -> PathedStorageConnector:
        """Lazy-loaded storage connector. Only initializes when first accessed."""
        if self._storage is None:
            self._storage = PathedStorageConnector.from_storage_kwargs(
                conn_type="edm.publishing.gis",
                storage_backend=StorageType.S3,
                s3_bucket=_bucket(),
                root_folder="datasets",
                _validate_root_path=False,
            )
        return self._storage

    @staticmethod
    def create() -> "GisDatasetsConnector":
        """Create a GisDatasetsConnector with lazy-loaded S3 storage."""
        return GisDatasetsConnector()

    def _gis_dataset_path(self, name: str, version: str) -> str:
        """Get the path to a GIS dataset file."""
        return f"{name}/{version}/{name}.zip"

    def _assert_gis_dataset_exists(self, name: str, version: str):
        """Assert that a GIS dataset exists in storage."""
        version = version.upper()
        path_key = self._gis_dataset_path(name, version)
        if not self.storage.exists(path_key):
            raise FileNotFoundError(f"GIS dataset {name} has no version {version}")

    def _get_gis_dataset_versions(
        self, dataset_name: str, sort_desc: bool = True
    ) -> list[str]:
        """
        Get all versions of GIS-published dataset in storage.
        """
        gis_version_formats = [r"^\d{2}[A-Z]$", r"^\d{8}$"]
        subfolders = []
        matched_formats = set()

        # Check if dataset folder exists
        if not self.storage.exists(dataset_name):
            raise AssertionError(f"No Dataset named {dataset_name} found.")

        # Get subfolders for the dataset
        folder_names = self.storage.get_subfolders(dataset_name)

        for folder_name in folder_names:
            for pattern in gis_version_formats:
                if re.match(pattern, folder_name):
                    subfolders.append(folder_name)
                    matched_formats.add(pattern)
                    break

        if subfolders and len(matched_formats) > 1:
            raise ValueError(
                f"Multiple version formats found for gis dataset {dataset_name}. Cannot determine latest version"
            )
        return sorted(subfolders, reverse=sort_desc)

    def _get_latest_gis_dataset_version(self, dataset_name: str) -> str:
        """
        Get latest version of GIS-published dataset in storage.
        """
        versions = self._get_gis_dataset_versions(dataset_name)
        if not versions:
            raise FileNotFoundError(f"No versions found for GIS dataset {dataset_name}")
        version = versions[0]
        self._assert_gis_dataset_exists(dataset_name, version)
        return version

    def _download_gis_dataset(
        self, dataset_name: str, version: str, target_folder: Path
    ):
        """
        Download GIS-published dataset from storage to target folder.
        """
        version = version.upper()
        if not target_folder.is_dir():
            raise ValueError(f"Target folder '{target_folder}' is not a directory")

        self._assert_gis_dataset_exists(dataset_name, version)

        source_key = self._gis_dataset_path(dataset_name, version)
        file_path = target_folder / f"{dataset_name}.zip"

        # Use PathedStorageConnector's pull method which handles all storage types
        self.storage.pull(key=source_key, destination_path=file_path)

        return file_path

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        pulled_path = self._download_gis_dataset(
            dataset_name=key, version=version, target_folder=destination_path
        )
        return {"path": pulled_path}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise PermissionError(
            "Currently, only GIS team pushes to edm-publishing/datasets"
        )

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        logger.info(f"Listing versions for {key}")
        return self._get_gis_dataset_versions(key, sort_desc=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self._get_latest_gis_dataset_version(key)

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

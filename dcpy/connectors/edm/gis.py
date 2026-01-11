from pathlib import Path
import re

from dcpy.configuration import (
    PUBLISHING_BUCKET,
)
from dcpy.connectors.registry import VersionedConnector
from dcpy.utils import s3
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


def _gis_dataset_path(name: str, version: str) -> str:
    return f"datasets/{name}/{version}/{name}.zip"


def _assert_gis_dataset_exists(name: str, version: str):
    bucket = _bucket()
    version = version.upper()
    if not s3.object_exists(bucket, _gis_dataset_path(name, version)):
        raise FileNotFoundError(f"GIS dataset {name} has no version {version}")


def get_gis_dataset_versions(dataset_name: str, sort_desc: bool = True) -> list[str]:
    """
    Get all versions of GIS-published dataset in edm-publishing/datasets
    """
    gis_version_formats = [r"^\d{2}[A-Z]$", r"^\d{8}$"]
    subfolders = []
    matched_formats = set()
    for f in s3.get_subfolders(_bucket(), f"datasets/{dataset_name}"):
        for p in gis_version_formats:
            if re.match(p, f):
                subfolders.append(f)
                matched_formats.add(p)
    if subfolders:
        if len(matched_formats) > 1:
            raise ValueError(
                f"Multiple version formats found for gis dataset {dataset_name}. Cannot determine latest version"
            )
    return sorted(subfolders, reverse=sort_desc)


def get_latest_gis_dataset_version(dataset_name: str) -> str:
    """
    Get latest version of GIS-published dataset in edm-publishing/datasets
    assuming versions are sortable
    """
    versions = get_gis_dataset_versions(dataset_name)
    if not versions:
        raise FileNotFoundError(f"No versions found for GIS dataset {dataset_name}")
    version = versions[0]
    _assert_gis_dataset_exists(dataset_name, version)
    return version


def download_gis_dataset(dataset_name: str, version: str, target_folder: Path):
    """
    Download GIS-published dataset from edm-publishing/datasets.
    Capitalizes supplied version when looking in s3 due to current conventions.
    Only quarterly (24a/24A) datasets currently use format other than just numeric datestrings
    """
    ## TODO - assumes versions are numeric or geosupport (which we use "24a" vs gis "24A")
    version = version.upper()
    assert target_folder.is_dir(), f"Target folder '{target_folder}' is not a directory"
    _assert_gis_dataset_exists(dataset_name, version)
    file_path = target_folder / f"{dataset_name}.zip"  ## we assume all gis datasets are
    s3.download_file(_bucket(), _gis_dataset_path(dataset_name, version), file_path)
    return file_path


class GisDatasetsConnector(VersionedConnector):
    conn_type: str = "edm.publishing.gis"

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        pulled_path = download_gis_dataset(
            dataset_name=key, version=version, target_folder=destination_path
        )
        return {"path": pulled_path}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise PermissionError(
            "Currently, only GIS team pushes to edm-publishing/datasets"
        )

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        logger.info(f"Listing versions for {key}")
        return get_gis_dataset_versions(key, sort_desc=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return get_latest_gis_dataset_version(key)

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

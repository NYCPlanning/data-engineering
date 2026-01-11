from pathlib import Path

from dcpy.configuration import (
    PUBLISHING_BUCKET,
)
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    DraftKey,
)
from dcpy.utils import s3, versions
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


def download_file(
    product_key: ProductKey, filepath: str, output_dir: Path | None = None
) -> Path:
    output_dir = output_dir or Path(".")
    is_file_path = output_dir.suffix in _TEMP_PUBLISHING_FILE_SUFFIXES
    output_filepath = (
        output_dir / Path(filepath).name if not is_file_path else output_dir
    )
    logger.info(f"Downloading {product_key}, {filepath} -> {output_filepath}")
    s3.download_file(_bucket(), f"{product_key.path}/{filepath}", output_filepath)
    return output_filepath


def get_draft_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(_bucket(), f"{product}/draft/"), reverse=True)


def get_draft_version_revisions(product: str, version: str) -> list[str]:
    return sorted(
        s3.get_subfolders(_bucket(), f"{product}/draft/{version}/"), reverse=True
    )


def get_draft_revision_label(product: str, version: str, revision_num: int) -> str:
    """Given a draft revision number, return draft revision label in s3."""
    draft_revision_label = None
    draft_revision_objects = [
        versions.parse_draft_version(version)
        for version in get_draft_version_revisions(product, version)
        if versions.parse_draft_version(version).revision_num == revision_num
    ]
    if len(draft_revision_objects) != 0:
        draft_revision_label = draft_revision_objects[0].label
    if draft_revision_label is None:
        raise ValueError(
            f"A draft revision with revision number of {revision_num} doesn't exist. Try again"
        )
    return draft_revision_label


class DraftsConnector(VersionedConnector):
    conn_type: str = "edm.publishing.drafts"

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        dataset: str | None = None,
        filepath: str,
        revision: str,
        **kwargs,
    ) -> dict:
        draft_key = DraftKey(key, version=version, revision=revision)

        path_prefix = dataset + "/" if dataset else ""
        file_path = f"{path_prefix}{filepath}"
        logger.info(f"Pulling Draft for {draft_key}, path={file_path}")
        pulled_path = download_file(draft_key, file_path, output_dir=destination_path)
        return {"path": pulled_path}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self._pull(
            key, version=version, destination_path=destination_path, **kwargs
        )

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        logger.info(f"Listing versions for {key}")
        versions = sorted(get_draft_versions(key), reverse=sort_desc)
        assert versions, (
            f"Product {key} should have versions, but none were found. This likely indicates a configuration problem."
        )
        return versions

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.list_versions(key)[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def data_local_sub_path(
        self, key: str, *, version: str, revision: str, **kwargs
    ) -> Path:
        return Path("edm") / "publishing" / "datasets" / key / version / revision

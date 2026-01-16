from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import yaml

from dcpy.lifecycle.builds.connector import get_drafts_default_connector
from dcpy.models.lifecycle.builds import BuildMetadata


def get_dataset_versions(product: str) -> list[str]:
    """Get draft versions using drafts connector.

    Note: keys for drafts have both the dataset version (e.g. "24v1")
    and the revision, e.g. 1-my-draft. This returns just the versions.
    """
    draft_versions = get_drafts_default_connector().list_versions(
        product, sort_desc=True
    )

    # Extract just the version part from version.revision format
    versions_set = set()
    for version_revision in draft_versions:
        if "." in version_revision:
            version = version_revision.split(".")[0]
            versions_set.add(version)

    return sorted(list(versions_set), reverse=True)


def get_dataset_version_revisions(product: str, version: str) -> list[str]:
    """Get draft revisions for a specific version using drafts connector.

    e.g. return all revisions (1-my-draft, 2-my-other-draft) for a version"""
    all_draft_versions = get_drafts_default_connector().list_versions(
        product, sort_desc=True
    )

    # Filter to only revisions for the specific version
    revisions = []
    for version_revision in all_draft_versions:
        if "." in version_revision:
            v, revision = version_revision.rsplit(".", maxsplit=1)
            if v == version:
                revisions.append(revision)

    return sorted(revisions, reverse=True)


def get_revision_label(product: str, version: str, revision_num: int) -> str:
    """Get draft revision label for a specific revision number using drafts connector."""
    revisions = get_dataset_version_revisions(product, version)
    for r in revisions:
        revision_split = r.split("-", maxsplit=1)
        if revision_split[0] == str(revision_num):
            return revision_split[1] if len(revision_split) > 1 else ""

    raise ValueError(f"Draft revision {revision_num} not found in {revisions}")


def get_source_data_versions(
    product: str, version: str, revision_num: int
) -> pd.DataFrame:
    """Get source data versions for a draft."""

    drafts_conn = get_drafts_default_connector()
    with TemporaryDirectory() as _dir:
        result = drafts_conn.pull_versioned(
            key=product,
            version=f"{version}.{revision_num}",
            source_path="source_data_versions.csv",
            destination_path=Path(_dir),
        )

        df = pd.read_csv(result.get("path"))
    return df


@dataclass
class _FullDraftKey:
    product: str
    dataset_version: str
    revision_num: int
    revision_label: str

    @property
    def revision(self) -> str:
        """Return formatted revision string (e.g., '1' or '1-label')."""
        if self.revision_label:
            return f"{self.revision_num}-{self.revision_label}"
        return str(self.revision_num)

    def __str__(self):
        return f"{self.product}::{self.dataset_version}::{self.revision}"


def resolve_full_version(
    product: str,
    dataset_version: str,
    *,
    # Pass either the revision_num or the full_revision (num+label)
    revision_num: int | None = None,
    full_revision: str = "",
) -> _FullDraftKey:
    """Resolve full draft version information from either revision_num or full_revision.

    Args:
        product: Product name
        dataset_version: Version string (e.g., "25v3")
        revision_num: Revision number (mutually exclusive with full_revision)
        full_revision: Full revision string like "1-fix-bug" (mutually exclusive with revision_num)

    Returns:
        _FullDraftKey: Object containing dataset_version, revision_num, and revision_label

    Raises:
        ValueError: If neither or both revision_num and full_revision are provided
    """
    if not (revision_num or full_revision) or (revision_num and full_revision):
        raise ValueError(
            "Exactly one of revision_num or full_revision must be provided"
        )

    if revision_num:
        return _FullDraftKey(
            product=product,
            dataset_version=dataset_version,
            revision_num=revision_num,
            revision_label=get_revision_label(product, dataset_version, revision_num),
        )
    else:
        revision_parts = full_revision.rsplit("-", maxsplit=1)
        return _FullDraftKey(
            product=product,
            dataset_version=dataset_version,
            revision_num=int(revision_parts[0]),
            revision_label=revision_parts[1] if len(revision_parts) == 2 else "",
        )


def get_metadata(product: str, full_version: str) -> BuildMetadata:
    """Get metadata for a draft.

    Args:
        product: Product name
        full_version: Full version string like "25v3.1-my-draft"
    """
    with TemporaryDirectory() as _dir:
        md_path = (
            get_drafts_default_connector()
            .pull_versioned(
                key=product,
                version=full_version,
                source_path="build_metadata.json",
                destination_path=Path(_dir),
            )
            .get("path")
        )
        return BuildMetadata(**yaml.safe_load(open(md_path).read()))

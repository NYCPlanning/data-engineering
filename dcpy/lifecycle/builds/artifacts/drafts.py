import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import yaml

from dcpy.lifecycle.builds import connector as build_conns
from dcpy.models.connectors.edm.publishing import PublishKey
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.utils import versions
from dcpy.utils.logging import logger


def get_versions(product: str) -> list[str]:
    """Get draft versions using drafts connector."""
    drafts_conn = build_conns.get_drafts_default_connector()
    draft_versions = drafts_conn.list_versions(product, sort_desc=True)

    # Extract just the version part from version.revision format
    versions_set = set()
    for version_revision in draft_versions:
        if "." in version_revision:
            version = version_revision.split(".")[0]
            versions_set.add(version)

    return sorted(list(versions_set), reverse=True)


def get_version_revisions(product: str, version: str) -> list[str]:
    """Get draft revisions for a specific version using drafts connector."""
    drafts_conn = build_conns.get_drafts_default_connector()
    all_draft_versions = drafts_conn.list_versions(product, sort_desc=True)

    # Filter to only revisions for the specific version
    revisions = []
    for version_revision in all_draft_versions:
        if "." in version_revision:
            v, revision = version_revision.split(".", 1)
            if v == version:
                revisions.append(revision)

    return sorted(revisions, reverse=True)


def get_revision_label(product: str, version: str, revision_num: int) -> str:
    """Get draft revision label for a specific revision number using drafts connector."""
    revisions = get_version_revisions(product, version)
    for r in revisions:
        revision_split = r.split("-", maxsplit=1)
        if revision_split[0] == str(revision_num):
            return revision_split[1] if len(revision_split) > 1 else ""

    raise ValueError(f"Draft revision {revision_num} not found in {revisions}")


def get_source_data_versions(
    product: str, version: str, revision_num: int
) -> pd.DataFrame:
    """Get source data versions for a draft."""

    drafts_conn = build_conns.get_drafts_default_connector()
    with TemporaryDirectory() as _dir:
        result = drafts_conn.pull_versioned(
            key=product,
            version=f"{version}.{revision_num}",
            source_path="source_data_versions.csv",
            destination_path=Path(_dir),
        )

        df = pd.read_csv(result.get("path"))
    return df


def get_metadata(product: str, version: str, revision_num: int) -> BuildMetadata:
    """Get metadata for a draft."""
    drafts_conn = build_conns.get_drafts_default_connector()
    revision_label = get_revision_label(product, version, revision_num)
    full_revision = f"{version}.{revision_num}" + (
        f"-{revision_label}" if revision_label else ""
    )
    with TemporaryDirectory() as _dir:
        md_path = drafts_conn.pull_versioned(
            key=product,
            version=full_revision,
            source_path="build_metadata.json",
            destination_path=Path(_dir),
        ).get("path")
        md = BuildMetadata(**yaml.safe_load(open(md_path).read()))
    return md


def validate_or_patch_version(product: str, version: str, is_patch: bool) -> str:
    """Given input arguments, determine the publish version, bumping it if necessary."""
    published_conn = build_conns.get_published_default_connector()
    published_versions = published_conn.list_versions(product, sort_desc=True)

    # Filters existing published versions for same version (patched or non-patched)
    published_same_version = versions.group_versions_by_base(
        version, published_versions
    )
    version_already_published = version in published_same_version

    if version_already_published:
        if is_patch:
            latest_version = published_same_version[-1]
            patched_version = versions.bump(
                previous_version=latest_version,
                bump_type=versions.VersionSubType.patch,
                bump_by=1,
            ).label
            assert patched_version not in published_versions  # sanity check
            return patched_version
        else:
            raise ValueError(
                f"Version '{version}' already exists in published folder and patch wasn't selected"
            )

    logger.info(f"Predicted version in publish folder: {version}")
    return version


def copy_draft_to_published(
    product: str,
    draft_version_revision: str,
    publish_version: str,
    acl: str = "",
) -> None:
    """Copy all files from draft to published using connectors."""
    # Use temporary directory approach as requested
    drafts_conn = build_conns.get_drafts_default_connector()
    published_conn = build_conns.get_published_default_connector()
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Pull entire draft folder to temp
        drafts_conn.pull_versioned(
            key=product, version=draft_version_revision, destination_path=temp_path
        )

        # Push entire folder to published
        published_conn.push_versioned(
            key=product, version=publish_version, source_path=str(temp_path), acl=acl
        )

        logger.info(
            f"Copied draft {product}/{draft_version_revision} to published {product}/{publish_version}"
        )


def download_metadata(product: str, version: str) -> None:
    """Download metadata from published version."""
    published_conn = build_conns.get_published_default_connector()
    with TemporaryDirectory() as temp_dir:
        result = published_conn.pull_versioned(
            key=product,
            version=version,
            source_path="build_metadata.json",
            destination_path=Path(temp_dir),
        )
        # Copy to current working directory
        import shutil

        shutil.copy2(result.get("path"), "build_metadata.json")
        logger.info(
            f"Downloaded build_metadata.json from {product}/{version} to current directory"
        )


def publish(
    product: str,
    version: str,
    draft_revision_num: int | None = None,
    acl: str = "",
    latest: bool = False,
    is_patch: bool = False,
    download_metadata_to_file: bool = False,
) -> PublishKey:
    """
    Publishes a draft to the published datastore using connectors.

    Args:
        product: Data product name
        version: Data product release version
        draft_revision_num: Draft revision number to publish (if None, uses latest)
        acl: Access control level for uploaded files
        latest: Whether to publish to latest folder as well
        is_patch: Whether to create a patched version if version already exists
        download_metadata: Whether to download metadata after publishing

    Returns:
        PublishKey: The key for the published version

    Raises:
        FileNotFoundError: If the draft does not exist.
        ValueError: If version validation fails.
    """
    logger.info(f'Publishing {product} version {version} with ACL "{acl}"')
    drafts_conn = build_conns.get_drafts_default_connector()
    published_conn = build_conns.get_published_default_connector()

    # Get draft revision to publish
    revision: str = ""
    if draft_revision_num is not None:
        draft_revision_label = get_revision_label(product, version, draft_revision_num)
        revision = (
            str(draft_revision_num) + f"-{draft_revision_label}"
            if draft_revision_label
            else ""
        )
    else:
        revision = get_version_revisions(product, version)[0]
        draft_revision_num = int(revision.split("-")[0])

    # Get current version from draft metadata
    draft_metadata = get_metadata(product, version, draft_revision_num)
    current_version = draft_metadata.version

    # Validate and determine final version
    new_version = validate_or_patch_version(product, current_version, is_patch)

    logger.info(
        f'Publishing {product} draft {revision} as version {new_version} with ACL "{acl}"'
    )

    # Handle metadata updates if this is a patch
    updated_metadata_path = None
    if new_version != current_version:
        # Update metadata with new version
        draft_metadata.version = new_version
        updated_metadata_path = Path("build_metadata.json")
        with open(updated_metadata_path, "w", encoding="utf-8") as f:
            json.dump(draft_metadata.model_dump(mode="json"), f, indent=4)

    # Copy draft to published
    copy_draft_to_published(product, f"{version}.{revision}", new_version, acl)

    # Upload updated metadata if this was a patch
    if updated_metadata_path and updated_metadata_path.exists():
        # Create metadata file locally and push via connector
        with TemporaryDirectory() as temp_dir:
            temp_metadata_path = Path(temp_dir) / "build_metadata.json"
            import shutil

            shutil.copy2(updated_metadata_path, temp_metadata_path)

            # Push updated metadata file to published
            published_conn.push_versioned(
                key=product,
                version=new_version,
                source_path=str(temp_metadata_path),
                target_path="build_metadata.json",
                acl=acl,
            )
            logger.info(
                f"Updated metadata for {product}/{new_version} with new version {new_version}"
            )

        # Clean up local file
        updated_metadata_path.unlink()

    publish_key = PublishKey(product, new_version)

    # Update 'latest' folder if requested
    if latest:
        latest_version = published_conn.get_latest_version(product)
        if latest_version:
            after_latest_version = (
                versions.is_newer(version_1=new_version, version_2=latest_version)
                or new_version == latest_version
            )
        else:
            after_latest_version = None

        if after_latest_version or latest_version is None:
            # Copy to 'latest' folder - use temp dir approach for published to latest
            with TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Pull from published version we just created
                published_conn.pull_versioned(
                    key=product, version=new_version, destination_path=temp_path
                )

                # Push to latest
                published_conn.push_versioned(
                    key=product, version="latest", source_path=str(temp_path), acl=acl
                )
            logger.info(f"Updated 'latest' folder with version {new_version}")
        else:
            raise ValueError(
                f"Unable to update 'latest' folder: the version {new_version} is older than 'latest' ({latest_version})"
            )

    # Download metadata if requested
    # TODO: This is just a straight copy from the old version of this, but it's really misplaced
    # Let's just make this a separate step in the gha workflow (meaning, extract it to a CLI fn)
    if download_metadata_to_file:
        download_metadata(product, new_version)

    logger.info(f"Successfully published {product} version {new_version}")
    return publish_key

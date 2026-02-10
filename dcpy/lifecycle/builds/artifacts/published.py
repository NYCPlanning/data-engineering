import json
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from dcpy.lifecycle.builds import connector as build_conns
from dcpy.lifecycle.builds.artifacts.drafts import (
    get_dataset_version_revisions,
    get_metadata,
    resolve_full_version,
)
from dcpy.lifecycle.builds.connector import (
    get_published_default_connector,
)
from dcpy.models.connectors.edm.publishing import PublishKey
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.utils import versions
from dcpy.utils.logging import logger

# connector = get_published_default_connector()

BUILD_METADATA_FILE = "build_metadata.json"


def get_latest_version(product: str) -> str | None:
    return get_published_default_connector().get_latest_version(product)


def get_versions(product: str, exclude_latest: bool = True) -> list[str]:
    """Get published versions using published connector."""
    return get_published_default_connector().list_versions(
        product, sort_desc=True, exclude_latest=exclude_latest
    )


def get_previous_version(
    product: str, version: str | versions.Version
) -> versions.Version:
    """Get previous version using published connector."""
    # Normalize version input
    match version:
        case str():
            version_obj = versions.parse(version)
        case versions.Version():
            version_obj = version

    # Get published versions using connector
    published_version_strings = get_versions(product)
    logger.info(f"Published versions of {product}: {published_version_strings}")

    published_versions = [versions.parse(v) for v in published_version_strings]
    published_versions.sort()

    if len(published_versions) == 0:
        raise LookupError(f"No published versions found for {product}")

    if version_obj in published_versions:
        index = published_versions.index(version_obj)
        if index == 0:
            raise LookupError(
                f"{product} - {version} is the oldest published version, and has no previous"
            )
        else:
            return published_versions[index - 1]
    else:
        latest = published_versions[-1]
        if version_obj > latest:
            return latest
        else:
            raise LookupError(
                f"{product} - {version} is not published and appears to be 'older' than latest published version. Cannot determine previous."
            )


def fetch_metadata_file(product: str, folder_version: str, out_path: Path):
    """Pulls the metadata file for a product.

    Note: we pass the 'folder_version', which might be a normal version, or 'latest'
    """
    meta_path = build_conns.get_published_default_connector().pull_versioned(
        key=product,
        version=folder_version,
        filepath=BUILD_METADATA_FILE,
        destination_path=out_path,
    )
    return Path(meta_path.get("path")) / BUILD_METADATA_FILE


def fetch_version_from_metadata(product: str, folder_version: str):
    with TemporaryDirectory() as tmp_dir_str:
        md_dir = Path(tmp_dir_str)
        fetch_metadata_file(product, folder_version, md_dir)
        return BuildMetadata(
            **yaml.safe_load(open(Path(md_dir) / BUILD_METADATA_FILE).read())
        ).version


def validate_or_patch_version(
    product: str,
    version: str,
    is_patch: bool,
) -> str:
    """Given input arguments, determine the publish version, bumping it if necessary."""
    published_versions = get_versions(product=product)

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


def patch_metadata(
    product: str,
    draft_version_revision: str,
    patch_version: str,
    publish_version: str,
    tmp_dir: Path,
    published_conn,
    acl: str = "",
) -> None:
    """Update metadata with patched version and upload to published."""
    logger.info(
        f"Patching metadata version {draft_version_revision} with new version {publish_version}"
    )
    md = get_metadata(product, draft_version_revision)
    md.version = patch_version
    patched_md_path = tmp_dir / "build_metadata_patched.json"
    patched_md_path.write_text(json.dumps(md.model_dump(mode="json"), indent=4))

    published_conn.push_versioned(
        key=product,
        version=publish_version,
        source_path=str(patched_md_path),
        target_path="build_metadata.json",
        acl=acl,
    )


def _copy_draft_to_published(
    product: str,
    draft_version_revision: str,
    publish_version: str,
    acl: str = "",
    is_patch: bool = False,
    copy_to_latest: bool = False,
) -> None:
    """Copy all files from draft to published using connectors."""
    drafts_conn = build_conns.get_drafts_default_connector()
    published_conn = build_conns.get_published_default_connector()
    with TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)

        # Pull entire draft folder to temp
        drafts_conn.pull_versioned(
            key=product, version=draft_version_revision, destination_path=tmp_dir
        )

        # Push entire folder to published
        published_conn.push_versioned(
            key=product, version=publish_version, source_path=str(tmp_dir), acl=acl
        )
        logger.info(
            f"Copied draft {product}/{draft_version_revision} to published {product}/{publish_version}"
        )
        if is_patch:
            logger.info(
                f"Also patching published metadata versions: {draft_version_revision} -> {publish_version}"
            )
            patch_metadata(
                product=product,
                draft_version_revision=draft_version_revision,
                patch_version=publish_version,
                publish_version=publish_version,
                tmp_dir=tmp_dir,
                published_conn=published_conn,
                acl=acl,
            )

        # TODO: TEMPORARY HACK - copy_to_latest is a workaround until we iron out
        # proper s3/blob storage copying mechanisms. This should be refactored
        # to use proper connector optimization when available.
        if copy_to_latest:
            published_conn.push_versioned(
                key=product,
                version="latest",
                source_path=str(tmp_dir),
                acl=acl,
            )
            logger.info(
                f"Copied {product}/{publish_version} to latest folder (temporary hack)"
            )
            if is_patch:
                logger.info(f"Also patching latest metadata to new version {is_patch}")
                patch_metadata(
                    product=product,
                    draft_version_revision=draft_version_revision,
                    patch_version=publish_version,
                    publish_version="latest",  # Updated for latest
                    tmp_dir=tmp_dir,
                    published_conn=published_conn,
                    acl=acl,
                )


def publish(
    product: str,
    version: str,
    draft_revision_num: int = 0,
    acl: str = "",
    latest: bool = False,
    is_patch: bool = False,
    metadata_file_dir: Path | None = None,
) -> PublishKey:
    """
    Publishes a draft to the published datastore using connectors.

    Args:
        product: Data product name
        version: Data product release version
        draft_revision_num: Draft revision number to publish (if 0, uses latest)
        acl: Access control level for uploaded files
        latest: Whether to publish to latest folder as well
        is_patch: Whether to create a patched version if version already exists
        metadata_file_dir: dir to download metadata after publishing

    Returns:
        PublishKey: The key for the published version

    Raises:
        FileNotFoundError: If the draft does not exist.
        ValueError: If version validation fails.
    """
    logger.info(f'Publishing {product} version {version} with ACL "{acl}"')

    # Get the full draft version information
    if draft_revision_num == 0:
        # If no specific revision requested, get the latest revision
        revisions = get_dataset_version_revisions(product, version)
        assert revisions, f"No revisions for {product}, {version}"
        latest_revision = revisions[0]
        draft_key = resolve_full_version(
            product, version, full_revision=latest_revision
        )
    else:
        draft_key = resolve_full_version(
            product, version, revision_num=draft_revision_num
        )

    # Validate and determine final version to publish
    publish_version = validate_or_patch_version(
        product, draft_key.dataset_version, is_patch
    )
    logger.info(f'Publishing {draft_key} as version {publish_version} with ACL "{acl}"')

    # When "latest" is requested, determine if that's possible
    if latest:
        latest_published_version = (
            build_conns.get_published_default_connector().get_latest_version(product)
        )
        if latest_published_version:
            after_or_equals_latest_version = (
                versions.is_newer(
                    version_1=publish_version, version_2=latest_published_version
                )
                or publish_version == latest_published_version
            )
        else:
            after_or_equals_latest_version = None

        if after_or_equals_latest_version or latest_published_version is None:
            logger.info(f"Updated 'latest' folder with version {publish_version}")
        else:
            raise ValueError(
                f"Unable to update 'latest' folder: the version {publish_version} is older than 'latest' ({latest_published_version})"
            )

    _copy_draft_to_published(
        product,
        draft_version_revision=f"{draft_key.dataset_version}.{draft_key.revision}",
        publish_version=publish_version,
        acl=acl,
        copy_to_latest=latest,
        is_patch=(version != publish_version),
    )

    # Download metadata if requested
    if metadata_file_dir:
        md_file_path = fetch_metadata_file(
            product, folder_version=publish_version, out_path=metadata_file_dir
        )
        logger.info(f"Pulled build_metadata.json to {md_file_path}")

    logger.info(f"Successfully published {product} version {publish_version}")
    return PublishKey(product, publish_version)

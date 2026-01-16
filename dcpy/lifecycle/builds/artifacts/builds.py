import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytz
import yaml

from dcpy.configuration import BUILD_NAME, CI
from dcpy.lifecycle.builds.artifacts import drafts
from dcpy.lifecycle.builds.connector import (
    get_builds_default_connector,
    get_drafts_default_connector,
)
from dcpy.models.connectors.edm.publishing import BuildKey, DraftKey
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.utils import git, versions
from dcpy.utils.logging import logger


def _generate_metadata(running_in_gha: bool = False) -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    metadata["commit"] = git.commit_hash()
    if running_in_gha:
        metadata["run-url"] = git.action_url()
    return metadata


def get_metadata(product: str, build: str) -> BuildMetadata:
    """Retrieve build metadata using builds connector."""
    builds_conn = get_builds_default_connector()
    with TemporaryDirectory() as temp_dir:
        result = builds_conn.pull_versioned(
            key=product,
            version=build,
            filepath="build_metadata.json",
            destination_path=Path(temp_dir),
        )
        metadata_path = result.get("path")
        if not metadata_path or not Path(metadata_path).exists():
            raise FileNotFoundError(f"Build metadata not found for {product}/{build}")

        return BuildMetadata(**yaml.safe_load(open(metadata_path).read()))


def get_version(product: str, build: str) -> str:
    """Get version from build metadata."""
    return get_metadata(product, build).version


def copy_build_to_draft(
    product: str, build: str, draft_version_revision: str, acl: str = "public-read"
) -> None:
    """Copy all files from build to draft using connectors."""
    builds_conn = get_builds_default_connector()
    drafts_conn = get_drafts_default_connector()
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Pull entire build folder to temp
        builds_conn.pull_versioned(
            key=product,
            version=build,
            destination_path=temp_path,
            connector_args={"build_note": build},
        )

        # Push entire folder to draft
        drafts_conn.push_versioned(
            key=product,
            version=draft_version_revision,
            source_path=str(temp_path),
            acl=acl,
        )

        logger.info(
            f"Copied build {product}/{build} to draft {product}/{draft_version_revision}"
        )


def download_draft_metadata(product: str, version: str, revision_label: str) -> None:
    """Download metadata from draft version."""
    # Use get_metadata from drafts.py to get the metadata
    metadata = drafts.get_metadata(product, version, revision_label)

    # Save to current working directory
    with open("build_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata.model_dump(mode="json"), f, indent=4)

    logger.info(
        f"Downloaded build_metadata.json from {product}/{version}.{revision_label} to current directory"
    )


def get_builds(product: str) -> list[str]:
    """Get all build versions using builds connector."""
    return get_builds_default_connector().list_versions(product, sort_desc=True)


def upload(
    output_path: Path,
    product: str,
    build: str | None = None,
    acl: str = "",  # TODO: keep in mind when you swap the references in gha
    # acl: str = "public-read",
) -> BuildKey:
    """
    Uploads a product build using the configured builds connector.

    Args:
        output_path: Path to local output folder to upload
        product: Name of data product
        build: Label of build (defaults to BUILD_NAME env var)
        acl: Access control level for uploaded files

    Returns:
        BuildKey: The key for the uploaded build

    Raises:
        FileNotFoundError: If the provided output_path does not exist.
        ValueError: If the build name is not provided and cannot be found in environment variables.
    """
    if not output_path.exists():
        raise FileNotFoundError(f"Path {output_path} does not exist")

    build_name = build or BUILD_NAME
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )

    build_key = BuildKey(product, build_name)
    meta = _generate_metadata(running_in_gha=CI)

    logger.info(f"Uploading {output_path} to {build_key.path}")
    builds_conn = get_builds_default_connector()
    result = builds_conn.push(
        key=product,
        build_path=output_path,
        connector_args={"build_note": build_name, "acl": acl, "metadata": meta},
    )

    logger.info(f"Successfully uploaded build: {result}")
    return build_key


def promote_to_draft(
    product: str,
    build: str,
    acl: str = "",
    keep_build: bool = True,
    draft_revision_summary: str = "",
    download_metadata: bool = False,
) -> DraftKey:
    """
    Promotes a product build to draft using the configured connectors.

    Args:
        product: Name of data product
        build: Label of build to promote
        acl: Access control level for uploaded files
        keep_build: Whether to keep the original build after promotion
        draft_revision_summary: Summary description for the draft revision
        download_metadata: Whether to download metadata after promotion

    Returns:
        DraftKey: The key for the created draft

    Raises:
        FileNotFoundError: If the build does not exist.
        ValueError: If required metadata is missing.
    """
    drafts_conn = get_drafts_default_connector()
    build_key = BuildKey(product, build)
    logger.info(f'Promoting {build_key.path} to draft with ACL "{acl}"')

    # Get version from build metadata
    version = get_version(product, build)

    # Generate draft revision number
    draft_revision_number = len(drafts.get_version_revisions(product, version)) + 1
    draft_revision_label = versions.DraftVersionRevision(
        draft_revision_number, draft_revision_summary
    ).label

    # Read build metadata, update it with draft label, and save it locally
    build_metadata = get_metadata(product, build)
    build_metadata.draft_revision_name = draft_revision_label
    build_metadata_path = Path("build_metadata.json")
    with open(build_metadata_path, "w", encoding="utf-8") as f:
        json.dump(build_metadata.model_dump(mode="json"), f, indent=4)

    # Create draft key
    draft_key = DraftKey(product, version, draft_revision_label)
    draft_version_revision = f"{version}.{draft_revision_label}"

    # Copy build to draft using connectors
    copy_build_to_draft(product, build, draft_version_revision, acl)

    # Upload updated metadata file
    with TemporaryDirectory() as temp_dir:
        temp_metadata_path = Path(temp_dir) / "build_metadata.json"
        import shutil

        shutil.copy2(build_metadata_path, temp_metadata_path)

        # Push updated metadata file to draft
        drafts_conn.push_versioned(
            key=product,
            version=draft_version_revision,
            source_path=str(temp_metadata_path),
            target_path="build_metadata.json",
            acl=acl,
        )
        logger.info(
            f"Updated metadata for {product}/{draft_version_revision} with draft revision {draft_revision_label}"
        )

    # Clean up local metadata file
    build_metadata_path.unlink()

    logger.info(f"Promoted {product} to drafts as {version}/{draft_revision_label}")

    # Delete build if requested
    if not keep_build:
        # TODO: Implement build deletion via connector
        logger.info(
            f"Would delete build {product}/{build} (deletion not implemented yet)"
        )

    # Download metadata if requested
    if download_metadata:
        download_draft_metadata(product, version, draft_revision_label)

    return draft_key

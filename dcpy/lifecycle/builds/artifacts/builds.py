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


def build_exists(product: str, build: str):
    return build in get_builds_default_connector().list_versions(
        product, sort_desc=True
    )


def get_build_metadata(product: str, build: str) -> BuildMetadata:
    """Retrieve build metadata using builds connector."""
    builds_conn = get_builds_default_connector()
    assert build_exists(product, build), (
        f"No build exists for product={product}, build={build}"
    )
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
    return get_build_metadata(product, build).version


def _copy_to_draft(
    product: str, build: str, draft_version: str, acl: str = "public-read"
) -> None:
    """Copy all files from build to draft using connectors.

    Note: draft_version is in the form of {dataset version}.{revision num and label}
    e.g. 24v1.1-my-build, where 24v1 is dataset version, and 1-my-build is the
    revision and label.
    """
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Pull entire build folder to temp
        get_builds_default_connector().pull_versioned(
            key=product,
            version=build,
            destination_path=temp_path,
            connector_args={"build_note": build},
        )

        # Push entire folder to draft
        get_drafts_default_connector().push_versioned(
            key=product,
            version=draft_version,
            source_path=temp_path,
            acl=acl,
        )

        logger.info(
            f"Copied build {product}/{build} to draft {product}/{draft_version}"
        )


def upload(
    output_path: Path,
    product: str,
    build: str | None = None,
    acl: str = "",  # TODO: keep in mind when you swap the references in gha
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
    if not output_path.is_dir():
        raise FileNotFoundError(f"Path {output_path} should exist and be a dir")

    build_name = build or BUILD_NAME
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )

    build_key = BuildKey(product, build_name)
    meta = _generate_metadata(running_in_gha=CI)  # for now, same thing.

    logger.info(f"Uploading {output_path} to {build_key.path}")
    result = get_builds_default_connector().push_versioned(
        key=product,
        version=build_name,
        build_path=output_path,
        connector_args={"build_note": build_name, "acl": acl, "metadata": meta},
    )

    logger.info(f"Successfully uploaded build: {result}")
    return build_key


def promote_to_draft(
    product: str,
    build: str,
    acl: str = "",
    # keep_build: bool = True, # TODO: implement delete
    draft_revision_summary: str = "",
    metadata_file_dir: Path | None = None,
) -> DraftKey:
    """
    Promotes a product build to draft using the configured connectors.

    Args:
        product: Name of data product
        build: Label of build to promote
        acl: Access control level for uploaded files
        keep_build: Whether to keep the original build after promotion
        draft_revision_summary: Summary description for the draft revision
        metadata_file_dir: Optional dir to save metadata file after promotion

    Returns:
        DraftKey: The key for the created draft

    Raises:
        FileNotFoundError: If the build does not exist.
        ValueError: If required metadata is missing.
    """
    logger.info(f'Promoting {product} {build} to draft with ACL "{acl}"')

    # Figure out version stuff
    version = get_version(product, build)
    draft_revision_number = (
        len(drafts.get_dataset_version_revisions(product, version)) + 1
    )
    draft_revision_label = versions.DraftVersionRevision(
        draft_revision_number, draft_revision_summary
    ).label

    # promote from build to draft
    draft_key = DraftKey(product, version, draft_revision_label)
    draft_version_revision = f"{version}.{draft_revision_label}"
    _copy_to_draft(product, build, draft_version_revision, acl)

    # Push updated metadata file to draft
    # TODO: We could push this file with the initial go above,
    # but we'll probably implement an s3/azure copy, and short-circuit
    # the download step above
    with TemporaryDirectory() as tmpdirname:
        build_metadata = get_build_metadata(product, build)
        build_metadata.draft_revision_name = draft_revision_label
        md_file_path = (metadata_file_dir or Path(tmpdirname)) / "build_metadata.json"

        md_file_path.write_text(
            json.dumps(build_metadata.model_dump(mode="json"), indent=4)
        )
        get_drafts_default_connector().push_versioned(
            key=product,
            version=draft_version_revision,
            source_path=md_file_path,
            target_path="build_metadata.json",
            acl=acl,
        )
        logger.info(
            f"Updated metadata for {product}/{draft_version_revision} with draft revision {draft_revision_label}"
        )

    logger.info(f"Promoted {product} to drafts as {version}/{draft_revision_label}")

    # TODO: Implement build deletion via connector
    # if not keep_build:
    #     logger.info(
    #         f"Would delete build {product}/{build} (deletion not implemented yet)"
    #     )

    return draft_key

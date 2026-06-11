"""Utility functions for lifecycle.builds that use connectors.

These functions encapsulate business logic and use configured connectors
to access data from publishing/recipes systems.
"""

import tempfile
from pathlib import Path

import pandas as pd

from dcpy.connectors.edm.models import BuildKey, DraftKey, ProductKey, PublishKey
from dcpy.lifecycle.builds.connector import get_published_default_connector
from dcpy.utils import versions
from dcpy.utils.logging import logger


def get_previous_version(
    product: str, version: str | versions.Version
) -> versions.Version:
    """Find the previous published version before the given version.

    Args:
        product: Product name
        version: Current version (string or Version object)

    Returns:
        The previous version as a Version object

    Raises:
        LookupError: If no previous version exists or version is not published
    """
    connector = get_published_default_connector()

    # Get all versions from connector
    published_version_strings = connector.list_versions(product, exclude_latest=True)
    logger.info(f"Published versions of {product}: {published_version_strings}")

    # Parse version
    match version:
        case str():
            version_obj = versions.parse(version)
        case versions.Version():
            version_obj = version

    # Parse and sort all published versions
    published_versions = [versions.parse(v) for v in published_version_strings]
    published_versions.sort()

    if len(published_versions) == 0:
        raise LookupError(f"No published versions found for {product}")

    # Find previous version
    if version_obj in published_versions:
        index = published_versions.index(version_obj)
        if index == 0:
            raise LookupError(
                f"{product} - {version} is the oldest published version, and has no previous"
            )
        return published_versions[index - 1]
    else:
        # Version is not yet published
        latest = published_versions[-1]
        if version_obj > latest:
            return latest
        else:
            raise LookupError(
                f"{product} - {version} is not published and appears to be 'older' than latest published version. Cannot determine previous."
            )


def get_source_data_versions(
    product_key: BuildKey | PublishKey | DraftKey | ProductKey,
) -> pd.DataFrame:
    """Read source_data_versions.csv from a product and transform it.

    Args:
        product_key: Product key identifying the version

    Returns:
        DataFrame with transformed columns and index

    Raises:
        FileNotFoundError: If source_data_versions.csv doesn't exist
    """
    connector = get_published_default_connector()

    # Pull file using standard interface
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    # Get version from product_key (handles BuildKey.build, DraftKey/PublishKey.version)
    if isinstance(product_key, BuildKey):
        version = product_key.build
    elif hasattr(product_key, "version"):
        version = product_key.version  # type: ignore[attr-defined]
    else:
        raise TypeError(
            f"product_key must have 'version' or 'build' attribute, got {type(product_key)}"
        )

    try:
        result = connector.pull_versioned(
            key=product_key.product,
            version=version,
            destination_path=tmp_path,
            filepath="source_data_versions.csv",
        )
        df = pd.read_csv(result["path"], dtype=str)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    # Business logic: parse/transform
    # Rename columns if they exist (handles both old and new formats)
    rename_map = {}
    if "schema_name" in df.columns:
        rename_map["schema_name"] = "datalibrary_name"
    elif "dataset" in df.columns:
        rename_map["dataset"] = "datalibrary_name"

    if "v" in df.columns:
        rename_map["v"] = "version"

    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    df.sort_values(by=["datalibrary_name"], ascending=True, inplace=True)
    df.set_index("datalibrary_name", inplace=True)

    return df


def get_file_contents(
    product_key: BuildKey | PublishKey | DraftKey | ProductKey, filepath: str
) -> bytes:
    """Get raw file contents from a product version.

    Args:
        product_key: Product key identifying the version
        filepath: Path to file within the version

    Returns:
        Raw bytes of the file

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    connector = get_published_default_connector()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = Path(tmp.name)

    # Get version from product_key (handles BuildKey.build, DraftKey/PublishKey.version)
    if isinstance(product_key, BuildKey):
        version = product_key.build
    elif hasattr(product_key, "version"):
        version = product_key.version  # type: ignore[attr-defined]
    else:
        raise TypeError(
            f"product_key must have 'version' or 'build' attribute, got {type(product_key)}"
        )

    try:
        result = connector.pull_versioned(
            key=product_key.product,
            version=version,
            destination_path=tmp_path,
            filepath=filepath,
        )
        with open(result["path"], "rb") as f:
            return f.read()
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

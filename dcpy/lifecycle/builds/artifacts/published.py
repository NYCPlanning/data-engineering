from dcpy.lifecycle.builds.connector import (
    get_published_default_connector,
)
from dcpy.utils import versions
from dcpy.utils.logging import logger

# connector = get_published_default_connector()


def get_latest_version(product: str) -> str | None:
    connector = get_published_default_connector()
    return connector.get_latest_version(product)


def get_versions(product: str, exclude_latest: bool = True) -> list[str]:
    """Get published versions using published connector."""
    connector = get_published_default_connector()
    return connector.list_versions(
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

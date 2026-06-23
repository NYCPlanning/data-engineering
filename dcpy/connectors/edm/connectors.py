"""Unified EDM connectors for publishing workflows.

This module provides connectors for different stages of the EDM publishing pipeline:
- DraftsConnector: For draft versions with version.revision structure
- BuildsConnector: For build artifacts
- PublishedConnector: For published versions
- PlanConnector: For planned recipes
"""

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import pytz

from dcpy.configuration import (
    BUILD_NAME,
    CI,
    PUBLISHING_BUCKET,
    PUBLISHING_BUCKET_ROOT_FOLDER,
)
from dcpy.connectors.edm.models import (
    BuildKey,
    DraftKey,
    PlanKey,
    ProductKey,
    PublishKey,
)
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.registry import VersionedConnector
from dcpy.utils import git, s3
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

# Mapping from recipe dataset names to S3 product names
# This handles cases where recipe names differ from S3 folder names
PRODUCT_NAME_MAPPING = {
    "zoningtaxlots": "db-zoningtaxlots",
    "zap": "db-zap",
    "template": "db-template",
    "pluto": "db-pluto",
    "lion": "db-lion",
    "gru-qaqc": "db-gru-qaqc",
    "green-fast-track": "db-green-fast-track",
    "factfinder": "db-factfinder",
    "facilities": "db-facilities",
    "edde": "db-eddt",  # Special case: edde maps to db-eddt
    "developments": "db-developments",
    "cscl": "db-cscl",
    "cpdb": "db-cpdb",
    "community-profiles": "db-community-profiles",
    "colp": "db-colp",
    "checkbook": "db-checkbook",
    "ceqr": "db-ceqr",
    "cdbg": "db-cdbg",
    "cbbr": "db-cbbr",
}


# All Postgres build/transform databases in the edm-data cluster (one per product).
# Source of truth for admin tooling that operates across the whole cluster, e.g.
# stale-schema cleanup and disk-usage reporting.
BUILD_DBS = [
    "db-cbbr",
    "db-cdbg",
    "db-ceqr",
    "db-checkbook",
    "db-colp",
    "db-cpdb",
    "db-cscl",
    "db-devdb",
    "db-facilities",
    "db-green-fast-track",
    "db-pluto",
    "db-template",
    "db-ztl",
    "kpdb",
]


def _map_product_name(product: str) -> str:
    """Map recipe product name to S3 product name if needed."""
    return PRODUCT_NAME_MAPPING.get(product, product)


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm connectors"
    )
    return PUBLISHING_BUCKET


@dataclass
class EdmConnectorConfig:
    """Configuration for EDM connector behavior."""

    conn_type: str
    folder_name: str  # e.g., "draft", "build", "publish", "plan"
    key_factory: Callable[
        [str, str, dict], ProductKey
    ]  # (product, version, kwargs) -> ProductKey
    version_parser: Callable[[str], dict] | None = (
        None  # version string -> dict of parsed parts
    )
    list_versions_impl: Callable[["EdmConnector", str, dict], list[str]] | None = None
    supports_latest: bool = True
    metadata_generator: Callable[[], dict] | None = None


class EdmConnector(VersionedConnector, arbitrary_types_allowed=True):
    """Unified connector for all EDM publishing workflows."""

    conn_type: str
    _storage: PathedStorageConnector | None = None
    _config: EdmConnectorConfig

    def __init__(
        self,
        config: EdmConnectorConfig,
        storage: PathedStorageConnector | None = None,
        **kwargs,
    ):
        # Pass conn_type to Pydantic initialization
        super().__init__(conn_type=config.conn_type, **kwargs)
        self._config = config
        if storage is not None:
            self._storage = storage

    @property
    def storage(self) -> PathedStorageConnector:
        if self._storage is None:
            self._storage = PathedStorageConnector.from_storage_kwargs(
                conn_type=self.conn_type,
                storage_backend=StorageType.S3,
                s3_bucket=_bucket(),
                root_folder=PUBLISHING_BUCKET_ROOT_FOLDER,
                _validate_root_path=False,
            )
        return self._storage

    def _parse_version(self, version: str) -> tuple[str, str]:
        """Parse version string using the configured version_parser.

        Returns a tuple (version, revision) for backwards compatibility with tests.
        """
        if self._config.version_parser:
            parsed = self._config.version_parser(version)
            # Return tuple for backwards compatibility
            return parsed.get("version", ""), parsed.get("revision", "")
        return ("", "")

    def _download_file(
        self, product_key: ProductKey, filepath: str, output_dir: Path | None = None
    ) -> Path:
        output_dir = output_dir or Path(".")

        # Determine if we're downloading a directory or a file
        # If filepath is empty, we're downloading the entire version directory
        is_directory_download = not filepath or filepath.endswith("/")

        if is_directory_download:
            # For directory downloads, use the product_key.path as the source
            # and output_dir as the destination
            source_key = product_key.path
            output_filepath = output_dir
        else:
            # For file downloads, build the full source path
            source_key = f"{product_key.path}/{filepath}"
            is_file_path = output_dir.suffix in _TEMP_PUBLISHING_FILE_SUFFIXES
            output_filepath = (
                output_dir / Path(filepath).name if not is_file_path else output_dir
            )

        logger.info(
            f"Downloading {product_key}, {filepath or '(entire directory)'} -> {output_filepath}"
        )

        if not self.storage.exists(source_key):
            # Provide helpful error with available files/versions
            error_msg = f"{'Directory' if is_directory_download else 'File'} {source_key} not found"

            # Check if the product version directory exists
            if self.storage.exists(product_key.path):
                # List available files in this version
                try:
                    available_files = self.storage.get_subfolders(product_key.path)
                    if available_files:
                        error_msg += (
                            f"\n\nAvailable files/directories in {product_key.path}:"
                        )
                        for file in available_files[:10]:  # Limit to first 10
                            error_msg += f"\n  - {file}"
                        if len(available_files) > 10:
                            error_msg += f"\n  ... and {len(available_files) - 10} more"
                except Exception:
                    pass  # If listing fails, just show the basic error
            else:
                # Version directory doesn't exist, list available versions
                try:
                    available_versions = self.list_versions(product_key.product)
                    if available_versions:
                        error_msg += (
                            f"\n\nVersion {product_key.path.split('/')[-1]} not found."
                        )
                        error_msg += (
                            f"\n\nAvailable versions for {product_key.product}:"
                        )
                        for version in available_versions[:10]:  # Limit to first 10
                            error_msg += f"\n  - {version}"
                        if len(available_versions) > 10:
                            error_msg += (
                                f"\n  ... and {len(available_versions) - 10} more"
                            )
                except Exception:
                    pass  # If listing fails, just show the basic error

            raise FileNotFoundError(error_msg)

        self.storage.pull(key=source_key, destination_path=output_filepath)
        return output_filepath

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        # Parse version if needed
        version_parts = {}
        if self._config.version_parser:
            version_parts = self._config.version_parser(version)

        # Create the product key
        product_key = self._config.key_factory(key, version, version_parts)

        # Handle filepath/dataset parameters
        filepath = kwargs.get("filepath", kwargs.get("source_path", ""))
        dataset = kwargs.get("dataset")
        path_prefix = f"{dataset}/" if dataset else ""
        full_filepath = f"{path_prefix}{filepath}"

        pulled_path = self._download_file(product_key, full_filepath, destination_path)
        return {"path": pulled_path}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        source_path = kwargs.get("source_path")
        if not source_path:
            raise ValueError("source_path is required for push_versioned")

        # Parse version if needed
        version_parts = {}
        if self._config.version_parser:
            version_parts = self._config.version_parser(version)

        # Create the product key
        product_key = self._config.key_factory(key, version, version_parts)

        # Build target key
        target_path = kwargs.get("target_path", "")
        full_target_key = (
            f"{product_key.path}/{target_path}" if target_path else product_key.path
        )

        logger.info(
            f"Pushing to {self._config.folder_name}: {source_path} -> {full_target_key}"
        )

        # Add metadata if available
        push_kwargs = {
            k: v for k, v in kwargs.items() if k not in ["source_path", "target_path"]
        }
        if self._config.metadata_generator and "metadata" not in push_kwargs:
            push_kwargs["metadata"] = self._config.metadata_generator()

        result = self.storage.push(
            key=full_target_key, filepath=source_path, **push_kwargs
        )
        return result

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        mapped_key = _map_product_name(key)
        if self._config.list_versions_impl:
            versions = self._config.list_versions_impl(self, mapped_key, kwargs)
        else:
            # Default implementation
            folder_key = f"{mapped_key}/{self._config.folder_name}"
            if not self.storage.exists(folder_key):
                return []
            versions = self.storage.get_subfolders(folder_key)
            # Filter out 'latest' for published
            if "exclude_latest" in kwargs and kwargs["exclude_latest"]:
                versions = [v for v in versions if v != "latest"]

        return sorted(versions, reverse=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        if not self._config.supports_latest:
            raise NotImplementedError(
                "Builds don't have a meaningful 'latest' version. Use list_versions() to see available builds."
            )
        versions = self.list_versions(key, exclude_latest=True)
        if not versions:
            raise IndexError(f"No versions found for '{key}'")
        return versions[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def resource_exists(
        self, key: str, version: str, resource_path: str, **kwargs
    ) -> bool:
        """Check if a resource exists in the product version.

        Args:
            key: Product name
            version: Version string
            resource_path: Path to resource within version (e.g., "build_metadata.json")
            **kwargs: Additional arguments

        Returns:
            True if resource exists, False otherwise
        """
        # Parse version if needed
        version_parts = {}
        if self._config.version_parser:
            version_parts = self._config.version_parser(version)

        # Create the product key
        product_key = self._config.key_factory(key, version, version_parts)

        # Build full storage key
        full_key = (
            f"{product_key.path}/{resource_path}" if resource_path else product_key.path
        )

        return self.storage.exists(full_key)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:
        # This could also be configurable if needed
        return Path("edm") / self._config.folder_name / "datasets" / key / version


# Version parsers
def _parse_draft_version(version: str) -> dict:
    """Parse version in format 'version.revision' into dict."""
    if "." not in version:
        raise ValueError(f"Version '{version}' should be in format 'version.revision'")
    parts = version.rsplit(".", 1)
    return {"version": parts[0], "revision": parts[1]}


def _parse_plan_version(version: str) -> dict:
    """Parse plan version in format 'version.revision' or partition format into dict.

    Examples:
        "26v1.1" -> {"version": "26v1", "revision": "1"}
        "26v1.1_my_note" -> {"version": "26v1", "revision": "1_my_note"}
        "2026v1:main:20260622T1325" -> {"version": "2026v1:main:20260622T1325", "revision": ""}
    """
    # Check if this is a partition key (format: version:branch:timestamp)
    if version.count(":") == 2:
        return {"version": version, "revision": ""}
    # Otherwise, parse traditional version.revision format
    if "." not in version:
        raise ValueError(
            f"Version '{version}' should be in format 'version.revision' or 'version:branch:timestamp'"
        )
    parts = version.rsplit(".", 1)
    return {"version": parts[0], "revision": parts[1]}


# Key factories
def _create_draft_key(product: str, version: str, parsed: dict) -> DraftKey:
    mapped_product = _map_product_name(product)
    return DraftKey(
        mapped_product, version=parsed["version"], revision=parsed["revision"]
    )


def _create_build_key(product: str, version: str, parsed: dict) -> BuildKey:
    mapped_product = _map_product_name(product)
    return BuildKey(mapped_product, build=version)


def _create_publish_key(product: str, version: str, parsed: dict) -> PublishKey:
    mapped_product = _map_product_name(product)
    return PublishKey(mapped_product, version=version)


def _create_plan_key(product: str, version: str, parsed: dict) -> PlanKey:
    """Create plan key. Plans don't use revisions - version is the complete identifier."""
    mapped_product = _map_product_name(product)
    # Plans are stored at {product}/plan/{version}/ without revision subfolders
    return PlanKey(mapped_product, version=version, revision="")


# List versions implementations
def _list_draft_versions(
    connector: "EdmConnector", key: str, kwargs: dict
) -> list[str]:
    """List draft versions in version.revision format.
    Note: key is already mapped by the time it reaches this function."""
    draft_folder_key = f"{key}/draft"
    if not connector.storage.exists(draft_folder_key):
        return []

    versions = []
    version_folders = connector.storage.get_subfolders(draft_folder_key)
    for version_name in version_folders:
        revision_folder_key = f"{key}/draft/{version_name}"
        revision_folders = connector.storage.get_subfolders(revision_folder_key)
        for revision_name in revision_folders:
            versions.append(f"{version_name}.{revision_name}")

    return versions


def _list_plan_versions(connector: "EdmConnector", key: str, kwargs: dict) -> list[str]:
    """List plan versions.
    Plans are stored at {product}/plan/{version}/ without revision subfolders.
    Note: key is already mapped by the time it reaches this function."""
    plan_folder_key = f"{key}/plan"
    if not connector.storage.exists(plan_folder_key):
        return []

    # Plans don't use revisions - just return version folders directly
    versions = connector.storage.get_subfolders(plan_folder_key)
    return versions


# Metadata generators
def _generate_build_metadata() -> dict:
    """Generate standard S3 metadata for builds."""
    metadata = {
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat(),
        "commit": git.commit_hash(),
    }
    if CI:
        metadata["run-url"] = git.action_url()
    return metadata


# Factory functions
def create_drafts_connector(
    storage: PathedStorageConnector | None = None,
) -> EdmConnector:
    config = EdmConnectorConfig(
        conn_type="edm.publishing.drafts",
        folder_name="draft",
        key_factory=_create_draft_key,
        version_parser=_parse_draft_version,
        list_versions_impl=_list_draft_versions,
    )
    return EdmConnector(config=config, storage=storage)


def create_builds_connector(
    storage: PathedStorageConnector | None = None,
) -> "_BuildsConnector":
    config = EdmConnectorConfig(
        conn_type="edm.publishing.builds",
        folder_name="build",
        key_factory=_create_build_key,
        supports_latest=False,
        metadata_generator=_generate_build_metadata,
    )
    # Create instance using object.__new__ to bypass custom __new__ logic
    connector = object.__new__(_BuildsConnector)
    EdmConnector.__init__(connector, config=config, storage=storage)
    return connector


def create_published_connector(
    storage: PathedStorageConnector | None = None,
) -> EdmConnector:
    config = EdmConnectorConfig(
        conn_type="edm.publishing.published",
        folder_name="publish",
        key_factory=_create_publish_key,
    )
    return EdmConnector(config=config, storage=storage)


def create_plan_connector(
    storage: PathedStorageConnector | None = None,
) -> "_PlanConnector":
    config = EdmConnectorConfig(
        conn_type="edm.publishing.plan",
        folder_name="plan",
        key_factory=_create_plan_key,
        version_parser=_parse_plan_version,
        list_versions_impl=_list_plan_versions,
        supports_latest=True,
        metadata_generator=_generate_build_metadata,
    )
    # Create instance using object.__new__ to bypass custom __new__ logic
    connector = object.__new__(_PlanConnector)
    EdmConnector.__init__(connector, config=config, storage=storage)
    return connector


# Connector wrapper classes for backwards compatibility
class DraftsConnector:
    """Connector for EDM draft publishing workflows.

    This is a compatibility wrapper around the unified EdmConnector.
    """

    def __new__(  # type: ignore[misc]
        cls, storage: PathedStorageConnector | None = None, **kwargs
    ) -> EdmConnector:
        """Create a DraftsConnector instance."""
        return create_drafts_connector(storage=storage)

    @staticmethod
    def create(storage: PathedStorageConnector | None = None) -> EdmConnector:
        """Create a DraftsConnector with lazy-loaded S3 storage."""
        return create_drafts_connector(storage=storage)


class _BuildsConnector(EdmConnector):
    """Internal BuildsConnector implementation.

    Extends the unified EdmConnector with build-specific push logic.
    """

    def _upload_build(
        self,
        build_dir: Path,
        product: str,
        *,
        acl: s3.ACL | None = None,
        build_name: str | None = None,
    ) -> BuildKey:
        """
        Uploads a product build to an S3 bucket using cloudpathlib.

        This function handles uploading a local output folder to a specified
        location in an S3 bucket. The path, product, and build name must be
        provided, along with an optional ACL (Access Control List) to control
        file access in S3.

        Raises:
            FileNotFoundError: If the provided output_path does not exist.
            ValueError: If the build name is not provided and cannot be found in the environment variables.
        """
        if not build_dir.exists():
            raise FileNotFoundError(f"Path {build_dir} does not exist")
        build_name = build_name or BUILD_NAME
        if not build_name:
            raise ValueError(
                f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
            )
        # Map product name (e.g., "edde" -> "db-eddt")
        mapped_product = _map_product_name(product)
        build_key = BuildKey(mapped_product, build_name)

        logger.info(f'Uploading {build_dir} to {build_key.path} with ACL "{acl}"')

        # Generate metadata using the config's metadata_generator
        metadata = (
            self._config.metadata_generator() if self._config.metadata_generator else {}
        )

        self.storage.push(
            key=build_key.path,
            filepath=str(build_dir),
            acl=str(acl) if acl else "private",
            metadata=metadata,
        )

        return build_key

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        # For builds, the "version" is the build name/ID
        connector_args = kwargs["connector_args"]
        acl = (
            s3.string_as_acl(connector_args["acl"])
            if connector_args.get("acl")
            else None
        )

        logger.info(f"Pushing build for product: {key}, build: {version}")
        result = self._upload_build(
            build_dir=kwargs["build_path"],
            product=key,
            acl=acl,
            build_name=version,
        )
        return asdict(result)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:
        # Builds use "builds" (plural) in the path
        return Path("edm") / "builds" / "datasets" / key / version


class _PlanConnector(EdmConnector):
    """Internal PlanConnector implementation.

    Extends the unified EdmConnector with plan-specific push logic including revision numbering.
    """

    def _upload_plan(
        self,
        plan_dir: Path,
        product: str,
        version: str,
        *,
        plan_note: str = "",
        acl: s3.ACL | None = None,
        target_filename: str | None = None,
    ) -> PlanKey:
        """
        Uploads a planned recipe to S3.

        Plans are stored directly at {product}/plan/{version}/ without revision subfolders.
        The version string itself serves as the unique identifier.

        Args:
            plan_dir: Directory containing recipe.lock.yml or path to recipe.lock.yml file
            product: Product name (e.g., 'db-eddt')
            version: Version string in any format (e.g., '2026:ar_edde:20260623T1809')
            plan_note: Unused for plans (kept for API compatibility)
            acl: S3 ACL for uploaded files
            target_filename: Optional target filename (used when uploading a single file)

        Returns:
            PlanKey with empty revision

        Raises:
            FileNotFoundError: If the provided plan_dir does not exist.
        """
        if not plan_dir.exists():
            raise FileNotFoundError(f"Path {plan_dir} does not exist")

        # Map product name to S3 product name
        mapped_product = _map_product_name(product)

        # Plans don't use revisions - the version is the complete identifier
        plan_key = PlanKey(mapped_product, version, "")

        # Determine upload path
        if target_filename:
            # Single file upload - append filename to path
            upload_key = f"{plan_key.path}{target_filename}"
            logger.info(f'Uploading plan file to {upload_key} with ACL "{acl}"')
        else:
            # Directory upload
            upload_key = plan_key.path
            logger.info(f'Uploading plan directory to {upload_key} with ACL "{acl}"')

        # Generate metadata using the config's metadata_generator
        metadata = (
            self._config.metadata_generator() if self._config.metadata_generator else {}
        )

        self.storage.push(
            key=upload_key,
            filepath=str(plan_dir),
            acl=str(acl) if acl else "private",
            metadata=metadata,
        )

        return plan_key

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        """Push a plan with automatic revision numbering."""
        plan_note = kwargs.get("plan_note", "")
        acl = kwargs.get("acl")
        target_path = kwargs.get("target_path")

        logger.info(
            f"Pushing plan for product: {key}, version: {version}, note: {plan_note}"
        )
        result = self._upload_plan(
            plan_dir=Path(kwargs["source_path"]),
            product=key,
            version=version,
            plan_note=plan_note,
            acl=acl,
            target_filename=target_path,
        )
        return asdict(result)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:
        # Plans use "plan" (singular) in the path
        return Path("edm") / "plan" / "datasets" / key / version


class BuildsConnector:
    """Connector for EDM build publishing workflows.

    This is a compatibility wrapper around the unified EdmConnector.
    """

    def __new__(  # type: ignore[misc]
        cls, storage: PathedStorageConnector | None = None, **kwargs
    ) -> "_BuildsConnector":
        """Create a BuildsConnector instance."""
        return create_builds_connector(storage=storage)

    @staticmethod
    def create(storage: PathedStorageConnector | None = None) -> "_BuildsConnector":
        """Create a BuildsConnector with lazy-loaded S3 storage."""
        return create_builds_connector(storage=storage)


class PublishedConnector:
    """Connector for EDM published workflows.

    This is a compatibility wrapper around the unified EdmConnector.
    """

    def __new__(  # type: ignore[misc]
        cls, storage: PathedStorageConnector | None = None, **kwargs
    ) -> EdmConnector:
        """Create a PublishedConnector instance."""
        return create_published_connector(storage=storage)

    @staticmethod
    def create(storage: PathedStorageConnector | None = None) -> EdmConnector:
        """Create a PublishedConnector with lazy-loaded S3 storage."""
        return create_published_connector(storage=storage)


class PlanConnector:
    """Connector for EDM plan workflows.

    This is a compatibility wrapper around the unified EdmConnector.
    """

    def __new__(  # type: ignore[misc]
        cls, storage: PathedStorageConnector | None = None, **kwargs
    ) -> "_PlanConnector":
        """Create a PlanConnector instance."""
        return create_plan_connector(storage=storage)

    @staticmethod
    def create(storage: PathedStorageConnector | None = None) -> "_PlanConnector":
        """Create a PlanConnector with lazy-loaded S3 storage."""
        return create_plan_connector(storage=storage)


# Helper function for external use
def get_builds(product: str) -> list[str]:
    """Get all build versions for a product."""
    mapped_product = _map_product_name(product)
    return sorted(
        s3.get_subfolders(_bucket(), f"{mapped_product}/build/"), reverse=True
    )

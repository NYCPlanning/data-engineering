from cloudpathlib.azure import AzureBlobClient
from cloudpathlib import S3Client, CloudPath
from dataclasses import dataclass
from enum import Enum
import logging as default_logging
import os
from pathlib import Path
import shutil
from typing import Any, NotRequired, TypedDict, Unpack

from dcpy.configuration import DEFAULT_S3_URL
from dcpy.utils.logging import logger
from dcpy.connectors.registry import Connector

default_logging.getLogger("azure").setLevel(
    "ERROR"
)  # The Azure info logging is... verbose


class LocalPathWrapper:
    """
    Wrapper for pathlib.Path that provides a .copy() and .copytree() method,
    mimicking cloudpathlib.CloudPath's API for local paths.
    Supports '/' operator for path joining.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)

    def copy(self, destination: "Path | HybridPath", **kwargs):
        """Copy self to destination folder of file, if self is a file."""
        if not self.path.exists():
            raise ValueError(f"Path {self.path} doesn't exist.")
        elif not self.path.is_file():
            raise ValueError(
                f"Path {self.path} should be a file. To copy a directory tree use the method copytree."
            )

        if isinstance(destination, LocalPathWrapper):
            shutil.copy(self.path, destination.path)
        elif isinstance(destination, Path):
            shutil.copy(self.path, destination)
        else:
            destination.upload_from(self.path, **kwargs)
        return destination

    def rmtree(self):
        shutil.rmtree(self.path)

    def copytree(self, target: "Path | HybridPath"):
        """
        Recursively copy this directory to the target path.
        If target exists, it will be overwritten.
        """
        if isinstance(target, Path):
            target = LocalPathWrapper(target)

        if target.exists():
            target.rmtree()

        if isinstance(target, LocalPathWrapper):
            shutil.copytree(self.path, target.path)
        elif isinstance(target, Path):
            shutil.copytree(self.path, target)
        else:
            target.upload_from(self.path)

        return target

    def __str__(self):
        return str(self.path)

    def __getattr__(self, attr):
        # Delegate other attributes/methods to the underlying Path
        return getattr(self.path, attr)

    def __truediv__(self, key: str | Path) -> "LocalPathWrapper":
        # Support '/' operator for path joining
        return LocalPathWrapper(self.path / key)


HybridPath = CloudPath | LocalPathWrapper


class StorageType(Enum):
    S3 = "s3"
    AZURE = "az"
    LOCAL = "local"


class StorageKwargs(TypedDict, total=True):
    storage_backend: StorageType
    s3_endpoint_url: NotRequired[str]
    s3_bucket: NotRequired[str]
    local_dir: NotRequired[Path]
    az_connection_string: NotRequired[str]
    az_container_name: NotRequired[str]
    root_folder: NotRequired[str | Path]


@dataclass
class HybridPathedStorage:
    """Underlying storage for a connector"""

    root_path: HybridPath
    storage_type: StorageType

    class Factory:
        @staticmethod
        def azure(
            az_container_name: str,
            az_connection_string: str | None = None,
            root_folder: Path | str = "",
        ) -> "HybridPathedStorage":
            root_path = AzureBlobClient(
                connection_string=az_connection_string
                or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            ).CloudPath(f"az://{az_container_name}/{root_folder}")
            return HybridPathedStorage(
                root_path=root_path, storage_type=StorageType.AZURE
            )

        @staticmethod
        def s3(
            s3_bucket: str,
            s3_endpoint_url: str | None = None,
            root_folder: Path | str = "",
            s3_public_upload_files: bool = False,
            s3_file_metadata: dict | None = None,
        ) -> "HybridPathedStorage":
            # Merge extra_args with ACL and Metadata if provided
            extra_args: dict[str, Any] = {}
            if s3_public_upload_files:
                extra_args["ACL"] = "public-read"
            if s3_file_metadata:
                extra_args["Metadata"] = s3_file_metadata
            root_path = S3Client(
                endpoint_url=s3_endpoint_url or DEFAULT_S3_URL, extra_args=extra_args
            ).CloudPath(f"s3://{s3_bucket}/{root_folder}")
            return HybridPathedStorage(root_path=root_path, storage_type=StorageType.S3)

        @staticmethod
        def local(local_dir: Path | str) -> "HybridPathedStorage":
            root_path = LocalPathWrapper(Path(local_dir))
            return HybridPathedStorage(
                root_path=root_path, storage_type=StorageType.LOCAL
            )

    @staticmethod
    def from_args(**storage_kwargs: Unpack[StorageKwargs]) -> "HybridPathedStorage":
        """
        Instantiate a HybridPathedStorage object from a set of keyword arguments.

        This method provides a generic interface for creating a storage instance using
        a dictionary of keyword arguments, rather than invoking the specific factory methods
        directly.

        Returns:
            HybridPathedStorage: An instance configured for the specified backend.
        """
        storage_backend = storage_kwargs["storage_backend"]
        match storage_backend:
            case StorageType.S3:
                assert "s3_bucket" in storage_kwargs, "s3_bucket must be provided"
                return HybridPathedStorage.Factory.s3(
                    s3_bucket=storage_kwargs["s3_bucket"],
                    s3_endpoint_url=storage_kwargs.get("s3_endpoint_url"),
                    root_folder=storage_kwargs.get("root_folder") or "",
                )
            case StorageType.AZURE:
                assert "az_container_name" in storage_kwargs, (
                    "az_container_name must be provided"
                )
                az_container_name = storage_kwargs["az_container_name"]
                return HybridPathedStorage.Factory.azure(
                    az_connection_string=storage_kwargs.get("az_connection_string"),
                    az_container_name=az_container_name,
                    root_folder=storage_kwargs.get("root_folder") or "",
                )
            case StorageType.LOCAL:
                assert "local_dir" in storage_kwargs, "local_dir must be provided"
                return HybridPathedStorage.Factory.local(storage_kwargs["local_dir"])
            case _:
                raise ValueError(f"Unsupported storage backend: {storage_backend}")

    def __str__(self):
        return f"{self.storage_type.value}://{self.root_path}"


class PathedStorageConnector(Connector, arbitrary_types_allowed=True):
    """Connector where all the keys are expected to be stringified relative paths.

    In theory they all work the same across all providers (s3, azure, localstorage, etc.) provided
    that `storage` implements CloudPath's augmented Pathlib API.
    """

    conn_type: str
    storage: HybridPathedStorage

    base_storage_kwargs: StorageKwargs

    @staticmethod
    def from_storage_kwargs(
        conn_type: str,
        _validate_root_path: bool = True,
        **storage_kwargs: Unpack[StorageKwargs],
    ) -> "PathedStorageConnector":
        storage = HybridPathedStorage.from_args(**storage_kwargs)
        if _validate_root_path:
            logger.info(f"validating {storage.root_path}")
            assert storage.root_path.exists(), (
                f"The root path {storage.root_path} doesn't exist"
            )

        # there is a case where we have to create a different storage object, ie uploading to s3
        # so we need to stash the kwargs
        return PathedStorageConnector(
            conn_type=conn_type,
            base_storage_kwargs=storage_kwargs,  # type: ignore
            storage=storage,
        )

    def push(self, key: str, **kwargs) -> dict:
        # Push a file or directory from local to storage at key
        metadata = kwargs.get("metadata", {})

        source = kwargs.get("filepath")
        if source is None:
            raise ValueError("'filepath' must be provided to push")
        source_path = LocalPathWrapper(source)

        acl = kwargs.get("acl")
        if acl == "public-read" and (self.storage.storage_type != StorageType.S3):
            raise ValueError(
                f"Public ACL is only supported for S3 storage, found ACL: {acl} for {self.storage.storage_type}"
            )

        # Leaky Abstraction Alert!
        # Handle S3 public ACL. Unfortunately we have to instantiate a new client
        # with the ACL and file metadata
        if self.storage.storage_type == StorageType.S3:
            dest_storage = HybridPathedStorage.Factory.s3(
                root_folder=self.base_storage_kwargs.get("root_folder") or "",
                s3_bucket=self.base_storage_kwargs["s3_bucket"],  # type: ignore
                s3_endpoint_url=self.base_storage_kwargs.get("s3_endpoint_url"),
                s3_public_upload_files=(acl == "public-read"),
                s3_file_metadata=metadata,
            )
        else:
            dest_storage = self.storage

        dest_path = dest_storage.root_path / key

        if source_path.is_dir():
            if dest_path.exists():
                dest_path.rmtree()
            source_path.copytree(
                dest_path,
            )
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if dest_path.exists():
                dest_path.unlink()
            source_path.copy(dest_path)

        # Set metadata for Azure blobs if needed. Unlike S3, you do this post-upload
        if dest_storage.storage_type == StorageType.AZURE and metadata:
            blob = dest_path.client.service_client.get_blob_client(  # type: ignore
                dest_path.container,  # type: ignore
                dest_path.blob,  # type: ignore
            )
            blob.set_blob_metadata(metadata)

        return {"path": str(dest_path)}

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        # Pull a file or directory from storage at key to local destination_path
        src_path = self.storage.root_path / key
        if not src_path.exists():
            raise FileNotFoundError(f"Source path {src_path} does not exist")
        if src_path.is_dir():
            # Recursively copy directory
            if destination_path.exists():
                shutil.rmtree(destination_path)
            src_path.copytree(destination_path)
        else:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            assert destination_path.parent.exists()
            src_path.copy(destination_path)
        return {"path": destination_path}

    def get_metadata(self, key: str):
        client = self.storage.root_path.client
        if isinstance(client, AzureBlobClient):
            return client._get_metadata(self.storage.root_path / key).get(  # type: ignore
                "metadata", {}
            )
        elif isinstance(client, S3Client):
            return client._get_metadata(self.storage.root_path / key).get("extra", {})  # type: ignore
        else:
            return {}

    def exists(self, key: str) -> bool:
        return (self.storage.root_path / key).exists()

    def get_subfolders(self, prefix: str) -> list[str]:
        # List subfolders under the given prefix
        folder = self.storage.root_path / prefix
        if not folder.exists() or not folder.is_dir():
            return []
        return [f.name for f in folder.iterdir() if f.is_dir()]

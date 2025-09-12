from cloudpathlib.azure import AzureBlobClient
from cloudpathlib import S3Client, CloudPath
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import shutil
from typing import TypedDict, Optional, Union, Unpack


DEFAULT_S3_URL = "https://nyc3.digitaloceanspaces.com"


class LocalPathWrapper:
    """
    Wrapper for pathlib.Path that provides a .copy() and .copytree() method,
    mimicking cloudpathlib.CloudPath's API for local paths.
    Supports '/' operator for path joining.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)

    def copy(self, target):
        """Copy this file to the target path."""
        shutil.copy(self.path, target)
        return LocalPathWrapper(Path(target))

    def copytree(self, target: "HybridPath"):
        """
        Recursively copy this directory to the target path.
        If target exists, it will be overwritten.
        """
        # target_path = Path(target)
        if target.exists():
            target.rmtree()

        if type(target) is LocalPathWrapper:
            shutil.copytree(self.path, target.path)
        else:
            target.upload_from(self.path)

        return target

    def __getattr__(self, attr):
        # Delegate other attributes/methods to the underlying Path
        return getattr(self.path, attr)

    def __truediv__(self, key: str | Path) -> "LocalPathWrapper":
        # Support '/' operator for path joining
        return LocalPathWrapper(self.path / key)


HybridPath = CloudPath | LocalPathWrapper


class StorageBackendType(Enum):
    S3 = "s3"
    AZURE = "az"
    LOCAL = "local"


class StorageKwargs(TypedDict, total=True):
    storage_backend: StorageBackendType
    s3_endpoint_url: Optional[str]
    s3_bucket: Optional[str]
    local_dir: Optional[Path]
    az_connection_string: Optional[str]
    az_container_name: Optional[str]
    root_folder: Optional[Union[str, Path]]


@dataclass
class HybridPathedStorage:
    root_path: HybridPath
    storage_type: StorageBackendType

    @property
    def can_set_acl(self) -> bool:
        return self.storage_type == StorageBackendType.S3

    class Factory:
        @staticmethod
        def azure(
            az_connection_string: str,
            az_container_name: str,
            root_folder: Path | str = "",
        ) -> "HybridPathedStorage":
            root_path = AzureBlobClient(
                connection_string=az_connection_string
            ).CloudPath(f"az://{az_container_name}/{root_folder}")
            assert root_path.exists(), f"Root path {root_path} does not exist"
            return HybridPathedStorage(
                root_path=root_path, storage_type=StorageBackendType.AZURE
            )

        @staticmethod
        def s3(
            s3_bucket: str,
            s3_endpoint_url: str | None = None,
            root_folder: Path | str = "",
        ) -> "HybridPathedStorage":
            root_path = S3Client(
                endpoint_url=s3_endpoint_url or DEFAULT_S3_URL
            ).CloudPath(f"s3://{s3_bucket}/{root_folder}")
            assert root_path.exists(), f"Root path {root_path} does not exist"
            return HybridPathedStorage(
                root_path=root_path, storage_type=StorageBackendType.S3
            )

        @staticmethod
        def local(local_dir: Path | str) -> "HybridPathedStorage":
            root_path = LocalPathWrapper(Path(local_dir))
            assert root_path.exists(), f"Root path {root_path} does not exist"
            return HybridPathedStorage(
                root_path=root_path, storage_type=StorageBackendType.LOCAL
            )

    @staticmethod
    def from_args(**storage_kwargs: Unpack[StorageKwargs]) -> "HybridPathedStorage":
        storage_backend = storage_kwargs["storage_backend"]
        if storage_backend == StorageBackendType.S3:
            assert storage_kwargs["s3_bucket"], "s3_bucket must be provided"
            return HybridPathedStorage.Factory.s3(
                s3_bucket=storage_kwargs["s3_bucket"],
                s3_endpoint_url=storage_kwargs.get("s3_endpoint_url"),
                root_folder=storage_kwargs.get("root_folder") or "",
            )
        elif storage_backend == StorageBackendType.AZURE:
            az_connection_string = storage_kwargs["az_connection_string"]
            assert az_connection_string, "az_connection_string must be provided"
            az_container_name = storage_kwargs["az_container_name"]
            assert az_container_name, "az_container_name must be provided"

            return HybridPathedStorage.Factory.azure(
                az_connection_string=az_connection_string,
                az_container_name=az_container_name,
                root_folder=storage_kwargs.get("root_folder") or "",
            )
        elif storage_backend == StorageBackendType.LOCAL:
            assert storage_kwargs["local_dir"], "local_dir must be provided"
            return HybridPathedStorage.Factory.local(storage_kwargs["local_dir"])
        else:
            raise ValueError(f"Unsupported storage backend: {storage_backend}")

import os

from boto3.s3.transfer import TransferConfig
from cloudpathlib import CloudPath

from dcpy.connectors.hybrid_pathed_storage import HybridPathedStorage


def _root(bucket: str, transfer_config: TransferConfig | None = None) -> CloudPath:
    root = HybridPathedStorage.Factory.s3(
        s3_bucket=bucket,
        s3_endpoint_url=os.environ["AWS_S3_ENDPOINT"],
        boto3_transfer_config=transfer_config,
    ).root_path
    assert isinstance(root, CloudPath)
    return root


class AzBlobGateway:
    """A bucket behind the S3 gateway in front of Azure Blob (versitygw), configured by
    AWS_S3_ENDPOINT and the usual AWS_* credentials."""

    def __init__(self, bucket: str):
        self.root = _root(bucket)
        # versitygw's Azure backend has no multipart copy (UploadPartCopy), so copies go
        # through a client that never splits them: one CopyObject each, up to 5 GiB.
        # Uploads keep the default multipart, so each request is short.
        self._copy_root = _root(bucket, TransferConfig(multipart_threshold=5 * 2**30))

    def copy_prefix(self, src: str, dst: str) -> int:
        """Server-side copy of every object under ``src`` to ``dst``; returns the count."""
        src_dir, dst_dir = self._copy_root / src, self._copy_root / dst
        copied = 0
        for folder, _, files in src_dir.walk():
            for name in files:
                f = folder / name
                f.copy(
                    dst_dir / str(f.relative_to(src_dir)), force_overwrite_to_cloud=True
                )
                copied += 1
        return copied

    def delete_prefix(self, prefix: str) -> None:
        (self.root / prefix).rmtree()
